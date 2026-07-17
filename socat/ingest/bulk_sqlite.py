"""
Build a fresh SOCat SQLite database from an ACT-format point-source FITS
catalog (fixed sources) and JPL Horizons batched-ephemeris parquet files
(solar system objects), e.g. the ones produced by sotrplib's
sotrplib/solar_system/download_ephem_from_horizons.py.

Why not the other ingest modules (socat.ingest.actfits.ingest_fits_file,
socat.ingest.jplparquet.ingest_jpl_parquet_file)? Both go through a
ClientBase (create_source()/create_sso()/create_ephem()), which for the DB
client means one INSERT + COMMIT per row -- appropriate for adding a
handful of sources to an existing catalog, but not for building a database
from scratch. A real ephemeris ingest is a different scale: at these
files' native ~2-hour cadence over 2015-2033, two ephemeris files hold on
the order of tens of millions of rows across a few hundred objects, and
one-commit-per-row would take on the order of days. This module instead
bulk-loads via raw sqlite3.executemany with SQLite tuned for bulk writes
(no WAL/fsync, foreign keys off during load), which gets the whole build
down to minutes.

The table schema itself is still created from socat's own SQLModel
metadata (see socat.database.sources), so its indexes -- moving_sources
(sso_id, time) and (time) -- are always whatever socat currently defines,
with no separate index-creation step needed here.

The database file is built on local node disk and then copied to the
requested destination -- SQLite (especially anything that touches the WAL
file) doesn't behave reliably over network filesystems like Lustre/NFS,
and a scratch/home mount is exactly where the final --output usually lives.
"""

import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import uuid7
from astropy.io import fits
from sqlalchemy import create_engine
from sqlmodel import SQLModel
from tqdm import tqdm

_TIME_FMT = "%Y-%m-%d %H:%M:%S.%f"


def create_schema(db_path: Path) -> None:
    """
    Create SOCat's tables via its own SQLModel metadata, so the on-disk
    schema (including indexes) always matches whatever socat currently
    defines.
    """
    from socat.database import ALL_TABLES  # noqa: F401  (registers metadata)

    engine = create_engine(f"sqlite:///{db_path}", future=True)
    SQLModel.metadata.create_all(bind=engine)
    engine.dispose()


def _tune_for_bulk_load(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=MEMORY")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-500000")  # ~500MB page cache


def _finalize_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")


def load_fixed_sources(
    conn: sqlite3.Connection,
    fits_path: Path,
    hdu: int = 1,
    monitored_flux_threshold_mJy: float = 20.0,
    pointing_flux_threshold_mJy: float = 300.0,
) -> int:
    """
    Load an ACT point-source FITS catalog into the `fixed_sources` table.
    Mirrors socat.ingest.actfits.ingest_fits_file's flagging logic, just
    vectorized and bulk-inserted instead of one create_source() per row.
    """
    data = fits.open(fits_path)[hdu].data
    ra = np.asarray(data["RADeg"], dtype=float)
    dec = np.asarray(data["decDeg"], dtype=float)
    flux_mJy = np.asarray(data["fluxJy"], dtype=float) * 1000.0
    names = [str(n).strip() for n in data["name"]]
    monitored = flux_mJy >= monitored_flux_threshold_mJy
    pointing = flux_mJy >= pointing_flux_threshold_mJy

    rows = [
        (
            uuid7.create().hex,
            float(ra[i]),
            float(dec[i]),
            float(flux_mJy[i]),
            names[i],
            bool(monitored[i]),
            bool(pointing[i]),
        )
        for i in range(len(data))
    ]
    conn.executemany(
        "INSERT INTO fixed_sources "
        "(source_id, ra_deg, dec_deg, flux_mJy, name, monitored, pointing) "
        "VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return len(rows)


def _parse_designation(designation: str) -> tuple[int | None, str]:
    """Split a designation like '1 Ceres' into MPC ID and name."""
    parts = str(designation).split(maxsplit=1)
    try:
        mpc_id = int(parts[0])
    except ValueError:
        mpc_id = None
    name = parts[1] if len(parts) > 1 else str(mpc_id)
    return mpc_id, name


def collect_sso_designations(
    parquet_paths: list[Path],
) -> dict[str, tuple[int | None, str]]:
    """
    Enumerate unique SSOs across all ephemeris files without reading the
    full ephemeris columns. These files have exactly one row group per
    object, so reading just the 'designation' column's first value per
    row group is enough -- pyarrow only decodes that one column, not
    ra/dec/time.
    """
    designations: dict[str, tuple[int | None, str]] = {}
    for path in parquet_paths:
        pf = pq.ParquetFile(path)
        for i in range(pf.metadata.num_row_groups):
            designation = pf.read_row_group(i, columns=["designation"])["designation"][
                0
            ].as_py()
            if designation not in designations:
                designations[designation] = _parse_designation(designation)
    return designations


def load_solar_system_objects(
    conn: sqlite3.Connection,
    designations: dict[str, tuple[int | None, str]],
) -> dict[str, str]:
    """Insert one row per unique SSO, return designation -> sso_id.hex."""
    sso_ids: dict[str, str] = {}
    rows = []
    for designation, (mpc_id, name) in designations.items():
        sso_id_hex = uuid7.create().hex
        sso_ids[designation] = sso_id_hex
        rows.append((sso_id_hex, mpc_id, name, True, False))
    conn.executemany(
        "INSERT INTO solarsystem_objects (sso_id, MPC_id, name, monitored, pointing) "
        "VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return sso_ids


def load_ephemerides(
    conn: sqlite3.Connection,
    parquet_paths: list[Path],
    sso_ids: dict[str, str],
    designations: dict[str, tuple[int | None, str]],
    chunk_rows: int = 500_000,
) -> int:
    """
    Bulk-load ephemeris points into `moving_sources`, one object (row
    group) at a time so peak memory stays bounded regardless of the
    total row count across all files.
    """
    total = 0
    insert_sql = (
        "INSERT INTO moving_sources "
        "(ephem_id, sso_id, MPC_id, name, time, ra_deg, dec_deg, flux_mJy) "
        "VALUES (?,?,?,?,?,?,?,?)"
    )
    for path in parquet_paths:
        pf = pq.ParquetFile(path)
        n_groups = pf.metadata.num_row_groups
        for i in tqdm(range(n_groups), desc=f"Ingesting {path.name}"):
            table = pf.read_row_group(
                i, columns=["designation", "datetime_utc", "ra_deg", "dec_deg"]
            )
            designation = table.column("designation")[0].as_py()
            mpc_id, name = designations[designation]
            sso_id_hex = sso_ids[designation]

            times = table.column("datetime_utc").to_pylist()
            ras = table.column("ra_deg").to_pylist()
            decs = table.column("dec_deg").to_pylist()

            rows = [
                (
                    uuid7.create().hex,
                    sso_id_hex,
                    mpc_id,
                    name,
                    t.strftime(_TIME_FMT),
                    ra,
                    dec,
                    None,
                )
                for t, ra, dec in zip(times, ras, decs)
            ]

            for start in range(0, len(rows), chunk_rows):
                conn.executemany(insert_sql, rows[start : start + chunk_rows])
            conn.commit()

            total += len(rows)
    return total


def build(
    fits_path: Path,
    ephem_paths: list[Path],
    output_path: Path,
    build_dir: Path | None = None,
) -> dict:
    """
    Build a full SOCat SQLite database from an ACT FITS catalog and one
    or more JPL ephemeris parquet files, writing the result to
    `output_path`. Returns a dict of row counts and elapsed time.
    """
    build_dir = Path(build_dir or tempfile.mkdtemp(prefix="socat_build_", dir="/tmp"))
    build_dir.mkdir(parents=True, exist_ok=True)
    build_path = build_dir / output_path.name

    if build_path.exists():
        build_path.unlink()

    t0 = time.time()
    create_schema(build_path)

    conn = sqlite3.connect(str(build_path))
    _tune_for_bulk_load(conn)

    n_fixed = load_fixed_sources(conn, fits_path)
    print(f"Loaded {n_fixed} fixed sources ({time.time() - t0:.1f}s elapsed)")

    designations = collect_sso_designations(ephem_paths)
    sso_ids = load_solar_system_objects(conn, designations)
    print(
        f"Loaded {len(sso_ids)} solar system objects ({time.time() - t0:.1f}s elapsed)"
    )

    n_ephem = load_ephemerides(conn, ephem_paths, sso_ids, designations)
    print(f"Loaded {n_ephem} ephemeris points ({time.time() - t0:.1f}s elapsed)")

    _finalize_pragmas(conn)
    conn.execute("ANALYZE")
    conn.commit()
    conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(build_path, output_path)
    build_path.unlink()
    try:
        build_dir.rmdir()
    except OSError:
        pass

    elapsed = time.time() - t0
    print(f"Wrote {output_path} in {elapsed:.1f}s")

    return {
        "n_fixed_sources": n_fixed,
        "n_sso": len(sso_ids),
        "n_ephem": n_ephem,
        "elapsed_sec": elapsed,
    }


def main():  # pragma: no cover
    import argparse as ap

    parser = ap.ArgumentParser(
        prog="socat-build-db",
        description=(
            "Build a SOCat SQLite database from an ACT FITS point-source "
            "catalog and one or more JPL Horizons ephemeris parquet files"
        ),
    )
    parser.add_argument(
        "--fits-file",
        type=Path,
        required=True,
        help="ACT-compatible FITS point source catalog (fixed sources)",
    )
    parser.add_argument(
        "--ephem-file",
        type=Path,
        action="append",
        required=True,
        dest="ephem_files",
        help="JPL Horizons batched ephemeris parquet file; repeatable",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--build-dir",
        type=Path,
        default=None,
        help="Local scratch dir to build in before copying to --output "
        "(default: a fresh temp dir under /tmp)",
    )
    args = parser.parse_args()

    build(
        fits_path=args.fits_file,
        ephem_paths=args.ephem_files,
        output_path=args.output,
        build_dir=args.build_dir,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
