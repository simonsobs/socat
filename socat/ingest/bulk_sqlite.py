"""
Build a fresh SOCat SQLite database from an ACT-format point-source FITS
catalog (fixed sources) and JPL Horizons batched-ephemeris parquet files
(solar system objects), e.g. the ones produced by sotrplib's
sotrplib/solar_system/download_ephem_from_horizons.py.

This module bulk-loads via raw sqlite3.executemany with 
SQLite tuned for bulk writes (no WAL/fsync, foreign keys off during load), 
which is much faster than the act-fits or jpl-parquet loading scripts.

The table schema itself is still created from socat's own SQLModel
metadata (see socat.database.sources), so its indexes -- moving_sources
(sso_id, time) and (time) -- are always whatever socat currently defines,
with no separate index-creation step needed here.

The database file is built on local node disk and then copied to the
requested destination -- SQLite (especially anything that touches the WAL
file) doesn't behave reliably over network filesystems like Lustre/NFS,
and a scratch/home mount is exactly where the final --output usually lives.

Ephemerides are committed one object (parquet row group) at a time, together
with a row in a `_bulk_ingest_progress` bookkeeping table in the same
transaction. If a build dies partway, rerunning with the same --build-dir
and --resume skips every row group already recorded there and picks up
where it left off; the bookkeeping table is dropped once the build finishes.
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
    # An on-disk rollback journal (not MEMORY) so a process killed
    # mid-transaction leaves a consistent database that --resume can reopen.
    conn.execute("PRAGMA journal_mode=TRUNCATE")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-500000")  # ~500MB page cache


_PROGRESS_TABLE = "_bulk_ingest_progress"


def _create_progress_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {_PROGRESS_TABLE} "
        "(file TEXT, row_group INTEGER, n_rows INTEGER, downsample INTEGER, "
        "PRIMARY KEY (file, row_group))"
    )
    conn.commit()


def _completed_row_groups(
    conn: sqlite3.Connection, downsample: int
) -> set[tuple[str, int]]:
    """
    Row groups already committed by a previous (interrupted) build.

    Raises
    ------
    ValueError
        If the partial build used a different downsample factor.
    """
    rows = conn.execute(
        f"SELECT file, row_group, downsample FROM {_PROGRESS_TABLE}"
    ).fetchall()
    mismatched = {d for _, _, d in rows if d != downsample}
    if mismatched:
        raise ValueError(
            f"Existing partial build used downsample={sorted(mismatched)}, "
            f"but this run requested downsample={downsample}"
        )
    return {(f, g) for f, g, _ in rows}


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
    """
    Insert one row per unique SSO, return designation -> sso_id.hex.
    SSOs already in the table (from an interrupted build) are reused
    rather than inserted again.
    """
    existing = {
        (mpc_id, name): sso_id
        for sso_id, mpc_id, name in conn.execute(
            "SELECT sso_id, MPC_id, name FROM solarsystem_objects"
        )
    }
    sso_ids: dict[str, str] = {}
    rows = []
    for designation, (mpc_id, name) in designations.items():
        if (mpc_id, name) in existing:
            sso_ids[designation] = existing[(mpc_id, name)]
            continue
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
    downsample: int = 1,
) -> int:
    """
    Bulk-load ephemeris points into `moving_sources`, one object (row
    group) at a time so peak memory stays bounded regardless of the
    total row count across all files. Each object's rows are committed
    together with its entry in the progress table, so row groups already
    recorded there (by an interrupted build) are skipped. `downsample`
    keeps every Nth point of each object, as socat-jpl-parqet does.
    Returns the number of points inserted by this call.
    """
    _create_progress_table(conn)
    done = _completed_row_groups(conn, downsample)
    total = 0
    insert_sql = (
        "INSERT INTO moving_sources "
        "(ephem_id, sso_id, MPC_id, name, time, ra_deg, dec_deg, flux_mJy) "
        "VALUES (?,?,?,?,?,?,?,?)"
    )
    for path in parquet_paths:
        pf = pq.ParquetFile(path)
        n_groups = pf.metadata.num_row_groups
        file_key = str(Path(path).resolve())
        for i in tqdm(range(n_groups), desc=f"Ingesting {path.name}"):
            if (file_key, i) in done:
                continue
            table = pf.read_row_group(
                i, columns=["designation", "datetime_utc", "ra_deg", "dec_deg"]
            )
            if downsample > 1:
                table = table.take(np.arange(0, table.num_rows, downsample))
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
            conn.execute(
                f"INSERT INTO {_PROGRESS_TABLE} VALUES (?,?,?,?)",
                (file_key, i, len(rows), downsample),
            )
            conn.commit()

            total += len(rows)
    return total


def build(
    fits_path: Path | None,
    ephem_paths: list[Path],
    output_path: Path,
    build_dir: Path | None = None,
    downsample: int = 1,
    resume: bool = False,
) -> dict:
    """
    Build a full SOCat SQLite database from an (optional) ACT FITS catalog
    and one or more JPL ephemeris parquet files, writing the result to
    `output_path`. With `resume`, an existing partial database in
    `build_dir` (left behind by an interrupted build) is continued instead
    of started over. Returns a dict of row counts and elapsed time; n_ephem
    counts only points inserted by this call.

    Raises
    ------
    ValueError
        If `resume` is set without an explicit `build_dir`.
    """
    if resume and build_dir is None:
        raise ValueError("resume requires an explicit build_dir")
    build_dir = Path(build_dir or tempfile.mkdtemp(prefix="socat_build_", dir="/tmp"))
    build_dir.mkdir(parents=True, exist_ok=True)
    build_path = build_dir / output_path.name

    if build_path.exists():
        if resume:
            print(f"Resuming partial build at {build_path}")
        else:
            build_path.unlink()

    t0 = time.time()
    create_schema(build_path)

    conn = sqlite3.connect(str(build_path))
    _tune_for_bulk_load(conn)

    n_fixed = 0
    if fits_path is not None:
        (n_existing,) = conn.execute("SELECT COUNT(*) FROM fixed_sources").fetchone()
        if n_existing:
            n_fixed = n_existing
            print(f"Keeping {n_fixed} fixed sources from the partial build")
        else:
            n_fixed = load_fixed_sources(conn, fits_path)
            print(f"Loaded {n_fixed} fixed sources ({time.time() - t0:.1f}s elapsed)")

    designations = collect_sso_designations(ephem_paths)
    sso_ids = load_solar_system_objects(conn, designations)
    print(
        f"Loaded {len(sso_ids)} solar system objects ({time.time() - t0:.1f}s elapsed)"
    )

    n_ephem = load_ephemerides(
        conn, ephem_paths, sso_ids, designations, downsample=downsample
    )
    print(f"Loaded {n_ephem} ephemeris points ({time.time() - t0:.1f}s elapsed)")

    conn.execute(f"DROP TABLE {_PROGRESS_TABLE}")
    conn.commit()
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
        default=None,
        help="ACT-compatible FITS point source catalog (fixed sources); "
        "omit to build an SSO-only database",
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
    parser.add_argument(
        "-d",
        "--downsample",
        type=int,
        default=1,
        help="Keep every Nth ephemeris point per object (e.g., 2 halves the cadence)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue an interrupted build left in --build-dir instead of "
        "starting over (requires --build-dir)",
    )
    args = parser.parse_args()
    if args.resume and args.build_dir is None:
        parser.error("--resume requires --build-dir")

    build(
        fits_path=args.fits_file,
        ephem_paths=args.ephem_files,
        output_path=args.output,
        build_dir=args.build_dir,
        downsample=args.downsample,
        resume=args.resume,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
