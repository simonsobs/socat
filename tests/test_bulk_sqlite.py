"""
Test the fast bulk-sqlite ingest path (socat.ingest.bulk_sqlite), used for
building a full database from files rather than adding sources one at a
time through a live client.
"""

import sqlite3

import astropy.units as u
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from astropy.coordinates import ICRS
from astropy.io import fits
from astropy.time import Time

from socat.client.db import Client
from socat.ingest import bulk_sqlite


@pytest.fixture
def act_fits_catalog(tmp_path):
    n_sources = 10
    data = np.zeros(
        n_sources,
        dtype=[("raDeg", "f8"), ("decDeg", "f8"), ("fluxJy", "f8"), ("name", "S20")],
    )
    data["raDeg"] = np.linspace(0, 359, n_sources)
    data["decDeg"] = np.linspace(-80, 80, n_sources)
    # Two sources deliberately above the monitored/pointing thresholds.
    data["fluxJy"] = np.full(n_sources, 0.001)
    data["fluxJy"][0] = 0.025  # 25 mJy -- above monitored (20 mJy)
    data["fluxJy"][1] = 0.5  # 500 mJy -- above pointing (300 mJy)
    data["name"] = np.array([f"Source_{i}" for i in range(n_sources)], dtype="S20")

    hdu = fits.BinTableHDU(data)
    fits_path = tmp_path / "act_catalog.fits"
    hdu.writeto(fits_path)
    return fits_path


@pytest.fixture
def jpl_ephem_parquet(tmp_path):
    """Two objects, 5 two-hour-cadence points each, written as one row
    group per designation -- matching the real JPL Horizons
    batched-ephemeris parquet files (verified against production data:
    each file has exactly one row group per object), which is the
    precondition collect_sso_designations()/load_ephemerides() rely on
    to avoid reading full ephemeris columns just to enumerate objects.
    A plain single-shot df.to_parquet() would NOT reproduce this (pandas
    writes one row group for the whole frame), so each object's rows are
    written as a separate row group explicitly.
    """
    start = Time("2025-01-01T00:00:00")
    parquet_path = tmp_path / "ephem.parquet"
    writer = None
    try:
        for designation, ra0 in [("1 Ceres", 10.0), ("2 Pallas", 200.0)]:
            rows = []
            for i in range(5):
                t = start + i * 2 * u.hour
                rows.append(
                    {
                        "designation": designation,
                        "datetime_utc": t.datetime,
                        "julian_day": t.jd,
                        "ra_deg": ra0 + i * 0.01,
                        "dec_deg": 5.0 + i * 0.01,
                        "distance_au": 2.5,
                    }
                )
            table = pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    return parquet_path


def test_create_schema_has_expected_indexes(tmp_path):
    db_path = tmp_path / "socat.db"
    bulk_sqlite.create_schema(db_path)

    conn = sqlite3.connect(str(db_path))
    index_names = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='moving_sources'"
        )
    }
    conn.close()
    assert "idx_moving_sources_sso_time" in index_names
    assert "ix_moving_sources_time" in index_names


def test_load_fixed_sources(tmp_path, act_fits_catalog):
    db_path = tmp_path / "socat.db"
    bulk_sqlite.create_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    bulk_sqlite._tune_for_bulk_load(conn)

    n_loaded = bulk_sqlite.load_fixed_sources(conn, act_fits_catalog)
    assert n_loaded == 10

    rows = conn.execute(
        "SELECT name, flux_mJy, monitored, pointing FROM fixed_sources ORDER BY name"
    ).fetchall()
    conn.close()
    assert len(rows) == 10

    by_name = {r[0]: r for r in rows}
    assert by_name["Source_0"][1] == pytest.approx(25.0)
    assert by_name["Source_0"][2] == 1  # monitored
    assert by_name["Source_0"][3] == 0  # not pointing
    assert by_name["Source_1"][1] == pytest.approx(500.0)
    assert by_name["Source_1"][3] == 1  # pointing
    assert by_name["Source_2"][2] == 0
    assert by_name["Source_2"][3] == 0


def test_parse_designation():
    assert bulk_sqlite._parse_designation("1 Ceres") == (1, "Ceres")
    assert bulk_sqlite._parse_designation("7335 (1989 JA)") == (7335, "(1989 JA)")


def test_collect_and_load_sso_and_ephemerides(tmp_path, jpl_ephem_parquet):
    db_path = tmp_path / "socat.db"
    bulk_sqlite.create_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    bulk_sqlite._tune_for_bulk_load(conn)

    designations = bulk_sqlite.collect_sso_designations([jpl_ephem_parquet])
    assert designations == {"1 Ceres": (1, "Ceres"), "2 Pallas": (2, "Pallas")}

    sso_ids = bulk_sqlite.load_solar_system_objects(conn, designations)
    assert set(sso_ids) == {"1 Ceres", "2 Pallas"}

    n_ephem = bulk_sqlite.load_ephemerides(
        conn, [jpl_ephem_parquet], sso_ids, designations
    )
    assert n_ephem == 10

    ceres_rows = conn.execute(
        "SELECT ra_deg, dec_deg, time FROM moving_sources WHERE sso_id = ? ORDER BY time",
        (sso_ids["1 Ceres"],),
    ).fetchall()
    conn.close()
    assert len(ceres_rows) == 5
    assert ceres_rows[0][0] == pytest.approx(10.0)
    assert ceres_rows[0][2] == "2025-01-01 00:00:00.000000"
    assert ceres_rows[-1][0] == pytest.approx(10.04)


def test_build_end_to_end(tmp_path, act_fits_catalog, jpl_ephem_parquet):
    output_path = tmp_path / "output" / "socat.db"
    build_dir = tmp_path / "build"

    summary = bulk_sqlite.build(
        fits_path=act_fits_catalog,
        ephem_paths=[jpl_ephem_parquet],
        output_path=output_path,
        build_dir=build_dir,
    )
    assert summary["n_fixed_sources"] == 10
    assert summary["n_sso"] == 2
    assert summary["n_ephem"] == 10
    assert output_path.exists()
    # build_dir should be cleaned up after the copy.
    assert not (build_dir / output_path.name).exists()

    conn = sqlite3.connect(str(output_path))
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()

    client = Client(db_url=f"sqlite:///{output_path}")
    fixed = client.get_box_fixed(
        lower_left=ICRS(ra=0.0 * u.deg, dec=-89.9 * u.deg),
        upper_right=ICRS(ra=359.999 * u.deg, dec=89.9 * u.deg),
    )
    assert len(fixed) == 10

    sso = client.get_box_sso(
        lower_left=ICRS(ra=0.0 * u.deg, dec=-89.9 * u.deg),
        upper_right=ICRS(ra=359.999 * u.deg, dec=89.9 * u.deg),
        t_min=Time("2025-01-01T00:00:00"),
        t_max=Time("2025-01-01T10:00:00"),
    )
    assert sorted(s.name for s in sso) == ["Ceres", "Pallas"]
