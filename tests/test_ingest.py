"""
Test ingestion of catalogs of various types.
"""

import os

import numpy as np
from astropy import units as u
from astropy.coordinates import ICRS
from astropy.io import fits
from pytest import fixture

from socat.client.mock import Client as MockClient
from socat.ingest import actfits


@fixture
def act_fits_catalog(tmp_path):
    # Astropy tables are just numpy recarrays; create one with
    # decDeg, raDeg, fluxJy, name columns
    n_sources = 10

    data = np.zeros(
        n_sources,
        dtype=[("raDeg", "f8"), ("decDeg", "f8"), ("fluxJy", "f8"), ("name", "S20")],
    )
    data["raDeg"] = np.random.uniform(0, 360, n_sources)
    data["decDeg"] = np.random.uniform(-90, 90, n_sources)
    data["fluxJy"] = np.random.uniform(0.1, 10.0, n_sources)
    data["name"] = np.array([f"Source_{i}" for i in range(n_sources)], dtype="S20")

    hdu = fits.BinTableHDU(data)
    fits_path = tmp_path / "act_catalog.fits"
    hdu.writeto(fits_path)

    yield fits_path

    os.remove(fits_path)


def test_ingest_act_fits(act_fits_catalog):
    client = MockClient()
    n_ingested = actfits.ingest_fits_file(client, act_fits_catalog)
    assert n_ingested == 10

    # Verify that the sources are in the client
    sources = client.get_box(
        lower_left=ICRS(ra=0 * u.deg, dec=-90 * u.deg),
        upper_right=ICRS(ra=359.999 * u.deg, dec=90 * u.deg),
    )
    assert len(sources) == 10

    # Verify that the source properties match
    for i, source in enumerate(sources):
        assert source.name == f"Source_{i}"
        assert 0.1 * u.Jy <= source.flux <= 10.0 * u.Jy
        assert 0.0 <= source.position.ra.deg <= 360.0
        assert -90.0 <= source.position.dec.deg <= 90.0
