import astropy.units as u
import pandas as pd
import pytest

from socat import core
from socat.ephem import read_jpl


@pytest.mark.asyncio
async def test_read_jpl(database_async_sessionmaker, tmp_path):
    # Create a mock JPL ephemeris file
    file_path = tmp_path / "jpl_ephem.parquet"
    data = {
        "designation": ["511 Davida", "511 Davida", "511 Davida"],
        "julian_day": [2451545.0, 2451546.0, 2451547.0],
        "ra_deg": [1.0, 1.5, 2.0],
        "dec_deg": [1.0, 1.5, 2.0],
        "flux_mJy": [1.5, 1.6, 1.7],
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_path)

    # Read the JPL ephemeris file and ingest into the database
    async with database_async_sessionmaker() as session:
        await read_jpl(file_path, session)

    # Verify that the SSO and ephemeris points were created in the database
    async with database_async_sessionmaker() as session:
        sso = await core.get_sso_name(sso_name="Davida", session=session)
        assert len(sso) == 1
        assert sso[0].name == "Davida"
        assert sso[0].MPC_id == 511

        ephems = await core.get_ephem_by_sso_id(sso[0].sso_id, session=session)
        assert len(ephems) == 3
        for i, ephem in enumerate(ephems):
            assert ephem.time == (data["julian_day"][i] - 2440587.5) * 86400
            assert ephem.position.ra.value == data["ra_deg"][i]
            assert ephem.position.dec.value == data["dec_deg"][i]
            assert ephem.flux == data["flux_mJy"][i] * u.mJy

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso[0].sso_id, session=session)
