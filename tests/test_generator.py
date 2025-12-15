import astropy.units as u
import pytest
from astropy.coordinates import ICRS

from socat import core


@pytest.mark.asyncio
async def test_add_and_retrieve(database_async_sessionmaker):
    t_min = 12345700
    t_max = 12345900
    async with database_async_sessionmaker() as session:
        sso = await core.create_sso(name="Davida", MPC_id=511, session=session)

        for i in range(10):
            position = ICRS(i * u.deg, 1.5 * i * u.deg)
            flux = (1.2 * i + 0.1) * u.mJy
            time = 12345600 + 100 * i
            await core.create_ephem(
                sso_id=sso.sso_id,
                MPC_id=511,
                name="Davida",
                time=time,
                position=position,
                flux=flux,
                session=session,
            )

        ephem_list = await core.get_ephem_points(sso, t_min, t_max, session=session)

    for ephem in ephem_list:
        assert t_min <= ephem.time
        assert ephem.time <= t_max
    assert len(ephem_list) == 3

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso.sso_id, session=session)
