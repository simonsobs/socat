import astropy.units as u
import numpy as np
import pytest
from astropy.coordinates import ICRS

from socat import core, generator


@pytest.mark.asyncio
async def test_gen(database_async_sessionmaker):
    t_min = 12345700
    t_max = 12345900
    async with database_async_sessionmaker() as session:
        sso = await core.create_sso(name="Davida", MPC_id=511, session=session)

        position = ICRS(1 * u.deg, 1 * u.deg)
        source = await core.create_source(
            position,
            session=session,
            name="mySrc",
            flux=1.5 * u.mJy,
        )

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
        gen = generator.SourceGenerator(source, 123456789, 987654321)
        await gen.init_interp(session=session)

    position, flux = gen.at_time(t=234567891)

    assert position.ra.value == 1
    assert position.dec.value == 1
    assert flux.value == 1.5

    async with database_async_sessionmaker() as session:
        gen = generator.SourceGenerator(sso, t_min=t_min, t_max=t_max)
        await gen.init_interp(session=session)

    position, flux = gen.at_time(12345850)

    assert position.ra.value == 2.5
    assert position.dec.value == 3.75
    assert np.isclose(flux.value, 3.1)

    # Check asking out of bounds doesn't work
    with pytest.raises(ValueError):
        gen.at_time(t_max + 100)

    # Check not initializing interp raises error
    with pytest.raises(RuntimeError):
        async with database_async_sessionmaker() as session:
            gen = generator.SourceGenerator(sso, t_min=t_min, t_max=t_max)
        gen.at_time(12345850)

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso.sso_id, session=session)
