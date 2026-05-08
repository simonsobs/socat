import astropy.units as u
import numpy as np
import pytest
from astropy.coordinates import ICRS
from astropy.time import Time

from socat import core, generator


@pytest.mark.asyncio
async def test_gen(database_async_sessionmaker):
    t_min = Time("2025-02-01T00:00:00.00")
    t_max = t_min + 1100 * u.s
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
            time = t_min + (100 * i) * u.s
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
    assert len(ephem_list) == 10

    async with database_async_sessionmaker() as session:
        gen = generator.SourceGenerator(
            source, Time("2025-01-01T00:00:00.00"), Time("2026-01-01T00:00:00.00")
        )
        await gen.init_interp(session=session)

    position, flux = gen.at_time(t=Time("2025-06-01T00:00:00.00"))

    assert position.ra.value == 1
    assert position.dec.value == 1
    assert flux.value == 1.5

    async with database_async_sessionmaker() as session:
        gen = generator.SourceGenerator(sso, t_min=t_min, t_max=t_max)
        await gen.init_interp(session=session)

    position, flux = gen.at_time(Time("2025-02-01 00:04:10.000000"))

    assert position.ra.value == 2.5
    assert position.dec.value == 3.75
    assert np.isclose(flux.value, 3.1)

    # Check asking out of bounds doesn't work
    with pytest.raises(ValueError):
        gen.at_time(t_max + 100 * u.s)

    # Check not initializing interp raises error
    with pytest.raises(RuntimeError):
        async with database_async_sessionmaker() as session:
            gen = generator.SourceGenerator(sso, t_min=t_min, t_max=t_max)
        gen.at_time(Time("2025-02-01 00:04:10.000000"))

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso.sso_id, session=session)
        await core.delete_source(source.source_id, session=session)


@pytest.mark.asyncio
async def test_get_box(database_async_sessionmaker):
    t_min = Time("2025-02-01T00:00:00.00")
    t_max = t_min + 3 * u.h
    lower_left = ICRS(0 * u.deg, 0 * u.deg)
    upper_right = ICRS(4 * u.deg, 4 * u.deg)
    async with database_async_sessionmaker() as session:
        position_m1 = ICRS(1 * u.deg, 1 * u.deg)
        m1 = await core.create_source(
            position_m1,
            session=session,
            name="mySrc1",
            flux=1.5 * u.mJy,
        )

        position_m2 = ICRS(10 * u.deg, 10 * u.deg)
        m2 = await core.create_source(
            position_m2,
            session=session,
            name="mySrc2",
            flux=1.5 * u.mJy,
        )

        davida = await core.create_sso(name="Davida", MPC_id=511, session=session)

        for i in range(10):
            position = ICRS(i * u.deg, i * u.deg)
            flux = (1.2 * i + 0.1) * u.mJy
            time = t_min + i * u.h
            await core.create_ephem(
                sso_id=davida.sso_id,
                MPC_id=davida.MPC_id,
                name=davida.name,
                time=time,
                position=position,
                flux=flux,
                session=session,
            )

        diotima = await core.create_sso(name="Diotima", MPC_id=423, session=session)

        for i in range(10):
            position = ICRS(i * u.deg, i * u.deg)
            flux = (0.5 * i + 0.1) * u.mJy
            time = (
                t_min + -9 * u.h + i * u.h
            )  # Don't come into the box until after the end time of the box
            await core.create_ephem(
                sso_id=diotima.sso_id,
                MPC_id=diotima.MPC_id,
                name=diotima.name,
                time=time,
                position=position,
                flux=flux,
                session=session,
            )

        ceres = await core.create_sso(name="Ceres", MPC_id=1, session=session)

        for i in range(10):
            position = ICRS((i + 5) * u.deg, (i + 5) * u.deg)  # Never come into the box
            flux = (2.5 * i + 0.1) * u.mJy
            time = t_min + i * u.h
            await core.create_ephem(
                sso_id=ceres.sso_id,
                MPC_id=ceres.MPC_id,
                name=ceres.name,
                time=time,
                position=position,
                flux=flux,
                session=session,
            )

    async with database_async_sessionmaker() as session:
        sources: list[generator.SourceGenerator] = await core.get_box(
            t_min=t_min,
            t_max=t_max,
            lower_left=lower_left,
            upper_right=upper_right,
            session=session,
        )

        fixed_sources = [m1, m2]
        sso_sources = [davida, diotima, ceres]

    source_names = [gen.source.name for gen in sources]

    assert m1.name in source_names
    assert m2.name not in source_names
    assert davida.name in source_names
    assert diotima.name not in source_names
    assert ceres.name not in source_names

    async with database_async_sessionmaker() as session:
        for source in sso_sources:
            await core.delete_sso(source.sso_id, session=session)
        for source in fixed_sources:
            await core.delete_source(source.source_id, session=session)
