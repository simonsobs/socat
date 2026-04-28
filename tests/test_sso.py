"""
Test the solar system object functions
"""

import astropy.units as u
import pytest
from astropy.coordinates import ICRS
from astropy.time import Time

from socat import core


@pytest.mark.asyncio
async def test_database_exists(database_async_sessionmaker):
    return


@pytest.mark.asyncio
async def test_add_and_retrieve(database_async_sessionmaker):
    position = ICRS(1 * u.deg, 1 * u.deg)
    flux = 1.5 * u.mJy
    async with database_async_sessionmaker() as session:
        sso_id = (
            await core.create_sso(name="Davida", MPC_id=511, session=session)
        ).sso_id

        ephem_id = (
            await core.create_ephem(
                sso_id=sso_id,
                MPC_id=511,
                name="Davida",
                time=123456789,
                position=position,
                flux=flux,
                session=session,
            )
        ).ephem_id

    async with database_async_sessionmaker() as session:
        sso = await core.get_sso(sso_id, session=session)
        ephem = await core.get_ephem(ephem_id, session=session)

    assert sso.sso_id == sso_id
    assert sso.name == "Davida"
    assert sso.MPC_id == 511

    assert ephem.ephem_id == ephem_id
    assert ephem.sso_id == sso_id
    assert ephem.MPC_id == 511
    assert ephem.name == "Davida"
    assert ephem.time == 123456789
    assert ephem.position.ra.value == 1.0
    assert ephem.position.dec.value == 1.0
    assert ephem.flux == flux

    async with database_async_sessionmaker() as session:
        sso = await core.get_sso_name(sso_name="Davida", session=session)

    assert len(sso) == 1
    assert sso[0].sso_id == sso_id
    assert sso[0].name == "Davida"
    assert sso[0].MPC_id == 511

    async with database_async_sessionmaker() as session:
        sso = await core.get_sso_MPC_id(MPC_id=511, session=session)

    assert len(sso) == 1
    assert sso[0].sso_id == sso_id
    assert sso[0].name == "Davida"
    assert sso[0].MPC_id == 511

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso_id, session=session)

    # Check the cascades work
    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            ephem = await core.get_ephem(ephem_id, session=session)
        if ephem is None:
            raise ValueError("Not found")


@pytest.mark.asyncio
async def test_update(database_async_sessionmaker):
    position = ICRS(1 * u.deg, 1 * u.deg)
    flux = 1.5 * u.mJy
    async with database_async_sessionmaker() as session:
        sso_id = (
            await core.create_sso(name="Davida", MPC_id=511, session=session)
        ).sso_id

        ephem_id = (
            await core.create_ephem(
                sso_id=sso_id,
                MPC_id=511,
                name="Davida",
                time=123456789,
                position=position,
                flux=flux,
                session=session,
            )
        ).ephem_id

    position = ICRS(0 * u.deg, 0 * u.deg)
    flux = 2.5 * u.mJy
    async with database_async_sessionmaker() as session:
        sso = await core.update_sso(
            sso_id=sso_id, name="Diotima", MPC_id=423, session=session
        )
        ephem = await core.update_ephem(
            ephem_id=ephem_id,
            sso_id=sso.sso_id,
            MPC_id=423,
            name="Diotima",
            time=987654321,
            position=position,
            flux=flux,
            session=session,
        )

    assert sso.sso_id == sso_id
    assert sso.name == "Diotima"
    assert sso.MPC_id == 423

    assert ephem.ephem_id == ephem_id
    assert ephem.sso_id == sso.sso_id
    assert ephem.MPC_id == 423
    assert ephem.name == "Diotima"
    assert ephem.time == 987654321
    assert ephem.position.ra.value == 0.0
    assert ephem.position.dec.value == 0.0
    assert ephem.flux == flux

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso_id, session=session)


@pytest.mark.asyncio
async def test_time_box(database_async_sessionmaker):
    # Make three asteroids, on three trajectories, of which one will be in our box.
    # Our box will run from 1 to 3 and from t = 0 to t = 100.
    # Davida will be in the time-box
    # Diotima will overlap in time but not space
    # Ceres will overlap in space but not time
    async with database_async_sessionmaker() as session:
        sso_id_1 = (
            await core.create_sso(name="Davida", MPC_id=511, session=session)
        ).sso_id
        flux = 1.5 * u.mJy
        for i in range(3):
            position = ICRS(
                i * u.deg, i * u.deg
            )  # this is totally unphysical but who cares
            time = i * 100
            await core.create_ephem(
                sso_id=sso_id_1,
                MPC_id=511,
                name="Davida",
                time=time,
                position=position,
                flux=flux,
                session=session,
            )

        sso_id_2 = (
            await core.create_sso(name="Diotima", MPC_id=423, session=session)
        ).sso_id
        flux = 0.5 * u.mJy
        for i in range(3):
            position = ICRS(
                i * u.deg, i * u.deg
            )  # this is totally unphysical but who cares
            time = 200 + i * 100
            await core.create_ephem(
                sso_id=sso_id_2,
                MPC_id=423,
                name="Diotima",
                time=time,
                position=position,
                flux=flux,
                session=session,
            )

        sso_id_3 = (
            await core.create_sso(name="Ceres", MPC_id=1, session=session)
        ).sso_id
        flux = 2.5 * u.mJy
        for i in range(3):
            position = ICRS(
                (4 + i) * u.deg, (4 + i) * u.deg
            )  # this is totally unphysical but who cares
            time = i * 100
            await core.create_ephem(
                sso_id=sso_id_3,
                MPC_id=1,
                name="Ceres",
                time=time,
                position=position,
                flux=flux,
                session=session,
            )

    async with database_async_sessionmaker() as session:
        lower_left = ICRS(1.0 * u.deg, 1.0 * u.deg)
        upper_right = ICRS(3.0 * u.deg, 3.0 * u.deg)
        t_min = Time(0, format="unix", scale="utc")
        t_max = Time(200, format="unix", scale="utc")
        ssos = await core.get_sso_box(
            lower_left=lower_left,
            upper_right=upper_right,
            t_min=t_min,
            t_max=t_max,
            session=session,
        )

    assert len(ssos) == 1
    assert ssos[0].name == "Davida"
    assert ssos[0].MPC_id == 511

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso_id_1, session=session)
        await core.delete_sso(sso_id_2, session=session)
        await core.delete_sso(sso_id_3, session=session)


@pytest.mark.asyncio
async def test_bad_id(database_async_sessionmaker):
    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.get_sso(sso_id=999999, session=session)

    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.get_sso_name(sso_name="badName", session=session)

    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.get_sso_MPC_id(MPC_id=999999, session=session)

    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.get_ephem(ephem_id=999999, session=session)

    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.update_sso(
                sso_id=999999,
                name="Davida",
                MPC_id=511,
                session=session,
            )

    position = ICRS(1 * u.deg, 1 * u.deg)
    flux = 1 * u.mJy
    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.update_ephem(
                ephem_id=999999,
                session=session,
                sso_id=1,
                name="Davida",
                MPC_id=511,
                time=123456789,
                position=position,
                flux=flux,
            )

    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.delete_sso(sso_id=999999, session=session)

    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.delete_ephem(ephem_id=999999, session=session)
