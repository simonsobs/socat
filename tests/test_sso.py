"""
Test the solar system object functions
"""

import astropy.units as u
import pytest
from astropy.coordinates import ICRS

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
                obj_id=sso_id,
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
    assert ephem.obj_id == sso_id
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
                obj_id=sso_id,
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
            obj_id=sso.sso_id,
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
    assert ephem.obj_id == sso.sso_id
    assert ephem.MPC_id == 423
    assert ephem.name == "Diotima"
    assert ephem.time == 987654321
    assert ephem.position.ra.value == 0.0
    assert ephem.position.dec.value == 0.0
    assert ephem.flux == flux

    async with database_async_sessionmaker() as session:
        await core.delete_sso(sso_id, session=session)
        await core.delete_ephem(ephem_id, session=session)
