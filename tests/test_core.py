"""
Test the core functions
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
        id = (
            await core.create_source(
                position=position, session=session, name="mySrc", flux=flux
            )
        ).id

    async with database_async_sessionmaker() as session:
        source = await core.get_source(id, session=session)

    assert source.id == id
    assert source.position.ra.value == 1.0
    assert source.position.dec.value == 1.0
    assert source.name == "mySrc"
    assert source.flux == flux

    async with database_async_sessionmaker() as session:
        await core.delete_source(id, session=session)


@pytest.mark.asyncio
async def test_box(database_async_sessionmaker):
    position1 = ICRS(1.0 * u.deg, 1.0 * u.deg)
    position2 = ICRS(2.0 * u.deg, 2.0 * u.deg)
    flux1 = 1.5 * u.mJy
    flux2 = 2.5 * u.mJy
    async with database_async_sessionmaker() as session:
        id1 = (
            await core.create_source(
                position=position1,
                session=session,
                name="mySrc1",
                flux=flux1,
            )
        ).id
        id2 = (
            await core.create_source(
                position=position2,
                session=session,
                name="mySrc2",
                flux=flux2,
            )
        ).id

    # Test we recover both sources
    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(3.0 * u.deg, 3.0 * u.deg)
    async with database_async_sessionmaker() as session:
        source_list = await core.get_box(
            lower_left=lower_left, upper_right=upper_right, session=session
        )

        id_list = []
        for source in source_list:
            id_list.append(source.id)

        assert id1 in id_list
        assert id2 in id_list

    # Test we don't recover source not in box
    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(1.5 * u.deg, 1.5 * u.deg)
    async with database_async_sessionmaker() as session:
        source_list = await core.get_box(
            lower_left=lower_left, upper_right=upper_right, session=session
        )

        id_list = []
        for source in source_list:
            id_list.append(source.id)

        assert id1 in id_list
        assert id2 not in id_list

    # Not sure if this cleanup is needed
    async with database_async_sessionmaker() as session:
        await core.delete_source(id1, session=session)
        await core.delete_source(id2, session=session)


@pytest.mark.asyncio
async def test_update(database_async_sessionmaker):
    position = ICRS(0 * u.deg, 0 * u.deg)
    flux = 1.5 * u.mJy
    async with database_async_sessionmaker() as session:
        id = (
            await core.create_source(
                position=position,
                session=session,
                name="mySrc",
                flux=flux,
            )
        ).id

    position = ICRS(1 * u.deg, 1 * u.deg)
    async with database_async_sessionmaker() as session:
        source = await core.update_source(
            source_id=id,
            position=position,
            session=session,
        )
    assert source.id == id
    assert source.position.ra.value == 1.0
    assert source.position.dec.value == 1.0

    # Not sure if this cleanup is needed
    async with database_async_sessionmaker() as session:
        await core.delete_source(id, session=session)


@pytest.mark.asyncio
async def test_bad_id(database_async_sessionmaker):
    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.get_source(
                source_id=999999, session=session
            )  # I suppose this isn't stictly safe if you have a test catalog with 1M entries

    position = ICRS(1 * u.deg, 1 * u.deg)
    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.update_source(
                source_id=999999, position=position, session=session
            )

    with pytest.raises(ValueError):
        async with database_async_sessionmaker() as session:
            await core.delete_source(source_id=999999, session=session)
