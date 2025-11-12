"""
Test the core functions
"""

import pytest

from socat import core


@pytest.mark.asyncio
async def test_database_exists(database_async_sesionmaker):
    return


@pytest.mark.asyncio
async def test_add_and_retrieve(database_async_sesionmaker):
    async with database_async_sesionmaker() as session:
        id = (await core.create_source(0.0, 0.0, session=session)).id

    async with database_async_sesionmaker() as session:
        source = await core.get_source(id, session=session)

    assert source.id == id
    assert source.ra == 0.0
    assert source.dec == 0.0

    async with database_async_sesionmaker() as session:
        await core.delete_source(id, session=session)


@pytest.mark.asyncio
async def test_box(database_async_sesionmaker):
    async with database_async_sesionmaker() as session:
        id1 = (await core.create_source(0.1, 0.0, session=session)).id
        id2 = (await core.create_source(1.0, 1.0, session=session)).id

    # Test we recover both sources
    async with database_async_sesionmaker() as session:
        source_list = await core.get_box(0, 2, -1, 2, session=session)

        id_list = []
        for source in source_list:
            id_list.append(source.id)

        assert id1 in id_list
        assert id2 in id_list

    # Test we don't recover source not in box
    async with database_async_sesionmaker() as session:
        source_list = await core.get_box(0, 0.5, -1, 0.5, session=session)

        id_list = []
        for source in source_list:
            id_list.append(source.id)

        assert id1 in id_list
        assert id2 not in id_list

    # Not sure if this cleanup is needed
    async with database_async_sesionmaker() as session:
        await core.delete_source(id1, session=session)
        await core.delete_source(id2, session=session)


@pytest.mark.asyncio
async def test_update(database_async_sesionmaker):
    async with database_async_sesionmaker() as session:
        id = (await core.create_source(0.0, 0.0, session=session)).id

    async with database_async_sesionmaker() as session:
        source = await core.update_source(id, 1.0, 1.0, session=session)

    assert source.id == id
    assert source.ra == 1.0
    assert source.dec == 1.0

    # Not sure if this cleanup is needed
    async with database_async_sesionmaker() as session:
        await core.delete_source(id, session=session)


@pytest.mark.asyncio
async def test_bad_id(database_async_sesionmaker):
    with pytest.raises(ValueError):
        async with database_async_sesionmaker() as session:
            await core.get_source(
                999999, session=session
            )  # I suppose this isn't stictly safe if you have a test catalog with 1M entries

    with pytest.raises(ValueError):
        async with database_async_sesionmaker() as session:
            await core.update_source(999999, 1.0, 1.0, session=session)

    with pytest.raises(ValueError):
        async with database_async_sesionmaker() as session:
            await core.delete_source(999999, session=session)
