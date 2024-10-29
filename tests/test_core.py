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
