"""
Core functionality providing access to the database.
"""

from socat.database import ExtragalacticSource, ExtragalacticSourceTable

from sqlalchemy.ext.asyncio import AsyncSession

async def create_source(ra: float, dec: float,  session: AsyncSession) -> ExtragalacticSource:
    """
    Create a new source in the database.
    """
    source = ExtragalacticSourceTable(ra=ra, dec=dec)

    async with session.begin():
        session.add(source)
        await session.commit()
    
    return source.to_model()

async def get_source(source_id: int, session: AsyncSession) -> ExtragalacticSource:
    """
    Get a source from the database.
    """
    source = await session.get(ExtragalacticSourceTable, source_id)

    if source is None:
        raise ValueError(f"Source with ID {source_id} not found")

    return source.to_model()

async def update_source(source_id: int, ra: float | None, dec: float | None, session: AsyncSession) -> ExtragalacticSource:
    """
    Update a source in the database.
    """

    async with session.begin():
        source = await session.get(ExtragalacticSourceTable, source_id)

        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        source.ra = ra if ra is not None else source.ra
        source.dec = dec if dec is not None else source.dec

        await session.commit()
    
    return source.to_model()

async def delete_source(source_id: int, session: AsyncSession) -> None:
    """
    Delete a source from the database.
    """

    async with session.begin():
        source = await session.get(ExtragalacticSourceTable, source_id)
        
        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        await session.delete(source)
        await session.commit()
    
    return 
