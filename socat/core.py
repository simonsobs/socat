"""
Core functionality providing access to the database.
"""
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import ExtragalacticSource, ExtragalacticSourceTable


async def create_source(
    ra: float, dec: float, session: AsyncSession
) -> ExtragalacticSource:
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

async def get_box(ra_min: float, ra_max:float, dec_min:float, dec_max: float, session: AsyncSession) -> list[ExtragalacticSource]:
    """
    Get all sources in a box bounded by ra_min, ra_max, dec_min, dec_max.

    Parameters
    ----------
    ra_min : float
        Min ra of box
    ra_max : float
        Max ra of box
    dec_min : float
        Min dec of box
    dec_max : float
        Max dec of box
    session : SessionDependency
        SQAlchemy session

    Returns
    -------
    response : list[ExtragalacticSource]
        List of sources in box
    """
    sources = await session.execute(select(ExtragalacticSourceTable).where(ra_min <= ExtragalacticSourceTable.ra).where(ExtragalacticSourceTable.ra <= ra_max)
                                   .where(dec_min <= ExtragalacticSourceTable.dec).where(ExtragalacticSourceTable.dec<= dec_max))
    if sources is None:
        raise ValueError(f"No sources found in {ra_min}<= ra <={ra_max}, {dec_min}<= dec <= {dec_max}")

    source_list = []
    for source in sources.scalars():
        print(type(source))
        source_list.append(source.to_model)
    return source_list

async def update_source(
    source_id: int, ra: float | None, dec: float | None, session: AsyncSession
) -> ExtragalacticSource:
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
