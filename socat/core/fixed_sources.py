"""
Core functionality providing access to the fixed sourcedatabase.
"""

from astropy.coordinates import ICRS
from astropy.units import Quantity
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import RegisteredFixedSource, RegisteredFixedSourceTable


async def create_source(
    position: ICRS,
    session: AsyncSession,
    name: str | None = None,
    flux: Quantity | None = None,
) -> RegisteredFixedSource:
    """
    Create a new source in the database.

    Parameters
    ----------
    position : ICRS
        ICRS position of source
    flux : Quantity | None
        Flux of source. Optional.
    name : str | None
        Name of source. Optional.
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source.to_model() : RegisteredFixedSource
        Source that has been created
    """
    if flux is not None:
        flux = flux.to_value("mJy")
    source = RegisteredFixedSourceTable(
        ra_deg=position.ra.to_value("deg"),
        dec_deg=position.dec.to_value("deg"),
        name=name,
        flux_mJy=flux,
    )

    async with session.begin():
        session.add(source)
        await session.commit()

    return source.to_model()


async def get_source(source_id: int, session: AsyncSession) -> RegisteredFixedSource:
    """
    Get a source from the database.

    Parameters
    ----------
    source_id : int
        ID of source of interest
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source.to_mode() : RegisteredFixedSource
        Source that has been created

    Raises
    ------
    ValueError
        If the source is not found.
    """
    source = await session.get(RegisteredFixedSourceTable, source_id)

    if source is None:
        raise ValueError(f"Source with ID {source_id} not found")

    return source.to_model()


async def get_box(
    lower_left: ICRS,
    upper_right: ICRS,
    session: AsyncSession,
) -> list[RegisteredFixedSource]:
    """
    Get all sources in a box bounded by ra_min, ra_max, dec_min, dec_max.

    Parameters
    ----------
    lower_left : ICRS
        Lower left bound of box
    upper_right : ICRS
        Upper right bound of box
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source_list : list[RegisteredFixedSource]
        List of sources in box
    """
    # Unclear why float casts are needed but
    # comparisons raise TypeError: Boolean value of this clause is not defined
    # without the cast.
    sources = await session.execute(
        select(RegisteredFixedSourceTable).where(
            float(lower_left.ra.to_value("deg")) <= RegisteredFixedSourceTable.ra_deg,
            RegisteredFixedSourceTable.ra_deg <= float(upper_right.ra.to_value("deg")),
            float(lower_left.dec.to_value("deg")) <= RegisteredFixedSourceTable.dec_deg,
            RegisteredFixedSourceTable.dec_deg
            <= float(upper_right.dec.to_value("deg")),
        )
    )

    source_list = [s.to_model() for s in sources.scalars()]

    return source_list


async def update_source(
    source_id: int,
    position: ICRS | None,
    session: AsyncSession,
    flux: Quantity | None = None,
    name: str | None = None,
) -> RegisteredFixedSource:
    """
    Update a source in the database.

    Parameters
    ----------
    position : ICRS | None
        Position of source in ICRS coordinates
    flux : Quanity | None
        Flux of source. Optional.
    session : AsyncSession
        Asynchronous session to use
    name : str | None
        Name of source

    Returns
    -------
    source.to_mode() : RegisteredFixedSource
        Source that has been updated

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        source = await session.get(RegisteredFixedSourceTable, source_id)

        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        source.ra_deg = (
            position.ra.to_value("deg") if position.ra is not None else source.ra_deg
        )
        source.dec_deg = (
            position.dec.to_value("deg") if position.dec is not None else source.dec_deg
        )
        source.flux_mJy = flux.to_value("mJy") if flux is not None else source.flux_mJy
        source.name = name if name is not None else source.name

        await session.commit()

    return source.to_model()


async def delete_source(source_id: int, session: AsyncSession) -> None:
    """
    Delete a source from the database.

    Parameters
    ----------
    id : int
        ID of source to delete
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        source = await session.get(RegisteredFixedSourceTable, source_id)

        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        await session.delete(source)
        await session.commit()

    return
