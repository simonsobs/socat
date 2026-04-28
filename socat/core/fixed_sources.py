"""
Core functionality providing access to the fixed sourcedatabase.
"""

from astropy.coordinates import ICRS
from astropy.units import Quantity
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import RegisteredFixedSource, RegisteredFixedSourceTable, statements


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
    [s.to_model() for s in sources.scalars()] : list[RegisteredFixedSource]
        List of sources in box
    """
    # Unclear why float casts are needed but
    # comparisons raise TypeError: Boolean value of this clause is not defined
    # without the cast.
    sources = await session.execute(
        statements.get_box(lower_left=lower_left, upper_right=upper_right)
    )

    return [s.to_model() for s in sources.scalars()]


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
        await session.execute(
            statements.update_source(
                source_id=source_id,
                position=position,
                flux=flux,
                name=name,
            )
        )

        source = await session.get(RegisteredFixedSourceTable, source_id)

        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        model = source.to_model()

        await session.commit()

    return model


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
