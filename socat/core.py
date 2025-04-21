"""
Core functionality providing access to the database.
"""

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import (
    AstroqueryService,
    AstroqueryServiceTable,
    ExtragalacticSource,
    ExtragalacticSourceTable,
)


async def create_service(
    name: str, config: dict[str, Any], session: AsyncSession
) -> AstroqueryService:
    """
    Create a new astroquery service in the database.

    Parameters
    ----------
    name : str
        Name of service
    session : AsyncSession
        Asynchronous session to use
    config: dict[str, Any]
        json to be deserialized to config options
    """
    service = AstroqueryServiceTable(name=name, config=config)

    async with session.begin():
        session.add(service)
        await session.commit()

    return service.to_model()


async def get_service(service_id: int, session: AsyncSession) -> AstroqueryService:
    """
    Get an astroquery service from the database by id.

    Parameters
    ----------
    service_id :  int
        ID of service
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    service.to_mode() : AstroqueryService
        Requested astroquery service

    Raises
    ------
    ValueError
        If the source is not found.
    """

    service = await session.get(AstroqueryServiceTable, service_id)

    if service is None:
        raise ValueError(f"Service with ID {service_id} not found.")

    return service


async def get_service_name(
    service_name: str, session: AsyncSession
) -> list[AstroqueryService]:
    """
    Get an astroquery service from the database by id.

    Parameters
    ----------
    service_name :  int
        ID of service
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    service.to_mode() : AstroqueryService
        Requested astroquery service

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        stmt = select(AstroqueryServiceTable).where(
            AstroqueryServiceTable.name == service_name
        )

        service = await session.execute(stmt)

    service_list = [s.to_model() for s in service.scalars().all()]

    if len(service_list) == 0:
        raise ValueError(f"Service with name {service_name} not found.")

    return service_list


async def update_service(
    service_id: int,
    name: str | None,
    config: dict[str, Any] | None,
    session: AsyncSession,
) -> AstroqueryService:
    """
    Update an astroquery service in the database.

     Parameters
     ----------
     service_name : int
         ID of service
     name : str | None
         Name of service
     config: dict[str, Any]  | None
         json to be deserialized to config options
     session : AsyncSession
         Asynchronous session to use

     Returns
     -------
     service.to_mode() : AstroqueryService
         Requested astroquery service

     Raises
     ------
     ValueError
         If the source is not found.
    """

    async with session.begin():
        source = await session.get(AstroqueryServiceTable, service_id)

        if source is None:
            raise ValueError(f"Source with ID {service_id} not found")

        source.name = name if name is not None else source.name
        source.config = config if config is not None else source.config

        await session.commit()

    return source.to_model()


async def delete_service(service_id: int, session: AsyncSession) -> None:
    """
    Delete a source from the database.

    Parameters
    ----------
    service_id : int
        ID of service
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
        source = await session.get(AstroqueryServiceTable, service_id)

        if source is None:
            raise ValueError(f"Service with ID {service_id} not found")

        await session.delete(source)
        await session.commit()

    return


async def create_source(
    ra: float,
    dec: float,
    session: AsyncSession,
    name: str | None = None,
) -> ExtragalacticSource:
    """
    Create a new source in the database.

    Parameters
    ----------
    ra : float
        RA of source
    dec : float
        Dec of source
    name : str | None
        Name of source. Not required.
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source.to_model() : ExtragalacticSource
        Source that has been created
    """
    source = ExtragalacticSourceTable(ra=ra, dec=dec, name=name)

    async with session.begin():
        session.add(source)
        await session.commit()

    return source.to_model()


async def get_source(source_id: int, session: AsyncSession) -> ExtragalacticSource:
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
    source.to_mode() : ExtragalacticSource
        Source that has been created

    Raises
    ------
    ValueError
        If the source is not found.
    """
    source = await session.get(ExtragalacticSourceTable, source_id)

    if source is None:
        raise ValueError(f"Source with ID {source_id} not found")

    return source.to_model()


async def get_box(
    ra_min: float,
    ra_max: float,
    dec_min: float,
    dec_max: float,
    session: AsyncSession,
) -> list[ExtragalacticSource]:
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
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source_list : list[ExtragalacticSource]
        List of sources in box
    """
    sources = await session.execute(
        select(ExtragalacticSourceTable).where(
            ra_min <= ExtragalacticSourceTable.ra,
            ExtragalacticSourceTable.ra <= ra_max,
            dec_min <= ExtragalacticSourceTable.dec,
            ExtragalacticSourceTable.dec <= dec_max,
        )
    )

    source_list = [s.to_model() for s in sources.scalars()]

    return source_list


async def update_source(
    source_id: int,
    ra: float | None,
    dec: float | None,
    session: AsyncSession,
    name: str | None = None,
) -> ExtragalacticSource:
    """
    Update a source in the database.

    Parameters
    ----------
    ra : float | None
        RA of source
    dec : float | None
        Dec of source
    session : AsyncSession
        Asynchronous session to use
    name : str | None
        Name of source

    Returns
    -------
    source.to_mode() : ExtragalacticSource
        Source that has been updated

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        source = await session.get(ExtragalacticSourceTable, source_id)

        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        source.ra = ra if ra is not None else source.ra
        source.dec = dec if dec is not None else source.dec
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
        source = await session.get(ExtragalacticSourceTable, source_id)

        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        await session.delete(source)
        await session.commit()

    return
