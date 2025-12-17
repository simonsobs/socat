"""
Core functionality providing access to the services database.
"""

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import (
    AstroqueryService,
    AstroqueryServiceTable,
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


async def get_all_services(session: AsyncSession) -> list[AstroqueryService]:
    """
    Return all astroquery services.

    Parameters
    ----------
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    service_list : list[AstroqueryService]
        List of all available astroquery services
    """

    async with session.begin():
        stmt = select(AstroqueryServiceTable)
        services = await session.execute(stmt)

    service_list = [s.to_model() for s in services.scalars().all()]

    return service_list


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
    service_list : list[AstroqueryService]
        Requested astroquery services

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
         If the service is not found.
    """

    async with session.begin():
        service = await session.get(AstroqueryServiceTable, service_id)

        if service is None:
            raise ValueError(f"Source with ID {service_id} not found")

        service.name = name if name is not None else service.name
        service.config = config if config is not None else service.config

        await session.commit()

    return service.to_model()


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
        If the service is not found.
    """

    async with session.begin():
        service = await session.get(AstroqueryServiceTable, service_id)

        if service is None:
            raise ValueError(f"Service with ID {service_id} not found")

        await session.delete(service)
        await session.commit()

    return
