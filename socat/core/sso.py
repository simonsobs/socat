"""
Core functionality providing access to the sso database.
"""

from astropy.coordinates import ICRS
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import (
    SolarSystemObject,
    SolarSystemObjectTable,
    statements,
)


async def create_sso(
    name: str,
    MPC_id: int | None,
    session: AsyncSession,
) -> SolarSystemObject:
    """
    Create a new solar system source in the database.

    Parameters
    ----------
    name : str
        Name of solar system source
    MPC_id : int | None
        Minor Planet Center ID of source
    session : AsyncSession
        Asynchronous session to use

    """
    source = SolarSystemObjectTable(MPC_id=MPC_id, name=name)

    async with session.begin():
        session.add(source)
        await session.commit()

    return source.to_model()


async def get_sso(sso_id: int, session: AsyncSession) -> SolarSystemObject:
    """
    Get a solar system source from the database by id.

    Parameters
    ----------
    source_id :  int
        ID of source
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source.to_mode() : SolarSystemObject
        Requested solar system source

    Raises
    ------
    ValueError
        If the source is not found.
    """

    source = await session.get(SolarSystemObjectTable, sso_id)

    if source is None:
        raise ValueError(f"Solar system source with ID {sso_id} not found.")

    return source


async def get_sso_box(
    lower_left: ICRS,
    upper_right: ICRS,
    t_min: int,
    t_max: int,
    session: AsyncSession,
) -> list[SolarSystemObject]:
    """

    Equivelent of get_box for SSO objects. Only gets objects which have at least one ephem point
    inside the box between t_min and t_max

    Parameters
    ----------
    lower_left : ICRS
        Lower left corner of box
    upper_right : ICRS
        Upper right corner of box
    t_min : int
        Start time of box
    t_max : int
        End time of box

    Returns
    -------
    [s.to_model() for s in ssos.scalars()] : list[SolarSystemObject]
        List of solarsystem objects in boundint time-box
    """
    ssos: SolarSystemObjectTable = await session.execute(
        statements.get_time_box(
            lower_left=lower_left, upper_right=upper_right, t_min=t_min, t_max=t_max
        )
    )

    return [s.to_model() for s in ssos.scalars()]


async def get_sso_name(sso_name: str, session: AsyncSession) -> list[SolarSystemObject]:
    """
    Get a solar system source by name.

    Parameters
    ----------
    source_name :  str
        Name of source
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source_list : list[SolarSystemObject]
        Requested solar system sources

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        stmt = select(SolarSystemObjectTable).where(
            SolarSystemObjectTable.name == sso_name
        )

        service = await session.execute(stmt)

    source_list = [s.to_model() for s in service.scalars().all()]

    if len(source_list) == 0:
        raise ValueError(f"Service with name {sso_name} not found.")

    return source_list


async def get_sso_MPC_id(MPC_id: int, session: AsyncSession) -> list[SolarSystemObject]:
    """
    Get a solar system source by MPC ID.

    Parameters
    ----------
    MPC_id :  int
        Minor Planet Center ID of source
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source_list : list[SolarSystemObject]
        Requested solar system sources

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        stmt = select(SolarSystemObjectTable).where(
            SolarSystemObjectTable.MPC_id == MPC_id
        )

        service = await session.execute(stmt)

    source_list = [s.to_model() for s in service.scalars().all()]

    if len(source_list) == 0:
        raise ValueError(f"Service with MPC ID {MPC_id} not found.")

    return source_list


async def update_sso(
    sso_id: int,
    name: str | None,
    MPC_id: int | None,
    session: AsyncSession,
) -> SolarSystemObject:
    """
    Update a solar system source.
    Parameters
    ----------
    sso_id : int
        Internal SO source ID
    name : str
        Name of solar system source
    MPC_id : int | None
        Minor Planet Center ID of source
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source.to_mode() : SolarSystemObject
        Modified solar system source
    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        source = await session.get(SolarSystemObjectTable, sso_id)

        if source is None:
            raise ValueError(f"Solar system source with ID {sso_id} not found")

        source.name = name if name is not None else source.name
        source.MPC_id = MPC_id if MPC_id is not None else source.MPC_id

        await session.commit()

    return source.to_model()


async def delete_sso(sso_id: int, session: AsyncSession) -> None:
    """
    Delete a solar system source from the dattabase.

    Parameters
    ----------
    sso_id : int
        ID of source
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
        source = await session.get(SolarSystemObjectTable, sso_id)

        if source is None:
            raise ValueError(f"Source with ID {sso_id} not found")

        await session.delete(source)
        await session.commit()
