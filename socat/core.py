"""
Core functionality providing access to the database.
"""

from typing import Any

from astropy.coordinates import ICRS
from astropy.units import Quantity
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import (
    AstroqueryService,
    AstroqueryServiceTable,
    ExtragalacticSource,
    ExtragalacticSourceTable,
    SolarSystemEphem,
    SolarSystemEphemTable,
    SolarSystemSource,
    SolarSystemTable,
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


async def create_source(
    position: ICRS,
    session: AsyncSession,
    name: str | None = None,
    flux: Quantity | None = None,
) -> ExtragalacticSource:
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
    source.to_model() : ExtragalacticSource
        Source that has been created
    """
    if flux is not None:
        flux = flux.to_value("mJy")
    source = ExtragalacticSourceTable(
        ra_deg=position.ra.to_value("deg"),
        dec_deg=position.dec.to_value("deg"),
        name=name,
        flux_mJy=flux,
    )

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
    lower_left: ICRS,
    upper_right: ICRS,
    session: AsyncSession,
) -> list[ExtragalacticSource]:
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
    source_list : list[ExtragalacticSource]
        List of sources in box
    """
    # Unclear why float casts are needed but
    # comparisons raise TypeError: Boolean value of this clause is not defined
    # without the cast.
    sources = await session.execute(
        select(ExtragalacticSourceTable).where(
            float(lower_left.ra.to_value("deg")) <= ExtragalacticSourceTable.ra_deg,
            ExtragalacticSourceTable.ra_deg <= float(upper_right.ra.to_value("deg")),
            float(lower_left.dec.to_value("deg")) <= ExtragalacticSourceTable.dec_deg,
            ExtragalacticSourceTable.dec_deg <= float(upper_right.dec.to_value("deg")),
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
) -> ExtragalacticSource:
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
        source = await session.get(ExtragalacticSourceTable, source_id)

        if source is None:
            raise ValueError(f"Source with ID {source_id} not found")

        await session.delete(source)
        await session.commit()

    return


async def create_solarsystem_source(
    name: str,
    MPC_id: int | None,
    session: AsyncSession,
) -> SolarSystemSource:
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
    source = SolarSystemTable(MPC_id=MPC_id, name=name)

    async with session.begin():
        session.add(source)
        await session.commit()

    return source.to_model()


async def get_solarsystem_source(
    solar_id: int, session: AsyncSession
) -> SolarSystemSource:
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
    source.to_mode() : SolarSystemSource
        Requested solar system source

    Raises
    ------
    ValueError
        If the source is not found.
    """

    source = await session.get(SolarSystemTable, solar_id)

    if source is None:
        raise ValueError(f"Solar system source with ID {solar_id} not found.")

    return source


async def get_solarsystem_source_name(
    source_name: str, session: AsyncSession
) -> list[SolarSystemSource]:
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
    source_list : list[SolarSystemSource]
        Requested solar system sources

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        stmt = select(SolarSystemTable).where(SolarSystemTable.name == source_name)

        service = await session.execute(stmt)

    source_list = [s.to_model() for s in service.scalars().all()]

    if len(source_list) == 0:
        raise ValueError(f"Service with name {source_name} not found.")

    return source_list


async def get_solarsystem_source_MPC_id(
    MPC_id: int, session: AsyncSession
) -> list[SolarSystemSource]:
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
    source_list : list[SolarSystemSource]
        Requested solar system sources

    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        stmt = select(SolarSystemTable).where(SolarSystemTable.MPC_id == MPC_id)

        service = await session.execute(stmt)

    source_list = [s.to_model() for s in service.scalars().all()]

    if len(source_list) == 0:
        raise ValueError(f"Service with MPC ID {MPC_id} not found.")

    return source_list


async def update_solarsystem_source(
    solar_id: int,
    name: str | None,
    MPC_id: int | None,
    session: AsyncSession,
) -> SolarSystemSource:
    """
    Update a solar system source.
    Parameters
    ----------
    solar_id : int
        Internal SO source ID
    name : str
        Name of solar system source
    MPC_id : int | None
        Minor Planet Center ID of source
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    source.to_mode() : SolarSystemSource
        Modified solar system source
    Raises
    ------
    ValueError
        If the source is not found.
    """

    async with session.begin():
        source = await session.get(SolarSystemTable, solar_id)

        if source is None:
            raise ValueError(f"Solar system source with ID {solar_id} not found")

        source.name = name if name is not None else source.name
        source.MPC_id = MPC_id if MPC_id is not None else source.MPC_id

        await session.commit()

    return source.to_model()


async def delete_solarsystem_source(solar_id: int, session: AsyncSession) -> None:
    """
    Delete a solar system source from the dattabase.

    Parameters
    ----------
    solar_id : int
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
        source = await session.get(SolarSystemTable, solar_id)

        if source is None:
            raise ValueError(f"Source with ID {solar_id} not found")

        await session.delete(source)
        await session.commit()

    return


async def create_ephem(
    session: AsyncSession,
    obj_id: int,
    MPC_id: int | None,
    name: str,
    time: int,
    position: ICRS,
    flux: Quantity | None = None,
) -> SolarSystemEphem:
    """
    Create a new solar system ephemeris point in the database.

    Parameters
    ----------
    session : AsyncSession
        Session to use
    obj_id :int
        Internal SO ID of source
    MPC_id : int | None
        MPC ID of source
    name : str
        Name of source
    time : int
        Time of source ephem, unix time
    position : AstroPydanticICRS
        Position of source at time in ICRS coordinates
    flux : Quantity  | None
        Flux of source at ephem point in mJy

    Returns
    -------
    ephem.to_model() : SolarSystemEphem
        Created ephem point
    """

    if flux is not None:
        flux = flux.to_value("mJy")
    ephem = SolarSystemEphemTable(
        obj_id=obj_id,
        MPC_id=MPC_id,
        name=name,
        time=time,
        ra_deg=position.ra.to_value("deg"),
        dec_deg=position.dec.to_value("deg"),
        flux_mJy=flux,
    )

    async with session.begin():
        session.add(ephem)
        await session.commit()

    return ephem.to_model()


async def get_ephem(ephem_id: int, session: AsyncSession) -> SolarSystemEphem:
    """
    Get a solar system ephemeris point from the database.

    Parameters
    ----------
    ephem_id : int
        ID of solar system ephemeris point
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    ephem.to_model() : SolarSystemEphem
        Requested ephemeris point

    Raises
    ------
    ValueError
        If the ephemeris point is not found.
    """
    ephem = await session.get(SolarSystemEphemTable, ephem_id)

    if ephem is None:
        raise ValueError(f"Ephemeris point with ID {ephem_id} not found")

    return ephem.to_model()


async def update_ephem(
    ephem_id: int,
    session: AsyncSession,
    obj_id: int | None,
    MPC_id: int | None,
    name: str | None,
    time: int | None,
    position: ICRS | None,
    flux: Quantity | None = None,
) -> SolarSystemEphem:
    """
    Create a new solar system ephemeris point in the database.

    Parameters
    ----------
    ephem_id : int
        ID of ephem.
    session : AsyncSession
        Session to use
    obj_id :int | None
        Internal SO ID of source
    MPC_id : int | None
        MPC ID of source
    name : str | None
        Name of source
    time : int | None
        Time of source ephem, unix time
    position : AstroPydanticICRS | None
        Position of source in ICRS coordinates
    flux : Quantity  | None
        Flux of source at ephem point in mJy

    Returns
    -------
    ephem.to_model() : SolarSystemEphem
        updated ephemeris point

    Raises
    ------
    ValueError
        If the ephemeris point is not found.
    """

    async with session.begin():
        ephem = await session.get(SolarSystemEphemTable, ephem_id)

        if ephem is None:
            raise ValueError(f"Ephem point with ID {ephem_id} not found.")

        ephem.obj_id = obj_id if obj_id is not None else ephem.obj_id
        ephem.MPC_id = MPC_id if MPC_id is not None else ephem.MPC_id
        ephem.name = name if name is not None else ephem.name
        ephem.time = time if time is not None else ephem.time
        ephem.ra_deg = (
            position.ra.to_value("deg") if position.ra is not None else ephem.ra_deg
        )
        ephem.dec_deg = (
            position.dec.to_value("deg") if position.dec is not None else ephem.dec_deg
        )
        ephem.flux_mJy = flux.to_value("mJy") if flux is not None else ephem.flux_mJy

        await session.commit()

    return ephem.to_model()


async def delete_ephem(ephem_id: int, session: AsyncSession) -> None:
    """
    Delete a solar system ephemeris point from the dattabase.

    Parameters
    ----------
    ephem_id : int
        ID of sephem pointource
    session : AsyncSession
        Asynchronous session to use

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the ephem point is not found.
    """

    async with session.begin():
        ephem = await session.get(SolarSystemEphemTable, ephem_id)

        if ephem is None:
            raise ValueError(f"Source with ID {ephem_id} not found")

        await session.delete(ephem)
        await session.commit()

    return
