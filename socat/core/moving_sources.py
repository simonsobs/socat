"""
Core functionality providing access to the moving source ephem database.
"""

from astropy.coordinates import ICRS
from astropy.units import Quantity
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from socat.database import (
    RegisteredMovingSource,
    RegisteredMovingSourceTable,
    SolarSystemObject,
)


async def create_ephem(
    session: AsyncSession,
    sso_id: int,
    MPC_id: int | None,
    name: str,
    time: int,
    position: ICRS,
    flux: Quantity | None = None,
) -> RegisteredMovingSource:
    """
    Create a new solar system ephemeris point in the database.

    Parameters
    ----------
    session : AsyncSession
        Session to use
    sso_id :int
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
    ephem.to_model() : RegisteredMovingSource
        Created ephem point
    """

    if flux is not None:
        flux = flux.to_value("mJy")
    ephem = RegisteredMovingSourceTable(
        sso_id=sso_id,
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


async def get_ephem(ephem_id: int, session: AsyncSession) -> RegisteredMovingSource:
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
    ephem.to_model() : RegisteredMovingSource
        Requested ephemeris point

    Raises
    ------
    ValueError
        If the ephemeris point is not found.
    """
    ephem = await session.get(RegisteredMovingSourceTable, ephem_id)

    if ephem is None:
        raise ValueError(f"Ephemeris point with ID {ephem_id} not found")

    return ephem.to_model()


async def get_ephem_points(
    source: SolarSystemObject, t_min: int, t_max: int, session: AsyncSession
) -> list[RegisteredMovingSource]:
    ephems = await session.execute(
        select(RegisteredMovingSourceTable).where(
            t_min <= RegisteredMovingSourceTable.time,
            RegisteredMovingSourceTable.time <= t_max,
            source.sso_id == RegisteredMovingSourceTable.sso_id,
        )
    )

    ephem_list = [e.to_model() for e in ephems.scalars()]

    return ephem_list


async def update_ephem(
    ephem_id: int,
    session: AsyncSession,
    sso_id: int | None,
    MPC_id: int | None,
    name: str | None,
    time: int | None,
    position: ICRS | None,
    flux: Quantity | None = None,
) -> RegisteredMovingSource:
    """
    Create a new solar system ephemeris point in the database.

    Parameters
    ----------
    ephem_id : int
        ID of ephem.
    session : AsyncSession
        Session to use
    sso_id :int | None
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
    ephem.to_model() : RegisteredMovingSource
        updated ephemeris point

    Raises
    ------
    ValueError
        If the ephemeris point is not found.
    """

    async with session.begin():
        ephem = await session.get(RegisteredMovingSourceTable, ephem_id)

        if ephem is None:
            raise ValueError(f"Ephem point with ID {ephem_id} not found.")

        ephem.sso_id = sso_id if sso_id is not None else ephem.sso_id
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
        ephem = await session.get(RegisteredMovingSourceTable, ephem_id)

        if ephem is None:
            raise ValueError(f"Source with ID {ephem_id} not found")

        await session.delete(ephem)
        await session.commit()

    return
