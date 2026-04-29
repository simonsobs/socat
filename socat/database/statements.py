"""
Contains functions that generate SQL(Alchemy) and associated statements for the
database that are anything more than simple cases.
"""

from importlib import import_module

import astropy.units as u
from astropy.coordinates import ICRS
from astropy.units import Quantity
from astroquery.query import BaseVOQuery
from sqlmodel import select, update

from socat.database.services import AstroqueryServiceTable
from socat.database.sources import (
    RegisteredFixedSourceTable,
    RegisteredMovingSourceTable,
    SolarSystemObjectTable,
)


def create_name(
    name: str, astroquery_service: str
) -> tuple[ICRS, str, Quantity | None]:
    """
    Create a name in the database by querying an astroquery service.

    Parameters
    ----------
    name : str
        Name of source to create
    astroquery_service : str
        Name of the astroquery service to use

    Returns
    -------
    tuple[ICRS, str, Quantity | None]:
        Tuple containing the position, name, and flux of the source.

    Raises
    ------
    ValueError
        If no results are found for the given name in the specified astroquery service.
    """
    service: BaseVOQuery = getattr(
        import_module(f"astroquery.{astroquery_service.lower()}"),
        astroquery_service,
    )

    requested_params = ["ra", "dec", "flux"]

    result_table = service.query_object(name)
    result_table["ra"].convert_unit_to("deg")
    result_table["dec"].convert_unit_to("deg")
    if "flux" in result_table.columns:
        result_table["flux"].convert_unit_to("mJy")  # pragma: no cover

    if len(result_table) == 0:
        raise ValueError(f"No results found for {name} in {astroquery_service}.")

    result_dict = {param: None for param in requested_params}
    for param in requested_params:
        try:
            result_dict[param] = result_table[param].value.data[0]
        except KeyError:  # pragma: no cover
            continue

    position = ICRS(
        ra=result_dict["ra"] * u.deg,
        dec=result_dict["dec"] * u.deg,
    )
    flux = result_dict.get("flux", None)
    if flux is not None:
        flux *= u.mJy

    return position, name, flux


def get_box(lower_left: ICRS, upper_right: ICRS) -> select:
    """
    Get the box coordinates for a given lower left and upper right corner.

    Parameters
    ----------
    lower_left : ICRS
        Lower left corner of box
    upper_right : ICRS
        Upper right corner of box

    Returns
    -------
    select:
        Database statement.

    """
    return select(RegisteredFixedSourceTable).where(
        float(lower_left.ra.to_value("deg")) <= RegisteredFixedSourceTable.ra_deg,
        RegisteredFixedSourceTable.ra_deg <= float(upper_right.ra.to_value("deg")),
        float(lower_left.dec.to_value("deg")) <= RegisteredFixedSourceTable.dec_deg,
        RegisteredFixedSourceTable.dec_deg <= float(upper_right.dec.to_value("deg")),
    )


def get_time_box(lower_left: ICRS, upper_right: ICRS, t_min: int, t_max: int) -> select:
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
    select:
        Database statement.
    """
    return (
        select(SolarSystemObjectTable)
        .outerjoin(
            RegisteredMovingSourceTable,
            RegisteredMovingSourceTable.sso_id == SolarSystemObjectTable.sso_id,
        )
        .where(
            t_min.unix <= RegisteredMovingSourceTable.time,
            RegisteredMovingSourceTable.time <= t_max.unix,
            float(lower_left.ra.to_value("deg")) <= RegisteredMovingSourceTable.ra_deg,
            RegisteredMovingSourceTable.ra_deg <= float(upper_right.ra.to_value("deg")),
            float(lower_left.dec.to_value("deg"))
            <= RegisteredMovingSourceTable.dec_deg,
            RegisteredMovingSourceTable.dec_deg
            <= float(upper_right.dec.to_value("deg")),
        )
        .distinct()
    )


def get_forced_photometry_sources(minimum_flux: Quantity) -> select:
    """
    Get sources for which to perform forced photometry, i.e. sources with flux
    above a certain threshold.

    Parameters
    ----------
    minimum_flux : Quantity
        Minimum flux of sources to return

    Returns
    -------
    select:
        Database statement.
    """
    return select(RegisteredFixedSourceTable).where(
        RegisteredFixedSourceTable.flux_mJy >= minimum_flux.to_value("mJy")
    )


def update_source(
    source_id: int,
    position: ICRS | None = None,
    flux: Quantity | None = None,
    name: str | None = None,
) -> update:
    """
    Generate an update statement for a source.

    Parameters
    ----------
    source_id : int
        ID of source to update
    position : ICRS | None
        Position of source in ICRS coordinates. Optional.
    flux : Quantity | None
        Flux of source. Optional.
    name : str | None
        Name of source. Optional.

    Returns
    -------
    update:
        Database statement.

    Raises
    ------
    ValueError
        If no fields are provided to update.
    """
    stmt = update(RegisteredFixedSourceTable).where(
        RegisteredFixedSourceTable.source_id == source_id
    )

    values = {
        k: v
        for k, v in {
            "ra_deg": position.ra.to_value("deg") if position is not None else None,
            "dec_deg": position.dec.to_value("deg") if position is not None else None,
            "flux_mJy": flux.to_value("mJy") if flux is not None else None,
            "name": name,
        }.items()
        if v is not None
    }

    if values:
        return stmt.values(**values)
    else:
        raise ValueError("At least one field must be provided to update the source")


def update_service(
    service_id: int,
    name: str | None,
    config: dict | None,
) -> update:
    """
    Generate an update statement for an astroquery service.

    Parameters
    ----------
    service_id : int
        The ID of the astroquery service to be updated.
    name: str | None
        The new name of the service.
    config: dict | None
        The new config for the service.

    Returns
    -------
    update:
        Database statement

    Raises
    ------
    ValueError
        If no fields are provided to update.
    """
    stmt = update(AstroqueryServiceTable).where(
        AstroqueryServiceTable.service_id == service_id
    )

    values = {
        k: v
        for k, v in {
            "name": name,
            "config": config,
        }.items()
        if v is not None
    }

    if values:
        return stmt.values(**values)
    else:
        raise ValueError("At least one field must be provided to update the service")


def update_sso(
    sso_id: int,
    name: str | None,
    MPC_id: int | None,
) -> update:
    """
    Generate an update statement for a sso source.

    Parameters
    ----------
    sso_id: int
        The ID of the sso source to be updated.
    name: str
        The new name to use.
    MPC_id: int
        The new MPC ID to use.

    Returns
    -------
    update:
        Database statement.

    Raises
    ------
    ValueError
        If no fields are provided to update.
    """
    stmt = update(SolarSystemObjectTable).where(SolarSystemObjectTable.sso_id == sso_id)

    values = {
        k: v
        for k, v in {
            "name": name,
            "MPC_id": MPC_id,
        }.items()
        if v is not None
    }

    if values:
        return stmt.values(**values)
    else:
        raise ValueError(
            "At least one field must be provided to update the solar system object"
        )


def update_ephem(
    ephem_id: int,
    sso_id: int | None,
    MPC_id: int | None,
    name: str | None,
    time: int | None,
    position: ICRS | None,
    flux: Quantity | None,
) -> update:
    """
    Generate an update statement for an ephemeris point.

    Parameters
    ----------
    ephem_id : int
        The ID of the ephemeris point to be updated.
    sso_id : int | None
        The new SSO ID for the ephemeris point.
    MPC_id : int | None
        The new MPC ID for the ephemeris point.
    name : str | None
        The new name for the ephemeris point.
    time : int | None
        The new time for the ephemeris point.
    position : ICRS | None
        The new position for the ephemeris point.
    flux : Quantity | None
        The new flux for the ephemeris point.

    Returns
    -------
    update:
        Database statement.

    Raises
    ------
    ValueError
        If no fields are provided to update.
    """
    stmt = update(RegisteredMovingSourceTable).where(
        RegisteredMovingSourceTable.ephem_id == ephem_id
    )

    values = {
        k: v
        for k, v in {
            "sso_id": sso_id,
            "MPC_id": MPC_id,
            "name": name,
            "time": time,
            "ra_deg": position.ra.to_value("deg") if position is not None else None,
            "dec_deg": position.dec.to_value("deg") if position is not None else None,
            "flux_mJy": flux.to_value("mJy") if flux is not None else None,
        }.items()
        if v is not None
    }

    if values:
        return stmt.values(**values)
    else:
        raise ValueError(
            "At least one field must be provided to update the ephemeris point"
        )
