"""
Contains functions that generate SQL(Alchemy) and associated statements for the
database that are anything more than simple cases.
"""

from importlib import import_module

import astropy.units as u
from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity
from astroquery.query import BaseVOQuery
from sqlmodel import select, union_all, update

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
    if flux is not None:  # pragma: no cover
        flux *= u.mJy

    return position, name, flux


def get_box_fixed(lower_left: ICRS, upper_right: ICRS) -> select:
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
    if lower_left.ra > upper_right.ra:
        box1 = select(RegisteredFixedSourceTable).where(
            float(lower_left.ra.to_value("deg")) <= RegisteredFixedSourceTable.ra_deg,
            RegisteredFixedSourceTable.ra_deg <= 360.0,
            float(lower_left.dec.to_value("deg")) <= RegisteredFixedSourceTable.dec_deg,
            RegisteredFixedSourceTable.dec_deg
            <= float(upper_right.dec.to_value("deg")),
        )
        box2 = select(RegisteredFixedSourceTable).where(
            0.0 <= RegisteredFixedSourceTable.ra_deg,
            RegisteredFixedSourceTable.ra_deg <= float(upper_right.ra.to_value("deg")),
            float(lower_left.dec.to_value("deg")) <= RegisteredFixedSourceTable.dec_deg,
            RegisteredFixedSourceTable.dec_deg
            <= float(upper_right.dec.to_value("deg")),
        )
        union_stmt = union_all(box1, box2)
        return select(RegisteredFixedSourceTable).from_statement(union_stmt)
    else:
        return select(RegisteredFixedSourceTable).where(
            float(lower_left.ra.to_value("deg")) <= RegisteredFixedSourceTable.ra_deg,
            RegisteredFixedSourceTable.ra_deg <= float(upper_right.ra.to_value("deg")),
            float(lower_left.dec.to_value("deg")) <= RegisteredFixedSourceTable.dec_deg,
            RegisteredFixedSourceTable.dec_deg
            <= float(upper_right.dec.to_value("deg")),
        )


def get_box_sso(
    lower_left: ICRS, upper_right: ICRS, t_min: Time, t_max: Time
) -> select:
    """
    Equivelent of get_box for SSO objects. Only gets objects which have at least one ephem point
    inside the box between t_min and t_max

    Parameters
    ----------
    lower_left : ICRS
        Lower left corner of box
    upper_right : ICRS
        Upper right corner of box
    t_min : AstroPydanticTime
        Start time of box
    t_max : AstroPydanticTime
        End time of box

    Returns
    -------
    select:
        Database statement.
    """
    if lower_left.ra > upper_right.ra:
        box1 = (
            select(SolarSystemObjectTable)
            .outerjoin(
                RegisteredMovingSourceTable,
                RegisteredMovingSourceTable.sso_id == SolarSystemObjectTable.sso_id,
            )
            .where(
                t_min.datetime <= RegisteredMovingSourceTable.time,
                RegisteredMovingSourceTable.time <= t_max.datetime,
                float(lower_left.ra.to_value("deg"))
                <= RegisteredMovingSourceTable.ra_deg,
                RegisteredMovingSourceTable.ra_deg <= 360.0,
                float(lower_left.dec.to_value("deg"))
                <= RegisteredMovingSourceTable.dec_deg,
                RegisteredMovingSourceTable.dec_deg
                <= float(upper_right.dec.to_value("deg")),
            )
        )
        box2 = (
            select(SolarSystemObjectTable)
            .outerjoin(
                RegisteredMovingSourceTable,
                RegisteredMovingSourceTable.sso_id == SolarSystemObjectTable.sso_id,
            )
            .where(
                t_min.datetime <= RegisteredMovingSourceTable.time,
                RegisteredMovingSourceTable.time <= t_max.datetime,
                0.0 <= RegisteredMovingSourceTable.ra_deg,
                RegisteredMovingSourceTable.ra_deg
                <= float(upper_right.ra.to_value("deg")),
                float(lower_left.dec.to_value("deg"))
                <= RegisteredMovingSourceTable.dec_deg,
                RegisteredMovingSourceTable.dec_deg
                <= float(upper_right.dec.to_value("deg")),
            )
        )
        union_stmt = union_all(box1, box2)
        return select(SolarSystemObjectTable).from_statement(union_stmt).distinct()
    else:
        return (
            select(SolarSystemObjectTable)
            .outerjoin(
                RegisteredMovingSourceTable,
                RegisteredMovingSourceTable.sso_id == SolarSystemObjectTable.sso_id,
            )
            .where(
                t_min.datetime <= RegisteredMovingSourceTable.time,
                RegisteredMovingSourceTable.time <= t_max.datetime,
                float(lower_left.ra.to_value("deg"))
                <= RegisteredMovingSourceTable.ra_deg,
                RegisteredMovingSourceTable.ra_deg
                <= float(upper_right.ra.to_value("deg")),
                float(lower_left.dec.to_value("deg"))
                <= RegisteredMovingSourceTable.dec_deg,
                RegisteredMovingSourceTable.dec_deg
                <= float(upper_right.dec.to_value("deg")),
            )
            .distinct()
        )


def get_monitored_fixed_sources() -> select:
    """
    Get all fixed sources with monitored=True.

    Returns
    -------
    select:
        Database statement.
    """
    return select(RegisteredFixedSourceTable).where(
        RegisteredFixedSourceTable.monitored == True  # noqa: E712
    )


def get_all_monitored_ssos() -> select:
    """
    Get all solar system objects with monitored=True.

    Returns
    -------
    select:
        Database statement.
    """
    return select(SolarSystemObjectTable).where(
        SolarSystemObjectTable.monitored == True  # noqa: E712
    )


def get_monitored_ssos(t_min: Time, t_max: Time) -> select:
    """
    Get solar system objects with monitored=True that have at least one
    ephemeris point in [t_min, t_max].

    Parameters
    ----------
    t_min : Time
        Start of the time range.
    t_max : Time
        End of the time range.

    Returns
    -------
    select:
        Database statement.
    """
    return (
        select(SolarSystemObjectTable)
        .join(
            RegisteredMovingSourceTable,
            RegisteredMovingSourceTable.sso_id == SolarSystemObjectTable.sso_id,
        )
        .where(
            SolarSystemObjectTable.monitored == True,  # noqa: E712
            t_min.datetime <= RegisteredMovingSourceTable.time,
            RegisteredMovingSourceTable.time <= t_max.datetime,
        )
        .distinct()
    )


def update_source(
    source_id: int,
    position: ICRS | None = None,
    flux: Quantity | None = None,
    name: str | None = None,
    monitored: bool | None = None,
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
    monitored : bool | None
        Whether this source is monitored. Optional.

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
            "monitored": monitored,
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
    time: Time | None,
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
    time : Time | None
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
            "time": time.datetime if time is not None else None,
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


def get_ephem_points(sso_id: int, t_min: Time, t_max: Time) -> select:
    """
    Generate a select statement to get ephemeris points for a solar system object between t_min and t_max.

    Parameters
    ----------
    sso_id : int
        The ID of the solar system object to get ephemeris points for.
    t_min : Time
        The minimum time for ephemeris points to return.
    t_max : Time
        The maximum time for ephemeris points to return.

    Returns
    -------
    select:
        Database statement to get ephemeris points for the specified solar system object between t_min and t_max.

    Raises
    ------
    ValueError
        If t_min is greater than t_max.
    """

    if t_min > t_max:
        raise ValueError("t_min must be less than or equal to t_max")

    return select(RegisteredMovingSourceTable).where(
        t_min.datetime <= RegisteredMovingSourceTable.time,
        RegisteredMovingSourceTable.time <= t_max.datetime,
        sso_id == RegisteredMovingSourceTable.sso_id,
    )
