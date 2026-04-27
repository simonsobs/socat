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
from socat.database.sources import RegisteredFixedSourceTable


def create_name(
    name: str, astroquery_service: str
) -> tuple[ICRS, str, Quantity | None]:
    """"""
    service: BaseVOQuery = getattr(
        import_module(f"astroquery.{astroquery_service.lower()}"),
        astroquery_service,
    )

    requested_params = ["ra", "dec"]

    result_table = service.query_object(name)
    result_table["ra"].convert_unit_to("deg")
    result_table["dec"].convert_unit_to("deg")
    if "flux" in result_table.columns:
        result_table["flux"].convert_unit_to("mJy")  # pragma: no cover
    result_dict = {param: None for param in requested_params}
    if len(result_table) == 0:
        return None
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
        Databse statement.

    """
    return select(RegisteredFixedSourceTable).where(
        float(lower_left.ra.to_value("deg")) <= RegisteredFixedSourceTable.ra_deg,
        RegisteredFixedSourceTable.ra_deg <= float(upper_right.ra.to_value("deg")),
        float(lower_left.dec.to_value("deg")) <= RegisteredFixedSourceTable.dec_deg,
        RegisteredFixedSourceTable.dec_deg <= float(upper_right.dec.to_value("deg")),
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
    """
    stmt = update(RegisteredFixedSourceTable).where(
        RegisteredFixedSourceTable.source_id == source_id
    )

    if position is not None:
        stmt = stmt.values(
            ra_deg=position.ra.to_value("deg"), dec_deg=position.dec.to_value("deg")
        )

    if flux is not None:
        stmt = stmt.values(flux_mJy=flux.to_value("mJy"))

    if name is not None:
        stmt = stmt.values(name=name)

    return stmt


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
    """
    stmt = update(AstroqueryServiceTable).where(
        AstroqueryServiceTable.service_id == service_id
    )

    if name is not None:
        stmt = stmt.values(name=name)

    if config is not None:
        stmt = stmt.values(config=config)

    return stmt
