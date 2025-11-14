import warnings
from importlib import import_module

import numpy as np
from astropy.coordinates import ICRS
from astropy.units import Quantity
from astroquery.query import BaseVOQuery
from asyncer import asyncify
from pydantic import BaseModel

from .core import AstroqueryService


class AstroqueryReturn(BaseModel):
    """
    Pydantic Model which contains information about a source returned by astroquery.

    Attributes
    ----------
    name : str
        Name of the source
    ra : float
        RA of source
    dec : float
        Dec of the source
    flux : float | None
        Flux of source in Jy. Optional.
    provider : str
        Service which resolved this source
    distance : float
        Distance of source to center of query
    """

    name: str
    ra: float
    dec: float
    flux: float | None
    provider: str
    distance: float


async def get_source_info(
    name: str, astroquery_service: str, requested_params: list[str] = ["ra", "dec"]
) -> dict:
    """
    Get source info by name using astroquery

    Parameters
    ----------
    name : str
        Name of source to resolve
    astroquery_service : str
        Name of astroquery service to use to resolve name
    requested_params : list[str], Default: ["ra", "dec"]
        Parameters of source to get.
        Must match astrotable column names.

    Returns
    -------
    source_info : dict
        Dict with keys matching requested_params and values from the requested service

    Raises
    ------
    RuntimeError
        If no source found in astroquery_service
    """

    service: BaseVOQuery = getattr(
        import_module(f"astroquery.{astroquery_service.lower()}"),
        astroquery_service,
    )

    result_table = await asyncify(service.query_object)(name)
    # I guess it's like marginally more efficient to only do these
    # conversions if the params are requested but that also opens
    # the door to bugs where the conversion doesn't happen for
    # some reason.
    result_table["ra"].convert_unit_to("deg")
    result_table["dec"].convert_unit_to("deg")
    if "flux" in result_table.keys():
        result_table["flux"].convert_unit_to("mJy")  # pragma: no cover
    if len(result_table) > 1:
        warnings.warn(
            "More than one source resolved, returning first"
        )  # pragma: no cover

    result_dict = {param: None for param in requested_params}
    if len(result_table) == 0:
        return result_dict
    for param in requested_params:
        try:
            result_dict[param] = result_table[param].value.data[
                0
            ]  # TODO: currently only take first match.
            if param == "ra" and result_dict[param] > 180:
                result_dict[param] = -1 * (
                    360 - result_dict[param]
                )  # Astroquery uses a 0-360 standard vs -180 to 180
        # Maybe should warn if more than one match?
        except KeyError:  # pragma: no cover
            continue

    return result_dict


async def cone_search(
    position: ICRS,
    service_list: list[AstroqueryService],
    radius: Quantity,
) -> list[AstroqueryReturn]:
    """
    Function which uses astroquery to perform a cone search across.
    The cone is centered on ra/dec with radius radius, and searches all services in service_list.
    If service_list isn't specified, then searches all available services.

    Parameters
    ----------
    position : ICRS
        Position of center of cone
    radius : Quantity
        Radius of cone search
    service_list : list[str] | None, Default: None
        Services to check. If None, all available services are searched

    Returns
    -------
    source_list : list[AstroqueryReturn]
        List of AstroqueryReturn objects specifying name, ra, dec, provider, and distance from center of source
    """

    source_list = []

    for service in service_list:
        cur_service: BaseVOQuery = getattr(
            import_module(f"astroquery.{service.name.lower()}"),
            service.name,
        )
        result_table = await asyncify(cur_service.query_region)(
            position,
            radius=radius,
        )
        result_table["ra"].convert_unit_to("deg")
        result_table["dec"].convert_unit_to("deg")
        if "flux" in result_table.keys():
            result_table["flux"].convert_unit_to("mJy")  # pragma: no cover
        for i in range(len(result_table)):
            name = result_table[service.config["name_col"]].value.data[i]
            cur_ra = result_table[service.config["ra_col"]].value.data[i]
            cur_dec = result_table[service.config["dec_col"]].value.data[i]
            cur_flux = (
                result_table[service.config["flux_col"]].value.data[i]
                if "flux_col" in service.config
                else None
            )
            source_list.append(
                AstroqueryReturn(
                    name=name,
                    ra=float(cur_ra),
                    dec=float(cur_dec),
                    flux=float(cur_flux) if cur_flux is not None else None,
                    provider=str(service.name),
                    distance=np.sqrt(
                        (position.ra.to_value("deg") - cur_ra) ** 2
                        + (position.dec.to_value("deg") - cur_dec) ** 2
                    ),  ##TODO: use astropy separation and skycoords
                )
            )

    return source_list
