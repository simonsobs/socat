import warnings
from importlib import import_module

import astropy.units as u
import numpy as np
from astropy import coordinates
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
    provider : str
        Service which resolved this source
    distance : float
        Distance of source to center of query
    """

    name: str
    ra: float
    dec: float
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
    ra: float, dec: float, service_list: list[AstroqueryService], radius: float = 1.5
) -> list[AstroqueryReturn]:
    """
    Function which uses astroquery to perform a cone search across.
    The cone is centered on ra/dec with radius radius, and searches all services in service_list.
    If service_list isn't specified, then searches all available services.

    Parameters
    ----------
    ra : float
        Ra of cone center, deg, -180 to 180 def
    dec : float
        Dec of cone center, deg
    radius : float, Default: 1.5
        Radius of cone search, arcmin
    service_list : list[str] | None, Default: None
        Services to check. If None, all available services are searched

    Returns
    -------
    source_list : list[AstroqueryReturn]
        List of AstroqueryReturn objects specifying name, ra, dec, provider, and distance from center of source
    """

    source_list = []
    center = coordinates.SkyCoord(ra * u.deg, dec * u.deg)

    for service in service_list:
        cur_service: BaseVOQuery = getattr(
            import_module(f"astroquery.{service.name.lower()}"),
            service.name,
        )
        result_table = await asyncify(cur_service.query_region)(
            center, radius=radius * u.arcmin
        )
        for i in range(len(result_table)):
            name = result_table[service.config["name_col"]].value.data[i]
            cur_ra = result_table[service.config["ra_col"]].value.data[i]
            cur_dec = result_table[service.config["dec_col"]].value.data[i]
            source_list.append(
                AstroqueryReturn(
                    name=name,
                    ra=float(cur_ra),
                    dec=float(cur_dec),
                    provider=str(service.name),
                    distance=np.sqrt((ra - cur_ra) ** 2 + (dec - cur_dec) ** 2),
                )
            )

    return source_list
