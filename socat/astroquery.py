import warnings
from importlib import import_module

from astroquery.query import BaseVOQuery
from asyncer import asyncify


async def get_source_info(
    name: str, astroquery_service: str, requested_params: list[str] = ["ra", "dec"]
):
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
