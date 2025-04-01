import warnings
from importlib import import_module


def get_source_info(
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

    service = getattr(
        import_module(f"astroquery.{astroquery_service.lower()}"),
        astroquery_service,
    )
    result_table = service.query_object(name)

    if len(result_table) > 1:
        warnings.warn("More than one source resolved, returning first")
    if len(result_table) == 0:
        raise RuntimeError(
            "Error: no source with name {name} resolved in {astroquery_service}"
        )

    result_dict = {param: None for param in requested_params}
    for param in requested_params:
        try:
            result_dict[param] = result_table[param].value.data[
                0
            ]  # TODO: currently only take first match.
        # Maybe should warn if more than one match?
        except KeyError:
            continue

    return result_dict
