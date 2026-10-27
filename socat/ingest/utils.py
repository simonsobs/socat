import re


def _parse_designation(designation: str) -> tuple[int | None, str | None]:
    """Split a designation like ``'1 Ceres'`` into MPC ID and name.

    Parameters
    ----------
    designation : str
        The designation string to parse.

    Returns
    -------
    tuple[int | None, str | None]
        The MPC ID and name, or `None` if not found.

    Notes
    -----
    Can also handle the MPC packed provisional designation format, e.g., ``'2011 KE'``. 
    In that case, the MPC ID will be `None` and the name will be the packed provisional designation.

    Examples
    --------
    '1 Ceres' -> (1, 'Ceres')

    '2011 KE' -> (None, '2011 KE')

    References
    ----------
    https://minorplanetcenter.net/mpcops/documentation/provisional-designation-definition/#packed_provid
    """

    designation = str(designation).strip()

    if not designation:
        return None, None

    # primary packed provisional designation
    # https://minorplanetcenter.net/mpcops/documentation/provisional-designation-definition/#packed_provid
    provisional = re.fullmatch(
        r"\d{4}\s+[A-HJ-Y][A-HJ-Z]\d*",
        designation,
    )

    if provisional:
        return None, designation

    # numbered asteroid, optionally followed by a name
    numbered = re.fullmatch(
        r"\(?(\d+)\)?(?:\s+(.+))?",
        designation,
    )

    if numbered:
        mpc_id = int(numbered.group(1))
        name = numbered.group(2) or str(mpc_id)
        return mpc_id, name

    # Name or other designation
    return None, designation