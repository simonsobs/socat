import numpy as np
import requests
from astropy.coordinates import Angle
from astropy.io import ascii
from astropy.table import Column
from astropy.time import Time
from astroquery.mpc import MPC


def get_ephem(ast: str, start: str, step: str = "1h", number: int = 1441):
    payload = MPC.get_ephemeris(
        ast, start=start, step="1h", number=1441, get_query_payload=True
    )

    # TBD if we want to use astroquery or just make our own payloads by hand.
    # Bellow is what a payload would look like, obviously some things would need to change
    _payload = {
        "ty": "e",
        "TextArea": "vesta",
        "uto": "0",
        "igd": "n",
        "ibh": "n",
        "fp": "y",
        "adir": "N",
        "tit": "",
        "bu": "",
        "c": "500",
        "d": "2025-01-01 000000",
        "i": "1",
        "u": "h",
        "l": 1441,
        "raty": "a",
        "s": "t",
        "m": "h",
    }

    response = requests.request(
        "POST", "https://cgi.minorplanetcenter.net/cgi-bin/mpeph2.cgi", data=payload
    )
    return parse_response(response)


def parse_response(
    response,
    unc_links: bool = False,
    ra_format: dict | None = None,
    dec_format: dict | None = None,
):
    """
    Function for parsing table returned by MPES. The code has been
    cribbed from astroquery: https://github.com/astropy/astroquery/blob/main/astroquery/mpc/core.py:_parse_result

    Parameters
    ----------
    unc_links : bool, optional
        Return columns with uncertainty map and offset links, if
        available.
    ra_format : dict, optional
        Format the RA column with
        `~astropy.coordinates.Angle.to_string` using these keyword
        arguments, e.g.,
        ``{'sep': ':', 'unit': 'hourangle', 'precision': 1}``.

    dec_format : dict, optional
        Format the Dec column with
        `~astropy.coordinates.Angle.to_string` using these keyword
        arguments, e.g., ``{'sep': ':', 'precision': 0}``.

    Raises
    ------
    RuntimeError
        If the query response found no data
    """
    content = response.content.decode()
    table_start = content.find("<pre>")
    if table_start == -1:
        raise RuntimeError("Error: querry returned no responses.")
    table_end = content.find("</pre>")
    text_table = content[table_start + 5 : table_end]
    SKY = "raty=a" in response.request.body
    HELIOCENTRIC = "raty=s" in response.request.body
    GEOCENTRIC = "raty=G" in response.request.body

    # find column headings
    if SKY:
        # slurp to newline after "h m s"
        # raise EmptyResponseError if no ephemeris lines are found in the query response
        try:
            i = text_table.index("\n", text_table.index("h m s")) + 1
        except ValueError:
            raise RuntimeError("Error: querry returned no responses.")
        columns = text_table[:i]
        data_start = columns.count("\n") - 1
    else:
        # slurp to newline after "JD_TT"
        # raise EmptyResponseError if no ephemeris lines are found in the query response
        try:
            i = text_table.index("\n", text_table.index("JD_TT")) + 1
        except ValueError:
            raise RuntimeError("Error: querry returned no responses.")
        columns = text_table[:i]
        data_start = columns.count("\n") - 1

    first_row = text_table.splitlines()[data_start + 1]

    if SKY:
        names = ("Date", "RA", "Dec", "Delta", "r", "Elongation", "Phase", "V")
        col_starts = (0, 18, 29, 39, 47, 56, 62, 69)
        col_ends = (17, 28, 38, 46, 55, 61, 68, 72)
        units = (None, None, None, "au", "au", "deg", "deg", "mag")

        if "s=t" in response.request.body:  # total motion
            names += ("Proper motion", "Direction")
            units += ("arcsec/h", "deg")
        elif "s=c" in response.request.body:  # coord Motion
            names += ("dRA", "dDec")
            units += ("arcsec/h", "arcsec/h")
        elif "s=s" in response.request.body:  # sky Motion
            names += ("dRA cos(Dec)", "dDec")
            units += ("arcsec/h", "arcsec/h")
        col_starts += (73, 82)
        col_ends += (81, 91)

        if "Moon" in columns:
            # table includes Alt, Az, Sun and Moon geometry
            names += (
                "Azimuth",
                "Altitude",
                "Sun altitude",
                "Moon phase",
                "Moon distance",
                "Moon altitude",
            )
            col_starts += tuple(
                col_ends[-1] + offset for offset in (1, 8, 13, 19, 26, 32)
            )
            col_ends += tuple(
                col_ends[-1] + offset for offset in (7, 12, 18, 25, 31, 36)
            )
            units += ("deg", "deg", "deg", None, "deg", "deg")
        if "Uncertainty" in columns:
            names += ("Uncertainty 3sig", "Unc. P.A.")
            col_starts += tuple(col_ends[-1] + offset for offset in (1, 10))
            col_ends += tuple(col_ends[-1] + offset for offset in (9, 15))
            units += ("arcsec", "deg")
        if ">Map</a>" in first_row and unc_links:
            names += ("Unc. map", "Unc. offsets")
            col_starts += (first_row.index(" / <a") + 3,)
            col_starts += (first_row.index(" / <a", col_starts[-1]) + 3,)
            # Unc. offsets is always last
            col_ends += (col_starts[-1] - 3, first_row.rindex("</a>") + 4)
            units += (None, None)
    elif HELIOCENTRIC:
        names = ("Object", "JD", "X", "Y", "Z", "X'", "Y'", "Z'")
        col_starts = (0, 12, 28, 45, 61, 77, 92, 108)
        col_ends = None
        units = (None, None, "au", "au", "au", "au/d", "au/d", "au/d")
    elif GEOCENTRIC:
        names = ("Object", "JD", "X", "Y", "Z")
        col_starts = (0, 12, 28, 45, 61)
        col_ends = None
        units = (None, None, "au", "au", "au")

    tab = ascii.read(
        text_table,
        format="fixed_width_no_header",
        names=names,
        col_starts=col_starts,
        col_ends=col_ends,
        data_start=data_start,
        fill_values=(("N/A", np.nan),),
        fast_reader=False,
    )

    for col, unit in zip(names, units):
        tab[col].unit = unit

    # Time for dates, Angle for RA and Dec; convert columns at user's request
    if SKY:
        # convert from MPES string to Time, MPES uses UT timescale
        tab["Date"] = Time(
            [
                f"{d[:4]}-{d[5:7]}-{d[8:10]} {d[11:13]}:{d[13:15]}:{d[15:17]}"
                for d in tab["Date"]
            ],
            scale="utc",
        )

        # convert from MPES string:
        ra = Angle(tab["RA"], unit="hourangle").to("deg")
        dec = Angle(tab["Dec"], unit="deg")

        # optionally convert back to a string
        if ra_format is not None:
            ra_unit = ra_format.get("unit", ra.unit)
            ra = ra.to_string(**ra_format)
        else:
            ra_unit = ra.unit

        if dec_format is not None:
            dec_unit = dec_format.get("unit", dec.unit)
            dec = dec.to_string(**dec_format)
        else:
            dec_unit = dec.unit

        # replace columns
        tab.remove_columns(("RA", "Dec"))
        tab.add_column(Column(ra, name="RA", unit=ra_unit), index=1)
        tab.add_column(Column(dec, name="Dec", unit=dec_unit), index=2)

        # convert proper motion columns
        for col in ("Proper motion", "dRA", "dRA cos(Dec)", "dDec"):
            if col in tab.colnames:
                tab[col].convert_unit_to("arcmin/h")
    else:
        # convert from MPES string to Time
        tab["JD"] = Time(tab["JD"], format="jd", scale="tt")
    return tab
