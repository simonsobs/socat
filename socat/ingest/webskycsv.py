"""
Ingest websky csv files into an instance of SOCat.
"""

import pickle
import re
from pathlib import Path

import pandas as pd
from astropy import units as u
from astropy.coordinates import ICRS, SkyCoord

from socat.client.core import ClientBase
from socat.client.mock import Client as MockClient


def ingest_csv_file(
    client: ClientBase,
    filename: Path,
    flux_lower_limit: u.Quantity = 0 * u.Jy,
) -> int:
    """
    Ingest a csv file into the provided SOCat client.

    Parameters
    ----------
    client: ClientBase
        The SOCat client to use.
    filename: Path
        Path to the websky-compatible CSV point source file to load.

    Returns
    -------
    number_of_sources: int
        The number of sources added to the catalog.
    """
    data = pd.read_csv(filename)
    data.columns = data.columns.str.strip().str.lstrip("# #").str.strip()
    number_of_sources = 0
    for _, row in data.iterrows():
        flux = row["flux(Jy)"] * u.Jy
        if flux < flux_lower_limit:
            continue
        ra = (
            row["RA(deg)"] if row["RA(deg)"] > 0.0 else row["RA(deg)"] + 360.0
        ) * u.deg
        dec = row["dec(deg)"] * u.deg
        strname = SkyCoord(ra=ra, dec=dec).to_string("hmsdms")
        IAUname = "J" + "".join(re.split(r"[ hmds]", strname))
        client.create(
            position=ICRS(
                ra=ra,
                dec=dec,
            ),
            flux=flux,
            name=IAUname,
        )
        number_of_sources += 1
    return number_of_sources


def main():  # pragma: no cover
    import argparse as ap

    parser = ap.ArgumentParser(
        prog="socat-websky-csv", description="Ingest a WebSky CSV catalog into SOCat"
    )

    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help="Input CSV file conforming to the WebSky point source standard",
        required=True,
    )

    parser.add_argument(
        "--flux-lower-limit",
        type=float,
        help="Lower limit on flux (Jy) for sources to ingest",
        required=False,
        default=0.0,
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="File to serialize the catalog as. If not provided, your configured SOCat environment is used",
        required=False,
        default=None,
    )

    args = parser.parse_args()

    if args.output is not None:
        client = MockClient()
        output_path = args.output
    else:
        from socat.client.settings import SOCatClientSettings

        settings = SOCatClientSettings()
        client = settings.client

        if settings.client_type == "pickle":
            output_path = settings.pickle_path
        else:
            output_path = None

    number_of_sources = ingest_csv_file(
        client=client, filename=args.file, flux_lower_limit=args.flux_lower_limit * u.Jy
    )

    print(f"Ingested {number_of_sources} sources")

    if output_path is not None:
        with open(output_path, "wb") as handle:
            pickle.dump(client, handle)

        print(f"Wrote serialized socat instance to {output_path}")
