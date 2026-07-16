import pickle
from pathlib import Path

import numpy as np
from astropy import units as u
from astropy.coordinates import ICRS

from socat.client.core import ClientBase
from socat.client.mock import Client as MockClient


def ingest_text_file(
    client: ClientBase,
    filename: Path,
) -> int:
    """
    Ingest a text file into an SOCat.

    Parameters
    ----------
    client: ClientBase
        The SOCat client to use.
    filename: Path
        Path to the text file to load. The file should have columns for ra, dec, and name.

    Returns
    -------
    number_of_sources: int
        The number of sources added to the catalog.
    """

    table = np.loadtxt(
        filename,
        dtype=[("ra", "f8"), ("dec", "f8"), ("name", "S20"), ("monitored", "S20")],
        skiprows=1,
    )
    names = [
        name.decode("utf-8") if isinstance(name, (bytes, np.bytes_)) else name
        for name in table["name"]
    ]
    table["name"] = names

    table["monitored"] = [bool(monitored.decode("utf-8")) for monitored in table["monitored"]]

    number_of_sources = 0

    for row in table:
        client.create_source(
            position=ICRS(
                ra=float(row["ra"]) * u.deg,
                dec=float(row["dec"]) * u.deg,
            ),
            name=row["name"],
            flags={
                "monitored": row["monitored"],
                "pointing": False,
            },
        )

        number_of_sources += 1

    return number_of_sources


def main():  # pragma: no cover
    import argparse as ap

    parser = ap.ArgumentParser(
        prog="socat-text", description="Ingest a text catalog into SOCat"
    )

    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help="Input text file containing the catalog",
        required=True,
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

    number_of_sources = ingest_text_file(
        client=client,
        filename=args.file,
    )

    print(f"Ingested {number_of_sources} sources")

    if output_path is not None:
        with open(output_path, "wb") as handle:
            pickle.dump(client, handle)

        print(f"Wrote serialized socat instance to {output_path}")
