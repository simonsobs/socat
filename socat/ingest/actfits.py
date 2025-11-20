"""
Ingest ACT fits files into an instance of SOCat.
"""

import pickle
from pathlib import Path

from astropy import units as u
from astropy.coordinates import ICRS
from astropy.io import fits

from socat.client.core import ClientBase
from socat.client.mock import Client as MockClient


def ingest_fits_file(
    client: ClientBase,
    filename: Path,
    hdu: int = 1,
) -> int:
    """
    Ingest a FITS file into the provided SOCat client.

    Parameters
    ----------
    client: ClientBase
        The SOCat client to use.
    filename: Path
        Path to the ACT-compatible FITS point source file to load.
    hdu: int = 1
        The HDU in the file that corresponds to the sources table.

    Returns
    -------
    number_of_sources: int
        The number of sources added to the catalog.
    """

    table = fits.open(filename, hdu=hdu)[hdu]

    number_of_sources = 0

    for row in table.data:
        client.create(
            position=ICRS(
                ra=row["raDeg"] * u.deg,
                dec=row["decDeg"] * u.deg,
            ),
            flux=row["fluxJy"] * u.Jy,
            name=row["name"],
        )

        number_of_sources += 1

    return number_of_sources


def main():  # pragma: no cover
    import argparse as ap

    parser = ap.ArgumentParser(
        prog="socat-act-fits", description="Ingest an ACT FITS catalog into SOCat"
    )

    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help="Input FITS file conforming to the ACT point source standard",
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

    number_of_sources = ingest_fits_file(client=client, filename=args.file)

    print(f"Ingested {number_of_sources} sources")

    if output_path is not None:
        with open(output_path, "wb") as handle:
            pickle.dump(client, handle)

        print(f"Wrote serialized socat instance to {output_path}")
