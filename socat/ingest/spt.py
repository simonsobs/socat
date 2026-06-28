"""
Ingest ACT fits files into an instance of SOCat.
"""

import csv
import pickle
from pathlib import Path

from astropy import units as u
from astropy.coordinates import ICRS
from astropy.io import fits

from socat.client.core import ClientBase
from socat.client.mock import Client as MockClient


def ingest_spt_txt_file(
    client: ClientBase,
    filename: Path,
    monitored_flux_threshold: u.Quantity = 20.0 * u.mJy,
    pointing_flux_threshold: u.Quantity = 300.0 * u.mJy,
) -> int:
    """
    Ingest an SPT point source txt catalog into the provided SOCat client.

    The file format has '#'-prefixed comment lines, a quoted tab-separated
    header row, and tab-separated data rows. Uses 150 GHz flux as the
    primary flux.

    Parameters
    ----------
    client: ClientBase
        The SOCat client to use.
    filename: Path
        Path to the SPT txt point source file to load.
    monitored_flux_threshold: u.Quantity = 20.0 * u.mJy
        Sources at or above this flux are flagged monitored=True.
    pointing_flux_threshold: u.Quantity = 300.0 * u.mJy
        Sources at or above this flux are flagged pointing=True.

    Returns
    -------
    number_of_sources: int
        The number of sources added to the catalog.
    """
    number_of_sources = 0

    with open(filename, "r") as f:
        non_comment_lines = (line for line in f if not line.startswith("#"))
        reader = csv.DictReader(non_comment_lines, delimiter="\t", quotechar='"')

        for row in reader:
            flux = float(row["S90raw(mJy)"]) * u.mJy
            client.create_source(
                position=ICRS(
                    ra=float(row["ra(deg)"]) * u.deg,
                    dec=float(row["dec(deg)"]) * u.deg,
                ),
                flux=flux,
                name=row["iau_name"],
                flags={
                    "monitored": flux >= monitored_flux_threshold,
                    "pointing": flux >= pointing_flux_threshold,
                },
            )
            number_of_sources += 1

    return number_of_sources


def main():  # pragma: no cover
    import argparse as ap

    parser = ap.ArgumentParser(
        prog="socat-spt-txt", description="Ingest an SPT catalog into SOCat"
    )

    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help="Input txt file conforming to the SPT point source standard",
        required=True,
    )

    parser.add_argument(
        "--monitored-flux-threshold-mJy",
        type=float,
        help="Flux threshold (mJy) above which sources are flagged monitored",
        default=20.0,
    )

    parser.add_argument(
        "--pointing-flux-threshold-mJy",
        type=float,
        help="Flux threshold (mJy) above which sources are flagged for pointing",
        default=300.0,
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

    number_of_sources = ingest_spt_txt_file(
        client=client,
        filename=args.file,
        monitored_flux_threshold=args.monitored_flux_threshold_mJy * u.mJy,
        pointing_flux_threshold=args.pointing_flux_threshold_mJy * u.mJy,
    )

    print(f"Ingested {number_of_sources} sources")

    if output_path is not None:
        with open(output_path, "wb") as handle:
            pickle.dump(client, handle)

        print(f"Wrote serialized socat instance to {output_path}")
