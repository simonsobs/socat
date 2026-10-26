"""
Ingest MPC ephemeris parquet files into mock SOCat moving-source clients.
"""

import argparse as ap
import pickle
import re
from pathlib import Path

import pandas as pd
from astropy import units as u
from astropy.coordinates import ICRS
from astropy.time import Time
from tqdm import tqdm

from socat.client.core import ClientBase
from socat.client.mock import Client

client = Client()


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

    Examples
    --------
    '1 Ceres'   -> (1, 'Ceres')

    '2011 KE'   -> (None, '2011 KE')
    """

    designation = str(designation).strip()

    if not designation:
        return None, None

    # standard unpacked provisional designation
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


def ingest_mpc_parquet_file(
    client: ClientBase,
    filename: Path,
    downsample: int = 1,
) -> tuple[int, int]:
    """
    Ingest an MPC parquet file into mock SOCat moving-source clients.

    Parameters
    ----------
    client : ClientBase
        SOCat client that will hold both the SSO entries and the time-dependent ephemeris positions.
    filename : Path
        Path to the MPC parquet file.
    downsample : int
        Downsample factor for the input ephem (e.g., 10 means keep every 10th row).

    Returns
    -------
    tuple[int, int]
        Number of SSOs created and number of ephemeris points created.

    Raises
    ------
    ValueError
        If the input file is missing required columns.
    """

    data = pd.read_parquet(filename)
    required_columns = {
        "encke_id",
        "designation",
        "time_utc",
        "skycoord.ra",
        "skycoord.dec",
        "delta",
    }
    missing_columns = required_columns.difference(data.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns in {filename}: {sorted(missing_columns)}"
        )

    number_of_ssos = 0
    number_of_ephems = 0

    for encke_id, rows in data.groupby("encke_id", sort=False):
        mpc_id, name = _parse_designation(encke_id)
        sso = client.create_sso(name=name, MPC_id=mpc_id, flags={"monitored": True})
        number_of_ssos += 1

        for _, row in tqdm(
            rows[::downsample].iterrows(),
            desc=f"Ingesting {encke_id}",
            total=len(rows[::downsample]),
        ):
            flux = None  # Default flux if not provided
            if "flux_mJy" in row.index and pd.notna(row["flux_mJy"]):
                flux = row["flux_mJy"] * u.mJy

            unix_time = Time(row["time_utc"], format="isot", scale="utc")

            client.create_ephem(
                sso_id=sso.sso_id,
                MPC_id=mpc_id,
                name=name,
                time=unix_time,
                position=ICRS(
                    ra=float(row["skycoord.ra"]) * u.deg,
                    dec=float(row["skycoord.dec"]) * u.deg,
                ),
                flux=flux,
            )
            number_of_ephems += 1

    return number_of_ssos, number_of_ephems


def build_mock_database(filename: Path, downsample: int = 1) -> dict:
    """Build a serializable mock database from an MPC parquet file.

    Parameters
    ----------
    filename : Path
        Path to the MPC parquet file.
    downsample : int, optional
        Downsample factor the input parquet file, by default 1

    Returns
    -------
    dict
        Dictionary containing the mock database.
    """

    number_of_ssos, number_of_ephems = ingest_mpc_parquet_file(
        client,
        filename=filename,
        downsample=int(downsample),
    )

    return {
        "solar_system": client._sso,
        "ephem": client._ephem,
        "meta": {
            "source_file": str(filename),
            "number_of_ssos": number_of_ssos,
            "number_of_ephems": number_of_ephems,
        },
    }


def main() -> None:  # pragma: no cover
    """Read command line arguments to build mock MPC database.

    Parses command line arguments and ingests the specified MPC parquet file into a mock SOCat moving-source client.
    """

    parser = ap.ArgumentParser(
        prog="socat-mpc-parquet",
        description="Ingest an MPC ephemeris parquet file into mock SOCat moving-source clients",
    )

    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help="Input parquet file from MPC",
        required=True,
    )

    parser.add_argument(
        "-d",
        "--downsample",
        type=int,
        help="Downsample factor for the input parquet file (e.g., 10 means keep every 10th row)",
        default=1,
    )

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="File to serialize the mock database as a pickle",
        required=False,
        default=None,
    )

    args = parser.parse_args()

    database = build_mock_database(args.file, downsample=args.downsample)
    meta = database["meta"]

    print(
        f"Ingested {meta['number_of_ssos']} solar system objects and "
        f"{meta['number_of_ephems']} ephemeris points"
    )

    if args.output is not None:
        with open(args.output, "wb") as handle:
            pickle.dump(database, handle)

        print(f"Wrote serialized mock MPC database to {args.output}")


if __name__ == "__main__":  # pragma: no cover
    main()
