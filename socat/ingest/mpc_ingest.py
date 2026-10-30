"""
Ingest MPC ephemeris parquet files into mock SOCat moving-source clients.
"""

import argparse as ap
import pickle
from pathlib import Path

import pandas as pd
from astropy import units as u
from astropy.coordinates import ICRS
from astropy.time import Time
from tqdm import tqdm

from socat.client.core import ClientBase
from socat.client.mock import Client

from .utils import _parse_designation

client = Client()


def ingest_mpc_parquet_file(
    client: ClientBase,
    filename: Path,
    downsample: int = 1,
) -> tuple[list[int], list[int]]:
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
    tuple[list[int], list[int]]
        Number of SSOs created and number of ephemeris points created at each index in the array.

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

    number_of_ssos = []
    number_of_ephems = []

    for ssos, (encke_id, rows) in enumerate(data.groupby("encke_id", sort=False, dropna=False), start = 1):
        # Handle MPC (Encke) IDs that don't exist
        if pd.isna(encke_id):
            mpc_id, name = _parse_designation(rows["designation"].iloc[0])
        else:
            mpc_id, name = _parse_designation(encke_id)
        sso = client.create_sso(name=name, MPC_id=mpc_id, flags={"monitored": True})
    
        number_of_ssos.append(ssos)
        ephems = 0

        for _, row in tqdm(
            rows[::downsample].iterrows(),
            desc=f"Ingesting {encke_id}" if mpc_id is not None else f"Ingesting {name}",
            total=len(rows[::downsample]),
        ):
            unix_time = Time(row["time_utc"], format="isot", scale="utc")

            client.create_ephem(
                sso_id=sso.sso_id,
                MPC_id=mpc_id,
                name=name,
                time=unix_time,
                position=ICRS(
                    ra=float(row["skycoord.ra"]) * u.deg,
                    dec=float(row["skycoord.dec"]) * u.deg,
                )
            )
            
            ephems += 1
        number_of_ephems.append(ephems)

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
