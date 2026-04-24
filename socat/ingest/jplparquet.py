"""
Ingest JPL ephemeris parquet files into mock SOCat moving-source clients.
"""

import pickle
from pathlib import Path

import pandas as pd
from astropy import units as u
from astropy.coordinates import ICRS
from astropy.time import Time
from tqdm import tqdm

from socat.client.core import ClientBase


def _parse_designation(designation: str) -> tuple[int | None, str | None]:
    """Split a designation like ``'1 Ceres'`` into MPC ID and name."""
    parts = str(designation).split(maxsplit=1)
    if not parts:
        return None, None

    try:
        mpc_id = int(parts[0])
    except ValueError:
        mpc_id = None

    if len(parts) > 1:
        name = parts[1]
    else:
        name = str(mpc_id)

    return mpc_id, name


def ingest_jpl_parquet_file(
    client: ClientBase,
    filename: Path,
) -> tuple[int, int]:
    """
    Ingest a JPL Horizons parquet file into mock SOCat moving-source clients.

    Parameters
    ----------
    client : ClientBase
        SOCat client that will hold both the SSO entries and the time-dependent ephemeris positions.
    filename : Path
        Path to the JPL parquet file.

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
    required_columns = {"designation", "julian_day", "ra_deg", "dec_deg"}
    missing_columns = required_columns.difference(data.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns in {filename}: {sorted(missing_columns)}"
        )

    number_of_ssos = 0
    number_of_ephems = 0

    for designation, rows in data.groupby("designation", sort=False):
        mpc_id, name = _parse_designation(designation)
        client.sso.create_sso(name=name, MPC_id=mpc_id)
        number_of_ssos += 1
        sso = client.sso.get_sso_MPC_id(MPC_id=mpc_id)[0]

        for _, row in tqdm(
            rows[::10].iterrows(), desc=f"Ingesting {designation}", total=len(rows[:10])
        ):
            flux = None
            if "flux_mJy" in row.index and pd.notna(row["flux_mJy"]):
                flux = row["flux_mJy"] * u.mJy

            unix_time = Time(row["julian_day"], format="jd").unix

            client.ephem.create_ephem(
                sso_id=sso.sso_id,
                MPC_id=mpc_id,
                name=name,
                time=unix_time,
                position=ICRS(
                    ra=float(row["ra_deg"]) * u.deg,
                    dec=float(row["dec_deg"]) * u.deg,
                ),
                flux=flux,
            )
            number_of_ephems += 1

    return number_of_ssos, number_of_ephems


def build_mock_database(filename: Path) -> dict:
    """Build a serializable mock database from a JPL parquet file."""
    from socat.client.settings import SOCatClientSettings

    settings = SOCatClientSettings()
    client = settings.client

    number_of_ssos, number_of_ephems = ingest_jpl_parquet_file(
        client,
        filename=filename,
    )

    return {
        "solar_system": client.sso,
        "ephem": client.ephem,
        "meta": {
            "source_file": str(filename),
            "number_of_ssos": number_of_ssos,
            "number_of_ephems": number_of_ephems,
        },
    }


def main():  # pragma: no cover
    import argparse as ap

    parser = ap.ArgumentParser(
        prog="socat-jpl-parqet",
        description="Ingest a JPL ephemeris parquet file into mock SOCat moving-source clients",
    )

    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help="Input parquet file downloaded from JPL Horizons",
        required=True,
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

    database = build_mock_database(args.file)
    meta = database["meta"]

    print(
        f"Ingested {meta['number_of_ssos']} solar system objects and "
        f"{meta['number_of_ephems']} ephemeris points"
    )

    if args.output is not None:
        with open(args.output, "wb") as handle:
            pickle.dump(database, handle)

        print(f"Wrote serialized mock JPL database to {args.output}")


if __name__ == "__main__":  # pragma: no cover
    main()
