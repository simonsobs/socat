from pathlib import Path

import astropy.units as u
import pyarrow.parquet as pq
from astropy.coordinates import ICRS
from sqlalchemy.ext.asyncio import AsyncSession

from socat import core


async def read_jpl(file_path: Path, my_session: AsyncSession):
    table = pq.read_table(file_path).to_pandas()

    designations = table["designation"].tolist()
    asts = list(set(designations))

    for ast in asts:
        async with my_session:
            MPC_id = ast.split()[0]
            name = ast.split()[1] if len(ast.split()) > 1 else None
            sso_id = (
                await core.create_sso(name=name, MPC_id=MPC_id, session=my_session)
            ).sso_id

            ast_df = table[table["designation"] == ast]

            for _, row in ast_df.iterrows():
                time = (row["julian_day"] - 2440587.5) * 86400
                position = ICRS(ra=row["ra_deg"] * u.deg, dec=row["dec_deg"] * u.deg)
                flux = row["flux_mJy"] * u.mJy if "flux_mJy" in row else None

                await core.create_ephem(
                    session=my_session,
                    sso_id=sso_id,
                    MPC_id=MPC_id,
                    name=name,
                    time=time,
                    position=position,
                    flux=flux,
                )
