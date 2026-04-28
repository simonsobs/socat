from astropy.coordinates import ICRS
from sqlalchemy.ext.asyncio import AsyncSession

from socat.generator import SourceGenerator


async def get_time_box(
    lower_left: ICRS,
    upper_right: ICRS,
    start_time: int,
    end_time: int,
    session: AsyncSession,
) -> list[SourceGenerator]:
    """
    Get a list of all sources within a box between given time bounds.
    """
