from astropy.coordinates import ICRS
from astropy.time import Time
from sqlalchemy.ext.asyncio import AsyncSession

from socat.core.fixed_sources import get_box_fixed
from socat.core.moving_sources import get_ephem_points
from socat.core.sso import get_box_sso
from socat.database import RegisteredFixedSource, SolarSystemObject, statements

from .generator import SourceGenerator


async def get_box(
    lower_left: ICRS,
    upper_right: ICRS,
    t_min: Time,
    t_max: Time,
    session: AsyncSession,
) -> list[SourceGenerator]:
    """
    Get a list of all sources within a box between given time bounds.
    """

    fixed_sources: list[RegisteredFixedSource] = await get_box_fixed(
        lower_left=lower_left, upper_right=upper_right, session=session
    )

    sso_sources: list[SolarSystemObject] = await get_box_sso(
        lower_left=lower_left,
        upper_right=upper_right,
        t_min=t_min,
        t_max=t_max,
        session=session,
    )

    all_sources = []

    for source in fixed_sources:
        gen = SourceGenerator(
            source=source,
            ephems=None,
        )
        all_sources.append(gen)

    for source in sso_sources:
        ephems = await get_ephem_points(
            source=source, t_min=t_min, t_max=t_max, session=session
        )
        gen = SourceGenerator(source=source, ephems=ephems)
        all_sources.append(gen)

    return all_sources


async def get_monitored_sources(
    t_min: Time,
    t_max: Time,
    session: AsyncSession,
) -> list[SourceGenerator]:
    """
    Get a list of all monitored sources (both fixed and moving) between given time bounds.

    Parameters
    ----------
    t_min : Time
        Start of time range.
    t_max : Time
        End of time range.
    session : AsyncSession
        Asynchronous session to use.

    Returns
    -------
    list[SourceGenerator]
        List of SourceGenerators for all monitored sources.
    """

    fixed_result = await session.execute(statements.get_monitored_fixed_sources())
    fixed_sources: list[RegisteredFixedSource] = [
        s.to_model() for s in fixed_result.scalars()
    ]

    sso_result = await session.execute(
        statements.get_monitored_ssos(t_min=t_min, t_max=t_max)
    )
    sso_sources: list[SolarSystemObject] = [s.to_model() for s in sso_result.scalars()]

    all_sources = []

    for source in fixed_sources:
        gen = SourceGenerator(source=source, ephems=None)
        all_sources.append(gen)

    for source in sso_sources:
        ephems = await get_ephem_points(
            source=source, t_min=t_min, t_max=t_max, session=session
        )
        gen = SourceGenerator(source=source, ephems=ephems)
        all_sources.append(gen)

    return all_sources


async def get_pointing_sources(
    t_min: Time,
    t_max: Time,
    session: AsyncSession,
) -> list[SourceGenerator]:
    """
    Get a list of all pointing sources (both fixed and moving) between given time bounds.

    Parameters
    ----------
    t_min : Time
        Start of time range.
    t_max : Time
        End of time range.
    session : AsyncSession
        Asynchronous session to use.

    Returns
    -------
    list[SourceGenerator]
        List of SourceGenerators for all pointing sources.
    """
    fixed_result = await session.execute(statements.get_pointing_fixed_sources())
    fixed_sources: list[RegisteredFixedSource] = [
        s.to_model() for s in fixed_result.scalars()
    ]

    sso_result = await session.execute(
        statements.get_pointing_ssos(t_min=t_min, t_max=t_max)
    )
    sso_sources: list[SolarSystemObject] = [s.to_model() for s in sso_result.scalars()]

    all_sources = []

    for source in fixed_sources:
        gen = SourceGenerator(source=source, ephems=None)
        all_sources.append(gen)

    for source in sso_sources:
        ephems = await get_ephem_points(
            source=source, t_min=t_min, t_max=t_max, session=session
        )
        gen = SourceGenerator(source=source, ephems=ephems)
        all_sources.append(gen)

    return all_sources
