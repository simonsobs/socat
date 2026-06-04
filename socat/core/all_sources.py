from astropy.coordinates import ICRS
from astropy.time import Time
from sqlalchemy.ext.asyncio import AsyncSession

from socat.core.fixed_sources import get_box_fixed
from socat.core.sso import get_box_sso
from socat.database import RegisteredFixedSource, SolarSystemObject, statements
from socat.generator import SourceGenerator


async def get_monitored_sources(
    session: AsyncSession,
    t_min: Time,
    t_max: Time,
) -> list[SourceGenerator]:
    """
    Get all monitored sources as initialized SourceGenerators.

    Returns fixed sources and SSOs with monitored=True.
    t_min/t_max bound the ephemeris range used to initialize SSO interpolators.

    Parameters
    ----------
    session : AsyncSession
        Asynchronous session to use
    t_min : Time
        Start of time range for SSO ephemeris interpolation.
    t_max : Time
        End of time range for SSO ephemeris interpolation.

    Returns
    -------
    list[SourceGenerator]
        Initialized SourceGenerators for all monitored sources.
    """
    fixed_result = await session.execute(statements.get_monitored_fixed_sources())
    sso_result = await session.execute(
        statements.get_monitored_ssos(t_min=t_min, t_max=t_max)
    )

    all_sources = [s.to_model() for s in fixed_result.scalars()] + [
        s.to_model() for s in sso_result.scalars()
    ]

    result = []
    for source in all_sources:
        gen = SourceGenerator(source, t_min, t_max)
        await gen.init_interp(session=session)
        result.append(gen)

    return result


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
        gen = SourceGenerator(source, t_min, t_max)
        await gen.init_interp(session=session)
        all_sources.append(gen)

    for source in sso_sources:
        gen = SourceGenerator(source, t_min, t_max)
        await gen.init_interp(session=session)
        all_sources.append(gen)

    return all_sources
