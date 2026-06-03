from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity
from sqlalchemy.ext.asyncio import AsyncSession

from socat.core.fixed_sources import get_box_fixed
from socat.core.sso import get_box_sso
from socat.database import RegisteredFixedSource, SolarSystemObject, statements
from socat.generator import SourceGenerator


async def get_forced_photometry(
    session: AsyncSession,
    t_min: Time,
    t_max: Time,
    minimum_flux: Quantity | None = None,
) -> list[SourceGenerator]:
    """
    Get all monitored sources for forced photometry as initialized SourceGenerators.

    Fixed sources are filtered to monitored=True and optionally to those above minimum_flux.
    SSOs are filtered to monitored=True; minimum_flux does not apply to SSOs.
    t_min/t_max bound the ephemeris range used to initialize SSO interpolators.

    Parameters
    ----------
    session : AsyncSession
        Asynchronous session to use
    t_min : Time
        Start of time range for SSO ephemeris interpolation.
    t_max : Time
        End of time range for SSO ephemeris interpolation.
    minimum_flux : Quantity | None
        If provided, additionally filter fixed sources to those with flux >= minimum_flux.

    Returns
    -------
    list[SourceGenerator]
        Initialized SourceGenerators for all monitored sources.
    """
    fixed_result = await session.execute(
        statements.get_forced_photometry_sources(minimum_flux=minimum_flux)
    )
    sso_result = await session.execute(statements.get_forced_photometry_ssos())

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
