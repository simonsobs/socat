from functools import lru_cache

import numpy as np
from astropy.coordinates import ICRS
from astropy.units import Quantity
from scipy.interpolate import make_interp_spline
from sqlalchemy.ext.asyncio import AsyncSession

from .core import get_ephem_points
from .database import ExtragalacticSource, SolarSystemSource


class SourceGenerator:
    def __init__(
        self,
        source: ExtragalacticSource | SolarSystemSource,
        t_min: int,
        t_max: int,
    ):
        self.source = source
        self.t_min = t_min
        self.t_max = t_max
        self.interp = None

    async def init_interp(self, session: AsyncSession) -> None:
        """
        Initialize the interpolator object.
        If our source type is a RegisteredFixedSource, then
        interp will always just return the same ra/dec/flux
        (Recall that the RegisteredFixedSource.flux is
        a fixed estimate and not the light curve). If
        the soure type is SolarSystemSource, then linear
        interp ra/dec/flux over the requested time range.

        Parameters
        ----------
        session : AsyncSession
            Session to use for querrying

        Returns
        -------
        None
        """
        if type(self.source) is ExtragalacticSource:
            self.ra_unit = self.source.position.ra.unit
            self.dec_unit = self.source.position.dec.unit
            self.flux_unit = self.source.flux.unit
            self.interp = lambda _: (
                self.source.position.ra.value,
                self.source.position.dec.value,
                self.source.flux.value,
            )

        elif type(self.source) is SolarSystemSource:
            ephems = await get_ephem_points(
                self.source, t_min=self.t_min, t_max=self.t_max, session=session
            )
            x = np.zeros(len(ephems))
            y = np.zeros((len(ephems), 3))
            for i, ephem in enumerate(ephems):
                x[i] = ephem.time
                y[i] = (
                    ephem.position.ra.value,
                    ephem.position.dec.value,
                    ephem.flux.value,
                )

            self.ra_unit = ephem.position.ra.unit  # This assumes all ephem points have same units but this should probably be enforced upstream anyway.
            self.dec_unit = ephem.position.dec.unit
            self.flux_unit = ephem.flux.unit
            self.interp = make_interp_spline(x, y, k=1)

    @lru_cache(maxsize=128)  # This can cause memory leaks so we might not want it
    def at_time(self, t: int) -> tuple[ICRS, Quantity]:
        """
        Get the ra/dec/flux of the source at the requested time.

        Parameters
        ----------
        t : int
            Time to get ra/dec/flux at

        Returns
        -------
        (position, flux) : tuple[ICRS, Quantity]
            Interpolated ra/dec/flux in the same units as self.source

        Raises
        ------
        RuntimeError
            If interp is not initialized.
        ValueError
            If t is outside t bounds.
        """
        if self.interp is None:
            raise RuntimeError(
                "Error: interp must be initialized first. Have you run init_interp?"
            )
        if t < self.t_min or self.t_max < t:
            raise ValueError(
                "Error, requested t={} outside initialized bounds {}-{}".format(
                    t, self.t_min, self.t_max
                )
            )

        ra, dec, flux = self.interp(t)
        position = ICRS(ra=ra * self.ra_unit, dec=dec * self.dec_unit)
        flux = flux * self.flux_unit

        return (position, flux)
