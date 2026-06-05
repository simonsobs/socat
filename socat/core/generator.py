import numpy as np
from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity
from scipy.interpolate import make_interp_spline

from ..database import RegisteredFixedSource, RegisteredMovingSource, SolarSystemObject


class SourceGenerator:
    """
    Class which provides a consistent interface to both fixed sources and SSOs.
    It also provides a consistent initialization interface for both types of sources.
    This means an end user can do source_gen = SourceGenerator(source, ephems) and then source_gen.at_time(t) to get the position and flux of the source at time t regardless of whether the source is fixed or an SSO.

    Attributes
    ----------
    source : RegisteredFixedSource | SolarSystemObject
        The source for which this is a generator.
    t_min : Time
        Minimum time for which this generator can be used. For fixed sources this is set to a very wide range, but for SSOs this is set to the min and max of the ephemris points provided.
    t_max : Time
        Maximum time for which this generator can be used.
    do_flux : bool
        Whether to interpolate flux or not. This is set to False at_time will return None for flux.
        Internally set based on whether flux is provided for the source (for fixed sources) or for all ephemris points (for SSOs).
    ra_unit : Unit
        Unit of right ascension for this source. Set based on the units of the source provided.
    dec_unit : Unit
        Unit of declination for this source. Set based on the units of the source provided.
    flux_unit : Unit | None
        Unit of flux for this source. Set based on the units of the source provided if do_flux is True, otherwise set to None.
    interp : function
        Interpolation function for this source.
    """

    def __init__(
        self,
        source: RegisteredFixedSource | SolarSystemObject,
        ephems: list[RegisteredMovingSource] | None = None,
    ):
        """
        Initialize a SourceGenerator for a given source and ephemeris points.
        Importantly functionality changes based on whether the sources is a fixed source or SSO.

        Parameters
        ----------
        source : RegisteredFixedSource | SolarSystemObject
            The source for which to initialize the generator.
        ephems : list[RegisteredMovingSource] | None
            The ephemeris points for the source. Required if source is a SolarSystemObject, ignored if source is a RegisteredFixedSource.

        Raises
        ------
        ValueError
            If source is a SolarSystemObject and ephems is not provided.
        """

        self.source = source
        if ephems is None:
            if isinstance(source, SolarSystemObject):
                raise ValueError(
                    "Error: ephems must be provided for SolarSystemObject sources"
                )
            self.t_min = Time("1970-01-01T00:00:00.00")
            self.t_max = Time("2100-01-01T00:00:00.00")
            self._init_interp_fixed()
        else:
            times = [ephem.time for ephem in ephems]
            self.t_min = min(times)
            self.t_max = max(times)
            self._init_interp_sso(ephems=ephems)

    def _init_interp_fixed(self):
        """
        Initialize interpolation function for fixed source.
        """
        self.ra_unit = self.source.position.ra.unit
        self.dec_unit = self.source.position.dec.unit
        self.do_flux = self.source.flux is not None
        self.flux_unit = self.source.flux.unit if self.do_flux else None
        self.interp = lambda _: (
            (
                self.source.position.ra.value,
                self.source.position.dec.value,
                self.source.flux.value,
            )
            if self.do_flux
            else (
                self.source.position.ra.value,
                self.source.position.dec.value,
            )
        )

    def _init_interp_sso(self, *, ephems: list[RegisteredMovingSource]):
        """
        Initialize interpolation function for solar system object source.

        Parameters
        ----------
        ephems : list[RegisteredMovingSource]
            List of ephemeris points for the solar system object. Must be provided to initialize interpolation for solar system objects.
        """

        x = np.zeros(len(ephems))

        self.do_flux = True
        for ephem in ephems:  # TODO: please someone have a better way to do this than looping through the ephems twice
            if ephem.flux is None:
                self.do_flux = False
                break

        if self.do_flux:
            y = np.zeros((len(ephems), 3))
        else:
            y = np.zeros((len(ephems), 2))

        for i, ephem in enumerate(ephems):
            x[i] = ephem.time.unix
            y[i] = (
                (
                    ephem.position.ra.value,
                    ephem.position.dec.value,
                    ephem.flux.value,
                )
                if self.do_flux
                else (
                    ephem.position.ra.value,
                    ephem.position.dec.value,
                )
            )

        self.ra_unit = ephem.position.ra.unit  # This assumes all ephem points have same units but this should probably be enforced upstream anyway.
        self.dec_unit = ephem.position.dec.unit
        self.flux_unit = ephem.flux.unit if self.do_flux else None
        self.interp = make_interp_spline(x, y, k=1)

    def at_time(self, t: Time) -> tuple[ICRS, Quantity]:
        """
        Get the ra/dec/flux of the source at the requested time.

        Parameters
        ----------
        t : Time
            Time to get ra/dec/flux at

        Returns
        -------
        (position, flux) : tuple[ICRS, Quantity]
            Interpolated ra/dec/flux in the same units as self.source

        Raises
        ------
        ValueError
            If t is outside t bounds.
        """
        if t < self.t_min or self.t_max < t:
            raise ValueError(
                f"Error, requested t={t} outside initialized bounds {self.t_min}-{self.t_max}"
            )

        if self.do_flux:
            ra, dec, flux = self.interp(t.unix)
            flux = flux * self.flux_unit if flux != 0 else None
        else:
            ra, dec = self.interp(t.unix)
            flux = None
        position = ICRS(ra=ra * self.ra_unit, dec=dec * self.dec_unit)

        return (position, flux)
