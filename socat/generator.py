from functools import cached_property

import numpy as np
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
        session: AsyncSession,
    ):
        self.source = source
        if type(self.source) is ExtragalacticSource:
            self.interp = lambda x: self.source.position

        elif type(self.source) is SolarSystemSource:
            ephems = get_ephem_points(t_min=t_min, t_max=t_max, session=session)
            x = np.zeros(len(ephems))
            y = np.zeros((len(ephems), 3))
            for i, ephem in enumerate(ephems):
                x[i] = ephem.time
                y[i] = ephem.position.ra.value, ephem.position.dec.value, ephem.flux
            self.interp = make_interp_spline(x, y, k=1)

    @cached_property
    def at_time(self, t):
        return self.interp(t)
