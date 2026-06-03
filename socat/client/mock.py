"""
Uses a local dictionary to implement the core.
"""

from importlib import import_module
from typing import Any

import astropy.units as u
import numpy as np
from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity
from astroquery.query import BaseVOQuery
from scipy.interpolate import make_interp_spline

from socat.database import (
    AstroqueryService,
    RegisteredFixedSource,
    RegisteredMovingSource,
    SolarSystemObject,
)

from .core import (
    AstroqueryClientBase,
    ClientBase,
    EphemClientBase,
    SolarSystemClientBase,
    SourceGeneratorBase,
)


class Client(ClientBase):
    """
    Mock client for testing

    Attributes
    ----------
    catalog : dict[int, RegisteredFixedSource]
        Dictionary of fixed sources replciating a catalog
    n : int
        Number of entries in catalog

    Methods
    -------
    create_source(self, *, position: ICRS, name: str | None = None, flux: Quantity | None = None)
        Create a source and add it to the catalog
    create_name(self, *, name: str, service_name: str)
        Create a source by name using astroquery and add it to the catalog
    get_box(self, *, lower_left: ICRS, upper_right: IRCS)
        Get sources within box
    get_source(self, *, source_id: int)
        Get a source by source_id
    update_source(self, *, source_id: int, position: ICRS | None = None, name: str | None = None, flux: Quantity | None = None)
        Update source by source_id
    delete_source(self, *, source_id: int)
        Delete source by source_id
    """

    catalog: dict[int, RegisteredFixedSource]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog
        """
        self.catalog = {}
        self.n = 0
        self._astroquery = AstorqueryClient()
        self._sso = SolarSystemClient()
        self._ephem = EphemClient()

    @property
    def astroquery(self) -> AstroqueryClientBase:
        return self._astroquery

    @property
    def sso(self) -> SolarSystemClientBase:
        return self._sso

    @property
    def ephem(self) -> EphemClientBase:
        return self._ephem

    def create_source(
        self, *, position: ICRS, name: str | None = None, flux: Quantity | None = None
    ) -> RegisteredFixedSource:
        """
        Create a new source and add it to the catalog.

        Parameters
        ----------
        position : ICRS
            Position of source in ICRS coordinates
        flux : Quantity | None, Default: None
            Flux of source.
        name : str | None, Default: None
            Name of source

        Returns
        -------
        source : RegisteredFixedSource
            Registered Fixed Source that was added
        """
        if flux is not None:
            flux = flux.to(u.mJy)
        source = RegisteredFixedSource(
            source_id=self.n,
            position=position,
            flux=flux,
            name=name,
        )
        self.catalog[self.n] = source
        self.n += 1

        return source

    def create_name(
        self, *, name: str, astroquery_service: str
    ) -> RegisteredFixedSource:
        """
        Create a new source by name and add it to the catalog.

        Parameters
        ----------
        name : str
            name of source to add
        astroquery_service : str
            Name of astroquery service to use

        Returns
        -------
        source : RegisteredFixedSource
            Registered Fixed Source that was added
        """

        service: BaseVOQuery = getattr(
            import_module(f"astroquery.{astroquery_service.lower()}"),
            astroquery_service,
        )

        requested_params = ["ra", "dec"]

        result_table = service.query_object(name)
        result_table["ra"].convert_unit_to("deg")
        result_table["dec"].convert_unit_to("deg")
        if "flux" in result_table.columns:
            result_table["flux"].convert_unit_to("mJy")  # pragma: no cover
        result_dict = {param: None for param in requested_params}
        if len(result_table) == 0:
            return None
        for param in requested_params:
            try:
                result_dict[param] = result_table[param].value.data[
                    0
                ]  # TODO: currently only take first match.
            # Maybe should warn if more than one match?
            except KeyError:  # pragma: no cover
                continue

        position = ICRS(
            ra=result_dict["ra"] * u.deg,
            dec=result_dict["dec"] * u.deg,
        )
        flux = result_dict.get("flux", None)
        if flux is not None:  # pragma: no cover
            flux *= u.mJy
        source = RegisteredFixedSource(
            source_id=self.n,
            position=position,
            name=name,
            flux=flux,
        )
        self.catalog[self.n] = source
        self.n += 1

        return source

    def get_box_fixed(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
    ) -> list[RegisteredFixedSource]:
        """
        Get sources within a box.

        Parameters
        ----------
        lower_left : ICRS
            Lower left corner of box in ICRS coordinates
        upper_right : ICRS
            Upper right corner of box in ICRS coordinates

        Returns
        -------
        list(sources) : list[RegisteredFixedSource]
            List of sources in box
        """
        ra_min = lower_left.ra.value
        dec_min = lower_left.dec.value
        ra_max = upper_right.ra.value
        dec_max = upper_right.dec.value
        sources = filter(
            lambda x: (
                (ra_min <= x.position.ra.value <= ra_max)
                and (dec_min <= x.position.dec.value <= dec_max)
            ),
            self.catalog.values(),
        )

        return list(sources)

    def get_source(self, *, source_id: int) -> RegisteredFixedSource | None:
        """
        Get source by id

        Parameters
        ----------
        source_id : int
            ID of source of interest


        Returns
        -------
        self.catalog.get(source_id, None) : RegisteredFixedSource | None
            Source corresponding to source_id. Returns None if source not found
        """
        return self.catalog.get(source_id, None)

    def get_forced_photometry_sources(
        self, *, minimum_flux: Quantity
    ) -> list[RegisteredFixedSource]:
        """
        Get all sources that are used for forced photometry based on a minimum flux.

        Parameters
        ----------
        minimum_flux : Quantity
            Minimum flux for source to be included

        Returns
        -------
        filter : iterable[RegisteredFixedSource]
            List of sources with flux greater than minimum_flux
        """
        return filter(
            lambda x: x.flux is not None and x.flux >= minimum_flux,
            self.catalog.values(),
        )

    def update_source(
        self,
        *,
        source_id: int,
        position: ICRS | None = None,
        name: str | None = None,
        flux: Quantity | None = None,
    ) -> RegisteredFixedSource | None:
        """
        Update a source by id

        Parameters
        ----------
        position : ICRS | Float, Default: None
            Position of source
        name : str | None, Default: None
            Name of source
        flux : Quantity | None, Default: None
            Flux of source

        Returns
        -------
        new : RegisteredFixedSource
            Source that has been updated
        """
        current = self.get_source(source_id=source_id)

        if current is None:
            return None

        new = RegisteredFixedSource(
            source_id=current.source_id,
            position=current.position if position is None else position,
            name=current.name if name is None else name,
            flux=current.flux if flux is None else flux,
        )

        self.catalog[source_id] = new

        return new

    def delete_source(self, *, source_id: int):
        """
        Delete source by id

        Parameters
        ----------
        source_id : int
            ID of source to be deleted

        Returns
        -------
        None
        """
        check = self.catalog.pop(source_id, None)
        if check is not None:
            self.n -= 1

    def create_service(self, *, name: str, config: dict[str, Any]) -> AstroqueryService:
        """
        Create a new astroquery service.

        Parameters
        ----------
        name : str
            Name of astroquery service
        config : dict[str, Any]
            Json to be deserialized containing config options

        Returns
        -------
        service : AstroqueryService
            Astroquery service that was added
        """
        return self.astroquery.create_service(name=name, config=config)

    def get_service(self, *, service_id: int) -> AstroqueryService | None:
        """
        Get a service by id number

        Parameters
        ----------
        service_id : int
            ID of service to get

        Returns
        -------
        self.astroquery.get_service(service_id=service_id) : AstroqueryService | None
            Service corresponding to service_id. Returns None if service not found
        """
        return self.astroquery.get_service(service_id=service_id)

    def get_service_name(self, *, name: str) -> list[AstroqueryService] | None:
        """
        Get a service by name

        Parameters
        ----------
        name : str
            Name of service to get

        Returns
        -------
         : list[AstroqueryService] | None
            List of services corresponding to service_id. Returns None if service not found
        """
        return self.astroquery.get_service_name(name=name)

    def update_service(
        self, *, service_id: int, name: str | None, config: dict[str, Any] | None
    ) -> AstroqueryService:
        """
        Update a service by id

        Parameters
        ----------
        service_id : int
            ID of service to be updated
        name : str | None, Default: None
            Name of source
        config : dict[str, Any] | None, Default: None
            Json to be deserialized containing config options

        Returns
        -------
        new : AstroqueryService
            Service that has been updated
        """
        return self.astroquery.update_service(
            service_id=service_id, name=name, config=config
        )

    def delete_service(self, *, service_id: int):
        """
        Delete service by id

        Parameters
        ----------
        service_id : int
            ID of service to be deleted

        Returns
        -------
        None
        """
        self.astroquery.delete_service(service_id=service_id)

    def create_ephem(
        self,
        *,
        sso_id: int,
        MPC_id: int,
        name: str,
        time: Time,
        position: ICRS,
        flux: Quantity | None = None,
    ) -> RegisteredMovingSource:
        """
        Create a single ephem point associated with a SSO.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of associated SSO (sso_id).
        MPC_id : int
            Minor Planet Center ID of SSO.
        name : str
            Name of SSO
        time : Time
            Time of ephemeris
        position : ICRS
            Position of ephemeris.
        flux : Quantity | None, default=None
            Flux at ephemeris.

        Returns
        -------
        ephem : RegisteredMovingSource
            Ephemeris point that was added.
        """
        return self.ephem.create_ephem(
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time,
            position=position,
            flux=flux,
        )

    def get_ephem(self, *, ephem_id: int) -> RegisteredMovingSource | None:
        """
        Get an ephem point by ID.
        Returns None if ephem not found.

        Parameters
        ----------
        ephem_id : int
            Internal SO ID of ephem point.

        Returns
        -------
        self.ephem.get_ephem(ephem_id=ephem_id) : RegisteredMovingSource | None
            Get requested ephem.
        """
        return self.ephem.get_ephem(ephem_id=ephem_id)

    def get_ephem_points(
        self, *, sso_id: int, t_min: Time, t_max: Time
    ) -> list[RegisteredMovingSource]:
        """
        Get all ephem points for a given source in a given time range.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of source for which to get ephemeris points
        t_min : Time
            Minimum time
        t_max : Time
            Maximum time

        Returns
        -------
        list[RegisteredMovingSource]
            List of requested ephemeris points

        """
        return self.ephem.get_ephem_points(sso_id=sso_id, t_min=t_min, t_max=t_max)

    def update_ephem(
        self,
        *,
        ephem_id: int,
        sso_id: int | None,
        MPC_id: int | None,
        name: str | None,
        time: Time | None,
        position: ICRS | None,
        flux: Quantity | None,
    ) -> RegisteredMovingSource | None:
        """
        Update a solar system ephem.
        Returns None if ephem not found.

        Parameters
        ----------
        ephem_id : int
            Internal SO ID of ephem point.
        sso_id : int
            Internal SO ID of associated SSO.
        MPC_id : int
            Minor Planet Center ID of associated SSO.
        name : str
            Name of associated SSO.
        time : Time
            Time of ephem.
        flux : Quantity | None, default=None
            Flux at ephemeris.

        Returns
        -------
        new : RegisteredMovingSource | None
            Ephemeris point that was updated.
        """
        return self.ephem.update_ephem(
            ephem_id=ephem_id,
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time,
            position=position,
            flux=flux,
        )

    def delete_ephem(self, *, ephem_id: int) -> None:
        """
        Delete an ephem point by ID.

        Parameters
        ----------
        ephem_id : int
            Internal SO ID of ephem point.

        Returns
        -------
        None
        """
        self.ephem.delete_ephem(ephem_id=ephem_id)

    def create_sso(self, *, name: str, MPC_id: int | None) -> SolarSystemObject:
        """
        Create a new solar system source.

        Parameters
        ----------
        name : str
            Name of source
        MPC_id : int
            Minor Planet Center ID of source

        Returns
        -------
        solar_source : SolarSystemObject
            Solar system source that was added.
        """
        return self.sso.create_sso(name=name, MPC_id=MPC_id)

    def get_sso(self, *, sso_id: int) -> SolarSystemObject | None:
        """
        Get a solar system source.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of solar system source

        Returns
        -------
        self.sso.get_sso(sso_id=sso_id) : SolarSytemSource
            Requested solar system source.
        """
        return self.sso.get_sso(sso_id=sso_id)

    def get_box_sso(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
    ) -> list[SolarSystemObject]:
        """
        Get solar system objects which are within a given box in a given time range.
        Note somewhat awkwardly you have to pass ephem_cat to this function, which is
        different from how the real function works. I can't think of another way to
        link these mock tables.

        Parameters
        ----------
        lower_left : ICRS
            Lower left corner of box in ICRS coordinates
        upper_right : ICRS
            Upper right corner of box in ICRS coordinates
        t_min : Time
            Minimum time
        t_max : Time
            Maximum time

        Returns
        -------
        list(solar_sources) : list[SolarSystemObject]
            List of solar system sources in box and time range.
        """
        ra_min = lower_left.ra.value
        dec_min = lower_left.dec.value
        ra_max = upper_right.ra.value
        dec_max = upper_right.dec.value
        ephems = filter(
            lambda x: (
                (ra_min <= x.position.ra.value <= ra_max)
                and (dec_min <= x.position.dec.value <= dec_max)
                and (t_min <= x.time <= t_max)
            ),
            self.ephem.catalog.values(),
        )

        ephem_ids = {ephem.sso_id for ephem in ephems}

        sources = filter(lambda x: x.sso_id in ephem_ids, self.sso.catalog.values())

        return list(sources)

    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
    ) -> list["SourceGenerator"] | None:
        """
        Get all sources (both fixed and moving) inside a given box within a given time range.

        Parameters
        ----------
        lower_left : ICRS
            Lower left corner of box in ICRS coordinates
        upper_right : ICRS
            Upper right corner of box in ICRS coordinates
        t_min : Time
            Minimum time
        t_max : Time
            Maximum time

        Returns
        -------
        list(SourceGenerator) : list[SourceGenerator]
            List of SourceGenerator for sources in box and time range.
        """
        fixed_sources = self.get_box_fixed(
            lower_left=lower_left, upper_right=upper_right
        )
        sso_sources = self.get_box_sso(
            lower_left=lower_left,
            upper_right=upper_right,
            t_min=t_min,
            t_max=t_max,
        )

        return [
            SourceGenerator(source=s, t_min=t_min, t_max=t_max, client=self)
            for s in fixed_sources + sso_sources
        ]

    def get_sso_name(self, *, name: str) -> list[SolarSystemObject] | None:
        """
        Get a solar system source by name.

        Parameters
        ----------
        name : str
            Name of solar system source

        Returns
        -------
        solars : list[SolarSystemObject] | None
            Requested solar system source.
        """
        return self.sso.get_sso_name(name=name)

    def get_sso_MPC_id(self, *, MPC_id: int) -> list[SolarSystemObject] | None:
        """
        Get a solar system source by Minor Planet Center ID.

        Parameters
        ----------
        MPC_id : int
            Minor planet center ID of source

        Returns
        -------
        solars : list[SolarSystemObject] | None
            List of sources with requested MPC ID
        """
        return self.sso.get_sso_MPC_id(MPC_id=MPC_id)

    def update_sso(
        self, *, sso_id: int, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update a solar system source by ID.
        If a variable is None, dont update it.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of solar system source
        name : str | None
            Name of source
        MPC_id : int | None
            Minor planet center ID of source

        Returns
        -------
        new : SolarSystemObject | None
            Updated solar system source
        """
        return self.sso.update_sso(sso_id=sso_id, name=name, MPC_id=MPC_id)

    def delete_sso(self, *, sso_id: int):
        """
        Delete a solar system source by ID.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of solar system source

        Returns
        -------
        None
        """
        self.sso.delete_sso(sso_id=sso_id)


class AstorqueryClient(AstroqueryClientBase):
    """
    Mock client for testing Astroquery

    Attributes
    ----------
    catalog : dict[int, AstroqueryService]
        Dictionary of Astroquery services replciating a catalog
    n : int
        Number of entries in catalog

    Methods
    -------
    create(self, *, name: str, config: str)
        Create a service and add it to the catalog

    """

    catalog: dict[int, AstroqueryService]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog
        """
        self.catalog = {}
        self.n = 0

    def create_service(self, *, name: str, config: dict[str, Any]) -> AstroqueryService:
        """
        Create a new astroquery service.

        Parameters
        ----------
        name : str
            Name of astroquery service
        config : dict[str, Any]
            Json to be deserialized containing config options

        Returns
        -------
        service : AstroqueryService
            Astroquery service that was added
        """
        service = AstroqueryService(service_id=self.n, name=name, config=config)
        self.catalog[self.n] = service
        self.n += 1

        return service

    def get_service(self, *, service_id: int) -> AstroqueryService | None:
        """
        Get a service by id number

        Parameters
        ----------
        service_id : int
            ID of service to get

        Returns
        -------
        self.catalog.get(service_id, None) : AstroqueryService | None
            Service corresponding to service_id. Returns None if service not found
        """
        return self.catalog.get(service_id, None)

    def get_service_name(self, *, name: str) -> list[AstroqueryService] | None:
        """
        Get a service by name

        Parameters
        ----------
        name : str
            Name of service to get

        Returns
        -------
         : list[AstroqueryService] | None
            List of services corresponding to service_id. Returns None if service not found
        """
        service = [
            self.catalog[service_id]
            for service_id in self.catalog
            if self.catalog[service_id].name == name
        ]

        if len(service) == 0:
            service = None

        return service

    def update_service(
        self, *, service_id: int, name: str | None, config: dict[str, Any] | None
    ) -> AstroqueryService:
        """
        Update a service by id

        Parameters
        ----------
        service_id : int
            ID of service to be updated
        name : str | None, Default: None
            Name of source
        config : dict[str, Any] | None, Default: None
            Json to be deserialized containing config options

        Returns
        -------
        new : AstroqueryService
            Service that has been updated
        """
        current = self.get_service(service_id=service_id)

        if current is None:
            return None

        new = AstroqueryService(
            service_id=current.service_id,
            name=current.name if name is None else name,
            config=current.config if config is None else config,
        )

        self.catalog[service_id] = new

        return new

    def delete_service(self, *, service_id: int):
        """
        Delete service by id

        Parameters
        ----------
        service_id : int
            ID of service to be deleted

        Returns
        -------
        None
        """
        check = self.catalog.pop(service_id, None)
        if check is not None:
            self.n -= 1


class EphemClient(EphemClientBase):
    """
    Mock ephemeris client.

    Attributes
    ----------
    catalog : dict[int, SolarSystemObject]
        Dictionary of solar system sources replicating a catalog
    n : int
        Number of entries in catalog

    Methods
    -------
    create_ephem(self,*,sso_id: int,MPC_id: int | None,name: str,time: Time,position: ICRS,flux: Quantity | None = None,)
        Create a single ephemera point for solar system source.
    get_ephem(self, *, ephem_id: int)
        Get a single ephem point.
    get_ephem_points(self, *, sso_id: int, t_min: Time, t_max: Time)
        Get all ephem points for a given source in a given time range. Note this takes sso_id instead of passing a SolarSystemObject.
    update_ephem(self,*,ephem_id: int,sso_id: int | None,MPC_id: int | None,name: str | None,time: Time | None,position: ICRS | None,flux: Quantity | None,)
        Update a single ephem point.
    delete_ephem(self, *, ephem_id: int)
        Delete a single ephem point.
    """

    catalog: dict[int, AstroqueryService]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog
        """
        self.catalog = {}
        self.n = 0

    def create_ephem(
        self,
        *,
        sso_id: int,
        MPC_id: int,
        name: str,
        time: Time,
        position: ICRS,
        flux: Quantity | None = None,
    ) -> RegisteredMovingSource:
        """
        Create a single ephem point associated with a SSO.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of associated SSO (sso_id).
        MPC_id : int
            Minor Planet Center ID of SSO.
        name : str
            Name of SSO
        time : Time
            Time of ephemeris
        position : ICRS
            Position of ephemeris.
        flux : Quantity | None, default=None
            Flux at ephemeris.

        Returns
        -------
        ephem : RegisteredMovingSource
            Ephemeris point that was added.
        """
        ephem = RegisteredMovingSource(
            ephem_id=self.n,
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time,
            position=position,
            flux=flux,
        )
        self.catalog[self.n] = ephem
        self.n += 1

        return ephem

    def get_ephem(self, *, ephem_id: int) -> RegisteredMovingSource | None:
        """
        Get an ephem point by ID.
        Returns None if ephem not found.

        Parameters
        ----------
        ephem_id : int
            Internal SO ID of ephem point.

        Returns
        -------
        self.catalog.get(ephem_id, None) : RegisteredMovingSource | None
            Get requested ephem.
        """

        return self.catalog.get(ephem_id, None)

    def get_ephem_points(
        self, *, sso_id: int, t_min: Time, t_max: Time
    ) -> list[RegisteredMovingSource]:
        """
        Get all ephem points for a given source in a given time range.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of source for which to get ephemeris points
        t_min : Time
            Minimum time
        t_max : Time
            Maximum time

        Returns
        -------
        list[RegisteredMovingSource]
            List of requested ephemeris points

        """
        ephems = filter(
            lambda x: (x.sso_id == sso_id) and (t_min <= x.time <= t_max),
            self.catalog.values(),
        )
        return list(ephems)

    def update_ephem(
        self,
        *,
        ephem_id: int,
        sso_id: int | None,
        MPC_id: int | None,
        name: str | None,
        time: Time | None,
        position: ICRS | None,
        flux: Quantity | None,
    ) -> RegisteredMovingSource | None:
        """
        Update a solar system ephem.
        Returns None if ephem not found.

        Parameters
        ----------
        ephem_id : int
            Internal SO ID of ephem point.
        sso_id : int
            Internal SO ID of associated SSO.
        MPC_id : int
            Minor Planet Center ID of associated SSO.
        name : str
            Name of associated SSO.
        time : Time
            Time of ephem.
        flux : Quantity | None, default=None
            Flux at ephemeris.

        Returns
        -------
        new : RegisteredMovingSource | None
            Ephemeris point that was updated.
        """
        current = self.get_ephem(ephem_id=ephem_id)

        if current is None:
            return None

        new = RegisteredMovingSource(
            ephem_id=current.ephem_id,
            sso_id=current.sso_id if sso_id is None else sso_id,
            MPC_id=current.MPC_id if MPC_id is None else MPC_id,
            name=current.name if name is None else name,
            time=current.time if time is None else time,
            position=current.position if position is None else position,
            flux=current.flux if flux is None else flux,
        )

        self.catalog[ephem_id] = new

        return new

    def delete_ephem(self, *, ephem_id: int) -> None:
        """
        Delete an ephem point by ID.

        Parameters
        ----------
        ephem_id : int
            Internal SO ID of ephem point.

        Returns
        -------
        None
        """
        check = self.catalog.pop(ephem_id, None)
        if check is not None:
            self.n -= 1


class SolarSystemClient(SolarSystemClientBase):
    """
    Mock solar system client for testing.

    Attributes
    ----------
    catalog : dict[int, SolarSystemObject]
        Dictionary of solar system sources replicating a catalog
    n : int
        Number of entries in catalog

    Methods
    -------
    create_sso(self, *, name: str, MPC_id: int | None)
        Create a solar system source.
    get_sso(self, *, sso_id: int)
        Get a solar system source.
    get_sso_name(self, *, name: str)
        Get a solar system source by name.
    get_sso_MPC_id(self, *, MPC_id: int)
        Get a solar system source by MPC ID.
    update_sso(self, *, sso_id: int, name: str | None, MPC_id: int | None)
        Update a solar system source.
    delete_sso(sefl, *, sso_id: int)
        Delete a solar system source.
    """

    catalog: dict[int, SolarSystemObject]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog.
        """
        self.catalog = {}
        self.n = 0

    def create_sso(self, *, name: str, MPC_id: int | None) -> SolarSystemObject:
        """
        Create a new solar system source.

        Parameters
        ----------
        name : str
            Name of source
        MPC_id : int
            Minor Planet Center ID of source

        Returns
        -------
        solar_source : SolarSystemObject
            Solar system source that was added.
        """
        solar_source = SolarSystemObject(sso_id=self.n, name=name, MPC_id=MPC_id)
        self.catalog[self.n] = solar_source
        self.n += 1

        return solar_source

    def get_sso(self, *, sso_id: int) -> SolarSystemObject | None:
        """
        Get a solar system source.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of solar system source

        Returns
        -------
        self.catalog.get(sso_id, None) : SolarSytemSource
            Requested solar system source.
        """
        return self.catalog.get(sso_id, None)

    def get_sso_name(self, *, name: str) -> list[SolarSystemObject] | None:
        """
        Get a solar system source by name.

        Parameters
        ----------
        name : str
            Name of solar system source

        Returns
        -------
        solars : list[SolarSystemObject] | None
            Requested solar system source.
        """
        solars = [
            self.catalog[id] for id in self.catalog if self.catalog[id].name == name
        ]

        if len(solars) == 0:
            solars = None

        return solars

    def get_sso_MPC_id(self, *, MPC_id: int) -> list[SolarSystemObject] | None:
        """
        Get a solar system source by Minor Planet Center ID.

        Parameters
        ----------
        MPC_id : int
            Minor planet center ID of source

        Returns
        -------
        solars : list[SolarSystemObject] | None
            List of sources with requested MPC ID
        """
        solars = [
            self.catalog[id] for id in self.catalog if self.catalog[id].MPC_id == MPC_id
        ]

        if len(solars) == 0:
            solars = None

        return solars

    def update_sso(
        self, *, sso_id: int, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update a solar system source by ID.
        If a variable is None, dont update it.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of solar system source
        name : str | None
            Name of source
        MPC_id : int | None
            Minor planet center ID of source

        Returns
        -------
        new : SolarSystemObject | None
            Updated solar system source
        """
        current = self.get_sso(sso_id=sso_id)

        if current is None:
            return None

        new = SolarSystemObject(
            sso_id=current.sso_id,
            name=current.name if name is None else name,
            MPC_id=current.MPC_id if MPC_id is None else MPC_id,
        )

        self.catalog[sso_id] = new

        return new

    def delete_sso(self, *, sso_id: int) -> None:
        """
        Delete solar system source by ID.

        Parameters:
        -----------
        sso_id : int
            Internal SO ID of source

        Returns
        -------
        None
        """
        check = self.catalog.pop(sso_id, None)
        if check is not None:
            self.n -= 1


class SourceGenerator(SourceGeneratorBase):
    def __init__(
        self,
        source: RegisteredFixedSource | SolarSystemObject,
        t_min: Time,
        t_max: Time,
        client: ClientBase,
    ):
        self.source = source
        self.t_min = t_min
        self.t_max = t_max
        self.interp = None
        self.client = client

    def init_interp(self) -> None:
        """
        Initialize the interpolator object.
        If our source type is a RegisteredFixedSource, then
        interp will always just return the same ra/dec/flux
        (Recall that the RegisteredFixedSource.flux is
        a fixed estimate and not the light curve). If
        the soure type is SolarSystemObject, then linear
        interp ra/dec/flux over the requested time range.

        Returns
        -------
        None
        """
        if type(self.source) is RegisteredFixedSource:
            self.ra_unit = self.source.position.ra.unit
            self.dec_unit = self.source.position.dec.unit
            self.flux_unit = self.source.flux.unit
            self.interp = lambda _: (
                self.source.position.ra.value,
                self.source.position.dec.value,
                self.source.flux.value,
            )

        elif type(self.source) is SolarSystemObject:
            # For simplicity just do linear interpolation between endpoints.
            # In real implementation would want to use all ephem points and do something more sophisticated.
            # Also in real implementation would want to query ephem points from database rather than having them passed in.
            ephems = self.client.get_ephem_points(
                sso_id=self.source.sso_id, t_min=self.t_min, t_max=self.t_max
            )
            x = np.zeros(len(ephems))
            y = np.zeros((len(ephems), 3))
            for i, ephem in enumerate(ephems):
                x[i] = ephem.time.unix
                y[i] = (
                    ephem.position.ra.value,
                    ephem.position.dec.value,
                    ephem.flux.value,
                )

            self.ra_unit = ephem.position.ra.unit  # This assumes all ephem points have same units but this should probably be enforced upstream anyway.
            self.dec_unit = ephem.position.dec.unit
            self.flux_unit = ephem.flux.unit
            self.interp = make_interp_spline(x, y, k=1)

    def at_time(self, *, t: Time) -> tuple[ICRS, Quantity]:
        """
        Get the position and flux of the source at a given time.

        Parameters
        ----------
        time : Time
            Time at which to get position and flux

        Returns
        -------
        position : ICRS
            Position of source at given time
        flux : Quantity
            Flux of source at given time

        Raises
        ------
        RuntimeError
            If interp is not initialized. Call init_interp() first.
        ValueError
            If time is out of range for source generator
        """
        if self.interp is None:
            raise RuntimeError(
                "Interpolator not initialized. Call init_interp() first."
            )
        if t < self.t_min or t > self.t_max:
            raise ValueError("Time out of range for source generator")
        ra, dec, flux = self.interp(t.unix)
        position = ICRS(ra=ra * self.ra_unit, dec=dec * self.dec_unit)
        flux = flux * self.flux_unit

        return (position, flux)
