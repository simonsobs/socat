"""
Uses a local dictionary to implement the core.
"""

from importlib import import_module
from typing import Any

import astropy.units as u
import uuid7 as uuid
from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity
from astroquery.query import BaseVOQuery

from socat.core import SourceGenerator
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
    get_box(self, *, lower_left: ICRS, upper_right: ICRS)
        Get sources within box
    get_source(self, *, source_id: uuid.UUID)
        Get a source by source_id
    update_source(self, *, source_id: uuid.UUID, position: ICRS | None = None, name: str | None = None, flux: Quantity | None = None)
        Update source by source_id
    delete_source(self, *, source_id: uuid.UUID)
        Delete source by source_id
    """

    catalog: dict[uuid.UUID, RegisteredFixedSource]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog
        """
        self.catalog = {}
        self.n = 0
        self._astroquery = AstroqueryClient()
        self._sso = SolarSystemClient()
        self._ephem = EphemClient()

    def create_source(
        self,
        *,
        position: ICRS,
        name: str | None = None,
        flux: Quantity | None = None,
        flags: dict | None = None,
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
        flags : dict | None, Default: None
            Dictionary of flag values. Accepted keys: 'monitored' (bool),
            'pointing' (bool).

        Returns
        -------
        source : RegisteredFixedSource
            Registered Fixed Source that was added
        """
        if flux is not None:
            flux = flux.to(u.mJy)
        if flags is None:
            flags = {}
        source = RegisteredFixedSource(
            source_id=uuid.create(),
            position=position,
            flux=flux,
            name=name,
            monitored=flags.get("monitored", False),
            pointing=flags.get("pointing", False),
        )
        self.catalog[source.source_id] = source
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
            source_id=uuid.create(),
            position=position,
            name=name,
            flux=flux,
        )
        self.catalog[source.source_id] = source
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

    def get_source(self, *, source_id: uuid.UUID) -> RegisteredFixedSource | None:
        """
        Get source by id

        Parameters
        ----------
        source_id : uuid.UUID
            ID of source of interest


        Returns
        -------
        self.catalog.get(source_id, None) : RegisteredFixedSource | None
            Source corresponding to source_id. Returns None if source not found
        """
        return self.catalog.get(source_id, None)

    def get_monitored_sources(
        self, *, t_min: Time, t_max: Time
    ) -> list[SourceGenerator]:
        """
        Get all sources flagged as monitored within the given time range.

        Parameters
        ----------
        t_min : Time
            Start of time range.
        t_max : Time
            End of time range.

        Returns
        -------
        list[SourceGenerator]
            List of SourceGenerators for all monitored sources.
        """
        fixed_sources = [x for x in self.catalog.values() if x.monitored]
        sso_sources = [
            x
            for x in self._sso.catalog.values()
            if x.monitored
            and any(
                e.sso_id == x.sso_id and t_min <= e.time <= t_max
                for e in self._ephem.catalog.values()
            )
        ]
        return [
            self.get_source_generator(source=s, t_min=t_min, t_max=t_max)
            for s in fixed_sources + sso_sources
        ]

    def get_pointing_sources(
        self, *, t_min: Time, t_max: Time
    ) -> list[SourceGenerator]:
        """
        Get all sources flagged as pointing sources within the given time range.

        Parameters
        ----------
        t_min : Time
            Start of time range.
        t_max : Time
            End of time range.

        Returns
        -------
        list[SourceGenerator]
            List of SourceGenerators for all pointing sources.
        """
        fixed_sources = [x for x in self.catalog.values() if x.pointing]
        sso_sources = [
            x
            for x in self._sso.catalog.values()
            if x.pointing
            and any(
                e.sso_id == x.sso_id and t_min <= e.time <= t_max
                for e in self._ephem.catalog.values()
            )
        ]
        return [
            self.get_source_generator(source=s, t_min=t_min, t_max=t_max)
            for s in fixed_sources + sso_sources
        ]

    def update_source(
        self,
        *,
        source_id: int,
        position: ICRS | None = None,
        name: str | None = None,
        flux: Quantity | None = None,
        flags: dict | None = None,
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
        flags : dict | None, Default: None
            Dictionary of flag values to update. Accepted keys: 'monitored' (bool),
            'pointing' (bool).

        Returns
        -------
        new : RegisteredFixedSource
            Source that has been updated
        """
        current = self.get_source(source_id=source_id)

        if current is None:
            return None

        if flags is None:
            flags = {}

        new = RegisteredFixedSource(
            source_id=current.source_id,
            position=current.position if position is None else position,
            name=current.name if name is None else name,
            flux=current.flux if flux is None else flux,
            monitored=flags.get("monitored", current.monitored),
            pointing=flags.get("pointing", current.pointing),
        )

        self.catalog[source_id] = new

        return new

    def delete_source(self, *, source_id: uuid.UUID):
        """
        Delete source by id

        Parameters
        ----------
        source_id : uuid.UUID
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
        return self._astroquery.create_service(name=name, config=config)

    def get_service(self, *, service_id: int) -> AstroqueryService | None:
        """
        Get a service by id number

        Parameters
        ----------
        service_id : int
            ID of service to get

        Returns
        -------
        self._astroquery.get_service(service_id=service_id) : AstroqueryService | None
            Service corresponding to service_id. Returns None if service not found
        """
        return self._astroquery.get_service(service_id=service_id)

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
        return self._astroquery.get_service_name(name=name)

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
        return self._astroquery.update_service(
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
        self._astroquery.delete_service(service_id=service_id)

    def create_ephem(
        self,
        *,
        sso_id: uuid.UUID,
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
        sso_id : uuid.UUID
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
        return self._ephem.create_ephem(
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time,
            position=position,
            flux=flux,
        )

    def get_ephem(self, *, ephem_id: uuid.UUID) -> RegisteredMovingSource | None:
        """
        Get an ephem point by ID.
        Returns None if ephem not found.

        Parameters
        ----------
        ephem_id : uuid.UUID
            Internal SO ID of ephem point.

        Returns
        -------
        self._ephem.get_ephem(ephem_id=ephem_id) : RegisteredMovingSource | None
            Get requested ephem.
        """
        return self._ephem.get_ephem(ephem_id=ephem_id)

    def get_ephem_points(
        self, *, sso_id: uuid.UUID, t_min: Time, t_max: Time
    ) -> list[RegisteredMovingSource]:
        """
        Get all ephem points for a given source in a given time range.

        Parameters
        ----------
        sso_id : uuid.UUID
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
        return self._ephem.get_ephem_points(sso_id=sso_id, t_min=t_min, t_max=t_max)

    def update_ephem(
        self,
        *,
        ephem_id: uuid.UUID,
        sso_id: uuid.UUID | None,
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
        ephem_id : uuid.UUID
            Internal SO ID of ephem point.
        sso_id : uuid.UUID | None
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
        return self._ephem.update_ephem(
            ephem_id=ephem_id,
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time,
            position=position,
            flux=flux,
        )

    def delete_ephem(self, *, ephem_id: uuid.UUID) -> None:
        """
        Delete an ephem point by ID.

        Parameters
        ----------
        ephem_id : uuid.UUID
            Internal SO ID of ephem point.

        Returns
        -------
        None
        """
        self._ephem.delete_ephem(ephem_id=ephem_id)

    def create_sso(
        self, *, name: str, MPC_id: int | None, flags: dict | None = None
    ) -> SolarSystemObject:
        """
        Create a new solar system source.

        Parameters
        ----------
        name : str
            Name of source
        MPC_id : int
            Minor Planet Center ID of source
        flags : dict | None, Default: None
            Dictionary of flag values. Accepted keys: 'monitored' (bool),
            'pointing' (bool).

        Returns
        -------
        solar_source : SolarSystemObject
            Solar system source that was added.
        """
        return self._sso.create_sso(name=name, MPC_id=MPC_id, flags=flags)

    def get_sso(self, *, sso_id: uuid.UUID) -> SolarSystemObject | None:
        """
        Get a solar system source.

        Parameters
        ----------
        sso_id : uuid.UUID
            Internal SO ID of solar system source

        Returns
        -------
        self._sso.get_sso(sso_id=sso_id) : SolarSytemSource
            Requested solar system source.
        """
        return self._sso.get_sso(sso_id=sso_id)

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
            self._ephem.catalog.values(),
        )

        ephem_ids = {ephem.sso_id for ephem in ephems}

        sources = filter(lambda x: x.sso_id in ephem_ids, self._sso.catalog.values())

        return list(sources)

    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
    ) -> list[SourceGenerator] | None:
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
            self.get_source_generator(source=s, t_min=t_min, t_max=t_max)
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
        return self._sso.get_sso_name(name=name)

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
        return self._sso.get_sso_MPC_id(MPC_id=MPC_id)

    def update_sso(
        self, *, sso_id: uuid.UUID, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update a solar system source by ID.
        If a variable is None, dont update it.

        Parameters
        ----------
        sso_id : uuid.UUID
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
        return self._sso.update_sso(sso_id=sso_id, name=name, MPC_id=MPC_id)

    def delete_sso(self, *, sso_id: uuid.UUID):
        """
        Delete a solar system source by ID.

        Parameters
        ----------
        sso_id : uuid.UUID
            Internal SO ID of solar system source

        Returns
        -------
        None
        """
        self._sso.delete_sso(sso_id=sso_id)

    def get_source_generator(
        self,
        *,
        source: RegisteredFixedSource | SolarSystemObject,
        t_min: Time,
        t_max: Time,
    ) -> SourceGenerator:
        """
        Get a source generator for a given source and time range.

        Parameters
        ----------
        source : RegisteredFixedSource | SolarSystemObject
            Source for which to get generator
        t_min : Time
            Minimum time for generator
        t_max : Time
            Maximum time for generator

        Returns
        -------
        SourceGenerator
            Source generator for given source and time range.
        """
        ephems = None
        if type(source) is SolarSystemObject:
            ephems = self.get_ephem_points(
                sso_id=source.sso_id, t_min=t_min, t_max=t_max
            )

        return SourceGenerator(source=source, ephems=ephems)


class AstroqueryClient(AstroqueryClientBase):
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

    catalog: dict[uuid.UUID, AstroqueryService]
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
        service = AstroqueryService(service_id=uuid.create(), name=name, config=config)
        self.catalog[service.service_id] = service
        self.n += 1

        return service

    def get_service(self, *, service_id: uuid.UUID) -> AstroqueryService | None:
        """
        Get a service by id number

        Parameters
        ----------
        service_id : uuid.UUID
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
        self, *, service_id: uuid.UUID, name: str | None, config: dict[str, Any] | None
    ) -> AstroqueryService:
        """
        Update a service by id

        Parameters
        ----------
        service_id : uuid.UUID
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

    def delete_service(self, *, service_id: uuid.UUID):
        """
        Delete service by id

        Parameters
        ----------
        service_id : uuid.UUID
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
    catalog : dict[uuid.UUID, SolarSystemObject]
        Dictionary of solar system sources replicating a catalog
    n : int
        Number of entries in catalog

    Methods
    -------
    create_ephem(self,*,sso_id: uuid.UUID,MPC_id: int | None,name: str,time: Time,position: ICRS,flux: Quantity | None = None,)
        Create a single ephemera point for solar system source.
    get_ephem(self, *, ephem_id: uuid.UUID)
        Get a single ephem point.
    get_ephem_points(self, *, sso_id: uuid.UUID, t_min: Time, t_max: Time)
        Get all ephem points for a given source in a given time range. Note this takes sso_id instead of passing a SolarSystemObject.
    update_ephem(self,*,ephem_id: uuid.UUID,sso_id: uuid.UUID | None,MPC_id: int | None,name: str | None,time: Time | None,position: ICRS | None,flux: Quantity | None,)
        Update a single ephem point.
    delete_ephem(self, *, ephem_id: uuid.UUID)
        Delete a single ephem point.
    """

    catalog: dict[uuid.UUID, AstroqueryService]
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
        sso_id: uuid.UUID,
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
        sso_id : uuid.UUID
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
            ephem_id=uuid.create(),
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time,
            position=position,
            flux=flux,
        )
        self.catalog[ephem.ephem_id] = ephem
        self.n += 1

        return ephem

    def get_ephem(self, *, ephem_id: uuid.UUID) -> RegisteredMovingSource | None:
        """
        Get an ephem point by ID.
        Returns None if ephem not found.

        Parameters
        ----------
        ephem_id : uuid.UUID
            Internal SO ID of ephem point.

        Returns
        -------
        self.catalog.get(ephem_id, None) : RegisteredMovingSource | None
            Get requested ephem.
        """

        return self.catalog.get(ephem_id, None)

    def get_ephem_points(
        self, *, sso_id: uuid.UUID, t_min: Time, t_max: Time
    ) -> list[RegisteredMovingSource]:
        """
        Get all ephem points for a given source in a given time range.

        Parameters
        ----------
        sso_id : uuid.UUID
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
        ephem_id: uuid.UUID,
        sso_id: uuid.UUID | None,
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
        ephem_id : uuid.UUID
            Internal SO ID of ephem point.
        sso_id : uuid.UUID | None
            Internal SO ID of associated SSO.
        MPC_id : int | None
            Minor Planet Center ID of associated SSO.
        name : str | None
            Name of associated SSO.
        time : Time | None
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

    def delete_ephem(self, *, ephem_id: uuid.UUID) -> None:
        """
        Delete an ephem point by ID.

        Parameters
        ----------
        ephem_id : uuid.UUID
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
    get_sso(self, *, sso_id: uuid.UUID)
        Get a solar system source.
    get_sso_name(self, *, name: str)
        Get a solar system source by name.
    get_sso_MPC_id(self, *, MPC_id: int)
        Get a solar system source by MPC ID.
    update_sso(self, *, sso_id: uuid.UUID, name: str | None, MPC_id: int | None)
        Update a solar system source.
    delete_sso(self, *, sso_id: uuid.UUID)
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

    def create_sso(
        self, *, name: str, MPC_id: int | None, flags: dict | None = None
    ) -> SolarSystemObject:
        """
        Create a new solar system source.

        Parameters
        ----------
        name : str
            Name of source
        MPC_id : int
            Minor Planet Center ID of source
        flags : dict | None, Default: None
            Dictionary of flag values. Accepted keys: 'monitored' (bool),
            'pointing' (bool).

        Returns
        -------
        solar_source : SolarSystemObject
            Solar system source that was added.
        """
        if flags is None:
            flags = {}
        solar_source = SolarSystemObject(
            sso_id=self.n,
            name=name,
            MPC_id=MPC_id,
            monitored=flags.get("monitored", False),
            pointing=flags.get("pointing", False),
        )
        self.catalog[self.n] = solar_source
        self.n += 1

        return solar_source

    def get_sso(self, *, sso_id: uuid.UUID) -> SolarSystemObject | None:
        """
        Get a solar system source.

        Parameters
        ----------
        sso_id : uuid.UUID
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
        self, *, sso_id: uuid.UUID, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update a solar system source by ID.
        If a variable is None, dont update it.

        Parameters
        ----------
        sso_id : uuid.UUID
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

    def delete_sso(self, *, sso_id: uuid.UUID) -> None:
        """
        Delete solar system source by ID.

        Parameters:
        -----------
        sso_id : uuid.UUID
            Internal SO ID of source

        Returns
        -------
        None
        """
        check = self.catalog.pop(sso_id, None)
        if check is not None:
            self.n -= 1
