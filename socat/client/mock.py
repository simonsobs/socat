"""
Uses a local dictionary to implement the core.
"""

from importlib import import_module
from typing import Any

import astropy.units as u
from astropy.coordinates import ICRS
from astropy.units import Quantity
from astroquery.query import BaseVOQuery

from socat.database import (
    AstroqueryService,
    ExtragalacticSource,
    SolarSystemEphem,
    SolarSystemSource,
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
    catalog : dict[int, ExtragalacticSource]
        Dictionary of Extragalactic sources replciating a catalog
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

    catalog: dict[int, ExtragalacticSource]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog
        """
        self.catalog = {}
        self.n = 0

    def create_source(
        self, *, position: ICRS, name: str | None = None, flux: Quantity | None = None
    ) -> ExtragalacticSource:
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
        source : ExtragalacticSource
            Extragalactic Source that was added
        """
        if flux is not None:
            flux = flux.to(u.mJy)
        source = ExtragalacticSource(
            source_id=self.n,
            position=position,
            flux=flux,
            name=name,
        )
        self.catalog[self.n] = source
        self.n += 1

        return source

    def create_name(self, *, name: str, astroquery_service: str) -> ExtragalacticSource:
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
        source : ExtragalacticSource
            Extragalactic Source that was added
        """

        service: BaseVOQuery = getattr(
            import_module(f"astroquery.{astroquery_service.lower()}"),
            astroquery_service,
        )

        requested_params = ["ra", "dec"]

        result_table = service.query_object(name)
        result_table["ra"].convert_unit_to("deg")
        result_table["dec"].convert_unit_to("deg")
        if "flux" in result_table.keys():
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
        if flux is not None:
            flux *= u.mJy
        source = ExtragalacticSource(
            source_id=self.n,
            position=position,
            name=name,
            flux=flux,
        )
        self.catalog[self.n] = source
        self.n += 1

        return source

    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
    ) -> ExtragalacticSource:
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
        list(sources) : list[ExtragalacticSource]
            List of sources in box
        """
        ra_min = lower_left.ra.value
        dec_min = lower_left.dec.value
        ra_max = upper_right.ra.value
        dec_max = upper_right.dec.value
        sources = filter(
            lambda x: (ra_min <= x.position.ra.value <= ra_max)
            and (dec_min <= x.position.dec.value <= dec_max),
            self.catalog.values(),
        )

        return list(sources)

    def get_source(self, *, source_id: int) -> ExtragalacticSource | None:
        """
        Get source by id

        Parameters
        ----------
        source_id : int
            ID of source of interest


        Returns
        -------
        self.catalog.get(source_id, None) : ExtragalacticSource | None
            Source corresponding to source_id. Returns None if source not found
        """
        return self.catalog.get(source_id, None)

    def get_forced_photometry_sources(
        self, *, minimum_flux: Quantity
    ) -> list[ExtragalacticSource]:
        """
        Get all sources that are used for forced photometry based on a minimum flux.

        Parameters
        ----------
        minimum_flux : Quantity
            Minimum flux for source to be included

        Returns
        -------
        sources : iterable[ExtragalacticSource]
            List of sources with flux greater than minimum_flux
        """
        sources = filter(
            lambda x: x.flux is not None and x.flux >= minimum_flux,
            self.catalog.values(),
        )

        return sources

    def update_source(
        self,
        *,
        source_id: int,
        position: ICRS | None = None,
        name: str | None = None,
        flux: Quantity | None = None,
    ) -> ExtragalacticSource | None:
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
        new : ExtragalacticSource
            Source that has been updated
        """
        current = self.get_source(source_id=source_id)

        if current is None:
            return None

        new = ExtragalacticSource(
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
        service = []
        for service_id in self.catalog:
            if self.catalog[service_id].name == name:
                service.append(self.catalog[service_id])

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


class SolarSystemClient(SolarSystemClientBase):
    """
    Mock solar system client for testing.

    Attributes
    ----------
    catalog : dict[int, SolarSystemSource]
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

    catalog: dict[int, SolarSystemSource]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog.
        """
        self.catalog = {}
        self.n = 0

    def create_sso(self, *, name: str, MPC_id: int | None) -> SolarSystemSource:
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
        solar_source : SolarSystemSource
            Solar system source that was added.
        """
        solar_source = SolarSystemSource(sso_id=self.n, name=name, MPC_id=MPC_id)
        self.catalog[self.n] = solar_source
        self.n += 1

        return solar_source

    def get_sso(self, *, sso_id: int) -> SolarSystemSource | None:
        """
        Get a solar system source.

        Parameters
        ----------
        sso_id : int
            Internal SO ID of solar system source

        Returns
        -------
        solar_source : SolarSytemSource
            Reuqested solar system source.
        """
        solar_source = self.catalog.get(sso_id, None)

        return solar_source

    def get_sso_name(self, *, name: str) -> list[SolarSystemSource] | None:
        """
        Get a solar system source by name.

        Parameters
        ----------
        name : str
            Name of solar system source

        Returns
        -------
        solars : list[SolarSystemSource] | None
            Requested solar system source.
        """
        solars = []
        for id in self.catalog:
            if self.catalog[id].name == name:
                solars.append(self.catalog[id])

        if len(solars) == 0:
            solars = None

        return solars

    def get_sso_MPC_id(self, *, MPC_id: int) -> list[SolarSystemSource] | None:
        """
        Get a solar system source by Minor Planet Center ID.

        Parameters
        ----------
        MPC_id : int
            Minor planet center ID of source

        Returns
        -------
        solars : list[SolarSystemSource] | None
            List of sources with requested MPC ID
        """
        solars = []
        for id in self.catalog:
            if self.catalog[id].MPC_id == MPC_id:
                solars.append(self.catalog[id])

        if len(solars) == 0:
            solars = None

        return solars

    def update_sso(
        self, *, sso_id: int, name: str | None, MPC_id: int | None
    ) -> SolarSystemSource | None:
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
        new : SolarSystemSource | None
            Updated solar system source
        """
        current = self.get_sso(sso_id=sso_id)

        if current is None:
            return None

        new = SolarSystemSource(
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


class EphemClient(EphemClientBase):
    """
    Mock ephemeris client.

    Attributes
    ----------
    catalog : dict[int, SolarSystemSource]
        Dictionary of solar system sources replicating a catalog
    n : int
        Number of entries in catalog

    Methods
    -------
    create_ephem(self,*,sso_id: int,MPC_id: int | None,name: str,time: int,position: ICRS,flux: Quantity | None = None,)
        Create a single ephemera point for solar system source.
    get_ephem(self, *, ephem_id: int)
        Get a single ephem point.
    update_ephem(self,*,ephem_id: int,sso_id: int | None,MPC_id: int | None,name: str | None,time: int | None,position: ICRS | None,flux: Quantity | None,)
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
        time: int,
        position: ICRS,
        flux: Quantity | None = None,
    ) -> SolarSystemEphem:
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
        time : int
            Time of ephemeris
        position : ICRS
            Position of ephemeris.
        flux : Quantity | None, default=None
            Flux at ephemeris.

        Returns
        -------
        ephem : SolarSystemEphem
            Ephemeris point that was added.
        """
        ephem = SolarSystemEphem(
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

    def get_ephem(self, *, ephem_id: int) -> SolarSystemEphem | None:
        """
        Get an ephem point by ID.
        Returns None if ephem not found.

        Parameters
        ----------
        ephem_id : int
            Internal SO ID of ephem point.

        Returns
        -------
        ephem : SolarSystemEphem | None
            Get requested ephem.
        """

        ephem = self.catalog.get(ephem_id, None)

        return ephem

    def update_ephem(
        self,
        *,
        ephem_id: int,
        sso_id: int | None,
        MPC_id: int | None,
        name: str | None,
        time: int | None,
        position: ICRS | None,
        flux: Quantity | None,
    ) -> SolarSystemEphem | None:
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
        time : int
            Time of ephem.
        flux : Quantity | None, default=None
            Flux at ephemeris.

        Returns
        -------
        new : SolarSystemEphem | None
            Ephemeris point that was updated.
        """
        current = self.get_ephem(ephem_id=ephem_id)

        if current is None:
            return None

        new = SolarSystemEphem(
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
