"""
Uses a local dictionary to implement the core.
"""

from importlib import import_module
from typing import Any

from astroquery.query import BaseVOQuery

from socat.database import AstroqueryService, ExtragalacticSource

from .core import AstroqueryClientBase, ClientBase


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
    create(self, *, ra: float, dec: float)
        Create a source and add it to the catalog
    create_name(self, *, name: str, service_name: str)
        Create a source by name using astroquery and add it to the catalog
    get_box(self, *, ra_min: float, ra_max: float, dec_min: float, dec_max: float)
        Get sources within box
    get_source(self, *, id: int)
        Get a source by id
    update_source(self, *, id: int, ra: float | None = None, dec: float | None = None)
        Update source by id
    delete_source(self, *, id: int)
        Delete source by id
    """

    catalog: dict[int, ExtragalacticSource]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog
        """
        self.catalog = {}
        self.n = 0

    def create(
        self, *, ra: float, dec: float, flux: float | None = None, name: str | None = None
    ) -> ExtragalacticSource:
        """
        Create a new source and add it to the catalog.

        Parameters
        ----------
        ra : float
            RA of source
        dec : float
            Dec of source
        name : str | None, Default: None
            Name of source

        Returns
        -------
        source : ExtragalacticSource
            Extragalactic Source that was added
        """
        source = ExtragalacticSource(id=self.n, ra=ra, dec=dec, name=name)
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
        source = ExtragalacticSource(
            id=self.n, ra=result_dict["ra"], dec=result_dict["dec"], name=name, flux=result_dict.get("flux", None)
        )
        self.catalog[self.n] = source
        self.n += 1

        return source

    def get_box(
        self, *, ra_min: float, ra_max: float, dec_min: float, dec_max: float
    ) -> ExtragalacticSource:
        """
        Get sources within a box.

        Parameters
        ----------
        ra_min : float
            Min ra of box
        ra_max : float
            Max ra of box
        dec_min : float
            Min dec of box
        dec_max : float
            Max dec of box

        Returns
        -------
        list(sources) : list[ExtragalacticSource]
            List of sources in box
        """
        sources = filter(
            lambda x: (ra_min <= x.ra <= ra_max) and (dec_min <= x.dec <= dec_max),
            self.catalog.values(),
        )

        return list(sources)

    def get_source(self, *, id: int) -> ExtragalacticSource | None:
        """
        Get source by id

        Parameters
        ----------
        id : int
            ID of source of interest


        Returns
        -------
        self.catalog.get(id, None) : ExtragalacticSource | None
            Source corresponding to id. Returns None if source not found
        """
        return self.catalog.get(id, None)

    def update_source(
        self,
        *,
        id: int,
        ra: float | None = None,
        dec: float | None = None,
        flux: float | None = None,
        name: str | None = None,
    ) -> ExtragalacticSource | None:
        """
        Update a source by id

        Parameters
        ----------
        ra : float | None, Default: None
            RA of source
        dec : float | None, Default: None
            Dec of source
        flux : float | None, Default: None
            Flux of source
        name : str | None, Default: None
            Name of source

        Returns
        -------
        new : ExtragalacticSource
            Source that has been updated
        """
        current = self.get_source(id=id)

        if current is None:
            return None

        new = ExtragalacticSource(
            id=current.id,
            ra=current.ra if ra is None else ra,
            dec=current.dec if dec is None else dec,
            name=current.name if name is None else name,
            flux=current.flux if flux is None else flux,
        )

        self.catalog[id] = new

        return new

    def delete_source(self, *, id: int):
        """
        Delete source by id

        Parameters
        ----------
        id : int
            ID of source to be deleted

        Returns
        -------
        None
        """
        self.catalog.pop(id, None)
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

    def create(self, *, name: str, config: dict[str, Any]) -> AstroqueryService:
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
        service = AstroqueryService(id=self.n, name=name, config=config)
        self.catalog[self.n] = service
        self.n += 1

        return service

    def get_service(self, *, id: int) -> AstroqueryService | None:
        """
        Get a service by id number

        Parameters
        ----------
        id : int
            ID of service to get

        Returns
        -------
        self.catalog.get(id, None) : AstroqueryService | None
            Service corresponding to id. Returns None if service not found
        """
        return self.catalog.get(id, None)

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
            List of services corresponding to id. Returns None if service not found
        """
        service = []
        for id in self.catalog:
            if self.catalog[id].name == name:
                service.append(self.catalog[id])

        if len(service) == 0:
            service = None

        return service

    def update_service(
        self, *, id: int, name: str | None, config: dict[str, Any] | None
    ) -> AstroqueryService:
        """
        Update a service by id

        Parameters
        ----------
        id : int
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
        current = self.get_service(id=id)

        if current is None:
            return None

        new = AstroqueryService(
            id=current.id,
            name=current.name if name is None else name,
            config=current.config if config is None else config,
        )

        self.catalog[id] = new

        return new

    def delete_service(self, *, id: int):
        """
        Delete service by id

        Parameters
        ----------
        id : int
            ID of service to be deleted

        Returns
        -------
        None
        """
        self.catalog.pop(id, None)
        self.n -= 1
