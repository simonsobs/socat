"""
Abstract client class that must be implemented by both the mock and the 'real'
Note none of the returns should ever be actually returned so no-covered
client.
"""

from abc import ABC, abstractmethod
from typing import Any

from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity

from socat.database import (
    AstroqueryService,
    RegisteredFixedSource,
    RegisteredMovingSource,
    SolarSystemObject,
)


class ClientBase(ABC):
    @property
    @abstractmethod
    def astroquery(self) -> "AstroqueryClientBase":
        """
        Access the astroquery-service client.
        """
        return  # pragma: no cover

    @property
    @abstractmethod
    def sso(self) -> "SolarSystemClientBase":
        """
        Access the solar-system-object client.
        """
        return  # pragma: no cover

    @property
    @abstractmethod
    def ephem(self) -> "EphemClientBase":
        """
        Access the ephemeris client.
        """
        return  # pragma: no cover

    @abstractmethod
    def create_source(
        self,
        *,
        position: ICRS,
        name: str | None = None,
        flux: Quantity | None = None,
        flags: dict | None = None,
    ) -> RegisteredFixedSource:
        """
        Create a new source in the catalog.
        flags dict keys: 'monitored' (bool), 'pointing' (bool), 'extra' (list[str]).
        """
        return  # pragma: no cover

    @abstractmethod
    def create_name(
        self, *, name: float, astroquery_service: float
    ) -> RegisteredFixedSource:
        """
        Create a new source in the catalog by name.
        """
        return  # pragma: no cover

    @abstractmethod
    def get_box_fixed(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
    ) -> list[RegisteredFixedSource]:
        """
        Get all sources within a box on the sky.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_source(self, *, source_id: int) -> RegisteredFixedSource | None:
        """
        Get information about a specific source. If the source is not found, we return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
    ) -> list["SourceGeneratorBase"]:
        """
        Get all sources (fixed and moving) within a sky box between time bounds.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_monitored_sources(
        self,
        *,
        t_min: Time,
        t_max: Time,
    ) -> list["SourceGeneratorBase"]:
        """
        Get all monitored sources (fixed and SSOs) as SourceGenerators.
        t_min/t_max bound the ephemeris range for SSO interpolators.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_pointing_sources(
        self,
        *,
        t_min: Time,
        t_max: Time,
    ) -> list["SourceGeneratorBase"]:
        """
        Get all pointing sources (fixed and SSOs) as SourceGenerators.
        t_min/t_max bound the ephemeris range for SSO interpolators.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_flagged_sources(
        self,
        *,
        flags: list[str],
        t_min: Time,
        t_max: Time,
        combine: str = "or",
    ) -> list["SourceGeneratorBase"]:
        """
        Get sources matched against the extra list using combine mode.
        combine: 'or' any, 'and' all, 'xor' exactly one, 'xand' none.
        t_min/t_max bound the ephemeris range for SSO interpolators.
        """
        return []  # pragma: no cover

    @abstractmethod
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
        Update a source. If the source is updated, return its new value. Else, return None.
        flags dict keys present are updated; absent keys are left unchanged.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_source(self, *, source_id: int) -> None:
        """
        Delete a source from the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def create_service(self, *, name: str, config: dict[str, Any]) -> AstroqueryService:
        """
        Create a new service in the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def get_service(self, *, service_id: int) -> AstroqueryService | None:
        """
        Get information about a specific service. If the service is not found, we return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def get_service_name(self, *, name: str) -> list[AstroqueryService] | None:
        """
        Get information about a specific service by name. If the service is not found, we return None.
        """
        return []  # pragma: no cover

    @abstractmethod
    def update_service(
        self,
        *,
        service_id: int,
        name: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> AstroqueryService | None:
        """
        Update a service. If the service is updated, return its new value. Else, return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_service(self, *, service_id: int) -> None:
        """
        Delete a service from the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def create_ephem(
        self,
        *,
        sso_id: int,
        MPC_id: int | None,
        name: str,
        time: Time,
        position: ICRS,
        flux: Quantity | None = None,
    ) -> RegisteredMovingSource:
        """
        Create a single ephemera point for solar system source.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_ephem(self, *, ephem_id: int) -> RegisteredMovingSource | None:
        """
        Get a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_ephem_points(
        self,
        *,
        sso_id: int,
        t_min: Time,
        t_max: Time,
    ) -> list[RegisteredMovingSource] | None:
        """
        Get all ephem points for a given solar system source within a given time range. Note this takes sso_id instead of passing a SolarSystemObject.
        """
        return []  # pragma: no cover

    @abstractmethod
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
        Update a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def delete_ephem(self, *, ephem_id: int) -> None:
        """
        Delete a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def create_sso(
        self, *, name: str, MPC_id: int | None, flags: dict | None = None
    ) -> SolarSystemObject:
        """
        Create a new solar system source in the catalog.
        flags dict keys: 'monitored' (bool), 'pointing' (bool), 'extra' (list[str]).
        """
        return  # pragma: no cover

    @abstractmethod
    def get_sso(self, *, sso_id: int) -> SolarSystemObject | None:
        """
        Get information about a specific solar system source. If the service is not found, we return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def get_box_sso(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
    ) -> list[SolarSystemObject] | None:
        """
        Get all ssos inside a given box within a given time range.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_sso_name(self, *, name: str) -> list[SolarSystemObject] | None:
        """
        Get information about a specific solar system source by name.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_sso_MPC_id(self, *, MPC_id: int) -> list[SolarSystemObject] | None:
        """
        Get information about a solar system source by Minor Planet Center ID.
        """
        return []  # pragma: no cover

    @abstractmethod
    def update_sso(
        self, *, sso_id: int, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update information about a solar system source.
        """
        return []  #  pragma: no cover

    @abstractmethod
    def delete_sso(self, *, sso_id: int) -> None:
        """
        Delete solar system source.
        """
        return []  # pragma: no cover


class AstroqueryClientBase(ABC):
    @abstractmethod
    def create_service(self, *, name: str, config: dict[str, Any]) -> AstroqueryService:
        """
        Create a new service in the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def get_service(self, *, service_id: int) -> AstroqueryService | None:
        """
        Get information about a specific service. If the service is not found, we return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def get_service_name(self, *, name: str) -> list[AstroqueryService] | None:
        """
        Get information about a specific service by name. If the service is not found, we return None.
        """
        return []  # pragma: no cover

    @abstractmethod
    def update_service(
        self,
        *,
        service_id: int,
        name: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> AstroqueryService | None:
        """
        Update a service. If the service is updated, return its new value. Else, return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_service(self, *, service_id: int) -> None:
        """
        Delete a service from the catalog.
        """
        return  # pragma: no cover


class EphemClientBase(ABC):
    @abstractmethod
    def create_ephem(
        self,
        *,
        sso_id: int,
        MPC_id: int | None,
        name: str,
        time: Time,
        position: ICRS,
        flux: Quantity | None = None,
    ) -> RegisteredMovingSource:
        """
        Create a single ephemera point for solar system source.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_ephem(self, *, ephem_id: int) -> RegisteredMovingSource | None:
        """
        Get a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_ephem_points(
        self,
        *,
        sso_id: int,
        t_min: Time,
        t_max: Time,
    ) -> list[RegisteredMovingSource] | None:
        """
        Get all ephem points for a given solar system source within a given time range. Note this takes sso_id instead of passing a SolarSystemObject.
        """
        return []  # pragma: no cover

    @abstractmethod
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
        Update a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def delete_ephem(self, *, ephem_id: int) -> None:
        """
        Delete a single ephem point.
        """
        return []  # pragma: no cover


class SolarSystemClientBase(ABC):
    @abstractmethod
    def create_sso(
        self, *, name: str, MPC_id: int | None, flags: dict | None = None
    ) -> SolarSystemObject:
        """
        Create a new solar system source in the catalog.
        flags dict keys: 'monitored' (bool), 'pointing' (bool), 'extra' (list[str]).
        """
        return  # pragma: no cover

    @abstractmethod
    def get_sso(self, *, sso_id: int) -> SolarSystemObject | None:
        """
        Get information about a specific solar system source. If the service is not found, we return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def get_sso_name(self, *, name: str) -> list[SolarSystemObject] | None:
        """
        Get information about a specific solar system source by name.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_sso_MPC_id(self, *, MPC_id: int) -> list[SolarSystemObject] | None:
        """
        Get information about a solar system source by Minor Planet Center ID.
        """
        return []  # pragma: no cover

    @abstractmethod
    def update_sso(
        self, *, sso_id: int, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update information about a solar system source.
        """
        return []  #  pragma: no cover

    @abstractmethod
    def delete_sso(self, *, sso_id: int) -> None:
        """
        Delete solar system source.
        """
        return []  # pragma: no cover


class SourceGeneratorBase(ABC):
    """
    Base class for source generators. Note that databases have to be passed explicitly as compared to the actual implementation
    as this class doesn't know about the databases.
    """

    @property
    @abstractmethod
    def client(self) -> ClientBase:
        """
        Get the client associated with this source generator.
        """
        return  # pragma: no cover

    @abstractmethod
    def init_interp(self) -> None:
        """
        Initialize the interpolator object.
        """

        return []  # pragma: no cover

    @abstractmethod
    def at_time(self, *, time: Time) -> tuple[ICRS, Quantity]:
        """
        Get the position and flux of the source at a given time.
        """
        return []  # pragma: no cover
