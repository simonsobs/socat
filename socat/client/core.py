"""
Abstract client class that must be implemented by both the mock and the 'real'
Note none of the returns should ever be actually returned so no-covered
client.
"""

from abc import ABC, abstractmethod
from typing import Any

import uuid7 as uuid
from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity

from socat.core import SourceGenerator
from socat.database import (
    AstroqueryService,
    RegisteredFixedSource,
    RegisteredMovingSource,
    SolarSystemObject,
)


class ClientBase(ABC):
    @abstractmethod
    def create_source(
        self, *, position: ICRS, name: str | None = None, flux: Quantity | None = None
    ) -> RegisteredFixedSource:
        """
        Create a new source in the catlaog.
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
    def get_source(self, *, source_id: uuid.UUID) -> RegisteredFixedSource | None:
        """
        Get information about a specific source. If the source is not found, we return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def get_forced_photometry_sources(
        self, *, minimum_flux: Quantity
    ) -> list[RegisteredFixedSource]:
        """
        Get all sources that are used for forced photometry based on a minimum flux.
        """
        return []  # pragma: no cover

    @abstractmethod
    def update_source(
        self,
        *,
        source_id: uuid.UUID,
        position: ICRS | None = None,
        name: str | None = None,
        flux: Quantity | None = None,
    ) -> RegisteredFixedSource | None:
        """
        Update a source. If the source is updated, return its new value. Else, return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_source(self, *, source_id: uuid.UUID) -> None:
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
    def get_service(self, *, service_id: uuid.UUID) -> AstroqueryService | None:
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
        service_id: uuid.UUID,
        name: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> AstroqueryService | None:
        """
        Update a service. If the service is updated, return its new value. Else, return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_service(self, *, service_id: uuid.UUID) -> None:
        """
        Delete a service from the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def create_ephem(
        self,
        *,
        sso_id: uuid.UUID,
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
    def get_ephem(self, *, ephem_id: uuid.UUID) -> RegisteredMovingSource | None:
        """
        Get a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_ephem_points(
        self,
        *,
        sso_id: uuid.UUID,
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
        ephem_id: uuid.UUID,
        sso_id: uuid.UUID | None,
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
    def delete_ephem(self, *, ephem_id: uuid.UUID) -> None:
        """
        Delete a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def create_sso(self, *, name: str, MPC_id: int | None) -> SolarSystemObject:
        """
        Create a new solar system source in the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def get_sso(self, *, sso_id: uuid.UUID) -> SolarSystemObject | None:
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
        self, *, sso_id: uuid.UUID, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update information about a solar system source.
        """
        return []  #  pragma: no cover

    @abstractmethod
    def delete_sso(self, *, sso_id: uuid.UUID) -> None:
        """
        Delete solar system source.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_source_generator(
        self,
        *,
        source: RegisteredFixedSource | SolarSystemObject,
        t_min: Time,
        t_max: Time,
    ) -> SourceGenerator:
        """
        Get a source generator for a given source and time range.
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
    def get_service(self, *, service_id: uuid.UUID) -> AstroqueryService | None:
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
        service_id: uuid.UUID,
        name: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> AstroqueryService | None:
        """
        Update a service. If the service is updated, return its new value. Else, return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_service(self, *, service_id: uuid.UUID) -> None:
        """
        Delete a service from the catalog.
        """
        return  # pragma: no cover


class EphemClientBase(ABC):
    @abstractmethod
    def create_ephem(
        self,
        *,
        sso_id: uuid.UUID,
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
    def get_ephem(self, *, ephem_id: uuid.UUID) -> RegisteredMovingSource | None:
        """
        Get a single ephem point.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_ephem_points(
        self,
        *,
        sso_id: uuid.UUID,
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
        ephem_id: uuid.UUID,
        sso_id: uuid.UUID | None,
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
    def delete_ephem(self, *, ephem_id: uuid.UUID) -> None:
        """
        Delete a single ephem point.
        """
        return []  # pragma: no cover


class SolarSystemClientBase(ABC):
    @abstractmethod
    def create_sso(self, *, name: str, MPC_id: int | None) -> SolarSystemObject:
        """
        Create a new solar system source in the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def get_sso(self, *, sso_id: uuid.UUID) -> SolarSystemObject | None:
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
        self, *, sso_id: uuid.UUID, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        """
        Update information about a solar system source.
        """
        return []  #  pragma: no cover

    @abstractmethod
    def delete_sso(self, *, sso_id: uuid.UUID) -> None:
        """
        Delete solar system source.
        """
        return []  # pragma: no cover
