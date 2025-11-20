"""
Abstract client class that must be implemented by both the mock and the 'real'
Note none of the returns should ever be actually returned so no-covered
client.
"""

from abc import ABC, abstractmethod
from typing import Any

from astropy.coordinates import ICRS
from astropy.units import Quantity

from socat.database import AstroqueryService, ExtragalacticSource


class ClientBase(ABC):
    @abstractmethod
    def create(
        self, *, position: ICRS, name: str | None = None, flux: Quantity | None = None
    ) -> ExtragalacticSource:
        """
        Create a new source in the catlaog.
        """
        return  # pragma: no cover

    @abstractmethod
    def create_name(
        self, *, name: float, astroquery_service: float
    ) -> ExtragalacticSource:
        """
        Create a new source in the catalog by name.
        """
        return  # pragma: no cover

    @abstractmethod
    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
    ) -> list[ExtragalacticSource]:
        """
        Get all sources within a box on the sky.
        """
        return []  # pragma: no cover

    @abstractmethod
    def get_source(self, *, id: int) -> ExtragalacticSource | None:
        """
        Get information about a specific source. If the source is not found, we return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def get_forced_photometry_sources(
        self, *, minimum_flux: Quantity
    ) -> list[ExtragalacticSource]:
        """
        Get all sources that are used for forced photometry based on a minimum flux.
        """
        return []  # pragma: no cover

    @abstractmethod
    def update_source(
        self,
        *,
        id: int,
        position: ICRS | None = None,
        name: str | None = None,
        flux: Quantity | None = None,
    ) -> ExtragalacticSource | None:
        """
        Update a source. If the source is updated, return its new value. Else, return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_source(self, *, id: int) -> None:
        """
        Delete a source from the catalog.
        """
        return None  # pragma: no cover


class AstroqueryClientBase(ABC):
    @abstractmethod
    def create(self, *, name: str, config: dict[str, Any]) -> AstroqueryService:
        """
        Create a new service in the catalog.
        """
        return  # pragma: no cover

    @abstractmethod
    def get_service(self, *, id: int) -> AstroqueryService | None:
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
        id: int,
        name: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> AstroqueryService | None:
        """
        Update a service. If the service is updated, return its new value. Else, return None.
        """
        return None  # pragma: no cover

    @abstractmethod
    def delete_service(self, *, id: int) -> None:
        """
        Delete a service from the catalog.
        """
        return None  # pragma: no cover
