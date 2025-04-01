"""
Abstract client class that must be implemented by both the mock and the 'real'
client.
"""

from abc import ABC, abstractmethod

from socat.database import ExtragalacticSource


class ClientBase(ABC):
    @abstractmethod
    def create(
        self, *, ra: float, dec: float, name: str | None = None
    ) -> ExtragalacticSource:
        """
        Create a new source in the catlaog.
        """
        return

    @abstractmethod
    def get_box(
        self, *, ra_min: float, ra_max: float, dec_min: float, dec_max: float
    ) -> list[ExtragalacticSource]:
        """
        Get all sources within a box on the sky.
        """
        return []

    @abstractmethod
    def get_source(self, *, id: int) -> ExtragalacticSource | None:
        """
        Get information about a specific source. If the source is not found, we return None.
        """
        return None

    @abstractmethod
    def update_source(
        self,
        *,
        id: int,
        ra: float | None = None,
        dec: float | None = None,
        name: str | None = None,
    ) -> ExtragalacticSource | None:
        """
        Update a source. If the source is updated, return its new value. Else, return None.
        """
        return None

    @abstractmethod
    def delete_source(self, *, id: int) -> None:
        """
        Delete a source from the catalog.
        """
        return None
