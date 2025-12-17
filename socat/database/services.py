"""
Core database tables storing information about services.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlmodel import JSON, Column, Field, SQLModel


class AstroqueryConfig(BaseModel):
    model_config = ConfigDict(strict=True)


class AstroqueryService(BaseModel):
    """An allowable astroquery service
    Attributes
    ----------
    service_id : int
        Unique service identifier
    name : str
        Name of service
    config: dict[str, Any]
        json to be deserialized to config options
    """

    service_id: int
    name: str
    config: dict[str, Any]

    # def model_post_init():
    #    config = AstroqueryConfig.model_validate_json(config)

    def __repr__(self):
        return f"AstroqueryService(id={self.service_id}, name={self.name})"  # pragma: no cover


class AstroqueryServiceTable(AstroqueryService, SQLModel, table=True):
    """An allowable astroquery service. This is the table model
    providing SQLModel functionality. You can export a base model, for example
    for responding to a query with using the `to_model` method.
    Attributes
    ----------
    id : int
        Unique service identifier
    """

    __tablename__ = "astroquery_services"

    service_id: int = Field(primary_key=True)
    name: str = Field(index=True)
    config: dict[str, Any] = Field(sa_column=Column(JSON))

    def to_model(self) -> AstroqueryService:
        """
        Return an astroquery service from table.

        Returns
        -------
        AstroqueryService : AstroqueryService
            Service corresponding to this id.
        """
        return AstroqueryService(
            service_id=self.service_id, name=self.name, config=self.config
        )
