"""
Core database tables storing information about sources.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import JSON, Column, Field, SQLModel

from .settings import settings


class AstroqueryConfig(BaseModel):
    model_config = ConfigDict(strict=True)


class AstroqueryService(BaseModel):
    """An allowable astroquery service
    Attributes
    ----------
    id : int
        Unique service identifier
    name : str
        Name of service
    config: dict[str, Any]
        json to be deserialized to config options
    """

    id: int
    name: str
    config: dict[str, Any]

    # def model_post_init():
    #    config = AstroqueryConfig.model_validate_json(config)

    def __repr__(self):
        return f"AstroqueryService(id={self.id}, name={self.name})"  # pragma: no cover


class AstroqueryServiceTable(AstroqueryService, SQLModel, table=True):
    """An allowable astroquery service. This is the table model
    providing SQLModel functionality. You can export a base model, for example
    for responding to a query with using the `to_model` method.
    Attributes
    ----------
    id : int
        Unique service identifier
    """

    __tablename__ = "astroquery_sources"

    id: int = Field(primary_key=True)
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
        return AstroqueryService(id=self.id, name=self.name, config=self.config)


class ExtragalacticSource(BaseModel):
    """
    An extragalactic (i.e. fixed RA, Dec) source.

    Attributes
    ----------
    id : int
        Unique source identifier. Internal to SO
    ra : float
        RA of source in degress (-180 to 180)
    dec : float
        Dec of source in degrees
    name : str | None
        Name of source. Not required
    """

    id: int
    ra: float = PydanticField(ge=-180.0, le=180.0)
    dec: float = PydanticField(ge=-90.0, le=90.0)
    name: str | None

    def __repr__(self):
        return f"ExtragalacticSource(id={self.id}, ra={self.ra}, dec={self.dec}, name={self.name})"  # pragma: no cover


class ExtragalacticSourceTable(ExtragalacticSource, SQLModel, table=True):
    """
    An extragalactic (i.e. fixed RA, Dec) source. This is the table model
    providing SQLModel functionality. You can export a base model, for example
    for responding to a query with using the `to_model` method.

    Attributes
    ----------
    id : int
        Unique source identifiers. Internal to SO
    """

    __tablename__ = "extragalactic_sources"

    id: int = Field(primary_key=True)
    name: str = Field(index=True, nullable=True)

    def to_model(self) -> ExtragalacticSource:
        """
        Return an Extragalactic source from table.

        Returns
        -------
        ExtragalaticSource : ExtragalacticSource
            Source corresponding to this id.
        """
        return ExtragalacticSource(id=self.id, ra=self.ra, dec=self.dec, name=self.name)


ALL_TABLES = [ExtragalacticSourceTable, AstroqueryServiceTable]

async_engine = create_async_engine(settings.database_url, echo=True, future=True)


async def get_async_session() -> AsyncSession:
    async_session = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    async with async_session() as session:
        yield session
