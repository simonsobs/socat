"""
Core database tables storing information about sources.
"""

from typing import Any

import astropy.units as u
from astropy.coordinates import ICRS
from astropydantic import AstroPydanticICRS, AstroPydanticQuantity
from pydantic import BaseModel, ConfigDict
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
    position : AstroPydanticICRS
        Position of source in ICRS coordinates
    flux_mJy : AstroPydanticQuantity | None
        Flux of source in mJy. Optional
    name : str | None
        Name of source. Optional
    """

    id: int | None = None
    position: AstroPydanticICRS
    flux: AstroPydanticQuantity | None = None
    name: str | None


class ExtragalacticSourceTable(SQLModel, table=True):
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
    ra_deg: float = Field(nullable=False)
    dec_deg: float = Field(nullable=False)
    flux_mJy: float | None = Field(nullable=True)

    name: str = Field(index=True, nullable=True)

    def to_model(self) -> ExtragalacticSource:
        """
        Return an Extragalactic source from table.

        Returns
        -------
        ExtragalaticSource : ExtragalacticSource
            Source corresponding to this id.
        """
        flux = self.flux_mJy
        if self.flux_mJy is not None:
            flux *= u.mJy
        return ExtragalacticSource(
            id=self.id,
            position=ICRS(ra=self.ra_deg * u.deg, dec=self.dec_deg * u.deg),
            flux=flux,
            name=self.name,
        )


ALL_TABLES = [ExtragalacticSourceTable, AstroqueryServiceTable]

async_engine = create_async_engine(settings.database_url, echo=True, future=True)


async def get_async_session() -> AsyncSession:
    async_session = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    async with async_session() as session:
        yield session
