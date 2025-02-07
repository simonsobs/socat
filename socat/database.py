"""
Core database tables storing information about sources.
"""

from pydantic import BaseModel
from pydantic import Field as PydanticField
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import Field, SQLModel

from .settings import settings


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
    """

    id: int
    ra: float = PydanticField(ge=-180.0, le=180.0)
    dec: float = PydanticField(ge=-90.0, le=90.0)

    def __repr__(self):
        return f"ExtragalacticSource(id={self.id}, ra={self.ra}, dec={self.dec})"  # pragma: no cover


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

    def to_model(self) -> ExtragalacticSource:
        """
        Return an Extragalactic source from table.

        Returns
        -------
        ExtragalaticSource : ExtragalacticSource
            Source corresponding to this id.
        """
        return ExtragalacticSource(id=self.id, ra=self.ra, dec=self.dec)


ALL_TABLES = [ExtragalacticSourceTable]

async_engine = create_async_engine(settings.database_url, echo=True, future=True)


async def get_async_session() -> AsyncSession:
    async_session = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    async with async_session() as session:
        yield session
