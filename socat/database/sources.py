from datetime import datetime

import astropy.units as u
import uuid7 as uuid
from astropy.coordinates import ICRS
from astropy.time import Time
from astropydantic import AstroPydanticICRS, AstroPydanticQuantity, AstroPydanticTime
from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class RegisteredSource(BaseModel):
    """
    Base class for sources.

    Attribtues
    ----------
    position : AstroPydanticICRS
        Position of source in ICRS coordinates
    flux : AstroPydanticQuantity | None
        Flux of source in mJy. Optional
    """

    position: AstroPydanticICRS
    flux: AstroPydanticQuantity | None = None


class RegisteredFixedSource(RegisteredSource):
    """
    A fixed source.

    Attributes
    ----------
    source_id : uuid.UUID
        Unique source identifier. Internal to SO
    name : str | None
        Name of source. Optional
    monitored : bool
        Whether this source is flagged for monitoring. Default False.
    pointing : bool
        Whether this source is flagged as a pointing source. Default False.
    """

    source_id: uuid.UUID
    name: str | None  # Not a foreign key
    monitored: bool = False
    pointing: bool = False


class RegisteredMovingSource(RegisteredSource):
    """
    Ephemeris points for a moving source. Note that
    these are the individual ephemeris (i.e., time/ra/dec)
    points for a source and not the source itself, which is
    SolarSystemObject. This somewhat odd naming scheme is
    because RegisteredFixedSource and RegisteredMovingSource
    are used analagously in the data base, and SolarSystemObject
    is not.

    Attributes
    ----------
    ephem_id : uuid.UUID
        ID of ephem point.
    sso_id : uuid.UUID
        Internal SO ID of source
    MPC_id : int | None
        MPC ID of source
    time : AstroPydanticTime
        Time of source ephem
    name : str
        Name of source
    """

    ephem_id: uuid.UUID
    sso_id: uuid.UUID
    MPC_id: int | None
    time: AstroPydanticTime
    name: str | None  # Foreign key to MovingSourceTable


class SolarSystemObject(BaseModel):
    """
    A Solar system source. This tracks time
    immutable attributes like name and ID.

    Attributes
    ----------
    sso_id : uuid.UUID
        Internal SO ID of source
    MPC_id : int | None
        Minor Planet Center ID of ephem.
    name : str
        Name of source
    monitored : bool
        Whether this SSO is flagged for monitoring. Default False.
    pointing : bool
        Whether this SSO is flagged as a pointing source. Default False.
    """

    sso_id: uuid.UUID
    MPC_id: int | None
    name: str
    monitored: bool = False
    pointing: bool = False


class RegisteredFixedSourceTable(SQLModel, table=True):
    """
    A fixed (i.e. fixed RA, Dec) source. This is the table model
    providing SQLModel functionality. You can export a base model, for example
    for responding to a query with using the `to_model` method.

    Attributes
    ----------
    source_id : int
        Unique source identifiers. Internal to SO
    """

    __tablename__ = "fixed_sources"

    source_id: uuid.UUID = Field(primary_key=True, default_factory=uuid.create)
    ra_deg: float = Field(nullable=False)
    dec_deg: float = Field(nullable=False)
    flux_mJy: float | None = Field(nullable=True)
    name: str = Field(index=True, nullable=True)
    monitored: bool = Field(default=False, nullable=False)
    pointing: bool = Field(default=False, nullable=False)

    def to_model(self) -> RegisteredFixedSource:
        """
        Return a fixed source from table.

        Returns
        -------
        RegisteredFixedSource : RegisteredFixedSource
            Source corresponding to this id.
        """

        flux = self.flux_mJy
        if self.flux_mJy is not None:
            flux *= u.mJy
        return RegisteredFixedSource(
            source_id=self.source_id,
            position=ICRS(ra=self.ra_deg * u.deg, dec=self.dec_deg * u.deg),
            flux=flux,
            name=self.name,
            monitored=self.monitored,
            pointing=self.pointing,
        )


class SolarSystemObjectTable(SolarSystemObject, SQLModel, table=True):
    """
    An solar system object. This is the table model
    providing SQLModel functionality. You can export a base model, for example
    for responding to a query with using the `to_model` method.
    """

    __tablename__ = "solarsystem_objects"

    sso_id: uuid.UUID = Field(primary_key=True, default_factory=uuid.create)
    MPC_id: int | None = Field(index=True, nullable=True, unique=True)
    name: str = Field(index=True, nullable=False, unique=True)
    monitored: bool = Field(default=False, nullable=False)
    pointing: bool = Field(default=False, nullable=False)

    def to_model(self) -> SolarSystemObject:
        """
        Return an Solar System object from table.

        Returns
        -------
        SolarSystemObject : SolarSystemObject
            Object corresponding to this id.
        """
        return SolarSystemObject(
            sso_id=self.sso_id,
            MPC_id=self.MPC_id,
            name=self.name,
            monitored=self.monitored,
            pointing=self.pointing,
        )


class RegisteredMovingSourceTable(SQLModel, table=True):
    """
    A Solar system source at a given time. This is the table model
    providing SQLModel functionality. You can export a base model, for example
    for responding to a query with using the `to_model` method.
    """

    __tablename__ = "moving_sources"

    ephem_id: uuid.UUID = Field(primary_key=True, default_factory=uuid.create)
    sso_id: uuid.UUID = Field(
        foreign_key="solarsystem_objects.sso_id",
        nullable=False,
        ondelete="CASCADE",
    )
    MPC_id: int | None = Field(
        foreign_key="solarsystem_objects.MPC_id",
        nullable=True,
        ondelete="CASCADE",
    )
    name: str = Field(
        foreign_key="solarsystem_objects.name",
        nullable=False,
        ondelete="CASCADE",
    )
    time: datetime
    ra_deg: float = Field(nullable=False)
    dec_deg: float = Field(nullable=False)
    flux_mJy: float | None = Field(nullable=True)

    def to_model(self) -> RegisteredMovingSource:
        """
        Return an solar system ephem from table.

        Returns
        -------
        RegisteredMovingSource : RegisteredMovingSource
            Source corresponding to this id at this time.
        """
        flux = self.flux_mJy
        if self.flux_mJy is not None:
            flux *= u.mJy
        return RegisteredMovingSource(
            ephem_id=self.ephem_id,
            sso_id=self.sso_id,
            MPC_id=self.MPC_id,
            name=self.name,
            time=Time(self.time),
            position=ICRS(ra=self.ra_deg * u.deg, dec=self.dec_deg * u.deg),
            flux=flux,
        )
