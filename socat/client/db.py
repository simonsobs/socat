"""
Uses a SQLAlchemy database connection to implement the client core.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

import numpy as np
from astropy.coordinates import ICRS
from astropy.time import Time
from astropy.units import Quantity
from scipy.interpolate import make_interp_spline
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from socat.database import (
    AstroqueryService,
    AstroqueryServiceTable,
    RegisteredFixedSource,
    RegisteredFixedSourceTable,
    RegisteredMovingSource,
    RegisteredMovingSourceTable,
    SolarSystemObject,
    SolarSystemObjectTable,
    statements,
)
from socat.database.session import (
    create_sync_session_factory,
    create_sync_session_interface,
)

from .core import (
    AstroqueryClientBase,
    ClientBase,
    EphemClientBase,
    SolarSystemClientBase,
    SourceGeneratorBase,
)


class Client(ClientBase):
    """
    DB-backed client implementation for fixed sources.
    """

    _get_session: Callable[[], AbstractContextManager]

    def __init__(
        self,
        *,
        db_url: str | None = None,
        engine: Engine | None = None,
        session_factory: sessionmaker | None = None,
    ):
        if session_factory is None:
            session_factory = create_sync_session_factory(
                db_url=db_url,
                engine=engine,
            )

        self._session_factory = session_factory
        self._get_session = create_sync_session_interface(
            db_url=db_url,
            engine=engine,
            session_factory=session_factory,
        )

        self._astroquery = AstorqueryClient(session_factory=session_factory)
        self._sso = SolarSystemClient(session_factory=session_factory)
        self._ephem = EphemClient(session_factory=session_factory)

    @property
    def astroquery(self) -> AstroqueryClientBase:
        return self._astroquery

    @property
    def sso(self) -> SolarSystemClientBase:
        return self._sso

    @property
    def ephem(self) -> EphemClientBase:
        return self._ephem

    def create_source(
        self,
        *,
        position: ICRS,
        name: str | None = None,
        flux: Quantity | None = None,
        monitored: bool = False,
    ) -> RegisteredFixedSource:
        if flux is not None:
            flux = flux.to_value("mJy")

        source = RegisteredFixedSourceTable(
            ra_deg=position.ra.to_value("deg"),
            dec_deg=position.dec.to_value("deg"),
            name=name,
            flux_mJy=flux,
            monitored=monitored,
        )
        with self._get_session() as session:
            session.add(source)
            session.commit()
            session.refresh(source)

        return source.to_model()

    def create_name(
        self, *, name: str, astroquery_service: str
    ) -> RegisteredFixedSource:
        position, name, flux = statements.create_name(name, astroquery_service)

        return self.create_source(position=position, name=name, flux=flux)

    def get_box_fixed(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
    ) -> list[RegisteredFixedSource]:
        with self._get_session() as session:
            sources = session.execute(
                statements.get_box_fixed(lower_left=lower_left, upper_right=upper_right)
            )

            return [s.to_model() for s in sources.scalars().all()]

    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
    ) -> list["SourceGenerator"]:
        return self._sso.get_box(
            lower_left=lower_left,
            upper_right=upper_right,
            t_min=t_min,
            t_max=t_max,
            source_cat=self,
            ephem_cat=self._ephem,
        )

    def get_source(self, *, source_id: int) -> RegisteredFixedSource | None:
        with self._get_session() as session:
            source = session.get(RegisteredFixedSourceTable, source_id)
            if source is None:
                raise ValueError(f"Source with ID {source_id} not found")
            return source.to_model()

    def get_forced_photometry_sources(
        self,
        *,
        t_min: Time,
        t_max: Time,
        minimum_flux: Quantity | None = None,
    ) -> list["SourceGenerator"]:
        with self._get_session() as session:
            fixed = session.execute(
                statements.get_forced_photometry_sources(minimum_flux=minimum_flux)
            )
            ssos = session.execute(statements.get_forced_photometry_ssos())
            all_sources = [s.to_model() for s in fixed.scalars().all()] + [
                s.to_model() for s in ssos.scalars().all()
            ]
        return [
            SourceGenerator(
                source=s,
                t_min=t_min,
                t_max=t_max,
                ephem_cat=self._ephem,
                session_factory=self._session_factory,
            )
            for s in all_sources
        ]

    def update_source(
        self,
        *,
        source_id: int,
        position: ICRS | None = None,
        name: str | None = None,
        flux: Quantity | None = None,
        monitored: bool | None = None,
    ) -> RegisteredFixedSource | None:
        with self._get_session() as session:
            session.execute(
                statements.update_source(
                    source_id=source_id,
                    position=position,
                    name=name,
                    flux=flux,
                    monitored=monitored,
                )
            )
            source = session.get(RegisteredFixedSourceTable, source_id)

            if source is None:
                raise ValueError(
                    f"Unable to update and/or find source with ID {source_id}"
                )

            model = source.to_model()

            session.commit()

            return model

    def delete_source(self, *, source_id: int) -> None:
        with self._get_session() as session:
            source = session.get(RegisteredFixedSourceTable, source_id)
            if source is None:
                return

            session.delete(source)
            session.commit()


class AstorqueryClient(AstroqueryClientBase):
    """
    DB-backed client implementation for astroquery services.
    """

    _get_session: Callable[[], AbstractContextManager]

    def __init__(
        self,
        *,
        db_url: str | None = None,
        engine: Engine | None = None,
        session_factory: sessionmaker | None = None,
    ):
        self._get_session = create_sync_session_interface(
            db_url=db_url,
            engine=engine,
            session_factory=session_factory,
        )

    def create_service(self, *, name: str, config: dict[str, Any]) -> AstroqueryService:
        service = AstroqueryServiceTable(name=name, config=config)

        with self._get_session() as session:
            session.add(service)
            session.commit()
            session.refresh(service)

            return service.to_model()

    def get_service(self, *, service_id: int) -> AstroqueryService | None:
        with self._get_session() as session:
            service = session.get(AstroqueryServiceTable, service_id)

            if service is None:
                raise ValueError(f"Service with ID {service_id} not found.")

            return service.to_model()

    def get_service_name(self, *, name: str) -> list[AstroqueryService] | None:
        with self._get_session() as session:
            services = session.execute(
                select(AstroqueryServiceTable).where(
                    AstroqueryServiceTable.name == name
                )
            )

            service_list = [s.to_model() for s in services.scalars().all()]
            if len(service_list) == 0:
                raise ValueError(f"Service with name {name} not found.")

            return service_list

    def update_service(
        self,
        *,
        service_id: int,
        name: str | None,
        config: dict[str, Any] | None,
    ) -> AstroqueryService | None:
        with self._get_session() as session:
            session.execute(
                statements.update_service(
                    service_id=service_id, name=name, config=config
                )
            )
            service = session.get(AstroqueryServiceTable, service_id)

            if service is None:
                raise ValueError(f"Source with ID {service_id} not found")

            model = service.to_model()
            session.commit()

            return model

    def delete_service(self, *, service_id: int) -> None:
        with self._get_session() as session:
            service = session.get(AstroqueryServiceTable, service_id)
            if service is None:
                return

            session.delete(service)
            session.commit()


class EphemClient(EphemClientBase):
    """
    DB-backed client implementation for moving-source ephemerides.
    """

    _get_session: Callable[[], AbstractContextManager]

    def __init__(
        self,
        *,
        db_url: str | None = None,
        engine: Engine | None = None,
        session_factory: sessionmaker | None = None,
    ):
        self._get_session = create_sync_session_interface(
            db_url=db_url,
            engine=engine,
            session_factory=session_factory,
        )

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
        flux_mJy = None if flux is None else flux.to_value("mJy")

        ephem = RegisteredMovingSourceTable(
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time.datetime,
            ra_deg=position.ra.to_value("deg"),
            dec_deg=position.dec.to_value("deg"),
            flux_mJy=flux_mJy,
        )

        with self._get_session() as session:
            session.add(ephem)
            session.commit()
            session.refresh(ephem)

            return ephem.to_model()

    def get_ephem(self, *, ephem_id: int) -> RegisteredMovingSource | None:
        with self._get_session() as session:
            ephem = session.get(RegisteredMovingSourceTable, ephem_id)

            if ephem is None:
                raise ValueError(f"Ephemeris point with ID {ephem_id} not found")

            return ephem.to_model()

    def get_ephem_points(
        self,
        *,
        sso_id: int,
        t_min: Time,
        t_max: Time,
    ) -> list[RegisteredMovingSource]:
        with self._get_session() as session:
            ephems = session.execute(
                statements.get_ephem_points(sso_id=sso_id, t_min=t_min, t_max=t_max)
            )

            return [e.to_model() for e in ephems.scalars().all()]

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
        with self._get_session() as session:
            session.execute(
                statements.update_ephem(
                    ephem_id=ephem_id,
                    sso_id=sso_id,
                    MPC_id=MPC_id,
                    name=name,
                    time=time,
                    position=position,
                    flux=flux,
                )
            )

            ephem = session.get(RegisteredMovingSourceTable, ephem_id)

            if ephem is None:
                raise ValueError(f"Ephemeris point with ID {ephem_id} not found")

            model = ephem.to_model()

            session.commit()

        return model

    def delete_ephem(self, *, ephem_id: int) -> None:
        with self._get_session() as session:
            ephem = session.get(RegisteredMovingSourceTable, ephem_id)
            if ephem is None:
                return

            session.delete(ephem)
            session.commit()


class SolarSystemClient(SolarSystemClientBase):
    """
    DB-backed client implementation for solar system objects.
    """

    _get_session: Callable[[], AbstractContextManager]

    def __init__(
        self,
        *,
        db_url: str | None = None,
        engine: Engine | None = None,
        session_factory: sessionmaker | None = None,
    ):
        if session_factory is None:
            session_factory = create_sync_session_factory(
                db_url=db_url,
                engine=engine,
            )
        self._session_factory = session_factory
        self._get_session = create_sync_session_interface(
            session_factory=session_factory,
        )

    def create_sso(
        self, *, name: str, MPC_id: int | None, monitored: bool = False
    ) -> SolarSystemObject:
        source = SolarSystemObjectTable(name=name, MPC_id=MPC_id, monitored=monitored)

        with self._get_session() as session:
            session.add(source)
            session.commit()
            session.refresh(source)

            return source.to_model()

    def get_sso(self, *, sso_id: int) -> SolarSystemObject | None:
        with self._get_session() as session:
            source = session.get(SolarSystemObjectTable, sso_id)

            if source is None:
                raise ValueError(f"Source with ID {sso_id} not found.")

            return source.to_model()

    def get_box_sso(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
        ephem_cat: EphemClient,
    ) -> list[SolarSystemObject] | None:
        with self._get_session() as session:
            sources = session.execute(
                statements.get_box_sso(
                    lower_left=lower_left,
                    upper_right=upper_right,
                    t_min=t_min,
                    t_max=t_max,
                )
            )

            return [s.to_model() for s in sources.scalars()]

    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
        t_min: Time,
        t_max: Time,
        source_cat: ClientBase,
        ephem_cat: EphemClient,
    ) -> list[SolarSystemObject | RegisteredFixedSource] | None:
        fixed_sources: list[RegisteredFixedSource] = source_cat.get_box_fixed(
            lower_left=lower_left,
            upper_right=upper_right,
        )
        sso_sources: list[SolarSystemObject] = self.get_box_sso(
            lower_left=lower_left,
            upper_right=upper_right,
            t_min=t_min,
            t_max=t_max,
            ephem_cat=ephem_cat,
        )

        return [
            SourceGenerator(
                source=s,
                t_min=t_min,
                t_max=t_max,
                ephem_cat=ephem_cat,
                session_factory=self._session_factory,
            )
            for s in fixed_sources + sso_sources
        ]

    def get_sso_name(self, *, name: str) -> list[SolarSystemObject] | None:
        with self._get_session() as session:
            sources = session.execute(
                select(SolarSystemObjectTable).where(
                    SolarSystemObjectTable.name == name
                )
            )

            source_list = [s.to_model() for s in sources.scalars().all()]
            if len(source_list) == 0:
                raise ValueError(f"Source with name {name} not found.")

            return source_list

    def get_sso_MPC_id(self, *, MPC_id: int) -> list[SolarSystemObject] | None:
        with self._get_session() as session:
            sources = session.execute(
                select(SolarSystemObjectTable).where(
                    SolarSystemObjectTable.MPC_id == MPC_id
                )
            )

            source_list = [s.to_model() for s in sources.scalars().all()]
            if len(source_list) == 0:
                raise ValueError(f"Source with MPC ID {MPC_id} not found.")

            return source_list

    def update_sso(
        self, *, sso_id: int, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        with self._get_session() as session:
            session.execute(
                statements.update_sso(sso_id=sso_id, name=name, MPC_id=MPC_id)
            )
            source = session.get(SolarSystemObjectTable, sso_id)

            if source is None:
                raise ValueError(f"Source with SSO ID {sso_id} not found")

            model = source.to_model()

            session.commit()

        return model

    def delete_sso(self, *, sso_id: int) -> None:
        with self._get_session() as session:
            source = session.get(SolarSystemObjectTable, sso_id)
            if source is None:
                return

            session.delete(source)
            session.commit()


class SourceGenerator(SourceGeneratorBase):
    _get_session: Callable[[], AbstractContextManager]

    def __init__(
        self,
        *,
        db_url: str | None = None,
        engine: Engine | None = None,
        session_factory: sessionmaker | None = None,
        source: RegisteredFixedSource | SolarSystemObject,
        t_min: Time,
        t_max: Time,
        ephem_cat: EphemClient,
    ):
        self._get_session = create_sync_session_interface(
            db_url=db_url,
            engine=engine,
            session_factory=session_factory,
        )
        self.source = source
        self.t_min = t_min
        self.t_max = t_max
        self.ephem_cat = ephem_cat
        self.interp = None

    def init_interp(self, *, ephem_cat: EphemClient) -> None:
        """
        Initialize the interpolator for this source generator. Must be called before at_time().

        Parameters
        ----------
        ephem_cat : EphemClient
            Ephemeris client to use for getting ephemeris points if source is a SolarSystemObject.

        Returns
        -------
        None
        """
        if type(self.source) is RegisteredFixedSource:
            self.ra_unit = self.source.position.ra.unit
            self.dec_unit = self.source.position.dec.unit
            self.do_flux = self.source.flux is not None
            self.flux_unit = self.source.flux.unit if self.do_flux else None
            self.interp = lambda _: (
                (
                    self.source.position.ra.value,
                    self.source.position.dec.value,
                    self.source.flux.value,
                )
                if self.do_flux
                else (
                    self.source.position.ra.value,
                    self.source.position.dec.value,
                )
            )

        elif type(self.source) is SolarSystemObject:
            ephem_points = ephem_cat.get_ephem_points(
                sso_id=self.source.sso_id, t_min=self.t_min, t_max=self.t_max
            )
            x = np.zeros(len(ephem_points))

            self.do_flux = True
            for ephem in ephem_points:
                if ephem.flux is None:
                    self.do_flux = False
                    break

            if self.do_flux:
                y = np.zeros((len(ephem_points), 3))
            else:
                y = np.zeros((len(ephem_points), 2))

            for i, ephem in enumerate(ephem_points):
                x[i] = ephem.time.unix
                y[i] = (
                    (
                        ephem.position.ra.value,
                        ephem.position.dec.value,
                        ephem.flux.value,
                    )
                    if self.do_flux
                    else (ephem.position.ra.value, ephem.position.dec.value)
                )

            self.ra_unit = ephem.position.ra.unit
            self.dec_unit = ephem.position.dec.unit
            self.flux_unit = ephem.flux.unit if self.do_flux else None
            self.interp = make_interp_spline(x, y, k=1)

    def at_time(self, *, t: Time) -> tuple[ICRS, Quantity]:
        """
        Get the position and flux of the source at a given time. init_interp() must be called before this method.

        Parameters
        ----------
        time : Time
            Time to get position and flux at.

        Returns
        -------
        position : ICRS
             Position of source at given time
        flux : Quantity
             Flux of source at given time

        Raises
        ------
        RuntimeError
            If interp is not initialized. Call init_interp() first.
        ValueError
            If time is out of range for source generator
        """
        if self.interp is None:
            raise RuntimeError(
                "Interpolator not initialized. Call init_interp() first."
            )

        if t < self.t_min or t > self.t_max:
            raise ValueError("Time out of range for source generator")

        if self.do_flux:
            ra_deg, dec_deg, flux_mJy = self.interp(t.unix)
            flux = flux_mJy * self.flux_unit
        else:
            ra_deg, dec_deg = self.interp(t.unix)
            flux = None

        position = ICRS(ra_deg * self.ra_unit, dec_deg * self.dec_unit)

        return (position, flux)
