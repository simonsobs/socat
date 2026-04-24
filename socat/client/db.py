"""
Uses a SQLAlchemy database connection to implement the client core.
"""

from importlib import import_module
from typing import Any, Callable, ContextManager

import astropy.units as u
from astropy.coordinates import ICRS
from astropy.units import Quantity
from astroquery.query import BaseVOQuery
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
)
from socat.database.session import create_sync_session_interface

from .core import (
    AstroqueryClientBase,
    ClientBase,
    EphemClientBase,
    SolarSystemClientBase,
)


class Client(ClientBase):
    """
    DB-backed client implementation for fixed sources.
    """

    _get_session: Callable[[], ContextManager]

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

    def create_source(
        self, *, position: ICRS, name: str | None = None, flux: Quantity | None = None
    ) -> RegisteredFixedSource:
        if flux is not None:
            flux = flux.to_value("mJy")

        source = RegisteredFixedSourceTable(
            ra_deg=position.ra.to_value("deg"),
            dec_deg=position.dec.to_value("deg"),
            name=name,
            flux_mJy=flux,
        )
        with self._get_session() as session:
            session.add(source)
            session.commit()
            session.refresh(source)

        return source.to_model()

    def create_name(
        self, *, name: str, astroquery_service: str
    ) -> RegisteredFixedSource:
        service: BaseVOQuery = getattr(
            import_module(f"astroquery.{astroquery_service.lower()}"),
            astroquery_service,
        )

        requested_params = ["ra", "dec"]

        result_table = service.query_object(name)
        result_table["ra"].convert_unit_to("deg")
        result_table["dec"].convert_unit_to("deg")
        if "flux" in result_table.columns:
            result_table["flux"].convert_unit_to("mJy")  # pragma: no cover
        result_dict = {param: None for param in requested_params}
        if len(result_table) == 0:
            return None
        for param in requested_params:
            try:
                result_dict[param] = result_table[param].value.data[0]
            except KeyError:  # pragma: no cover
                continue

        position = ICRS(
            ra=result_dict["ra"] * u.deg,
            dec=result_dict["dec"] * u.deg,
        )
        flux = result_dict.get("flux", None)
        if flux is not None:
            flux *= u.mJy

        return self.create_source(position=position, name=name, flux=flux)

    def get_box(
        self,
        *,
        lower_left: ICRS,
        upper_right: ICRS,
    ) -> list[RegisteredFixedSource]:
        with self._get_session() as session:
            sources = session.execute(
                select(RegisteredFixedSourceTable).where(
                    float(lower_left.ra.to_value("deg"))
                    <= RegisteredFixedSourceTable.ra_deg,
                    RegisteredFixedSourceTable.ra_deg
                    <= float(upper_right.ra.to_value("deg")),
                    float(lower_left.dec.to_value("deg"))
                    <= RegisteredFixedSourceTable.dec_deg,
                    RegisteredFixedSourceTable.dec_deg
                    <= float(upper_right.dec.to_value("deg")),
                )
            )

            return [s.to_model() for s in sources.scalars().all()]

    def get_source(self, *, source_id: int) -> RegisteredFixedSource | None:
        with self._get_session() as session:
            source = session.get(RegisteredFixedSourceTable, source_id)
            if source is None:
                return None
            return source.to_model()

    def get_forced_photometry_sources(
        self, *, minimum_flux: Quantity
    ) -> list[RegisteredFixedSource]:
        minimum_flux_mJy = minimum_flux.to_value("mJy")

        with self._get_session() as session:
            sources = session.execute(
                select(RegisteredFixedSourceTable).where(
                    RegisteredFixedSourceTable.flux_mJy.is_not(None),
                    RegisteredFixedSourceTable.flux_mJy >= minimum_flux_mJy,
                )
            )
            return [s.to_model() for s in sources.scalars().all()]

    def update_source(
        self,
        *,
        source_id: int,
        position: ICRS | None = None,
        name: str | None = None,
        flux: Quantity | None = None,
    ) -> RegisteredFixedSource | None:
        with self._get_session() as session:
            source = session.get(RegisteredFixedSourceTable, source_id)

            if source is None:
                return None

            if position is not None:
                source.ra_deg = position.ra.to_value("deg")
                source.dec_deg = position.dec.to_value("deg")
            source.name = source.name if name is None else name
            source.flux_mJy = source.flux_mJy if flux is None else flux.to_value("mJy")

            session.commit()
            session.refresh(source)

            return source.to_model()

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

    _get_session: Callable[[], ContextManager]

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
                return None

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
                return None

            return service_list

    def update_service(
        self,
        *,
        service_id: int,
        name: str | None,
        config: dict[str, Any] | None,
    ) -> AstroqueryService | None:
        with self._get_session() as session:
            service = session.get(AstroqueryServiceTable, service_id)

            if service is None:
                return None

            service.name = service.name if name is None else name
            service.config = service.config if config is None else config

            session.commit()
            session.refresh(service)

            return service.to_model()

    def delete_service(self, *, service_id: int) -> None:
        with self._get_session() as session:
            service = session.get(AstroqueryServiceTable, service_id)
            if service is None:
                return

            session.delete(service)
            session.commit()


class SolarSystemClient(SolarSystemClientBase):
    """
    DB-backed client implementation for solar system objects.
    """

    _get_session: Callable[[], ContextManager]

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

    def create_sso(self, *, name: str, MPC_id: int | None) -> SolarSystemObject:
        source = SolarSystemObjectTable(name=name, MPC_id=MPC_id)

        with self._get_session() as session:
            session.add(source)
            session.commit()
            session.refresh(source)

            return source.to_model()

    def get_sso(self, *, sso_id: int) -> SolarSystemObject | None:
        with self._get_session() as session:
            source = session.get(SolarSystemObjectTable, sso_id)

            if source is None:
                return None

            return source.to_model()

    def get_sso_name(self, *, name: str) -> list[SolarSystemObject] | None:
        with self._get_session() as session:
            sources = session.execute(
                select(SolarSystemObjectTable).where(
                    SolarSystemObjectTable.name == name
                )
            )

            source_list = [s.to_model() for s in sources.scalars().all()]
            if len(source_list) == 0:
                return None

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
                return None

            return source_list

    def update_sso(
        self, *, sso_id: int, name: str | None, MPC_id: int | None
    ) -> SolarSystemObject | None:
        with self._get_session() as session:
            source = session.get(SolarSystemObjectTable, sso_id)

            if source is None:
                return None

            source.name = source.name if name is None else name
            source.MPC_id = source.MPC_id if MPC_id is None else MPC_id

            session.commit()
            session.refresh(source)

            return source.to_model()

    def delete_sso(self, *, sso_id: int) -> None:
        with self._get_session() as session:
            source = session.get(SolarSystemObjectTable, sso_id)
            if source is None:
                return

            session.delete(source)
            session.commit()


class EphemClient(EphemClientBase):
    """
    DB-backed client implementation for moving-source ephemerides.
    """

    _get_session: Callable[[], ContextManager]

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
        time: int,
        position: ICRS,
        flux: Quantity | None = None,
    ) -> RegisteredMovingSource:
        flux_mJy = None if flux is None else flux.to_value("mJy")

        ephem = RegisteredMovingSourceTable(
            sso_id=sso_id,
            MPC_id=MPC_id,
            name=name,
            time=time,
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
                return None

            return ephem.to_model()

    def update_ephem(
        self,
        *,
        ephem_id: int,
        sso_id: int | None,
        MPC_id: int | None,
        name: str | None,
        time: int | None,
        position: ICRS | None,
        flux: Quantity | None,
    ) -> RegisteredMovingSource | None:
        with self._get_session() as session:
            ephem = session.get(RegisteredMovingSourceTable, ephem_id)

            if ephem is None:
                return None

            ephem.sso_id = ephem.sso_id if sso_id is None else sso_id
            ephem.MPC_id = ephem.MPC_id if MPC_id is None else MPC_id
            ephem.name = ephem.name if name is None else name
            ephem.time = ephem.time if time is None else time
            if position is not None:
                ephem.ra_deg = position.ra.to_value("deg")
                ephem.dec_deg = position.dec.to_value("deg")
            ephem.flux_mJy = ephem.flux_mJy if flux is None else flux.to_value("mJy")

            session.commit()
            session.refresh(ephem)

            return ephem.to_model()

    def delete_ephem(self, *, ephem_id: int) -> None:
        with self._get_session() as session:
            ephem = session.get(RegisteredMovingSourceTable, ephem_id)
            if ephem is None:
                return

            session.delete(ephem)
            session.commit()
