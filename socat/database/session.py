"""
Shared SQLAlchemy session factories for async API and sync client access.
"""

from contextlib import contextmanager
from functools import lru_cache
from typing import Annotated, AsyncIterator, Callable, ContextManager, Iterator

from fastapi import Depends
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sqlmodel import SQLModel

from socat.settings import Settings


def initialize_database_schema(engine: Engine) -> None:
    """
    Ensure all SOCat tables exist for a synchronous engine.
    """
    # Import table models before touching SQLModel metadata.
    from socat.database import ALL_TABLES

    del ALL_TABLES
    SQLModel.metadata.create_all(bind=engine)


async def initialize_database_schema_async(engine: AsyncEngine) -> None:
    """
    Ensure all SOCat tables exist for an asynchronous engine.
    """
    # Import table models before touching SQLModel metadata.
    from socat.database import ALL_TABLES

    del ALL_TABLES
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


def _enable_sqlite_foreign_keys(engine: Engine) -> None:
    if not engine.url.drivername.startswith("sqlite"):
        return

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        del connection_record
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def create_sync_session_factory(
    *,
    db_url: str | None = None,
    engine: Engine | None = None,
) -> sessionmaker[Session]:
    """
    Build a synchronous SQLAlchemy session factory.
    """
    if engine is None:
        engine = create_engine(db_url or Settings().sync_database_url, future=True)

    _enable_sqlite_foreign_keys(engine)
    initialize_database_schema(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@lru_cache(maxsize=1)
def get_database_sync_session_factory() -> sessionmaker[Session]:
    """
    Return a process-level sync session factory for client access.
    """
    return create_sync_session_factory()


def create_sync_session_interface(
    *,
    db_url: str | None = None,
    engine: Engine | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> Callable[[], ContextManager[Session]]:
    """
    Build a functional sync-session interface like get_async_session.
    """
    if session_factory is None:
        session_factory = create_sync_session_factory(db_url=db_url, engine=engine)

    @contextmanager
    def get_sync_session() -> Iterator[Session]:
        with session_factory() as session:
            yield session

    return get_sync_session


def create_async_session_factory(
    *,
    db_url: str | None = None,
    engine: AsyncEngine | None = None,
) -> async_sessionmaker[AsyncSession]:
    """
    Build an asynchronous SQLAlchemy session factory.
    """
    if engine is None:
        engine = create_async_engine(
            db_url or Settings().database_url,
            echo=True,
            future=True,
        )

    _enable_sqlite_foreign_keys(engine.sync_engine)
    return async_sessionmaker(bind=engine, expire_on_commit=False)


@lru_cache(maxsize=1)
def get_database_async_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Return a process-level async session factory for API dependencies.
    """
    return create_async_session_factory()


@contextmanager
def get_sync_session() -> Iterator[Session]:
    """
    Yield a sync SQLAlchemy session for client operations.
    """
    with get_database_sync_session_factory()() as session:
        yield session


async def get_async_session() -> AsyncIterator[AsyncSession]:
    """
    Yield an async SQLAlchemy session for API routes.
    """
    async with get_database_async_session_factory()() as session:
        yield session


DatabaseSessionDependency = Annotated[
    AsyncSession,
    Depends(get_async_session),
]

# Backward-compatible aliases for older imports.
get_database_sync_session = get_sync_session
get_database_async_session = get_async_session
SessionDependency = DatabaseSessionDependency
