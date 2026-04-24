"""
The web API to access the socat database.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from ..database import (
    ALL_TABLES,
)
from ..settings import Settings
from .routers import fixed_sources, moving_sources, services, sso


@asynccontextmanager
async def lifespan(_app: FastAPI):  # pragma: no cover
    async_engine = create_async_engine(Settings().database_url, echo=True, future=True)
    # Use SQLModel to create the tables.
    print("Creating tables")
    for table in ALL_TABLES:
        print("Creating table", table)
        async with async_engine.begin() as conn:
            await conn.run_sync(table.metadata.create_all)
    yield


app = FastAPI(lifespan=lifespan)

app.include_router(fixed_sources.router)
app.include_router(moving_sources.router)
app.include_router(services.router)
app.include_router(sso.router)
