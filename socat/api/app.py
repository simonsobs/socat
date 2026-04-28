"""
The web API to access the socat database.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from ..database.session import initialize_database_schema_async
from ..settings import Settings
from .routers import fixed_sources, moving_sources, services, sso


@asynccontextmanager
async def lifespan(_app: FastAPI):  # pragma: no cover
    async_engine = create_async_engine(Settings().database_url, echo=True, future=True)
    await initialize_database_schema_async(async_engine)
    try:
        yield
    finally:
        await async_engine.dispose()


app = FastAPI(lifespan=lifespan)

app.include_router(fixed_sources.router)
app.include_router(moving_sources.router)
app.include_router(services.router)
app.include_router(sso.router)
