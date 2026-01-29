from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..settings import settings

async_engine = create_async_engine(settings.database_url, echo=True, future=True)


async def get_async_session() -> AsyncSession:
    async_session = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    async with async_session() as session:
        yield session


SessionDependency = Annotated[AsyncSession, Depends(get_async_session)]
