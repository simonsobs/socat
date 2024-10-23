"""
The web API to access the socat database.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

import socat.core as core

from .database import ExtragalacticSource, async_engine, get_async_session


async def lifetime(f: FastAPI):
    # Use SQLModel to create the tables.
    for table in core.ALL_TABLES:
        await table.metadata.create_all(async_engine)
    yield


app = FastAPI(lifetime=lifetime)

router = APIRouter(prefix="/api/v1")

SessionDependency = Annotated[AsyncSession, Depends(get_async_session)]


class SourceModificationRequest(BaseModel):
    ra: float | None
    dec: float | None


@router.put("/source/new")
async def create_source(
    model: SourceModificationRequest, session: SessionDependency
) -> ExtragalacticSource:
    if model.ra is None or model.dec is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="RA and Dec must be provided",
        )

    response = await core.create_source(model.ra, model.dec, session=session)
    return response


@router.get("/source/{source_id}")
async def get_source(source_id: int, session: SessionDependency) -> ExtragalacticSource:
    try:
        response = await core.get_source(source_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.post("/source/{source_id}")
async def update_source(
    source_id: int, model: SourceModificationRequest, session: SessionDependency
) -> ExtragalacticSource:
    try:
        response = await core.update_source(
            source_id, model.ra, model.dec, session=session
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.delete("/source/{source_id}")
async def delete_source(source_id: int, session: SessionDependency) -> None:
    try:
        await core.delete_source(source_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return


app.include_router(router)
