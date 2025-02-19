"""
The web API to access the socat database.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

import socat.core as core

from .database import ALL_TABLES, ExtragalacticSource, async_engine, get_async_session


async def lifespan(f: FastAPI):
    # Use SQLModel to create the tables.
    print("Creating tables")
    for table in ALL_TABLES:
        print("Creating table", table)
        async with async_engine.begin() as conn:
            await conn.run_sync(table.metadata.create_all)
    yield


app = FastAPI(lifespan=lifespan)

router = APIRouter(prefix="/api/v1")

SessionDependency = Annotated[AsyncSession, Depends(get_async_session)]


class SourceModificationRequest(BaseModel):
    """
    Class which defines which atributes are available to modify

    Attributes
    ----------
    ra : float | None
        RA of source
    dec : float | None
        Dec of source
    """

    ra: float | None
    dec: float | None


class BoxRequest(BaseModel):
    """
    Class which defines attributes of box requests

    Attributes
    ----------
    ra_min : float
        Minimum RA of box
    ra_max : float
        Maximum RA of box
    dec_min : float
        Minimum dec of box
    dec_max : float
        Maximum dec of box
    """

    ra_min: float
    ra_max: float
    dec_min: float
    dec_max: float


@router.put("/source/new")
async def create_source(
    model: SourceModificationRequest, session: SessionDependency
) -> ExtragalacticSource:
    """
    Create a new source in the catalog

    Parameters
    ----------
    model : SourceModificationRequest
        Object which contains all attributes of source
    session : SessionDependencey
        Asynchronous session to be used

    Returns
    -------
    response : ExtragalacticSource
        socat.database.ExtragalacticSource object which was added to the catalog.
    Raises
    ------
    HTTPException
        If the model does not contain required info or api response is malformed
    """
    if model.ra is None or model.dec is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="RA and Dec must be provided",
        )

    try:
        response = await core.create_source(model.ra, model.dec, session=session)
    except ValidationError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.errors())

    return response


@router.post("/source/box")
async def get_box(
    box: BoxRequest, session: SessionDependency
) -> list[ExtragalacticSource]:
    """
    Get all sources in a box bounded by ra_min, ra_max, dec_min, dec_max.

    Parameters
    ----------
    box : BoxRequest
        BoxRequest class containing ra_min,
        ra_max, dec_min, dec_max
    session : SessionDependeny
        Asynchronous session to use

    Returns
    -------
    response : list[ExtragalacticSource]
        List of socat.database.ExtragalacticSource sources in box

    Raises
    ------
    HTTPException
        If unphysical box bounds
    """
    if box.ra_min > box.ra_max or box.dec_min > box.dec_max:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="RA/Dec min must be <= max",
        )

    response = await core.get_box(
        box.ra_min, box.ra_max, box.dec_min, box.dec_max, session=session
    )

    return response


@router.get("/source/{source_id}")
async def get_source(source_id: int, session: SessionDependency) -> ExtragalacticSource:
    """
    Get a source by id from the database

    Parameters
    ----------
    source_id : int
        ID of source to querry
    session : SessionDependency
        Asynchronous session to use

    Returns:
    --------
    response : ExtragalacticSource
        socat.database.ExtragalacticSource corresponding to id

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.get_source(source_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.post("/source/{source_id}")
async def update_source(
    source_id: int, model: SourceModificationRequest, session: SessionDependency
) -> ExtragalacticSource:
    """
    Update source parameters by id

    Parameters
    ----------
    source_id : int
        ID of source to update
    model : SourceModificationRequest
        Parameters of model to modify
    session : SessionDependency
        Asynchronous session to use

    Returns
    -------
    response :  ExtragalacticSource
        socat.database.ExtragalacticSource that has been modified

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.update_source(
            source_id, model.ra, model.dec, session=session
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.delete("/source/{source_id}")
async def delete_source(source_id: int, session: SessionDependency) -> None:
    """
    Delete a source by id

    Parameters
    ----------
    source_id : int
        ID of source to delete
    session : SessionDependency
        Asynchronous session to use

    Returns
    -------
    None


    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        await core.delete_source(source_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return


app.include_router(router)
