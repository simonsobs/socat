"""
The web API to access the socat database.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

import socat.astroquery as soaq
import socat.core as core

from .database import (
    ALL_TABLES,
    AstroqueryService,
    ExtragalacticSource,
    async_engine,
    get_async_session,
)


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


class ServiceModificationRequestion(BaseModel):
    """
    Class which defines which service atributes are available to modify

    Attributes
    ----------
    name : str | None
        Name of service
    config: str | None
        json to be deserialized to config options
    """

    name: str | None
    config: str | None


class SourceModificationRequest(BaseModel):
    """
    Class which defines which source atributes are available to modify

    Attributes
    ----------
    ra : float | None
        RA of source
    dec : float | None
        Dec of source
    name : str | None
        Name of source
    """

    ra: float | None
    dec: float | None
    name: str | None


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


@router.put("/service/new")
async def create_service(
    model: ServiceModificationRequestion,
    session: SessionDependency,
) -> AstroqueryService:
    """
    Create a new astroquery service in the catalog

    Parameters
    ----------
    model : ServiceModificationRequest
        Object which contains name, common_api, and common attributes of service
    session : SessionDependency
        Asynchronous session to be used

    Returns
    -------
    response : AstroqueryService
       socat.database.AstroqueryService object which was added to the catalog.

    Raises
    ------
    HTTPException
        If the model does not contain required info or api response is malformed
    """
    if model.name is None or model.config is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Name, config must be provided.",
        )

    try:
        response = await core.create_service(
            name=model.name, config=model.config, session=session
        )
    except ValidationError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.errors())

    return response


@router.get("/service/{service_id}")
async def get_service(service_id: int, session: SessionDependency) -> AstroqueryService:
    """
    Get a astroquery service by id or name from the database

    Parameters
    ----------
    service_id : str
        ID of source to querry
    session : SessionDependency
        Asynchronous session to use

    Returns:
    --------
    response : ExtragalacticSource
        socat.database.AstroqueryService corresponding to id

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.get_service(service_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.get("/service/")
async def get_service_name(
    service_name: str, session: SessionDependency
) -> list[AstroqueryService]:
    try:
        response = await core.get_service_name(service_name, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.post("/service/{service_id}")
async def update_service(
    service_id: int, model: ServiceModificationRequestion, session: SessionDependency
) -> AstroqueryService:
    """
    Update astroquery service parameters by id

    Parameters
    ----------
    service_name : int
        Name of source to update
    model : ServiceModificationRequestion
        Parameters of service to modify
    session : SessionDependency
        Asynchronous session to use

    Returns
    -------
    response :  AstroqueryService
        socat.database.AstroqueryService that has been modified

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.update_service(
            service_id, model.name, config=model.config, session=session
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.delete("/service/{service_id}")
async def delete_service(service_id: int, session: SessionDependency) -> None:
    """
    Delete a astroquery service by id

    Parameters
    ----------
    service_id : int
        ID of astroquery service to delete
    session : SessionDependency
        Asynchronous session to use

    Returns
    -------
    None


    Raises
    ------
    HTTPException
        If name does not correspond to any service
    """
    try:
        await core.delete_service(service_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return


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
    session : SessionDependency
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
        response = await core.create_source(
            model.ra, model.dec, name=model.name, session=session
        )
    except ValidationError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.errors())

    return response


@router.post("/source/new")
async def create_source_name(
    name: str,
    astroquery_service: str,
    session: SessionDependency,
    requested_params: list[str] = ["ra", "dec"],
) -> ExtragalacticSource:
    """
    Create a new source by name, resolve using astroquery_service.

    Parameters
    ----------
    name : str
        Name of source to resolve
    astroquery_service : str
        Name of astroquery service to use to resolve name
    requested_params : list[str], Default: ["ra", "dec"]
        Parameters of source to get.
        Must match astrotable column names.
    session : SessionDependency
        Asynchronous session to be used

    Returns
    -------
    response : ExtragalacticSource
        socat.database.ExtragalacticSource object which was added to the catalog.

    Raises
    ------
    HTTPException
        If the astroquery service is not supported, if RA/dec aren't requested, or api response is malformed.
    """
    if "ra" not in requested_params or "dec" not in requested_params:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="RA and Dec must be requested.",
        )

    services = await get_service_name(astroquery_service, session=session)

    if len(services) == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Service {} is not available.".format(astroquery_service),
        )

    result_table = soaq.get_source_info(
        name=name,
        astroquery_service=astroquery_service,
        requested_params=requested_params,
    )

    if result_table["ra"] is None or result_table["dec"] is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="RA or Dec unresolved by {}.".format(astroquery_service),
        )

    try:
        response = await core.create_source(
            result_table["ra"], result_table["dec"], name=name, session=session
        )
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
