"""
The web API to access the socat fixed source database.
"""

from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ValidationError

import socat.core as core

from ...database.services import AstroqueryService
from ..async_ses import SessionDependency

router = APIRouter(prefix="/api/v1")


class ServiceModificationRequestion(BaseModel):
    """
    Class which defines which service atributes are available to modify

    Attributes
    ----------
    name : str | None
        Name of service
    config: dict[str, Any]  | None
        json to be deserialized to config options
    """

    name: str | None
    config: dict[str, Any]


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
    Get a astroquery service by id from the database

    Parameters
    ----------
    service_id : int
        ID of service to querry
    session : SessionDependency
        Asynchronous session to use

    Returns:
    --------
    response : AstroqueryService
        socat.database.AstroqueryService corresponding to id

    Raises
    ------
    HTTPException
        If id does not correspond to any service
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
    """
    Get an astroquery service by name from the database.

    Parameters
    ----------
    service_name : str
        Name of service to query
    session : SessionDependency
        Asynchronous session to use

    Returns:
    --------
    response : AstroqueryService
        socat.database.AstroqueryService corresponding to name

    Raises
    ------
    HTTPException
        If name does not correspond to any service
    """
    try:
        response = await core.get_service_name(service_name, session=session)
    except ValueError as e:  # pragma: no cover
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
    except ValueError as e:  # pragma: no cover
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
    except ValueError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return
