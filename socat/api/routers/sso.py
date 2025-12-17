"""
The web API to access the socat moving source database.
"""

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ValidationError

import socat.core as core

from ...database import SolarSystemObject
from ..async_ses import SessionDependency

router = APIRouter(prefix="/api/v1")


class SolarSystemObjectRequest(BaseModel):
    """
    Class which defines which source atributes are available to modify for a solar system source

    Attributes
    ----------
    name : str | None
        Name of source
    MPC_id : int | None
        Minor Planet Center ID of source
    """

    name: str
    MPC_id: int | None = None


@router.put("/sso/new")
async def create_sso(
    model: SolarSystemObjectRequest, session: SessionDependency
) -> SolarSystemObject:
    """
    Create a new solar system source

    Parameters
    ----------
    model : SolarSystemObjectRequest
        Object which contains all attributes of source
    session : SessionDependency
        Asynchronous session to be used

    Returns
    -------
    response : SolarSystemObject
        socat.database.SolarSystemObject object which was added to the catalog.

    Raises
    ------
    HTTPException
        If the model does not contain required info or api response is malformed
    """
    if model.name is None:  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Source name must be provided",
        )
    try:
        response = await core.create_sso(
            name=model.name,
            MPC_id=model.MPC_id,
            session=session,
        )
    except ValidationError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.errors())

    return response


@router.get("/sso/{sso_id}")
async def get_sso(sso_id: int, session: SessionDependency) -> SolarSystemObject:
    """
    Get a solar sytem source by id from the database

    Parameters
    ----------
    sso_id : int
        ID of solar system source to querry
    session : SessionDependency
        Asynchronous session to use

    Returns:
    --------
    response : SolarSystemObject
        socat.database.SolarSystemObject corresponding to id

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.get_sso(sso_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.post("/sso/{sso_id}")
async def update_sso(
    sso_id: int,
    model: SolarSystemObjectRequest,
    session: SessionDependency,
) -> SolarSystemObject:
    """
    Update solar system source parameters by id

    Parameters
    ----------
    sso_id : int
        ID of solar system source to update
    model : SolarSystemObjectRequest
        Parameters of model to modify
    session : SessionDependency
        Asynchronous session to use

    Returns
    -------
    response : SolarSystemObject
        socat.database.SolarSystemObject that has been modified

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.update_sso(
            sso_id,
            name=model.name,
            MPC_id=model.MPC_id,
            session=session,
        )
    except ValueError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.delete("/sso/{sso_id}")
async def delete_sso(sso_id: int, session: SessionDependency) -> None:
    """
    Delete a solar system source by id

    Parameters
    ----------
    sso_id : int
        ID of solar system source to delete
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
        await core.delete_sso(sso_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return
