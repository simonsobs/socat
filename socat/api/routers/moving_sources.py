"""
The web API to access the socat moving source database.
"""

import astropy.units as u
from astropydantic import AstroPydanticICRS, AstroPydanticQuantity
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ValidationError

import socat.core as core

from ...database import RegisteredMovingSource
from ..async_ses import SessionDependency

router = APIRouter(prefix="/api/v1")


class EphemModificationRequest(BaseModel):
    """
    Class which defines which ephemeris attributes are available to modify for an ephemeris point

    Attributes
    ----------
    sso_id : int | None
        Internal SO identifier of solar system source
    MPC_id : int | None
        MPC ID of source
    name : str | None
        Name of source
    time : int | None
        Time of source ephem, unix time
    position : AstroPydanticICRS | None
        Position of source at time in ICRS coordinates
    flux : Quantity  | None
        Flux of source at ephem point in mJy
    """

    sso_id: int | None
    MPC_id: int | None
    name: str | None
    time: int | None
    position: AstroPydanticICRS | None
    flux: AstroPydanticQuantity[u.mJy] | None


@router.put("/ephem/new")
async def create_ephem(
    model: EphemModificationRequest, session: SessionDependency
) -> RegisteredMovingSource:
    """
    Create a new ephemeris point

    Parameters
    ----------
    model : EphemModificationRequest
        Object which contains all attributes of ephemeris point
    session : SessionDependency
        Asynchronous session to be used

    Returns
    -------
    response : RegisteredMovingSource
        socat.database.RegisteredMovingSource object which was added to the catalog.

    Raises
    ------
    HTTPException
        If the model does not contain required info or api response is malformed
    """
    if model.position is None:  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Source position must be provided",
        )
    try:
        response = await core.create_ephem(
            session=session,
            sso_id=model.sso_id,
            MPC_id=model.MPC_id,
            name=model.name,
            time=model.time,
            position=model.position,
            flux=model.flux,
        )
    except ValidationError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.errors())

    return response


@router.get("/ephem/{ephem_id}")
async def get_ephem(
    ephem_id: int, session: SessionDependency
) -> RegisteredMovingSource:
    """
    Get an ephem point by id from the database

    Parameters
    ----------
    ephem_id : int
        ID of ephemeris point to querry
    session : SessionDependency
        Asynchronous session to use

    Returns:
    --------
    response : RegisteredMovingSource
        socat.database.RegisteredMovingSource corresponding to id

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.get_ephem(ephem_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.post("/ephem/{ephem_id}")
async def update_ephem(
    ephem_id: int, model: EphemModificationRequest, session: SessionDependency
) -> RegisteredMovingSource:
    """
    Update an ephem point by id

    Parameters
    ----------
    ephem_id : int
        ID of ephem point to update
    model : EphemModificationRequest
        Parameters of model to modify
    session : SessionDependency
        Asynchronous session to use

    Returns
    -------
    response :  RegisteredMovingSource
        socat.database.RegisteredMovingSource that has been modified

    Raises
    ------
    HTTPException
        If id does not correspond to any ephem point
    """
    try:
        response = await core.update_ephem(
            ephem_id,
            session=session,
            sso_id=model.sso_id,
            MPC_id=model.MPC_id,
            name=model.name,
            time=model.time,
            position=model.position,
            flux=model.flux,
        )
    except ValueError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    return response


@router.delete("/ephem/{ephem_id}")
async def delete_ephem(ephem_id: int, session: SessionDependency) -> None:
    """
    Delete a ephem point by id

    Parameters
    ----------
    ephem_id : int
        ID of ephem point to delete
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
        await core.delete_ephem(ephem_id, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return
