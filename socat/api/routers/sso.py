"""
The web API to access the socat moving source database.
"""

from astropydantic import AstroPydanticICRS
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ValidationError

from socat import core
from socat.database.session import SessionDependency

from ...database import SolarSystemObject

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


class BoxRequest(BaseModel):
    """
    Class which defines attributes of box requests

    Attributes
    ----------
    bottom_left : AstroPydanticICRS
        Bottom left corner of box
    top_right : AstroPydanticICRS
        Top right corner of box
    """

    lower_left: AstroPydanticICRS
    upper_right: AstroPydanticICRS


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


@router.post("/sso/box")
async def get_sso_box(
    box: BoxRequest,
    t_min: int,
    t_max: int,
    session: SessionDependency,
) -> list[SolarSystemObject]:
    """
    Equivelent of fixed_sources.get_box for SSO objects. Only gets objects which have at least one ephem point
    inside the box between t_min and t_max.

    Parameters
    ----------
    box : BoxRequest
        Box to search for SSOs
    t_min : int
        Minimum time in unix time for box search. TODO: Maybe should accept some sort of Astropydantic time quantity that can be converted to unix.
    t_max : int
        Maximum time in unix time for box search.
    session : SessionDependency
        Asynchronous session to use

    Returns
    -------
    response : list[SolarSystemObject]

    Raises
    ------
    HTTPException
        If unphysical box bounds or time bounds
    """
    if (
        box.lower_left.ra > box.upper_right.ra
        or box.lower_left.dec > box.upper_right.dec
    ):  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="RA/Dec min must be <= max",
        )

    if t_max <= t_min:  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="t_min must be strictly less than t_max.",
        )

    return await core.get_time_box(
        lower_left=box.lower_left,
        upper_right=box.upper_right,
        t_min=t_min,
        t_max=t_max,
        session=session,
    )


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
