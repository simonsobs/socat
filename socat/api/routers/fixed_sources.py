"""
The web API to access the socat fixed source database.
"""

import astropy.units as u
from astropy.coordinates import ICRS
from astropydantic import AstroPydanticICRS, AstroPydanticQuantity
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ValidationError

import socat.astroquery as soaq
import socat.core as core
from socat.astroquery import AstroqueryReturn

from ...database.sources import RegisteredFixedSource
from ..async_ses import SessionDependency
from .services import get_service_name

router = APIRouter(prefix="/api/v1")


class SourceModificationRequest(BaseModel):
    """
    Class which defines which source atributes are available to modify

    Attributes
    ----------
    position : AstroPydanticICRS | None
        ICRS coordinates of source
    flux : Quantity | None
        Flux of source.
    name : str | None
        Name of source
    """

    position: AstroPydanticICRS | None
    flux: AstroPydanticQuantity[u.mJy] | None
    name: str | None = None


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


class ConeRequest(BaseModel):
    """
    Class which defines attribues of cone requests

    Attributes
    ----------
    position : AstroPydanticICRS
        Cone center
    radius : AstroPydanticQuantity
        Radius of cone center. Unitfull.
    """

    position: AstroPydanticICRS
    radius: AstroPydanticQuantity[u.arcmin]


@router.put("/source/new")
async def create_source(
    model: SourceModificationRequest, session: SessionDependency
) -> RegisteredFixedSource:
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
    response : RegisteredFixedSource
        socat.database.RegisteredFixedSource object which was added to the catalog.

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
        response = await core.create_source(
            position=model.position,
            flux=model.flux,
            session=session,
            name=model.name,
        )
    except ValidationError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.errors())

    return response


@router.post("/source/new")
async def create_source_name(
    name: str,
    astroquery_service: str,
    session: SessionDependency,
) -> RegisteredFixedSource:
    """
    Create a new source by name, resolve using astroquery_service.

    Parameters
    ----------
    name : str
        Name of source to resolve
    astroquery_service : str
        Name of astroquery service to use to resolve name
    session : SessionDependency
        Asynchronous session to be used

    Returns
    -------
    response : RegisteredFixedSource
        socat.database.RegisteredFixedSource object which was added to the catalog.

    Raises
    ------
    HTTPException
        If the astroquery service is not supported, if RA/dec aren't requested, or api response is malformed.
    """

    services = await get_service_name(astroquery_service, session=session)

    if len(services) == 0:  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Service {} is not available.".format(astroquery_service),
        )

    result_table = await soaq.get_source_info(
        name=name,
        astroquery_service=astroquery_service,
    )

    if result_table.get("ra", None) is None or result_table.get("dec", None) is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="RA or Dec unresolved by {}.".format(astroquery_service),
        )

    # Note the conversion to degrees happens in soaq.get_source_info
    position = ICRS(
        ra=result_table.get("ra", None) * u.deg,
        dec=result_table.get("dec", None) * u.deg,
    )
    flux = result_table.get("flux", None)
    if flux is not None:
        flux *= u.mJy

    try:
        response = await core.create_source(
            position=position,
            session=session,
            flux=flux,
            name=name,
        )
    except ValidationError as e:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.errors())

    return response


@router.post("/cone")  # TODO: Not sure if this is the right path
async def get_cone_astroquery(
    cone: ConeRequest,
    session: SessionDependency,
) -> list[
    AstroqueryReturn
]:  # TODO: Should this return info other than names like ra/dec/what service it came from
    """
    Get all sources in cone centered on ra/dec with radius using astroquery.
    All services in service_list will be queried.
    If service_list is none, then all available services in AstroqueryServiceTable will be searched

    Parameters
    ----------
    cone : ConeRequest
        Cone request specifying ra/dec and radius of cond
    session :  SessionDependeny
        Asynchronous session to use

    Returns
    -------
    source_list : list[AstroqueryReturn]
        List of AstroqueryReturn objects specifying name, ra, dec, provider, and distance from center of source
    """
    service_list = await core.get_all_services(session=session)

    source_list = await soaq.cone_search(
        position=cone.position,
        service_list=service_list,
        radius=cone.radius,
    )

    return source_list


@router.post("/source/box")
async def get_box(
    box: BoxRequest, session: SessionDependency
) -> list[RegisteredFixedSource]:
    """
    Get all sources in a box bounded by ra_min, ra_max, dec_min, dec_max.

    Parameters
    ----------
    box : BoxRequest
        BoxRequest class containing lower_left, upper_right
    session : SessionDependeny
        Asynchronous session to use

    Returns
    -------
    response : list[RegisteredFixedSource]
        List of socat.database.RegisteredFixedSource sources in box

    Raises
    ------
    HTTPException
        If unphysical box bounds
    """
    if (
        box.lower_left.ra > box.upper_right.ra
        or box.lower_left.dec > box.upper_right.dec
    ):  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="RA/Dec min must be <= max",
        )

    response = await core.get_box(
        lower_left=box.lower_left, upper_right=box.upper_right, session=session
    )

    return response


@router.get("/source/{source_id}")
async def get_source(
    source_id: int, session: SessionDependency
) -> RegisteredFixedSource:
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
    response : RegisteredFixedSource
        socat.database.RegisteredFixedSource corresponding to id

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
) -> RegisteredFixedSource:
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
    response :  RegisteredFixedSource
        socat.database.RegisteredFixedSource that has been modified

    Raises
    ------
    HTTPException
        If id does not correspond to any source
    """
    try:
        response = await core.update_source(
            source_id, model.position, session=session, flux=model.flux, name=model.name
        )
    except ValueError as e:  # pragma: no cover
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
