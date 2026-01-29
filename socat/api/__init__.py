"""
The web API to access the socat database.
"""

from .app import app
from .async_ses import SessionDependency
from .routers.fixed_sources import (
    create_source,
    create_source_name,
    delete_source,
    get_box,
    get_cone_astroquery,
    get_source,
    update_source,
)
from .routers.moving_sources import create_ephem, delete_ephem, get_ephem, update_ephem
from .routers.services import (
    create_service,
    delete_service,
    get_service,
    get_service_name,
    update_service,
)
from .routers.sso import create_sso, delete_sso, get_sso, update_sso

__all__ = [
    "app",
    "SessionDependency",
    "create_source",
    "create_source_name",
    "get_cone_astroquery",
    "get_box",
    "get_source",
    "update_source",
    "delete_source",
    "create_ephem",
    "get_ephem",
    "update_ephem",
    "delete_ephem",
    "create_service",
    "get_service",
    "get_service_name",
    "update_service",
    "delete_service",
    "create_sso",
    "get_sso",
    "update_sso",
    "delete_sso",
]
