"""
The web API to access the socat database.
"""

from socat.database.session import SessionDependency

from .app import app
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
from .routers.sso import create_sso, delete_sso, get_sso, get_sso_box, update_sso

__all__ = [
    "SessionDependency",
    "app",
    "create_ephem",
    "create_service",
    "create_source",
    "create_source_name",
    "create_sso",
    "delete_ephem",
    "delete_service",
    "delete_source",
    "delete_sso",
    "get_box",
    "get_cone_astroquery",
    "get_ephem",
    "get_service",
    "get_service_name",
    "get_source",
    "get_sso",
    "get_sso_box",
    "update_ephem",
    "update_service",
    "update_source",
    "update_sso",
]
