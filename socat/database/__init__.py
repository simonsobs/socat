"""
Table definitions
"""

from .services import AstroqueryService, AstroqueryServiceTable
from .sources import (
    RegisteredFixedSource,
    RegisteredFixedSourceTable,
    RegisteredMovingSource,
    RegisteredMovingSourceTable,
    SolarSystemObject,
    SolarSystemObjectTable,
)

__all__ = [
    "AstroqueryService",
    "AstroqueryServiceTable",
    "RegisteredFixedSource",
    "RegisteredFixedSourceTable",
    "RegisteredMovingSource",
    "RegisteredMovingSourceTable",
    "SolarSystemObject",
    "SolarSystemObjectTable",
]

ALL_TABLES = [
    RegisteredFixedSourceTable,
    RegisteredMovingSourceTable,
    SolarSystemObjectTable,
    AstroqueryServiceTable,
]
