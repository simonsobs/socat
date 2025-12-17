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
    "RegisteredFixedSource",
    "RegisteredFixedSourceTable",
    "RegisteredMovingSource",
    "RegisteredMovingSourceTable",
    "SolarSystemObject",
    "SolarSystemObjectTable",
    "AstroqueryService",
    "AstroqueryServiceTable",
]

ALL_TABLES = [
    RegisteredFixedSourceTable,
    RegisteredMovingSourceTable,
    SolarSystemObjectTable,
    AstroqueryServiceTable,
]
