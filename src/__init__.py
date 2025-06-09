"""
EigenLayer Analysis Package

A comprehensive toolkit for analyzing EigenLayer operators, AVS, and strategies
with high-performance caching and advanced analytics.
"""

__version__ = "0.1.0"
__author__ = "EigenLayer Analysis Team"
__email__ = "analysis@eigenlayer.xyz"

from .core.client import EigenLayerClient
from .database.manager import DatabaseManager
from .query.engine import QueryEngine
from .cache.manager import CacheManager

__all__ = [
    "EigenLayerClient",
    "DatabaseManager", 
    "QueryEngine",
    "CacheManager",
    "__version__"
]