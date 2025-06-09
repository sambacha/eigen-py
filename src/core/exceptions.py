"""
Custom exceptions for EigenLayer analysis.
"""


class EigenLayerError(Exception):
    """Base exception for EigenLayer analysis errors."""
    pass


class RateLimitError(EigenLayerError):
    """Raised when API rate limits are exceeded."""
    pass


class CacheError(EigenLayerError):
    """Raised when cache operations fail."""
    pass


class DatabaseError(EigenLayerError):
    """Raised when database operations fail."""
    pass


class QueryError(EigenLayerError):
    """Raised when query execution fails."""
    pass


class ConfigurationError(EigenLayerError):
    """Raised when configuration is invalid."""
    pass


class NetworkError(EigenLayerError):
    """Raised when network operations fail."""
    pass