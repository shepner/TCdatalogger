"""Torn City API exceptions."""

class TornAPIError(Exception):
    """Base exception for Torn City API errors."""
    def __init__(self, message: str, code: int = None):
        self.code = code
        super().__init__(message)

    def __str__(self):
        if self.code:
            return f"[{self.code}] {super().__str__()}"
        return super().__str__()

class EndpointError(TornAPIError):
    """Exception raised for endpoint-specific errors."""
    pass

class TornAuthError(TornAPIError):
    """Exception raised for authentication errors."""
    pass

class TornAPIRateLimitError(TornAPIError):
    """Exception raised when API rate limit is exceeded."""
    pass

class TornServerError(TornAPIError):
    """Exception raised for server-side errors."""
    pass

class TornClientError(TornAPIError):
    """Exception raised for client-side errors."""
    pass

class TornDataError(TornAPIError):
    """Exception raised for data validation errors."""
    pass

class SchemaError(TornAPIError):
    """Exception raised for schema validation errors."""
    pass

class ProcessingError(TornAPIError):
    """Exception raised for data processing errors."""
    pass

class StorageError(TornAPIError):
    """Exception raised for storage-related errors."""
    pass

class DataValidationError(TornAPIError):
    """Exception raised for data validation errors."""
    pass

class TornAPIKeyError(TornAPIError):
    """Raised when there are issues with API keys."""
    pass

class TornAPIConnectionError(TornAPIError):
    """Raised when connection to the API fails."""
    pass

class TornAPITimeoutError(TornAPIError):
    """Raised when API request times out."""
    pass 