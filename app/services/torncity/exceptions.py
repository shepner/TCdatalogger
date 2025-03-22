"""Torn City API exceptions.

This module defines a hierarchy of exceptions for handling various error conditions
that may occur when interacting with the Torn City API, processing data, or
storing results.

Exception Hierarchy:
    TornAPIError (base)
    ├── EndpointError
    ├── TornAuthError
    ├── TornAPIRateLimitError
    ├── TornServerError
    ├── TornClientError
    ├── TornDataError
    ├── SchemaError
    ├── ProcessingError
    ├── StorageError
    ├── DataValidationError
    ├── TornAPIKeyError
    ├── TornAPIConnectionError
    └── TornAPITimeoutError
"""

class TornAPIError(Exception):
    """Base exception for Torn City API errors.
    
    This is the parent class for all Torn City API related exceptions.
    It adds support for error codes and formatted error messages.
    
    Args:
        message: Human-readable error description
        code: Optional error code from the API
    """
    def __init__(self, message: str, code: int = None):
        self.code = code
        super().__init__(message)

    def __str__(self):
        if self.code:
            return f"[{self.code}] {super().__str__()}"
        return super().__str__()

class EndpointError(TornAPIError):
    """Exception raised for endpoint-specific errors.
    
    Raised when an error occurs that is specific to a particular API endpoint,
    such as invalid parameters or unsupported operations.
    """
    pass

class TornAuthError(TornAPIError):
    """Exception raised for authentication errors.
    
    Raised when there are issues with API authentication, such as:
    - Invalid API key
    - Expired API key
    - Insufficient permissions
    """
    pass

class TornAPIRateLimitError(TornAPIError):
    """Exception raised when API rate limit is exceeded.
    
    Raised when the application has made too many requests and needs to
    wait before making more requests.
    """
    pass

class TornServerError(TornAPIError):
    """Exception raised for server-side errors.
    
    Raised when the Torn API server encounters an error (5xx status codes).
    """
    pass

class TornClientError(TornAPIError):
    """Exception raised for client-side errors.
    
    Raised when the client makes an invalid request (4xx status codes).
    """
    pass

class TornDataError(TornAPIError):
    """Exception raised for data validation errors.
    
    Raised when the data received from the API fails validation checks,
    such as missing required fields or invalid data types.
    """
    pass

class SchemaError(TornAPIError):
    """Exception raised for schema validation errors.
    
    Raised when the data structure does not match the expected schema,
    such as missing columns or incompatible data types.
    """
    pass

class ProcessingError(TornAPIError):
    """Exception raised for data processing errors.
    
    Raised when an error occurs during data transformation or processing,
    such as calculation errors or data format issues.
    """
    pass

class StorageError(TornAPIError):
    """Exception raised for storage-related errors.
    
    Raised when an error occurs while storing or retrieving data,
    such as database connection issues or write failures.
    """
    pass

class DataValidationError(TornAPIError):
    """Exception raised for data validation errors.
    
    Raised when data fails business logic validation rules,
    such as out-of-range values or invalid combinations.
    """
    pass

class TornAPIKeyError(TornAPIError):
    """Raised when there are issues with API keys.
    
    Raised when there are problems with API key management,
    such as missing keys or key rotation failures.
    """
    pass

class TornAPIConnectionError(TornAPIError):
    """Raised when connection to the API fails.
    
    Raised when the application cannot establish a connection
    to the Torn API, such as network issues or DNS failures.
    """
    pass

class TornAPITimeoutError(TornAPIError):
    """Raised when API request times out.
    
    Raised when an API request takes longer than the specified
    timeout period to complete.
    """
    pass 