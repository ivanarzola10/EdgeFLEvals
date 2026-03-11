class EdgeFLError(Exception):
    """Base exception for all EdgeFL errors."""
    def __init__(self, message: str, original_error: Exception = None, context: dict = None):
        super().__init__(message)
        self.original_error = original_error
        self.context = context or {}

class EdgeFLConnectionError(EdgeFLError):
    """Raised when a network connection fails (e.g., to the aggregator or blockchain)."""
    pass

class EdgeFLValidationError(EdgeFLError):
    """Raised when data is invalid or missing."""
    pass

class EdgeFLTimeoutError(EdgeFLError):
    """Raised when a network request times out."""
    pass

class EdgeFLFileError(EdgeFLError):
    """Raised when file read/write operations fail."""
    pass
