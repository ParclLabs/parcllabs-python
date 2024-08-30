class ParclLabsException(Exception):
    """Base exception class for ParclLabs SDK"""

    pass


class NotFoundError(ParclLabsException):
    """Exception raised when no data is found matching search criteria (404 error)."""

    def __init__(
        self,
        message="No data found matching search criteria. Try a different set of parameters.",
        *args,
        **kwargs,
    ):
        super().__init__(message, *args, **kwargs)


class DataValidationError(ParclLabsException):
    """Exception raised for data validation errors (422 error)."""

    def __init__(
        self, message="Data validation error occurred.", details=None, *args, **kwargs
    ):
        self.details = details
        super().__init__(message, *args, **kwargs)

    def __str__(self):
        if self.details:
            return f"{super().__str__()}\nDetails: {self.details}"
        return super().__str__()
