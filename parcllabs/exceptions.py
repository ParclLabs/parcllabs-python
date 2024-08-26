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
