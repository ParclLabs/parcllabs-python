class ParclLabsError(Exception):
    """Base exception class for ParclLabs SDK"""

    pass


class NotFoundError(ParclLabsError):
    """Exception raised when no data is found matching search criteria (404 error)."""

    def __init__(
        self,
        message: str = (
            "No data found matching search criteria. Try a different set of parameters."
        ),
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(message, *args, **kwargs)


class DataValidationError(ParclLabsError):
    """Exception raised for data validation errors (422 error)."""

    def __init__(
        self,
        message: str = "Data validation error occurred.",
        details: object = None,
        *args: object,
        **kwargs: object,
    ) -> None:
        self.details = details
        super().__init__(message, *args, **kwargs)

    def __str__(self) -> str:
        if self.details:
            return f"{super().__str__()}\nDetails: {self.details}"
        return super().__str__()
