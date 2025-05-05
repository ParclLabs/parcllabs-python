import pytest

from parcllabs.services.validators import (
    Validators,
)


def test_validate_date() -> None:
    # Test valid date
    valid_date = "2023-06-18"
    assert Validators.validate_date(valid_date) == valid_date

    # Test invalid date format
    with pytest.raises(ValueError) as excinfo:
        Validators.validate_date("18-06-2023")
    assert str(excinfo.value) == "Date 18-06-2023 is not in the correct format YYYY-MM-DD."

    # Test invalid date
    with pytest.raises(ValueError) as excinfo:
        Validators.validate_date("2023-02-30")
    assert str(excinfo.value) == "Date 2023-02-30 is not in the correct format YYYY-MM-DD."
