import pytest
from datetime import datetime
from parcllabs.common import VALID_PORTFOLIO_SIZES, VALID_PROPERTY_TYPES
from parcllabs.services.validators import (
    Validators,
)


def test_validate_date():
    # Test valid date
    valid_date = "2023-06-18"
    assert Validators.validate_date(valid_date) == valid_date

    # Test invalid date format
    with pytest.raises(ValueError) as excinfo:
        Validators.validate_date("18-06-2023")
    assert (
        str(excinfo.value) == "Date 18-06-2023 is not in the correct format YYYY-MM-DD."
    )

    # Test invalid date
    with pytest.raises(ValueError) as excinfo:
        Validators.validate_date("2023-02-30")
    assert (
        str(excinfo.value) == "Date 2023-02-30 is not in the correct format YYYY-MM-DD."
    )


def test_validate_property_type():
    # Test valid property type
    valid_property_type = "single_family"
    assert Validators.validate_property_type(valid_property_type) == "SINGLE_FAMILY"

    # Test invalid property type
    with pytest.raises(ValueError) as excinfo:
        Validators.validate_property_type("villa")
    assert (
        str(excinfo.value)
        == f"Property type VILLA is not valid. Must be one of {', '.join(VALID_PROPERTY_TYPES)}."
    )


def test_validate_portfolio_size():
    # Test valid portfolio size
    valid_portfolio_size = "portfolio_1000_plus"
    assert (
        Validators.validate_portfolio_size(valid_portfolio_size)
        == "PORTFOLIO_1000_PLUS"
    )

    # Test invalid portfolio size
    with pytest.raises(ValueError) as excinfo:
        Validators.validate_portfolio_size("extra-large")
    assert (
        str(excinfo.value)
        == f"Portfolio size EXTRA-LARGE is not valid. Must be one of {', '.join(VALID_PORTFOLIO_SIZES)}."
    )


def test_validate_from_list():
    # Testing the _validate_from_list method directly (if needed)
    valid_list = ["ONE", "TWO", "THREE"]

    # Test valid value
    valid_value = "one"
    assert (
        Validators._validate_from_list(valid_value, valid_list, "Test value") == "ONE"
    )

    # Test invalid value
    with pytest.raises(ValueError) as excinfo:
        Validators._validate_from_list("four", valid_list, "Test value")
    assert (
        str(excinfo.value)
        == "Test value FOUR is not valid. Must be one of ONE, TWO, THREE."
    )
