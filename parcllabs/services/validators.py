from datetime import datetime
from typing import List
from parcllabs.common import (
    VALID_PORTFOLIO_SIZES,
    VALID_PROPERTY_TYPES,
)


class Validators:
    @staticmethod
    def validate_date(date_str: str) -> str:
        """
        Validates the date string and returns it in the 'YYYY-MM-DD' format.
        Raises ValueError if the date is invalid or not in the expected format.
        """
        if date_str:
            try:
                formatted_date = datetime.strptime(date_str, "%Y-%m-%d").strftime(
                    "%Y-%m-%d"
                )
                return formatted_date
            except ValueError:
                raise ValueError(
                    f"Date {date_str} is not in the correct format YYYY-MM-DD."
                )

    @staticmethod
    def validate_property_type(property_type: str) -> str:
        return Validators._validate_from_list(
            property_type, VALID_PROPERTY_TYPES, "Property type"
        )

    @staticmethod
    def validate_portfolio_size(portfolio_size: str) -> str:
        """
        Validates the portfolio size string and returns it in the expected format.
        Raises ValueError if the portfolio size is invalid or not in the expected format.
        """
        return Validators._validate_from_list(
            portfolio_size, VALID_PORTFOLIO_SIZES, "Portfolio size"
        )

    @staticmethod
    def _validate_from_list(value: str, valid_list: List[str], value_type: str) -> str:
        if value:
            value = value.strip().upper()
            if value.lower() not in [v.lower() for v in valid_list]:
                raise ValueError(
                    f"{value_type} {value} is not valid. Must be one of {', '.join(valid_list)}."
                )
        return value
