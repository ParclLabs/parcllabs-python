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

    @staticmethod
    def validate_input_str_param(
        param: str, param_name: str, valid_values: List[str], params_dict: dict = None
    ):
        if param:

            param = param.upper()

            params_dict[param_name] = param

            if param not in valid_values:
                raise ValueError(
                    f"{param_name} value error. Valid values are: {valid_values}. Received: {param}"
                )

        return params_dict

    @staticmethod
    def validate_input_bool_param(param, param_name: str, params_dict: dict = None):
        if param is None:
            return params_dict  # or return None if that’s preferred

        if not isinstance(param, bool):
            raise ValueError(
                f"{param_name} value error. Expected boolean. Received: {param}"
            )

        params_dict[param_name] = "true" if param else "false"
        return params_dict

    @staticmethod
    def validate_parcl_ids(parcl_ids):
        if isinstance(parcl_ids, int):
            parcl_ids = [parcl_ids]
        return parcl_ids

    @staticmethod
    def validate_us_zip_code(zip_code: str) -> str:
        """
        Validates the US zip code string and returns it in the expected format.
        Raises ValueError if the zip code is invalid or not in the expected format.
        """
        zip_code = zip_code.strip()
        if not zip_code.isdigit() or len(zip_code) != 5:
            raise ValueError(f"Zip code {zip_code} is not a valid 5-digit US zip code.")
        return zip_code

    @staticmethod
    def validate_field_exists(data: dict, fields: List[str]):
        """
        Validates that the required fields exist in the provided dictionary.
        """
        missing_fields = [field for field in fields if field not in data.keys()]
        if missing_fields:
            raise ValueError(
                f"Missing required fields: {', '.join(missing_fields)}. Provided request: {data}"
            )
