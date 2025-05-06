from collections import deque

import pandas as pd

from parcllabs.exceptions import NotFoundError
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertySearch(ParclLabsService):
    """
    Retrieve parcl_property_id for geographic markets in the Parcl Labs API.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def _handle_numeric_params(self, params: dict, **kwargs: object) -> dict:
        """Handle numeric parameters for property search."""
        numeric_params = [
            "bedrooms_max",
            "bedrooms_min",
            "bathrooms_max",
            "bathrooms_min",
            "year_built_max",
            "year_built_min",
            "square_footage_max",
            "square_footage_min",
        ]

        for param in numeric_params:
            if kwargs.get(param) is not None:
                params[param] = kwargs[param]

        return params

    def _handle_date_params(
        self,
        params: dict,
        record_added_date_start: str | None = None,
        record_added_date_end: str | None = None,
    ) -> dict:
        """Handle date parameters for property search."""
        if record_added_date_start:
            record_added_date_start = Validators.validate_date(record_added_date_start)
            params["record_added_date_start"] = record_added_date_start
        if record_added_date_end:
            record_added_date_end = Validators.validate_date(record_added_date_end)
            params["record_added_date_end"] = record_added_date_end

        return params

    def _prepare_params(
        self,
        property_type: str,
        square_footage_min: int | None = None,
        square_footage_max: int | None = None,
        bedrooms_min: int | None = None,
        bedrooms_max: int | None = None,
        bathrooms_min: int | None = None,
        bathrooms_max: int | None = None,
        year_built_min: int | None = None,
        year_built_max: int | None = None,
        current_entity_owner_name: str | None = None,
        event_history_sale_flag: bool | None = None,
        event_history_rental_flag: bool | None = None,
        event_history_listing_flag: bool | None = None,
        current_new_construction_flag: bool | None = None,
        current_owner_occupied_flag: bool | None = None,
        current_investor_owned_flag: bool | None = None,
        record_added_date_start: str | None = None,
        record_added_date_end: str | None = None,
        current_on_market_flag: bool | None = None,
    ) -> dict:
        params = {}

        if property_type:
            params["property_type"] = property_type.upper()

        if current_entity_owner_name:
            params["current_entity_owner_name"] = current_entity_owner_name.upper()

        # Handle boolean flags
        bool_flags = {
            "event_history_sale_flag": event_history_sale_flag,
            "event_history_rental_flag": event_history_rental_flag,
            "event_history_listing_flag": event_history_listing_flag,
            "current_new_construction_flag": current_new_construction_flag,
            "current_owner_occupied_flag": current_owner_occupied_flag,
            "current_investor_owned_flag": current_investor_owned_flag,
            "current_on_market_flag": current_on_market_flag,
        }

        for param_name, param_value in bool_flags.items():
            params = Validators.validate_input_bool_param(
                param=param_value,
                param_name=param_name,
                params_dict=params,
            )

        # Handle numeric parameters
        params = self._handle_numeric_params(
            params,
            bedrooms_max=bedrooms_max,
            bedrooms_min=bedrooms_min,
            bathrooms_max=bathrooms_max,
            bathrooms_min=bathrooms_min,
            year_built_max=year_built_max,
            year_built_min=year_built_min,
            square_footage_max=square_footage_max,
            square_footage_min=square_footage_min,
        )

        # Handle date parameters
        return self._handle_date_params(
            params,
            record_added_date_start=record_added_date_start,
            record_added_date_end=record_added_date_end,
        )

    def retrieve(
        self,
        parcl_ids: list[int],
        property_type: str,
        square_footage_min: int | None = None,
        square_footage_max: int | None = None,
        bedrooms_min: int | None = None,
        bedrooms_max: int | None = None,
        bathrooms_min: int | None = None,
        bathrooms_max: int | None = None,
        year_built_min: int | None = None,
        year_built_max: int | None = None,
        current_entity_owner_name: str | None = None,
        event_history_sale_flag: bool | None = None,
        event_history_rental_flag: bool | None = None,
        event_history_listing_flag: bool | None = None,
        current_new_construction_flag: bool | None = None,
        current_owner_occupied_flag: bool | None = None,
        current_investor_owned_flag: bool | None = None,
        record_added_date_start: str | None = None,
        record_added_date_end: str | None = None,
        current_on_market_flag: bool | None = None,
    ) -> pd.DataFrame:
        """
        Retrieve parcl_property_id for geographic markets based on specified criteria.

        Args:
            parcl_ids (List[int]): List of Parcl IDs for the geographic markets.
            property_type (str): The type of property to search for
            (e.g., 'single_family').
            square_footage_min (int, optional): Minimum square footage.
            Defaults to None.
            square_footage_max (int, optional): Maximum square footage.
            Defaults to None.
            bedrooms_min (int, optional): Minimum number of bedrooms. Defaults to None.
            bedrooms_max (int, optional): Maximum number of bedrooms. Defaults to None.
            bathrooms_min (int, optional): Minimum number of bathrooms.
            Defaults to None.
            bathrooms_max (int, optional): Maximum number of bathrooms.
            Defaults to None.
            year_built_min (int, optional): Minimum year built. Defaults to None.
            year_built_max (int, optional): Maximum year built. Defaults to None.
            current_entity_owner_name (str, optional): Filter by current owner entity
            name. Defaults to None.
            event_history_sale_flag (bool, optional): Filter properties with sale events
            in history. Defaults to None.
            event_history_rental_flag (bool, optional): Filter properties with rental
            events in history. Defaults to None.
            event_history_listing_flag (bool, optional): Filter properties with listing
            events in history. Defaults to None.
            current_new_construction_flag (bool, optional): Filter properties that are
            new construction. Defaults to None.
            current_owner_occupied_flag (bool, optional): Filter properties that are
            owner-occupied. Defaults to None.
            current_investor_owned_flag (bool, optional): Filter properties that are
            investor-owned. Defaults to None.
            record_added_date_start (str, optional): Filter properties added on or after
            this date (YYYY-MM-DD). Defaults to None.
            record_added_date_end (str, optional): Filter properties added on or before
            this date (YYYY-MM-DD). Defaults to None.
            current_on_market_flag (bool, optional): Filter properties currently on the
            market. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the parcl_property_id and other detail
            for matching properties. Returns an empty DataFrame if no data is found for
            any market.
        """
        params = self._prepare_params(
            property_type=property_type,
            square_footage_min=square_footage_min,
            square_footage_max=square_footage_max,
            bedrooms_min=bedrooms_min,
            bedrooms_max=bedrooms_max,
            bathrooms_min=bathrooms_min,
            bathrooms_max=bathrooms_max,
            year_built_min=year_built_min,
            year_built_max=year_built_max,
            current_entity_owner_name=current_entity_owner_name,
            event_history_sale_flag=event_history_sale_flag,
            event_history_rental_flag=event_history_rental_flag,
            event_history_listing_flag=event_history_listing_flag,
            current_new_construction_flag=current_new_construction_flag,
            current_owner_occupied_flag=current_owner_occupied_flag,
            current_investor_owned_flag=current_investor_owned_flag,
            record_added_date_start=record_added_date_start,
            record_added_date_end=record_added_date_end,
            current_on_market_flag=current_on_market_flag,
        )

        output_data = deque()
        markets_with_no_data = []

        for parcl_id in parcl_ids:
            try:
                params["parcl_id"] = parcl_id
                response = self._get(url=self.full_url, params=params)
                data = response.json()
                df_container = pd.DataFrame(data.get("items"))
                self._update_account_info(data.get("account"))
                output_data.append(df_container)
            except NotFoundError:
                # Track markets with no data
                markets_with_no_data.append(parcl_id)
                continue
            except Exception:
                # For other exceptions, simply re-raise without specifying
                raise

        if not output_data:
            # If no data was found for any market, return empty DataFrame
            return pd.DataFrame()

        results = pd.concat(output_data).reset_index(drop=True)

        # Print message about markets with no data if any exist
        if markets_with_no_data:
            print(
                f"No data found for markets with parcl_ids: "
                f"{', '.join(map(str, markets_with_no_data))}"
            )

        return results
