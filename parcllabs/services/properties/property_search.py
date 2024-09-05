from collections import deque
import pandas as pd
from alive_progress import alive_bar
from typing import List
from parcllabs.services.validators import Validators
from parcllabs.common import VALID_PROPERTY_TYPES_UNIT_SEARCH, VALID_ENTITY_NAMES
from parcllabs.services.streaming.parcl_labs_streaming_service import (
    ParclLabsStreamingService,
)


class PropertySearch(ParclLabsStreamingService):
    """
    Retrieve parcl_property_id for geographic markets in the Parcl Labs API.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrieve(
        self,
        parcl_ids: List[int],
        property_type: str,
        square_footage_min: int = None,
        square_footage_max: int = None,
        bedrooms_min: int = None,
        bedrooms_max: int = None,
        bathrooms_min: int = None,
        bathrooms_max: int = None,
        year_built_min: int = None,
        year_built_max: int = None,
        current_entity_owner_name: str = None,
        event_history_sale_flag: bool = None,
        event_history_rental_flag: bool = None,
        event_history_listing_flag: bool = None,
        current_new_construction_flag: bool = None,
        current_owner_occupied_flag: bool = None,
        current_investor_owned_flag: bool = None,
    ):
        params = {}

        params = Validators.validate_input_str_param(
            param=property_type,
            param_name="property_type",
            valid_values=VALID_PROPERTY_TYPES_UNIT_SEARCH,
            params_dict=params,
        )

        params = Validators.validate_input_str_param(
            param=current_entity_owner_name,
            param_name="current_entity_owner_name",
            valid_values=VALID_ENTITY_NAMES,
            params_dict=params,
        )

        params = Validators.validate_input_bool_param(
            param=event_history_sale_flag,
            param_name="event_history_sale_flag",
            params_dict=params,
        )

        params = Validators.validate_input_bool_param(
            param=event_history_rental_flag,
            param_name="event_history_rental_flag",
            params_dict=params,
        )

        params = Validators.validate_input_bool_param(
            param=event_history_listing_flag,
            param_name="event_history_listing_flag",
            params_dict=params,
        )

        params = Validators.validate_input_bool_param(
            param=current_new_construction_flag,
            param_name="current_new_construction_flag",
            params_dict=params,
        )

        params = Validators.validate_input_bool_param(
            param=current_owner_occupied_flag,
            param_name="current_owner_occupied_flag",
            params_dict=params,
        )

        params = Validators.validate_input_bool_param(
            param=current_investor_owned_flag,
            param_name="current_investor_owned_flag",
            params_dict=params,
        )

        if bedrooms_max:
            params["bedrooms_max"] = bedrooms_max

        if bedrooms_min:
            params["bedrooms_min"] = bedrooms_min

        if bathrooms_max:
            params["bathrooms_max"] = bathrooms_max

        if bathrooms_min:
            params["bathrooms_min"] = bathrooms_min

        if year_built_max:
            params["year_built_max"] = year_built_max

        if year_built_min:
            params["year_built_min"] = year_built_min

        if square_footage_max:
            params["square_footage_max"] = square_footage_max

        if square_footage_min:
            params["square_footage_min"] = square_footage_min

        output_data = deque()
        total_parcl_ids = len(parcl_ids)

        with alive_bar(total_parcl_ids, title="Processing Parcl IDs") as bar:
            for parcl_id in parcl_ids:
                params["parcl_id"] = parcl_id
                response = self._get(url=self.full_url, params=params)
                data = response.text
                df_container = pd.DataFrame()

                for df_batch in self._process_streaming_data(
                    data, batch_size=10000, num_workers=self.client.num_workers
                ):
                    df_container = pd.concat(
                        [df_container, df_batch], ignore_index=True
                    )

                output_data.append(df_container)
                bar()

        results = pd.concat(output_data).reset_index(drop=True)
        self.client.estimated_session_credit_usage += results.shape[0]
        return results
