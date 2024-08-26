import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import pandas as pd
from alive_progress import alive_bar
from typing import List
from parcllabs.services.data_utils import (
    validate_input_str_param,
    validate_input_bool_param,
)
from parcllabs.common import VALID_PROPERTY_TYPES, VALID_ENTITY_NAMES
from parcllabs.services.parcllabs_service import ParclLabsService


class PropertySearch(ParclLabsService):
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

        params = validate_input_str_param(
            param=property_type,
            param_name="property_type",
            valid_values=VALID_PROPERTY_TYPES,
            params_dict=params,
        )

        params = validate_input_str_param(
            param=current_entity_owner_name,
            param_name="current_entity_owner_name",
            valid_values=VALID_ENTITY_NAMES,
            params_dict=params,
        )

        params = validate_input_bool_param(
            param=event_history_sale_flag,
            param_name="event_history_sale_flag",
            params_dict=params,
        )

        params = validate_input_bool_param(
            param=event_history_rental_flag,
            param_name="event_history_rental_flag",
            params_dict=params,
        )

        params = validate_input_bool_param(
            param=event_history_listing_flag,
            param_name="event_history_listing_flag",
            params_dict=params,
        )

        params = validate_input_bool_param(
            param=current_new_construction_flag,
            param_name="current_new_construction_flag",
            params_dict=params,
        )

        params = validate_input_bool_param(
            param=current_owner_occupied_flag,
            param_name="current_owner_occupied_flag",
            params_dict=params,
        )

        params = validate_input_bool_param(
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
                headers = self._get_headers()
                data = fetch_data(self.full_url, headers=headers, params=params)
                df_container = pd.DataFrame()

                for df_batch in process_data(
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


def fetch_data(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.text


def process_chunk(chunk):
    try:
        return json.loads(chunk)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def process_data(data, batch_size=10000, num_workers=None):
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        chunks = deque(data.strip().split("\n"))
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks if chunk]

        buffer = deque()
        for future in as_completed(futures):
            result = future.result()
            if result:
                buffer.append(result)

            if len(buffer) >= batch_size:
                yield pd.DataFrame(buffer)
                buffer.clear()

        if buffer:
            yield pd.DataFrame(buffer)
