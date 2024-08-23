import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from typing import Any, Mapping, Optional, List
from requests.exceptions import RequestException
from parcllabs.services.data_utils  import (
    validate_input_str_param,
    validate_input_bool_param
)
from parcllabs.common import DEFAULT_LIMIT, VALID_PROPERTY_TYPES, VALID_ENTITY_NAMES
from parcllabs.services.parcllabs_service import ParclLabsService


class PropertySearch(ParclLabsService):
    """
    Retrieve parcl_property_id for geographic markets in the Parcl Labs API.
    """

    def __init__(self, limit: int = DEFAULT_LIMIT, *args, **kwargs):
        super().__init__(limit=limit, *args, **kwargs)

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        return pd.DataFrame(data)

    def retrieve(
        self,
        zip: str,
        zips: List[str] = None,
        sq_ft_min: int = None,
        sq_ft_max: int = None,
        bedrooms_min: int = None,
        bedrooms_max: int = None,
        bathrooms_min: int = None,
        bathrooms_max: int = None,
        year_built_min: int = None,
        year_built_max: int = None,
        property_type: str = None,
        params: Optional[Mapping[str, Any]] = None,
    ):
        """
        Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.

        Args:

            zip (str): The 5 digit zip code to filter results by.
            sq_ft_min (int, optional): The minimum square footage to filter results by.
            sq_ft_max (int, optional): The maximum square footage to filter results by.
            bedrooms_min (int, optional): The minimum number of bedrooms to filter results by.
            bedrooms_max (int, optional): The maximum number of bedrooms to filter results by.
            bathrooms_min (int, optional): The minimum number of bathrooms to filter results by.
            bathrooms_max (int, optional): The maximum number of bathrooms to filter results by.
            year_built_min (int, optional): The minimum year built to filter results by.
            year_built_max (int, optional): The maximum year built to filter results by.
            property_type (str, optional): The property type to filter results by.
            params (dict, optional): Additional parameters to include in the request.

        Returns:

            Any: The JSON response as a pandas DataFrame.
        """

        if zip and not zips:
            zips = [zip]
        
        if zip and zips:
            raise ValueError(
                f"zip and zips cannot be used together. Please use one or the other."
            )
        
        property_type = validate_input_str_param(
            param=property_type, 
            param_name='property_type', 
            valid_values=VALID_PROPERTY_TYPES
        )
        
        output_data = []

        for zip in zips:

            try:

                params = {
                    "zip5": zip,
                    "square_footage_min": sq_ft_min,
                    "square_footage_max": sq_ft_max,
                    "bedrooms_min": bedrooms_min,
                    "bedrooms_max": bedrooms_max,
                    "bathrooms_min": bathrooms_min,
                    "bathrooms_max": bathrooms_max,
                    "year_built_min": year_built_min,
                    "year_built_max": year_built_max,
                    "property_type": property_type,
                    **(params or {}),
                }
                results = self._sync_request(params=params)
                data = self._as_pd_dataframe(results)
                output_data.append(data)
            
            except RequestException as e:
                continue

        data = pd.concat(output_data)
        return data


class PropertySearchNew(ParclLabsService):
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
        current_investor_owned_flag: bool = None
    ):
        """
        Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.

        Args:

            parcl_id (int): Market parcl_id to retrieve units within.
            square_footage_min (int, optional): The minimum square footage to filter results by.
            square_footage_max (int, optional): The maximum square footage to filter results by.
            bedrooms_min (int, optional): The minimum number of bedrooms to filter results by.
            bedrooms_max (int, optional): The maximum number of bedrooms to filter results by.
            bathrooms_min (int, optional): The minimum number of bathrooms to filter results by.
            bathrooms_max (int, optional): The maximum number of bathrooms to filter results by.
            year_built_min (int, optional): The minimum year built to filter results by.
            year_built_max (int, optional): The maximum year built to filter results by.
            current_entity_owner_name (str, optional): The current entity owner name to filter results by.
            event_history_sale_flag (bool, optional): The event history sale flag to filter results by.
            event_history_rental_flag (bool, optional): The event history rental flag to filter results by.
            event_history_listing_flag (bool, optional): The event history listing flag to filter results by.
            current_new_construction_flag (bool, optional): The current new construction flag to filter results by.
            current_owner_occupied_flag (bool, optional): The current owner occupied flag to filter results by.
            current_investor_owned_flag (bool, optional): The current investor owned flag to filter results by.
            property_type (str, optional): The property type to filter results by.
            params (dict, optional): Additional parameters to include in the request.

        Returns:

            Any: The JSON response as a pandas DataFrame.
        """

        params = {}
        
        params = validate_input_str_param(
            param=property_type, 
            param_name='property_type', 
            valid_values=VALID_PROPERTY_TYPES,
            params_dict=params
        )

        params = validate_input_str_param(
            param=current_entity_owner_name, 
            param_name='current_entity_owner_name', 
            valid_values=VALID_ENTITY_NAMES,
            params_dict=params
        )

        params = validate_input_bool_param(
            param=event_history_sale_flag, 
            param_name='event_history_sale_flag',
            params_dict=params
        )

        params = validate_input_bool_param(
            param=event_history_rental_flag, 
            param_name='event_history_rental_flag',
            params_dict=params
        )

        params = validate_input_bool_param(
            param=event_history_listing_flag, 
            param_name='event_history_listing_flag',
            params_dict=params
        )

        params = validate_input_bool_param(
            param=current_new_construction_flag, 
            param_name='current_new_construction_flag',
            params_dict=params
        )

        params = validate_input_bool_param(
            param=current_owner_occupied_flag, 
            param_name='current_owner_occupied_flag',
            params_dict=params
        )

        params = validate_input_bool_param(
            param=current_investor_owned_flag, 
            param_name='current_investor_owned_flag',
            params_dict=params
        )

        if bedrooms_max:
            params['bedrooms_max'] = bedrooms_max

        if bedrooms_min:
            params['bedrooms_min'] = bedrooms_min
        
        if bathrooms_max:
            params['bathrooms_max'] = bathrooms_max

        if bathrooms_min:
            params['bathrooms_min'] = bathrooms_min

        if year_built_max:
            params['year_built_max'] = year_built_max

        if year_built_min:
            params['year_built_min'] = year_built_min

        if square_footage_max:
            params['square_footage_max'] = square_footage_max

        if square_footage_min:
            params['square_footage_min'] = square_footage_min

        output_data = []

        for parcl_id in parcl_ids:
            params['parcl_id'] = parcl_id
            headers = self._get_headers()
            data = fetch_data(self.full_url, headers=headers, params=params)
            tmp = pd.DataFrame()

            for df_batch in process_data(data, batch_size=10000):
                tmp = pd.concat([tmp, df_batch], ignore_index=True)

            output_data.append(tmp)
        
        results = pd.concat(output_data)
        self.client.estimated_session_credit_usage += results.shape[0]
        return results
    

def fetch_data(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    print("Data fetched")
    return response.text

def process_chunk(chunk):
    try:
        return json.loads(chunk)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

def process_data(data, batch_size=10000):
    print("Processing data")
    with ThreadPoolExecutor() as executor:
        chunks = data.strip().split('\n')
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks if chunk]
        
        buffer = []
        for future in as_completed(futures):
            result = future.result()
            if result:
                buffer.append(result)
            
            if len(buffer) >= batch_size:
                yield pd.DataFrame(buffer)
                buffer = []
        
        if buffer:
            yield pd.DataFrame(buffer)
