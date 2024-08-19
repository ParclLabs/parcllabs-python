import pandas as pd
from typing import Any, Mapping, Optional, List
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

        if property_type and property_type not in VALID_PROPERTY_TYPES:
            raise ValueError(
                f"property_type value error. Valid values are: {VALID_PROPERTY_TYPES}. Received: {property_type}"
            )

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
        return data


class PropertySearchNew(ParclLabsService):
    """
    Retrieve parcl_property_id for geographic markets in the Parcl Labs API.
    """

    def __init__(self, limit: int = DEFAULT_LIMIT, *args, **kwargs):
        super().__init__(limit=limit, *args, **kwargs)

    def retrieve(
        self,
        parcl_id: int,
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
        current_entity_owner_name: str = None,
        event_history_sale_flag: bool = None,
        event_history_rental_flag: bool = None,
        event_history_listing_flag: bool = None,
        current_new_construction_flag: bool = None,
        current_owner_occupied_flag: bool = None,
        current_investor_owned_flag: bool = None,
        limit: int = 1000,
        auto_paginate: bool = False,
    ):
        """
        Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.

        Args:

            parcl_id (int): Market parcl_id to retrieve units within.
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

        if property_type:
            property_type = property_type.lower()
            if property_type not in VALID_PROPERTY_TYPES:
                raise ValueError(
                    f"property_type value error. Valid values are: {VALID_PROPERTY_TYPES}. Received: {property_type}"
                )

        if current_entity_owner_name:
            current_entity_owner_name = current_entity_owner_name.upper()
            if current_entity_owner_name not in VALID_ENTITY_NAMES:
                raise ValueError(
                    f"current_entity_owner_name value error. Valid values are: {VALID_ENTITY_NAMES}. Received: {current_entity_owner_name}"
                )

        if event_history_sale_flag is not None:
            event_history_sale_flag = "true" if event_history_sale_flag else "false"

        if event_history_rental_flag is not None:
            event_history_rental_flag = "true" if event_history_rental_flag else "false"

        if event_history_listing_flag is not None:
            event_history_listing_flag = (
                "true" if event_history_listing_flag else "false"
            )

        if current_new_construction_flag is not None:
            current_new_construction_flag = (
                "true" if current_new_construction_flag else "false"
            )

        if current_owner_occupied_flag is not None:
            current_owner_occupied_flag = (
                "true" if current_owner_occupied_flag else "false"
            )

        if current_investor_owned_flag is not None:
            current_investor_owned_flag = (
                "true" if current_investor_owned_flag else "false"
            )

        params = {
            "parcl_id": parcl_id,
            "square_footage_min": sq_ft_min,
            "square_footage_max": sq_ft_max,
            "bedrooms_min": bedrooms_min,
            "bedrooms_max": bedrooms_max,
            "bathrooms_min": bathrooms_min,
            "bathrooms_max": bathrooms_max,
            "year_built_min": year_built_min,
            "year_built_max": year_built_max,
            "property_type": property_type,
            "current_entity_owner_name": current_entity_owner_name,
            "event_history_sale_flag": event_history_sale_flag,
            "event_history_rental_flag": event_history_rental_flag,
            "event_history_listing_flag": event_history_listing_flag,
            "current_new_construction_flag": current_new_construction_flag,
            "current_owner_occupied_flag": current_owner_occupied_flag,
            "current_investor_owned_flag": current_investor_owned_flag,
            "limit": limit,
            **(params or {}),
        }
        results = self._sync_request(params=params)

        if auto_paginate:
            tmp = results.copy()
            while results["links"].get("next"):
                results = self._sync_request(url=results["links"]["next"], is_next=True)
                tmp["items"].extend(results["items"])
            tmp["links"] = results["links"]
            results = tmp

        results = pd.json_normalize(results["items"])
        # data = self._as_pd_dataframe(results)
        return results
