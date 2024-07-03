import pandas as pd
from typing import Any, Mapping, Optional, List
from parcllabs.common import (
    DEFAULT_LIMIT,
    VALID_PROPERTY_TYPES,
)
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
            auto_paginate (bool, optional): Automatically paginate through the results.

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
