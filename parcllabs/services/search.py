import pandas as pd
from typing import Any, Mapping, Optional, List
from parcllabs.common import (
    VALID_LOCATION_TYPES,
    VALID_US_REGIONS,
    VALID_US_STATE_ABBREV,
    VALID_US_STATE_FIPS_CODES,
    VALID_SORT_BY,
    VALID_SORT_ORDER,
)
from parcllabs.services.validators import Validators
from parcllabs.services.parcllabs_service import ParclLabsService


class SearchMarkets(ParclLabsService):
    """
    Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        return pd.DataFrame(data)

    def retrieve(
        self,
        query: Optional[str] = None,
        location_type: str = None,
        region: str = None,
        state_abbreviation: str = None,
        state_fips_code: str = None,
        parcl_id: int = None,
        geoid: str = None,
        sort_by: str = None,
        sort_order: str = None,
        limit: Optional[int] = None,
        params: Optional[Mapping[str, Any]] = None,
        auto_paginate: bool = False,
    ):
        """
        Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.

        Args:

            query (str, optional): The search query to filter results by.
            location_type (str, optional): The location type to filter results by.
            region (str, optional): The region to filter results by.
            state_abbreviation (str, optional): The state abbreviation to filter results by.
            state_fips_code (str, optional): The state FIPS code to filter results by.
            parcl_id (int, optional): The parcl_id to filter results by.
            geoid (str, optional): The geoid to filter results by.
            sort_by (str, optional): The field to sort results by.
            sort_order (str, optional): The sort order to apply to the results.
            limit (int, optional): The number of items to return per page.
            params (dict, optional): Additional parameters to include in the request.
            auto_paginate (bool, optional): Automatically paginate through the results.

        Returns:

            Any: The JSON response as a pandas DataFrame.
        """

        params = {}

        params = Validators.validate_input_str_param(
            param=location_type,
            param_name="location_type",
            valid_values=VALID_LOCATION_TYPES,
            params_dict=params,
        )

        params = Validators.validate_input_str_param(
            param=region,
            param_name="region",
            valid_values=VALID_US_REGIONS,
            params_dict=params,
        )

        params = Validators.validate_input_str_param(
            param=state_abbreviation,
            param_name="state_abbreviation",
            valid_values=VALID_US_STATE_ABBREV,
            params_dict=params,
        )

        params = Validators.validate_input_str_param(
            param=state_fips_code,
            param_name="state_fips_code",
            valid_values=VALID_US_STATE_FIPS_CODES,
            params_dict=params,
        )

        params = Validators.validate_input_str_param(
            param=sort_by,
            param_name="sort_by",
            valid_values=VALID_SORT_BY,
            params_dict=params,
        )

        params = Validators.validate_input_str_param(
            param=sort_order,
            param_name="sort_order",
            valid_values=VALID_SORT_ORDER,
            params_dict=params,
        )

        if query:
            params["query"] = query

        if parcl_id:
            params["parcl_id"] = parcl_id

        if geoid:
            params["geoid"] = geoid

        if limit:
            params["limit"] = self._validate_limit("GET", limit)

        results = self._fetch_get(
            url=self.full_url, params=params, auto_paginate=auto_paginate
        )

        data = self._as_pd_dataframe(results.get("items"))
        self.markets = data
        return data
