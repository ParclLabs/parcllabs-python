import pandas as pd
from typing import Any, Mapping, Optional, List
from parcllabs.common import (
    DEFAULT_LIMIT,
    VALID_LOCATION_TYPES,
    VALID_US_REGIONS,
    VALID_US_STATE_ABBREV,
    VALID_US_STATE_FIPS_CODES,
    VALID_SORT_BY,
    VALID_SORT_ORDER,
)
from parcllabs.services.parcllabs_service import ParclLabsService


class SearchMarkets(ParclLabsService):
    """
    Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.
    """

    def __init__(self, limit: int = DEFAULT_LIMIT, *args, **kwargs):
        super().__init__(limit=limit, *args, **kwargs)

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

        if location_type and location_type not in VALID_LOCATION_TYPES:
            raise ValueError(
                f"location_type value error. Valid values are: {VALID_LOCATION_TYPES}. Received: {location_type}"
            )

        if region and region not in VALID_US_REGIONS:
            raise ValueError(
                f"region value error. Valid values are: {VALID_US_REGIONS}. Received: {region}"
            )

        if state_abbreviation and state_abbreviation not in VALID_US_STATE_ABBREV:
            raise ValueError(
                f"state_abbreviation value error. Valid values are: {VALID_US_STATE_ABBREV}. Received: {state_abbreviation}"
            )

        if state_fips_code and state_fips_code not in VALID_US_STATE_FIPS_CODES:
            raise ValueError(
                f"state_fips_code value error. Valid values are: {VALID_US_STATE_FIPS_CODES}. Received: {state_fips_code}"
            )

        if sort_by and sort_by not in VALID_SORT_BY:
            raise ValueError(
                f"sort_by value error. Valid values are: {VALID_SORT_BY}. Received: {sort_by}"
            )

        if sort_order and sort_order not in VALID_SORT_ORDER:
            raise ValueError(
                f"sort_order value error. Valid values are: {VALID_SORT_ORDER}. Received: {sort_order}"
            )

        params = {
            "query": query,
            "location_type": location_type,
            "region": region,
            "state_abbreviation": state_abbreviation,
            "state_fips_code": state_fips_code,
            "parcl_id": parcl_id,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "geoid": geoid,
            "limit": limit if limit is not None else self.limit,
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

        data = self._as_pd_dataframe(results.get("items"))
        self.markets = data
        return data
