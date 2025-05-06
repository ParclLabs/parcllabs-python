from collections.abc import Mapping
from typing import Any

import pandas as pd

from parcllabs.common import (
    GET_METHOD,
)
from parcllabs.services.parcllabs_service import ParclLabsService


class SearchMarkets(ParclLabsService):
    """
    Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def _as_pd_dataframe(self, data: list[Mapping[str, Any]]) -> pd.DataFrame:
        final_df = pd.DataFrame(data)
        # Convert numpy integers to regular Python integers
        for col in final_df.columns:
            if final_df[col].dtype in ["int64", "int32", "int16", "int8"]:
                final_df[col] = final_df[col].astype(int)
        return final_df

    def _prepare_params(
        self,
        query: str | None = None,
        location_type: str | None = None,
        region: str | None = None,
        state_abbreviation: str | None = None,
        state_fips_code: str | None = None,
        parcl_id: int | None = None,
        geoid: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        limit: int | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        params = params or {}

        # Handle uppercase parameters
        uppercase_params = {
            "location_type": location_type,
            "region": region,
            "state_abbreviation": state_abbreviation,
            "state_fips_code": state_fips_code,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }

        for key, value in uppercase_params.items():
            if value:
                params[key] = value.upper()

        # Handle regular parameters
        if query:
            params["query"] = query
        if parcl_id:
            params["parcl_id"] = parcl_id
        if geoid:
            params["geoid"] = geoid

        # Handle limit parameter
        if limit:
            params["limit"] = self._validate_limit(GET_METHOD, limit)
        elif self.client.limit:
            params["limit"] = self._validate_limit(GET_METHOD, self.client.limit)

        return params

    def retrieve(
        self,
        query: str | None = None,
        location_type: str | None = None,
        region: str | None = None,
        state_abbreviation: str | None = None,
        state_fips_code: str | None = None,
        parcl_id: int | None = None,
        geoid: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        limit: int | None = None,
        params: Mapping[str, Any] | None = None,
        auto_paginate: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.

        Args:
            query (str, optional): The search query to filter results by.
            location_type (str, optional): The location type to filter results by.
            region (str, optional): The region to filter results by.
            state_abbreviation (str, optional): The state abbreviation to filter results
            by.
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
        params = self._prepare_params(
            query,
            location_type,
            region,
            state_abbreviation,
            state_fips_code,
            parcl_id,
            geoid,
            sort_by,
            sort_order,
            limit,
            params,
        )

        results = self._fetch_get(url=self.full_url, params=params, auto_paginate=auto_paginate)
        data = self._as_pd_dataframe(results.get("items"))
        self._update_account_info(results.get("account"))
        self.markets = data
        return data
