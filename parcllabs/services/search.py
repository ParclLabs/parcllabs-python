from typing import Any, Mapping, Optional, List

import pandas as pd

from parcllabs.services.parcllabs_service import ParclLabsService

VALID_LOCATION_TYPES = [
    "COUNTY",
    "CITY",
    "ZIP5",
    "CDP",
    "VILLAGE",
    "TOWN",
    "CBSA",
    "ALL",
]

VALID_US_REGIONS = [
    "EAST_NORTH_CENTRAL",
    "EAST_SOUTH_CENTRAL",
    "MIDDLE_ATLANTIC",
    "MOUNTAIN",
    "NEW_ENGLAND",
    "PACIFIC",
    "SOUTH_ATLANTIC",
    "WEST_NORTH_CENTRAL",
    "WEST_SOUTH_CENTRAL",
    "ALL",
]

VALID_US_STATE_ABBREV = [
    "AK",
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "PR",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VI",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
    "ALL",
]

VALID_US_STATE_FIPS_CODES = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "60",
    "66",
    "69",
    "72",
    "78",
    "ALL",
]

VALID_SORT_BY = [
    "TOTAL_POPULATION",
    "MEDIAN_INCOME",
    "CASE_SHILLER_20_MARKET",
    "CASE_SHILLER_10_MARKET",
    "PRICEFEED_MARKET",
    "PARCL_EXCHANGE_MARKET",
]

VALID_SORT_ORDER = ["ASC", "DESC"]


class SearchMarkets(ParclLabsService):
    """
    Retrieve parcl_id and metadata for geographic markets in the Parcl Labs API.
    """

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
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
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
            params (dict, optional): Additional parameters to include in the request.
            as_dataframe (bool, optional): Return the results as a pandas DataFrame.
            auto_paginate (bool, optional): Automatically paginate through the results.

        Returns:

            Any: The JSON response as a dictionary or a pandas DataFrame if as_dataframe is True.
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
            **(params or {}),
        }
        results = self._request(url="/v1/search/markets", params=params)

        if auto_paginate:
            tmp = results.copy()
            while results["links"].get("next"):
                results = self._request(url=results["links"]["next"], is_next=True)
                tmp["items"].extend(results["items"])
            tmp["links"] = results["links"]
            results = tmp

        if as_dataframe:
            return self._as_pd_dataframe(results.get("items"))

        return results
