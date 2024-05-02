from enum import Enum
from typing import Any, Mapping, Optional, List

import pandas as pd

from parcllabs.services.base_service import ParclLabsService

valid_locations = [
    "COUNTY",
    "CITY",
    "ZIP5",
    "CDP",
    "VILLAGE",
    "TOWN",
    "CBSA",
    "ALL",
]

valid_regions = [
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

valid_state_abbreviations = [
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

valid_state_fips_codes = [
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

valid_sort_by = [
    "TOTAL_POPULATION",
    "MEDIAN_INCOME",
    "CASE_SHILLER_20_MARKET",
    "CASE_SHILLER_10_MARKET",
    "PRICEFEED_MARKET",
    "PARCL_EXCHANGE_MARKET",
]

valid_sort_order = ["ASC", "DESC"]


class SearchMarkets(ParclLabsService):
    """
    Gets weekly updated rolling counts of newly listed for sale properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given <parcl_id>.
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
        auto_paginate: bool = True,
    ):
        if location_type is not None and location_type not in valid_locations:
            raise ValueError(
                f"location_type value error. Valid values are: {valid_locations}. Received: {location_type}"
            )

        if region is not None and region not in valid_regions:
            raise ValueError(
                f"region value error. Valid values are: {valid_regions}. Received: {region}"
            )

        if (
            state_abbreviation is not None
            and state_abbreviation not in valid_state_abbreviations
        ):
            raise ValueError(
                f"state_abbreviation value error. Valid values are: {valid_state_abbreviations}. Received: {state_abbreviation}"
            )

        if (
            state_fips_code is not None
            and state_fips_code not in valid_state_fips_codes
        ):
            raise ValueError(
                f"state_fips_code value error. Valid values are: {valid_state_fips_codes}. Received: {state_fips_code}"
            )

        if sort_by is not None and sort_by not in valid_sort_by:
            raise ValueError(
                f"sort_by value error. Valid values are: {valid_sort_by}. Received: {sort_by}"
            )

        if sort_order is not None and sort_order not in valid_sort_order:
            raise ValueError(
                f"sort_order value error. Valid values are: {valid_sort_order}. Received: {sort_order}"
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
            while results["links"].get("next") is not None:
                results = self._request(url=results["links"]["next"], is_next=True)
                tmp["items"].extend(results["items"])
            tmp["links"] = results["links"]
            results = tmp

        if as_dataframe:
            return self._as_pd_dataframe(results.get("items"))

        return results
