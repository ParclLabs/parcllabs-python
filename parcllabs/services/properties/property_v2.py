from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from parcllabs.enums import RequestLimits
from parcllabs.schemas.schemas import PropertyV2RetrieveParamCategories, PropertyV2RetrieveParams
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertyV2Service(ParclLabsService):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.simple_bool_validator = Validators.validate_input_bool_param_simple

    def _fetch_post(self, params: dict[str, Any], data: dict[str, Any]) -> list[dict]:
        """Fetch data using POST request with pagination support."""
        response = self._post(url=self.full_post_url, data=data, params=params)
        result = response.json()

        pagination = result.get("pagination")
        metadata = result.get("metadata")
        all_data = [result]

        returned_count = metadata.get("results", {}).get("returned_count", 0)

        if pagination:
            limit = pagination.get("limit")
            if returned_count == limit:
                return all_data

            # If we need to paginate, use concurrent requests
            if pagination.get("has_more"):
                print("More pages to fetch, paginating additional pages...")
                offset = pagination.get("offset")
                total_count = metadata.get("results", {}).get("total_available", 0)

                # Calculate how many more pages we need
                remaining_pages = (total_count - limit) // limit
                if (total_count - limit) % limit > 0:
                    remaining_pages += 1

                # Generate all the URLs we need to fetch
                urls = []
                current_offset = offset + limit
                for _ in range(remaining_pages):
                    urls.append(f"{self.full_post_url}?limit={limit}&offset={current_offset}")
                    current_offset += limit

                # Use ThreadPoolExecutor to make concurrent requests
                with ThreadPoolExecutor(max_workers=self.client.num_workers) as executor:
                    future_to_url = {
                        executor.submit(self._post, url=url, data=data, params=params): url
                        for url in urls
                    }

                    for future in as_completed(future_to_url):
                        try:
                            response = future.result()
                            page_result = response.json()
                            all_data.append(page_result)
                        except Exception as exc:
                            print(f"Request failed: {exc}")

        return all_data

    def _as_pd_dataframe(self, data: list[Mapping[str, Any]]) -> pd.DataFrame:
        """
        Convert API response data to a pandas DataFrame with events as rows
        using json_normalize.
        """
        # First, extract all properties with their events
        properties_with_events = []

        for results in data:
            if results is None or not results.get("data"):
                continue

            account_info = results.get("account_info")
            query_data = results.get("data")

            # Process each property
            for property_data in query_data:
                events = property_data.get("events", [])

                # Create a property record without events
                property_record = {k: v for k, v in property_data.items() if k != "events"}

                if not events:
                    # If no events, add the property as is
                    properties_with_events.append(property_record)
                else:
                    # For each event, create a record with property data and this event
                    for event in events:
                        combined_record = property_record.copy()
                        combined_record["event"] = event
                        properties_with_events.append(combined_record)

            self._update_account_info(account_info)

        if not properties_with_events:
            return pd.DataFrame()

        # Use json_normalize to flatten the nested structure
        all_data_df = pd.json_normalize(
            properties_with_events,
            sep="_",  # Use underscore as separator for nested fields
        )

        # If we have event data, normalize it
        if "event" in all_data_df.columns:
            # Get indices of rows with events
            event_indices = all_data_df["event"].notna()

            if event_indices.any():
                # Normalize the event data
                event_df = pd.json_normalize(
                    all_data_df.loc[event_indices, "event"].tolist(), sep="_"
                )

                # Add event_ prefix to all columns
                event_df.columns = ["event_" + col for col in event_df.columns]

                # Add the event data back to the main dataframe
                for col in event_df.columns:
                    all_data_df.loc[event_indices, col] = event_df[col].to_numpy()

                # Drop the original event column
                all_data_df = all_data_df.drop("event", axis=1)

        return all_data_df

    def _get_metadata(self, results: list[Mapping[str, Any]]) -> dict[str, Any]:
        """Get metadata from results with accurate returned_count."""
        if not results:
            return {}

        # Start with a copy of the first result's metadata
        metadata = results[0].get("metadata", {}).copy()

        # Calculate total returned_count
        total_returned = sum(
            result.get("metadata", {}).get("results", {}).get("returned_count", 0)
            for result in results
        )
        if "results" in metadata:
            metadata["results"]["returned_count"] = total_returned

        return metadata

    def _build_search_criteria(
        self,
        parcl_ids: list[int] | None = None,
        parcl_property_ids: list[int] | None = None,
        geo_coordinates: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """Build and validate search criteria."""
        data = {}

        if parcl_ids:
            data["parcl_ids"] = parcl_ids

        if parcl_property_ids:
            data["parcl_property_ids"] = parcl_property_ids

        if geo_coordinates:
            data["geo_coordinates"] = geo_coordinates

        return data

    def _build_numeric_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:
        """Build numeric property filters."""
        filters = {}

        if params.min_beds is not None:
            filters["min_beds"] = params.min_beds
        if params.max_beds is not None:
            filters["max_beds"] = params.max_beds
        if params.min_baths is not None:
            filters["min_baths"] = params.min_baths
        if params.max_baths is not None:
            filters["max_baths"] = params.max_baths
        if params.min_sqft is not None:
            filters["min_sqft"] = params.min_sqft
        if params.max_sqft is not None:
            filters["max_sqft"] = params.max_sqft
        if params.min_year_built is not None:
            filters["min_year_built"] = params.min_year_built
        if params.max_year_built is not None:
            filters["max_year_built"] = params.max_year_built

        return filters

    def _build_date_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:
        """Build date-related property filters."""
        filters = {}

        if params.min_record_added_date is not None:
            filters["min_record_added_date"] = params.min_record_added_date
        if params.max_record_added_date is not None:
            filters["max_record_added_date"] = params.max_record_added_date

        return filters

    def _build_boolean_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:
        """Build boolean property filters."""
        filters = {}

        if params.current_on_market_flag is not None:
            filters["current_on_market_flag"] = self.simple_bool_validator(
                params.current_on_market_flag
            )
        if params.current_on_market_rental_flag is not None:
            filters["current_on_market_rental_flag"] = self.simple_bool_validator(
                params.current_on_market_rental_flag
            )
        if params.include_property_details is not None:
            filters["include_property_details"] = self.simple_bool_validator(
                params.include_property_details
            )

        return filters

    def _build_property_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:
        """Build property filters from validated Pydantic schema."""
        property_filters = {}

        # Build numeric filters
        property_filters.update(self._build_numeric_filters(params))

        # Build date filters
        property_filters.update(self._build_date_filters(params))

        # Build boolean filters
        property_filters.update(self._build_boolean_filters(params))

        # Handle property types
        if params.property_types:
            property_filters["property_types"] = [
                property_type.upper() for property_type in params.property_types
            ]

        return property_filters

    def _build_event_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:
        """Build event filters from validated Pydantic schema."""
        event_filters = {}

        # Handle event names
        if params.event_names:
            event_filters["event_names"] = [event_name.upper() for event_name in params.event_names]

        # Handle date and price filters
        if params.min_event_date is not None:
            event_filters["min_event_date"] = params.min_event_date
        if params.max_event_date is not None:
            event_filters["max_event_date"] = params.max_event_date
        if params.min_record_updated_date is not None:
            event_filters["min_record_updated_date"] = params.min_record_updated_date
        if params.max_record_updated_date is not None:
            event_filters["max_record_updated_date"] = params.max_record_updated_date
        if params.min_price is not None:
            event_filters["min_price"] = params.min_price
        if params.max_price is not None:
            event_filters["max_price"] = params.max_price

        # Handle boolean parameters
        if params.is_new_construction is not None:
            event_filters["is_new_construction"] = self.simple_bool_validator(
                params.is_new_construction
            )

        return event_filters

    def _build_owner_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:
        """Build owner filters from validated Pydantic schema."""
        owner_filters = {}

        # Handle owner names
        if params.owner_name:
            owner_filters["owner_name"] = [owner_name.upper() for owner_name in params.owner_name]

        # Handle boolean parameters
        if params.is_current_owner is not None:
            owner_filters["is_current_owner"] = self.simple_bool_validator(params.is_current_owner)
        if params.is_investor_owned is not None:
            owner_filters["is_investor_owned"] = self.simple_bool_validator(
                params.is_investor_owned
            )
        if params.is_owner_occupied is not None:
            owner_filters["is_owner_occupied"] = self.simple_bool_validator(
                params.is_owner_occupied
            )

        return owner_filters

    def _validate_limit(self, limit: int | None) -> int:
        """Validate limit parameter."""
        max_limit = RequestLimits.PROPERTY_V2_MAX.value

        # If auto-paginate is enabled or no limit is provided, use maximum limit
        if limit in (None, 0):
            print(f"No limit provided. Setting limit to maximum value of {max_limit}.")
            return max_limit

        # If limit exceeds maximum, cap it
        if limit > max_limit:
            print(
                f"Supplied limit value is too large for requested endpoint."
                f"Setting limit to maximum value of {max_limit}."
            )
            return max_limit

        return limit

    def _build_param_categories(
        self, params: PropertyV2RetrieveParams
    ) -> PropertyV2RetrieveParamCategories:
        """Build parameter categories from validated Pydantic schema."""
        return PropertyV2RetrieveParamCategories(
            property_filters=self._build_property_filters(params),
            event_filters=self._build_event_filters(params),
            owner_filters=self._build_owner_filters(params),
        )

    def retrieve(
        self,
        parcl_ids: list[int] | None = None,
        parcl_property_ids: list[int] | None = None,
        geo_coordinates: dict[str, float] | None = None,
        property_types: list[str] | None = None,
        min_beds: int | None = None,
        max_beds: int | None = None,
        min_baths: float | None = None,
        max_baths: float | None = None,
        min_sqft: int | None = None,
        max_sqft: int | None = None,
        min_year_built: int | None = None,
        max_year_built: int | None = None,
        include_property_details: bool | None = None,
        min_record_added_date: str | None = None,
        max_record_added_date: str | None = None,
        event_names: list[str] | None = None,
        min_event_date: str | None = None,
        max_event_date: str | None = None,
        min_price: int | None = None,
        max_price: int | None = None,
        is_new_construction: bool | None = None,
        min_record_updated_date: str | None = None,
        max_record_updated_date: str | None = None,
        is_current_owner: bool | None = None,
        owner_name: list[str] | None = None,
        is_investor_owned: bool | None = None,
        is_owner_occupied: bool | None = None,
        current_on_market_flag: bool | None = None,
        current_on_market_rental_flag: bool | None = None,
        limit: int | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve property data based on search criteria and filters.

        Args:
            parcl_ids: List of parcl_ids to filter by.
            parcl_property_ids: List of parcl_property_ids to filter by.
            geo_coordinates: Dictionary containing latitude, longitude, and radius (in miles)
            to filter by.
            property_types: List of property types to filter by.
            min_beds: Minimum number of bedrooms to filter by.
            max_beds: Maximum number of bedrooms to filter by.
            min_baths: Minimum number of bathrooms to filter by.
            max_baths: Maximum number of bathrooms to filter by.
            min_sqft: Minimum square footage to filter by.
            max_sqft: Maximum square footage to filter by.
            min_year_built: Minimum year built to filter by.
            max_year_built: Maximum year built to filter by.
            include_property_details: Whether to include property details.
            min_record_added_date: Minimum record added date to filter by.
            max_record_added_date: Maximum record added date to filter by.
            event_names: List of event names to filter by.
            min_event_date: Minimum event date to filter by.
            max_event_date: Maximum event date to filter by.
            min_price: Minimum price to filter by.
            max_price: Maximum price to filter by.
            is_new_construction: Whether to filter by new construction.
            min_record_updated_date: Minimum record updated date to filter by.
            max_record_updated_date: Maximum record updated date to filter by.
            is_current_owner: Whether to filter by current owner.
            owner_name: List of owner names to filter by.
            is_investor_owned: Whether to filter by investor owned.
            is_owner_occupied: Whether to filter by owner occupied.
            current_on_market_flag: Whether to filter by current_on_market flag.
            current_on_market_rental_flag: Whether to filter by current_on_market_rental flag.
            limit: Number of results to return.
            params: Additional parameters to pass to the request.
        Returns:
            A tuple containing (pandas DataFrame, metadata dictionary).
        """
        print("Processing property search request...")

        # Validate and process input parameters using Pydantic schema
        input_params = PropertyV2RetrieveParams(
            parcl_ids=parcl_ids,
            parcl_property_ids=parcl_property_ids,
            geo_coordinates=geo_coordinates,
            property_types=property_types,
            min_beds=min_beds,
            max_beds=max_beds,
            min_baths=min_baths,
            max_baths=max_baths,
            min_sqft=min_sqft,
            max_sqft=max_sqft,
            min_year_built=min_year_built,
            max_year_built=max_year_built,
            include_property_details=include_property_details,
            min_record_added_date=min_record_added_date,
            max_record_added_date=max_record_added_date,
            event_names=event_names,
            min_event_date=min_event_date,
            max_event_date=max_event_date,
            min_price=min_price,
            max_price=max_price,
            is_new_construction=is_new_construction,
            min_record_updated_date=min_record_updated_date,
            max_record_updated_date=max_record_updated_date,
            is_current_owner=is_current_owner,
            owner_name=owner_name,
            is_investor_owned=is_investor_owned,
            is_owner_occupied=is_owner_occupied,
            current_on_market_flag=current_on_market_flag,
            current_on_market_rental_flag=current_on_market_rental_flag,
            limit=limit,
            params=params or {},
        )

        # Build search criteria
        data = self._build_search_criteria(
            parcl_ids=input_params.parcl_ids,
            parcl_property_ids=input_params.parcl_property_ids,
            geo_coordinates=(
                input_params.geo_coordinates.model_dump() if input_params.geo_coordinates else None
            ),
        )

        # Build parameter categories using validated parameters
        param_categories = self._build_param_categories(input_params)

        # Update data with categories
        data.update(param_categories.model_dump(exclude_none=True))

        # Set limit
        request_params = input_params.params.copy()
        request_params["limit"] = self._validate_limit(input_params.limit)

        # Make request with params
        results = self._fetch_post(params=request_params, data=data)

        # Get metadata from results
        metadata = self._get_metadata(results)

        # Process results
        final_df = self._as_pd_dataframe(results)

        return final_df, metadata
