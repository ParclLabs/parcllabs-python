from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertyV2Service(ParclLabsService):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def _fetch_post(self, params: dict[str, Any], data: dict[str, Any]) -> list[dict]:
        """Fetch data using POST request with pagination support."""
        response = self._post(url=self.full_post_url, data=data, params=params)
        result = response.json()

        pagination = result.get("pagination")
        metadata = result.get("metadata")
        all_data = [result]

        # If we need to paginate, use concurrent requests
        if pagination and pagination.get("has_more"):
            limit = pagination.get("limit")
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
        location: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """Build and validate search criteria."""
        data = {}

        if parcl_ids:
            data["parcl_ids"] = parcl_ids

        if parcl_property_ids:
            data["parcl_property_ids"] = parcl_property_ids

        if location and all(k in location for k in ["latitude", "longitude", "radius"]):
            data.update(location)

        return data

    def _build_property_filters(self, **kwargs: dict) -> dict[str, Any]:
        """Build property filters from keyword arguments."""
        property_filters = {}

        # Handle numeric range filters
        numeric_filters = {
            "bedrooms": ("min_beds", "max_beds"),
            "bathrooms": ("min_baths", "max_baths"),
            "square_feet": ("min_sqft", "max_sqft"),
            "year_built": ("min_year_built", "max_year_built"),
        }

        for _, (min_key, max_key) in numeric_filters.items():
            if kwargs.get(min_key):
                property_filters[min_key] = kwargs[min_key]
            if kwargs.get(max_key):
                property_filters[max_key] = kwargs[max_key]

        # Handle property types
        if kwargs.get("property_types"):
            property_filters["property_types"] = [
                property_type.upper() for property_type in kwargs["property_types"]
            ]

        # Handle date filters
        date_filters = [
            ("min_record_added_date", "min_record_added_date"),
            ("max_record_added_date", "max_record_added_date"),
        ]

        for param_key, filter_key in date_filters:
            if kwargs.get(param_key):
                property_filters[filter_key] = Validators.validate_date(kwargs[param_key])

        # Handle boolean parameters
        if "include_property_details" in kwargs:
            property_filters = Validators.validate_input_bool_param(
                param=kwargs["include_property_details"],
                param_name="include_property_details",
                params_dict=property_filters,
            )

        return property_filters

    def _build_event_filters(self, **kwargs: dict) -> dict[str, Any]:
        """Build event filters from keyword arguments."""
        event_filters = {}

        # Handle event names
        if kwargs.get("event_names"):
            event_filters["event_names"] = [name.upper() for name in kwargs["event_names"]]

        # Handle date filters
        date_filters = [
            ("min_event_date", "min_event_date"),
            ("max_event_date", "max_event_date"),
            ("min_record_updated_date", "min_record_updated_date"),
            ("max_record_updated_date", "max_record_updated_date"),
        ]

        for param_key, filter_key in date_filters:
            if kwargs.get(param_key):
                event_filters[filter_key] = Validators.validate_date(kwargs[param_key])

        # Handle price filters
        if kwargs.get("min_price"):
            event_filters["min_price"] = kwargs["min_price"]
        if kwargs.get("max_price"):
            event_filters["max_price"] = kwargs["max_price"]

        # Handle boolean parameters
        if "is_new_construction" in kwargs:
            event_filters = Validators.validate_input_bool_param(
                param=kwargs["is_new_construction"],
                param_name="is_new_construction",
                params_dict=event_filters,
            )

        return event_filters

    def _build_owner_filters(self, **kwargs: dict) -> dict[str, Any]:
        """Build owner filters from keyword arguments."""
        owner_filters = {}

        # Handle owner names
        if kwargs.get("owner_name"):
            owner_filters["owner_name"] = [name.upper() for name in kwargs["owner_name"]]

        # Handle boolean parameters
        bool_params = ["is_current_owner", "is_investor_owned", "is_owner_occupied"]

        for param_name in bool_params:
            if param_name in kwargs:
                owner_filters = Validators.validate_input_bool_param(
                    param=kwargs[param_name],
                    param_name=param_name,
                    params_dict=owner_filters,
                )

        return owner_filters

    def retrieve(
        self,
        parcl_ids: list[int] | None = None,
        parcl_property_ids: list[int] | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        radius: float | None = None,
        property_types: list[str] | None = None,
        min_beds: int | None = None,
        max_beds: int | None = None,
        min_baths: int | None = None,
        max_baths: int | None = None,
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
        params: Mapping[str, Any] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve property data based on search criteria and filters.

        Args:
            parcl_ids: List of parcl_ids to filter by.
            parcl_property_ids: List of parcl_property_ids to filter by.
            latitude: Latitude to filter by.
            longitude: Longitude to filter by.
            radius: Radius to filter by.
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
            params: Additional parameters to pass to the request.
        Returns:
            A pandas DataFrame containing the property data.
        """
        # Build location dict if latitude, longitude and radius are provided
        location = None
        if all(v is not None for v in [latitude, longitude, radius]):
            location = {"latitude": latitude, "longitude": longitude, "radius": radius}

        # Build search criteria
        data = self._build_search_criteria(
            parcl_ids=parcl_ids,
            parcl_property_ids=parcl_property_ids,
            location=location,
        )

        # Build filters using all provided parameters
        kwargs = locals()
        # Remove self and parameters that aren't filter-related
        for key in [
            "self",
            "parcl_ids",
            "parcl_property_ids",
            "latitude",
            "longitude",
            "radius",
            "params",
            "data",
            "location",
        ]:
            kwargs.pop(key, None)

        # Build and add filters to data
        data["property_filters"] = self._build_property_filters(**kwargs)
        data["event_filters"] = self._build_event_filters(**kwargs)
        data["owner_filters"] = self._build_owner_filters(**kwargs)

        # Make request with pagination
        results = self._fetch_post(params=params or {}, data=data)

        # Get metadata from results
        metadata = self._get_metadata(results)

        # Process results
        final_df = self._as_pd_dataframe(results)

        return final_df, metadata
