import copy
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from parcllabs.common import PARCL_PROPERTY_IDS, PARCL_PROPERTY_IDS_LIMIT
from parcllabs.enums import RequestLimits
from parcllabs.schemas.schemas import PropertyV2RetrieveParamCategories, PropertyV2RetrieveParams
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators
from parcllabs.warnings import (
    warn_incomplete_pages,
    warn_integrity_mismatch,
    warn_truncation,
)

# Transient page failures are retried before a page is abandoned.
PAGE_FETCH_ATTEMPTS = 3
PAGE_FETCH_BACKOFF_SECONDS = 1.0


class PropertyV2Service(ParclLabsService):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.simple_bool_validator = Validators.validate_input_bool_param_simple

    @staticmethod
    def _raise_http_error(chunk_num: int, status_code: int, response_preview: str) -> None:
        error_msg = f"Chunk {chunk_num} failed: HTTP {status_code}"
        raise RuntimeError(f"{error_msg}\nResponse content: {response_preview}...")

    @staticmethod
    def _raise_empty_response_error(chunk_num: int) -> None:
        raise RuntimeError(f"Chunk {chunk_num} failed: Empty response from API")

    def _fetch_page(
        self,
        data: dict[str, Any],
        params: dict[str, Any],
        offset: int,
        limit: int,
    ) -> dict:
        """Fetch a single page, retrying transient failures with exponential backoff.

        Raises the last exception if every attempt fails.
        """
        page_params = dict(params)
        page_params["limit"] = limit
        page_params["offset"] = offset

        last_exc: Exception | None = None
        for attempt in range(PAGE_FETCH_ATTEMPTS):
            try:
                return self._post(url=self.full_post_url, data=data, params=page_params).json()
            except Exception as exc:  # retried below, then re-raised
                last_exc = exc
                if attempt < PAGE_FETCH_ATTEMPTS - 1:
                    time.sleep(PAGE_FETCH_BACKOFF_SECONDS * (2**attempt))
        raise last_exc  # type: ignore[misc]

    def _fetch_post(
        self,
        params: dict[str, Any],
        data: dict[str, Any],
        max_results: int | None = None,
    ) -> list[dict]:
        """Fetch data using POST, paginating until ``max_results`` is satisfied.

        Args:
            params: Request params. ``params["limit"]`` is the page size.
            data: POST body containing the search criteria and filters.
            max_results: Maximum number of properties to return in total. ``None``
                retrieves every matching property.

        Returns:
            List of raw page payloads. Failed pages are omitted and reported via a
            ``ParclLabsIncompleteResultWarning``.
        """
        params = dict(params)
        response = self._post(url=self.full_post_url, data=data, params=params)
        result = response.json()
        all_data = [result]

        pagination = result.get("pagination") or {}
        results_meta = (result.get("metadata") or {}).get("results") or {}
        total_available = results_meta.get("total_available", 0)
        retrieved = results_meta.get("returned_count", 0)

        # How many properties do we actually want? Never more than exist.
        target = total_available if max_results is None else min(max_results, total_available)

        if retrieved >= target or not pagination.get("has_more"):
            # Report what was actually returned, not what was requested.
            if retrieved < total_available:
                warn_truncation(retrieved, total_available)
            return all_data

        page_size = pagination.get("limit") or params.get("limit") or retrieved
        offset = pagination.get("offset", 0)

        # Request exactly the pages needed to reach `target` -- the final page is
        # trimmed so an explicit `limit` is honoured precisely rather than overshot.
        pages: list[tuple[int, int]] = []
        current_offset = offset + retrieved
        remaining = target - retrieved
        while remaining > 0:
            this_limit = min(page_size, remaining)
            pages.append((current_offset, this_limit))
            current_offset += this_limit
            remaining -= this_limit

        failed_offsets: list[int] = []
        with ThreadPoolExecutor(max_workers=self.client.num_workers) as executor:
            future_to_offset = {
                executor.submit(
                    self._fetch_page, data, params, page_offset, page_limit
                ): page_offset
                for page_offset, page_limit in pages
            }
            for future in as_completed(future_to_offset):
                page_offset = future_to_offset[future]
                try:
                    all_data.append(future.result())
                except Exception:  # surfaced as a warning below
                    failed_offsets.append(page_offset)

        actually_retrieved = self._total_returned(all_data)

        if failed_offsets:
            # Report the shortfall and stop. Deliberately no truncation warning here:
            # we did NOT return `target`, so claiming we did would contradict this
            # warning, and the truncation notice is once-per-session -- burning it on a
            # misleading message would suppress a legitimate one later in the run.
            # `total_available` is included so capping information is not lost.
            failed_offsets.sort()
            warn_incomplete_pages(failed_offsets, target, actually_retrieved, total_available)
            all_data[0].setdefault("_parcllabs", {})["incomplete_pages"] = failed_offsets
            return all_data

        if target < total_available:
            warn_truncation(actually_retrieved, total_available)

        return all_data

    @staticmethod
    def _total_returned(pages: list[dict]) -> int:
        """Sum the properties actually returned across assembled pages."""
        return sum(
            (page.get("metadata") or {}).get("results", {}).get("returned_count", 0)
            for page in pages
        )

    def _fetch_post_parcl_property_ids(
        self,
        params: dict[str, Any],
        data: dict[str, Any],
    ) -> list[dict]:
        """Fetch data using POST request with parcl_property_ids, chunking the request

        Args:
            params: Dictionary of parameters to pass to the request.
            data: Dictionary of data to pass to the request.

        Returns:
            List of dictionaries containing the data from the request.
        """
        parcl_property_ids = data.get(PARCL_PROPERTY_IDS)
        num_ids = len(parcl_property_ids)
        if num_ids <= PARCL_PROPERTY_IDS_LIMIT:
            return self._fetch_post(params=params, data=data)

        # If we exceed PARCL_PROPERTY_IDS_LIMIT, chunk the request
        parcl_property_ids_chunks = [
            parcl_property_ids[i : i + PARCL_PROPERTY_IDS_LIMIT]
            for i in range(0, num_ids, PARCL_PROPERTY_IDS_LIMIT)
        ]
        num_chunks = len(parcl_property_ids_chunks)

        print(f"Fetching {num_chunks} chunks...")

        all_data = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Create a copy of data for each chunk to avoid race conditions
            future_to_chunk = {}
            for idx, chunk in enumerate(parcl_property_ids_chunks):
                # Create a copy of data with the specific chunk
                chunk_data = data.copy()
                chunk_data[PARCL_PROPERTY_IDS] = chunk

                # Submit the task
                future = executor.submit(
                    self._post,
                    url=self.full_post_url,
                    data=chunk_data,
                    params=params,
                )
                future_to_chunk[future] = idx + 1

                # Small delay between submissions to avoid rate limiting
                if idx < len(parcl_property_ids_chunks) - 1:  # Don't delay after the last one
                    time.sleep(0.1)

            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk_num = future_to_chunk[future]
                try:
                    result = future.result()

                    # Check HTTP status code
                    if result.status_code != 200:
                        response_preview = (
                            result.text[:200] if result.text else "No response content"
                        )
                        self._raise_http_error(chunk_num, result.status_code, response_preview)

                    # Check if response has content
                    if not result.text.strip():
                        self._raise_empty_response_error(chunk_num)

                    # Try to parse JSON
                    try:
                        response = result.json()
                        all_data.append(response)
                        print(f"Completed chunk {chunk_num} of {num_chunks}")
                    except ValueError as json_exc:
                        response_preview = (
                            result.text[:200] if result.text else "No response content"
                        )
                        raise RuntimeError(
                            f"Chunk {chunk_num} failed: Invalid JSON - {json_exc}\n"
                            f"Response content: {response_preview}..."
                        ) from json_exc

                except Exception as exc:
                    # If it's already a RuntimeError from above, re-raise it
                    if isinstance(exc, RuntimeError):
                        raise

                    # For any other unexpected errors, wrap and raise
                    raise RuntimeError(
                        f"Chunk {chunk_num} failed with unexpected error: {exc} "
                        f"(Exception type: {type(exc).__name__})"
                    ) from exc

        print(f"All {num_chunks} chunks completed successfully.")
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

        # Deep copy: a shallow .copy() leaves metadata["results"] aliased to the raw
        # first page, so assigning returned_count below would mutate the response.
        metadata = copy.deepcopy(results[0].get("metadata", {}))

        # Calculate total returned_count
        total_returned = sum(
            result.get("metadata", {}).get("results", {}).get("returned_count", 0)
            for result in results
        )
        if "results" in metadata:
            metadata["results"]["returned_count"] = total_returned

        # Surface any pages that could not be fetched (see _fetch_post).
        incomplete = (results[0].get("_parcllabs") or {}).get("incomplete_pages")
        if incomplete:
            metadata["incomplete_pages"] = incomplete

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
            data[PARCL_PROPERTY_IDS] = parcl_property_ids

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

        if params.include_property_details is not None:
            filters["include_property_details"] = self.simple_bool_validator(
                params.include_property_details
            )
        if params.current_on_market_flag is not None:
            filters["current_on_market_flag"] = self.simple_bool_validator(
                params.current_on_market_flag
            )
        if params.current_on_market_rental_flag is not None:
            filters["current_on_market_rental_flag"] = self.simple_bool_validator(
                params.current_on_market_rental_flag
            )
        if params.current_new_construction_flag is not None:
            filters["current_new_construction_flag"] = self.simple_bool_validator(
                params.current_new_construction_flag
            )
        if params.current_owner_occupied_flag is not None:
            filters["current_owner_occupied_flag"] = self.simple_bool_validator(
                params.current_owner_occupied_flag
            )
        if params.current_investor_owned_flag is not None:
            filters["current_investor_owned_flag"] = self.simple_bool_validator(
                params.current_investor_owned_flag
            )
        if params.has_pool is not None:
            filters["has_pool"] = self.simple_bool_validator(params.has_pool)

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

        # Handle current entity owner name
        if params.current_entity_owner_name is not None:
            property_filters["current_entity_owner_name"] = params.current_entity_owner_name

        return property_filters

    def _build_event_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:  # noqa: C901
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
        if params.include_events is not None:
            event_filters["include_events"] = self.simple_bool_validator(params.include_events)
        if params.include_full_event_history is not None:
            event_filters["include_full_event_history"] = self.simple_bool_validator(
                params.include_full_event_history
            )

        return event_filters

    def _build_owner_filters(self, params: PropertyV2RetrieveParams) -> dict[str, Any]:
        """Build owner filters from validated Pydantic schema."""
        owner_filters = {}

        # Handle owner names
        if params.owner_name:
            owner_filters["owner_name"] = [owner_name.upper() for owner_name in params.owner_name]

        # Handle entity seller names
        if params.entity_seller_name:
            owner_filters["entity_seller_name"] = [
                entity_seller_name.upper() for entity_seller_name in params.entity_seller_name
            ]

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

    def _set_limit_pagination(self, limit: int | None) -> tuple[int, int | None]:
        """Resolve the caller's ``limit`` into a page size and a total cap.

        ``limit`` is the maximum number of *properties* to return in total, not a
        page size. Pagination is an internal detail of satisfying it.

        Args:
            limit: Maximum properties to return. ``None`` (or ``0``) means no cap.

        Returns:
            ``(page_size, max_results)`` where ``max_results`` is ``None`` when
            unbounded.
        """
        max_limit = RequestLimits.PROPERTY_V2_MAX.value

        # `0` is treated as "no cap" defensively only. Callers cannot reach this via
        # retrieve(): PropertyV2RetrieveParams enforces ge=1, so 0 and negatives are
        # already rejected at validation time.
        if limit == 0 or limit is None:
            return max_limit, None

        # A limit above the API's per-request ceiling is satisfied by paginating,
        # rather than by letting the server reject the request outright.
        return min(limit, max_limit), limit

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
        entity_seller_name: list[str] | None = None,
        is_investor_owned: bool | None = None,
        is_owner_occupied: bool | None = None,
        current_on_market_flag: bool | None = None,
        current_on_market_rental_flag: bool | None = None,
        current_new_construction_flag: bool | None = None,
        current_owner_occupied_flag: bool | None = None,
        current_investor_owned_flag: bool | None = None,
        has_pool: bool | None = None,
        current_entity_owner_name: str | None = None,
        include_events: bool | None = None,
        include_full_event_history: bool | None = None,
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
            entity_seller_name: List of entity seller names to filter by.
            is_investor_owned: Whether to filter by investor owned.
            is_owner_occupied: Whether to filter by owner occupied.
            current_on_market_flag: Whether to filter by current_on_market flag.
            current_on_market_rental_flag: Whether to filter by current_on_market_rental flag.
            current_new_construction_flag: Whether to filter by current_new_construction flag.
            current_owner_occupied_flag: Whether to filter by current_owner_occupied flag.
            current_investor_owned_flag: Whether to filter by current_investor_owned flag.
            has_pool: Whether to filter by pool availability.
            current_entity_owner_name: Current entity owner name to filter by.
            include_events: Whether to include events in the response.
            include_full_event_history: Whether to include full event history in the response.
            limit: Maximum number of *properties* to return in total. Pagination is
                handled internally to satisfy it, so values above the API's
                per-request ceiling (50,000) are fetched across several pages rather
                than rejected. Omit (or pass None) to retrieve every matching
                property. Two things to note: credits are charged per property
                returned, not per event; and because the returned DataFrame is
                event-level, ``len(df)`` is NOT bounded by ``limit`` -- one property
                may contribute many rows. If ``limit`` withholds matching data, a
                ParclLabsTruncationWarning is emitted and
                ``metadata["results"]`` reports both counts.
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
            entity_seller_name=entity_seller_name,
            is_investor_owned=is_investor_owned,
            is_owner_occupied=is_owner_occupied,
            current_on_market_flag=current_on_market_flag,
            current_on_market_rental_flag=current_on_market_rental_flag,
            current_new_construction_flag=current_new_construction_flag,
            current_owner_occupied_flag=current_owner_occupied_flag,
            current_investor_owned_flag=current_investor_owned_flag,
            has_pool=has_pool,
            current_entity_owner_name=current_entity_owner_name,
            include_events=include_events,
            include_full_event_history=include_full_event_history,
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

        # Set limit. `auto_paginate` is deliberately NOT placed in request_params --
        # it is an internal concern and was previously leaking into the query string.
        request_params = input_params.params.copy()

        # Make request with params
        if data.get(PARCL_PROPERTY_IDS):
            # Querying by explicit property IDs: the ID list bounds the result, so a
            # single page of PARCL_PROPERTY_IDS_LIMIT can always hold it, and >that
            # many IDs are chunked in _fetch_post_parcl_property_ids. The caller's
            # `limit` is not honoured on this path (breaking change -> DAT-122).
            request_params["limit"] = PARCL_PROPERTY_IDS_LIMIT
            results = self._fetch_post_parcl_property_ids(params=request_params, data=data)
        else:
            page_size, max_results = self._set_limit_pagination(input_params.limit)
            request_params["limit"] = page_size
            results = self._fetch_post(params=request_params, data=data, max_results=max_results)

        # Get metadata from results
        metadata = self._get_metadata(results)

        # Process results
        final_df = self._as_pd_dataframe(results)

        self._check_pagination_integrity(final_df, metadata)

        return final_df, metadata

    @staticmethod
    def _check_pagination_integrity(final_df: pd.DataFrame, metadata: dict[str, Any]) -> None:
        """Warn if assembled pages did not yield the expected property count.

        Offset pagination is only safe while the server applies a stable sort. This
        is a cheap guard so an upstream ordering change surfaces here rather than as
        silently duplicated or missing rows in a customer's dataset.
        """
        if final_df.empty or "parcl_property_id" not in final_df.columns:
            return
        # Pages that failed outright are already reported; don't double-warn.
        if metadata.get("incomplete_pages"):
            return

        expected = (metadata.get("results") or {}).get("returned_count")
        if not expected:
            return

        unique_properties = final_df["parcl_property_id"].nunique()
        if unique_properties != expected:
            warn_integrity_mismatch(unique_properties, expected)
