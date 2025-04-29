from typing import Any, Dict, List, Mapping, Optional, Union
from collections import deque
import pandas as pd
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators
from concurrent.futures import ThreadPoolExecutor, as_completed


class PropertyV2Service(ParclLabsService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _fetch_post(self, params: dict[str, Any], data: dict[str, Any]) -> Any:
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
    
    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> pd.DataFrame:
        """Convert API response data to a pandas DataFrame with events as rows using json_normalize."""
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
                property_record = {
                    k: v for k, v in property_data.items() if k != "events"
                }
                
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
            sep='_'  # Use underscore as separator for nested fields
        )
        
        # If we have event data, normalize it
        if 'event' in all_data_df.columns:
            # Get indices of rows with events
            event_indices = all_data_df['event'].notna()
            
            if event_indices.any():
                # Normalize the event data
                event_df = pd.json_normalize(
                    all_data_df.loc[event_indices, 'event'].tolist(),
                    sep='_'
                )
                
                # Add event_ prefix to all columns
                event_df.columns = ['event_' + col for col in event_df.columns]
                
                # Add the event data back to the main dataframe
                for col in event_df.columns:
                    all_data_df.loc[event_indices, col] = event_df[col].values
                
                # Drop the original event column
                all_data_df = all_data_df.drop('event', axis=1)
        
        return all_data_df
    
    def _build_search_criteria(self, 
                              parcl_ids: Optional[List[int]] = None,
                              parcl_property_ids: Optional[List[int]] = None,
                              location: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """Build and validate search criteria."""
        data = {}
        valid_search_method = False
        
        if parcl_ids:
            data["parcl_ids"] = Validators.validate_integer_list(parcl_ids, "parcl_ids")
            valid_search_method = True
            
        if parcl_property_ids:
            data["parcl_property_ids"] = Validators.validate_integer_list(
                parcl_property_ids, "parcl_property_ids"
            )
            valid_search_method = True
            
        if location and all(k in location for k in ["latitude", "longitude", "radius"]):
            data.update(location)
            valid_search_method = True
        
        if not valid_search_method:
            raise ValueError("No valid search method provided, use parcl_ids, "
                "parcl_property_ids, or latitude/longitude/radius")
                
        return data
    
    def _build_property_filters(self, **kwargs) -> Dict[str, Any]:
        """Build property filters from keyword arguments."""
        property_filters = {}
        
        # Handle numeric range filters
        numeric_filters = {
            "bedrooms": ("min_beds", "max_beds"),
            "bathrooms": ("min_baths", "max_baths"),
            "square_feet": ("min_sqft", "max_sqft"),
            "year_built": ("min_year_built", "max_year_built"),
        }
        
        for field, (min_key, max_key) in numeric_filters.items():
            if kwargs.get(min_key):
                property_filters[min_key] = kwargs[min_key]
            if kwargs.get(max_key):
                property_filters[max_key] = kwargs[max_key]
        
        # Handle property types
        if kwargs.get("property_types"):
            property_filters["property_types"] = Validators.validate_property_types(
                kwargs["property_types"]
            )
        
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
    
    def _build_event_filters(self, **kwargs) -> Dict[str, Any]:
        """Build event filters from keyword arguments."""
        event_filters = {}
        
        # Handle event names
        if kwargs.get("event_names"):
            event_filters["event_names"] = Validators.validate_event_names(
                kwargs["event_names"]
            )
        
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
    
    def _build_owner_filters(self, **kwargs) -> Dict[str, Any]:
        """Build owner filters from keyword arguments."""
        owner_filters = {}
        
        # Handle owner names
        if kwargs.get("owner_name"):
            owner_filters["owner_name"] = Validators.validate_owner_names(
                kwargs["owner_name"]
            )
        
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
        parcl_ids: List[int] = None,
        parcl_property_ids: List[int] = None,
        latitude: float = None,
        longitude: float = None,
        radius: float = None,
        property_types: List[str] = None,
        min_beds: int = None,
        max_beds: int = None,
        min_baths: int = None,
        max_baths: int = None,
        min_sqft: int = None,
        max_sqft: int = None,
        min_year_built: int = None,
        max_year_built: int = None,
        include_property_details: bool = None,
        min_record_added_date: str = None,
        max_record_added_date: str = None,
        event_names: List[str] = None,
        min_event_date: str = None,
        max_event_date: str = None,
        min_price: int = None,
        max_price: int = None,
        is_new_construction: bool = None,
        min_record_updated_date: str = None,
        max_record_updated_date: str = None,
        is_current_owner: bool = None,
        owner_name: List[str] = None,
        is_investor_owned: bool = None,
        is_owner_occupied: bool = None,
        limit: Optional[int] = None,
        params: Optional[Mapping[str, Any]] = None
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve property data based on search criteria and filters.
        
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
            location=location
        )
        
        # Build filters using all provided parameters
        kwargs = locals()
        # Remove self and parameters that aren't filter-related
        for key in ['self', 'parcl_ids', 'parcl_property_ids', 'latitude', 'longitude', 
                   'radius', 'limit', 'params', 'data', 'location']:
            kwargs.pop(key, None)
            
        # Build and add filters to data
        data["property_filters"] = self._build_property_filters(**kwargs)
        data["event_filters"] = self._build_event_filters(**kwargs)
        data["owner_filters"] = self._build_owner_filters(**kwargs)
        
        # Make request with pagination
        results = self._fetch_post(
            params=params or {}, 
            data=data
        )

        metadata = {}
        if results:
            #get metadata
            metadata = results[0].get("metadata")

        # Process results
        final_df = self._as_pd_dataframe(results)
        
        return final_df, metadata
