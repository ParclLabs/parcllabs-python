from typing import Any, List, Mapping, Optional

from parcllabs.common import POST_METHOD
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertySearchV2Service(ParclLabsService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrieve(
        self,
        parcl_property_ids: List[int],
        parcl_ids: List[int],
        latitude: float = None,
        longitude: float = None,
        radius: float = None,
        property_types: List[str] = None,
        min_bedrooms: int = None,
        max_bedrooms: int = None,
        min_bathrooms: int = None,
        max_bathrooms: int = None,
        min_square_feet: int = None,
        max_square_feet: int = None,
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
        params: Optional[Mapping[str, Any]] = {},
        auto_paginate: bool = False,
    ):
        valid_search_method = 0
        data = {}
        if parcl_ids:
            parcl_ids = Validators.validate_integer_list(parcl_ids, "parcl_ids")
            data["parcl_ids"] = parcl_ids
            valid_search_method += 1
        if parcl_property_ids:
            parcl_property_ids = Validators.validate_integer_list(
                parcl_property_ids, "parcl_property_ids"
            )
            data["parcl_property_ids"] = parcl_property_ids
            valid_search_method += 1
        if latitude and longitude and radius:
            data["latitude"] = latitude
            data["longitude"] = longitude
            data["radius"] = radius
            valid_search_method += 1
        
        if valid_search_method == 0:
            raise ValueError("No valid search method provided")
        
        if valid_search_method > 1:
            raise ValueError("Only one search method can be provided: parcl_ids, parcl_property_ids, or latitude/longitude/radius")

        # Property filters
        property_filters = {}
        property_types = Validators.validate_property_types(property_types)
        if property_types:
            property_filters["property_types"] = property_types
        
        if min_bedrooms:
            property_filters["min_bedrooms"] = min_bedrooms
        if max_bedrooms:
            property_filters["max_bedrooms"] = max_bedrooms
        if min_bathrooms:
            property_filters["min_bathrooms"] = min_bathrooms
        if max_bathrooms:
            property_filters["max_bathrooms"] = max_bathrooms
        if min_square_feet:
            property_filters["min_square_feet"] = min_square_feet
        if max_square_feet:
            property_filters["max_square_feet"] = max_square_feet
        if min_year_built:
            property_filters["min_year_built"] = min_year_built
        if max_year_built:
            property_filters["max_year_built"] = max_year_built

        property_filters = Validators.validate_input_bool_param(
            param=include_property_details,
            param_name="include_property_details",
            params_dict=property_filters,
        )
        
        if min_record_added_date:
            min_record_added_date = Validators.validate_date(min_record_added_date)
            property_filters["min_record_added_date"] = min_record_added_date

        if max_record_added_date:
            min_record_added_date = Validators.validate_date(min_record_added_date)
            property_filters["max_record_added_date"] = max_record_added_date

        # Event filters
        event_filters = {}
        event_names = Validators.validate_event_names(event_names)
        if event_names:
            event_filters["event_names"] = event_names
        
        if min_event_date:
            min_event_date = Validators.validate_date(min_event_date)
            event_filters["min_event_date"] = min_event_date

        if max_event_date:
            max_event_date = Validators.validate_date(max_event_date)
            event_filters["max_event_date"] = max_event_date

        if min_price:
            event_filters["min_price"] = min_price
        
        if max_price:
            event_filters["max_price"] = max_price

        event_filters = Validators.validate_input_bool_param(
            param=is_new_construction,
            param_name="is_new_construction",
            params_dict=event_filters,
        )

        if min_record_updated_date:
            min_record_updated_date = Validators.validate_date(min_record_updated_date)
            event_filters["min_record_updated_date"] = min_record_updated_date

        if max_record_updated_date:
            max_record_updated_date = Validators.validate_date(max_record_updated_date)
            event_filters["max_record_updated_date"] = max_record_updated_date

        # Owner filters
        owner_filters = {}
        owner_filters = Validators.validate_input_bool_param(
            param=is_current_owner,
            param_name="is_current_owner",
            params_dict=owner_filters,
        )

        owner_name = Validators.validate_owner_names(owner_name)
        if owner_name:
            owner_filters["owner_name"] = owner_name

        owner_filters = Validators.validate_input_bool_param(
            param=is_investor_owned,
            param_name="is_investor_owned",
            params_dict=owner_filters,
        )
        owner_filters = Validators.validate_input_bool_param(
            param=is_owner_occupied,
            param_name="is_owner_occupied",
            params_dict=owner_filters,
        )

        data["property_filters"] = property_filters
        data["event_filters"] = event_filters
        data["owner_filters"] = owner_filters

        params = {}
        if limit:
            params["limit"] = self._validate_limit(POST_METHOD, limit)
        elif self.client.limit:
            params["limit"] = self._validate_limit(POST_METHOD, self.client.limit)

        results = self._fetch_post(params=params, data=data, auto_paginate=auto_paginate)
        data = self._as_pd_dataframe(results.get("data"))
        self._update_account_info(results.get("account_info"))
        return data
