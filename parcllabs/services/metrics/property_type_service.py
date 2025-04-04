from typing import Any, List, Mapping, Optional

from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertyTypeService(ParclLabsService):
    def retrieve(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        property_type: str = None,
        limit: Optional[int] = None,
        params: Optional[Mapping[str, Any]] = {},
        auto_paginate: bool = False,
    ):
        property_type = Validators.validate_property_type(property_type)
        parcl_ids = Validators.validate_integer_list(parcl_ids, "parcl_ids")

        if property_type:
            params["property_type"] = property_type

        return super().retrieve(
            parcl_ids=parcl_ids,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            params=params,
            auto_paginate=auto_paginate,
        )
