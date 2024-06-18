from typing import Any, Mapping, Optional
from parcllabs.common import DEFAULT_LIMIT
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertyTypeService(ParclLabsService):
    def __init__(self, client, url: str, limit: int = DEFAULT_LIMIT):
        super().__init__(client=client, url=url, limit=limit)

    def retrieve(
        self,
        parcl_ids: int,
        start_date: str = None,
        end_date: str = None,
        property_type: str = None,
        limit: Optional[int] = None,
        params: Optional[Mapping[str, Any]] = {},
        auto_paginate: bool = False,
    ):
        property_type = Validators.validate_property_type(property_type)

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
