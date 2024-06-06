from typing import Any, Mapping, Optional, List
from parcllabs.services.parcllabs_service import ParclLabsService


class PropertyTypeService(ParclLabsService):
    def __init__(self, client, url):
        super().__init__(client=client, url=url)

    def retrieve(
        self,
        parcl_id: int,
        start_date: str = None,
        end_date: str = None,
        property_type: str = None,
        params: Optional[Mapping[str, Any]] = {},
        as_dataframe: bool = False,
        auto_paginate: bool = False,
    ):
        property_type = self.validate_property_type(property_type)

        if property_type:
            params["property_type"] = property_type

        return super().retrieve(
            parcl_id=parcl_id,
            start_date=start_date,
            end_date=end_date,
            params=params,
            as_dataframe=as_dataframe,
            auto_paginate=auto_paginate,
        )

    def retrieve_many(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        property_type: str = None,
        params: Optional[Mapping[str, Any]] = {},
        as_dataframe: bool = False,
        auto_paginate: bool = False,
    ):
        property_type = self.validate_property_type(property_type)

        if property_type:
            params["property_type"] = property_type

        return super().retrieve_many(
            parcl_ids=parcl_ids,
            start_date=start_date,
            end_date=end_date,
            params=params,
            as_dataframe=as_dataframe,
            auto_paginate=auto_paginate,
            get_key_on_last_request=["property_type"],
        )
