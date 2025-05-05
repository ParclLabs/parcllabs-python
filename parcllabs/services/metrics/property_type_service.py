from collections.abc import Mapping
from typing import Any

import pandas as pd

from parcllabs.services.parcllabs_service import ParclLabsService


class PropertyTypeService(ParclLabsService):
    def retrieve(
        self,
        parcl_ids: list[int],
        start_date: str | None = None,
        end_date: str | None = None,
        property_type: str | None = None,
        limit: int | None = None,
        params: Mapping[str, Any] | None = {},
        auto_paginate: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieve property type metrics for given parameters.
        """
        if property_type:
            params["property_type"] = property_type.upper()

        return super().retrieve(
            parcl_ids=parcl_ids,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            params=params,
            auto_paginate=auto_paginate,
        )
