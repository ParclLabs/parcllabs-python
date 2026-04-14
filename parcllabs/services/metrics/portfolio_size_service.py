from typing import Any

import pandas as pd

from parcllabs.services.parcllabs_service import ParclLabsService


class PortfolioSizeService(ParclLabsService):
    def retrieve(
        self,
        parcl_ids: list[int],
        start_date: str | None = None,
        end_date: str | None = None,
        portfolio_size: str | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
        auto_paginate: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieve portfolio size metrics for given parameters.
        """
        if params is None:
            params = {}

        if portfolio_size:
            params["portfolio_size"] = portfolio_size.upper()

        return super().retrieve(
            parcl_ids=parcl_ids,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            params=params,
            auto_paginate=auto_paginate,
        )
