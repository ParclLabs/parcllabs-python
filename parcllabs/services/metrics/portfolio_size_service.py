from collections.abc import Mapping
from typing import Any

import pandas as pd

from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PortfolioSizeService(ParclLabsService):
    def retrieve(
        self,
        parcl_ids: list[int],
        start_date: str | None = None,
        end_date: str | None = None,
        portfolio_size: str | None = None,
        limit: int | None = None,
        params: Mapping[str, Any] | None = {},
        auto_paginate: bool = False,
    ) -> pd.DataFrame:
        portfolio_size = Validators.validate_portfolio_size(portfolio_size)
        parcl_ids = Validators.validate_integer_list(parcl_ids, "parcl_ids")

        if portfolio_size:
            params["portfolio_size"] = portfolio_size

        return super().retrieve(
            parcl_ids=parcl_ids,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            params=params,
            auto_paginate=auto_paginate,
        )
