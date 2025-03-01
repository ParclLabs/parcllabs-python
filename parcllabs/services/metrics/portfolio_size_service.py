from typing import Any, Mapping, Optional, List
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PortfolioSizeService(ParclLabsService):
    def retrieve(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        portfolio_size: str = None,
        limit: Optional[int] = None,
        params: Optional[Mapping[str, Any]] = {},
        auto_paginate: bool = False,
    ):
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
