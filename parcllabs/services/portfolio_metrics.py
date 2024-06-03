from typing import Any, Mapping, Optional, List

import pandas as pd

from parcllabs.services.parcllabs_service import ParclLabsService


class PortfolioMetricsBaseService(ParclLabsService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrieve(
        self,
        parcl_id: int,
        start_date: str = None,
        end_date: str = None,
        portfolio_size: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)
        portfolio_size = self.validate_portfolio_size(portfolio_size)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "portfolio_size": portfolio_size,
            **(params or {}),
        }
        results = self._request(
            parcl_id=parcl_id,
            params=params,
        )

        if as_dataframe:
            fmt = {results.get("parcl_id"): results.get("items")}
            df = self._as_pd_dataframe(fmt)
            if portfolio_size:
                df["portfolio_size"] = results.get("portfolio_size")
            return df

        return results

    def retrieve_many(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        portfolio_size: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)
        portfolio_size = self.validate_portfolio_size(portfolio_size)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "portfolio_size": portfolio_size,
            **(params or {}),
        }

        get_key_on_last_request = "portfolio_size" if portfolio_size else None
        results, p_size = self.retrieve_many_items(
            parcl_ids=parcl_ids,
            params=params,
            get_key_on_last_request=get_key_on_last_request,
        )

        if as_dataframe:
            df = self._as_pd_dataframe(results)
            if portfolio_size:
                df["portfolio_size"] = p_size
            return df

        return results
