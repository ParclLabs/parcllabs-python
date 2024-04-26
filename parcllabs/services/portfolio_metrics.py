from typing import Any, Mapping, Optional, List

import pandas as pd

from parcllabs.services.base_service import ParclLabsService


class PortfolioMetricsSFHousingStockOwnership(ParclLabsService):
    """
    Get analytics on investor ownership and transaction activity for single family homes in markets (parcl_id), segmented by the unit size of the investor's portfolio
    """

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        out = []
        for k, l in data.items():
            for v in l:
                date = v.get("date")
                count_portfolio_2_to_9 = v.get("count").get("portfolio_2_to_9")
                count_portfolio_10_to_99 = v.get("count").get("portfolio_10_to_99")
                count_portfolio_100_to_999 = v.get("count").get("portfolio_100_to_999")
                count_portfolio_1000_plus = v.get("count").get("portfolio_1000_plus")
                count_all_portfolios = v.get("count").get("all_portfolios")
                pct_sf_housing_stock_portfolio_2_to_9 = v.get(
                    "pct_sf_housing_stock"
                ).get("portfolio_2_to_9")
                pct_sf_housing_stock_portfolio_10_to_99 = v.get(
                    "pct_sf_housing_stock"
                ).get("portfolio_10_to_99")
                pct_sf_housing_stock_portfolio_100_to_999 = v.get(
                    "pct_sf_housing_stock"
                ).get("portfolio_100_to_999")
                pct_sf_housing_stock_portfolio_1000_plus = v.get(
                    "pct_sf_housing_stock"
                ).get("portfolio_1000_plus")
                pct_sf_housing_stock_all_portfolios = v.get("pct_sf_housing_stock").get(
                    "all_portfolios"
                )

                tmp = pd.DataFrame(
                    {
                        "date": date,
                        "count_portfolio_2_to_9": count_portfolio_2_to_9,
                        "count_portfolio_10_to_99": count_portfolio_10_to_99,
                        "count_portfolio_100_to_999": count_portfolio_100_to_999,
                        "count_portfolio_1000_plus": count_portfolio_1000_plus,
                        "count_all_portfolios": count_all_portfolios,
                        "pct_sf_housing_stock_portfolio_2_to_9": pct_sf_housing_stock_portfolio_2_to_9,
                        "pct_sf_housing_stock_portfolio_10_to_99": pct_sf_housing_stock_portfolio_10_to_99,
                        "pct_sf_housing_stock_portfolio_100_to_999": pct_sf_housing_stock_portfolio_100_to_999,
                        "pct_sf_housing_stock_portfolio_1000_plus": pct_sf_housing_stock_portfolio_1000_plus,
                        "pct_sf_housing_stock_all_portfolios": pct_sf_housing_stock_all_portfolios,
                    },
                    index=[0],
                )
                tmp["parcl_id"] = k
                out.append(tmp)
        return pd.concat(out)

    def retrieve(
        self,
        parcl_id: int,
        start_date: str = None,
        end_date: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            **(params or {}),
        }
        results = self._request(
            url=f"/v1/portfolio_metrics/{parcl_id}/sf_housing_stock_ownership",
            params=params,
        )

        if as_dataframe:
            fmt = {results.get("parcl_id"): results.get("items")}
            return self._as_pd_dataframe(fmt)
        return results

    def retrieve_many(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)
        params = {
            "start_date": start_date,
            "end_date": end_date,
            **(params or {}),
        }
        results = {}
        for parcl_id in parcl_ids:
            results[parcl_id] = self.retrieve(parcl_id=parcl_id, params=params).get(
                "items"
            )

        if as_dataframe:
            return self._as_pd_dataframe(results)

        return results
