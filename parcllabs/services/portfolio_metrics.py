from typing import Any, Mapping, Optional, List

import pandas as pd

from parcllabs.services.base_service import ParclLabsService


valid_portfolio_sizes = [
    "PORTFOLIO_2_TO_9",
    "PORTOFLIO_10_TO_99",
    "PORTFOLIO_100_TO_999",
    "PORTFOLIO_1000_PLUS",
    "ALL_PORTFOLIOS",
]


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
        return pd.concat(out).reset_index(drop=True)

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


class PortfolioMetricsNewListingsForSaleRollingCounts(ParclLabsService):
    """
    Get analytics on the number of new listings for sale in markets (parcl_id), segmented by the unit size of the investor's portfolio
    """

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        out = []
        for k, l in data.items():
            for v in l:
                date = v.get("date")
                roll_7_count = v.get("count").get("rolling_7_day")
                roll_30_count = v.get("count").get("rolling_30_day")
                roll_60_count = v.get("count").get("rolling_60_day")
                roll_90_count = v.get("count").get("rolling_90_day")
                pct_7_count = v.get("pct_sf_for_sale_market").get("rolling_7_day")
                pct_30_count = v.get("pct_sf_for_sale_market").get("rolling_30_day")
                pct_60_count = v.get("pct_sf_for_sale_market").get("rolling_60_day")
                pct_90_count = v.get("pct_sf_for_sale_market").get("rolling_90_day")
                counts = [roll_7_count, roll_30_count, roll_60_count, roll_90_count]
                pcts = [pct_7_count, pct_30_count, pct_60_count, pct_90_count]
                names = [
                    "rolling_7_day",
                    "rolling_30_day",
                    "rolling_60_day",
                    "rolling_90_day",
                ]
                tmp = pd.DataFrame(
                    {
                        "date": date,
                        "period": names,
                        "counts": counts,
                        "pct_sf_for_sale_market": pcts,
                    }
                )
                tmp["parcl_id"] = k
                out.append(tmp)
        return pd.concat(out).reset_index(drop=True)

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

        if portfolio_size is not None and portfolio_size not in valid_portfolio_sizes:
            raise ValueError(
                f"location_type value error. Valid values are: {valid_portfolio_sizes}. Received: {portfolio_size}"
            )

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "portfolio_size": portfolio_size,
            **(params or {}),
        }
        results = self._request(
            url=f"/v1/portfolio_metrics/{parcl_id}/new_listings_for_sale_rolling_counts",
            params=params,
        )

        if as_dataframe:
            fmt = {results.get("parcl_id"): results.get("items")}
            df = self._as_pd_dataframe(fmt)
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

        if portfolio_size is not None and portfolio_size not in valid_portfolio_sizes:
            raise ValueError(
                f"location_type value error. Valid values are: {valid_portfolio_sizes}. Received: {portfolio_size}"
            )

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "portfolio_size": portfolio_size,
            **(params or {}),
        }
        results = {}
        for parcl_id in parcl_ids:
            output = self.retrieve(parcl_id=parcl_id, params=params)
            results[parcl_id] = output.get("items")
            p_size = output.get("portfolio_size")

        if as_dataframe:
            df = self._as_pd_dataframe(results)
            df["portfolio_size"] = p_size
            return df

        return results
