from typing import Any, Mapping, Optional, List

import pandas as pd

from parcllabs.services.base_service import ParclLabsService


class InvestorMetricsHousingEventCounts(ParclLabsService):
    """
    Gets counts of investor-owned properties and their corresponding percentage ownership share of the total housing stock, for a specified <parcl_id>.
    """

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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = self._request(
            url=f"/v1/investor_metrics/{parcl_id}/housing_event_counts", params=params
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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = {}
        for parcl_id in parcl_ids:
            results[parcl_id] = self.retrieve(parcl_id=parcl_id, params=params).get(
                "items"
            )

        if as_dataframe:
            return self._as_pd_dataframe(results)

        return results


class InvestorMetricsPurchaseToSaleRatio(ParclLabsService):
    """
    Gets counts of investor-owned properties and their corresponding percentage ownership share of the total housing stock, for a specified <parcl_id>.
    """

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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = self._request(
            url=f"/v1/investor_metrics/{parcl_id}/purchase_to_sale_ratio", params=params
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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = {}
        for parcl_id in parcl_ids:
            results[parcl_id] = self.retrieve(parcl_id=parcl_id, params=params).get(
                "items"
            )

        if as_dataframe:
            return self._as_pd_dataframe(results)

        return results


class InvestorMetricsHousingStockOwnership(ParclLabsService):
    """
    Gets counts of investor-owned properties and their corresponding percentage ownership share of the total housing stock, for a specified <parcl_id>.
    """

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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = self._request(
            url=f"/v1/investor_metrics/{parcl_id}/housing_stock_ownership",
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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = {}
        for parcl_id in parcl_ids:
            results[parcl_id] = self.retrieve(parcl_id=parcl_id, params=params).get(
                "items"
            )

        if as_dataframe:
            return self._as_pd_dataframe(results)

        return results


class InvesetorMetricsNewListingsForSaleRollingCounts(ParclLabsService):
    """
    Gets weekly updated rolling counts of investor-owned properties newly listed for sale, and their corresponding percentage share of the total for-sale listings market. These metrics are segmented into 7, 30, 60, and 90-day periods ending on a specified date, based on a given <parcl_id>
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
                pct_7_count = v.get("pct_for_sale_market").get("rolling_7_day")
                pct_30_count = v.get("pct_for_sale_market").get("rolling_30_day")
                pct_60_count = v.get("pct_for_sale_market").get("rolling_60_day")
                pct_90_count = v.get("pct_for_sale_market").get("rolling_90_day")
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
                        "pct_for_sale_market": pcts,
                    }
                )
                tmp["parcl_id"] = k
                out.append(tmp)
        return pd.concat(out)

    def retrieve(
        self,
        parcl_id: int,
        start_date: str = None,
        end_date: str = None,
        property_type: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)
        property_type = self.validate_property_type(property_type)
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "property_type": property_type,
            **(params or {}),
        }
        results = self._request(
            url=f"/v1/investor_metrics/{parcl_id}/new_listings_for_sale_rolling_counts",
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
        property_type: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)
        property_type = self.validate_property_type(property_type)
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "property_type": property_type,
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


class InvestorMetricsHousingStockOwnership(ParclLabsService):
    """
    Gets counts of investor-owned properties and their corresponding percentage ownership share of the total housing stock, for a specified <parcl_id>.
    """

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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = self._request(
            url=f"/v1/investor_metrics/{parcl_id}/housing_stock_ownership",
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
        params = {"start_date": start_date, "end_date": end_date, **(params or {})}
        results = {}
        for parcl_id in parcl_ids:
            results[parcl_id] = self.retrieve(parcl_id=parcl_id, params=params).get(
                "items"
            )

        if as_dataframe:
            return self._as_pd_dataframe(results)

        return results
