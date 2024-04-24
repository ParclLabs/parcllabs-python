from typing import Any, Mapping, Optional, List

import pandas as pd

from parcllabs.services.base_service import ParclLabsService


class MarketMetricsHousingEventCounts(ParclLabsService):
    """
    Gets monthly counts of housing events, including sales, new sale listings, and new rental listings, based on a specified <parcl_id>.
    """

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
            url=f"/v1/market_metrics/{parcl_id}/housing_event_counts", params=params
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


class MarketMetricsHousingStock(ParclLabsService):
    """
    Gets housing stock for a specified <parcl_id>. Housing stock represents the total number of properties, broken out by single family homes, townhouses, and condos.
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
            url=f"/v1/market_metrics/{parcl_id}/housing_stock", params=params
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


class MarketMetricsHousingEventPrices(ParclLabsService):
    """
    Gets monthly statistics on prices for housing events, including sales, new for-sale listings, and new rental listings, based on a specified <parcl_id>.
    """

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        out = []
        for k, l in data.items():
            for v in l:
                date = v.get("date")
                price_median_sales = v.get("price").get("median").get("sales")
                price_median_new_listings_for_sale = (
                    v.get("price").get("median").get("new_listings_for_sale")
                )
                price_median_new_rental_listings = (
                    v.get("price").get("median").get("new_rental_listings")
                )
                price_standard_deviation_sales = (
                    v.get("price").get("standard_deviation").get("sales")
                )
                price_standard_deviation_new_listings_for_sale = (
                    v.get("price")
                    .get("standard_deviation")
                    .get("new_listings_for_sale")
                )
                price_standard_deviation_new_rental_listings = (
                    v.get("price").get("standard_deviation").get("new_rental_listings")
                )
                price_percentile_20th_sales = (
                    v.get("price").get("percentile_20th").get("sales")
                )
                price_percentile_20th_new_listings_for_sale = (
                    v.get("price").get("percentile_20th").get("new_listings_for_sale")
                )
                price_percentile_20th_new_rental_listings = (
                    v.get("price").get("percentile_20th").get("new_rental_listings")
                )
                price_percentile_80th_sales = (
                    v.get("price").get("percentile_80th").get("sales")
                )
                price_percentile_80th_new_listings_for_sale = (
                    v.get("price").get("percentile_80th").get("new_listings_for_sale")
                )
                price_percentile_80th_new_rental_listings = (
                    v.get("price").get("percentile_80th").get("new_rental_listings")
                )
                price_per_square_foot_median_sales = (
                    v.get("price_per_square_foot").get("median").get("sales")
                )
                price_per_square_foot_median_new_listings_for_sale = (
                    v.get("price_per_square_foot")
                    .get("median")
                    .get("new_listings_for_sale")
                )
                price_per_square_foot_median_new_rental_listings = (
                    v.get("price_per_square_foot")
                    .get("median")
                    .get("new_rental_listings")
                )
                price_per_square_foot_standard_deviation_sales = (
                    v.get("price_per_square_foot")
                    .get("standard_deviation")
                    .get("sales")
                )
                price_per_square_foot_standard_deviation_new_listings_for_sale = (
                    v.get("price_per_square_foot")
                    .get("standard_deviation")
                    .get("new_listings_for_sale")
                )
                price_per_square_foot_standard_deviation_new_rental_listings = (
                    v.get("price_per_square_foot")
                    .get("standard_deviation")
                    .get("new_rental_listings")
                )
                price_per_square_foot_percentile_20th_sales = (
                    v.get("price_per_square_foot").get("percentile_20th").get("sales")
                )
                price_per_square_foot_percentile_20th_new_listings_for_sale = (
                    v.get("price_per_square_foot")
                    .get("percentile_20th")
                    .get("new_listings_for_sale")
                )
                price_per_square_foot_percentile_20th_new_rental_listings = (
                    v.get("price_per_square_foot")
                    .get("percentile_20th")
                    .get("new_rental_listings")
                )
                price_per_square_foot_percentile_80th_sales = (
                    v.get("price_per_square_foot").get("percentile_80th").get("sales")
                )
                price_per_square_foot_percentile_80th_new_listings_for_sale = (
                    v.get("price_per_square_foot")
                    .get("percentile_80th")
                    .get("new_listings_for_sale")
                )
                price_per_square_foot_percentile_80th_new_rental_listings = (
                    v.get("price_per_square_foot")
                    .get("percentile_80th")
                    .get("new_rental_listings")
                )
                tmp = pd.DataFrame(
                    {
                        "date": date,
                        "price_median_sales": price_median_sales,
                        "price_median_new_listings_for_sale": price_median_new_listings_for_sale,
                        "price_median_new_rental_listings": price_median_new_rental_listings,
                        "price_standard_deviation_sales": price_standard_deviation_sales,
                        "price_standard_deviation_new_listings_for_sale": price_standard_deviation_new_listings_for_sale,
                        "price_standard_deviation_new_rental_listings": price_standard_deviation_new_rental_listings,
                        "price_percentile_20th_sales": price_percentile_20th_sales,
                        "price_percentile_20th_new_listings_for_sale": price_percentile_20th_new_listings_for_sale,
                        "price_percentile_20th_new_rental_listings": price_percentile_20th_new_rental_listings,
                        "price_percentile_80th_sales": price_percentile_80th_sales,
                        "price_percentile_80th_new_listings_for_sale": price_percentile_80th_new_listings_for_sale,
                        "price_percentile_80th_new_rental_listings": price_percentile_80th_new_rental_listings,
                        "price_per_square_foot_median_sales": price_per_square_foot_median_sales,
                        "price_per_square_foot_median_new_listings_for_sale": price_per_square_foot_median_new_listings_for_sale,
                        "price_per_square_foot_median_new_rental_listings": price_per_square_foot_median_new_rental_listings,
                        "price_per_square_foot_standard_deviation_sales": price_per_square_foot_standard_deviation_sales,
                        "price_per_square_foot_standard_deviation_new_listings_for_sale": price_per_square_foot_standard_deviation_new_listings_for_sale,
                        "price_per_square_foot_standard_deviation_new_rental_listings": price_per_square_foot_standard_deviation_new_rental_listings,
                        "price_per_square_foot_percentile_20th_sales": price_per_square_foot_percentile_20th_sales,
                        "price_per_square_foot_percentile_20th_new_listings_for_sale": price_per_square_foot_percentile_20th_new_listings_for_sale,
                        "price_per_square_foot_percentile_20th_new_rental_listings": price_per_square_foot_percentile_20th_new_rental_listings,
                        "price_per_square_foot_percentile_80th_sales": price_per_square_foot_percentile_80th_sales,
                        "price_per_square_foot_percentile_80th_new_listings_for_sale": price_per_square_foot_percentile_80th_new_listings_for_sale,
                        "price_per_square_foot_percentile_80th_new_rental_listings": price_per_square_foot_percentile_80th_new_rental_listings,
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
            url=f"/v1/market_metrics/{parcl_id}/housing_event_prices", params=params
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
