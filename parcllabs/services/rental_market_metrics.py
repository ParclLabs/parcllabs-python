from typing import Any, Mapping, Optional, List

from parcllabs.services.base_service import ParclLabsService


class RentalMarketMetricsGrossYield(ParclLabsService):
    """
    Gets weekly updated rolling counts of newly listed for sale properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given <parcl_id>.
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
            url=f"/v1/rental_market_metrics/{parcl_id}/gross_yield", params=params
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


class RentalMarketMetricsRentalUnitsConcentration(ParclLabsService):
    """
    Gets weekly updated rolling counts of newly listed for sale properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given <parcl_id>.
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
            url=f"/v1/rental_market_metrics/{parcl_id}/rental_units_concentation",
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
