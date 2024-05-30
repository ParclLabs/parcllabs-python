from typing import Any, Mapping, Optional, List

import pandas as pd


from parcllabs.services.parcllabs_service import ParclLabsService


class ForSaleMarketMetricsBaseService(ParclLabsService):
    """
    Base class for for sale market metrics services.
    """

    def __init__(self, url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url

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
            url=self.url.format(parcl_id=parcl_id),
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

        results, _ = self.retrieve_many_items(parcl_ids=parcl_ids, params=params)

        if as_dataframe:
            return self._as_pd_dataframe(results)

        return results
