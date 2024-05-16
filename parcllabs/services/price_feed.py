from typing import Any, Mapping, Optional, List

from parcllabs.services.parcllabs_service import ParclLabsService


class PriceFeedBase(ParclLabsService):
    """
    Base class for price feed services.
    """

    def __init__(self, url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url

    def retrieve(
        self,
        parcl_id: int,
        start_date: str = None,
        end_date: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
        auto_paginate: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            **(params or {}),
        }
        results = self._request(
            url=self.url.format(parcl_id=parcl_id),
            params=params,
        )

        if auto_paginate:
            tmp = results.copy()
            while results["links"].get("next"):
                results = self._request(url=results["links"]["next"], is_next=True)
                tmp["items"].extend(results["items"])
            tmp["links"] = results["links"]
            results = tmp

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
        auto_paginate: bool = False,
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            **(params or {}),
        }
        results, _ = self.retrieve_many_items(
            parcl_ids=parcl_ids,
            params=params,
            auto_paginate=auto_paginate,
        )

        if as_dataframe:
            return self._as_pd_dataframe(results)

        return results
