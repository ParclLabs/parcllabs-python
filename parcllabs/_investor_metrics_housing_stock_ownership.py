from typing import Any, Mapping, Optional, List

from parcllabs._parcllabs_service import ParclLabsService


class InvestorMetricsHousingStockOwnership(ParclLabsService):

    def retrieve(
            self,
            parcl_id: int,
            params: Optional[Mapping[str, Any]] = None
    ):
        return self._request(
            url=f'/v1/investor_metrics/{parcl_id}/housing_stock_ownership', 
            params=params
        )
    
    def retrieve_many(
            self,
            parcl_ids: List[int],
            params: Optional[Mapping[str, Any]] = None
    ):
        results = {}
        for parcl_id in parcl_ids:
            results[parcl_id] = self.retrieve(
                parcl_id=parcl_id, 
                params=params
            ).get('items')
        return results
    
    def retrieve_many_as_pd_dataframe(
            self,
            parcl_ids: List[int],
            params: Optional[Mapping[str, Any]] = None
    ):
        results = self.retrieve_many(parcl_ids=parcl_ids, params=params)
        return self._as_pd_dataframe(results)