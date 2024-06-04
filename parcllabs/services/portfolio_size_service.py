from typing import Any, Mapping, Optional, List
from parcllabs.services.parcllabs_service import ParclLabsService


class PortfolioSizeService(ParclLabsService):
    def __init__(self, client, url):
        super().__init__(client=client, url=url)

    def retrieve(
        self,
        parcl_id: int,
        start_date: str = None,
        end_date: str = None,
        portfolio_size: str = None,
        params: Optional[Mapping[str, Any]] = {},
        as_dataframe: bool = False,
    ):
        portfolio_size = self.validate_portfolio_size(portfolio_size)

        if portfolio_size:
            params["portfolio_size"] = portfolio_size

        return super().retrieve(
            parcl_id=parcl_id,
            start_date=start_date,
            end_date=end_date,
            params=params,
            as_dataframe=as_dataframe,
        )

    def retrieve_many(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        portfolio_size: str = None,
        params: Optional[Mapping[str, Any]] = {},
        as_dataframe: bool = False,
    ):
        portfolio_size = self.validate_portfolio_size(portfolio_size)

        if portfolio_size:
            params["portfolio_size"] = portfolio_size

        return super().retrieve_many(
            parcl_ids=parcl_ids,
            start_date=start_date,
            end_date=end_date,
            params=params,
            as_dataframe=as_dataframe,
            get_key_on_last_request=["portfolio_size"],
        )
