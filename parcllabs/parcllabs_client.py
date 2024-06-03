import json
import requests
from typing import Dict
from requests.exceptions import RequestException
from parcllabs import api_base
from parcllabs.services.price_feed import (
    PriceFeedBaseService,
)
from parcllabs.services.investor_metrics import (
    InvestorMetricsBaseService,
)
from parcllabs.services.market_metrics import (
    MarketMetricsBaseService,
)
from parcllabs.services.for_sale_market_metrics import (
    ForSaleMarketMetricsBaseService,
)
from parcllabs.services.rental_market_metrics import (
    RentalMarketMetricsBaseService,
)
from parcllabs.services.portfolio_metrics import (
    PortfolioMetricsBaseService,
)
from parcllabs.services.search import SearchMarkets


class ParclLabsClient:
    def __init__(self, api_key: str, limit: int = 12):
        """
        Create a ParclLabsClient client.

        Args:
            api_key (str): A Parcl Labs API key.
            limit (int, optional): The number of items to return per page. Defaults to 12.

        Raises:
            ValueError: If the API key is not provided.

        """
        if not api_key:
            raise ValueError(
                "API Key is required. Please visit https://dashboard.parcllabs.com/signup to get an API key."
            )

        self.api_key = api_key
        self.api_url = api_base
        self.limit = limit

        # price feed services
        self.price_feed = PriceFeedBaseService(
            url="/v1/price_feed/{parcl_id}/price_feed", client=self
        )
        self.price_feed_volatility = PriceFeedBaseService(
            url="/v1/price_feed/{parcl_id}/volatility", client=self
        )

        self.rental_price_feed = PriceFeedBaseService(
            url="/v1/price_feed/{parcl_id}/rental_price_feed", client=self
        )

        # investor metrics services
        self.investor_metrics_housing_stock_ownership = InvestorMetricsBaseService(
            url="/v1/investor_metrics/{parcl_id}/housing_stock_ownership", client=self
        )
        self.investor_metrics_new_listings_for_sale_rolling_counts = InvestorMetricsBaseService(
            url="/v1/investor_metrics/{parcl_id}/new_listings_for_sale_rolling_counts",
            client=self,
        )
        self.investor_metrics_purchase_to_sale_ratio = InvestorMetricsBaseService(
            url="/v1/investor_metrics/{parcl_id}/purchase_to_sale_ratio", client=self
        )
        self.investor_metrics_housing_event_counts = InvestorMetricsBaseService(
            url="/v1/investor_metrics/{parcl_id}/housing_event_counts", client=self
        )
        self.investor_metrics_housing_event_prices = InvestorMetricsBaseService(
            url="/v1/investor_metrics/{parcl_id}/housing_event_prices", client=self
        )

        # market metrics services
        self.market_metrics_housing_event_prices = MarketMetricsBaseService(
            url="/v1/market_metrics/{parcl_id}/housing_event_prices", client=self
        )
        self.market_metrics_all_cash = MarketMetricsBaseService(
            url="/v1/market_metrics/{parcl_id}/all_cash", client=self
        )
        self.market_metrics_housing_stock = MarketMetricsBaseService(
            url="/v1/market_metrics/{parcl_id}/housing_stock", client=self
        )
        self.market_metrics_housing_event_counts = MarketMetricsBaseService(
            url="/v1/market_metrics/{parcl_id}/housing_event_counts", client=self
        )

        # for sale market metrics
        self.for_sale_market_metrics_new_listings_rolling_counts = ForSaleMarketMetricsBaseService(
            url="/v1/for_sale_market_metrics/{parcl_id}/new_listings_rolling_counts",
            client=self,
        )
        self.for_sale_market_metrics_for_sale_inventory = (
            ForSaleMarketMetricsBaseService(
                url="/v1/for_sale_market_metrics/{parcl_id}/for_sale_inventory",
                client=self,
            )
        )

        # rental market metrics services
        self.rental_market_metrics_rental_units_concentration = (
            RentalMarketMetricsBaseService(
                url="/v1/rental_market_metrics/{parcl_id}/rental_units_concentration",
                client=self,
            )
        )
        self.rental_market_metrics_gross_yield = RentalMarketMetricsBaseService(
            url="/v1/rental_market_metrics/{parcl_id}/gross_yield", client=self
        )
        self.rental_market_metrics_new_listings_for_rent_rolling_counts = RentalMarketMetricsBaseService(
            url="/v1/rental_market_metrics/{parcl_id}/new_listings_for_rent_rolling_counts",
            client=self,
        )

        # portfolio metrics services
        self.portfolio_metrics_sf_housing_stock_ownership = PortfolioMetricsBaseService(
            url="/v1/portfolio_metrics/{parcl_id}/sf_housing_stock_ownership",
            client=self,
        )
        self.portfolio_metrics_new_listings_for_sale_rolling_counts = PortfolioMetricsBaseService(
            url="/v1/portfolio_metrics/{parcl_id}/sf_new_listings_for_sale_rolling_counts",
            client=self,
        )
        self.portfolio_metrics_sf_new_listings_for_rent_rolling_counts = PortfolioMetricsBaseService(
            url="/v1/portfolio_metrics/{parcl_id}/sf_new_listings_for_rent_rolling_counts",
            client=self,
        )
        self.portfolio_metrics_sf_housing_event_counts = PortfolioMetricsBaseService(
            url="/v1/portfolio_metrics/{parcl_id}/sf_housing_event_counts",
            client=self,
        )

        # search services
        self.search_markets = SearchMarkets(url="/v1/search/markets", client=self)

    def get(self, url: str, params: dict = None, is_next: bool = False):
        """
        Send a GET request to the specified URL with the given parameters.

        Args:
            url (str): The URL endpoint to request.
            params (dict, optional): The parameters to send in the query string.

        Returns:
            dict: The JSON response as a dictionary.
        """

        if params:
            if not params.get("limit"):
                params["limit"] = self.limit
        try:
            if is_next:
                full_url = url
            else:
                full_url = self.api_url + url
            headers = self._get_headers()
            response = requests.get(full_url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            try:
                error_details = response.json()
                error_message = error_details.get("detail", "No detail provided by API")
                error = error_message
                if response.status_code == 403:
                    error = f"{error_message}. Visit https://dashboard.parcllabs.com for more information or reach out to team@parcllabs.com."
                if response.status_code == 429:
                    error = error_details.get("error", "Rate Limit Exceeded")
            except json.JSONDecodeError:
                error_message = "Failed to decode JSON error response"
            type_of_error = ""
            if 400 <= response.status_code < 500:
                type_of_error = "Client"
            elif 500 <= response.status_code < 600:
                type_of_error = "Server"
            msg = f"{response.status_code} {type_of_error} Error: {error}"
            raise RequestException(msg)
        except requests.exceptions.RequestException as err:
            raise RequestException(f"Request failed: {str(err)}")
        except Exception as e:
            raise RequestException(f"An unexpected error occurred: {str(e)}")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json",
        }
