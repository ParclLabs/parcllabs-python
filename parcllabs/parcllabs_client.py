from parcllabs import api_base
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.portfolio_size_service import PortfolioSizeService
from parcllabs.services.property_events_service import PropertyEventsService
from parcllabs.services.property_search import PropertySearch
from parcllabs.services.property_type_service import PropertyTypeService
from parcllabs.services.search import SearchMarkets


class ServiceGroup:
    """
    Class to organize services into groups for easier access.

    Args:
        client (ParclLabsClient): The ParclLabsClient object.
        limit (int): The number of items to return per request.
    """

    def __init__(self, client, limit):
        self._client = client
        self._limit = limit
        self._services = {}

    def add_service(self, name, url, service_class):
        service = service_class(url=url, client=self._client, limit=self._limit)
        setattr(self, name, service)
        self._services[name] = service

    @property
    def services(self):
        return list(self._services.keys())


class ParclLabsClient:
    """
    Client for the Parcl Labs API.

    Args:
        api_key (str): The API key for the Parcl Labs API.
        limit (int): The number of items to return per request.
    """

    def __init__(self, api_key: str, limit: int = 12):
        if not api_key:
            raise ValueError(
                "API Key is required. Please visit https://dashboard.parcllabs.com/signup to get an API key."
            )

        self.api_key = api_key
        self.api_url = api_base
        self.limit = limit
        self.estimated_session_credit_usage = 0

        self.price_feed = ServiceGroup(self, limit)
        self.price_feed.add_service(
            "price_feed", "/v1/price_feed/{parcl_id}/price_feed", ParclLabsService
        )
        self.price_feed.add_service(
            "volatility", "/v1/price_feed/{parcl_id}/volatility", ParclLabsService
        )
        self.price_feed.add_service(
            "rental_price_feed",
            "/v1/price_feed/{parcl_id}/rental_price_feed",
            ParclLabsService,
        )

        self.investor_metrics = ServiceGroup(self, limit)
        self.investor_metrics.add_service(
            "housing_stock_ownership",
            "/v1/investor_metrics/{parcl_id}/housing_stock_ownership",
            ParclLabsService,
        )
        self.investor_metrics.add_service(
            "new_listings_for_sale_rolling_counts",
            "/v1/investor_metrics/{parcl_id}/new_listings_for_sale_rolling_counts",
            PropertyTypeService,
        )
        self.investor_metrics.add_service(
            "purchase_to_sale_ratio",
            "/v1/investor_metrics/{parcl_id}/purchase_to_sale_ratio",
            ParclLabsService,
        )
        self.investor_metrics.add_service(
            "housing_event_counts",
            "/v1/investor_metrics/{parcl_id}/housing_event_counts",
            ParclLabsService,
        )
        self.investor_metrics.add_service(
            "housing_event_prices",
            "/v1/investor_metrics/{parcl_id}/housing_event_prices",
            ParclLabsService,
        )

        self.market_metrics = ServiceGroup(self, limit)
        self.market_metrics.add_service(
            "housing_event_prices",
            "/v1/market_metrics/{parcl_id}/housing_event_prices",
            PropertyTypeService,
        )
        self.market_metrics.add_service(
            "all_cash", "/v1/market_metrics/{parcl_id}/all_cash", PropertyTypeService
        )
        self.market_metrics.add_service(
            "housing_stock",
            "/v1/market_metrics/{parcl_id}/housing_stock",
            ParclLabsService,
        )
        self.market_metrics.add_service(
            "housing_event_counts",
            "/v1/market_metrics/{parcl_id}/housing_event_counts",
            PropertyTypeService,
        )
        self.market_metrics.add_service(
            "housing_event_property_attributes",
            "/v1/market_metrics/{parcl_id}/housing_event_property_attributes",
            PropertyTypeService,
        )

        self.new_construction_metrics = ServiceGroup(self, limit)

        self.new_construction_metrics.add_service(
            "housing_event_prices",
            "/v1/new_construction_metrics/{parcl_id}/housing_event_prices",
            PropertyTypeService,
        )
        self.new_construction_metrics.add_service(
            "housing_event_counts",
            "/v1/new_construction_metrics/{parcl_id}/housing_event_counts",
            PropertyTypeService,
        )

        self.for_sale_market_metrics = ServiceGroup(self, limit)
        self.for_sale_market_metrics.add_service(
            "new_listings_rolling_counts",
            "/v1/for_sale_market_metrics/{parcl_id}/new_listings_rolling_counts",
            PropertyTypeService,
        )
        self.for_sale_market_metrics.add_service(
            "for_sale_inventory",
            "/v1/for_sale_market_metrics/{parcl_id}/for_sale_inventory",
            PropertyTypeService,
        )
        self.for_sale_market_metrics.add_service(
            "for_sale_inventory_price_changes",
            "/v1/for_sale_market_metrics/{parcl_id}/for_sale_inventory_price_changes",
            PropertyTypeService,
        )

        self.rental_market_metrics = ServiceGroup(self, limit)
        self.rental_market_metrics.add_service(
            "rental_units_concentration",
            "/v1/rental_market_metrics/{parcl_id}/rental_units_concentration",
            PropertyTypeService,
        )
        self.rental_market_metrics.add_service(
            "gross_yield",
            "/v1/rental_market_metrics/{parcl_id}/gross_yield",
            PropertyTypeService,
        )
        self.rental_market_metrics.add_service(
            "new_listings_for_rent_rolling_counts",
            "/v1/rental_market_metrics/{parcl_id}/new_listings_for_rent_rolling_counts",
            PropertyTypeService,
        )

        self.portfolio_metrics = ServiceGroup(self, limit)
        self.portfolio_metrics.add_service(
            "sf_housing_stock_ownership",
            "/v1/portfolio_metrics/{parcl_id}/sf_housing_stock_ownership",
            ParclLabsService,
        )
        self.portfolio_metrics.add_service(
            "sf_new_listings_for_sale_rolling_counts",
            "/v1/portfolio_metrics/{parcl_id}/sf_new_listings_for_sale_rolling_counts",
            PortfolioSizeService,
        )
        self.portfolio_metrics.add_service(
            "sf_new_listings_for_rent_rolling_counts",
            "/v1/portfolio_metrics/{parcl_id}/sf_new_listings_for_rent_rolling_counts",
            PortfolioSizeService,
        )
        self.portfolio_metrics.add_service(
            "sf_housing_event_counts",
            "/v1/portfolio_metrics/{parcl_id}/sf_housing_event_counts",
            PortfolioSizeService,
        )

        self.search = ServiceGroup(self, limit)
        self.search.add_service("markets", "/v1/search/markets", SearchMarkets)

        self.property = ServiceGroup(self, limit)
        self.property.add_service(
            "search", "/v1/property/search_markets", PropertySearch
        )
        self.property.add_service(
            "events", "/v1/property/event_history", PropertyEventsService
        )
