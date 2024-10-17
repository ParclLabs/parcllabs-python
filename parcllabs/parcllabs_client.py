from typing import Dict, Any, Optional

from parcllabs import api_base
from parcllabs.services.search import SearchMarkets
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.metrics.property_type_service import PropertyTypeService
from parcllabs.services.metrics.portfolio_size_service import PortfolioSizeService
from parcllabs.services.properties.property_events_service import PropertyEventsService
from parcllabs.services.properties.property_search import PropertySearch
from parcllabs.services.properties.property_address import PropertyAddressSearch


class ServiceGroup:
    def __init__(self, client):
        self._client = client
        self._services = {}

    def add_service(
        self,
        name: str,
        url: str,
        service_class: Any,
        post_url: Optional[str] = None,
        alias: Optional[str] = None,
    ):
        service = service_class(url=url, post_url=post_url, client=self._client)
        setattr(self, name, service)
        self._services[name] = service
        if alias:
            setattr(self, alias, service)
            self._services[alias] = service

    @property
    def services(self):
        return list(self._services.keys())


class ParclLabsClient:
    def __init__(
        self,
        api_key: str,
        api_url: str = api_base,
        limit: Optional[int] = None,
        turbo_mode: bool = False,
        num_workers: Optional[int] = None,
    ):
        if not api_key:
            raise ValueError(
                "API Key is required. Please visit https://dashboard.parcllabs.com/signup to get an API key."
            )

        self.api_key = api_key
        self.api_url = api_url
        self.estimated_session_credit_usage = 0
        self.num_workers = num_workers
        self.turbo_mode = turbo_mode

        self._initialize_services()

    def _initialize_services(self):
        self.price_feed = self._create_price_feed_services()
        self.investor_metrics = self._create_investor_metrics_services()
        self.market_metrics = self._create_market_metrics_services()
        self.new_construction_metrics = self._create_new_construction_metrics_services()
        self.for_sale_market_metrics = self._create_for_sale_market_metrics_services()
        self.rental_market_metrics = self._create_rental_market_metrics_services()
        self.portfolio_metrics = self._create_portfolio_metrics_services()
        self.search = self._create_search_services()
        self.property = self._create_property_services()
        self.property_address = self._create_property_address_services()

    def _create_service_group(self):
        return ServiceGroup(self)

    def _add_services_to_group(
        self, group: ServiceGroup, services: Dict[str, Dict[str, Any]]
    ):
        for name, config in services.items():
            group.add_service(name=name, **config)

    def _create_price_feed_services(self):
        group = self._create_service_group()
        services = {
            "price_feed": {
                "url": "/v1/price_feed/{parcl_id}/price_feed",
                "post_url": "/v1/price_feed/price_feed",
                "service_class": ParclLabsService,
            },
            "volatility": {
                "url": "/v1/price_feed/{parcl_id}/volatility",
                "post_url": "/v1/price_feed/volatility",
                "service_class": ParclLabsService,
            },
            "rental_price_feed": {
                "url": "/v1/price_feed/{parcl_id}/rental_price_feed",
                "post_url": "/v1/price_feed/rental_price_feed",
                "service_class": ParclLabsService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_investor_metrics_services(self):
        group = self._create_service_group()
        services = {
            "housing_stock_ownership": {
                "url": "/v1/investor_metrics/{parcl_id}/housing_stock_ownership",
                "post_url": "/v1/investor_metrics/housing_stock_ownership",
                "service_class": ParclLabsService,
            },
            "new_listings_for_sale_rolling_counts": {
                "url": "/v1/investor_metrics/{parcl_id}/new_listings_for_sale_rolling_counts",
                "post_url": "/v1/investor_metrics/new_listings_for_sale_rolling_counts",
                "service_class": PropertyTypeService,
            },
            "purchase_to_sale_ratio": {
                "url": "/v1/investor_metrics/{parcl_id}/purchase_to_sale_ratio",
                "post_url": "/v1/investor_metrics/purchase_to_sale_ratio",
                "service_class": ParclLabsService,
            },
            "housing_event_counts": {
                "url": "/v1/investor_metrics/{parcl_id}/housing_event_counts",
                "post_url": "/v1/investor_metrics/housing_event_counts",
                "service_class": ParclLabsService,
            },
            "housing_event_prices": {
                "url": "/v1/investor_metrics/{parcl_id}/housing_event_prices",
                "post_url": "/v1/investor_metrics/housing_event_prices",
                "service_class": ParclLabsService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_market_metrics_services(self):
        group = self._create_service_group()
        services = {
            "housing_event_prices": {
                "url": "/v1/market_metrics/{parcl_id}/housing_event_prices",
                "post_url": "/v1/market_metrics/housing_event_prices",
                "service_class": PropertyTypeService,
            },
            "all_cash": {
                "url": "/v1/market_metrics/{parcl_id}/all_cash",
                "post_url": "/v1/market_metrics/all_cash",
                "service_class": PropertyTypeService,
            },
            "housing_stock": {
                "url": "/v1/market_metrics/{parcl_id}/housing_stock",
                "post_url": "/v1/market_metrics/housing_stock",
                "service_class": ParclLabsService,
            },
            "housing_event_counts": {
                "url": "/v1/market_metrics/{parcl_id}/housing_event_counts",
                "post_url": "/v1/market_metrics/housing_event_counts",
                "service_class": PropertyTypeService,
            },
            "housing_event_property_attributes": {
                "url": "/v1/market_metrics/{parcl_id}/housing_event_property_attributes",
                "post_url": "/v1/market_metrics/housing_event_property_attributes",
                "service_class": PropertyTypeService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_new_construction_metrics_services(self):
        group = self._create_service_group()
        services = {
            "housing_event_prices": {
                "url": "/v1/new_construction_metrics/{parcl_id}/housing_event_prices",
                "post_url": "/v1/new_construction_metrics/housing_event_prices",
                "service_class": PropertyTypeService,
            },
            "housing_event_counts": {
                "url": "/v1/new_construction_metrics/{parcl_id}/housing_event_counts",
                "post_url": "/v1/new_construction_metrics/housing_event_counts",
                "service_class": PropertyTypeService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_for_sale_market_metrics_services(self):
        group = self._create_service_group()
        services = {
            "new_listings_rolling_counts": {
                "url": "/v1/for_sale_market_metrics/{parcl_id}/new_listings_rolling_counts",
                "post_url": "/v1/for_sale_market_metrics/new_listings_rolling_counts",
                "service_class": PropertyTypeService,
            },
            "for_sale_inventory": {
                "url": "/v1/for_sale_market_metrics/{parcl_id}/for_sale_inventory",
                "post_url": "/v1/for_sale_market_metrics/for_sale_inventory",
                "service_class": PropertyTypeService,
            },
            "for_sale_inventory_price_changes": {
                "url": "/v1/for_sale_market_metrics/{parcl_id}/for_sale_inventory_price_changes",
                "post_url": "/v1/for_sale_market_metrics/for_sale_inventory_price_changes",
                "service_class": PropertyTypeService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_rental_market_metrics_services(self):
        group = self._create_service_group()
        services = {
            "rental_units_concentration": {
                "url": "/v1/rental_market_metrics/{parcl_id}/rental_units_concentration",
                "post_url": "/v1/rental_market_metrics/rental_units_concentration",
                "service_class": PropertyTypeService,
            },
            "gross_yield": {
                "url": "/v1/rental_market_metrics/{parcl_id}/gross_yield",
                "post_url": "/v1/rental_market_metrics/gross_yield",
                "service_class": PropertyTypeService,
            },
            "new_listings_for_rent_rolling_counts": {
                "url": "/v1/rental_market_metrics/{parcl_id}/new_listings_for_rent_rolling_counts",
                "post_url": "/v1/rental_market_metrics/new_listings_for_rent_rolling_counts",
                "service_class": PropertyTypeService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_portfolio_metrics_services(self):
        group = self._create_service_group()
        services = {
            "sf_housing_stock_ownership": {
                "url": "/v1/portfolio_metrics/{parcl_id}/sf_housing_stock_ownership",
                "post_url": "/v1/portfolio_metrics/sf_housing_stock_ownership",
                "service_class": ParclLabsService,
            },
            "sf_new_listings_for_sale_rolling_counts": {
                "url": "/v1/portfolio_metrics/{parcl_id}/sf_new_listings_for_sale_rolling_counts",
                "post_url": "/v1/portfolio_metrics/sf_new_listings_for_sale_rolling_counts",
                "service_class": PortfolioSizeService,
            },
            "sf_new_listings_for_rent_rolling_counts": {
                "url": "/v1/portfolio_metrics/{parcl_id}/sf_new_listings_for_rent_rolling_counts",
                "post_url": "/v1/portfolio_metrics/sf_new_listings_for_rent_rolling_counts",
                "service_class": PortfolioSizeService,
            },
            "sf_housing_event_counts": {
                "url": "/v1/portfolio_metrics/{parcl_id}/sf_housing_event_counts",
                "post_url": "/v1/portfolio_metrics/sf_housing_event_counts",
                "service_class": PortfolioSizeService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_search_services(self):
        group = self._create_service_group()
        services = {
            "markets": {
                "url": "/v1/search/markets",
                "service_class": SearchMarkets,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_property_services(self):
        group = self._create_service_group()
        services = {
            "search": {"url": "/v1/property/search", "service_class": PropertySearch},
            "events": {
                "url": "/v1/property/event_history",
                "post_url": "/v1/property/event_history",
                "service_class": PropertyEventsService,
            },
        }
        self._add_services_to_group(group, services)
        return group

    def _create_property_address_services(self):
        group = self._create_service_group()
        services = {
            "search": {
                "url": "/v1/property/search_address",
                "post_url": "/v1/property/search_address",
                "service_class": PropertyAddressSearch,
            },
        }
        self._add_services_to_group(group, services)
        return group
