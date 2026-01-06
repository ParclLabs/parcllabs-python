### v1.16.2
- Deprecate `volatility` endpoints.

### v1.16.1
- Fix bug in auto pagination logic

### v1.16.0
- Added the `entity_seller_name` parameter to the `property_v2.search` endpoint

### v1.15.2
- Update `property_v2.search` to fix limit logic bug.

### v1.15.1
- Fix bug where `OTHER` property type was not being allowed in the `property_types` parameter in the `property_v2.search` endpoint. 

### v1.15.0
- Update to reflect enhanced `property_v2.search` endpoint.
- Chunking logic for `property_v2.search` endpoint for parcl_property_id requests.

### v1.14.5
- Update `property_v2.search` to fix pagination logic bug.

### v1.14.4
- Internal code improvements.

### v1.14.3
- Update `property_v2.search` to simplify pagination logic..

### v1.14.2
- Add autopaginate support for `property_v2.search` endpoint, increase default limit to 100,000.

### v1.14.1
- Update `property_v2.search` endpoint params.

### v1.14.0
- Simplification of parameter validation, reduce complexity of code.

### v1.13.0
- Add support for `property_v2.search` endpoint.

### v1.12.0
- Added `current_on_market_flag` parameter to `client.property.search.retrieve` to filter properties based on their current market status.

### v1.11.2
- Fixes bug on format for markets identified in the search market endpoint.
- Fixes bug on `property.search` where markets with no data prevented the function from returning existing data.

### v1.11.1
- Add list validator on all endpoints

### v1.11.0
- Add `est_credits_used` attribute to `account.client()` call to track per-request credit usage.
- Enhancements and simplification of backend logic. 

### v1.10.0
- Add support for new record_added_date and record_updated_date filters for `property.search` and `property.events` endpoints.

### v1.9.0
- Add support for response schema changes for property endpoints.
- New `remaining_credits` and `estimated_session_credit_usage` attributes to `ParclLabsClient` to track credit usage. Accessible via `client.account()` method.

### v1.8.0
- Add support for 3 new institutional owners: MAYMONT_HOMES, SFR3, VINEBROOK_HOMES for `property.search` and `property.events`

### v1.7.0
- Add support for `property_address.search` endpoint. This endpoint allows users to pass in a list of addresses and get the corresponding `parcl_property_id` back.
```python
addresses = client.property_address.search.retrieve(
    addresses=[
        {
            "address": "123 Main St",
            "city": "New York",
            "state_abbreviation": "NY",
            "zip_code": "10001",
            "source_id": "123",
        },
        {
            "address": "6251 coldwater canyon ave",
            "unit": "unit 311",
            "city": "north hollywood",
            "state_abbreviation": "CA",
            "zip_code": "91606",
            "source_id": "456",
        },
    ]
)
```

### v1.6.4
- Performance improvements to property endpoints

### v1.6.3
- Fix bug to enforce linting throughout the repository.
- Minor code improvements

### v1.6.2
- Fix bug where `limit` parameter was not being applied when `turbo_mode` was enabled.
- Update handingling of `limit` parameter. If the `limit` parameter is greater than maximum allowed limit on the particular endpoint, the `limit` will automatically default to the maximum allowed value instead of throwing an error. 
- Refactor `ParclLabsClient` so that `limit` is not set during client instantiation. Limit should be set when calling specific `retrieve` methods.

### v1.6.1
- Bug fix for handling of 422 validation errors.

### v1.6.0
- Introduce `turbo_mode` - smart routing for premium api users to reduce latency for bulk data pulls. 
- Bux fix on output dataframes - inconsistent column order.
- Improved handling of 422 data validation errors. Explicit error messages for invalid query parameters.
- Update `property.events` to accomodate api `v0.18.0` streaming changes. 
- Bug fix on property.search - incorrect validation property types. Included `OTHER`
- Introduce 3 new entity types to search both `property.search` and `property.events`: `HOME_PARTNERS_OF_AMERICA`, `PROGRESS_RESIDENTIAL`, `AMHERST`

### v1.5.0
- Performance improvement related to data processing and memory usage for `property.search`.
- `property.events` now supports concurrent requests. 
- Introduce client parameter `num_workers` to control the number of concurrent requests. Default is None, which translates to `max_workers = min(32, (os.cpu_count() or 1) + 4)`. See [docs](https://github.com/python/cpython/blob/dcc3eaef98cd94d6cb6cb0f44bd1c903d04f33b1/Lib/concurrent/futures/thread.py#L137) for more details. 

### v1.4.0
- Introduced `property.search_units`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/prop-search-refactor) for more details.

Refactored `property.search` to point to endpoint `/v1/property/search`. This is a breaking change. `property.search` used to point to an endpoint that is retiring, `/v1/property/search_markets`. 

Fixed a bug in `property.events` where records were dropping.

More flexible query dynamics for unit search. See examples: 

```python
homes = client.property.search.retrieve(
    parcl_ids=[pid],
    property_type='SINGLE_FAMILY',
    # square_footage_min=1000,
    # square_footage_max=2500,
    # bedrooms_min=3,
    # bedrooms_max=4,
    # bathrooms_min=2,
    # bathrooms_max=3,
    # year_built_min=2010,
    # year_built_max=2023,
    current_entity_owner_name='invitation_homes',
    # current_history_sale_flag=True,
    # current_history_rental_flag=True,
    # current_history_listing_flag=True,
    # current_new_oncstruciton_flag=True,
    # current_owner_occupied_flag=True,
    # current_investor_owned_flag=True,
)
```


### v1.3.0
- Introduced `property.search_units`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/prop-search-refactor) for more details.

More flexible query dynamics for unit search. See examples: 

```python
tricon_tampa_units = client.property.search_units.retrieve(
    parcl_id=2900417,
    property_type='single_family',
    current_entity_owner_name='tricon',
    limit=100
)

amh_tampa_units = client.property.search_units.retrieve(
    parcl_id=2900417,
    property_type='single_family',
    current_entity_owner_name='amh',
    limit=100
)
```

### v1.1.2
- Introduce `property.search` and `property.events`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/property-data-api-beta) for more details.

### v1.1.1
- Minor code formatting updates to improve readability

### v1.1.0
- Introduce `new_construction_metrics.housing_event_counts` and `new_construction_metrics.housing_event_prices`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/new-construction-endpoints-and-price-change-methodology-update) for more details. 
- Add beta features, including charting, technical analysis, and utilities for data analysis. 

### v1.0.1
- Bugfix on nested loops within jupyter notebook environments

### v1.0.0
**Note**: This release includes breaking changes.
- Asynchronous support for all endpoints (retrieve)
- Refactor `retrieve_many` into one `retrieve` method to support 1 to n parcl_ids
- Add utility method `client.estimated_session_credit_usage` to estimate session credit usage

### v0.5.0
- Introduce `for_sale_inventory_price_changes` and `housing_event_property_attributes`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/for-sale-market-metrics-inventory-prices-market-metrics-housing-event-property-attributes) for more details. 

### v0.4.2
- Add `auto_paginate` support for all endpoints (retrieve & retrieve_many)

### v0.4.1
- Backend Parcl Labs Services refactor

### v0.4.0
- Introduce `for_sale_inventory`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/for-sale-market-metrics-for-sale-inventory) for more details.

### v0.3.0
- Introduce `price_feed_rentals`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/price-feed-rental-price-feed) for more details.

### v0.2.2
- Add explicit throttling error message for search.

### v0.2.1
- Add `auto_paginate` to price_feed, price_feed_volatility given max limit on API is less than all observations

### v0.2.0
- Introduce `market_metrics_all_cash`, `price_feed`, `price_feed_volatiltiy`. See [Parcl Labs Changelog](https://docs.parcllabs.com/changelog/market-all-cash-price-feed-price-feed-volatility-endpoints) for more details.

### v0.1.21
- Bugfix on parsing for `new_rental_listings` metrics in `v1/investor_metrics/{parcl_id}/housing_event_prices`

### v0.1.16
- Gracefully handle no data found in retrieve_many methods

### v0.1.15
- Include progress bar for `retrieve_many` methods

### v0.1.14
- Update `portfolio_metrics` endpoint name from `new_listings_for_sale_rolling_counts` to `sf_new_listings_for_sale_rolling_counts` to reflect the new endpoint name.

### v0.1.13
- bug fix: valid portfolio parameter values.

Valid portfolio size parameter values include: 
- PORTFOLIO_2_TO_9
- PORTFOLIO_10_TO_99
- PORTFOLIO_100_TO_999
- PORTFOLIO_1000_PLUS
- ALL_PORTFOLIOS

### v0.1.12
- bug fix: dataframe index's on `retrieve_many` methods
- Update readme examples

### v0.1.11

- Update `search` to accomodate `sort_order`, `sort_by`
- `sort_by` valid values include `TOTAL_POPULATION`, `MEDIAN_INCOME`, `CASE_SHILLER_20_MARKET`, `CASE_SHILLER_10_MARKET`, `PRICEFEED_MARKET`, `PARCL_EXCHANGE_MARKET`
- include endpoint `/v1/portfolio_metrics/{parcl_id}/new_listings_for_sale_rolling_counts`
- include endpoint `/v1/rental_market_metrics/{parcl_id}/new_listings_for_rent_rolling_counts`
