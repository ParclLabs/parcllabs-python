<!-- readme header split -->
![Logo](img/labs.png)
![GitHub Tag](https://img.shields.io/github/v/tag/ParclLabs/parcllabs-python)
![PyPI - Downloads](https://img.shields.io/pypi/dm/parcllabs)
<!-- readme header end -->

# Parcl Labs Python SDK

**We're on a mission to create the world's best API developer experience and community for housing data.**

Our SDK is designed to supercharge your API experience and accelerate your time to insight. It enables you to efficiently pull the data you need, analyze it, and visualize your findings.

## Table of Contents
- [Data Overview](#parcl-labs-data-overview)
- [Getting Started](#getting-started)
- [Services](#services)
  - [Search](#search)
  - [Rental Market Metrics](#rental-market-metrics)
  - [For Sale Market Metrics](#for-sale-market-metrics)
  - [Market Metrics](#market-metrics)
  - [New Construction Metrics](#new-construction-metrics)
  - [Investor Metrics](#investor-metrics)
  - [Portfolio Metrics](#portfolio-metrics)
  - [Price Feeds](#price-feeds)
  - [Property](#property)
  - [Property Address Search](#property-address-search)
  - [Property Search V2](#property-search-v2)
  - [Account Info](#account-info)
- [Cookbook](#cookbook)

<!-- readme header split -->
## Parcl Labs Data Overview <a id="parcl-labs-data-overview"></a>

The Parcl Labs API provides **instant insights into the U.S. housing market**, delivering data on housing supply, sales, listings, rentals, investor activities, and market trends.

_The most complete picture of US residential real estate_

| Category           | Coverage                                                                                                                                                      |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Property Types** | **🏘️ All Residential Assets:**<br>✅ Single Family<br>✅ Townhouses<br>✅ Condos<br>✅ Other                                                                      |
| **Markets**        | **🇺🇸 Complete National Coverage, 70k+ Unique Markets at Any Level of Granularity:**<br>✅ Regions<br>✅ States<br>✅ Metros<br>✅ Cities<br>✅ Counties<br>✅ Towns<br>✅ Zips<br>✅ Census Places |
| **Housing Events** | **🔄 The Full Property Lifecycle:**<br>✅ Sales<br>✅ For Sale Listings<br>✅ Rentals                                                                              |
<!-- readme header end -->

## Cookbook <a id="cookbook"></a>

We maintain a repository of examples that demonstrate how to use the Parcl Labs API for analysis. You can find the examples in the [Parcl Labs Cookbook](https://github.com/parcllabs/parcllabs-cookbook)

## Getting Started <a id="getting-started"></a>

### Step 1. Sign Up for an API Key

To use the Parcl Labs API, you need an API key. To get an API key, sign up at [ParclLabs](https://dashboard.parcllabs.com/signup). In the subsequent examples, the API key is stored in the `PARCLLABS_API_KEY` environment variable.

### Step 2. Installation

You can install the package via pip:

```bash
pip install -U parcllabs
```

### Step 3. Usage

The `ParclLabsClient` class is the entry point to the Parcl Labs API. You can use the client to access methods that allow you to retrieve and analyze data from the Parcl Labs API. You'll need to pass in your API key when you create an instance of the `ParclLabsClient` class.

```python
import os

from parcllabs import ParclLabsClient


api_key = os.getenv('PARCL_LABS_API_KEY')
client = ParclLabsClient(api_key)
```

#### Num Workers

The `num_workers` parameter is used to specify the number of workers to use for parallel requests. The default is None, which translates to `min(32, (os.cpu_count() or 1) + 4)`. See [docs](https://github.com/python/cpython/blob/dcc3eaef98cd94d6cb6cb0f44bd1c903d04f33b1/Lib/concurrent/futures/thread.py#L137) for more details. 

```python
client = ParclLabsClient(api_key, num_workers=20)
```

## Services <a id="services"></a>

### Search <a id="search"></a>

Search is your entry point into finding one or many of over 70,000 markets in the United States. You can search for markets by `name`, `state`, `region`, `fips`, or `zip code`. You can also search for markets by their unique `parcl_id`.

##### Search Markets
```python
# get top 2 metros by population
markets = client.search.markets.retrieve(
        location_type='CBSA',
        sort_by='TOTAL_POPULATION',
        sort_order='DESC',
        limit=2
)
# top 2 metros based on population. We will use these markets to query other services in the remainder of this readme
top_market_parcl_ids = markets['parcl_id'].tolist()
# parcl_id country  geoid state_fips_code                                   name state_abbreviation region location_type  total_population  median_income  parcl_exchange_market  pricefeed_market  case_shiller_10_market  case_shiller_20_market
#  2900187     USA  35620            None  New York-Newark-Jersey City, Ny-Nj-Pa               None   None          CBSA          19908595          93610                      0                 1                       1                       1
#  2900078     USA  31080            None     Los Angeles-Long Beach-Anaheim, Ca               None   None          CBSA          13111917          89105                      0                 1                       1                       1
```

### Rental Market Metrics <a id="rental-market-metrics"></a>

##### Gross Yield
Gets the percent gross yield for a specified `parcl_id`. At the market level, identified by `parcl_id`, gross yield is calculated by dividing the annual median rental income—derived from multiplying the monthly median new rental listing price by 12—by its median new listings for sale price.

##### Rental Units Concentration
Gets the number of rental units, total units, and percent rental unit concentration for a specified `parcl_id`.

##### New Listings for Rent Rolling Counts
Gets weekly updated rolling counts of newly listed for rent properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given `parcl_id`.

```python
start_date = '2024-04-01'
end_date = '2024-04-01'

results_rental_units_concentration = client.rental_market_metrics.rental_units_concentration.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_gross_yield = client.rental_market_metrics.gross_yield.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

rentals_new_listings_rolling_counts = client.rental_market_metrics.new_listings_for_rent_rolling_counts.retrieve(
        parcl_ids=top_market_parcl_ids
)
```

### For Sale Market Metrics <a id="for-sale-market-metrics"></a>

##### New Listings Rolling Counts
Gets weekly updated rolling counts of newly listed for sale properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given `parcl_id`.

##### For Sale Inventory
Gets the weekly updated current count of total inventory listed on market for sale, based on a specified `parcl_id` . The data series for the for sale inventory begins on September 1, 2022 (2022-09-01).

##### For Sale Inventory Price Changes
Gets weekly updated metrics on the price behavior of current for sale inventory, based on a specified `parcl_id`. Available metrics include the count of price changes, count of price drops, median days between price changes, median price change, and the percentage of inventory with price changes. The data series for the for sale inventory metrics begins on September 1, 2022 (2022-09-01).

```python
start_date = '2024-04-01'
end_date = '2024-04-01'
property_type = 'single_family'

results_for_sale_new_listings = client.for_sale_market_metrics.new_listings_rolling_counts.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    property_type=property_type
)

for_sale_inventory = client.for_sale_market_metrics.for_sale_inventory.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

for_sale_inventory_price_changes = client.for_sale_market_metrics.for_sale_inventory_price_changes.retrieve(
        parcl_ids=top_market_parcl_ids,
        start_date=start_date,
        end_date=end_date,
)
```

### Market Metrics <a id="market-metrics"></a>

##### Housing Event Counts
Gets monthly counts of housing events, including sales, new sale listings, and new rental listings, based on a specified `parcl_id`.

##### Housing Stock
Gets housing stock for a specified `parcl_id`. Housing stock represents the total number of properties, broken out by single family homes, townhouses, and condos.

##### Housing Event Prices
Gets monthly statistics on prices for housing events, including sales, new for-sale listings, and new rental listings, based on a specified `parcl_id`.

##### Housing Event Property Attributes
Gets monthly statistics on the physical attributes of properties involved in housing events, including sales, new for sale listings, and new rental listings, based on a specified `parcl_id`.

##### All Cash
Gets monthly counts of all cash transactions and their percentage share of total sales, based on a specified `parcl_id`.

```python
start_date = '2024-01-01'
end_date = '2024-04-01'

results_housing_event_prices = client.market_metrics.housing_event_prices.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_housing_stock = client.market_metrics.housing_stock.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_housing_event_counts = client.market_metrics.housing_event_counts.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

housing_event_property_attributes = client.market_metrics.housing_event_property_attributes.retrieve(
        parcl_ids=top_market_parcl_ids,
        start_date=start_date,
        end_date=end_date
)

results_all_cash = client.market_metrics.all_cash.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)
```

### New Construction Metrics <a id="new-construction-metrics"></a>

##### Housing Event Counts
Gets monthly counts of new construction housing events, including sales, new for sale listings, and new rental listings, based on a specified `parcl_id`.

##### Housing Event Prices
Gets monthly median prices for new construction housing events, including sales, new for sale listings, and new rental listings, based on a specified `parcl_id`.

```python
start_date = '2024-01-01'
end_date = '2024-04-01'

results_new_construction_housing_event_prices = client.new_construction_metrics.housing_event_prices.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_new_construction_housing_event_counts = client.new_construction_metrics.housing_event_counts.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)
```

### Investor Metrics <a id="investor-metrics"></a>

##### Housing Event Counts
Gets monthly counts of investor housing events, including acquisitions, dispositions, new sale listings, and new rental listings, based on a specified `parcl_id`.

##### Purchase to Sale Ratio
Gets the monthly investor purchase to sale ratio for a specified `parcl_id`.

##### New Listings for Sale Rolling Counts
Gets weekly updated rolling counts of investor-owned properties newly listed for sale, and their corresponding percentage share of the total for-sale listings market. These metrics are segmented into 7, 30, 60, and 90-day periods ending on a specified date, based on a given `parcl_id`

##### Housing Stock Ownership
Gets counts of investor-owned properties and their corresponding percentage ownership share of the total housing stock, for a specified `parcl_id`.

##### Housing Event Prices
Gets monthly median prices for investor housing events, including acquisitions, dispositions, new sale listings, and new rental listings, based on a specified `parcl_id`.

```python
start_date = '2024-01-01'
end_date = '2024-04-01'

results_housing_stock_ownership = client.investor_metrics.housing_stock_ownership.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_new_listings_for_sale_rolling_counts = client.investor_metrics.new_listings_for_sale_rolling_counts.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_purchase_to_sale_ratio = client.investor_metrics.purchase_to_sale_ratio.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_housing_event_counts = client.investor_metrics.housing_event_counts.retrieve(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results = client.investor_metrics.housing_event_prices.retrieve(
        parcl_ids=top_market_parcl_ids,
        start_date=start_date,
        end_date=end_date,
)
```

### Portfolio Metrics <a id="portfolio-metrics"></a>

##### Single Family Housing Event Counts
Gets monthly counts of investor-owned single family property housing events, segmented by portfolio size, for a specified `parcl_id`. Housing events include acquisitions, dispositions, new for sale listings, and new rental listings.

##### Single Family Housing Stock Ownership
Gets counts of investor-owned single family properties and their corresponding percentage of the total single family housing stock, segmented by portfolio size, for a specified `parcl_id`. The data series for portfolio metrics begins on March 1, 2024 (2024-03-01).

##### New Listings for Sale Rolling Counts
Gets counts of investor-owned single family properties and their corresponding percentage of the total single family housing stock, segmented by portfolio size, for a specified `parcl_id`. The data series for portfolio metrics begins on April 15, 2024 (2024-04-15).

##### New Listings for Rent Rolling Counts
Gets weekly updated rolling counts of investor-owned single family properties newly listed for rent, segmented by portfolio size, and their corresponding percentage share of the total single family for rent listings market. These metrics are divided into 7, 30, 60, and 90 day periods ending on a specified date, based on a given `parcl_id`. The data series for portfolio metrics begins on April 22, 2024 (2024-04-22).

```python
results_housing_stock_ownership = client.portfolio_metrics.sf_housing_stock_ownership.retrieve(
    parcl_ids=top_market_parcl_ids,
)

# get new listings for specific portfolio sizes
portfolio_metrics_new_listings = client.portfolio_metrics.sf_new_listings_for_sale_rolling_counts.retrieve(
        parcl_ids=top_market_parcl_ids,
        portfolio_size='PORTFOLIO_1000_PLUS',
)

results = client.portfolio_metrics.sf_housing_event_counts.retrieve(
    parcl_ids=top_market_parcl_ids,
    portfolio_size='PORTFOLIO_1000_PLUS'
)

results = client.portfolio_metrics.sf_new_listings_for_rent_rolling_counts.retrieve(
        parcl_ids=top_market_parcl_ids,
        portfolio_size='PORTFOLIO_1000_PLUS'
)
```

### Price Feeds <a id="price-feeds"></a>

The Parcl Labs Price Feed (PLPF) is a daily-updated, real-time indicator of residential real estate prices, measured by price per square foot, across select US markets.

The Price Feeds category allows you to access our daily-updated PLPF and derivative metrics, such as volatility.

##### Price Feed
Gets the daily price feed for a specified `parcl_id`.

##### Price Feed Volatility
Gets the daily price feed volatility for a specified `parcl_id`.

##### Rental Price Feed
Gets the daily updated Parcl Labs Rental Price Feed for a given `parcl_id`.

```python
# get 2 price feeds trading on the Parcl Exchange
pricefeed_markets = client.search.markets.retrieve(
        sort_by='PARCL_EXCHANGE_MARKET', # use PRICEFEED_MARKET for all price feed markets
        sort_order='DESC',
        limit=2
)
# top 2 metros based on population. We will use these markets to query other services in the remainder of this readme
pricefeed_ids = pricefeed_markets['parcl_id'].tolist()
start_date = '2024-06-01'
end_date = '2024-06-05'

price_feeds = client.price_feed.price_feed.retrieve(
    parcl_ids=pricefeed_ids,
    start_date=start_date,
    end_date=end_date
)
rental_price_feeds = client.price_feed.rental_price_feed.retrieve(
    parcl_ids=pricefeed_ids,
    start_date=start_date,
    end_date=end_date
)
price_feed_volatility = client.price_feed.volatility.retrieve(
    parcl_ids=pricefeed_ids,
    start_date=start_date,
    end_date=end_date
)
```

### Property <a id="property"></a>

##### Property Search Markets
Gets a list of unique identifiers (parcl_property_id) for units that correspond to specific markets or parameters defined by the user. The parcl_property_id is key to navigating the Parcl Labs API, serving as the core mechanism for retrieving unit-level information.

```python
# search by operators
invitation_homes_tampa_units = client.property.search.retrieve(
    parcl_ids=[2900417],
    property_type='single_family',
    # square_footage_min=1000,
    # square_footage_max=2500,
    # bedrooms_min=2,
    # bedrooms_max=5,
    # bathrooms_min=2,
    # bathrooms_max=3,
    # year_built_min=2010,
    # year_built_max=2023,
    current_entity_owner_name='invitation_homes',
    # event_history_sale_flag=True,
    # event_history_rental_flag=True,
    # event_history_listing_flag=True,
    # current_new_oncstruciton_flag=True,
    # current_owner_occupied_flag=True,
    # current_investor_owned_flag=True,
)

# search by buy box - only look at units that have rented
# and review rental rates
rental_buy_box = client.property.search.retrieve(
    parcl_ids=[2900417],
    property_type='single_family',
    square_footage_min=1000,
    square_footage_max=2500,
    bedrooms_min=2,
    bedrooms_max=5,
    # bathrooms_min=2,
    # bathrooms_max=3,
    year_built_min=2010,
    year_built_max=2023,
    # current_entity_owner_name='invitation_homes',
    # event_history_sale_flag=True,
    event_history_rental_flag=True,
    # event_history_listing_flag=True,
    # current_new_oncstruciton_flag=True,
    # current_owner_occupied_flag=True,
    # current_investor_owned_flag=True,
)

# to extract parcl_property_id's to retrieve expanded history for 
# any of these queries, use: 
parcl_property_id_list = rental_buy_box['parcl_property_id'].tolist()
```

##### Property Event History
Gets unit-level properties and their housing event history, including sales, listings, and rentals. The response includes detailed property information and historical event data for each specified property.

```python
sale_events = client.property.events.retrieve(
        parcl_property_ids=parcl_property_id_list[0:10],
        event_type='SALE',
        start_date='2020-01-01',
        end_date='2024-06-30'
)

rental_events = client.property.events.retrieve(
        parcl_property_ids=parcl_property_id_list[0:10],
        event_type='RENTAL',
        start_date='2020-01-01',
        end_date='2024-06-30'
)
```

### Property Address Search <a id="property-address-search"></a>

Pass in a list of addresses -- `address, unit, city, state_abbreviation, zip_code, source_id` -- and receive the associated `parcl_property_id`, if there is a match. `unit` and `source_id` are optional fields.

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

### Property Search V2 <a id="property-search-v2"></a>

Gets a list of unique properties and their associated metadata and events based on a set of property, event, and owner filters. Use one of three search methods:
1. `parcl_ids`
2. `parcl_property_ids`
3. `geo_coordinates` (must provide latitude, longitude, and radius)

Use limit to specify the number of matched properties to return. Set auto_paginate to `True` to retrieve all results, this will override the limit.

```python
results, filter_data = client.property_v2.search.retrieve(
    # parcl_ids=[5495449],
    parcl_property_ids=[78353317, 135921544],
    # geo_coordinates= {"latitude": 36.159445, "longitude": -86.483244, radius: 1},
    event_names=["LISTED_RENT"],
    is_new_construction=False,
    max_event_date="2024-12-31",
    min_event_date="2023-01-01",
    max_price=3000,
    min_price=100,
    is_investor_owned=True,
    is_owner_occupied=False,
    owner_name=["BLACKSTONE"],
    include_property_details=True,
    max_beds=5,
    min_beds=1,
    max_year_built=2020,
    min_year_built=1998,
    min_baths=1,
    min_sqft=500,
    max_record_added_date="2024-12-31",
    min_record_added_date="2024-12-13",
    property_types=["SINGLE_FAMILY", "CONDO", "TOWNHOUSE"],
    limit=10,
    # auto_paginate=True,
)
```

### Account Info <a id="account-info"></a>

Monitor your API usage and quota limits by calling the `account()` method in the `ParclLabsClient` class.
```python
client = ParclLabsClient(api_key)
account_info = client.account()
```