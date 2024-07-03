<!-- readme header split -->
![Logo](img/labs.png)
![GitHub Tag](https://img.shields.io/github/v/tag/ParclLabs/parcllabs-python)
![PyPI - Downloads](https://img.shields.io/pypi/dm/parcllabs)
<!-- readme header end -->
## **Welcome to the Parcl Labs Python SDK**

**We're on a mission to create the world's best API developer experience and community for housing data.**

Our SDK is designed to supercharge your API experience and accelerate your time to insight. It enables you to efficiently pull the data you need, analyze it, and visualize your findings.

<!-- readme header split -->
## Parcl Labs Data Overview

The Parcl Labs API provides **instant insights into the U.S. housing market**, delivering data on housing supply, sales, listings, rentals, investor activities, and market trends.



_The most complete picture of US residential real estate_

| Category           | Coverage                                                                                                                                                      |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Property Types** | **🏘️ All Residential Assets:**<br>✅ Single Family<br>✅ Townhouses<br>✅ Condos<br>✅ Other                                                                      |
| **Markets**        | **🇺🇸 Complete National Coverage, 70k+ Unique Markets at Any Level of Granularity:**<br>✅ Regions<br>✅ States<br>✅ Metros<br>✅ Cities<br>✅ Counties<br>✅ Towns<br>✅ Zips<br>✅ Census Places |
| **Housing Events** | **🔄 The Full Property Lifecycle:**<br>✅ Sales<br>✅ For Sale Listings<br>✅ Rentals                                                                              |

<!-- readme header end -->
### Cookbook

We maintain a repository of examples that demonstrate how to use the Parcl Labs API for analysis. You can find the examples in the [Parcl Labs Cookbook](https://github.com/parcllabs/parcllabs-cookbook)

## Getting Started

### Step 1. Sign Up for an API Key

To use the Parcl Labs API, you need an API key. To get an API key, sign up at [ParclLabs](https://dashboard.parcllabs.com/signup). In the subsequent examples, the API key is stored in the `PARCLLABS_API_KEY` environment variable.



### Step 2. Installation

You can install the package via pip:

```bash
pip install parcllabs
```


### Step 3. Usage

The `ParclLabsClient` class is the entry point to the Parcl Labs API. You can use the client to access methods that allow you to retrieve and analyze data from the Parcl Labs API. You'll need to pass in your API key when you create an instance of the `ParclLabsClient` class.

```python
import os

from parcllabs import ParclLabsClient


api_key = os.getenv('PARCL_LABS_API_KEY')
client = ParclLabsClient(api_key)
```

#### Search
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

#### Services

Services are the core of the Parcl Labs API. They provide access to a wide range of data and analytics on the housing market. The services are divided into the following categories: `Price Feeds`, `Rental Market Metrics`, `For Sale Market Metrics`, `Market Metrics`, `Investor Metrics`, and `Portfolio Metrics`.

#### Price Feeds
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

# want to save to csv? Use .to_csv method as follow:
# price_feeds.to_csv('price_feeds.csv', index=False)
# rental_price_feeds.to_csv('rental_price_feeds.csv', index=False)
# price_feed_volatility.to_csv('price_feed_volatility.csv', index=False)
```

#### Rental Market Metrics

##### Gross Yield
Gets the percent gross yield for a specified `parcl_id`. At the market level, identified by `parcl_id`, gross yield is calculated by dividing the annual median rental income—derived from multiplying the monthly median new rental listing price by 12—by its median new listings for sale price.

##### Rental Units Concentration
Gets the number of rental units, total units, and percent rental unit concentration for a specified `parcl_id`.

##### New Listings for Rent Rolling Counts
Gets weekly updated rolling counts of newly listed for rent properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given `parcl_id`.

###### Get all rental market metrics
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

#### For Sale Market Metrics

##### New Listings Rolling Counts
Gets weekly updated rolling counts of newly listed for sale properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given `parcl_id`.

##### For Sale Inventory
Gets the weekly updated current count of total inventory listed on market for sale, based on a specified `parcl_id` . The data series for the for sale inventory begins on September 1, 2022 (2022-09-01).

##### For Sale Inventory Price Changes
Gets weekly updated metrics on the price behavior of current for sale inventory, based on a specified `parcl_id`. Available metrics include the count of price changes, count of price drops, median days between price changes, median price change, and the percentage of inventory with price changes. The data series for the for sale inventory metrics begins on September 1, 2022 (2022-09-01).

###### Get all for sale market metrics
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

#### Market Metrics

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


###### Get all market metrics
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

#### New Construction Metrics

##### Housing Event Counts
Gets monthly counts of new construction housing events, including sales, new for sale listings, and new rental listings, based on a specified `parcl_id`.

##### Housing Event Prices
Gets monthly median prices for new construction housing events, including sales, new for sale listings, and new rental listings, based on a specified `parcl_id`.

###### Get all new construction metrics
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

#### Investor Metrics

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

###### Get all investor metrics
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

#### Portfolio Metrics

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

#### Property

##### Property Search Markets
Gets a list of unique identifiers (parcl_property_id) for units that correspond to specific markets or parameters defined by the user. The parcl_property_id is key to navigating the Parcl Labs API, serving as the core mechanism for retrieving unit-level information.
```python
# get all condos over 3000 sq ft in the 10001 zip code area
units = client.property.search.retrieve(
        zip=10001,
        sq_ft_min=3000,
        property_type='condo',
)
# to use these ids in event history
parcl_property_id_list = units['parcl_property_id'].tolist()
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

##### Utility Functions
Want to keep track of the estimated number of credits you are using in a given session? 

```python
# get the number of credits used in a given session
credits_used = client.estimated_session_credit_usage
print(f"Estimated session credit usage: {credits_used}")
```