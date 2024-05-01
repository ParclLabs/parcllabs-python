![Logo](img/labs.jpg)
# parcllabs-python

## Sign Up for an API Key

To use the Parcl Labs API, you need an API key. To get an API key, sign up at [ParclLabs](https://dashboard.parcllabs.com/signup).

## Examples

We maintain a repository of examples that demonstrate how to use the Parcl Labs API for analysis. You can find the examples in the [ParclLabs Examples](https://github.com/parcllabs/parcllabs-examples)

## Installation

You can install the package via pip:

```bash
pip install parcllabs
```

### Search
Search is your entry point into finding one or many of over 70,000 markets in the United States. You can search for markets by name, state, region, fips, or zip code. You can also search for markets by their Parcl ID.

#### Search Markets
```python
import os

from parcllabs import ParclLabsClient


api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

# all cities in EAST_NORTH_CENTRAL census region
results = client.search.markets.retrieve_many(
    location_type='CITY',
    region='EAST_NORTH_CENTRAL',
    as_dataframe=True
)
print(results.head())
#     parcl_id country    geoid state_fips_code             name state_abbreviation              region location_type
# 0    5333443     USA  5500100              55  Abbotsford City                 WI  EAST_NORTH_CENTRAL          CITY
# 1    5387920     USA  1700113              17    Abingdon City                 IL  EAST_NORTH_CENTRAL          CITY
# 2    5403368     USA  5500275              55       Adams City                 WI  EAST_NORTH_CENTRAL          CITY
# 3    5278505     USA  2600440              26      Adrian City                 MI  EAST_NORTH_CENTRAL          CITY
# 4    5332624     USA  3901000              39       Akron City                 OH  EAST_NORTH_CENTRAL          CITY
```

### Rental Market Metrics

#### Gross Yield
Gets the percent gross yield for a specified <parcl_id>. At the market level, identified by <parcl_id>, gross yield is calculated by dividing the annual median rental income—derived from multiplying the monthly median new rental listing price by 12—by its median new listings for sale price.

#### Rental Units Concentration
Gets the number of rental units, total units, and percent rental unit concentration for a specified <parcl_id>.

##### Get all rental market metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import ParclLabsClient

api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()


start_date = '2020-01-01'
end_date = '2024-04-01'

results_rental_units_concentration = client.rental_market_metrics_rental_units_concentration.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)

results_gross_yield = client.rental_market_metrics_gross_yield.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)
```

### For Sale Market Metrics

#### New Listings Rolling Counts
Gets weekly updated rolling counts of newly listed for sale properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given <parcl_id>.


##### Get all for sale market metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import ParclLabsClient

api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()

start_date = '2020-01-01'
end_date = '2024-04-01'
property_type = 'single_family'

results_for_sale_new_listings = client.for_sale_market_metrics_new_listings_rolling_counts.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    property_type=property_type,
    as_dataframe=True
)
```

### Market Metrics

#### Housing Event Counts
Gets monthly counts of housing events, including sales, new sale listings, and new rental listings, based on a specified <parcl_id>.

#### Housing Stock
Gets housing stock for a specified <parcl_id>. Housing stock represents the total number of properties, broken out by single family homes, townhouses, and condos.

#### Housing Event Prices
Gets monthly statistics on prices for housing events, including sales, new for-sale listings, and new rental listings, based on a specified <parcl_id>.


##### Get all market metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import ParclLabsClient


api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()

start_date = '2020-01-01'
end_date = '2024-04-01'

results_housing_event_prices = client.market_metrics_housing_event_prices.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)

results_housing_stock = client.market_metrics_housing_stock.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)

results_housing_event_counts = client.market_metrics_housing_event_counts.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)
```

### Investor Metrics

#### Housing Event Counts
Gets monthly counts of investor housing events, including acquisitions, dispositions, new sale listings, and new rental listings, based on a specified <parcl_id>.

#### Purchase to Sale Ratio
Gets the monthly investor purchase to sale ratio for a specified <parcl_id>.

#### New Listings for Sale Rolling Counts
Gets weekly updated rolling counts of investor-owned properties newly listed for sale, and their corresponding percentage share of the total for-sale listings market. These metrics are segmented into 7, 30, 60, and 90-day periods ending on a specified date, based on a given <parcl_id>

#### Housing Stock Ownership
Gets counts of investor-owned properties and their corresponding percentage ownership share of the total housing stock, for a specified <parcl_id>.

##### Get all investor metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import ParclLabsClient


api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()

start_date = '2020-01-01'
end_date = '2024-04-01'

results_housing_stock_ownership = client.investor_metrics_housing_stock_ownership.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)

results_new_listings_for_sale_rolling_counts = client.investor_metrics_new_listings_for_sale_rolling_counts.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)

results_purchase_to_sale_ratio = client.investor_metrics_purchase_to_sale_ratio.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)

results_housing_event_counts = client.investor_metrics_housing_event_counts.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    as_dataframe=True
)
```

#### Portfolio Metrics
Gets counts of investor-owned single family properties and their corresponding percentage of the total single family housing stock, segmented by portfolio size, for a specified <parcl_id>. The data series for portfolio metrics begins on March 1, 2024 (2024-03-01).

##### Single Family Home Portfolio Metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import ParclLabsClient


api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()

results_housing_stock_ownership = client.portfolio_metrics_sf_housing_stock_ownership.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    as_dataframe=True
)
```
