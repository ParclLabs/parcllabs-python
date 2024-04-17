# parcllabs-python
Parcl Labs Python SDK

## For Sale Market Metrics

### New Listings Rolling Counts
Gets weekly updated rolling counts of newly listed for sale properties, segmented into 7, 30, 60, and 90 day periods ending on a specified date, based on a given <parcl_id>.


#### Get all for sale market metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import (
    ParclLabsClient,
    ForSaleMarketMetricsNewListingsRollingCounts
)

api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()

for_sale_new_listings = ForSaleMarketMetricsNewListingsRollingCounts(client)

start_date = '2020-01-01'
end_date = '2024-04-01'
property_type = 'single_family'

results_for_sale_new_listings = for_sale_new_listings.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date,
    property_type=property_type
)
```

## Market Metrics

### Housing Event Counts
Gets monthly counts of housing events, including sales, new sale listings, and new rental listings, based on a specified <parcl_id>.

### Housing Stock
Gets housing stock for a specified <parcl_id>. Housing stock represents the total number of properties, broken out by single family homes, townhouses, and condos.

### Housing Event Prices
Gets monthly statistics on prices for housing events, including sales, new for-sale listings, and new rental listings, based on a specified <parcl_id>.


#### Get all market metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import (
    ParclLabsClient,
    MarketMetricsHousingEventPrices,
    MarketMetricsHousingStock,
    MarketMetricsHousingEventCounts
)

api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()

housing_event_prices = MarketMetricsHousingEventPrices(client)
housing_stock = MarketMetricsHousingStock(client)
housing_event_counts = MarketMetricsHousingEventCounts(client)

start_date = '2020-01-01'
end_date = '2024-04-01'

results_housing_event_prices = housing_event_prices.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_housing_stock = housing_stock.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_housing_event_counts = housing_event_counts.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)
```

## Investor Metrics

### Housing Event Counts
Gets monthly counts of investor housing events, including acquisitions, dispositions, new sale listings, and new rental listings, based on a specified <parcl_id>.

### Purchase to Sale Ratio
Gets the monthly investor purchase to sale ratio for a specified <parcl_id>.

### New Listings for Sale Rolling Counts
Gets weekly updated rolling counts of investor-owned properties newly listed for sale, and their corresponding percentage share of the total for-sale listings market. These metrics are segmented into 7, 30, 60, and 90-day periods ending on a specified date, based on a given <parcl_id>

### Housing Stock Ownership
Gets counts of investor-owned properties and their corresponding percentage ownership share of the total housing stock, for a specified <parcl_id>.

#### Get all investor metrics
```python
import os

from parcllabs.search.top_markets import get_top_n_metros
from parcllabs import (
    ParclLabsClient,
    InvestorMetricsHousingStockOwnership, 
    InvesetorMetricsNewListingsForSaleRollingCounts,
    InvestorMetricsPurchaseToSaleRatio,
    InvestorMetricsHousingEventCounts
)

api_key = os.getenv('PARCLLABS_API_KEY')
client = ParclLabsClient(api_key)

top_markets = get_top_n_metros(n=10)
top_market_parcl_ids = top_markets['parcl_id'].tolist()

housing_stock_ownership = InvestorMetricsHousingStockOwnership(client)
new_listings_for_sale_rolling_counts = InvesetorMetricsNewListingsForSaleRollingCounts(client)
purchase_to_sale_ratio = InvestorMetricsPurchaseToSaleRatio(client)
housing_event_counts = InvestorMetricsHousingEventCounts(client)

start_date = '2020-01-01'
end_date = '2024-04-01'

results_housing_stock_ownership = housing_stock_ownership.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_new_listings_for_sale_rolling_counts = new_listings_for_sale_rolling_counts.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_purchase_to_sale_ratio = purchase_to_sale_ratio.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_housing_event_counts = housing_event_counts.retrieve_many(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)
```
