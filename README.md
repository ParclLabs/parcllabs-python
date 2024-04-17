# parcllabs-python
Parcl Labs Python SDK




### Investor Metrics

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

results_housing_stock_ownership = housing_stock_ownership.get_investor_metrics(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_new_listings_for_sale_rolling_counts = new_listings_for_sale_rolling_counts.get_investor_metrics(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_purchase_to_sale_ratio = purchase_to_sale_ratio.get_investor_metrics(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)

results_housing_event_counts = housing_event_counts.get_investor_metrics(
    parcl_ids=top_market_parcl_ids,
    start_date=start_date,
    end_date=end_date
)
```
