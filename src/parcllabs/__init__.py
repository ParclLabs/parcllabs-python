from typing import Optional

# Constants
DEFAULT_API_BASE = 'https://api.parcllabs.com'

api_key: Optional[str] = None
api_base = DEFAULT_API_BASE


from parcllabs.services.investor_metrics import (
    InvestorMetricsHousingStockOwnership, 
    InvesetorMetricsNewListingsForSaleRollingCounts,
    InvestorMetricsPurchaseToSaleRatio,
    InvestorMetricsHousingEventCounts
)