from typing import Optional
from parcllabs.__version__ import VERSION as __version__

# Constants
DEFAULT_API_BASE = "https://api.parcllabs.com"

api_key: Optional[str] = None
api_base = DEFAULT_API_BASE


from parcllabs.plabs_client import ParclLabsClient


from parcllabs.services.investor_metrics import (
    InvestorMetricsHousingStockOwnership,
    InvesetorMetricsNewListingsForSaleRollingCounts,
    InvestorMetricsPurchaseToSaleRatio,
    InvestorMetricsHousingEventCounts,
)

from parcllabs.services.market_metrics import (
    MarketMetricsHousingEventPrices,
    MarketMetricsHousingStock,
    MarketMetricsHousingEventCounts,
)

from parcllabs.services.for_sale_market_metrics import (
    ForSaleMarketMetricsNewListingsRollingCounts,
)

from parcllabs.services.rental_market_metrics import (
    RentalMarketMetricsRentalUnitsConcentration,
    RentalMarketMetricsGrossYield,
)

from parcllabs.services.portfolio_metrics import PortfolioMetricsSFHousingStockOwnership
