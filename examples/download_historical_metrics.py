import os
import parcllabs
from parcllabs import ParclLabsClient

api_key = os.getenv("PARCL_LABS_API_KEY")
client = ParclLabsClient(api_key=api_key)

def main():
    # Search for markets
    markets = client.search.markets.retrieve(
        query="United States",
        sort_by="TOTAL_POPULATION",
        sort_order="DESC",
        limit=1,
    )

    PID = markets["parcl_id"].values[0]

    start_date = "2023-01-01"
    end_date = "2024-08-01"

    # Retrieve various metrics
    metrics = {
        "housing_event_counts": client.market_metrics.housing_event_counts,
        "all_cash": client.market_metrics.all_cash,
        "investor_housing_stock_ownership": client.investor_metrics.housing_stock_ownership,
        "investor_housing_event_counts": client.investor_metrics.housing_event_counts
    }

    # Retrieve data and save to CSV
    for metric_name, metric_client in metrics.items():
        results = metric_client.retrieve(
            parcl_ids=[PID],
            start_date=start_date,
            end_date=end_date,
            limit=100 if metric_name == "housing_event_counts" else None
        )
        results.to_csv(f"us_{metric_name}.csv", index=False)

if __name__ == "__main__":
    main()