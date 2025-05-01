import argparse
import json
import logging
import os
import time
from pathlib import Path

import parcllabs
from parcllabs import ParclLabsClient
from parcllabs.services.parcllabs_service import ParclLabsService

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("API_Latency_Profiler")

# Default values
ENV = "prod"
DEFAULT_NUMBER_OF_PROPERTIES = 10000
DEFAULT_EXPERIMENT_NAME = "test_with_processing"
DEFAULT_OUTPUT_FILE = "api_latency_properties_test.json"


def profile_api_call(
    section_name: str, client_method: ParclLabsService, output_file: str, **kwargs: dict
) -> None:
    """
    Profiles an API call, logs the results, and saves to a file.

    Args:
        section_name (str): The name of the section being profiled.
        client_method: The client method to be called.
        output_file (str): The file where the results should be saved.
        **kwargs: Arguments to pass to the client method.
    """
    start_time = time.time()
    result = client_method.retrieve(**kwargs)
    elapsed_time = time.time() - start_time

    if result is not None:
        # Serialize the DataFrame to a JSON string and calculate its size
        result = result.reset_index(drop=True)  # Ensure the index is unique
        result_json = result.to_json(orient="records")
        result_size_mb = len(result_json.encode("utf-8")) / (1024**2)
        result_size_kb = len(result_json.encode("utf-8")) / 1024
    else:
        result_size_mb = 0
        result_size_kb = 0

    url = client_method.url if hasattr(client_method, "url") else "Unknown URL"
    logger.info(f"{section_name} - Time taken: {elapsed_time:.4f} seconds")
    logger.info(
        f"{section_name} - Response size: {result_size_mb:.4f} MB "
        f"({result_size_kb:.2f} KB)"
    )
    logger.info(f"{section_name} - API Endpoint: {url}")

    # Save the results to a file
    if result is not None:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_time))
        record = {
            "sdk_version": parcllabs.__version__,
            "experiment": kwargs.get("experiment", DEFAULT_EXPERIMENT_NAME),
            "timestamp": timestamp,
            "section": section_name,
            "duration_seconds": elapsed_time,
            "response_size_mb": result_size_mb,
            "response_size_kb": result_size_kb,
            "url": url,
        }
        with Path(output_file).open("a") as f:
            f.write(json.dumps(record) + "\n")

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile ParclLabs API calls.")

    parser.add_argument(
        "--number_of_properties",
        type=int,
        default=DEFAULT_NUMBER_OF_PROPERTIES,
        help="Number of properties to retrieve.",
    )
    parser.add_argument(
        "--experiment_name",
        type=str,
        default=DEFAULT_EXPERIMENT_NAME,
        help="Name of the experiment.",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default=DEFAULT_OUTPUT_FILE,
        help="File to save the API call results.",
    )
    parser.add_argument(
        "--env", type=str, default=ENV, help="Environment to use for the API calls"
    )

    parser.add_argument(
        "--num_workers", type=int, default=10, help="Number of workers to use"
    )

    args = parser.parse_args()

    api_key = os.getenv("PARCL_LABS_API_KEY")

    if args.env == "dev":
        api_key = os.getenv("PARCL_LABS_DEV_API_KEY")
        client = ParclLabsClient(
            api_key=api_key,
            api_url=os.getenv("PARCL_LABS_DEV_API_URL"),
            limit=100,
            num_workers=args.num_workers,
        )
    else:
        client = ParclLabsClient(
            api_key=api_key, limit=100, num_workers=args.num_workers
        )

    logger.info(f"Parcl Labs Client Version: {parcllabs.__version__}")

    # Main profiling
    output_file = args.output_file

    markets = profile_api_call(
        "Retrieve Top 2 Metros by Population",
        client.search.markets,
        output_file,
        location_type="CBSA",
        sort_by="TOTAL_POPULATION",
        sort_order="DESC",
        limit=10,
    )
    markets["parcl_id"].tolist()

    profile_api_call(
        "Search by Property Type",
        client.property.search,
        output_file,
        parcl_ids=[2900128],
        property_type="single_family",
        # current_entity_owner_name="invitation_homes",
    )

    profile_api_call(
        "Search by Operators",
        client.property.search,
        output_file,
        parcl_ids=[2900128],
        property_type="single_family",
        current_entity_owner_name="invitation_homes",
    )

    rental_buy_box = profile_api_call(
        "Search by Buy Box",
        client.property.search,
        output_file,
        parcl_ids=[2900128],
        property_type="single_family",
        # square_footage_min=1000,
        # square_footage_max=2500,
        year_built_max=2023,
        year_built_min=2000,
    )
    parcl_property_id_list = rental_buy_box["parcl_property_id"].tolist()
    num_units = min(args.number_of_properties, len(parcl_property_id_list))
    parcl_property_id_list = parcl_property_id_list[:num_units]

    print(f"Total number of parcl property ids: {len(parcl_property_id_list)}")

    profile_api_call(
        "Retrieve Sale Events",
        client.property.events,
        output_file,
        parcl_property_ids=parcl_property_id_list,
        event_type="SALE",
        start_date="2020-01-01",
        end_date="2024-06-30",
    )

    profile_api_call(
        "Retrieve Rental Events",
        client.property.events,
        output_file,
        parcl_property_ids=parcl_property_id_list,
        event_type="RENTAL",
        start_date="2020-01-01",
        end_date="2024-06-30",
    )

    profile_api_call(
        "Retrieve Rental History",
        client.property.events,
        output_file,
        parcl_property_ids=parcl_property_id_list,
    )

    # pricefeed_markets = profile_api_call(
    #     "Retrieve Top 2 Price Feed Markets",
    #     client.search.markets,
    #     output_file,
    #     sort_by="PARCL_EXCHANGE_MARKET",
    #     sort_order="DESC",
    #     limit=2,
    # )
    # pricefeed_ids = pricefeed_markets["parcl_id"].tolist()

    # start_date = "2024-06-01"
    # end_date = "2024-06-05"

    # profile_api_call(
    #     "Retrieve Price Feeds",
    #     client.price_feed.price_feed,
    #     output_file,
    #     parcl_ids=pricefeed_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Rental Price Feeds",
    #     client.price_feed.rental_price_feed,
    #     output_file,
    #     parcl_ids=pricefeed_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Price Feed Volatility",
    #     client.price_feed.volatility,
    #     output_file,
    #     parcl_ids=pricefeed_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # start_date = "2024-04-01"
    # end_date = "2024-04-01"

    # profile_api_call(
    #     "Retrieve Rental Units Concentration",
    #     client.rental_market_metrics.rental_units_concentration,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Gross Yield",
    #     client.rental_market_metrics.gross_yield,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve New Listings for Rent Rolling Counts",
    #     client.rental_market_metrics.new_listings_for_rent_rolling_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    # )

    # profile_api_call(
    #     "Retrieve New Listings for Sale Rolling Counts",
    #     client.for_sale_market_metrics.new_listings_rolling_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    #     property_type="single_family",
    # )

    # profile_api_call(
    #     "Retrieve For Sale Inventory",
    #     client.for_sale_market_metrics.for_sale_inventory,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve For Sale Inventory Price Changes",
    #     client.for_sale_market_metrics.for_sale_inventory_price_changes,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Housing Event Prices",
    #     client.market_metrics.housing_event_prices,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Housing Stock",
    #     client.market_metrics.housing_stock,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Housing Event Counts",
    #     client.market_metrics.housing_event_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Housing Event Property Attributes",
    #     client.market_metrics.housing_event_property_attributes,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve All Cash Transactions",
    #     client.market_metrics.all_cash,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve New Construction Housing Event Prices",
    #     client.new_construction_metrics.housing_event_prices,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve New Construction Housing Event Counts",
    #     client.new_construction_metrics.housing_event_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Housing Stock Ownership",
    #     client.investor_metrics.housing_stock_ownership,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve New Listings for Sale Rolling Counts by Portfolio Size",
    #     client.investor_metrics.new_listings_for_sale_rolling_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Purchase to Sale Ratio",
    #     client.investor_metrics.purchase_to_sale_ratio,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     start_date=start_date,
    #     end_date=end_date,
    # )

    # profile_api_call(
    #     "Retrieve Portfolio Metrics - Housing Stock Ownership",
    #     client.portfolio_metrics.sf_housing_stock_ownership,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    # )

    # profile_api_call(
    #     "Retrieve Portfolio Metrics - New Listings for Sale",
    #     client.portfolio_metrics.sf_new_listings_for_sale_rolling_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     portfolio_size="PORTFOLIO_1000_PLUS",
    # )

    # profile_api_call(
    #     "Retrieve Portfolio Metrics - Housing Event Counts",
    #     client.portfolio_metrics.sf_housing_event_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     portfolio_size="PORTFOLIO_1000_PLUS",
    # )

    # profile_api_call(
    #     "Retrieve Portfolio Metrics - New Listings for Rent",
    #     client.portfolio_metrics.sf_new_listings_for_rent_rolling_counts,
    #     output_file,
    #     parcl_ids=top_market_parcl_ids,
    #     portfolio_size="PORTFOLIO_1000_PLUS",
    # )

    # Logging estimated credits used
    credits_used = client.estimated_session_credit_usage
    logger.info(f"Estimated session credit usage: {credits_used}")


if __name__ == "__main__":
    main()
