import os

from parcllabs import ParclLabsClient


if __name__ == "__main__":
    api_key = os.getenv("PARCL_LABS_PROD_API_KEY")
    client = ParclLabsClient(api_key=api_key)

    # metros = client.search.markets.retrieve(
    # 	location_type="CBSA",
    # 	sort_by="TOTAL_POPULATION",
    # 	sort_order="DESC",
    # 	limit=1,
    # )

    events = client.property.search.retrieve(
        parcl_ids=[5332726], property_type="SINGLE_FAMILY", year_built_max=2010
    )

    # print(metros)
    print(events)
