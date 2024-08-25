import os

import parcllabs
from parcllabs import ParclLabsClient

# parcllabs.__version__ >= '1.4.0'

api_key = os.getenv("PARCL_LABS_API_KEY")

client = ParclLabsClient(api_key=api_key)


def main():

    # search
    markets = client.search.markets.retrieve(
        query="Davidson County",
        location_type="COUNTY",
        sort_by="TOTAL_POPULATION",
        state_abbreviation="TN",
        sort_order="DESC",
        limit=1,
    )

    print(markets)

    DAVIDSON_COUNTY_PID = markets["parcl_id"].values[0]

    homes = client.property.search.retrieve(
        parcl_ids=[DAVIDSON_COUNTY_PID],
        property_type="SINGLE_FAMILY",
        # square_footage_min=1000,
        # square_footage_max=2500,
        # bedrooms_min=3,
        # bedrooms_max=4,
        # bathrooms_min=2,
        # bathrooms_max=3,
        # year_built_min=2010,
        # year_built_max=2023,
        # current_entity_owner_name='AMH',
        # current_history_sale_flag=True,
        # current_history_rental_flag=True,
        # current_history_listing_flag=True,
        # current_new_oncstruciton_flag=True,
        # current_owner_occupied_flag=True,
        # current_investor_owned_flag=True,
    )

    parcl_property_ids = homes.parcl_property_id.unique()

    events = client.property.events.retrieve(
        parcl_property_ids=parcl_property_ids,
        event_type="SALE",
        start_date="2023-01-01",
        end_date="2023-12-31",
        # entity_owner_name='AMH',
    )

    # homes.to_csv('homes.csv', index=False)
    # events.to_csv('events.csv', index=False)


if __name__ == "__main__":
    main()
