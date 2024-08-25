import os
import click
from parcllabs import ParclLabsClient

@click.command()
@click.option('--market', required=True, help='The market to search. Use "United States" for nationwide searches.')
@click.option('--current-entity-owner-name', default=None, help='Filter by the current entity owner name.')
@click.option('--event-type', required=True, type=click.Choice(['sale', 'rental', 'listing'], case_sensitive=False), help='The type of event to retrieve.')
@click.option('--output-file', required=True, help='The name and location of the output file.')
@click.option('--start-date', default="2024-07-24", help='The start date for the event search.')
@click.option('--end-date', default="2024-08-25", help='The end date for the event search.')
def main(market, current_entity_owner_name, event_type, output_file, start_date, end_date):
    api_key = os.getenv("PARCL_LABS_API_KEY")
    client = ParclLabsClient(api_key=api_key)

    location_type = None if market.lower() == "united states" else "CBSA"

    # search
    markets = client.search.markets.retrieve(
        query=market,
        location_type=location_type,
        sort_by="TOTAL_POPULATION",
        sort_order="DESC",
        limit=1,
    )

    name = markets['name'].values[0]
    location_type = markets['location_type'].values[0]
    total_population = markets['total_population'].values[0]
    income = markets['median_income'].values[0]

    click.echo(f"Market data found: Name: {name}, Location Type: {location_type}, Total Population: {total_population:,}, Median Income: ${income:,}")
    confirm = click.confirm('Is this the correct market?', default=True)
    if not confirm:
        click.echo('Aborting.')
        return

    market_id = markets["parcl_id"].values[0]

    homes = client.property.search.retrieve(
        parcl_ids=[market_id],
        property_type="single_family",
        current_entity_owner_name=current_entity_owner_name,
    )

    parcl_property_ids = homes.parcl_property_id.unique()

    events = client.property.events.retrieve(
        parcl_property_ids=parcl_property_ids,
        event_type=event_type.upper(),
        start_date=start_date,
        end_date=end_date,
    )

    # Save to file
    homes_file = output_file.replace('.csv', '_homes.csv')
    events_file = output_file.replace('.csv', '_events.csv')

    homes.to_csv(homes_file, index=False)
    events.to_csv(events_file, index=False)

    total_units = homes['parcl_property_id'].nunique()
    distinct_units = events['parcl_property_id'].nunique()
    median_price = events['price'].median()

    click.echo(f"Homes data saved to {homes_file}")
    click.echo(f"Events data saved to {events_file}")

    entity_name = current_entity_owner_name.replace('invitation_homes', 'Invitation Homes').replace('amh', 'American Homes 4 Rent')

    if event_type == 'rental':
        click.echo(f"Summary {entity_name}: {distinct_units} units on {event_type} market in {market} with a median price of ${median_price:,.2f} an estimated vacancy rate of {distinct_units/total_units:.2%}")
        click.echo(f"Preview of events data:")
        click.echo(events.head())
        click.echo(f"Preview of all units owned:")
        click.echo(homes.head())
    else:
        click.echo(f"Summary: {distinct_units} units on {event_type} market in {market}")

if __name__ == "__main__":
    main()

    # python operators.py --market "United States" --current-entity-owner-name "invitation_homes" --event-type rental --output-file "invh.csv"
