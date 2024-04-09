import pandas as pd
from parcllabs.client import ParclLabsClient, ParclLabsMetrics


if __name__ == '__main__':
    client = ParclLabsMetrics(
        api_key='p73DfBWqFSzP2aQMj4aucFhK72QOTr15dGqYxnFcgrY'
    )

    markets = pd.read_csv('data/msa_parcl_ids_top50.csv')

    ids = markets['parcl_id'].tolist()

    gross_yields = client.get_gross_yield(
        ids,
        property_type='single_family'
    )
    purchase_to_sale_ratio = client.get_purchase_to_sale_ratio(ids)
    output = gross_yields.merge(purchase_to_sale_ratio, on=['parcl_id', 'date'])
    output = pd.merge(markets, output, on='parcl_id')
    output.to_csv('data/msa_parcl_metrics.csv', index=False)