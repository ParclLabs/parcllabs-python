import json

import pandas as pd

from parcllabs.search.metros import top_50_metros


def get_top_n_metros(n=50):
    return pd.DataFrame(top_50_metros[:n])


if __name__ == "__main__":
    markets = get_top_n_metros(n=3)
    print(markets)
    # markets.to_csv('data/msa_parcl_ids_top50.csv', index=False)
