import requests
from typing import List

import pandas as pd


class ParclLabsClient:
    def __init__(
            self, 
            api_key: str, 
            api_url: str="https://api.devstage.parcllabs.com"
        ):
        self.api_key = api_key
        self.api_url = api_url

    def get(self, path: str, params: dict = None):
        url = self.api_url + path
        headers = self._get_headers()
        response = requests.get(url, headers=headers, params=params)
        return response.json()

    def _get_headers(self):
        return {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json",
        }
    
class ParclLabsMetrics(ParclLabsClient):

    def get_gross_yield(
            self, 
            ids: List[int],
            property_type: str='all_properties'
        ):
        results = {}
        all_items = []
        for id in ids:
            results[id] = self.get(path=f'/rental_market_metrics/{id}/gross_yield', params={'property_type': property_type})
            data = pd.DataFrame(results[id]['items'])
            data['parcl_id'] = id
            # data['property_type'] = results[id]['property_type']
            data = data.rename(columns={'pct_gross_yield': f'pct_gross_yield_{property_type}'})
            all_items.append(data)
        
        output = pd.concat(all_items, ignore_index=True)
        
        return output
    
    def get_purchase_to_sale_ratio(self, ids: List[int]):
        results = {}
        all_items = []
        for id in ids:
            results[id] = self.get(path=f'/investor_metrics/{id}/purchase_to_sale_ratio')
            data = pd.DataFrame(results[id]['items'])
            data['parcl_id'] = id
            data = data.rename(columns={'purchase_to_sale_ratio': 'investor_purchase_to_sale_ratio_all_properties'})
            all_items.append(data)
        
        output = pd.concat(all_items, ignore_index=True)
        
        return output