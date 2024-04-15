import requests
from typing import List

import pandas as pd

from parcllabs import api_base


class ParclLabsClient:
    def __init__(
            self, 
            api_key: str        
        ):
        if api_key is None:
            raise ValueError('api_key is required')
        self.api_key = api_key
        self.api_url = api_base

    def get(self, url: str, params: dict = None):
        url = self.api_url + url
        headers = self._get_headers()
        response = requests.get(url, headers=headers, params=params)
        return response.json()

    def _get_headers(self):
        return {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json",
        }