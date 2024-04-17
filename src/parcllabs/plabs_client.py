import requests
from typing import Dict, Union
from requests.exceptions import RequestException

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
        """
        Send a GET request to the specified URL with the given parameters.

        Args:
            url (str): The URL endpoint to request.
            params (dict, optional): The parameters to send in the query string.

        Returns:
            dict: The JSON response as a dictionary.
        """
        try:
            url = self.api_url + url
            headers = self._get_headers()
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raises a HTTPError for bad responses
            return response.json()
        except RequestException as e:
            print(f"Request failed: {e}")
            return None

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json",
        }