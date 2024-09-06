from typing import Any, Mapping, Optional, List, Dict
from collections import deque
import json

import requests
from requests.exceptions import RequestException
import platform
import pandas as pd

from parcllabs.common import (
    DELETE_FROM_OUTPUT,
    DEFAULT_LIMIT_SMALL,
    DEFAULT_LIMIT_LARGE,
)
from parcllabs.exceptions import NotFoundError
from parcllabs.services.validators import Validators
from parcllabs.services.data_utils import safe_concat_and_format_dtypes
from parcllabs.__version__ import VERSION


class ParclLabsService:
    """
    Base class for working with data from the Parcl Labs API.
    """

    def __init__(self, url: str, client: Any, post_url: str = None) -> None:
        self.url = url
        self.post_url = post_url
        self.client = client
        if client is None:
            raise ValueError("Missing required client object.")
        self.api_url = client.api_url
        self.full_url = self.api_url + self.url
        self.full_post_url = self.api_url + self.post_url if post_url else None
        self.api_key = client.api_key
        self.headers = self._get_headers()

    def _get_headers(self) -> Dict[str, str]:
        """
        Generate the headers for API requests.

        This method constructs a dictionary of headers to be used in API requests.
        It includes authentication, content type, client version, and platform information.

        Returns:
            Dict[str, str]: A dictionary containing the following headers:
                - Authorization: The API key for authentication.
                - Content-Type: Set to "application/json" for JSON payloads.
                - X-Parcl-Labs-Python-Client-Version: The version of the Python client.
                - X-Parcl-Labs-Python-Client-Platform: The operating system of the client.
                - X-Parcl-Labs-Python-Client-Platform-Version: The Python version of the client.
        """
        return {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json",
            "X-Parcl-Labs-Python-Client-Version": VERSION,
            "X-Parcl-Labs-Python-Client-Platform": platform.system(),
            "X-Parcl-Labs-Python-Client-Platform-Version": platform.python_version(),
        }

    def _clean_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove None values from the parameters dictionary.

        This method filters out key-value pairs from the input dictionary
        where the value is None. This is useful for preparing request
        parameters, ensuring that only non-None values are sent in the request.

        Args:
            params (Dict[str, Any]): The original parameters dictionary.

        Returns:
            Dict[str, Any]: A new dictionary with all None values removed.
        """
        return {k: v for k, v in params.items() if v is not None}

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Generic method to make HTTP requests and handle errors.

        Args:
            method (str): The HTTP method ('GET' or 'POST').
            url (str): The URL endpoint to request.
            **kwargs: Additional arguments to pass to the request method.

        Returns:
            requests.Response: The response object.

        Raises:
            RequestException: If the request fails or an unexpected error occurs.
        """
        try:
            if method.upper() == "GET":
                params = kwargs.get("params", {})
                kwargs["params"] = params

            response = requests.request(method, url, headers=self.headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError:
            self.error_handling(response)
        except requests.exceptions.RequestException as err:
            raise RequestException(f"Request failed: {str(err)}")
        except Exception as e:
            raise RequestException(f"An unexpected error occurred: {str(e)}")

    def _post(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """
        Send a POST request to the specified URL with the given data.

        Args:
            url (str): The URL endpoint to request.
            params (dict, optional): The parameters to send in the query string.
            data (dict, optional): The data to send in the request body.

        Returns:
            requests.Response: The response object.
        """
        return self._make_request("POST", url, params=params, json=data)

    def _get(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Send a GET request to the specified URL with the given parameters.

        Args:
            url (str): The URL endpoint to request.
            params (dict, optional): The parameters to send in the query string.

        Returns:
            requests.Response: The response object.
        """
        return self._make_request("GET", url, params=params)

    def _fetch(
        self,
        parcl_ids: List[int],
        params: Optional[Mapping[str, Any]],
        auto_paginate: bool = False,
    ):
        """
        Fetch data for given Parcl IDs with specified parameters.

        This method handles the fetching of data based on the provided Parcl IDs and parameters.
        It supports both GET and POST requests, depending on the client's configuration and the number of Parcl IDs.

        Args:
            parcl_ids (List[int]): A list of Parcl IDs to fetch data for.
            params (Optional[Mapping[str, Any]]): Additional parameters for the request.
                If not provided or None, no additional parameters will be used.
            auto_paginate (bool, optional): Whether to automatically handle pagination.
                Defaults to False.

        Returns:
            The result of the fetch operation. The exact return type depends on the specific
            fetch method called (_fetch_post, _fetch_get, or _fetch_get_many_parcl_ids).
        """

        params = self._clean_params(params)

        if self.client.turbo_mode and self.full_post_url:
            # convert the list of parcl_ids into post body params, formatted
            # as strings
            if params.get("limit"):
                params["limit"] = self._validate_limit("POST", params["limit"])

            data = {"parcl_id": [str(pid) for pid in parcl_ids], **params}
            params = {"limit": params["limit"]} if params.get("limit") else {}

            print(f"data: {data}, params: {params}")

            return self._fetch_post(params, data, auto_paginate)
        else:
            if params.get("limit"):
                params["limit"] = self._validate_limit("GET", params["limit"])

            if len(parcl_ids) == 1:
                url = self.full_url.format(parcl_id=parcl_ids[0])
                return self._fetch_get(url, params, auto_paginate)
            else:
                return self._fetch_get_many_parcl_ids(parcl_ids, params, auto_paginate)

    def _fetch_get_many_parcl_ids(
        self, parcl_ids: List[int], params: Dict[str, Any], auto_paginate: bool
    ) -> List[Dict[str, Any]]:
        """
        Fetch data for multiple Parcl IDs using individual GET requests.

        This method iterates through a list of Parcl IDs, making a separate GET request
        for each ID. It's used when multiple Parcl IDs need to be fetched and the API
        doesn't support bulk fetching in a single request.

        Args:
            parcl_ids (List[int]): A list of Parcl IDs to fetch data for.
            params (Dict[str, Any]): Additional parameters to include in each GET request.
            auto_paginate (bool): Whether to automatically handle pagination for each request.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary contains
            the data fetched for a single Parcl ID. The order of the results corresponds
            to the order of the input parcl_ids.
        """
        results = []

        for parcl_id in parcl_ids:
            try:
                url = self.full_url.format(parcl_id=parcl_id)
                result = self._fetch_get(url, params, auto_paginate)
                results.append(result)
            except NotFoundError:
                continue

        return results

    def _fetch_post(
        self, params: Dict[str, Any], data: Dict[str, Any], auto_paginate: bool
    ):
        response = self._post(self.full_post_url, params=params, data=data)
        return self._process_and_paginate_response(
            response,
            auto_paginate,
            original_params=params,
            data=data,
            referring_method="post",
        )

    def _fetch_get(self, url: str, params: Dict[str, Any], auto_paginate: bool):
        response = self._get(url, params=params)
        result = self._process_and_paginate_response(
            response, auto_paginate, original_params=params, referring_method="get"
        )
        return result

    def _process_and_paginate_response(
        self,
        response,
        auto_paginate,
        original_params,
        data=None,
        referring_method: str = "get",
    ):

        if response.status_code == 404:
            return None
        response.raise_for_status()
        result = response.json()

        if (
            auto_paginate
            and "links" in result
            and result["links"].get("next") is not None
        ):
            all_items = result["items"]
            while result["links"].get("next") is not None:
                next_url = result["links"]["next"]
                if referring_method == "post":
                    next_response = self._post(
                        next_url, data=data, params=original_params
                    )
                else:
                    next_response = self._get(next_url, params=original_params)
                next_response.raise_for_status()
                result = next_response.json()
                all_items.extend(result["items"])
            result["items"] = all_items

        self.client.estimated_session_credit_usage += len(result.get("items", []))
        return result

    def retrieve(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        limit: Optional[int] = None,
        params: Optional[Mapping[str, Any]] = None,
        auto_paginate: bool = False,
    ):
        parcl_ids = Validators.validate_parcl_ids(parcl_ids)
        start_date = Validators.validate_date(start_date)
        end_date = Validators.validate_date(end_date)

        params = self._clean_params(
            {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit if limit else None,
                **(params or {}),
            }
        )

        data_container = []
        max_parcl_ids = 1000
        for i in range(0, len(parcl_ids), max_parcl_ids):
            try:
                chunk = parcl_ids[i : i + max_parcl_ids]
                results = self._fetch(chunk, params, auto_paginate=auto_paginate)
                data_container.extend(
                    results if isinstance(results, list) else [results]
                )
            except NotFoundError:
                # we don't want to kill the entire process if one of the chunks fails due to no data
                # sparse parcl_ids can result in no data found.
                # The post request can handle all 10k in one request, however the get request is one by one.
                # get handles this direclty per parcl_id, while post handles all at once.
                continue

        return self._as_pd_dataframe(data_container)

    @staticmethod
    def sanitize_output(data: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in data.items() if k not in DELETE_FROM_OUTPUT}

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> pd.DataFrame:
        data_container = deque()
        for results in data:
            if results is None:
                continue
            results = self.sanitize_output(results)
            meta_fields = [k for k in results.keys() if k != "items"]
            df = pd.json_normalize(results, record_path="items", meta=meta_fields)
            updated_cols_names = [c.replace(".", "_") for c in df.columns.tolist()]
            df.columns = updated_cols_names
            data_container.append(df)

        return safe_concat_and_format_dtypes(data_container)

    @staticmethod
    def error_handling(response: requests.Response) -> None:
        try:
            error_details = response.json()
            error_message = error_details.get("detail", "No detail provided by API")
            if response.status_code == 403:
                error_message += " Visit https://dashboard.parcllabs.com for more information or reach out to team@parcllabs.com."
            elif response.status_code == 422:
                details = error_details.get("detail")
                error_message = details[0].get("msg", "validation error")
            elif response.status_code == 429:
                error_message = error_details.get("error", "Rate Limit Exceeded")
            elif response.status_code == 404:
                raise NotFoundError(
                    "No data found matching search criteria. Try a different set of parameters."
                )

        except json.JSONDecodeError:
            error_message = "Failed to decode JSON error response"

        type_of_error = "Client" if 400 <= response.status_code < 500 else "Server"
        msg = f"{response.status_code} {type_of_error} Error: {error_message}"
        raise requests.RequestException(msg)

    @staticmethod
    def _validate_limit(method: str, limit: int) -> int:
        if method.upper() == "POST":
            if limit > DEFAULT_LIMIT_LARGE:
                print(
                    f"Supplied limit value is too large for requested endpoint. Setting limit to maxium value of {DEFAULT_LIMIT_LARGE}."
                )
                limit = DEFAULT_LIMIT_LARGE
        elif method.upper() == "GET":
            if limit > DEFAULT_LIMIT_SMALL:
                print(
                    f"Supplied limit value is too large for requested endpoint. Setting limit to maxium value of {DEFAULT_LIMIT_SMALL}."
                )
                limit = DEFAULT_LIMIT_SMALL
        else:
            raise ValueError("Invalid method. Must be either 'GET' or 'POST'.")

        return limit
