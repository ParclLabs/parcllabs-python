import pandas as pd
import aiohttp
import asyncio
import json
import requests
import platform
import nest_asyncio
from requests.exceptions import RequestException
from typing import Any, Mapping, Optional, List, Dict
from alive_progress import alive_bar
from parcllabs.common import DELETE_FROM_OUTPUT, DEFAULT_LIMIT
from parcllabs.services.validators import Validators
from parcllabs.services.data_utils import safe_concat_and_format_dtypes
from parcllabs.__version__ import VERSION

nest_asyncio.apply()


class ParclLabsService(object):
    """
    Base class for working with data from the Parcl Labs API.
    """

    def __init__(self, url: str, client: Any, limit: int = DEFAULT_LIMIT) -> None:
        self.url = url
        if url is None:
            raise ValueError("Missing required url parameter.")
        self.client = client
        if client is None:
            raise ValueError("Missing required client object.")

        self.limit = limit
        self.client = client
        self.api_url = client.api_url
        self.api_key = client.api_key
        self.markets = {}

    async def _fetch(
        self,
        session,
        parcl_id: int,
        params: Optional[Mapping[str, Any]],
        auto_paginate: bool = False,
    ):
        if params:
            if not params.get("limit"):
                params["limit"] = self.limit
        try:
            full_url = self.api_url + self.url.format(parcl_id=parcl_id)
            headers = self._get_headers()
            async with session.get(
                full_url,
                headers=headers,
                params={k: v for k, v in params.items() if v is not None},
            ) as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                result = await response.json()

                # If auto_paginate is True, recursively fetch all pages
                if (
                    auto_paginate
                    and "links" in result
                    and result["links"].get("next") is not None
                ):
                    all_items = result["items"]
                    while result["links"].get("next") is not None:
                        next_url = result["links"]["next"]
                        async with session.get(
                            next_url, headers=headers
                        ) as next_response:
                            next_response.raise_for_status()
                            result = await next_response.json()
                            all_items.extend(result["items"])
                    result["items"] = (
                        all_items  # Replace the items with the accumulated items
                    )
                self.client.estimated_session_credit_usage += len(
                    result.get("items", [])
                )
                return result
        except aiohttp.ClientResponseError as e:
            try:
                error_details = await response.json()
                error_message = error_details.get("detail", "No detail provided by API")
                error = error_message
                if response.status == 403:
                    error = f"{error_message}. Visit https://dashboard.parcllabs.com for more information or reach out to team@parcllabs.com."
                if response.status == 429:
                    error = error_details.get("error", "Rate Limit Exceeded")
            except json.JSONDecodeError:
                error_message = "Failed to decode JSON error response"
            type_of_error = ""
            if 400 <= response.status < 500:
                type_of_error = "Client"
            elif 500 <= response.status < 600:
                type_of_error = "Server"
            msg = f"{response.status} {type_of_error} Error: {error}"
            raise aiohttp.ClientResponseError(msg, status=e.status)
        except aiohttp.ClientError as err:
            raise aiohttp.ClientError(f"Request failed: {str(err)}")
        except Exception as e:
            raise aiohttp.ClientError(f"An unexpected error occurred: {str(e)}")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json",
            "X-Parcl-Labs-Python-Client-Version": f"{VERSION}",
            "X-Parcl-Labs-Python-Client-Platform": f"{platform.system()}",
            "X-Parcl-Labs-Python-Client-Platform-Version": f"{platform.python_version()}",
        }

    async def _fetch_all(self, parcl_ids, params, auto_paginate=False):
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch(session, parcl_id, params, auto_paginate=auto_paginate)
                for parcl_id in parcl_ids
            ]
            return await asyncio.gather(*tasks)

    async def _retrieve(self, parcl_ids: List[int], params, auto_paginate: bool):
        results = []
        with alive_bar(len(parcl_ids)) as bar:
            for i in range(0, len(parcl_ids), 10):
                batch_ids = parcl_ids[i : i + 10]
                batch_results = await self._fetch_all(
                    batch_ids, params, auto_paginate=auto_paginate
                )
                for result in batch_results:
                    if result is None:
                        continue
                    results.append(result)
                    bar()
        return results

    def retrieve(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        limit: Optional[int] = None,
        params: Optional[Mapping[str, Any]] = None,
        auto_paginate: bool = False,
    ):
        start_date = Validators.validate_date(start_date)
        end_date = Validators.validate_date(end_date)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit if limit is not None else self.limit,
            **(params or {}),
        }

        loop = asyncio.get_event_loop()
        data_container = loop.run_until_complete(
            self._retrieve(parcl_ids, params, auto_paginate=auto_paginate)
        )

        output = self._as_pd_dataframe(data_container)
        return output

    def sanitize_output(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Removes unwanted keys from the output data.
        """
        for key in DELETE_FROM_OUTPUT:
            if key in data:
                del data[key]
        return data

    def _sync_request(
        self,
        parcl_id: int = None,
        url: str = None,
        params: Optional[Mapping[str, Any]] = None,
        is_next: bool = False,
        method: str = "GET",
    ) -> Any:
        if url:
            url = url
        elif parcl_id:
            url = self.url.format(parcl_id=parcl_id)
        else:
            url = self.url
        if method == "GET":
            return self.get(url=url, params=params, is_next=is_next)
        elif method == "POST":
            return self.post(url=url, params=params)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

    def error_handling(self, response: requests.Response) -> None:
        try:
            error = ""
            error_details = response.json()
            error_message = error_details.get("detail", "No detail provided by API")
            error = error_message
            if response.status_code == 403:
                error = f"{error_message}. Visit https://dashboard.parcllabs.com for more information or reach out to team@parcllabs.com."
            if response.status_code == 429:
                error = error_details.get("error", "Rate Limit Exceeded")
        except json.JSONDecodeError:
            error_message = "Failed to decode JSON error response"
        type_of_error = ""
        if 400 <= response.status_code < 500:
            type_of_error = "Client"
        elif 500 <= response.status_code < 600:
            type_of_error = "Server"
        msg = f"{response.status_code} {type_of_error} Error: {error}"
        raise RequestException(msg)

    def get(self, url: str, params: dict = None, is_next: bool = False):
        """
        Send a GET request to the specified URL with the given parameters.

        Args:
            url (str): The URL endpoint to request.
            params (dict, optional): The parameters to send in the query string.

        Returns:
            dict: The JSON response as a dictionary.
        """

        if params:
            if not params.get("limit"):
                params["limit"] = self.limit
        try:
            if is_next:
                full_url = url
            else:
                full_url = self.api_url + url
            headers = self._get_headers()
            response = requests.get(full_url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            self.error_handling(response)
        except requests.exceptions.RequestException as err:
            raise RequestException(f"Request failed: {str(err)}")
        except Exception as e:
            raise RequestException(f"An unexpected error occurred: {str(e)}")

    def post(self, url: str, params: dict = None):
        """
        Send a GET request to the specified URL with the given parameters.

        Args:
            url (str): The URL endpoint to request.
            params (dict, optional): The parameters to send in the query string.

        Returns:
            dict: The JSON response as a dictionary.
        """
        try:
            full_url = self.api_url + url
            headers = self._get_headers()
            response = requests.post(full_url, headers=headers, json=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            self.error_handling(response)
        except requests.exceptions.RequestException as err:
            raise RequestException(f"Request failed: {str(err)}")
        except Exception as e:
            raise RequestException(f"An unexpected error occurred: {str(e)}")

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        data_container = []
        for results in data:
            results = self.sanitize_output(results)
            meta_fields = [k for k in results.keys() if k != "items"]
            df = pd.json_normalize(results, record_path="items", meta=meta_fields)
            updated_cols_names = [
                c.replace(".", "_") for c in df.columns.tolist()
            ]  # for nested json
            df.columns = updated_cols_names
            data_container.append(df)

        output = safe_concat_and_format_dtypes(data_container)
        return output
