import json
import platform
from collections import deque
from collections.abc import Mapping
from typing import Any

import pandas as pd
import requests
from requests.exceptions import RequestException

from parcllabs.__version__ import VERSION
from parcllabs.common import DELETE_FROM_OUTPUT, GET_METHOD, POST_METHOD
from parcllabs.enums import RequestLimits, RequestMethods, ResponseCodes
from parcllabs.exceptions import NotFoundError
from parcllabs.services.data_utils import safe_concat_and_format_dtypes
from parcllabs.services.validators import Validators


class ParclLabsService:
    """
    Base class for working with data from the Parcl Labs API.
    """

    def __init__(self, url: str, client: object, post_url: str | None = None) -> None:
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

    def _get_headers(self) -> dict[str, str]:
        """
        Generate the headers for API requests.

        This method constructs a dictionary of headers to be used in API requests.
        It includes authentication, content type, client version, and platform
        information.

        Returns:
            Dict[str, str]: A dictionary containing the following headers:
                - Authorization: The API key for authentication.
                - Content-Type: Set to "application/json" for JSON payloads.
                - X-Parcl-Labs-Python-Client-Version: The version of the Python client.
                - X-Parcl-Labs-Python-Client-Platform: The operating system of the
                client.
                - X-Parcl-Labs-Python-Client-Platform-Version: The Python version of the
                 client.
        """
        return {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json",
            "X-Parcl-Labs-Python-Client-Version": VERSION,
            "X-Parcl-Labs-Python-Client-Platform": platform.system(),
            "X-Parcl-Labs-Python-Client-Platform-Version": platform.python_version(),
        }

    def _clean_params(self, params: dict[str, Any]) -> dict[str, Any]:
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

    def _make_request(self, method: str, url: str, **kwargs: dict) -> requests.Response:
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
            if method == GET_METHOD:
                params = kwargs.get("params", {})
                kwargs["params"] = params

            response = requests.request(method, url, headers=self.headers, **kwargs)  # noqa: S113
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.error_handling(response)
        except requests.exceptions.RequestException as err:
            raise RequestException(f"Request failed: {err!s}") from err
        except Exception as e:
            raise RequestException(f"An unexpected error occurred: {e!s}") from e
        else:
            return response

    def _post(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
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
        return self._make_request(POST_METHOD, url, params=params, json=data)

    def _get(self, url: str, params: dict[str, Any] | None = None) -> requests.Response:
        """
        Send a GET request to the specified URL with the given parameters.

        Args:
            url (str): The URL endpoint to request.
            params (dict, optional): The parameters to send in the query string.

        Returns:
            requests.Response: The response object.
        """
        return self._make_request(GET_METHOD, url, params=params)

    def _fetch(
        self,
        parcl_ids: list[int],
        params: Mapping[str, Any] | None,
        auto_paginate: bool = False,
    ) -> object:
        """
        This method handles the fetching of data based on the provided Parcl IDs and
        parameters.
        It supports both GET and POST requests, depending on the client's configuration
        and the number of Parcl IDs.
        Args:
            parcl_ids (List[int]): A list of Parcl IDs to fetch data for.
            params (Optional[Mapping[str, Any]]): Additional parameters for the request.
                If not provided or None, no additional parameters will be used.
            auto_paginate (bool, optional): Whether to automatically handle pagination.
                Defaults to False.
        Returns:
            The result of the fetch operation. The exact return type depends on the
            specific fetch method called (_fetch_post, _fetch_get, or
            _fetch_get_many_parcl_ids).
        """
        params = self._clean_params(params) if params else {}

        # Use client's default limit if no limit specified in params
        if "limit" not in params or params["limit"] is None:
            params["limit"] = self.client.limit

        if self.full_post_url:
            # convert the list of parcl_ids into post body params, formatted
            # as strings
            if params.get("limit"):
                params["limit"] = self._validate_limit(POST_METHOD, params["limit"])

            data = {"parcl_id": [str(pid) for pid in parcl_ids], **params}
            params = {"limit": params["limit"]} if params.get("limit") else {}

            print(f"data: {data}, params: {params}")

            return self._fetch_post(params, data, auto_paginate)
        if params.get("limit"):
            params["limit"] = self._validate_limit(GET_METHOD, params["limit"])

        if len(parcl_ids) == 1:
            url = self.full_url.format(parcl_id=parcl_ids[0])
            return self._fetch_get(url, params, auto_paginate)
        return self._fetch_get_many_parcl_ids(parcl_ids, params, auto_paginate)

    def _fetch_get_many_parcl_ids(
        self, parcl_ids: list[int], params: dict[str, Any], auto_paginate: bool
    ) -> list[dict[str, Any]]:
        """
        Fetch data for multiple Parcl IDs using individual GET requests.

        This method iterates through a list of Parcl IDs, making a separate GET request
        for each ID. It's used when multiple Parcl IDs need to be fetched and the API
        doesn't support bulk fetching in a single request.

        Args:
            parcl_ids (List[int]): A list of Parcl IDs to fetch data for.
            params (Dict[str, Any]): Additional parameters to include in each GET
            request.
            auto_paginate (bool): Whether to automatically handle pagination for each
            request.

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
        self,
        params: dict[str, Any],
        data: dict[str, Any],
        auto_paginate: bool,
    ) -> object:
        response = self._post(self.full_post_url, params=params, data=data)
        return self._process_and_paginate_response(
            response,
            auto_paginate,
            original_params=params,
            data=data,
            referring_method="post",
        )

    def _fetch_get(self, url: str, params: dict[str, Any], auto_paginate: bool) -> object:
        response = self._get(url, params=params)
        return self._process_and_paginate_response(
            response, auto_paginate, original_params=params, referring_method="get"
        )

    def _process_and_paginate_response(
        self,
        response: requests.Response,
        auto_paginate: bool,
        original_params: dict[str, Any],
        data: dict[str, Any] | None = None,
        referring_method: str = "get",
    ) -> object:
        if response.status_code == ResponseCodes.NOT_FOUND.value:
            return None
        response.raise_for_status()
        result = response.json()

        if auto_paginate and "links" in result and result["links"].get("next") is not None:
            all_items = result["items"]
            while result["links"].get("next") is not None:
                next_url = result["links"]["next"]
                if referring_method == "post":
                    next_response = self._post(next_url, data=data, params=original_params)
                else:
                    next_response = self._get(next_url, params=original_params)
                next_response.raise_for_status()
                result = next_response.json()
                all_items.extend(result["items"])
            result["items"] = all_items

        return result

    def retrieve(
        self,
        parcl_ids: list[int],
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        params: Mapping[str, Any] | None = None,
        auto_paginate: bool = False,
    ) -> pd.DataFrame:
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
                data_container.extend(results if isinstance(results, list) else [results])
            except NotFoundError:
                # we don't want to kill the entire process if one of the chunks fails
                # due to no data. sparse parcl_ids can result in no data found.
                # The post request can handle all 10k in one request, however the get
                # request is one by one. get handles this direclty per parcl_id, while
                # post handles all at once.
                continue

        return self._as_pd_dataframe(data_container)

    def _update_account_info(self, account_info: dict) -> None:
        """
        Update the account info for the client.
        """
        if account_info:
            final_account_dict = {
                "est_credits_used": account_info.get("est_credits_used", 0),
                "est_session_credits_used": (
                    self.client.account_info.get("est_session_credits_used", 0)
                    + account_info.get("est_credits_used", 0)
                ),
                "est_remaining_credits": account_info.get("est_remaining_credits", 0),
            }
            self.client.account_info = final_account_dict

    @staticmethod
    def sanitize_output(data: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in data.items() if k not in DELETE_FROM_OUTPUT}

    def _as_pd_dataframe(self, data: list[Mapping[str, Any]]) -> pd.DataFrame:
        data_container = deque()
        for results in data:
            if results is None:
                continue
            account_info = results.get("account")
            sanitized_results = self.sanitize_output(results)
            meta_fields = [k for k in sanitized_results.keys() if k != "items"]
            normalized_df = pd.json_normalize(
                sanitized_results, record_path="items", meta=meta_fields
            )
            updated_cols_names = [c.replace(".", "_") for c in normalized_df.columns.tolist()]
            normalized_df.columns = updated_cols_names
            data_container.append(normalized_df)
            self._update_account_info(account_info)

        return safe_concat_and_format_dtypes(data_container)

    @staticmethod
    def error_handling(response: requests.Response) -> None:
        try:
            error_details = response.json()
            error_message = error_details.get("detail", "No detail provided by API")
            if response.status_code == ResponseCodes.FORBIDDEN.value:
                error_message += (
                    " Visit https://dashboard.parcllabs.com for more information"
                    " or reach out to team@parcllabs.com."
                )
            elif response.status_code == ResponseCodes.VALIDATION_ERROR.value:
                details = error_details.get("detail")
                error_message = details[0].get("msg", "validation error")
            elif response.status_code == ResponseCodes.RATE_LIMIT_EXCEEDED.value:
                error_message = error_details.get("error", "Rate Limit Exceeded")
            elif response.status_code == ResponseCodes.NOT_FOUND.value:
                raise NotFoundError(
                    "No data found matching search criteria.Try a different set of parameters."
                )

        except json.JSONDecodeError:
            error_message = "Failed to decode JSON error response"

        type_of_error = (
            "Client"
            if ResponseCodes.CLIENT_ERROR.value
            <= response.status_code
            < ResponseCodes.SERVER_ERROR.value
            else "Server"
        )
        msg = f"{response.status_code} {type_of_error} Error: {error_message}"
        raise requests.RequestException(msg)

    @staticmethod
    def _validate_limit(method: RequestMethods, limit: int) -> int:
        default_large = RequestLimits.DEFAULT_LARGE.value
        default_small = RequestLimits.DEFAULT_SMALL.value

        if method == POST_METHOD:
            if limit > default_large:
                print(
                    f"Supplied limit value is too large for requested endpoint."
                    f"Setting limit to maxium value of {default_large}."
                )
                limit = default_large

        elif method == GET_METHOD:
            if limit > default_small:
                print(
                    f"Supplied limit value is too large for requested endpoint."
                    f"Setting limit to maxium value of {default_small}."
                )
                limit = default_small

        else:
            raise ValueError("Invalid method. Must be either 'GET' or 'POST'.")

        return limit
