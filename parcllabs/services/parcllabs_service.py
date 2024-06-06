import pandas as pd
from datetime import datetime
from typing import Any, Mapping, Optional, List, Dict
from requests.exceptions import RequestException
from alive_progress import alive_bar
from parcllabs.common import VALID_PORTFOLIO_SIZES, VALID_PROPERTY_TYPES


class ParclLabsService(object):
    """
    Base class for working with data from the Parcl Labs API.
    """

    def __init__(self, url: str, client: Any) -> None:
        self.url = url
        if url is None:
            raise ValueError("Missing required url parameter.")
        self.client = client
        if client is None:
            raise ValueError("Missing required client object.")

    def _request(
        self,
        parcl_id: int = None,
        url: str = None,
        params: Optional[Mapping[str, Any]] = None,
        is_next: bool = False,
    ) -> Any:
        if url:
            url = url
        elif parcl_id:
            url = self.url.format(parcl_id=parcl_id)
        else:
            url = self.url
        return self.client.get(url=url, params=params, is_next=is_next)

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        output = []

        for key, value in data.items():
            data_df = pd.json_normalize(value)  # for nested json
            data_df["parcl_id"] = key

            updated_cols_names = [
                c.replace(".", "_") for c in data_df.columns.tolist()
            ]  # for nested json
            data_df.columns = updated_cols_names
            output.append(data_df)

        return pd.concat(output).reset_index(drop=True)

    def _get_valid_property_types(self) -> List[str]:
        return VALID_PROPERTY_TYPES

    def _get_valid_portfolio_sizes(self) -> List[str]:
        return VALID_PORTFOLIO_SIZES

    def validate_date(self, date_str: str) -> str:
        """
        Validates the date string and returns it in the 'YYYY-MM-DD' format.
        Raises ValueError if the date is invalid or not in the expected format.
        """
        if date_str:
            try:
                formatted_date = datetime.strptime(date_str, "%Y-%m-%d").strftime(
                    "%Y-%m-%d"
                )
                return formatted_date
            except ValueError:
                raise ValueError(
                    f"Date {date_str} is not in the correct format YYYY-MM-DD."
                )

    def validate_property_type(self, property_type: str) -> str:
        """
        Validates the property type string and returns it in the 'single_family' or 'multi_family' format.
        Raises ValueError if the property type is invalid or not in the expected format.
        """
        valid_property_types = self._get_valid_property_types()
        if property_type:
            if property_type.lower() not in valid_property_types:
                raise ValueError(
                    f"Property type {property_type} is not valid. Must be one of {', '.join(valid_property_types)}."
                )
            return property_type

    def validate_portfolio_size(self, portfolio_size: str) -> str:
        """
        Validates the portfolio size string and returns it in the expected format.
        Raises ValueError if the portfolio size is invalid or not in the expected format.
        """
        valid_portfolio_sizes = self._get_valid_portfolio_sizes()
        if portfolio_size:
            if portfolio_size.upper() not in valid_portfolio_sizes:
                raise ValueError(
                    f"Portfolio size {portfolio_size} is not valid. Must be one of {', '.join(valid_portfolio_sizes)}."
                )
            return portfolio_size.upper()

    def retrieve(
        self,
        parcl_id: int,
        start_date: str = None,
        end_date: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
        auto_paginate: bool = False,
    ):
        """
        Retrieves data for a single parcl_id.

        Args:
            parcl_id (int): The parcl_id to retrieve data for.
            params (dict, optional): Additional parameters to include in the request.
            as_dataframe (bool, optional): Return the results as a pandas DataFrame.
            auto_paginate (bool, optional): Automatically paginate through the results.
        """
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            **(params or {}),
        }
        results = self._request(
            parcl_id=parcl_id,
            params=params,
        )

        if auto_paginate:
            tmp = results.copy()
            while results["links"].get("next"):
                results = self._request(url=results["links"]["next"], is_next=True)
                tmp["items"].extend(results["items"])
            tmp["links"] = results["links"]
            results = tmp

        if as_dataframe:
            fmt = {results.get("parcl_id"): results.get("items")}
            df = self._as_pd_dataframe(fmt)
            if "property_type" in params:
                df["property_type"] = results.get("property_type")
            if "portfolio_size" in params:
                df["portfolio_size"] = results.get("portfolio_size")
            return df

        return results

    def retrieve_many_items(
        self,
        parcl_ids: List[int],
        params: Optional[Mapping[str, Any]] = None,
        auto_paginate: bool = False,
        get_key_on_last_request: List[str] = [],
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Retrieves data for multiple parcl_ids.

        Args:

            parcl_ids (List[int]): The list of parcl_ids to retrieve data for.
            params (dict, optional): Additional parameters to include in the request.
            get_key_on_last_request (str, optional): The key to retrieve from the last request.

        Returns:
            dict: A dictionary containing the results for each parcl_id.
        """

        results = {}
        with alive_bar(len(parcl_ids)) as bar:
            for parcl_id in parcl_ids:
                try:
                    output = self.retrieve(
                        parcl_id=parcl_id,
                        params=params,
                        auto_paginate=auto_paginate,
                        **kwargs,
                    )
                    results[parcl_id] = output.get("items")
                except RequestException as e:
                    # continue if no data is found for the parcl_id
                    if "404" in str(e):
                        continue
                bar()

        additional_output = {key: output.get(key) for key in get_key_on_last_request}
        return results, additional_output

    def retrieve_many(
        self,
        parcl_ids: List[int],
        start_date: str = None,
        end_date: str = None,
        params: Optional[Mapping[str, Any]] = None,
        as_dataframe: bool = False,
        auto_paginate: bool = False,
        get_key_on_last_request: List[str] = [],
    ):
        start_date = self.validate_date(start_date)
        end_date = self.validate_date(end_date)

        params = {
            "start_date": start_date,
            "end_date": end_date,
            **(params or {}),
        }

        results, additional_output = self.retrieve_many_items(
            parcl_ids=parcl_ids,
            params=params,
            auto_paginate=auto_paginate,
            get_key_on_last_request=get_key_on_last_request,
        )

        if as_dataframe:
            df = self._as_pd_dataframe(results)
            if "property_type" in additional_output:
                df["property_type"] = additional_output.get("property_type")
            if "portfolio_size" in additional_output:
                df["portfolio_size"] = additional_output.get("portfolio_size")
            return df

        return results
