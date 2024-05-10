from abc import abstractmethod
from datetime import datetime
from typing import Any, Mapping, Optional, List, Dict
from requests.exceptions import RequestException

import pandas as pd
from alive_progress import alive_bar

from parcllabs.common import VALID_PROPERTY_TYPES


class ParclLabsService(object):
    """
    Base class for working with data from the Parcl Labs API.
    """

    def __init__(self, client: Any) -> None:
        self.client = client
        if client is None:
            raise ValueError("Missing required client object.")

    def _request(
        self,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        is_next: bool = False,
    ) -> Any:

        return self.client.get(url=url, params=params, is_next=is_next)

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        output = []

        for key, value in data.items():
            data_df = pd.DataFrame(value)
            data_df["parcl_id"] = key
            output.append(data_df)

        return pd.concat(output).reset_index(drop=True)

    def _get_valid_property_types(self) -> List[str]:
        return VALID_PROPERTY_TYPES

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

    @abstractmethod
    def retrieve(self, parcl_id: int, params: Optional[Mapping[str, Any]] = None):
        """
        Retrieves data for a single parcl_id.

        Args:
            parcl_id (int): The parcl_id to retrieve data for.
            params (dict, optional): Additional parameters to include in the request.

        """
        pass

    def retrieve_many_items(
        self,
        parcl_ids: List[int],
        params: Optional[Mapping[str, Any]] = None,
        get_key_on_last_request: str = None,
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
                    output = self.retrieve(parcl_id=parcl_id, params=params)
                    results[parcl_id] = output.get("items")
                except RequestException as e:
                    # continue if no data is found for the parcl_id
                    if "404" in str(e):
                        continue
                bar()

        additional_output = (
            output.get(get_key_on_last_request) if get_key_on_last_request else None
        )
        return results, additional_output
