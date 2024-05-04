from abc import abstractmethod
from datetime import datetime
from requests.exceptions import RequestException
from typing import Any, Mapping, Optional, List, Dict

import pandas as pd
from alive_progress import alive_bar


class ParclLabsService(object):

    def __init__(self, client: Any) -> None:
        self.client = client

    def _request(
        self,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        is_next: bool = False,
    ) -> Any:
        return self.client.get(url=url, params=params, is_next=is_next)

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        out = []
        for k, v in data.items():
            tmp = pd.DataFrame(v)
            tmp["parcl_id"] = k
            out.append(tmp)
        return pd.concat(out).reset_index(drop=True)

    def validate_date(self, date_str: str) -> str:
        """
        Validates the date string and returns it in the 'YYYY-MM-DD' format.
        Raises ValueError if the date is invalid or not in the expected format.
        """
        if date_str:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"Date {date_str} is not in the correct format YYYY-MM-DD."
                )

    def validate_property_type(self, property_type: str) -> str:
        """
        Validates the property type string and returns it in the 'single_family' or 'multi_family' format.
        Raises ValueError if the property type is invalid or not in the expected format.
        """
        valid_property_types = ["single_family", "condo", "townhouse", "all_properties"]
        if property_type:
            if property_type.lower() not in valid_property_types:
                raise ValueError(
                    f"Property type {property_type} is not valid. Must be either {', '.join(valid_property_types)}."
                )
            return property_type

    @abstractmethod
    def retrieve(self, parcl_id: int, params: Optional[Mapping[str, Any]] = None):
        pass

    def retrieve_many_items(
        self,
        parcl_ids: List[int],
        params: Optional[Mapping[str, Any]] = None,
        get_key_on_last_request: str = None,
    ) -> Dict[str, Any]:
        results = {}
        with alive_bar(len(parcl_ids)) as bar:
            for parcl_id in parcl_ids:
                try:
                    output = self.retrieve(parcl_id=parcl_id, params=params)
                    results[parcl_id] = output.get("items")
                except RequestException as e:
                    # continue if no data is found for the parcl_id
                    if '404' in str(e):
                        continue
                bar()

        additional_output = None
        if get_key_on_last_request:
            additional_output = output.get(get_key_on_last_request)

        return results, additional_output
