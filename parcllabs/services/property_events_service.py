import pandas as pd
from alive_progress import alive_bar
from typing import Any, Mapping, Optional, List
from parcllabs.common import (
    DEFAULT_LIMIT,
    VALID_EVENT_TYPES,
)
from parcllabs.services.data_utils import safe_concat_and_format_dtypes
from parcllabs.services.parcllabs_service import ParclLabsService


CHUNK_SIZE = 1000


class PropertyEventsService(ParclLabsService):
    """
    Retrieve parcl_property_id for geographic markets in the Parcl Labs API.
    """

    def __init__(self, limit: int = DEFAULT_LIMIT, *args, **kwargs):
        super().__init__(limit=limit, *args, **kwargs)

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        data_container = []
        for results in data:
            meta_fields = [["property", key] for key in results["property"].keys()]
            df = pd.json_normalize(results, "events", meta=meta_fields)
            updated_cols_names = [
                c.replace("property.", "") for c in df.columns.tolist()
            ]  # for nested json
            df.columns = updated_cols_names
            data_container.append(df)
        output = safe_concat_and_format_dtypes(data_container)
        return output

    def retrieve(
        self,
        parcl_property_ids: List[int],
        event_type: str = None,
        start_date: str = None,
        end_date: str = None,
        params: Optional[Mapping[str, Any]] = {},
    ):
        """
        Retrieve property events for given parameters.
        """
        if event_type:
            if event_type not in VALID_EVENT_TYPES:
                raise ValueError(
                    f"event_type value error. Valid values are: {VALID_EVENT_TYPES}. Received: {event_type}"
                )
            else:
                params["event_type"] = event_type
        parcl_property_ids = [str(i) for i in parcl_property_ids]
        data_container = []
        with alive_bar(len(parcl_property_ids)) as bar:
            for i in range(0, len(parcl_property_ids), CHUNK_SIZE):
                batch_ids = parcl_property_ids[i : i + CHUNK_SIZE]
                params = {
                    "parcl_property_id": batch_ids,
                    "start_date": start_date,
                    "end_date": end_date,
                    **(params or {}),
                }
                batch_results = self._sync_request(params=params, method="POST")
                for result in batch_results:
                    if result is None:
                        continue
                    bar()
                data = self._as_pd_dataframe(batch_results)
                data_container.append(data)

        output = safe_concat_and_format_dtypes(data_container)
        return output
