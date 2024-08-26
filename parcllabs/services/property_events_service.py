from collections import deque
from typing import Any, Mapping, Optional, List
import pandas as pd
from alive_progress import alive_bar
from parcllabs.common import (
    DEFAULT_LIMIT,
    VALID_EVENT_TYPES,
    VALID_ENTITY_NAMES,
    MAX_POST_LIMIT,
)
from parcllabs.services.data_utils import (
    safe_concat_and_format_dtypes,
    validate_input_str_param,
)
from parcllabs.services.parcllabs_service import ParclLabsService
from concurrent.futures import ThreadPoolExecutor, as_completed
from parcllabs.exceptions import (
    NotFoundError,
)  # Assuming this is the exception for a 404 error


class PropertyEventsService(ParclLabsService):
    """
    Retrieve parcl_property_id event history.
    """

    def __init__(self, limit: int = DEFAULT_LIMIT, *args, **kwargs):
        super().__init__(limit=limit, *args, **kwargs)

    def _as_pd_dataframe(self, data: List[Mapping[str, Any]]) -> Any:
        data_container = []
        for results in data:
            df = pd.json_normalize(
                results, "events", meta=[["property", "parcl_property_id"]]
            )
            updated_cols_names = [
                c.replace("property.", "") for c in df.columns.tolist()
            ]
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
        entity_owner_name: str = None,
        params: Optional[Mapping[str, Any]] = {},
    ):
        """
        Retrieve property events for given parameters.
        """
        params = {}
        params = validate_input_str_param(
            param=event_type,
            param_name="event_type",
            valid_values=VALID_EVENT_TYPES,
            params_dict=params,
        )

        params = validate_input_str_param(
            param=entity_owner_name,
            param_name="entity_owner_name",
            valid_values=VALID_ENTITY_NAMES,
            params_dict=params,
        )

        if start_date:
            params["start_date"] = start_date

        if end_date:
            params["end_date"] = end_date

        parcl_property_ids = [str(i) for i in parcl_property_ids]
        data_container = deque()
        total_properties = len(parcl_property_ids)

        def process_batch(batch_ids):
            local_params = params.copy()
            local_params["parcl_property_id"] = batch_ids
            try:
                batch_results = self._sync_request(params=local_params, method="POST")
                if batch_results:
                    return clean_results(batch_results)
            except NotFoundError:
                # Handle 404 Not Found Error: Skip the batch
                return []
            except Exception as e:
                # Optionally, log other exceptions or re-raise them
                print(f"Error processing batch {batch_ids}: {str(e)}")
                return []

        with (
            alive_bar(total_properties) as bar,
            ThreadPoolExecutor(max_workers=self.client.num_workers) as executor,
        ):
            futures = {}
            for i in range(0, total_properties, MAX_POST_LIMIT):
                batch_ids = parcl_property_ids[i : i + MAX_POST_LIMIT]
                future = executor.submit(process_batch, batch_ids)
                futures[future] = len(batch_ids)

            for future in as_completed(futures):
                batch_data = future.result()
                if batch_data:
                    data_container.extend(batch_data)
                bar(futures[future])

        df = pd.DataFrame(data_container)
        columns_order = ["parcl_property_id"] + [
            col for col in df.columns if col != "parcl_property_id"
        ]
        df = df[columns_order]
        df["event_date"] = pd.to_datetime(df["event_date"])
        return df


def clean_results(batch_results: List[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    """
    Flatten the events and include the parcl_property_id in each event.
    """
    cleaned = []
    for result in batch_results:
        events = result.get("events", [])
        pid = result.get("property", {}).get("parcl_property_id")

        for event in events:
            event["parcl_property_id"] = pid
            cleaned.append(event)

    return cleaned
