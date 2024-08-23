import pandas as pd
from alive_progress import alive_bar
from typing import Any, Mapping, Optional, List
from parcllabs.common import (
    DEFAULT_LIMIT,
    VALID_EVENT_TYPES,
    VALID_ENTITY_NAMES
)
from parcllabs.services.data_utils import (
    safe_concat_and_format_dtypes,
    validate_input_str_param
)

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
            # meta_fields = [["property", key] for key in results["property"].keys()]
            df = pd.json_normalize(results, "events", meta=[['property', 'parcl_property_id']])
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
        entity_owner_name: str = None,
        params: Optional[Mapping[str, Any]] = {},
    ):
        """
        Retrieve property events for given parameters.
        """
        params = {}
        params = validate_input_str_param(
            param=event_type, 
            param_name='event_type', 
            valid_values=VALID_EVENT_TYPES,
            params_dict=params
        )

        params = validate_input_str_param(
            param=entity_owner_name, 
            param_name='entity_owner_name', 
            valid_values=VALID_ENTITY_NAMES,
            params_dict=params
        )

        if start_date:
            params['start_date'] = start_date

        if end_date:
            params['end_date'] = end_date


        parcl_property_ids = [str(i) for i in parcl_property_ids]
        data_container = []
        total_properties = len(parcl_property_ids)

        with alive_bar(total_properties) as bar:
            for i in range(0, total_properties, CHUNK_SIZE):
                batch_ids = parcl_property_ids[i : i + CHUNK_SIZE]
                params['parcl_property_id'] = batch_ids
                batch_results = self._sync_request(params=params, method="POST")

                # Process the batch results
                if batch_results:
                    data = self._as_pd_dataframe(batch_results)
                    if not data.empty:
                        data_container.append(data)

                # Update the progress bar for each property in this batch
                for _ in range(len(batch_ids)):
                    bar()

        output = safe_concat_and_format_dtypes(data_container)
        # organize output
        format_cols = ['parcl_property_id', 'sale_index', 'event_date', 'event_type', 'event_name', 'price', 'owner_occupied_flag', 'new_construction_flag', 'investor_flag', 'entity_owner_name']
        non_null_format_cols = []
        for col in format_cols:
            if col in output.columns:
                non_null_format_cols.append(col)
        output = output[non_null_format_cols]

        self.client.estimated_session_credit_usage += output.shape[0]
        return output
