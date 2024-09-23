from collections import deque
from typing import Any, Mapping, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from alive_progress import alive_bar

from parcllabs.common import (
    VALID_EVENT_TYPES,
    VALID_ENTITY_NAMES,
    MAX_POST_LIMIT,
)
from parcllabs.services.data_utils import (
    safe_concat_and_format_dtypes,
)
from parcllabs.services.validators import Validators
from parcllabs.services.streaming.parcllabs_streaming_service import (
    ParclLabsStreamingService,
)
from parcllabs.exceptions import (
    NotFoundError,
)  # Assuming this is the exception for a 404 error


class PropertyEventsService(ParclLabsStreamingService):
    """
    Retrieve parcl_property_id event history.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        params = Validators.validate_input_str_param(
            param=event_type,
            param_name="event_type",
            valid_values=VALID_EVENT_TYPES,
            params_dict=params,
        )

        params = Validators.validate_input_str_param(
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
        total_properties = len(parcl_property_ids)

        def process_batch(batch_ids):
            local_params = params.copy()
            local_params["parcl_property_id"] = batch_ids
            try:
                response = self._post(url=self.full_post_url, data=local_params)
                data = response.json()
                return data  # Return data as json
            except NotFoundError:
                return None
            except Exception as e:
                print(f"Error processing batch {batch_ids}: {str(e)}")
                return None

        all_data = deque()
        with alive_bar(total_properties, title="Processing Parcl Property IDs") as bar:
            with ThreadPoolExecutor(max_workers=self.client.num_workers) as executor:
                futures = {
                    executor.submit(
                        process_batch, parcl_property_ids[i : i + MAX_POST_LIMIT]
                    ): len(parcl_property_ids[i : i + MAX_POST_LIMIT])
                    for i in range(0, total_properties, MAX_POST_LIMIT)
                }

                for future in as_completed(futures):
                    batch_result = future.result()
                    if batch_result:
                        batch_df = pd.DataFrame(batch_result)
                        all_data.append(batch_df)
                    bar(futures[future])

        df = safe_concat_and_format_dtypes(all_data)
        return df
