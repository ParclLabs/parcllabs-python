from collections import deque
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from parcllabs.enums import RequestLimits
from parcllabs.exceptions import (
    NotFoundError,
)
from parcllabs.services.data_utils import (
    safe_concat_and_format_dtypes,
)
from parcllabs.services.parcllabs_service import ParclLabsService
from parcllabs.services.validators import Validators


class PropertyEventsService(ParclLabsService):
    """
    Retrieve parcl_property_id event history.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def _prepare_params(
        self,
        event_type: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        entity_owner_name: str | None = None,
        record_updated_date_start: str | None = None,
        record_updated_date_end: str | None = None,
    ) -> dict:
        """Prepare parameters for property events retrieval."""
        params = {}

        if event_type:
            params["event_type"] = event_type.upper()

        if entity_owner_name:
            params["entity_owner_name"] = entity_owner_name.upper()

        if start_date:
            params["start_date"] = Validators.validate_date(start_date)

        if end_date:
            params["end_date"] = Validators.validate_date(end_date)

        if record_updated_date_start:
            record_updated_date_start = Validators.validate_date(record_updated_date_start)
            params["record_updated_date_start"] = record_updated_date_start

        if record_updated_date_end:
            record_updated_date_end = Validators.validate_date(record_updated_date_end)
            params["record_updated_date_end"] = record_updated_date_end

        return params

    def retrieve(
        self,
        parcl_property_ids: list[int],
        event_type: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        entity_owner_name: str | None = None,
        record_updated_date_start: str | None = None,
        record_updated_date_end: str | None = None,
        params: Mapping[str, Any] | None = {},
    ) -> pd.DataFrame:
        """
        Retrieve property events for given parameters.
        """
        params = self._prepare_params(
            event_type=event_type,
            start_date=start_date,
            end_date=end_date,
            entity_owner_name=entity_owner_name,
            record_updated_date_start=record_updated_date_start,
            record_updated_date_end=record_updated_date_end,
        )

        parcl_property_ids = [str(i) for i in parcl_property_ids]
        total_properties = len(parcl_property_ids)

        def process_batch(batch_ids: list[str]) -> list[dict] | None:
            local_params = params.copy()
            local_params["parcl_property_id"] = batch_ids
            try:
                response = self._post(url=self.full_post_url, data=local_params)
                data = response.json()
                self._update_account_info(data.get("account"))
                return data.get("items")  # Return data as json
            except NotFoundError:
                return None
            except Exception as e:
                print(f"Error processing batch {batch_ids}: {e!s}")
                return None

        all_data = deque()
        with ThreadPoolExecutor(max_workers=self.client.num_workers) as executor:
            max_post_limit = RequestLimits.MAX_POST.value
            futures = {
                executor.submit(process_batch, parcl_property_ids[i : i + max_post_limit]): len(
                    parcl_property_ids[i : i + max_post_limit]
                )
                for i in range(0, total_properties, max_post_limit)
            }

            for future in as_completed(futures):
                batch_result = future.result()
                if batch_result:
                    batch_df = pd.DataFrame(batch_result)
                    all_data.append(batch_df)

        return safe_concat_and_format_dtypes(all_data)
