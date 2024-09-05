from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

import pandas as pd

from parcllabs.services.parcllabs_service import ParclLabsService


class ParclLabsStreamingService(ParclLabsService):
    def _convert_text_to_json(self, chunk):
        try:
            return json.loads(chunk)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return None

    def _process_streaming_data(self, data, batch_size=10000, num_workers=None):
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            chunks = deque(data.strip().split("\n"))
            futures = [
                executor.submit(self._convert_text_to_json, chunk)
                for chunk in chunks
                if chunk
            ]

            buffer = deque()
            for future in as_completed(futures):
                result = future.result()
                if result:
                    buffer.append(result)

                if len(buffer) >= batch_size:
                    yield pd.DataFrame(buffer)
                    buffer.clear()

            if buffer:
                yield pd.DataFrame(buffer)
