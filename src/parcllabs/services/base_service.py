from abc import abstractmethod
from typing import Any, Mapping, Optional, List

import pandas as pd

class ParclLabsService(object):

    def __init__(self, client: Any) -> None:
        self.client = client

    def _request(
            self, 
            url: str,
            api_mode: str='v1',
            params: Optional[Mapping[str, Any]] = None
        ) -> Any:
        return self.client.get(url=url, params=params)
    
    def _as_pd_dataframe(
            self, 
            data: List[Mapping[str, Any]]
        ) -> Any:
        out = []
        for k, v in data.items():
            tmp = pd.DataFrame(v)
            tmp['parcl_id'] = k
            out.append(tmp)
        return pd.concat(out)
    
    @abstractmethod
    def retrieve(
            self,
            parcl_id: int,
            params: Optional[Mapping[str, Any]] = None
    ):
        pass