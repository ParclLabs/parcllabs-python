from unittest.mock import Mock, patch

import pandas as pd
import pytest

from parcllabs.parcllabs_client import ParclLabsClient
from parcllabs.services.metrics.property_type_service import PropertyTypeService
from parcllabs.services.parcllabs_service import ParclLabsService

API_URL = "https://api.example.com"
SERVICE_PATHS = {
    "price_feed": "/v2/price_feed/price_feed",
    "price_feed_smoothed": "/v2/price_feed/price_feed_smoothed",
}
ITEMS = [
    {"date": "2024-01-01", "price_feed": 250.5, "parcl_id": 2900187},
    {"date": "2024-01-02", "price_feed": 251.0, "parcl_id": 2900187},
]


def _page(items: list[dict]) -> dict:
    return {
        "items": items,
        "total": len(items),
        "limit": 1000,
        "offset": 0,
        "links": {"first": None, "last": None, "self": None, "next": None, "prev": None},
    }


@pytest.fixture
def client() -> ParclLabsClient:
    return ParclLabsClient(api_key="test_api_key", api_url=API_URL)


@pytest.fixture(params=list(SERVICE_PATHS))
def service(request: pytest.FixtureRequest, client: ParclLabsClient) -> PropertyTypeService:
    return getattr(client.price_feed_v2, request.param)


class TestPriceFeedV2Wiring:
    def test_group_lists_both_services(self, client: ParclLabsClient) -> None:
        assert client.price_feed_v2.services == list(SERVICE_PATHS)

    @pytest.mark.parametrize(("name", "path"), list(SERVICE_PATHS.items()))
    def test_services_are_post_only(self, client: ParclLabsClient, name: str, path: str) -> None:
        svc = getattr(client.price_feed_v2, name)
        assert isinstance(svc, PropertyTypeService)
        assert svc.full_post_url == API_URL + path
        assert svc.url is None
        assert svc.full_url is None


class TestPostOnlyServiceInit:
    @pytest.fixture
    def mock_client(self) -> Mock:
        mock_client = Mock()
        mock_client.api_url = API_URL
        mock_client.api_key = "test_api_key"
        return mock_client

    def test_post_url_only(self, mock_client: Mock) -> None:
        svc = ParclLabsService(url=None, client=mock_client, post_url="/v2/x")
        assert svc.full_url is None
        assert svc.full_post_url == API_URL + "/v2/x"

    def test_requires_url_or_post_url(self, mock_client: Mock) -> None:
        with pytest.raises(ValueError, match="url or post_url"):
            ParclLabsService(url=None, client=mock_client)


class TestPriceFeedV2Retrieve:
    def test_property_type_sent_in_post_body(self, service: PropertyTypeService) -> None:
        with patch.object(service, "_fetch_post", return_value=_page(ITEMS)) as mock_post:
            service.retrieve(
                parcl_ids=[2900187, 2900078],
                start_date="2024-01-01",
                end_date="2024-01-31",
                property_type="single_family",
            )

        params, data, auto_paginate = mock_post.call_args.args
        assert data["parcl_id"] == ["2900187", "2900078"]
        assert data["start_date"] == "2024-01-01"
        assert data["end_date"] == "2024-01-31"
        assert data["property_type"] == "SINGLE_FAMILY"
        assert "property_type" not in params
        assert auto_paginate is False

    def test_property_type_omitted_by_default(self, service: PropertyTypeService) -> None:
        with patch.object(service, "_fetch_post", return_value=_page(ITEMS)) as mock_post:
            service.retrieve(parcl_ids=[2900187])

        _, data, _ = mock_post.call_args.args
        assert "property_type" not in data

    def test_limit_sent_as_query_param(self, service: PropertyTypeService) -> None:
        with patch.object(service, "_fetch_post", return_value=_page(ITEMS)) as mock_post:
            service.retrieve(parcl_ids=[2900187], limit=500, auto_paginate=True)

        params, _, auto_paginate = mock_post.call_args.args
        assert params == {"limit": 500}
        assert auto_paginate is True

    def test_returns_dataframe(self, service: PropertyTypeService) -> None:
        with patch.object(service, "_fetch_post", return_value=_page(ITEMS)):
            results = service.retrieve(parcl_ids=[2900187])

        assert isinstance(results, pd.DataFrame)
        assert len(results) == 2
        assert {"date", "price_feed", "parcl_id"} <= set(results.columns)
        assert results["parcl_id"].tolist() == [2900187, 2900187]
