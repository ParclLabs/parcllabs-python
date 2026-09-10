from unittest.mock import Mock, patch

import pandas as pd
import pytest

from parcllabs.parcllabs_client import ParclLabsClient, ServiceGroup
from parcllabs.services.metrics.property_type_service import PropertyTypeService
from parcllabs.services.parcllabs_service import ParclLabsService

API_URL = "https://api.example.com"
SERVICE_PATHS = {
    "price_feed": "/v2/price_feed/price_feed",
    "price_feed_smoothed": "/v2/price_feed/price_feed_smoothed",
}
PARCL_ID = 2900187
PAGE_ONE = [
    {"date": "2024-01-01", "price_feed": 250.5, "parcl_id": PARCL_ID},
    {"date": "2024-01-02", "price_feed": 251.0, "parcl_id": PARCL_ID},
]
PAGE_TWO = [
    {"date": "2024-01-03", "price_feed": 251.5, "parcl_id": PARCL_ID},
    {"date": "2024-01-04", "price_feed": 252.0, "parcl_id": PARCL_ID},
]
REQUEST_TARGET = "parcllabs.services.parcllabs_service.requests.request"


def _page(items: list[dict], next_link: str | None = None) -> dict:
    return {
        "items": [dict(item) for item in items],  # the service extends items in place
        "total": len(items),
        "limit": 2,
        "offset": 0,
        "links": {
            "first": None,
            "last": None,
            "self": None,
            "next": next_link,
            "prev": None,
        },
    }


def _response(items: list[dict], next_link: str | None = None) -> Mock:
    response = Mock()
    response.status_code = 200
    response.json.return_value = _page(items, next_link)
    return response


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


class TestServiceGroupCompatibility:
    def test_positional_registration_still_works(self, client: ParclLabsClient) -> None:
        group = ServiceGroup(client)
        group.add_service("custom", "/custom/{parcl_id}", ParclLabsService)

        assert group.custom.full_url == API_URL + "/custom/{parcl_id}"
        assert group.custom.full_post_url is None
        assert group.services == ["custom"]

    def test_keyword_registration_with_alias(self, client: ParclLabsClient) -> None:
        group = ServiceGroup(client)
        group.add_service(
            name="custom",
            url="/custom/{parcl_id}",
            service_class=ParclLabsService,
            post_url="/custom",
            alias="custom_alias",
        )

        assert group.custom is group.custom_alias
        assert group.custom.full_post_url == API_URL + "/custom"


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
        with patch.object(service, "_fetch_post", return_value=_page(PAGE_ONE)) as mock_post:
            service.retrieve(
                parcl_ids=[PARCL_ID, 2900078],
                start_date="2024-01-01",
                end_date="2024-01-31",
                property_type="single_family",
            )

        params, data, auto_paginate = mock_post.call_args.args
        assert data == {
            "parcl_id": [str(PARCL_ID), "2900078"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "property_type": "SINGLE_FAMILY",
        }
        assert params == {}
        assert auto_paginate is False

    def test_property_type_omitted_by_default(self, service: PropertyTypeService) -> None:
        with patch.object(service, "_fetch_post", return_value=_page(PAGE_ONE)) as mock_post:
            service.retrieve(parcl_ids=[PARCL_ID])

        _, data, _ = mock_post.call_args.args
        assert "property_type" not in data

    def test_limit_and_offset_are_query_params(self, service: PropertyTypeService) -> None:
        with patch.object(service, "_fetch_post", return_value=_page(PAGE_ONE)) as mock_post:
            service.retrieve(parcl_ids=[PARCL_ID], limit=2, params={"offset": 2})

        params, data, _ = mock_post.call_args.args
        assert params == {"limit": 2, "offset": 2}
        assert "limit" not in data
        assert "offset" not in data

    def test_does_not_mutate_caller_params(self, client: ParclLabsClient) -> None:
        shared_params: dict = {}
        daily = client.price_feed_v2.price_feed
        smoothed = client.price_feed_v2.price_feed_smoothed

        with patch.object(daily, "_fetch_post", return_value=_page(PAGE_ONE)):
            daily.retrieve(
                parcl_ids=[PARCL_ID], property_type="SINGLE_FAMILY", params=shared_params
            )
        assert shared_params == {}

        with patch.object(smoothed, "_fetch_post", return_value=_page(PAGE_ONE)) as mock_post:
            smoothed.retrieve(parcl_ids=[PARCL_ID], params=shared_params)
        _, data, _ = mock_post.call_args.args
        assert "property_type" not in data

    def test_returns_dataframe(self, service: PropertyTypeService) -> None:
        with patch.object(service, "_fetch_post", return_value=_page(PAGE_ONE)):
            results = service.retrieve(parcl_ids=[PARCL_ID])

        assert isinstance(results, pd.DataFrame)
        assert len(results) == 2
        assert {"date", "price_feed", "parcl_id"} <= set(results.columns)
        assert results["parcl_id"].tolist() == [PARCL_ID, PARCL_ID]


class TestPriceFeedV2Pagination:
    def test_auto_paginate_follows_next_links(self, service: PropertyTypeService) -> None:
        url = API_URL + service.post_url
        next_link = f"{url}?limit=2&offset=2"
        expected_body = {
            "parcl_id": [str(PARCL_ID)],
            "start_date": "2024-01-01",
            "end_date": "2024-01-04",
            "property_type": "SINGLE_FAMILY",
        }

        with patch(
            REQUEST_TARGET,
            side_effect=[_response(PAGE_ONE, next_link), _response(PAGE_TWO)],
        ) as mock_request:
            results = service.retrieve(
                parcl_ids=[PARCL_ID],
                start_date="2024-01-01",
                end_date="2024-01-04",
                property_type="SINGLE_FAMILY",
                limit=2,
                auto_paginate=True,
            )

        assert mock_request.call_count == 2
        first, second = mock_request.call_args_list
        assert first.args == ("POST", url)
        assert first.kwargs["params"] == {"limit": 2}
        assert first.kwargs["json"] == expected_body
        assert second.args == ("POST", next_link)
        assert second.kwargs["params"] == {"limit": 2}
        assert second.kwargs["json"] == expected_body
        assert len(results) == 4
        assert results["date"].is_unique

    def test_auto_paginate_does_not_reapply_initial_offset(
        self, service: PropertyTypeService
    ) -> None:
        next_link = f"{API_URL}{service.post_url}?limit=2&offset=4"

        with patch(
            REQUEST_TARGET,
            side_effect=[_response(PAGE_ONE, next_link), _response(PAGE_TWO)],
        ) as mock_request:
            service.retrieve(
                parcl_ids=[PARCL_ID], limit=2, params={"offset": 2}, auto_paginate=True
            )

        first, second = mock_request.call_args_list
        assert first.kwargs["params"] == {"limit": 2, "offset": 2}
        assert second.args[1] == next_link
        assert second.kwargs["params"] == {"limit": 2}

    def test_without_auto_paginate_returns_first_page_only(
        self, service: PropertyTypeService
    ) -> None:
        next_link = f"{API_URL}{service.post_url}?limit=2&offset=2"

        with patch(REQUEST_TARGET, return_value=_response(PAGE_ONE, next_link)) as mock_request:
            results = service.retrieve(parcl_ids=[PARCL_ID], limit=2)

        assert mock_request.call_count == 1
        assert len(results) == 2
