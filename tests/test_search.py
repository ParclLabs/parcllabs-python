from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from parcllabs.services.search import SearchMarkets

# Mock Data for testing
mock_response = {
    "items": [
        {"parcl_id": 1, "name": "Test Market 1"},
        {"parcl_id": 2, "name": "Test Market 2"},
    ],
    "links": {},
}


@pytest.fixture
def search_service() -> SearchMarkets:
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    client_mock.limit = 100
    return SearchMarkets(client=client_mock, url="/v1/search/markets")


@patch("parcllabs.services.search.SearchMarkets._fetch_get")
def test_retrieve(mock_sync_request: Mock, search_service: SearchMarkets) -> None:
    mock_sync_request.return_value = mock_response
    result = search_service.retrieve(query="test")
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "name" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["parcl_id"] == 1
    assert result.iloc[1]["parcl_id"] == 2
