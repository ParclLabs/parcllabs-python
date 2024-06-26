import pytest
from parcllabs.services.search import SearchMarkets
from unittest.mock import MagicMock

# Mock Data for testing
mock_response = {
    "items": [
        {"parcl_id": 1, "name": "Test Market 1"},
        {"parcl_id": 2, "name": "Test Market 2"},
    ],
    "links": {},
}


@pytest.fixture
def search_service():
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    service = SearchMarkets(client=client_mock, url="/v1/search/markets")
    service._sync_request = MagicMock(return_value=mock_response)
    return service


def test_retrieve(search_service):
    result = search_service.retrieve(query="test")
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "name" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["parcl_id"] == 1
    assert result.iloc[1]["parcl_id"] == 2


def test_validate_location_type(search_service):
    with pytest.raises(ValueError):
        search_service.retrieve(location_type="invalid_type")


def test_validate_region(search_service):
    with pytest.raises(ValueError):
        search_service.retrieve(region="invalid_region")


def test_validate_state_abbreviation(search_service):
    with pytest.raises(ValueError):
        search_service.retrieve(state_abbreviation="invalid_abbrev")


def test_validate_state_fips_code(search_service):
    with pytest.raises(ValueError):
        search_service.retrieve(state_fips_code="invalid_fips")


def test_validate_sort_by(search_service):
    with pytest.raises(ValueError):
        search_service.retrieve(sort_by="invalid_sort_by")


def test_validate_sort_order(search_service):
    with pytest.raises(ValueError):
        search_service.retrieve(sort_order="invalid_sort_order")
