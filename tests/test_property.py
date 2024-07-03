import pytest
from parcllabs.services.property_search import PropertySearch
from unittest.mock import MagicMock

# Mock Data for testing
mock_response = {
    "parcl_property_id": [123456, 456789],
}


@pytest.fixture
def property_search_service():
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    service = PropertySearch(client=client_mock, url="/property/v1/search_markets")
    service._sync_request = MagicMock(return_value=mock_response)
    return service


def test_proerty_search_retrieve(property_search_service):
    result = property_search_service.retrieve(zip="10001")
    assert not result.empty
    assert "parcl_property_id" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["parcl_property_id"] == 123456
    assert result.iloc[1]["parcl_property_id"] == 456789


def test_validate_property_type(property_search_service):
    with pytest.raises(ValueError):
        property_search_service.retrieve(zip="10001", property_type="invalid_type")
