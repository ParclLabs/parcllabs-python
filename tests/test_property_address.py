import json
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from parcllabs.services.properties.property_address import PropertyAddressSearch


SAMPLE_ADDRESSES = [
    {
        "address": "5967 coplin street",
        "city": "detroit",
        "state_abbreviation": "mi",
        "zip_code": "90001",
        "source_id": "123",
    },
    {
        "address": "7239 rea croft dr",
        "city": "charlotte",
        "state_abbreviation": "NC",
        "zip_code": "28226",
        "source_id": "456",
    },
]


SAMPLE_RESPONSE = """{
    "items": [
        {
            "address": "5967 COPLIN STREET",
            "unit": "",
            "city": "DETROIT",
            "state_abbreviation": "MI",
            "zip_code": "90001",
            "source_id": "123"
        },
        {
            "address": "7239 REA CROFT DR",
            "unit": "",
            "city": "CHARLOTTE",
            "state_abbreviation": "NC",
            "zip_code": "28226",
            "source_id": "456"
        }
    ]
}"""


@pytest.fixture
def property_events_service():
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    client_mock.num_workers = 1
    service = PropertyAddressSearch(
        client=client_mock, url="/v1/property/search_address"
    )
    return service


@patch("parcllabs.services.properties.property_address.PropertyAddressSearch._post")
def test_retrieve_success(mock_post, property_events_service):
    mock_response = MagicMock()
    mock_response.json.return_value = json.loads(SAMPLE_RESPONSE)
    mock_post.return_value = mock_response

    result = property_events_service.retrieve(addresses=SAMPLE_ADDRESSES)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "address" in result.columns
    assert "city" in result.columns
    assert "state_abbreviation" in result.columns
    assert "zip_code" in result.columns
    assert "source_id" in result.columns
