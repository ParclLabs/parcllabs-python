import pandas as pd
import pytest
from parcllabs.services.property_search import PropertySearch
from parcllabs.services.property_events_service import PropertyEventsService
from unittest.mock import MagicMock

# Mock Data for testing
mock_search_response = {
    "parcl_property_id": [123456, 456789],
}
mock_event_response = [
    {
        "property": {
            "parcl_property_id": 123456,
            "address": "123 Main St",
            "unit": "#123",
            "city": "NEW YORK",
            "state_abbreviation": "NY",
            "zip5": "10001",
            "zip4": "5509",
            "latitude": 123.123,
            "longitude": -123.123,
            "property_type": "CONDO",
            "bedrooms": 0,
            "bathrooms": 0.0,
            "square_footage": 2000,
            "year_built": 2020,
        },
        "events": [
            {
                "event_date": "2023-03-04",
                "event_type": "RENTAL",
                "event_name": "LISTED_RENT",
                "price": 6995.0,
            },
            {
                "event_date": "2022-10-21",
                "event_type": "RENTAL",
                "event_name": "LISTING_REMOVED",
                "price": None,
            },
            {
                "event_date": "2020-09-21",
                "event_type": "RENTAL",
                "event_name": "LISTED_RENT",
                "price": 7000.0,
            },
        ],
    },
]


@pytest.fixture
def property_search_service():
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    service = PropertySearch(client=client_mock, url="v1//property/search_markets")
    service._sync_request = MagicMock(return_value=mock_search_response)
    return service


@pytest.fixture
def property_events_service():
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    service = PropertyEventsService(
        client=client_mock, url="/v1/property/event_history"
    )
    service._sync_request = MagicMock(return_value=mock_event_response)
    return service


def test_property_search_retrieve(property_search_service):
    result = property_search_service.retrieve(zip="10001")
    assert not result.empty
    assert "parcl_property_id" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["parcl_property_id"] == 123456
    assert result.iloc[1]["parcl_property_id"] == 456789


def test_validate_property_type(property_search_service):
    with pytest.raises(ValueError):
        property_search_service.retrieve(zip="10001", property_type="invalid_type")


def test_property_event_history_retrieve(property_events_service):
    result = property_events_service.retrieve(parcl_property_ids=[123456])
    assert not result.empty
    assert "parcl_property_id" in result.columns
    assert "address" in result.columns
    assert "event_date" in result.columns
    assert "event_type" in result.columns
    assert "event_name" in result.columns
    assert len(result) == 3
    assert result.iloc[0]["parcl_property_id"] == 123456
    assert result.iloc[0]["event_date"] == pd.Timestamp("2023-03-04")
    assert result.iloc[0]["event_type"] == "RENTAL"
    assert result.iloc[1]["event_date"] == pd.Timestamp("2022-10-21")
    assert result.iloc[1]["event_name"] == "LISTING_REMOVED"
    assert result.iloc[2]["event_date"] == pd.Timestamp("2020-09-21")


def test_validate_event_type(property_events_service):
    with pytest.raises(ValueError):
        property_events_service.retrieve(
            parcl_property_ids=[123456], event_type="invalid_type"
        )