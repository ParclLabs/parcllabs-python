import json
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from parcllabs.services.properties.property_events_service import PropertyEventsService
from parcllabs.exceptions import NotFoundError

# Sample data for testing
sample_response = """{
    "items": [
    {
        "parcl_property_id": 173637433,
        "event_date": "2015-08-13",
        "event_type": "SALE",
        "event_name": "SOLD",
        "price": 268750,
        "owner_occupied_flag": 1,
        "new_construction_flag": 1,
        "sale_index": 3,
        "investor_flag": null,
        "entity_owner_name": null
    },
    {
        "parcl_property_id": 173637433,
        "event_date": "2012-03-23",
        "event_type": "SALE",
        "event_name": "SOLD",
        "price": null,
        "owner_occupied_flag": 0,
        "new_construction_flag": 1,
        "sale_index": 2,
        "investor_flag": null,
        "entity_owner_name": null
    }]
}"""


@pytest.fixture
def property_events_service():
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    client_mock.num_workers = 1
    service = PropertyEventsService(client=client_mock, url="/v1/property_events")
    return service


@patch(
    "parcllabs.services.properties.property_events_service.PropertyEventsService._post"
)
def test_retrieve_success(mock_post, property_events_service):
    mock_response = MagicMock()
    mock_response.json.return_value = json.loads(sample_response)
    mock_post.return_value = mock_response

    result = property_events_service.retrieve(parcl_property_ids=[123456])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "parcl_property_id" in result.columns
    assert "event_type" in result.columns
    assert "price" in result.columns


def test_retrieve_invalid_event_type(property_events_service):
    with pytest.raises(ValueError):
        property_events_service.retrieve(
            parcl_property_ids=[123456], event_type="INVALID_EVENT"
        )


def test_retrieve_invalid_entity_owner_name(property_events_service):
    with pytest.raises(ValueError):
        property_events_service.retrieve(
            parcl_property_ids=[123456], entity_owner_name="INVALID_ENTITY"
        )


@patch(
    "parcllabs.services.properties.property_events_service.PropertyEventsService._post"
)
def test_retrieve_not_found_error(mock_post, property_events_service):
    mock_post.side_effect = NotFoundError("Not found")

    result = property_events_service.retrieve(parcl_property_ids=[123456])

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@patch(
    "parcllabs.services.properties.property_events_service.PropertyEventsService._post"
)
def test_retrieve_general_exception(mock_post, property_events_service):
    mock_post.side_effect = Exception("General error")

    result = property_events_service.retrieve(parcl_property_ids=[123456])

    assert isinstance(result, pd.DataFrame)
    assert result.empty
