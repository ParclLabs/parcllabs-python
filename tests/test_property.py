import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from parcllabs.services.properties.property_events_service import PropertyEventsService
from parcllabs.exceptions import NotFoundError

# Sample data for testing
sample_response = """
{
    "property": {
        "parcl_property_id": "123456"
    },
    "events": [
        {
            "event_type": "LISTED_FOR_SALE",
            "date": "2023-01-01",
            "price": 500000
        },
        {
            "event_type": "SOLD",
            "date": "2023-02-15",
            "price": 495000
        }
    ]
}
"""


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
@patch(
    "parcllabs.services.properties.property_events_service.PropertyEventsService._process_streaming_data"
)
def test_retrieve_success(
    mock_process_streaming_data, mock_post, property_events_service
):
    mock_response = MagicMock()
    mock_response.text = sample_response
    mock_post.return_value = mock_response
    mock_process_streaming_data.return_value = [
        pd.DataFrame(
            {
                "parcl_property_id": ["123456", "123456"],
                "event_type": ["LISTED_FOR_SALE", "SOLD"],
                "date": ["2023-01-01", "2023-02-15"],
                "price": [500000, 495000],
            }
        )
    ]

    result = property_events_service.retrieve(parcl_property_ids=[123456])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "parcl_property_id" in result.columns
    assert "event_type" in result.columns
    assert "date" in result.columns
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
