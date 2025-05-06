import json
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from parcllabs.exceptions import NotFoundError
from parcllabs.services.properties.property_events_service import PropertyEventsService
from parcllabs.services.properties.property_search import PropertySearch

# Sample data for testing PropertyEventsService
sample_events_response = """{
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

# Sample data for testing PropertySearch
sample_search_response = """{
    "items": [
        {
            "parcl_property_id": 12345,
            "address": "123 Main St",
            "city": "Anytown",
            "state_abbreviation": "CA",
            "zip_code": "90210",
            "current_on_market_flag": 1
        },
        {
            "parcl_property_id": 67890,
            "address": "456 Oak Ave",
            "city": "Anytown",
            "state_abbreviation": "CA",
            "zip_code": "90210",
            "current_on_market_flag": 0
        }
    ],
    "account": { "credits_used_session": 10 }
}"""


@pytest.fixture
def property_events_service() -> PropertyEventsService:
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    client_mock.num_workers = 1
    return PropertyEventsService(client=client_mock, url="/v1/property_events")


@patch("parcllabs.services.properties.property_events_service.PropertyEventsService._post")
def test_retrieve_success(mock_post: Mock, property_events_service: PropertyEventsService) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = json.loads(sample_events_response)
    mock_post.return_value = mock_response

    result = property_events_service.retrieve(parcl_property_ids=[123456])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "parcl_property_id" in result.columns
    assert "event_type" in result.columns
    assert "price" in result.columns


@patch("parcllabs.services.properties.property_events_service.PropertyEventsService._post")
def test_retrieve_not_found_error(
    mock_post: Mock, property_events_service: PropertyEventsService
) -> None:
    mock_post.side_effect = NotFoundError("Not found")

    result = property_events_service.retrieve(parcl_property_ids=[123456])

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@patch("parcllabs.services.properties.property_events_service.PropertyEventsService._post")
def test_retrieve_general_exception(
    mock_post: Mock, property_events_service: PropertyEventsService
) -> None:
    mock_post.side_effect = Exception("General error")

    result = property_events_service.retrieve(parcl_property_ids=[123456])

    assert isinstance(result, pd.DataFrame)
    assert result.empty


# --- Tests for PropertySearch ---


@pytest.fixture
def property_search_service() -> PropertySearch:
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    # PropertySearch specific attributes if needed
    return PropertySearch(client=client_mock, url="/v1/property/search")


@patch("parcllabs.services.properties.property_search.PropertySearch._get")
def test_retrieve_with_on_market_flag(
    mock_get: Mock, property_search_service: PropertySearch
) -> None:
    """Test retrieve method with current_on_market_flag parameter."""
    mock_response = MagicMock()
    # Use the search-specific sample response
    mock_response.json.return_value = json.loads(sample_search_response)
    mock_get.return_value = mock_response

    parcl_ids_to_test = [5503877]
    property_type_to_test = "single_family"

    # Test with flag = True
    result_on = property_search_service.retrieve(
        parcl_ids=parcl_ids_to_test,
        property_type=property_type_to_test,
        current_on_market_flag=True,
    )

    # Test with flag = False
    result_off = property_search_service.retrieve(
        parcl_ids=parcl_ids_to_test,
        property_type=property_type_to_test,
        current_on_market_flag=False,
    )

    # Assertions
    assert isinstance(result_on, pd.DataFrame)
    assert len(result_on) == 2  # Based on sample_search_response
    assert "current_on_market_flag" in result_on.columns

    assert isinstance(result_off, pd.DataFrame)

    # Check the calls made directly to the _get mock using call_args_list
    expected_params_on = {
        "property_type": property_type_to_test.upper(),
        "current_on_market_flag": "true",
        "parcl_id": parcl_ids_to_test[0],
    }
    expected_params_off = {
        "property_type": property_type_to_test.upper(),
        "current_on_market_flag": "false",
        "parcl_id": parcl_ids_to_test[0],
    }

    assert len(mock_get.call_args_list) == 2

    # Check first call (flag=True)
    call_on_args, call_on_kwargs = mock_get.call_args_list[0]
    assert call_on_kwargs["url"] == property_search_service.full_url
    assert call_on_kwargs["params"] == expected_params_on

    # Check second call (flag=False)
    call_off_args, call_off_kwargs = mock_get.call_args_list[1]
    assert call_off_kwargs["url"] == property_search_service.full_url
    assert call_off_kwargs["params"] == expected_params_off

    # Check that .json() was called on the response mock twice
    assert mock_response.json.call_count == 2
