from unittest.mock import MagicMock, Mock, patch

import pytest

from parcllabs.enums import RequestLimits
from parcllabs.services.properties.property_v2 import PropertyV2Service


@pytest.fixture
def property_v2_service() -> PropertyV2Service:
    client_mock = MagicMock()
    client_mock.api_url = "https://api.parcllabs.com"
    client_mock.api_key = "test_api_key"
    client_mock.num_workers = 1
    return PropertyV2Service(client=client_mock, url="/v2/property_search")


@pytest.fixture
def mock_response() -> Mock:
    mock = Mock()
    mock.json.return_value = {
        "data": [
            {
                "parcl_id": 123,
                "address": "123 Main St",
                "bedrooms": 3,
                "bathrooms": 2,
                "square_feet": 1500,
                "events": [
                    {
                        "event_id": 456,
                        "event_name": "LISTING",
                        "event_date": "2023-01-01",
                        "price": 500000,
                    }
                ],
            }
        ],
        "metadata": {"results": {"returned_count": 1, "total_available": 1}},
        "pagination": {"limit": 100, "offset": 0, "has_more": False},
        "account_info": {"credits_used": 1, "credits_remaining": 999},
    }
    return mock


def test_build_search_criteria(property_v2_service: PropertyV2Service) -> None:
    # Test with parcl_ids
    criteria = property_v2_service._build_search_criteria(parcl_ids=[123, 456])
    assert criteria == {"parcl_ids": [123, 456]}

    # Test with parcl_property_ids
    criteria = property_v2_service._build_search_criteria(parcl_property_ids=[789, 101])
    assert criteria == {"parcl_property_ids": [789, 101]}

    # Test with location
    geo_coordinates = {"latitude": 37.7749, "longitude": -122.4194, "radius": 5.0}
    criteria = property_v2_service._build_search_criteria(geo_coordinates=geo_coordinates)
    assert criteria == {"geo_coordinates": geo_coordinates}

    # Test with all parameters
    criteria = property_v2_service._build_search_criteria(
        parcl_ids=[123], parcl_property_ids=[456], geo_coordinates=geo_coordinates
    )
    assert criteria == {
        "parcl_ids": [123],
        "parcl_property_ids": [456],
        "geo_coordinates": geo_coordinates,
    }


def test_build_property_filters(property_v2_service: PropertyV2Service) -> None:
    filters = property_v2_service._build_property_filters(
        min_beds=2,
        max_beds=4,
        min_baths=1.5,
        max_baths=3,
        min_sqft=1000,
        max_sqft=2000,
        min_year_built=1980,
        max_year_built=2020,
        property_types=["SINGLE_FAMILY", "CONDO"],
        include_property_details=True,
        min_record_added_date="2023-01-01",
        max_record_added_date="2023-12-31",
    )

    assert filters == {
        "min_beds": 2,
        "max_beds": 4,
        "min_baths": 1.5,
        "max_baths": 3,
        "min_sqft": 1000,
        "max_sqft": 2000,
        "min_year_built": 1980,
        "max_year_built": 2020,
        "property_types": ["SINGLE_FAMILY", "CONDO"],
        "include_property_details": "true",
        "min_record_added_date": "2023-01-01",
        "max_record_added_date": "2023-12-31",
    }


def test_build_event_filters(property_v2_service: PropertyV2Service) -> None:
    filters = property_v2_service._build_event_filters(
        event_names=["LISTING", "SALE"],
        min_event_date="2023-01-01",
        max_event_date="2023-12-31",
        min_price=300000,
        max_price=800000,
        is_new_construction=True,
        min_record_updated_date="2023-01-01",
        max_record_updated_date="2023-12-31",
    )

    assert filters == {
        "event_names": ["LISTING", "SALE"],
        "min_event_date": "2023-01-01",
        "max_event_date": "2023-12-31",
        "min_price": 300000,
        "max_price": 800000,
        "is_new_construction": "true",
        "min_record_updated_date": "2023-01-01",
        "max_record_updated_date": "2023-12-31",
    }


def test_build_owner_filters(property_v2_service: PropertyV2Service) -> None:
    filters = property_v2_service._build_owner_filters(
        owner_name=["Blackstone", "Amherst"],
        is_current_owner=True,
        is_investor_owned=False,
        is_owner_occupied=True,
    )

    assert filters == {
        "owner_name": ["BLACKSTONE", "AMHERST"],
        "is_current_owner": "true",
        "is_investor_owned": "false",
        "is_owner_occupied": "true",
    }


def test_validate_limit(property_v2_service: PropertyV2Service) -> None:
    assert (
        property_v2_service._validate_limit(limit=None, auto_paginate=True)
        == RequestLimits.PROPERTY_V2_MAX.value
    )
    assert (
        property_v2_service._validate_limit(limit=None, auto_paginate=False)
        == RequestLimits.PROPERTY_V2_MAX.value
    )
    assert (
        property_v2_service._validate_limit(limit=100, auto_paginate=True)
        == RequestLimits.PROPERTY_V2_MAX.value
    )
    assert property_v2_service._validate_limit(limit=100, auto_paginate=False) == 100
    assert (
        property_v2_service._validate_limit(limit=1000000000, auto_paginate=True)
        == RequestLimits.PROPERTY_V2_MAX.value
    )


@patch.object(PropertyV2Service, "_post")
def test_fetch_post_single_page(
    mock_post: Mock, property_v2_service: PropertyV2Service, mock_response: Mock
) -> None:
    mock_post.return_value = mock_response
    result = property_v2_service._fetch_post(params={}, data={}, auto_paginate=False)

    assert len(result) == 1
    assert result[0] == mock_response.json()
    mock_post.assert_called_once()


@patch.object(PropertyV2Service, "_post")
def test_fetch_post_pagination(mock_post: Mock, property_v2_service: PropertyV2Service) -> None:
    # First response with pagination
    first_response = Mock()
    first_response.json.return_value = {
        "data": [{"parcl_id": 123}],
        "metadata": {"results": {"total_available": 2, "returned_count": 1}},
        "pagination": {"limit": 1, "offset": 0, "has_more": True},
        "account_info": {"credits_used": 1, "credits_remaining": 999},
    }

    # Second response for pagination
    second_response = Mock()
    second_response.json.return_value = {
        "data": [{"parcl_id": 456}],
        "metadata": {"results": {"total_available": 2, "returned_count": 1}},
        "pagination": {"limit": 1, "offset": 1, "has_more": False},
        "account_info": {"credits_used": 1, "credits_remaining": 998},
    }

    # Set up the mock to return different responses
    mock_post.side_effect = [first_response, second_response]

    result = property_v2_service._fetch_post(params={"limit": 1}, data={}, auto_paginate=True)

    assert len(result) == 2
    assert result[0]["data"][0]["parcl_id"] == 123
    assert result[1]["data"][0]["parcl_id"] == 456
    assert mock_post.call_count == 2


def test_as_pd_dataframe(property_v2_service: PropertyV2Service, mock_response: Mock) -> None:
    data = [mock_response.json()]
    test_df = property_v2_service._as_pd_dataframe(data)

    # Check that we have one row
    assert len(test_df) == 1

    # check that the property data is in the dataframe
    assert test_df.iloc[0]["parcl_id"] == 123
    assert test_df.iloc[0]["address"] == "123 Main St"
    assert test_df.iloc[0]["bedrooms"] == 3

    # check that the event data is flattened with event_ prefix
    assert test_df.iloc[0]["event_event_id"] == 456
    assert test_df.iloc[0]["event_event_name"] == "LISTING"
    assert test_df.iloc[0]["event_price"] == 500000


def test_get_metadata(property_v2_service: PropertyV2Service) -> None:
    results = [
        {"metadata": {"results": {"returned_count": 2, "total_available": 5}}},
        {"metadata": {"results": {"returned_count": 3, "total_available": 5}}},
    ]

    metadata = property_v2_service._get_metadata(results)

    assert metadata["results"]["returned_count"] == 5
    assert metadata["results"]["total_available"] == 5


@patch.object(PropertyV2Service, "_fetch_post")
def test_retrieve(
    mock_fetch_post: Mock, property_v2_service: PropertyV2Service, mock_response: Mock
) -> None:
    mock_fetch_post.return_value = [mock_response.json()]

    df, metadata = property_v2_service.retrieve(
        parcl_ids=[123],
        property_types=["SINGLE_FAMILY"],
        min_beds=2,
        max_beds=4,
        event_names=["LISTING"],
        limit=10,
    )
    # check that the dataframe has the expected data
    assert len(df) == 1
    assert df.iloc[0]["parcl_id"] == 123

    # check that the metadata is returned
    assert metadata == mock_response.json()["metadata"]

    # check that the correct data was passed to _fetch_post
    call_args = mock_fetch_post.call_args[1]
    assert call_args["params"] == {"limit": 10}

    data = call_args["data"]
    assert data["parcl_ids"] == [123]
    assert data["property_filters"]["property_types"] == ["SINGLE_FAMILY"]
    assert data["property_filters"]["min_beds"] == 2
    assert data["property_filters"]["max_beds"] == 4
    assert data["event_filters"]["event_names"] == ["LISTING"]
