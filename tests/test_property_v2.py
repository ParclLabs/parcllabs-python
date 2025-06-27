from unittest.mock import MagicMock, Mock, patch

import pytest

from parcllabs.enums import RequestLimits
from parcllabs.schemas.schemas import GeoCoordinates, PropertyV2RetrieveParams
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


def test_build_property_filters_from_schema(property_v2_service: PropertyV2Service) -> None:
    """Test building property filters from Pydantic schema."""
    params = PropertyV2RetrieveParams(
        min_beds=2,
        max_beds=4,
        min_baths=1.5,
        max_baths=3.0,
        min_sqft=1000,
        max_sqft=2000,
        min_year_built=1980,
        max_year_built=2020,
        property_types=["SINGLE_FAMILY", "CONDO"],
        include_property_details=True,
        min_record_added_date="2023-01-01",
        max_record_added_date="2023-12-31",
    )

    filters = property_v2_service._build_property_filters(params)

    assert filters == {
        "min_beds": 2,
        "max_beds": 4,
        "min_baths": 1.5,
        "max_baths": 3.0,
        "min_sqft": 1000,
        "max_sqft": 2000,
        "min_year_built": 1980,
        "max_year_built": 2020,
        "property_types": ["SINGLE_FAMILY", "CONDO"],
        "include_property_details": "true",
        "min_record_added_date": "2023-01-01",
        "max_record_added_date": "2023-12-31",
    }


def test_build_event_filters_from_schema(property_v2_service: PropertyV2Service) -> None:
    """Test building event filters from Pydantic schema."""
    params = PropertyV2RetrieveParams(
        event_names=["LISTING", "SALE"],
        min_event_date="2023-01-01",
        max_event_date="2023-12-31",
        min_price=300000,
        max_price=800000,
        is_new_construction=True,
        min_record_updated_date="2023-01-01",
        max_record_updated_date="2023-12-31",
    )

    filters = property_v2_service._build_event_filters(params)

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


def test_build_owner_filters_from_schema(property_v2_service: PropertyV2Service) -> None:
    """Test building owner filters from Pydantic schema."""
    params = PropertyV2RetrieveParams(
        owner_name=["Blackstone", "Amherst"],
        is_current_owner=True,
        is_investor_owned=False,
        is_owner_occupied=True,
    )

    filters = property_v2_service._build_owner_filters(params)

    assert filters == {
        "owner_name": ["BLACKSTONE", "AMHERST"],
        "is_current_owner": "true",
        "is_investor_owned": "false",
        "is_owner_occupied": "true",
    }


def test_schema_validation() -> None:
    """Test Pydantic schema validation."""
    # Test valid parameters
    params = PropertyV2RetrieveParams(
        parcl_ids=[123, 456],
        property_types=["SINGLE_FAMILY"],
        min_beds=2,
        max_beds=4,
        min_price=500000,
        max_price=1000000,
    )
    assert params.parcl_ids == [123, 456]
    assert params.property_types == ["SINGLE_FAMILY"]
    assert params.min_beds == 2
    assert params.max_beds == 4

    # Test geo coordinates
    geo = GeoCoordinates(latitude=40.7128, longitude=-74.0060, radius=10.0)
    params_with_geo = PropertyV2RetrieveParams(geo_coordinates=geo)
    assert params_with_geo.geo_coordinates == geo


def test_schema_validation_errors() -> None:
    """Test Pydantic schema validation errors."""
    # Test invalid property type
    with pytest.raises(ValueError, match="Invalid property type"):
        PropertyV2RetrieveParams(property_types=["INVALID_TYPE"])

    # Test invalid geo coordinates
    with pytest.raises(ValueError, match="Input should be less than or equal to 90"):
        GeoCoordinates(latitude=100, longitude=-74.0060, radius=10.0)

    # Test invalid date format
    with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
        PropertyV2RetrieveParams(min_event_date="2023/01/01")

    # Test invalid range (min > max)
    with pytest.raises(ValueError, match="max_beds cannot be less than min_beds"):
        PropertyV2RetrieveParams(min_beds=5, max_beds=3)

    # Test invalid price range
    with pytest.raises(ValueError, match="max_price cannot be less than min_price"):
        PropertyV2RetrieveParams(min_price=1000000, max_price=500000)


def test_schema_with_none_values() -> None:
    """Test schema handles None values correctly."""
    params = PropertyV2RetrieveParams()
    assert params.parcl_ids is None
    assert params.property_types is None
    assert params.min_beds is None
    assert params.max_beds is None
    assert params.geo_coordinates is None
    assert params.params == {}


def test_validate_limit(property_v2_service: PropertyV2Service) -> None:
    assert property_v2_service._validate_limit(limit=None) == RequestLimits.PROPERTY_V2_MAX.value
    assert property_v2_service._validate_limit(limit=None) == RequestLimits.PROPERTY_V2_MAX.value
    assert property_v2_service._validate_limit(limit=100) == 100
    assert (
        property_v2_service._validate_limit(limit=1000000000) == RequestLimits.PROPERTY_V2_MAX.value
    )


@patch.object(PropertyV2Service, "_post")
def test_fetch_post_single_page(
    mock_post: Mock, property_v2_service: PropertyV2Service, mock_response: Mock
) -> None:
    mock_post.return_value = mock_response
    result = property_v2_service._fetch_post(params={}, data={})

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

    result = property_v2_service._fetch_post(params={"limit": 1}, data={})

    assert len(result) == 1
    assert result[0]["data"][0]["parcl_id"] == 123
    assert mock_post.call_count == 1


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


@patch.object(PropertyV2Service, "_fetch_post")
def test_retrieve_with_geo_coordinates(
    mock_fetch_post: Mock, property_v2_service: PropertyV2Service, mock_response: Mock
) -> None:
    """Test retrieve method with geo coordinates."""
    mock_fetch_post.return_value = [mock_response.json()]

    df, metadata = property_v2_service.retrieve(
        geo_coordinates={"latitude": 40.7128, "longitude": -74.0060, "radius": 10.0},
        property_types=["CONDO"],
        min_price=500000,
        max_price=1000000,
    )

    # check that the dataframe has the expected data
    assert len(df) == 1
    assert df.iloc[0]["parcl_id"] == 123

    # check that the correct data was passed to _fetch_post
    call_args = mock_fetch_post.call_args[1]
    data = call_args["data"]
    assert data["geo_coordinates"] == {"latitude": 40.7128, "longitude": -74.0060, "radius": 10.0}
    assert data["property_filters"]["property_types"] == ["CONDO"]
    assert data["event_filters"]["min_price"] == 500000
    assert data["event_filters"]["max_price"] == 1000000


@patch.object(PropertyV2Service, "_fetch_post")
def test_retrieve_with_schema_validation_errors(
    mock_fetch_post: Mock,
    property_v2_service: PropertyV2Service,  # noqa: ARG001
) -> None:
    """Test that retrieve method properly validates input using schema."""
    # This should raise a validation error due to invalid property type
    with pytest.raises(ValueError, match="Invalid property type"):
        property_v2_service.retrieve(
            parcl_ids=[123],
            property_types=["INVALID_TYPE"],
        )

    # This should raise a validation error due to invalid range
    with pytest.raises(ValueError, match="max_beds cannot be less than min_beds"):
        property_v2_service.retrieve(
            parcl_ids=[123],
            min_beds=5,
            max_beds=3,
        )

    # This should raise a validation error due to invalid date format
    with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
        property_v2_service.retrieve(
            parcl_ids=[123],
            min_event_date="2023/01/01",
        )
