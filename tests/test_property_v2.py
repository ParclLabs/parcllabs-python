import warnings
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from requests.exceptions import RequestException

from parcllabs import warnings as parcllabs_warnings
from parcllabs.common import PARCL_PROPERTY_IDS
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
    assert criteria == {PARCL_PROPERTY_IDS: [789, 101]}

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
        PARCL_PROPERTY_IDS: [456],
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
        has_pool=True,
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
        "has_pool": "true",
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


def test_set_limit_pagination_resolves_page_size_and_cap(
    property_v2_service: PropertyV2Service,
) -> None:
    max_limit = RequestLimits.PROPERTY_V2_MAX.value

    # No limit -> max page size, no cap (retrieve everything).
    assert property_v2_service._set_limit_pagination(limit=None) == (max_limit, None)

    # An explicit limit is a TOTAL CAP; page size matches it while under the ceiling.
    assert property_v2_service._set_limit_pagination(limit=100) == (100, 100)

    # A limit above the API ceiling is satisfied by paginating, not by erroring.
    assert property_v2_service._set_limit_pagination(limit=120_000) == (max_limit, 120_000)


def test_limit_zero_treated_as_unbounded_defensively(
    property_v2_service: PropertyV2Service,
) -> None:
    """0 falls back to 'no cap' in the helper, but callers cannot reach it."""
    assert property_v2_service._set_limit_pagination(limit=0) == (
        RequestLimits.PROPERTY_V2_MAX.value,
        None,
    )


@pytest.mark.parametrize("bad_limit", [0, -1, -5000])
def test_non_positive_limit_rejected_at_validation(bad_limit: int) -> None:
    """The schema rejects 0 and negatives, so a computed zero cannot trigger an
    unbounded pull."""
    with pytest.raises(ValueError, match="greater than or equal to 1"):
        PropertyV2RetrieveParams(limit=bad_limit)


def test_limit_above_api_ceiling_is_accepted() -> None:
    """Values above the per-request ceiling must validate; pagination satisfies them."""
    assert PropertyV2RetrieveParams(limit=120_000).limit == 120_000


def _page(
    parcl_property_id: int,
    *,
    total_available: int,
    returned_count: int = 1,
    limit: int = 1,
    offset: int = 0,
    has_more: bool = False,
) -> Mock:
    """Build a mock page response."""
    response = Mock()
    response.json.return_value = {
        "data": [{"parcl_property_id": parcl_property_id}],
        "metadata": {
            "results": {"total_available": total_available, "returned_count": returned_count}
        },
        "pagination": {"limit": limit, "offset": offset, "has_more": has_more},
        "account_info": {"credits_used": returned_count, "credits_remaining": 999},
    }
    return response


@pytest.fixture(autouse=True)
def _reset_truncation_flag() -> None:
    """Truncation warns once per session; reset between tests."""
    parcllabs_warnings._reset_truncation_warning()


@patch.object(PropertyV2Service, "_post")
def test_fetch_post_single_page(
    mock_post: Mock, property_v2_service: PropertyV2Service, mock_response: Mock
) -> None:
    mock_post.return_value = mock_response
    result = property_v2_service._fetch_post(params={"limit": 100}, data={})

    assert len(result) == 1
    assert result[0] == mock_response.json()
    mock_post.assert_called_once()


@patch.object(PropertyV2Service, "_post")
def test_fetch_post_pagination(mock_post: Mock, property_v2_service: PropertyV2Service) -> None:
    """Unbounded fetch must actually walk every page and merge the results."""
    mock_post.side_effect = [
        _page(123, total_available=2, limit=1, offset=0, has_more=True),
        _page(456, total_available=2, limit=1, offset=1, has_more=False),
    ]

    result = property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=None)

    assert mock_post.call_count == 2
    assert [page["data"][0]["parcl_property_id"] for page in result] == [123, 456]


@patch.object(PropertyV2Service, "_post")
def test_fetch_post_stops_at_max_results(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    """An explicit cap must not keep paginating toward total_available."""
    mock_post.return_value = _page(123, total_available=100, limit=1, offset=0, has_more=True)

    result = property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=1)

    assert mock_post.call_count == 1
    assert len(result) == 1


@patch.object(PropertyV2Service, "_post")
def test_fetch_post_trims_final_page_to_cap(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    """A cap that is not a multiple of the page size must not overshoot."""
    mock_post.side_effect = [
        _page(1, total_available=100, returned_count=10, limit=10, offset=0, has_more=True),
        _page(2, total_available=100, returned_count=5, limit=5, offset=10, has_more=True),
    ]

    property_v2_service._fetch_post(params={"limit": 10}, data={}, max_results=15)

    assert mock_post.call_count == 2
    # Second call asks for only the outstanding 5, at the correct offset.
    second_params = mock_post.call_args_list[1][1]["params"]
    assert second_params["limit"] == 5
    assert second_params["offset"] == 10


@patch.object(PropertyV2Service, "_post")
def test_truncation_warning_fires_once_per_session(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    mock_post.return_value = _page(1, total_available=108_288, limit=1, offset=0, has_more=True)

    with pytest.warns(parcllabs_warnings.ParclLabsTruncationWarning, match="108,288"):
        property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=1)

    # Second identical call is silent -- once per session.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=1)
    assert not [w for w in caught if w.category is parcllabs_warnings.ParclLabsTruncationWarning]


@patch.object(PropertyV2Service, "_post")
def test_no_truncation_warning_when_complete(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    mock_post.return_value = _page(1, total_available=1, limit=1, offset=0, has_more=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=1)

    assert not [w for w in caught if w.category is parcllabs_warnings.ParclLabsTruncationWarning]


@patch.object(PropertyV2Service, "_post")
def test_page_retry_recovers_from_transient_failure(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    mock_post.side_effect = [
        _page(1, total_available=2, limit=1, offset=0, has_more=True),
        RequestException("boom"),
        _page(2, total_available=2, limit=1, offset=1, has_more=False),
    ]

    with patch("parcllabs.services.properties.property_v2.time.sleep"):
        result = property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=None)

    assert mock_post.call_count == 3
    assert len(result) == 2


@patch.object(PropertyV2Service, "_post")
def test_incomplete_pages_warn_every_call(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    """Exhausted retries must warn on EVERY call and record the failed offsets."""
    first = _page(1, total_available=2, limit=1, offset=0, has_more=True)

    def responses() -> list[object]:
        return [first, *[RequestException("boom")] * 3]

    for _ in range(2):
        mock_post.reset_mock()
        mock_post.side_effect = responses()
        with (
            patch("parcllabs.services.properties.property_v2.time.sleep"),
            pytest.warns(parcllabs_warnings.ParclLabsIncompleteResultWarning),
        ):
            result = property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=None)
        assert result[0]["_parcllabs"]["incomplete_pages"] == [1]


@patch.object(PropertyV2Service, "_post")
def test_failed_pages_suppress_contradictory_truncation_warning(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    """A capped request whose pages fail must not also claim the cap was met.

    Regression: _fetch_post emitted the incomplete warning with the real retrieved
    count and *then* warned truncation with `target`, claiming the full cap was
    returned. The two messages contradicted each other, and the truncation notice --
    which fires only once per session -- was burned on the misleading one.
    """
    first = _page(1, total_available=108_295, limit=1, offset=0, has_more=True)
    mock_post.side_effect = [first, *[RequestException("boom")] * 3]

    with (
        patch("parcllabs.services.properties.property_v2.time.sleep"),
        warnings.catch_warnings(record=True) as caught,
    ):
        warnings.simplefilter("always")
        property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=2)

    incomplete = [
        w for w in caught if w.category is parcllabs_warnings.ParclLabsIncompleteResultWarning
    ]
    truncation = [w for w in caught if w.category is parcllabs_warnings.ParclLabsTruncationWarning]

    assert len(incomplete) == 1
    assert "1 of an expected 2" in str(incomplete[0].message)
    # Capping information is preserved in the incomplete message instead.
    assert "108,295" in str(incomplete[0].message)
    assert not truncation, "truncation warning must not contradict the incomplete warning"

    # The once-per-session budget must be intact for a later, legitimate truncation.
    assert parcllabs_warnings._truncation_warned is False


@patch.object(PropertyV2Service, "_post")
def test_truncation_reports_actual_not_requested_count(
    mock_post: Mock, property_v2_service: PropertyV2Service
) -> None:
    """The truncation message must state what was returned, not what was asked for."""
    mock_post.return_value = _page(1, total_available=500, limit=1, offset=0, has_more=True)

    with pytest.warns(parcllabs_warnings.ParclLabsTruncationWarning) as caught:
        property_v2_service._fetch_post(params={"limit": 1}, data={}, max_results=1)

    assert "Returned 1 of 500" in str(caught[0].message)


def test_incomplete_pages_surfaced_in_metadata(property_v2_service: PropertyV2Service) -> None:
    results = [
        {
            "metadata": {"results": {"returned_count": 1, "total_available": 2}},
            "_parcllabs": {"incomplete_pages": [1]},
        }
    ]
    assert property_v2_service._get_metadata(results)["incomplete_pages"] == [1]


def test_get_metadata_does_not_mutate_response(property_v2_service: PropertyV2Service) -> None:
    """A shallow copy would rewrite returned_count on the caller's raw page."""
    results = [
        {"metadata": {"results": {"returned_count": 2, "total_available": 5}}},
        {"metadata": {"results": {"returned_count": 3, "total_available": 5}}},
    ]

    metadata = property_v2_service._get_metadata(results)

    assert metadata["results"]["returned_count"] == 5
    assert results[0]["metadata"]["results"]["returned_count"] == 2


def test_integrity_check_warns_on_property_count_mismatch(
    property_v2_service: PropertyV2Service,
) -> None:
    # Two rows but only one distinct property, against a reported count of 2.
    final_df = pd.DataFrame({"parcl_property_id": [1, 1]})
    metadata = {"results": {"returned_count": 2, "total_available": 2}}

    with pytest.warns(parcllabs_warnings.ParclLabsIncompleteResultWarning, match="integrity"):
        property_v2_service._check_pagination_integrity(final_df, metadata)


def test_integrity_check_silent_when_counts_agree(
    property_v2_service: PropertyV2Service,
) -> None:
    final_df = pd.DataFrame({"parcl_property_id": [1, 1, 2]})
    metadata = {"results": {"returned_count": 2, "total_available": 2}}

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        property_v2_service._check_pagination_integrity(final_df, metadata)

    assert not caught


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

    # `limit` is the page size here; `auto_paginate` must NOT leak into the query
    # string (it previously did, and was asserted as correct).
    call_args = mock_fetch_post.call_args[1]
    assert call_args["params"] == {"limit": 10}
    assert "auto_paginate" not in call_args["params"]
    assert call_args["max_results"] == 10

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
    mock_fetch_post: Mock,  # noqa: ARG001
    property_v2_service: PropertyV2Service,
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


def test_build_boolean_filters_has_pool(property_v2_service: PropertyV2Service) -> None:
    """Test has_pool boolean filter."""
    # True
    params = PropertyV2RetrieveParams(has_pool=True)
    filters = property_v2_service._build_boolean_filters(params)
    assert filters["has_pool"] == "true"

    # False
    params = PropertyV2RetrieveParams(has_pool=False)
    filters = property_v2_service._build_boolean_filters(params)
    assert filters["has_pool"] == "false"

    # None (omitted)
    params = PropertyV2RetrieveParams()
    filters = property_v2_service._build_boolean_filters(params)
    assert "has_pool" not in filters
