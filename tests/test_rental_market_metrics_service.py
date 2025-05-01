from unittest.mock import Mock

import pytest

from parcllabs import ParclLabsClient

# Mock Data for testing
mock_response = {"parcl_id": 1, "items": [{"metric": 10}, {"metric": 20}], "links": {}}


@pytest.fixture
def client() -> ParclLabsClient:
    client = ParclLabsClient(api_key="test_api_key")
    client.rental_market_metrics.rental_units_concentration._fetch = Mock(
        return_value=mock_response
    )
    client.rental_market_metrics.gross_yield._fetch = Mock(return_value=mock_response)
    client.rental_market_metrics.new_listings_for_rent_rolling_counts._fetch = Mock(
        return_value=mock_response
    )
    return client


def test_rental_market_metrics_rental_units_concentration_retrieve(
    client: ParclLabsClient,
) -> None:
    result = client.rental_market_metrics.rental_units_concentration.retrieve(
        parcl_ids=[1]
    )
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


def test_rental_market_metrics_gross_yield_retrieve(
    client: ParclLabsClient,
) -> None:
    result = client.rental_market_metrics.gross_yield.retrieve(parcl_ids=[1])
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


def test_rental_market_metrics_new_listings_for_rent_rolling_counts_retrieve(
    client: ParclLabsClient,
) -> None:
    result = client.rental_market_metrics.new_listings_for_rent_rolling_counts.retrieve(
        parcl_ids=[1]
    )
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20
