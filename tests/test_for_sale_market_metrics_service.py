from unittest.mock import Mock

import pytest

from parcllabs import ParclLabsClient

# Mock Data for testing
mock_response = {"parcl_id": 1, "items": [{"metric": 10}, {"metric": 20}], "links": {}}


@pytest.fixture
def client() -> ParclLabsClient:
    client = ParclLabsClient(api_key="test_api_key")
    client.for_sale_market_metrics.new_listings_rolling_counts._fetch = Mock(
        return_value=mock_response
    )
    client.for_sale_market_metrics.for_sale_inventory._fetch = Mock(
        return_value=mock_response
    )
    client.for_sale_market_metrics.for_sale_inventory_price_changes._fetch = Mock(
        return_value=mock_response
    )
    return client


def test_for_sale_market_metrics_new_listings_rolling_counts_retrieve(
    client: ParclLabsClient,
) -> None:
    result = client.for_sale_market_metrics.new_listings_rolling_counts.retrieve(
        parcl_ids=[1]
    )
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


def test_for_sale_market_metrics_for_sale_inventory_retrieve(
    client: ParclLabsClient,
) -> None:
    result = client.for_sale_market_metrics.for_sale_inventory.retrieve(parcl_ids=[1])
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


def test_for_sale_market_metrics_for_sale_inventory_price_changes_retrieve(
    client: ParclLabsClient,
) -> None:
    result = client.for_sale_market_metrics.for_sale_inventory_price_changes.retrieve(
        parcl_ids=[1]
    )
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20
