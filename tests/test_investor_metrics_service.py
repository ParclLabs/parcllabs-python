import pytest
import pandas as pd
from unittest.mock import AsyncMock
from parcllabs import ParclLabsClient

import nest_asyncio

nest_asyncio.apply()

# Mock Data for testing
mock_response = {"parcl_id": 1, "items": [{"metric": 10}, {"metric": 20}], "links": {}}


@pytest.fixture
def client():
    client = ParclLabsClient(api_key="test_api_key")
    client.investor_metrics.housing_stock_ownership._fetch = AsyncMock(
        return_value=mock_response
    )
    client.investor_metrics.new_listings_for_sale_rolling_counts._fetch = AsyncMock(
        return_value=mock_response
    )
    client.investor_metrics.purchase_to_sale_ratio._fetch = AsyncMock(
        return_value=mock_response
    )
    client.investor_metrics.housing_event_counts._fetch = AsyncMock(
        return_value=mock_response
    )
    client.investor_metrics.housing_event_prices._fetch = AsyncMock(
        return_value=mock_response
    )
    return client


@pytest.mark.asyncio
async def test_investor_metrics_housing_stock_ownership_retrieve(client):
    result = client.investor_metrics.housing_stock_ownership.retrieve(parcl_ids=[1])
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


@pytest.mark.asyncio
async def test_investor_metrics_new_listings_for_sale_rolling_counts_retrieve(client):
    result = client.investor_metrics.new_listings_for_sale_rolling_counts.retrieve(
        parcl_ids=[1]
    )
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


@pytest.mark.asyncio
async def test_investor_metrics_purchase_to_sale_ratio_retrieve(client):
    result = client.investor_metrics.purchase_to_sale_ratio.retrieve(parcl_ids=[1])
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


@pytest.mark.asyncio
async def test_investor_metrics_housing_event_counts_retrieve(client):
    result = client.investor_metrics.housing_event_counts.retrieve(parcl_ids=[1])
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20


@pytest.mark.asyncio
async def test_investor_metrics_housing_event_prices_retrieve(client):
    result = client.investor_metrics.housing_event_prices.retrieve(parcl_ids=[1])
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "metric" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["metric"] == 10
    assert result.iloc[1]["metric"] == 20
