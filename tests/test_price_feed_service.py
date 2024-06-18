import pytest
from unittest.mock import AsyncMock
from parcllabs import ParclLabsClient

import nest_asyncio

nest_asyncio.apply()

# Mock Data for testing
mock_response = {"parcl_id": 1, "items": [{"price": 100}, {"price": 200}], "links": {}}


@pytest.fixture
def client():
    client = ParclLabsClient(api_key="test_api_key")
    client.price_feed.price_feed._fetch = AsyncMock(return_value=mock_response)
    client.price_feed.volatility._fetch = AsyncMock(return_value=mock_response)
    client.price_feed.rental_price_feed._fetch = AsyncMock(return_value=mock_response)
    return client


@pytest.mark.asyncio
async def test_price_feed_retrieve(client):
    # Call the retrieve method without await
    result = client.price_feed.price_feed.retrieve(parcl_ids=[1])
    print(result)
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "price" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["price"] == 100
    assert result.iloc[1]["price"] == 200


@pytest.mark.asyncio
async def test_price_feed_volatility_retrieve(client):
    # Call the retrieve method without await
    result = client.price_feed.volatility.retrieve(parcl_ids=[1])
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "price" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["price"] == 100
    assert result.iloc[1]["price"] == 200


@pytest.mark.asyncio
async def test_rental_price_feed_retrieve(client):
    # Call the retrieve method without await
    result = client.price_feed.rental_price_feed.retrieve(parcl_ids=[1])
    assert not result.empty
    assert "parcl_id" in result.columns
    assert "price" in result.columns
    assert len(result) == 2
    assert result.iloc[0]["price"] == 100
    assert result.iloc[1]["price"] == 200
