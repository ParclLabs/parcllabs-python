import pytest
import requests
from requests.exceptions import RequestException
import requests_mock
from parcllabs import ParclLabsClient


def test_initialization(api_key):
    client = ParclLabsClient(api_key)
    assert client.api_key == api_key


def test_initialization_without_api_key():
    with pytest.raises(ValueError):
        ParclLabsClient(api_key=None)


def test_get_successful(client):
    with requests_mock.Mocker() as m:
        m.get("https://example.com/api/data", json={"data": "value"}, status_code=200)
        client.api_url = "https://example.com/api"
        response = client.get("/data")
        assert response == {"data": "value"}


def test_get_with_failed_request(client):
    with requests_mock.Mocker() as m:
        m.get(
            "https://example.com/api/data",
            status_code=400,
            json={"detail": "Client Error"},
        )
        client.api_url = "https://example.com/api"

        # Use pytest.raises to check for the RequestException
        with pytest.raises(RequestException) as excinfo:
            response = client.get("/data")

        # Assert that the exception message contains '400 Client Error'
        assert "400 Client Error" in str(excinfo.value)
        assert "Client Error" in str(excinfo.value)


def test_get_headers(client):
    with requests_mock.Mocker() as m:
        m.get(
            "https://example.com/api/data",
            json={},
            request_headers={
                "Authorization": "fake_api_key",
                "Content-Type": "application/json",
            },
        )
        client.api_url = "https://example.com/api"
        client.get("/data")
        history = m.request_history[0]
        assert history.headers["Authorization"] == "fake_api_key"
        assert history.headers["Content-Type"] == "application/json"


def test_service_initialization(client):
    assert client.investor_metrics_housing_stock_ownership.client is client
    assert client.investor_metrics_new_listings_for_sale_rolling_counts.client is client
    assert client.investor_metrics_purchase_to_sale_ratio.client is client
    assert client.investor_metrics_housing_event_counts.client is client
    assert client.market_metrics_housing_event_prices.client is client
    assert client.market_metrics_housing_stock.client is client
    assert client.market_metrics_housing_event_counts.client is client
    assert client.for_sale_market_metrics_new_listings_rolling_counts.client is client
    assert client.rental_market_metrics_rental_units_concentration.client is client
    assert client.rental_market_metrics_gross_yield.client is client
