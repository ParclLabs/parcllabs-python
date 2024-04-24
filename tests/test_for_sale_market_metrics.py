import pytest
from unittest.mock import Mock
from requests.exceptions import HTTPError

import pandas as pd

from parcllabs.services.for_sale_market_metrics import (
    ForSaleMarketMetricsNewListingsRollingCounts,
)


# Mock client and service setup
@pytest.fixture
def mock_client():
    client = Mock()
    client.get.return_value = {"data": "mocked response"}
    return client


@pytest.fixture
def service(mock_client):
    return ForSaleMarketMetricsNewListingsRollingCounts(client=mock_client)


@pytest.fixture
def api_response():
    return {
        "parcl_id": 101,
        "items": [
            {"date": "2023-01-01", "count": 10},
            {"date": "2023-01-08", "count": 15},
        ],
    }


def test_retrieve_success(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    result = service.retrieve(
        parcl_id=101,
        start_date="2023-01-01",
        end_date="2023-01-08",
        property_type="condo",
    )
    assert result == api_response
    assert mock_client.get.called
    assert mock_client.get.call_args[1]["params"]["start_date"] == "2023-01-01"


def test_retrieve_as_dataframe(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    result = service.retrieve(
        parcl_id=101,
        start_date="2023-01-01",
        end_date="2023-01-08",
        property_type="condo",
        as_dataframe=True,
    )
    assert isinstance(result, pd.DataFrame)
    assert "parcl_id" in result.columns


def test_retrieve_many_success(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    parcl_ids = [101, 102]
    result = service.retrieve_many(
        parcl_ids=parcl_ids,
        start_date="2023-01-01",
        end_date="2023-01-08",
        property_type="condo",
    )
    assert all(pid in result for pid in parcl_ids)
    assert mock_client.get.call_count == len(parcl_ids)


def test_retrieve_many_as_dataframe(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    parcl_ids = [101, 102]
    result = service.retrieve_many(
        parcl_ids=parcl_ids,
        start_date="2023-01-01",
        end_date="2023-01-08",
        property_type="condo",
        as_dataframe=True,
    )
    assert isinstance(result, pd.DataFrame)
    assert "parcl_id" in result.columns
    assert len(result["parcl_id"].unique()) == len(parcl_ids)


def test_retrieve_api_error(service, mock_client):
    mock_client.get.side_effect = HTTPError("API error")
    with pytest.raises(HTTPError):
        service.retrieve(
            parcl_id=101,
            start_date="2023-01-01",
            end_date="2023-01-08",
            property_type="condo",
        )
