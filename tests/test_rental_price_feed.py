import pytest
from unittest.mock import Mock
from requests.exceptions import HTTPError

import pandas as pd

from parcllabs.services.price_feed import (
    PriceFeedBase,
)


# Mock client and service setup
@pytest.fixture
def mock_client():
    client = Mock()
    client.get.return_value = {"data": "mocked response"}
    return client


@pytest.fixture
def service(mock_client):
    return PriceFeedBase(
        client=mock_client, url="/v1/price_feed/{parcl_id}/rental_price_feed"
    )


@pytest.fixture
def api_response():
    return {
        "parcl_id": 101,
        "items": [
            {"date": "2024-05-14", "rental_price_feed": 392.66},
            {"date": "2024-05-13", "rental_price_feed": 391.86},
            {"date": "2024-05-12", "rental_price_feed": 391.27},
            {"date": "2024-05-11", "rental_price_feed": 390.92},
        ],
    }


def test_retrieve_success(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    result = service.retrieve(
        parcl_id=101,
        start_date="2023-05-11",
        end_date="2023-05-14",
    )
    assert result == api_response
    assert mock_client.get.called
    assert mock_client.get.call_args[1]["params"]["start_date"] == "2023-05-11"


def test_retrieve_as_dataframe(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    result = service.retrieve(
        parcl_id=101,
        start_date="2023-05-11",
        end_date="2023-05-14",
        as_dataframe=True,
    )
    assert isinstance(result, pd.DataFrame)
    assert "parcl_id" in result.columns
