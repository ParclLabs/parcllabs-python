import pandas as pd
import pytest
from unittest.mock import Mock
from requests.exceptions import HTTPError
from parcllabs.services.parcllabs_service import (
    ParclLabsService,
)


# Mock client and service setup
@pytest.fixture
def mock_client():
    client = Mock()
    client.get.return_value = {"data": "mocked response"}
    return client


@pytest.fixture
def service(mock_client):
    return ParclLabsService(
        url="/v1/portfolio_metrics/{parcl_id}/sf_new_listings_for_rent_rolling_counts",
        client=mock_client,
    )


@pytest.fixture
def api_response():
    return {
        "parcl_id": 101,
        "items": [
            {
                "date": "2024-05-01",
                "count": {
                    "rolling_7_day": 11,
                    "rolling_30_day": 48,
                    "rolling_60_day": 82,
                    "rolling_90_day": 144,
                },
                "pct_sf_for_rent_market": {
                    "rolling_7_day": 12.22,
                    "rolling_30_day": 14.5,
                    "rolling_60_day": 12.75,
                    "rolling_90_day": 14.37,
                },
            },
        ],
    }


def test_retrieve_success(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    result = service.retrieve(
        parcl_id=101,
        start_date="2024-04-28",
        end_date="2024-05-02",
        portfolio_size="PORTFOLIO_2_TO_9",
    )
    assert result == api_response
    assert mock_client.get.called
    assert mock_client.get.call_args[1]["params"]["start_date"] == "2024-04-28"


def test_retrieve_as_dataframe(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    result = service.retrieve(
        parcl_id=101,
        start_date="2024-04-28",
        end_date="2024-05-02",
        portfolio_size="PORTFOLIO_2_TO_9",
        as_dataframe=True,
    )
    assert isinstance(result, pd.DataFrame)
    assert "parcl_id" in result.columns
    assert "portfolio_size" in result.columns
    assert "count_rolling_30_day" in result.columns


def test_retrieve_many_success(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    parcl_ids = [101, 102]
    result = service.retrieve_many(
        parcl_ids=parcl_ids,
        start_date="2024-04-28",
        end_date="2024-05-02",
        portfolio_size="PORTFOLIO_2_TO_9",
    )
    assert all(pid in result for pid in parcl_ids)
    assert mock_client.get.call_count == len(parcl_ids)


def test_retrieve_many_as_dataframe(service, mock_client, api_response):
    mock_client.get.return_value = api_response
    parcl_ids = [101, 102]
    result = service.retrieve_many(
        parcl_ids=parcl_ids,
        start_date="2024-04-28",
        end_date="2024-05-02",
        portfolio_size="PORTFOLIO_2_TO_9",
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
            start_date="2024-04-01",
            end_date="2024-05-02",
            portfolio_size="PORTFOLIO_2_TO_9",
        )
