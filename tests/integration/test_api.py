import json
import os
from pathlib import Path

import pandas as pd
import pytest

from parcllabs import ParclLabsClient


def get_test_data_path(filename: str) -> Path:
    """Return the path to a test data file."""
    return Path(__file__).parent.parent / "data" / filename


# Load test data
with Path(get_test_data_path("test_pids.json")).open() as f:
    TEST_PIDS = json.load(f)

with Path(get_test_data_path("pricefeed_markets.json")).open() as f:
    PRICEFEED_MARKETS = json.load(f)

API_KEY = os.getenv("PARCL_LABS_API_KEY")


@pytest.fixture
def client() -> ParclLabsClient:
    return ParclLabsClient(api_key=API_KEY)


def test_singular_get_request_with_limit(client: ParclLabsClient) -> None:
    singular_pid = [5821868]
    test_limit = 12
    results = client.market_metrics.housing_event_prices.retrieve(
        parcl_ids=singular_pid, limit=test_limit, auto_paginate=False
    )
    assert results.shape[0] == test_limit
    assert results["parcl_id"].unique() == singular_pid[0]


def test_property_filter(client: ParclLabsClient) -> None:
    singular_pid = [5821868]
    test_limit = 12
    results = client.market_metrics.housing_event_prices.retrieve(
        parcl_ids=singular_pid,
        limit=test_limit,
        auto_paginate=False,
        property_type="single_family",
    )
    assert results["property_type"].unique() == "SINGLE_FAMILY"


def test_singular_get_request_with_pagination(client: ParclLabsClient) -> None:
    test_pid = [5826765]  # US parcl id, longest hist
    start_date = "2010-01-01"
    end_date = "2023-12-31"
    days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1

    results = client.price_feed.price_feed.retrieve(
        parcl_ids=test_pid,
        limit=1000,
        start_date=start_date,
        end_date=end_date,
        auto_paginate=True,
    )

    assert results.shape[0] == days
    assert results["date"].min().date() == pd.to_datetime(start_date).date()
    assert results["date"].max().date() == pd.to_datetime(end_date).date()


def test_multiple_get_requests(client: ParclLabsClient) -> None:
    start_date = "2010-01-01"
    end_date = "2023-12-31"
    limit = 1000
    test_pricefeed_markets = PRICEFEED_MARKETS[0:3]

    results = client.price_feed.price_feed.retrieve(
        parcl_ids=test_pricefeed_markets,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
    )

    assert results.shape[0] == len(test_pricefeed_markets) * limit


def test_multiple_get_requests_with_bad_parcl_id(client: ParclLabsClient) -> None:
    start_date = "2010-01-01"
    end_date = "2023-12-31"
    limit = 1000
    test_pricefeed_markets = PRICEFEED_MARKETS[0:3] + [123]

    results = client.price_feed.price_feed.retrieve(
        parcl_ids=test_pricefeed_markets,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
    )

    assert results.shape[0] == (len(test_pricefeed_markets) - 1) * limit


def test_multiple_get_requests_with_bad_parcl_id_and_auto_pagination(
    client: ParclLabsClient,
) -> None:
    start_date = "2010-01-01"
    end_date = "2023-12-31"
    days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1
    test_pricefeed_markets = PRICEFEED_MARKETS[0:3] + [123]

    results = client.price_feed.price_feed.retrieve(
        parcl_ids=test_pricefeed_markets,
        limit=1000,
        start_date=start_date,
        end_date=end_date,
        auto_paginate=True,
    )

    assert results.shape[0] == (len(test_pricefeed_markets) - 1) * days


def test_singular_post_request(client: ParclLabsClient) -> None:
    results = client.rental_market_metrics.gross_yield.retrieve(
        parcl_ids=[TEST_PIDS[100]],
        start_date="2023-01-01",
        end_date="2023-12-31",
        limit=52,
    )

    assert results["parcl_id"].unique() == TEST_PIDS[100]
    assert results.shape[0] == 12  # 12 months in a year


def test_multiple_post_requests(client: ParclLabsClient) -> None:
    results = client.rental_market_metrics.gross_yield.retrieve(
        parcl_ids=TEST_PIDS,
        start_date="2023-01-01",
        end_date="2023-12-31",
        auto_paginate=True,
    )

    assert results.shape[0] == len(TEST_PIDS) * 12
    assert results.groupby("parcl_id").size().unique() == 12


def test_multiple_post_requests_with_bad_parcl_ids(client: ParclLabsClient) -> None:
    bad_pids = list(range(1, 1001))
    results = client.rental_market_metrics.gross_yield.retrieve(
        parcl_ids=TEST_PIDS + bad_pids,
        start_date="2023-01-01",
        end_date="2023-12-31",
        auto_paginate=True,
    )

    assert results.shape[0] == len(TEST_PIDS) * 12
    assert results.groupby("parcl_id").size().unique() == 12
