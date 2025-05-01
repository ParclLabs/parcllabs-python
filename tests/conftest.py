from unittest.mock import Mock

import pytest

from parcllabs import ParclLabsClient
from parcllabs.services.parcllabs_service import (
    ParclLabsService,
)  # Adjust the import based on your project structure


# Setup for tests
@pytest.fixture
def client_mock() -> Mock:
    return Mock()


@pytest.fixture
def service(client_mock: Mock) -> ParclLabsService:
    return ParclLabsService(url="http://example.com/{parcl_id}", client=client_mock)


@pytest.fixture
def api_key() -> str:
    return "fake_api_key"


@pytest.fixture
def client(api_key: str) -> ParclLabsClient:
    return ParclLabsClient(api_key=api_key)
