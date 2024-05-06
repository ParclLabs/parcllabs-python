import pytest
from unittest.mock import Mock
from datetime import datetime
import requests_mock
from parcllabs import ParclLabsClient

from parcllabs.services.parcllabs_service import (
    ParclLabsService,
)  # Adjust the import based on your project structure


# Setup for tests
@pytest.fixture
def client_mock():
    return Mock()


@pytest.fixture
def service(client_mock):
    return ParclLabsService(client=client_mock)


import pytest
import requests


@pytest.fixture
def api_key():
    return "fake_api_key"


@pytest.fixture
def client(api_key):
    return ParclLabsClient(api_key=api_key)
