import pytest
import platform
import parcllabs
from unittest.mock import Mock, patch
from parcllabs.services.parcllabs_service import ParclLabsService


class MockClient:
    api_url = "https://api.example.com/"
    api_key = "test_api_key"


@pytest.fixture
def parcl_labs_service():
    client = MockClient()
    return ParclLabsService(
        url="https://api.example.com/{parcl_id}", client=client, limit=10
    )


def test_get_headers(parcl_labs_service):
    headers = parcl_labs_service._get_headers()
    assert headers == {
            "Authorization": "test_api_key",
            "Content-Type": "application/json",
            "X-Parcl-Labs-Python-Client-Version": f"{parcllabs.__version__}",
            "X-Parcl-Labs-Python-Client-Platform": f"{platform.system()}",
            "X-Parcl-Labs-Python-Client-Platform-Version": f"{platform.python_version()}"
        }


@patch("parcllabs.services.parcllabs_service.requests.get")
def test_sync_request(mock_get, parcl_labs_service):
    mock_response = Mock()
    mock_response.json.return_value = {"key": "value"}
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    url = "https://api.example.com/123"
    params = {"limit": 10}
    response = parcl_labs_service._sync_request(parcl_id=123, params=params)
    assert response == {"key": "value"}


def test_as_pd_dataframe(parcl_labs_service):
    data = [
        {
            "items": [{"field1": "value1"}, {"field1": "value2"}],
            "meta_field": "meta_value",
        }
    ]
    df = parcl_labs_service._as_pd_dataframe(data)
    assert not df.empty
    assert "field1" in df.columns
    assert df.iloc[0]["field1"] == "value1"
    assert df.iloc[1]["field1"] == "value2"
