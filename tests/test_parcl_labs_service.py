import pytest
import platform
import parcllabs
from parcllabs.services.parcllabs_service import ParclLabsService


class MockClient:
    def __init__(self):
        self.api_url = "https://api.example.com/"
        self.api_key = "test_api_key"
        self.account_info = {"est_session_credits_used": 0}


@pytest.fixture
def parcl_labs_service():
    client = MockClient()
    return ParclLabsService(url="https://api.example.com/{parcl_id}", client=client)


def test_get_headers(parcl_labs_service):
    headers = parcl_labs_service._get_headers()
    assert headers == {
        "Authorization": "test_api_key",
        "Content-Type": "application/json",
        "X-Parcl-Labs-Python-Client-Version": f"{parcllabs.__version__}",
        "X-Parcl-Labs-Python-Client-Platform": f"{platform.system()}",
        "X-Parcl-Labs-Python-Client-Platform-Version": f"{platform.python_version()}",
    }


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


def test_update_account_info(parcl_labs_service):
    data = {"est_credits_used": 1, "est_remaining_credits": 9999}
    parcl_labs_service._update_account_info(data)
    assert parcl_labs_service.client.account_info["est_session_credits_used"] == 1
    assert parcl_labs_service.client.account_info["est_remaining_credits"] == 9999