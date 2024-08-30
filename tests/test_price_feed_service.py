import pytest
import pandas as pd
import requests
import json
from unittest.mock import Mock, patch
from parcllabs.services.parcllabs_service import (
    ParclLabsService,
    ParclLabsStreamingService,
)
from parcllabs.exceptions import NotFoundError
from requests.exceptions import RequestException


class TestParclLabsService:

    @pytest.fixture
    def service(self):
        mock_client = Mock()
        mock_client.api_url = "https://api.example.com"
        mock_client.api_key = "test_api_key"
        mock_client.turbo_mode = False
        mock_client.estimated_session_credit_usage = 0
        return ParclLabsService("/test", mock_client)

    def test_init(self, service):
        assert service.url == "/test"
        assert service.api_url == "https://api.example.com"
        assert service.full_url == "https://api.example.com/test"
        assert service.api_key == "test_api_key"

    def test_init_missing_client(self):
        with pytest.raises(ValueError, match="Missing required client object."):
            ParclLabsService("/test", None)

    def test_get_headers(self, service):
        headers = service._get_headers()
        assert "Authorization" in headers
        assert "Content-Type" in headers
        assert "X-Parcl-Labs-Python-Client-Version" in headers

    def test_clean_params(self, service):
        params = {"a": 1, "b": None, "c": "test"}
        cleaned = service._clean_params(params)
        assert cleaned == {"a": 1, "c": "test"}

    @patch("requests.request")
    def test_make_request_get(self, mock_request, service):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        service._make_request("GET", "https://api.example.com/test")
        mock_request.assert_called_once_with(
            "GET",
            "https://api.example.com/test",
            headers=service.headers,
            params={},
        )

    @patch("requests.request")
    def test_make_request_post(self, mock_request, service):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        service._make_request(
            "POST", "https://api.example.com/test", json={"data": "test"}
        )
        mock_request.assert_called_once_with(
            "POST",
            "https://api.example.com/test",
            headers=service.headers,
            json={"data": "test"},
        )

    @patch("requests.request")
    def test_make_request_http_error(self, mock_request, service):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_response.status_code = 400  # Set a specific status code
        mock_response.json.return_value = {"detail": "Bad Request"}
        mock_request.return_value = mock_response

        with pytest.raises(RequestException):
            service._make_request("GET", "https://api.example.com/test")

    def test_post(self, service):
        with patch.object(service, "_make_request") as mock_make_request:
            service._post("https://api.example.com/test", {"data": "test"})
            mock_make_request.assert_called_once_with(
                "POST", "https://api.example.com/test", json={"data": "test"}
            )

    def test_get(self, service):
        with patch.object(service, "_make_request") as mock_make_request:
            service._get("https://api.example.com/test", {"param": "test"})
            mock_make_request.assert_called_once_with(
                "GET", "https://api.example.com/test", params={"param": "test"}
            )

    def test_fetch_get(self, service):
        with patch.object(service, "_fetch_get") as mock_fetch_get:
            service._fetch([1], {"param": "test"})
            mock_fetch_get.assert_called_once()

    def test_fetch_post(self, service):
        service.client.turbo_mode = True
        service.full_post_url = "https://api.example.com/test_post"
        with patch.object(service, "_fetch_post") as mock_fetch_post:
            service._fetch([1, 2], {"param": "test"})
            mock_fetch_post.assert_called_once()

    def test_fetch_get_many_parcl_ids(self, service):
        with patch.object(service, "_fetch_get") as mock_fetch_get:
            service._fetch_get_many_parcl_ids([1, 2], {"param": "test"}, False)
            assert mock_fetch_get.call_count == 2

    @patch('parcllabs.services.parcllabs_service.ParclLabsService._post')
    def test_process_and_paginate_response_post(self, mock_post, service):
        mock_response = Mock()
        mock_response.json.return_value = {
            "items": [1, 2],
            "links": {"next": "https://api.example.com/next"},
        }
        mock_response.status_code = 200

        mock_next_response = Mock()
        mock_next_response.json.return_value = {"items": [3, 4], "links": {}}
        mock_next_response.status_code = 200

        mock_post.return_value = mock_next_response

        result = service._process_and_paginate_response(mock_response, True, {}, "post")
        assert result["items"] == [1, 2, 3, 4]
        assert service.client.estimated_session_credit_usage == 4
        mock_post.assert_called_once_with("https://api.example.com/next", data={})

    @patch('parcllabs.services.parcllabs_service.ParclLabsService._get')
    def test_process_and_paginate_response_get(self, mock_get, service):
        mock_response = Mock()
        mock_response.json.return_value = {
            "items": [1, 2],
            "links": {"next": "https://api.example.com/next"},
        }
        mock_response.status_code = 200

        mock_next_response = Mock()
        mock_next_response.json.return_value = {"items": [3, 4], "links": {}}
        mock_next_response.status_code = 200

        mock_get.return_value = mock_next_response

        result = service._process_and_paginate_response(mock_response, True, {}, "get")
        assert result["items"] == [1, 2, 3, 4]
        assert service.client.estimated_session_credit_usage == 4
        mock_get.assert_called_once_with("https://api.example.com/next", params={})

    def test_retrieve(self, service):
        with patch.object(service, "_fetch") as mock_fetch:
            mock_fetch.return_value = {"items": [{"id": 1}, {"id": 2}]}
            result = service.retrieve([1, 2])
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2

    def test_sanitize_output(self, service):
        data = {"keep": "value", "delete": "value"}
        with patch(
            "parcllabs.services.parcllabs_service.DELETE_FROM_OUTPUT", ["delete"]
        ):
            result = service.sanitize_output(data)
            assert "keep" in result
            assert "delete" not in result

    def test_as_pd_dataframe(self, service):
        data = [{"items": [{"id": 1}, {"id": 2}], "meta": "data"}]
        result = service._as_pd_dataframe(data)
        assert isinstance(result, pd.DataFrame)
        assert "id" in result.columns
        assert "meta" in result.columns

    def test_error_handling_403(self, service):
        response = Mock()
        response.status_code = 403
        response.json.return_value = {"detail": "Forbidden"}
        with pytest.raises(RequestException, match="403 Client Error"):
            service.error_handling(response)

    def test_error_handling_404(self, service):
        response = Mock()
        response.status_code = 404
        with pytest.raises(NotFoundError):
            service.error_handling(response)

    def test_error_handling_422(self, service):
        response = Mock()
        response.status_code = 422
        response.json.return_value = {
            "detail": [{'msg': 'Invalid input'}],
        }
        with pytest.raises(RequestException, match="422 Client Error"):
            service.error_handling(response)

    def test_error_handling_429(self, service):
        response = Mock()
        response.status_code = 429
        response.json.return_value = {"error": "Rate Limit Exceeded"}
        with pytest.raises(RequestException, match="429 Client Error"):
            service.error_handling(response)


class TestParclLabsStreamingService:

    @pytest.fixture
    def streaming_service(self):
        mock_client = Mock()
        mock_client.api_url = "https://api.example.com"
        mock_client.api_key = "test_api_key"
        return ParclLabsStreamingService("/test", mock_client)

    def test_convert_text_to_json_valid(self, streaming_service):
        json_str = '{"key": "value"}'
        result = streaming_service._convert_text_to_json(json_str)
        assert result == {"key": "value"}

    def test_convert_text_to_json_invalid(self, streaming_service):
        json_str = "invalid json"
        result = streaming_service._convert_text_to_json(json_str)
        assert result is None

    def test_process_streaming_data(self, streaming_service):
        data = '{"id": 1}\n{"id": 2}\n{"id": 3}'
        result = list(streaming_service._process_streaming_data(data, batch_size=2))
        assert len(result) == 2
        assert len(result[0]) == 2
        assert len(result[1]) == 1
