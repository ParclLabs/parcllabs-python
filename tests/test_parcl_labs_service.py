def test_request_with_default_parameters(service, client_mock):
    client_mock.get.return_value = "mock_response"
    response = service._request(url="http://example.com", is_next=False)
    client_mock.get.assert_called_once_with(
        url="http://example.com", params=None, is_next=False
    )
    assert response == "mock_response"


def test_request_with_custom_parameters(service, client_mock):
    params = {"key": "value"}
    client_mock.get.return_value = "mock_response"
    response = service._request(url="http://example.com", params=params, is_next=False)
    client_mock.get.assert_called_once_with(
        url="http://example.com", params=params, is_next=False
    )
    assert response == "mock_response"
