import pytest
from app.services.torncity.client import TornClient, TornAPIKeyError, TornAPIRateLimitError, TornAPITimeoutError
import requests

def test_init_with_api_key():
    client = TornClient("test_key")
    assert client.api_keys == {"default": "test_key"}

def test_init_with_api_keys_file(tmp_path):
    api_keys_file = tmp_path / "api_keys.json"
    api_keys_file.write_text('{"default": "test_key_1", "secondary": "test_key_2"}')
    client = TornClient(str(api_keys_file))
    assert client.api_keys == {"default": "test_key_1", "secondary": "test_key_2"}

def test_init_with_malformed_api_keys_file(tmp_path):
    api_keys_file = tmp_path / "api_keys.json"
    api_keys_file.write_text('{"invalid": "test_key"}')
    with pytest.raises(TornAPIKeyError, match="API keys file must contain 'default' key"):
        TornClient(str(api_keys_file))

def test_init_with_nonexistent_file():
    with pytest.raises(TornAPIKeyError, match="API keys file not found"):
        TornClient("nonexistent.json")

def test_make_request(mock_torn_api):
    client = TornClient("test_key")
    response = client.make_request("test_endpoint")
    assert response == {"status": True, "data": {"test": "data"}}

def test_rate_limit_handling(mock_torn_api, monkeypatch):
    monkeypatch.setattr(requests, "get", mock_torn_api["rate_limit"])
    client = TornClient("test_key")
    with pytest.raises(TornAPIRateLimitError, match="Rate limit exceeded for key default"):
        client.make_request("test_endpoint")

def test_timeout_handling(mock_torn_api, monkeypatch):
    monkeypatch.setattr(requests, "get", mock_torn_api["timeout"])
    client = TornClient("test_key")
    with pytest.raises(TornAPITimeoutError, match="Request timed out"):
        client.make_request("test_endpoint")

def test_concurrent_requests(mock_torn_api):
    client = TornClient("test_key")
    responses = client.make_concurrent_requests(["endpoint1", "endpoint2"])
    assert all(response == {"status": True, "data": {"test": "data"}} for response in responses) 