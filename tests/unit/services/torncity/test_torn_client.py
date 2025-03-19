import pytest
from app.services.torncity.client import TornClient, TornAPIKeyError, TornAPIRateLimitError, TornAPITimeoutError
import requests
from requests.exceptions import Timeout

@pytest.fixture
def torn_client():
    """Create a TornClient instance for testing."""
    return TornClient("abcd1234efgh5678")

def test_init_with_api_key():
    client = TornClient("abcd1234efgh5678")
    assert client.api_keys == {"default": "abcd1234efgh5678"}

def test_init_with_api_keys_file(tmp_path):
    api_keys_file = tmp_path / "api_keys.json"
    api_keys_file.write_text('{"default": "abcd1234efgh5678", "secondary": "ijkl9012mnop3456"}')
    client = TornClient(str(api_keys_file))
    assert client.api_keys == {"default": "abcd1234efgh5678", "secondary": "ijkl9012mnop3456"}

def test_init_with_malformed_api_keys_file(tmp_path):
    api_keys_file = tmp_path / "api_keys.json"
    api_keys_file.write_text('{"invalid": "test_key"}')
    with pytest.raises(TornAPIKeyError, match="API keys file must contain 'default' key"):
        TornClient(str(api_keys_file))

def test_init_with_nonexistent_file():
    with pytest.raises(TornAPIKeyError, match="API keys file not found"):
        TornClient("nonexistent.json")

def test_make_request(torn_client, mock_torn_api, monkeypatch):
    """Test making a request to the Torn API."""
    monkeypatch.setattr(torn_client.session, 'get', mock_torn_api['normal'])
    response = torn_client.make_request('user')
    assert response == {"error": None, "data": {"test": "data"}}

def test_rate_limit_handling(torn_client, mock_torn_api, monkeypatch):
    """Test handling of rate limit errors."""
    monkeypatch.setattr(torn_client.session, 'get', mock_torn_api['rate_limit'])
    with pytest.raises(TornAPIRateLimitError) as exc_info:
        torn_client.make_request('user')
    assert "Rate limit exceeded" in str(exc_info.value)

def test_timeout_handling(torn_client, mock_torn_api, monkeypatch):
    """Test handling of timeout errors."""
    monkeypatch.setattr(torn_client.session, 'get', mock_torn_api['timeout'])
    with pytest.raises(TornAPITimeoutError) as exc_info:
        torn_client.make_request('user')
    assert "Request timed out" in str(exc_info.value)

def test_concurrent_requests(torn_client, mock_torn_api, monkeypatch):
    """Test making concurrent requests to the Torn API."""
    monkeypatch.setattr(torn_client.session, 'get', mock_torn_api['normal'])
    responses = torn_client.make_concurrent_requests(['user', 'market'])
    assert all(response == {"error": None, "data": {"test": "data"}} for response in responses) 