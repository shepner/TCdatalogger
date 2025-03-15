"""Unit tests for Torn City API client."""

import json
from unittest.mock import Mock, patch, call
from datetime import datetime, timedelta

import pytest
import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
from tenacity import RetryError

from app.services.torncity.client import (
    TornClient,
    TornAPIError,
    TornAPIKeyError,
    TornAPIRateLimitError,
    TornAPIConnectionError,
    TornAPITimeoutError
)

class TestTornClient:
    """Test suite for TornClient."""

    @pytest.fixture
    def client(self, mock_api_keys):
        """Create a TornClient instance for testing."""
        return TornClient(mock_api_keys)

    @pytest.fixture
    def mock_response(self):
        """Create a mock response object."""
        mock = Mock()
        mock.status_code = 200
        mock.json.return_value = {"status": True, "data": {"test": "data"}}
        return mock

    def test_init_with_valid_api_keys(self, mock_api_keys):
        """Test client initialization with valid API keys."""
        client = TornClient(mock_api_keys)
        assert client.api_keys == {"default": "test_key_1", "secondary": "test_key_2"}
        assert hasattr(client, "session")

    def test_init_with_invalid_api_keys_file(self, tmp_path):
        """Test client initialization with invalid API keys file."""
        invalid_file = tmp_path / "nonexistent.json"
        with pytest.raises(TornAPIKeyError, match="API key file not found"):
            TornClient(str(invalid_file))

    def test_init_with_malformed_api_keys_file(self, tmp_path):
        """Test client initialization with malformed API keys file."""
        bad_file = tmp_path / "bad_keys.json"
        with open(bad_file, "w") as f:
            f.write("not json")
        
        with pytest.raises(TornAPIKeyError, match="Invalid JSON"):
            TornClient(str(bad_file))

    @patch("requests.Session.get")
    def test_fetch_data_success(self, mock_get, client):
        """Test successful data fetch."""
        mock_response = Mock()
        mock_response.json.return_value = {"success": True, "data": {"id": 1}}
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        data = client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")
        assert data == {"success": True, "data": {"id": 1}}
        mock_get.assert_called_once()

    @patch("requests.Session.get")
    def test_fetch_data_invalid_key(self, mock_get, client):
        """Test fetch with invalid API key."""
        with pytest.raises(TornAPIKeyError, match="API key not found"):
            client.fetch_data("https://api.torn.com/user/{API_KEY}", "nonexistent")

    @patch("requests.Session.get")
    def test_fetch_data_rate_limit(self, mock_get, client):
        """Test rate limit handling."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "error": {"code": 5, "error": "Rate limit exceeded"}
        }
        mock_response.status_code = 429
        mock_get.return_value = mock_response

        with pytest.raises(TornAPIRateLimitError, match="Rate limit exceeded"):
            client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")

    @patch("requests.Session.get")
    def test_fetch_data_connection_error(self, mock_get, client):
        """Test connection error handling."""
        mock_get.side_effect = ConnectionError("Connection failed")

        with pytest.raises(TornAPIConnectionError, match="Failed to connect"):
            client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")

    @patch("requests.Session.get")
    def test_fetch_data_timeout(self, mock_get, client):
        """Test timeout handling."""
        mock_get.side_effect = Timeout("Request timed out")

        with pytest.raises(TornAPITimeoutError, match="API request timed out"):
            client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")

    @patch("requests.Session.get")
    def test_fetch_data_retry_success(self, mock_get, client):
        """Test successful retry after transient failure."""
        # First call fails with connection error
        mock_get.side_effect = [
            ConnectionError("Temporary failure"),
            Mock(
                status_code=200,
                json=lambda: {"success": True, "data": {"id": 1}}
            )
        ]

        data = client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")
        assert data == {"success": True, "data": {"id": 1}}
        assert mock_get.call_count == 2

    def test_cleanup_on_deletion(self, client):
        """Test session cleanup on client deletion."""
        session = client.session
        with patch.object(session, "close") as mock_close:
            del client
            mock_close.assert_called_once()

    @patch("requests.Session.get")
    def test_rate_limiting(self, mock_get, client):
        """Test rate limiting between requests."""
        mock_response = Mock(
            status_code=200,
            json=lambda: {"success": True}
        )
        mock_get.return_value = mock_response

        # Make two quick requests
        client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")
        start_time = client._last_request_time["default"]
        
        client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")
        end_time = client._last_request_time["default"]
        
        # Verify minimum time gap
        assert (end_time - start_time) >= client.min_request_interval

    @patch("requests.Session.get")
    def test_sensitive_data_masking(self, mock_get, client):
        """Test masking of sensitive data in logs and errors."""
        mock_get.side_effect = RequestException("Error with key test_key_1")
        
        with pytest.raises(TornAPIError) as exc:
            client.fetch_data("https://api.torn.com/user/{API_KEY}", "default")
        
        # Verify API key is not in error message
        assert "test_key_1" not in str(exc.value)
        assert "***" in str(exc.value)

    def test_successful_api_call(self, client, mock_response):
        """Test successful API call."""
        with patch('requests.get', return_value=mock_response):
            response = client.make_request("user", "basic")
            assert response == {"test": "data"}

    def test_invalid_api_key(self, client):
        """Test handling of invalid API key."""
        mock_error = Mock()
        mock_error.status_code = 403
        mock_error.json.return_value = {
            "error": {
                "code": 2,
                "error": "Incorrect key"
            }
        }
        
        with patch('requests.get', return_value=mock_error):
            with pytest.raises(TornAPIKeyError) as exc:
                client.make_request("user", "basic")
            assert "Invalid API key" in str(exc.value)

    def test_rate_limit_handling(self, client):
        """Test handling of rate limit errors."""
        mock_error = Mock()
        mock_error.status_code = 429
        mock_error.json.return_value = {
            "error": {
                "code": 5,
                "error": "Too many requests"
            }
        }
        
        with patch('requests.get', return_value=mock_error):
            with pytest.raises(TornAPIRateLimitError) as exc:
                client.make_request("user", "basic")
            assert "Rate limit exceeded" in str(exc.value)

    def test_connection_error_handling(self, client):
        """Test handling of connection errors."""
        with patch('requests.get', side_effect=requests.ConnectionError()):
            with pytest.raises(TornAPIConnectionError) as exc:
                client.make_request("user", "basic")
            assert "Failed to connect" in str(exc.value)

    def test_timeout_error_handling(self, client):
        """Test handling of timeout errors."""
        with patch('requests.get', side_effect=requests.Timeout()):
            with pytest.raises(TornAPITimeoutError) as exc:
                client.make_request("user", "basic")
            assert "Request timed out" in str(exc.value)

    def test_retry_on_rate_limit(self, client, mock_response):
        """Test retry behavior on rate limit errors."""
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.json.return_value = {
            "error": {
                "code": 5,
                "error": "Too many requests"
            }
        }
        
        with patch('requests.get') as mock_get:
            # First two calls return rate limit, third succeeds
            mock_get.side_effect = [
                rate_limit_response,
                rate_limit_response,
                mock_response
            ]
            
            response = client.make_request("user", "basic")
            assert response == {"test": "data"}
            assert mock_get.call_count == 3

    def test_retry_max_attempts(self, client):
        """Test maximum retry attempts."""
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.json.return_value = {
            "error": {
                "code": 5,
                "error": "Too many requests"
            }
        }
        
        with patch('requests.get', return_value=rate_limit_response):
            with pytest.raises(RetryError):
                client.make_request("user", "basic", max_retries=3)

    def test_retry_backoff(self, client, mock_response):
        """Test exponential backoff behavior."""
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.json.return_value = {
            "error": {
                "code": 5,
                "error": "Too many requests"
            }
        }
        
        start_time = datetime.now()
        
        with patch('requests.get') as mock_get:
            mock_get.side_effect = [
                rate_limit_response,
                rate_limit_response,
                mock_response
            ]
            
            client.make_request("user", "basic")
            
            # Check that appropriate time has passed
            elapsed = datetime.now() - start_time
            # First retry after 1s, second after 2s
            assert elapsed.total_seconds() >= 3

    def test_api_key_rotation(self, client, mock_response):
        """Test API key rotation on errors."""
        key_error_response = Mock()
        key_error_response.status_code = 403
        key_error_response.json.return_value = {
            "error": {
                "code": 2,
                "error": "Incorrect key"
            }
        }
        
        with patch('requests.get') as mock_get:
            # First key fails, second key succeeds
            mock_get.side_effect = [key_error_response, mock_response]
            
            response = client.make_request("user", "basic")
            assert response == {"test": "data"}
            
            # Verify different keys were used
            first_call = mock_get.call_args_list[0]
            second_call = mock_get.call_args_list[1]
            assert first_call != second_call

    def test_endpoint_validation(self, client, mock_response):
        """Test validation of endpoint and selection parameters."""
        # Test invalid endpoint
        with pytest.raises(ValueError):
            client.make_request("invalid_endpoint", "basic")
        
        # Test invalid selection
        with pytest.raises(ValueError):
            client.make_request("user", "invalid_selection")
        
        # Test valid combination
        with patch('requests.get', return_value=mock_response):
            response = client.make_request("user", "basic")
            assert response == {"test": "data"}

    def test_response_validation(self, client):
        """Test validation of API response format."""
        invalid_response = Mock()
        invalid_response.status_code = 200
        invalid_response.json.return_value = {"invalid": "format"}
        
        with patch('requests.get', return_value=invalid_response):
            with pytest.raises(ValueError) as exc:
                client.make_request("user", "basic")
            assert "Invalid response format" in str(exc.value)

    def test_request_timeout_config(self, client, mock_response):
        """Test request timeout configuration."""
        with patch('requests.get') as mock_get:
            mock_get.return_value = mock_response
            
            # Test default timeout
            client.make_request("user", "basic")
            assert mock_get.call_args[1]['timeout'] == 30
            
            # Test custom timeout
            client.make_request("user", "basic", timeout=60)
            assert mock_get.call_args[1]['timeout'] == 60

    def test_concurrent_requests(self, client, mock_response):
        """Test handling of concurrent requests."""
        with patch('requests.get', return_value=mock_response):
            # Simulate multiple concurrent requests
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                    executor.submit(client.make_request, "user", "basic")
                    for _ in range(3)
                ]
                
                # All requests should succeed
                results = [f.result() for f in futures]
                assert all(r == {"test": "data"} for r in results)

    def test_error_logging(self, client, caplog):
        """Test error logging functionality."""
        error_response = Mock()
        error_response.status_code = 500
        error_response.json.return_value = {
            "error": {
                "code": 9,
                "error": "Internal server error"
            }
        }
        
        with patch('requests.get', return_value=error_response):
            with pytest.raises(Exception):
                client.make_request("user", "basic")
            
            # Verify error was logged
            assert "API request failed" in caplog.text
            assert "Internal server error" in caplog.text 