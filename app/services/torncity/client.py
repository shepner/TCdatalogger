"""Torn City API integration module.

This module provides core connectivity and transport functionality for the Torn City API:
- API key management
- Basic request handling
- Rate limiting
- Error handling
- Retry logic

All business logic and data transformation should be handled by endpoint processors.
"""

import json
import logging
import time
from typing import Optional, Dict, Any, Union, Tuple, List
import re
from datetime import datetime, timedelta
import os
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
    after_log
)

from .exceptions import (
    TornAPIError,
    TornAPIKeyError,
    TornAPIRateLimitError,
    TornAPIConnectionError,
    TornAPITimeoutError
)


class TornClient:
    """Client for interacting with the Torn API."""

    # Default minimum interval between requests (in seconds)
    MIN_REQUEST_INTERVAL = timedelta(seconds=1)

    def __init__(self, api_key_or_file: str):
        """Initialize the Torn API client.

        Args:
            api_key_or_file: Either a direct API key or path to a JSON file containing API keys.
        """
        self.api_keys = {}
        self.api_key_or_file = api_key_or_file
        self._last_request_time = {}  # Initialize the request time tracking dict
        self.logger = logging.getLogger(__name__)  # Initialize logger
        self._load_api_keys()

    def _load_api_keys(self) -> None:
        """Load API keys from file or direct key.

        Raises:
            TornAPIKeyError: If API keys cannot be loaded or are invalid.
        """
        try:
            # Check if the input looks like a file path
            if self.api_key_or_file.endswith('.json'):
                if not os.path.exists(self.api_key_or_file):
                    raise TornAPIKeyError("API keys file not found")
                try:
                    with open(self.api_key_or_file, 'r') as f:
                        self.api_keys = json.load(f)
                except json.JSONDecodeError:
                    raise TornAPIKeyError("API keys file must contain valid JSON")
                if not isinstance(self.api_keys, dict):
                    raise TornAPIKeyError("API keys file must contain a JSON object")
                if "default" not in self.api_keys:
                    raise TornAPIKeyError("API keys file must contain 'default' key")
            else:
                # Treat the input as a direct API key
                self.api_keys = {"default": self.api_key_or_file}

        except TornAPIKeyError:
            raise
        except Exception as e:
            raise TornAPIKeyError(f"Unexpected error loading API keys: {str(e)}")

    def _setup_retry_config(self):
        """Set up retry configuration."""
        return {
            'stop': stop_after_attempt(3),
            'wait': wait_exponential(multiplier=1, min=4, max=10),
            'retry': retry_if_exception_type((
                TornAPIRateLimitError,
                TornAPIConnectionError,
                TornAPITimeoutError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout
            ))
        }

    def cleanup(self):
        """Clean up resources and close the session."""
        if hasattr(self, 'session'):
            self.session.close()

    def __del__(self):
        """Clean up resources when the client is deleted."""
        self.cleanup()

    def _mask_sensitive_url(self, url: str) -> str:
        """Mask sensitive information in URLs.
        
        Args:
            url: URL containing sensitive information
            
        Returns:
            str: URL with sensitive information masked
        """
        # Mask API keys in URLs
        masked = re.sub(r'key=[^&]+', 'key=***', url)
        # Mask any other potential sensitive data
        for key in self.api_keys.values():
            masked = masked.replace(key, '***')
        return masked

    def _enforce_rate_limit(self, api_key: str) -> None:
        """Enforce rate limiting for API requests.
        
        Args:
            api_key: API key identifier to track
            
        Raises:
            TornAPIRateLimitError: If rate limit would be exceeded
        """
        now = datetime.now()
        if api_key in self._last_request_time:
            time_since_last = now - self._last_request_time[api_key]
            if time_since_last < self.MIN_REQUEST_INTERVAL:
                sleep_time = (self.MIN_REQUEST_INTERVAL - time_since_last).total_seconds()
                time.sleep(sleep_time)
        self._last_request_time[api_key] = now

    def _handle_api_response(self, response):
        """Handle the API response and check for errors.
        
        Args:
            response: Response from the API
            
        Returns:
            dict: The response data
            
        Raises:
            TornAPIError: If the response indicates an error
            TornAPIKeyError: If the API key is invalid
            TornAPIRateLimitError: If rate limit is exceeded
        """
        try:
            data = response.json() if hasattr(response, 'json') else response
            
            if isinstance(data, dict):
                if 'error' in data:
                    error = data['error']
                    if isinstance(error, dict):
                        code = error.get('code')
                        message = error.get('error', '')
                        
                        if code == 2:  # Invalid API key
                            error_msg = f"API key not found: {self._mask_api_key(message)}"
                            logging.error(error_msg)
                            raise TornAPIKeyError(error_msg)
                        elif code == 5:  # Rate limit
                            error_msg = "Rate limit exceeded"
                            logging.error(error_msg)
                            raise TornAPIRateLimitError(error_msg)
                        elif code == 9:  # API system disabled
                            error_msg = "API system disabled"
                            logging.error(error_msg)
                            raise TornAPIError(error_msg)
                        else:
                            error_msg = f"API Error: {self._mask_api_key(message)}"
                            logging.error(error_msg)
                            raise TornAPIError(error_msg)
                    else:
                        error_msg = f"API Error: {self._mask_api_key(str(error))}"
                        logging.error(error_msg)
                        raise TornAPIError(error_msg)
                
                # Return the full response structure
                return data
            
            raise TornAPIError("Invalid response format")
        except (ValueError, AttributeError) as e:
            raise TornAPIError(f"Failed to parse API response: {str(e)}")

    def _mask_api_key(self, message: str) -> str:
        """Mask API keys in error messages.

        Args:
            message: The message that may contain API keys.

        Returns:
            str: Message with API keys masked.
        """
        masked_message = message
        # Mask actual API key values
        for key in self.api_keys.values():
            masked_message = masked_message.replace(key, "***")
        
        # Mask key identifiers
        for key_name in self.api_keys.keys():
            masked_message = masked_message.replace(key_name, "***")
        
        # Mask any potential API key patterns (16 alphanumeric characters)
        masked_message = re.sub(r'[a-zA-Z0-9]{16}', '***', masked_message)
        
        return masked_message

    def _get_api_key(self, selection: str = "default") -> str:
        """Get the API key for the given selection.
        
        Args:
            selection: The key selection to use (defaults to "default")
            
        Returns:
            str: The API key
            
        Raises:
            TornAPIKeyError: If the selected API key is not found
        """
        if selection not in self.api_keys:
            raise TornAPIKeyError(f"API key selection '{selection}' not found")
        return self.api_keys[selection]

    def _get_next_api_key(self, current_key):
        """Get the next API key to use."""
        if current_key not in self.api_keys:
            raise TornAPIKeyError(f"API key not found: {current_key}")
        return self.api_keys[current_key]

    def get_timeout_config(self, timeout=None):
        """Get timeout configuration for requests.
        
        Args:
            timeout: Optional timeout value. Can be an integer for both connect and read timeouts,
                    or a tuple of (connect_timeout, read_timeout).
                    If None, returns default timeout of (5, 30).
        
        Returns:
            int or tuple: Timeout configuration
        """
        if timeout is None:
            return (5, 30)
        if isinstance(timeout, (int, float)):
            return timeout
        return timeout

    def _make_request_internal(self, endpoint: str, api_key: str, timeout: Optional[int] = None) -> Dict[str, Any]:
        """Make an internal request to the Torn API.

        Args:
            endpoint: The API endpoint to request.
            api_key: The API key to use.
            timeout: Optional request timeout in seconds.

        Returns:
            Dict containing the API response data.

        Raises:
            TornAPIError: For any API-related errors.
        """
        url = f"https://api.torn.com/{endpoint}?key={api_key}"
        
        try:
            # Enforce rate limiting
            self._enforce_rate_limit(api_key)
            
            # Make the request with retries
            for attempt in range(3):  # Try up to 3 times
                try:
                    response = requests.get(url, timeout=self.get_timeout_config(timeout))
                    data = response.json()
                    
                    # Check for error response
                    if "error" in data:
                        error_code = data["error"].get("code", 0)
                        error_msg = data["error"].get("error", "Unknown error")
                        
                        if error_code == 5:  # Too many requests
                            logging.warning(f"Rate limit hit for key {api_key}, attempt {attempt + 1}")
                            time.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        elif error_code == 2:  # Incorrect key
                            raise TornAPIKeyError(f"Invalid API key: {error_msg}")
                        else:
                            raise TornAPIError(f"API error: {error_msg}")
                    
                    return data
                    
                except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                    if attempt == 2:  # Last attempt
                        raise TornAPIConnectionError(f"Failed to connect to Torn API: {str(e)}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
            
            # If we get here, we've exhausted our retries
            raise TornAPIRateLimitError(f"Rate limit exceeded for key {api_key}")
            
        except Exception as e:
            if isinstance(e, (TornAPIError, TornAPIKeyError, TornAPIRateLimitError, TornAPIConnectionError)):
                raise
            raise TornAPIError(f"Unexpected error: {str(e)}")

    def make_request(self, endpoint: str, selection: str = "default") -> Dict[str, Any]:
        """Make a request to the Torn API.

        Args:
            endpoint: The API endpoint to request
            selection: The API key selection to use

        Returns:
            The API response data

        Raises:
            TornAPIError: For general API errors
            TornAPIKeyError: For API key related errors
            TornAPIRateLimitError: For rate limit errors
            TornAPITimeoutError: For timeout errors
            TornAPIConnectionError: For connection errors
        """
        try:
            api_key = self._get_api_key(selection)
            return self._make_request_internal(endpoint, api_key)
        except TornAPITimeoutError:
            raise
        except TornAPIConnectionError as e:
            if "Request timed out" in str(e):
                raise TornAPITimeoutError("Request timed out")
            raise TornAPIError(f"API request failed: {str(e)}")
        except Exception as e:
            raise TornAPIError(f"API request failed: {str(e)}")

    def make_concurrent_requests(self, endpoints: List[str], selection: str = 'default') -> List[Dict]:
        """Make multiple concurrent requests to the Torn API.
        
        Args:
            endpoints: List of API endpoints to call
            selection: Which API key to use ('default' or 'secondary')
            
        Returns:
            List of API response data
        """
        api_key = self._get_api_key(selection)
        responses = []
        
        for endpoint in endpoints:
            try:
                response = self._make_request_internal(endpoint, api_key)
                responses.append(response)
            except Exception as e:
                # Log error but continue with other requests
                self.logger.error(f"Error making request to {endpoint}: {str(e)}")
                responses.append(None)
                
        return responses

    def fetch_data(self, url: str, timeout: Optional[Union[int, Tuple[int, int]]] = None) -> Dict[str, Any]:
        """Fetch data from a specific URL.

        Args:
            url: URL to fetch data from
            timeout: Request timeout in seconds

        Returns:
            Dict[str, Any]: Response data

        Raises:
            TornAPIError: For any API-related errors
        """
        retry_decorator = retry(
            stop=self.retry_config['stop'],
            wait=self.retry_config['wait'],
            retry=self.retry_config['retry'],
            before=self.retry_config['before'],
            after=self.retry_config['after']
        )

        try:
            def _fetch():
                try:
                    response = self.session.get(url, timeout=timeout or (5, 30))
                    response.raise_for_status()
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    raise TornAPIError("Failed to parse API response")
                except requests.exceptions.Timeout:
                    raise TornAPITimeoutError("Request timed out")
                except requests.exceptions.ConnectionError:
                    raise TornAPIConnectionError("Failed to connect to API")
                except requests.exceptions.RequestException as e:
                    raise TornAPIError(f"Request failed: {str(e)}")

            return retry_decorator(_fetch)()
        except Exception as e:
            if isinstance(e, (TornAPIError, TornAPIConnectionError, TornAPITimeoutError)):
                raise
            raise TornAPIError(f"Request failed: {str(e)}")

