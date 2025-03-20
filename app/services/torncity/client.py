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
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    
    # Default timeouts
    DEFAULT_CONNECT_TIMEOUT = 5
    DEFAULT_READ_TIMEOUT = 30

    # API Base URL
    API_BASE_URL = "https://api.torn.com"
    
    # Default retry settings
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 0.5
    RETRY_STATUS_FORCELIST = [408, 429, 500, 502, 503, 504]

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
        self._init_session()
        self.retry_config = self._setup_retry_config()

    def _init_session(self) -> None:
        """Initialize a requests session with retry configuration."""
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.BACKOFF_FACTOR,
            status_forcelist=self.RETRY_STATUS_FORCELIST,
            allowed_methods=["GET"]
        )
        
        # Configure adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default timeouts
        original_request = self.session.request
        self.session.request = lambda *args, **kwargs: original_request(
            *args,
            timeout=kwargs.pop('timeout', (self.DEFAULT_CONNECT_TIMEOUT, self.DEFAULT_READ_TIMEOUT)),
            **kwargs
        )

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

            # Validate all API keys
            for key_name, api_key in self.api_keys.items():
                if not isinstance(api_key, str) or not api_key.strip():
                    raise TornAPIKeyError(f"Invalid API key for '{key_name}'")
                if not re.match(r'^[A-Za-z0-9]{16}$', api_key):
                    raise TornAPIKeyError(f"API key '{key_name}' has invalid format")

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
            )),
            'before': before_log(self.logger, logging.DEBUG),
            'after': after_log(self.logger, logging.DEBUG)
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
                if sleep_time > 0:  # Only sleep if positive time
                    self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
        self._last_request_time[api_key] = now

    def _handle_api_response(self, response: requests.Response) -> dict:
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
                if 'error' in data and data['error'] is not None:  # Only check for errors if error is not None
                    error = data['error']
                    if isinstance(error, dict):
                        code = error.get('code')
                        message = error.get('error', '')
                        
                        error_mapping = {
                            0: ("Unknown error", TornAPIError),
                            1: ("Private endpoint", TornAPIError),
                            2: ("Invalid API key", TornAPIKeyError),
                            3: ("Authentication error", TornAPIKeyError),
                            4: ("Invalid format specified", TornAPIError),
                            5: ("Rate limit exceeded", TornAPIRateLimitError),
                            6: ("Incorrect ID specified", TornAPIError),
                            7: ("Incorrect ID-entity relation", TornAPIError),
                            8: ("IP block", TornAPIError),
                            9: ("API system disabled", TornAPIError),
                            10: ("Key owner is in federal jail", TornAPIError),
                            11: ("Key change error", TornAPIError),
                            12: ("Key read error", TornAPIError),
                            13: ("The requested selection is invalid", TornAPIError),
                            14: ("The requested selection is disabled", TornAPIError),
                            15: ("The requested selection is not available", TornAPIError),
                            16: ("The requested selection is not available for the specified ID", TornAPIError)
                        }
                        
                        error_msg, error_class = error_mapping.get(code, ("Unknown error", TornAPIError))
                        error_msg = f"{error_msg}: {self._mask_api_key(message)}"
                        self.logger.error(error_msg)
                        raise error_class(error_msg)
                    else:
                        error_msg = f"API Error: {self._mask_api_key(str(error))}"
                        self.logger.error(error_msg)
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

    def get_timeout_config(self, timeout: Optional[Union[int, Tuple[int, int]]] = None) -> Tuple[int, int]:
        """Get timeout configuration for requests.
        
        Args:
            timeout: Optional timeout value. Can be an integer for both connect and read timeouts,
                    or a tuple of (connect_timeout, read_timeout).
                    If None, returns default timeout of (5, 30).
        
        Returns:
            Tuple[int, int]: A tuple of (connect_timeout, read_timeout)
        """
        if timeout is None:
            return (self.DEFAULT_CONNECT_TIMEOUT, self.DEFAULT_READ_TIMEOUT)
        elif isinstance(timeout, (int, float)):
            return (timeout, timeout)
        elif isinstance(timeout, (tuple, list)) and len(timeout) == 2:
            return (int(timeout[0]), int(timeout[1]))
        else:
            raise ValueError("Invalid timeout configuration")

    def make_request(self, endpoint: str, selection: Optional[str] = None, timeout: Optional[Union[int, Tuple[int, int]]] = None) -> Dict[str, Any]:
        """Make a request to the Torn API.

        Args:
            endpoint: API endpoint to call
            selection: Optional data selection filter
            timeout: Optional request timeout override

        Returns:
            API response data

        Raises:
            TornAPIError: On API error
            TornAPITimeoutError: On request timeout
            TornAPIRateLimitError: On rate limit exceeded
        """
        api_key = self.api_keys.get(selection or "default")
        if not api_key:
            raise TornAPIKeyError(f"API key not found for selection: {selection}")

        # Build URL with base
        url = f"{self.API_BASE_URL}/{endpoint}?key={api_key}"
        
        # Configure timeout
        timeout_config = timeout or (self.DEFAULT_CONNECT_TIMEOUT, self.DEFAULT_READ_TIMEOUT)

        try:
            # Enforce rate limiting
            self._enforce_rate_limit(api_key)
            
            # Make request
            response = self.session.get(url, timeout=timeout_config)
            response.raise_for_status()
            
            # Handle response
            return self._handle_api_response(response)
            
        except requests.exceptions.Timeout:
            raise TornAPITimeoutError("Request timed out")
        except requests.exceptions.RequestException as e:
            raise TornAPIError(f"Request failed: {str(e)}")

    def make_concurrent_requests(self, endpoints: List[str], selection: str = 'default', max_workers: int = 5) -> List[Dict]:
        """Make concurrent requests to multiple endpoints.
        
        Args:
            endpoints: List of API endpoints to request
            selection: The API key selection to use
            max_workers: Maximum number of concurrent requests
            
        Returns:
            List[Dict]: List of API response data
            
        Raises:
            TornAPIError: If any request fails
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.make_request, endpoint, selection)
                for endpoint in endpoints
            ]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    # Re-raise the first error encountered
                    raise
            
            return results

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

