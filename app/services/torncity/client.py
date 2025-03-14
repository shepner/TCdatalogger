"""Torn City API integration module.

This module provides core connectivity and transport functionality for the Torn City API:
- API key management
- Basic request handling
- Rate limiting
- Error handling

All business logic and data transformation should be handled by endpoint processors.
"""

import json
import logging
import time
from typing import Optional, Dict, Any
import re
from datetime import datetime, timedelta

import requests
from requests.exceptions import RequestException


class TornAPIError(Exception):
    """Base exception for Torn API errors."""
    pass


class TornAPIKeyError(TornAPIError):
    """Raised when there are issues with API keys."""
    pass


class TornAPIRateLimitError(TornAPIError):
    """Raised when rate limits are exceeded."""
    pass


class TornClient:
    """Generic Torn City API client for basic transport operations."""

    def __init__(self, api_key_file: str):
        """Initialize Torn City API client.
        
        Args:
            api_key_file: Path to the API keys configuration file
        """
        self.api_key_file = api_key_file
        self.api_keys = self._load_api_keys()
        self._last_request_time = {}
        self.min_request_interval = timedelta(seconds=1)  # Basic rate limiting

    def _load_api_keys(self) -> Dict[str, str]:
        """Load API keys from configuration file.
        
        Returns:
            Dict[str, str]: Mapping of API key identifiers to values
            
        Raises:
            TornAPIKeyError: If API keys cannot be loaded
        """
        try:
            with open(self.api_key_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            raise TornAPIKeyError(f"API key file not found: {self.api_key_file}")
        except json.JSONDecodeError:
            raise TornAPIKeyError(f"Invalid JSON in API key file: {self.api_key_file}")
        except Exception as e:
            raise TornAPIKeyError(f"Error loading API keys: {str(e)}")

    def _mask_sensitive_url(self, url: str) -> str:
        """Mask sensitive information in URLs.
        
        Args:
            url: URL containing sensitive information
            
        Returns:
            str: URL with sensitive information masked
        """
        return re.sub(r'key=[^&]+', 'key=***', url)

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
            if time_since_last < self.min_request_interval:
                sleep_time = (self.min_request_interval - time_since_last).total_seconds()
                time.sleep(sleep_time)
        self._last_request_time[api_key] = now

    def fetch_data(self, url: str, api_key: str) -> Dict:
        """Fetch data from Torn City API.
        
        Pure transport function - handles only request/response cycle.
        
        Args:
            url: API endpoint URL with {API_KEY} placeholder
            api_key: API key identifier
            
        Returns:
            Dict: Raw API response data
            
        Raises:
            TornAPIKeyError: If API key is invalid
            TornAPIRateLimitError: If rate limit is exceeded
            TornAPIError: For other API-related errors
        """
        try:
            # Validate API key
            if api_key not in self.api_keys:
                raise TornAPIKeyError(f"API key not found: {api_key}")
            
            # Apply rate limiting
            self._enforce_rate_limit(api_key)
            
            # Make request
            full_url = url.replace("{API_KEY}", self.api_keys[api_key])
            logging.info("Fetching data from: %s", self._mask_sensitive_url(full_url))
            
            response = requests.get(full_url)
            response.raise_for_status()
            
            return response.json()
            
        except RequestException as e:
            # Mask any sensitive data in error message
            error_msg = str(e)
            if self.api_keys[api_key] in error_msg:
                error_msg = error_msg.replace(self.api_keys[api_key], "***")
            raise TornAPIError(f"API request failed: {error_msg}")

