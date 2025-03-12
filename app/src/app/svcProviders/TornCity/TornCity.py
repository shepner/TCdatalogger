"""Torn City API integration module.

This module provides functionality for:
- Loading and managing Torn City API keys
- Making authenticated requests to the Torn City API
- Handling API responses and errors

The module supports various Torn City API endpoints and handles
authentication and error cases appropriately.
"""

import json
import logging
from typing import Optional, Dict, Any
import re

import requests


def tc_load_api_key(tc_api_key_file: str) -> Optional[str]:
    """Load the Torn City API key from a file.

    Args:
        tc_api_key_file: Path to the file containing the API key.

    Returns:
        str: The API key if successfully loaded, None otherwise.

    Example:
        >>> api_key = tc_load_api_key("config/TC_API_key.txt")
        >>> if api_key:
        ...     print("API key loaded successfully")
    """
    try:
        with open(tc_api_key_file, "r") as f:
            return f.read().strip()  # Ensure no extra spaces/newlines
    except FileNotFoundError:
        logging.error("API key file '%s' not found", tc_api_key_file)
        return None
    except Exception as e:
        logging.error("Error reading API key file: %s", str(e))
        return None


def mask_sensitive_url(url: str) -> str:
    """Mask sensitive information in URLs.
    
    Args:
        url: The URL containing sensitive information.
        
    Returns:
        str: URL with sensitive information masked.
    """
    # Mask API key
    masked_url = re.sub(r'key=[^&]+', 'key=***', url)
    return masked_url


def tc_fetch_api_data(url: str, api_key: str) -> Optional[Dict]:
    """Fetch data from Torn City API.
    
    Args:
        url: API endpoint URL with {API_KEY} placeholder.
        api_key: Torn City API key.
        
    Returns:
        Optional[Dict]: API response data if successful, None otherwise.
    """
    try:
        # Replace API key placeholder
        full_url = url.replace("{API_KEY}", api_key)
        
        # Log masked URL
        logging.info("Fetching data from: %s", mask_sensitive_url(full_url))
        
        response = requests.get(full_url)
        response.raise_for_status()
        
        return response.json()
        
    except requests.exceptions.RequestException as e:
        # Mask any sensitive data in error message
        error_msg = str(e)
        if api_key in error_msg:
            error_msg = error_msg.replace(api_key, "***")
        logging.error("API request failed: %s", error_msg)
        return None

