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


def tc_load_api_key(tc_api_key_file: str) -> Optional[Dict[str, str]]:
    """Load the Torn City API keys from a JSON file.

    Args:
        tc_api_key_file: Path to the file containing the API keys.

    Returns:
        Dict[str, str]: Dictionary mapping API key identifiers to their values,
                       or None if loading fails.

    Example:
        >>> api_keys = tc_load_api_key("config/TC_API_key.json")
        >>> if api_keys:
        ...     print("API keys loaded successfully")
    """
    try:
        with open(tc_api_key_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("API key file '%s' not found", tc_api_key_file)
        return None
    except json.JSONDecodeError:
        logging.error("Invalid JSON format in API key file: %s", tc_api_key_file)
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


def tc_fetch_api_data(url: str, api_key: str, api_keys: Dict[str, str]) -> Optional[Dict]:
    """Fetch data from Torn City API.
    
    Args:
        url: API endpoint URL with {API_KEY} placeholder.
        api_key: API key identifier (e.g., 'faction_40832').
        api_keys: Dictionary mapping API key identifiers to their values.
        
    Returns:
        Optional[Dict]: API response data if successful, None otherwise.
    """
    try:
        # Get the actual API key value
        if api_key not in api_keys:
            logging.error("API key identifier '%s' not found in configuration", api_key)
            return None
            
        actual_key = api_keys[api_key]
        
        # Replace API key placeholder
        full_url = url.replace("{API_KEY}", actual_key)
        
        # Log masked URL
        logging.info("Fetching data from: %s", mask_sensitive_url(full_url))
        
        response = requests.get(full_url)
        response.raise_for_status()
        
        return response.json()
        
    except requests.exceptions.RequestException as e:
        # Mask any sensitive data in error message
        error_msg = str(e)
        if actual_key in error_msg:
            error_msg = error_msg.replace(actual_key, "***")
        logging.error("API request failed: %s", error_msg)
        return None

