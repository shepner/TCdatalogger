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


def tc_fetch_api_data(url: str, tc_api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch data from Torn City API.

    Makes an authenticated request to the Torn City API and handles
    various response cases and errors.

    Args:
        url: The API endpoint URL with {API_KEY} placeholder.
        tc_api_key: The API key to use for authentication.

    Returns:
        Dict[str, Any]: The API response data if successful, None otherwise.

    Raises:
        requests.RequestException: If there's an error making the HTTP request.

    Example:
        >>> url = "https://api.torn.com/v2/faction/members?key={API_KEY}"
        >>> data = tc_fetch_api_data(url, "your-api-key")
        >>> if data:
        ...     print(f"Found {len(data.get('members', []))} members")
    """
    if tc_api_key is None:
        logging.error("Error: API key is missing. Cannot proceed with API call.")
        return None

    # Replace placeholder `{API_KEY}` with the actual key
    formatted_url = url.replace("{API_KEY}", tc_api_key)
    logging.info("Fetching data from: %s", formatted_url)

    try:
        response = requests.get(formatted_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error("Error fetching data: %s", str(e))
        return None
    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON response: %s", str(e))
        return None

