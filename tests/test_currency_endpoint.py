#!/usr/bin/env python3
"""
Test script for currency endpoint data loading.
This script uses the existing code but tests only the currency endpoint.
"""

import os
import sys
import json
import logging
from datetime import datetime

# Add the app/src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app/src'))

from app.core.common import process_api_endpoint, setup_logging
from app.services.torncity.client import tc_load_api_key, tc_fetch_api_data

def test_currency_endpoint():
    """Test the currency endpoint specifically."""
    # Set up logging
    setup_logging()
    logging.info("Starting currency endpoint test")

    # Load configuration (paths are now relative to project root)
    config_dir = "app/config"
    config = {
        "config_dir": config_dir,
        "gcp_credentials_file": os.path.join(config_dir, "credentials.json"),
        "tc_api_key_file": os.path.join(config_dir, "TC_API_key.json"),
        "tc_api_config_file": os.path.join(config_dir, "TC_API_config.json"),
    }

    # Load API keys
    api_keys = tc_load_api_key(config["tc_api_key_file"])
    if not api_keys:
        logging.error("Failed to load API keys")
        return False

    # Load API config
    try:
        with open(config["tc_api_config_file"], 'r') as f:
            api_config = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load API config: {str(e)}")
        return False

    # Find the currency endpoint configuration
    currency_endpoint = None
    for endpoint in api_config.get("endpoints", []):
        if endpoint["name"] == "v2_faction_40832_currency":
            currency_endpoint = endpoint
            break

    if not currency_endpoint:
        logging.error("Currency endpoint not found in configuration")
        return False

    # First process server timestamp endpoint to ensure we have the timestamp
    timestamp_endpoint = None
    for endpoint in api_config.get("endpoints", []):
        if endpoint["name"] == "server_timestamp":
            timestamp_endpoint = endpoint
            break

    if not timestamp_endpoint:
        logging.error("Server timestamp endpoint not found in configuration")
        return False

    # Process server timestamp first
    logging.info("Processing server timestamp endpoint")
    # Fetch data from API
    data = tc_fetch_api_data(timestamp_endpoint["url"], timestamp_endpoint.get("api_key", "default"), api_keys)
    if not data:
        logging.error("Failed to fetch data for server timestamp")
        return False
    
    # Add fetched_at timestamp
    data["fetched_at"] = datetime.now().isoformat()
    
    # Process server timestamp endpoint
    success = process_api_endpoint("server_timestamp", config, data)
    if not success:
        logging.error("Failed to process server timestamp endpoint")
        return False

    # Process currency endpoint
    logging.info("Processing currency endpoint")
    # Fetch currency data
    data = tc_fetch_api_data(currency_endpoint["url"], currency_endpoint.get("api_key", "default"), api_keys)
    if not data:
        logging.error("Failed to fetch data for currency endpoint")
        return False
    
    # Add fetched_at timestamp
    data["fetched_at"] = datetime.now().isoformat()
    
    success = process_api_endpoint("v2_faction_40832_currency", config, data)
    if not success:
        logging.error("Failed to process currency endpoint")
        return False

    logging.info("Currency endpoint test completed successfully")
    return True

if __name__ == "__main__":
    success = test_currency_endpoint()
    sys.exit(0 if success else 1) 