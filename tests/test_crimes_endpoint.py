#!/usr/bin/env python3
"""
Test script for crimes endpoint data loading.
This script uses the existing code but tests only the crimes endpoint.
"""

import os
import sys
import json
import logging
from datetime import datetime
from google.cloud import bigquery

# Add the app/src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app/src'))

from app.core.common import process_api_endpoint, setup_logging
from app.services.torncity.client import tc_load_api_key, tc_fetch_api_data

def delete_table(client, table_id):
    """Delete a BigQuery table if it exists."""
    try:
        client.delete_table(table_id)
        logging.info(f"Table {table_id} deleted successfully")
    except Exception as e:
        logging.warning(f"Error deleting table {table_id}: {str(e)}")

def test_crimes_endpoint():
    """Test the crimes endpoint specifically."""
    # Set up logging
    setup_logging()
    logging.info("Starting crimes endpoint test")

    # Set project ID and table ID
    project_id = "torncity-402423"
    table_id = f"{project_id}.torn_data.v2_faction_40832_crimes"

    # Load configuration (paths are now relative to project root)
    config_dir = "app/config"
    config = {
        "config_dir": config_dir,
        "gcp_credentials_file": os.path.join(config_dir, "credentials.json"),
        "tc_api_key_file": os.path.join(config_dir, "TC_API_key.json"),
        "tc_api_config_file": os.path.join(config_dir, "TC_API_config.json"),
    }

    # Initialize BigQuery client with project
    try:
        client = bigquery.Client(project=project_id)
        logging.info("Connected to BigQuery")
    except Exception as e:
        logging.error(f"Failed to connect to BigQuery: {str(e)}")
        return False

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

    # Find the crimes endpoint configuration
    crimes_endpoint = None
    for endpoint in api_config.get("endpoints", []):
        if endpoint["name"] == "v2_faction_40832_crimes":
            crimes_endpoint = endpoint
            break

    if not crimes_endpoint:
        logging.error("Crimes endpoint not found in configuration")
        return False

    # Delete the existing crimes table
    logging.info(f"Deleting table {table_id} if it exists")
    delete_table(client, table_id)

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
    
    # Store the data in the endpoint config for processing
    timestamp_endpoint["data"] = data
    
    try:
        df = process_api_endpoint("v2_server_timestamp", config, data)
        if df.empty:
            logging.error("Failed to process server timestamp endpoint - empty DataFrame")
            return False
    except Exception as e:
        logging.error(f"Failed to process server timestamp endpoint: {str(e)}")
        return False

    # Process crimes endpoint
    logging.info("Processing crimes endpoint")
    # Fetch data from API
    data = tc_fetch_api_data(crimes_endpoint["url"], crimes_endpoint.get("api_key", "default"), api_keys)
    if not data:
        logging.error("Failed to fetch data for crimes endpoint")
        return False

    # Add fetched_at timestamp
    data["fetched_at"] = datetime.now().isoformat()

    # Store the data in the endpoint config for processing
    crimes_endpoint["data"] = data

    # Process the endpoint
    try:
        result = process_api_endpoint("v2_faction_40832_crimes", config, data)
        if result is None:
            logging.info("Successfully processed crimes endpoint")
        elif isinstance(result, dict):
            if not any(not d.empty for d in result.values()):
                logging.error("Failed to process crimes endpoint - all DataFrames empty")
                return False
        elif result.empty:
            logging.error("Failed to process crimes endpoint - empty DataFrame")
            return False
    except Exception as e:
        logging.error(f"Failed to process crimes endpoint: {str(e)}")
        return False

    logging.info("Crimes endpoint test completed successfully")
    return True

if __name__ == "__main__":
    success = test_crimes_endpoint()
    sys.exit(0 if success else 1) 