#!/usr/bin/env python3
"""
Test script for items endpoint data loading.
This script uses the existing code but tests only the items endpoint.
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

def test_items_endpoint():
    """Test the items endpoint specifically."""
    # Set up logging
    setup_logging()
    logging.info("Starting items endpoint test")

    # Set project ID and table ID
    project_id = "torncity-402423"
    table_id = f"{project_id}.torn_data.v2_torn_items"

    # Load configuration (paths are now relative to project root)
    config = {
        "config_dir": "app/config",
        "gcp_credentials_file": "app/config/credentials.json",
        "tc_api_key_file": "app/config/TC_API_key.json",
        "tc_api_config_file": "app/config/TC_API_config.json",
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

    # Find the items endpoint configuration
    items_endpoint = None
    for endpoint in api_config.get("endpoints", []):
        if endpoint["name"] == "v2_torn_items":
            items_endpoint = endpoint
            break

    if not items_endpoint:
        logging.error("Items endpoint not found in configuration")
        return False

    # Delete the existing items table
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
    
    # Process the endpoint
    success = process_api_endpoint(timestamp_endpoint["name"], config, data)
    if not success:
        logging.error("Failed to process server timestamp endpoint")
        return False

    # Process items endpoint
    logging.info("Processing items endpoint")
    # Fetch data from API
    data = tc_fetch_api_data(items_endpoint["url"], items_endpoint.get("api_key", "default"), api_keys)
    if not data:
        logging.error("Failed to fetch data for items endpoint")
        return False

    # Add fetched_at timestamp
    data["fetched_at"] = datetime.now().isoformat()

    # Process the endpoint
    success = process_api_endpoint(items_endpoint["name"], config, data)
    if not success:
        logging.error("Failed to process items endpoint")
        return False

    logging.info("Items endpoint test completed successfully")
    return True

if __name__ == "__main__":
    success = test_items_endpoint()
    sys.exit(0 if success else 1) 