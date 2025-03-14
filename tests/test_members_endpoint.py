#!/usr/bin/env python3
"""
Test script for members endpoint data loading.
This script uses the existing code but tests only the members endpoint.
"""

import os
import sys
import json
import logging
from datetime import datetime
import pandas as pd
from google.cloud import bigquery

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app/src'))

from app.core.common import setup_logging
from app.services.torncity.client import tc_load_api_key, tc_fetch_api_data
from app.services.google.client import upload_to_bigquery

def delete_table(client, table_id):
    """Delete a BigQuery table if it exists."""
    try:
        client.delete_table(table_id)
        logging.info(f"Table {table_id} deleted successfully")
    except Exception as e:
        logging.warning(f"Error deleting table {table_id}: {str(e)}")

def process_members_data(data):
    """Process members data into a DataFrame with one row per member."""
    if not data or 'members' not in data:
        return pd.DataFrame()

    members_list = []
    server_ts = pd.to_datetime(data.get('timestamp', datetime.now().timestamp()), unit='s')
    fetched_at = pd.to_datetime(data.get('fetched_at', datetime.now().isoformat()))
    
    logging.info(f"Raw server timestamp: {data.get('timestamp')}")
    logging.info(f"Converted server timestamp: {server_ts}")
    logging.info(f"Raw fetched_at: {data.get('fetched_at')}")
    logging.info(f"Converted fetched_at: {fetched_at}")

    for member_data in data['members']:
        # Log raw timestamp values
        logging.info(f"Member {member_data.get('id')} timestamps:")
        logging.info(f"  last_action_timestamp: {member_data.get('last_action', {}).get('timestamp')}")
        logging.info(f"  status_until: {member_data.get('status', {}).get('until')}")
        
        member_record = {
            'server_timestamp': server_ts,
            'member_id': member_data.get('id'),
            'name': member_data.get('name'),
            'level': member_data.get('level'),
            'days_in_faction': member_data.get('days_in_faction'),
            'last_action_status': member_data.get('last_action', {}).get('status'),
            'last_action_timestamp': member_data.get('last_action', {}).get('timestamp'),
            'last_action_relative': member_data.get('last_action', {}).get('relative'),
            'status_description': member_data.get('status', {}).get('description'),
            'status_details': member_data.get('status', {}).get('details'),
            'status_state': member_data.get('status', {}).get('state'),
            'status_until': member_data.get('status', {}).get('until'),
            'life_current': member_data.get('life', {}).get('current'),
            'life_maximum': member_data.get('life', {}).get('maximum'),
            'revive_setting': member_data.get('revive', {}).get('setting'),
            'position': member_data.get('position'),
            'is_revivable': member_data.get('is_revivable'),
            'is_on_wall': member_data.get('is_on_wall'),
            'is_in_oc': member_data.get('is_in_oc'),
            'has_early_discharge': member_data.get('has_early_discharge'),
            'fetched_at': fetched_at
        }
        members_list.append(member_record)

    df = pd.DataFrame(members_list)
    
    # Convert numeric columns
    numeric_cols = ['member_id', 'level', 'days_in_faction', 
                   'status_details', 'status_until', 'life_current', 'life_maximum']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert timestamp columns
    if 'last_action_timestamp' in df.columns:
        df['last_action_timestamp'] = pd.to_datetime(df['last_action_timestamp'], unit='s', errors='coerce')

    # Log DataFrame info after conversions
    logging.info("DataFrame types after conversion:")
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            logging.info(f"{col} dtype: {df[col].dtype}")
            logging.info(f"{col} sample values: {df[col].head()}")

    return df

def test_members_endpoint():
    """Test the members endpoint specifically."""
    # Set up logging
    setup_logging()
    logging.info("Starting members endpoint test")

    # Set project ID and table ID
    project_id = "torncity-402423"
    table_id = f"{project_id}.torn_data.v2_faction_40832_members"

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

    # Find the members endpoint configuration
    members_endpoint = None
    for endpoint in api_config.get("endpoints", []):
        if endpoint["name"] == "v2_faction_40832_members":
            members_endpoint = endpoint
            break

    if not members_endpoint:
        logging.error("Members endpoint not found in configuration")
        return False

    # Delete the existing members table
    logging.info(f"Deleting table {table_id} if it exists")
    delete_table(client, table_id)

    # Process members endpoint
    logging.info("Processing members endpoint")
    logging.info("Processing endpoint: %s", members_endpoint['name'])
    
    # Fetch data from API
    data = tc_fetch_api_data(members_endpoint["url"], members_endpoint.get("api_key", "default"), api_keys)
    if not data:
        logging.error("Failed to fetch data for %s", members_endpoint['name'])
        return False
    data["fetched_at"] = datetime.now().isoformat()
    
    # Process the data into rows
    df = process_members_data(data)
    if df.empty:
        logging.error("No data processed for %s", members_endpoint['name'])
        return False
    
    # Upload to BigQuery with specified storage mode
    storage_mode = members_endpoint.get("storage_mode", "replace")
    logging.info("Uploading data to BigQuery table: %s (storage mode: %s)", 
                members_endpoint['table'], storage_mode)
    try:
        upload_to_bigquery(config, df, members_endpoint["table"], storage_mode)
    except Exception as e:
        logging.error("Error uploading to BigQuery for %s: %s", members_endpoint['name'], str(e))
        return False

    logging.info("Successfully processed %s", members_endpoint['name'])
    logging.info("Records processed: %d", len(df))
    logging.info("Columns: %d", len(df.columns))
    logging.info("Data types: %s", df.dtypes.value_counts().to_dict())
    
    logging.info("Members endpoint test completed successfully")
    return True

if __name__ == "__main__":
    success = test_members_endpoint()
    sys.exit(0 if success else 1) 