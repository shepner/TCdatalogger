import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

from app.common.common import find_config_directory, process_data
from app.svcProviders.TornCity.TornCity import tc_load_api_key, tc_fetch_api_data
from app.svcProviders.Google.Google import upload_to_bigquery

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('tcdata.log')
        ]
    )

def load_config(config_directories: List[str]) -> Optional[Dict]:
    """Load configuration from the first valid directory."""
    config_dir = find_config_directory(config_directories)
    
    if not config_dir:
        logging.error("No configuration directory found.")
        return None
        
    config = {
        "gcp_credentials_file": os.path.join(config_dir, "credentials.json"),
        "tc_api_key_file": os.path.join(config_dir, "TC_API_key.txt"),
        "tc_api_config_file": os.path.join(config_dir, "TC_API_config.json"),
    }
    
    # Verify all required files exist
    for key, filepath in config.items():
        if not os.path.exists(filepath):
            logging.error(f"Required configuration file not found: {filepath}")
            return None
            
    return config

def process_api_endpoint(config: Dict, api_config: Dict) -> bool:
    """Process a single API endpoint and upload data to BigQuery."""
    try:
        logging.info(f"Processing endpoint: {api_config['name']}")
        
        # Fetch data from API
        data = tc_fetch_api_data(api_config["url"], tc_api_key)
        if not data:
            logging.error(f"Failed to fetch data for {api_config['name']}")
            return False
            
        # Process the data
        logging.info(f"Processing data for {api_config['name']}")
        df = process_data(api_config["name"], data)
        if df.empty:
            logging.error(f"No data processed for {api_config['name']}")
            return False
            
        # Upload to BigQuery
        logging.info(f"Uploading data to BigQuery table: {api_config['table']}")
        upload_to_bigquery(config, df, api_config["table"])
        
        logging.info(f"Successfully processed {api_config['name']}")
        logging.info(f"Records processed: {len(df)}")
        logging.info(f"Columns: {len(df.columns)}")
        logging.info(f"Data types: {df.dtypes.value_counts().to_dict()}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing {api_config['name']}: {str(e)}")
        return False

def main():
    """Main application entry point."""
    start_time = datetime.now()
    setup_logging()
    logging.info("Starting TCdatalogger")
    
    # Configuration directories to search
    config_directories = [
        '/mnt/config',
        '/app/config',
        './config',
        '../config'
    ]
    
    # Load configuration
    config = load_config(config_directories)
    if not config:
        sys.exit(1)
    
    logging.info(f"Using configuration from: {os.path.dirname(config['tc_api_config_file'])}")
    
    # Load API configurations
    try:
        with open(config["tc_api_config_file"], "r") as f:
            tc_api_calls = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load API configuration: {str(e)}")
        sys.exit(1)
    
    # Load API key
    global tc_api_key
    tc_api_key = tc_load_api_key(config["tc_api_key_file"])
    if not tc_api_key:
        logging.error("Failed to load API key")
        sys.exit(1)
    
    # Process each API endpoint
    success_count = 0
    for api_config in tc_api_calls:
        if process_api_endpoint(config, api_config):
            success_count += 1
    
    # Report final status
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"Processing completed in {duration}")
    logging.info(f"Successfully processed {success_count} out of {len(tc_api_calls)} endpoints")
    
    if success_count < len(tc_api_calls):
        sys.exit(1)

if __name__ == '__main__':
    main()
