"""Main application entry point for TCdatalogger.

This module orchestrates the data pipeline:
1. Sets up logging
2. Loads configuration
3. Processes API endpoints
4. Reports results

The application will:
- Load configuration from the first valid config directory
- Process each configured API endpoint
- Upload data to BigQuery
- Report success/failure for each endpoint
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from typing import NoReturn

from app.common.common import setup_logging, load_config, process_api_endpoint
from app.svcProviders.TornCity.TornCity import tc_load_api_key
from app.svcProviders.Google.Google import drop_tables

def main() -> NoReturn:
    """Main application entry point.
    
    This function:
    1. Initializes logging
    2. Loads configuration and API key
    3. Processes each configured endpoint
    4. Reports results
    
    The function will exit with status code 1 if:
    - Configuration cannot be loaded
    - API key is invalid
    - Any endpoint fails to process
    
    Returns:
        NoReturn: Function always exits via sys.exit
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='TCdatalogger - Torn City Data Logger')
    parser.add_argument('--drop-tables', action='store_true', help='Drop and recreate all tables')
    args = parser.parse_args()

    # Initialize logging
    setup_logging()
    logging.info("Starting TCdatalogger")
    
    try:
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
            logging.error("Failed to load configuration")
            sys.exit(1)
            
        # Load API key
        api_key_path = os.path.join(config['config_dir'], 'TC_API_key.txt')
        tc_api_key = tc_load_api_key(api_key_path)
        if not tc_api_key:
            logging.error("Failed to load API key")
            sys.exit(1)
            
        # Load API configuration
        api_config_path = os.path.join(config['config_dir'], 'TC_API_config.json')
        try:
            with open(api_config_path, 'r') as f:
                api_config_data = json.load(f)
                api_configs = api_config_data.get('endpoints', [])
                if not api_configs:
                    logging.error("No endpoints found in API configuration")
                    sys.exit(1)
        except Exception as e:
            logging.error("Failed to load API configuration: %s", str(e))
            sys.exit(1)

        # Drop tables if requested
        if args.drop_tables:
            logging.info("Dropping all tables...")
            for api_config in api_configs:
                drop_tables(config, api_config["table"])
            logging.info("All tables dropped")
            
        # Process timestamp endpoint first
        timestamp_config = next((cfg for cfg in api_configs if cfg['name'] == 'server_timestamp'), None)
        if timestamp_config:
            logging.info("Processing timestamp endpoint first")
            if not process_api_endpoint(config, timestamp_config, tc_api_key):
                logging.error("Failed to process timestamp endpoint")
                sys.exit(1)
                
        # Process remaining endpoints
        success_count = 0
        for api_config in api_configs:
            if api_config['name'] != 'server_timestamp':  # Skip timestamp endpoint as it's already processed
                if process_api_endpoint(config, api_config, tc_api_key):
                    success_count += 1
                    
        # Report results
        total_endpoints = len(api_configs) - (1 if timestamp_config else 0)
        logging.info("Processing complete. Success: %d/%d", success_count, total_endpoints)
        
        if success_count < total_endpoints:
            sys.exit(1)
            
    except Exception as e:
        logging.error("Unexpected error: %s", str(e))
        sys.exit(1)
        
    sys.exit(0)

if __name__ == '__main__':
    main()
