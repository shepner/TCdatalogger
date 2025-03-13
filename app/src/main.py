"""Main application entry point for TCdatalogger.

This module orchestrates the data pipeline:
1. Sets up logging
2. Loads configuration
3. Processes API endpoints according to their schedules
4. Reports results

The application will:
- Load configuration from the first valid config directory
- Process each configured API endpoint on its defined schedule
- Upload data to BigQuery
- Report success/failure for each endpoint
"""

import os
import sys
import json
import time
import logging
import argparse
import threading
from datetime import datetime, timedelta
from typing import NoReturn, Dict
import isodate
import schedule

from app.common.common import setup_logging, load_config, process_api_endpoint
from app.svcProviders.TornCity.TornCity import tc_load_api_key
from app.svcProviders.Google.Google import drop_tables

def schedule_endpoint(config: Dict, api_config: Dict, api_keys: Dict[str, str]) -> None:
    """Schedule and process an API endpoint.
    
    Args:
        config: Application configuration
        api_config: API endpoint configuration
        api_keys: Dictionary mapping API key identifiers to their values
    """
    logging.info(f"Processing endpoint: {api_config['name']}")
    if not process_api_endpoint(config, api_config, api_keys):
        logging.error(f"Failed to process endpoint: {api_config['name']}")

def setup_schedules(config: Dict, api_configs: list, api_keys: Dict[str, str]) -> None:
    """Set up schedules for all endpoints.
    
    Args:
        config: Application configuration
        api_configs: List of API endpoint configurations
        api_keys: Dictionary mapping API key identifiers to their values
    """
    for api_config in api_configs:
        # Convert ISO duration to minutes
        duration = isodate.parse_duration(api_config['frequency'])
        minutes = int(duration.total_seconds() / 60)
        
        # Schedule the job
        schedule.every(minutes).minutes.do(
            schedule_endpoint, 
            config=config, 
            api_config=api_config, 
            api_keys=api_keys
        )
        
        logging.info(f"Scheduled {api_config['name']} to run every {minutes} minutes")

def main() -> NoReturn:
    """Main application entry point.
    
    This function:
    1. Initializes logging
    2. Loads configuration and API keys
    3. Sets up schedules for each endpoint
    4. Runs the scheduler
    
    The function will exit with status code 1 if:
    - Configuration cannot be loaded
    - API keys are invalid
    
    Returns:
        NoReturn: Function runs indefinitely unless error occurs
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
            
        # Load API keys
        api_key_path = os.path.join(config['config_dir'], 'TC_API_key.json')
        api_keys = tc_load_api_key(api_key_path)
        if not api_keys:
            logging.error("Failed to load API keys")
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
            
        # Set up schedules for all endpoints
        setup_schedules(config, api_configs, api_keys)
        
        # Run all jobs immediately on startup
        logging.info("Running initial jobs...")
        for api_config in api_configs:
            schedule_endpoint(config, api_config, api_keys)
            
        # Run the scheduler
        logging.info("Starting scheduler...")
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    except Exception as e:
        logging.error("Unexpected error: %s", str(e))
        sys.exit(1)

if __name__ == '__main__':
    main()
