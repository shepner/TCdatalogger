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
from datetime import datetime
from typing import NoReturn

from app.common.common import setup_logging, load_config, process_api_endpoint
from app.svcProviders.TornCity.TornCity import tc_load_api_key

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
    try:
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
            logging.error("Failed to load configuration")
            sys.exit(1)
        
        logging.info(f"Using configuration from: {os.path.dirname(config['tc_api_config_file'])}")
        
        # Load API configurations
        try:
            with open(config["tc_api_config_file"], "r") as f:
                tc_api_calls = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Failed to load API configuration: {str(e)}")
            sys.exit(1)
        
        # Load API key
        tc_api_key = tc_load_api_key(config["tc_api_key_file"])
        if not tc_api_key:
            logging.error("Failed to load API key")
            sys.exit(1)
        
        # Process each API endpoint
        success_count = 0
        for api_config in tc_api_calls:
            if process_api_endpoint(config, api_config, tc_api_key):
                success_count += 1
        
        # Report final status
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Processing completed in {duration}")
        logging.info(f"Successfully processed {success_count} out of {len(tc_api_calls)} endpoints")
        
        if success_count < len(tc_api_calls):
            sys.exit(1)
        sys.exit(0)
        
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
