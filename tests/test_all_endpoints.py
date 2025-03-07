"""Integration tests for all API endpoints.

This module provides end-to-end testing of the data pipeline by:
1. Loading configuration
2. Fetching data from each endpoint
3. Processing the data
4. Validating the results
"""

import os
import sys
import json
import logging
from io import StringIO

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from app.common.common import process_data, setup_logging, load_config
from app.svcProviders.TornCity.TornCity import tc_load_api_key, tc_fetch_api_data

def test_endpoint(api_config: dict, api_key: str) -> bool:
    """Test data processing for a single endpoint.
    
    Args:
        api_config: Configuration for the API endpoint.
        api_key: Torn City API key.
        
    Returns:
        bool: True if test was successful, False otherwise.
    """
    logging.info("="*80)
    logging.info(f"Testing endpoint: {api_config['name']}")
    logging.info("="*80)
    
    # Fetch data
    data = tc_fetch_api_data(api_config['url'], api_key)
    if not data:
        logging.error(f"Could not fetch data for {api_config['name']}")
        return False
    
    # Process the data
    logging.info(f"Processing {api_config['name']} data...")
    df = process_data(api_config['name'], data)
    
    # Log DataFrame info
    logging.info("\nDataFrame Info:")
    buffer = StringIO()
    df.info(buf=buffer)
    logging.info(buffer.getvalue())
    
    logging.info("\nDataFrame Head:")
    logging.info(df.head().to_string())
    
    logging.info("\nColumn Types:")
    logging.info(df.dtypes.to_string())
    
    # Log sample of any remaining nested data
    nested_cols = [col for col in df.columns 
                  if df[col].apply(lambda x: isinstance(x, str) and x.startswith('{')).any()]
    if nested_cols:
        logging.info("\nSample of nested data in columns:")
        for col in nested_cols[:3]:  # Show first 3 nested columns
            logging.info(f"\n{col}:")
            sample = df[col].iloc[0]
            if len(sample) > 200:
                logging.info(f"{sample[:200]}...")
            else:
                logging.info(sample)
    
    logging.info(f"\n✅ Successfully processed {api_config['name']}")
    logging.info(f"   Shape: {df.shape}")
    logging.info(f"   Columns: {len(df.columns)}")
    logging.info(f"   Data types: {df.dtypes.value_counts().to_dict()}")
    
    return True

def main() -> None:
    """Run integration tests for all configured endpoints."""
    setup_logging()
    logging.info("Starting endpoint integration tests")
    
    # Load configuration
    config_directories = [
        '/mnt/config',
        '/app/config',
        './config',
        '../config'
    ]
    config = load_config(config_directories)
    if not config:
        logging.error("Failed to load configuration")
        return
    
    # Load API key
    api_key = tc_load_api_key(config["tc_api_key_file"])
    if not api_key:
        logging.error("Failed to load API key")
        return
    
    # Load API configurations
    try:
        with open(config["tc_api_config_file"], "r") as f:
            api_configs = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load API configuration: {str(e)}")
        return
    
    # Test each endpoint
    success_count = 0
    for api_config in api_configs:
        if test_endpoint(api_config, api_key):
            success_count += 1
    
    # Report results
    logging.info("\nTest Results:")
    logging.info(f"Successfully tested {success_count} out of {len(api_configs)} endpoints")
    
    if success_count < len(api_configs):
        logging.error("Some tests failed")
        sys.exit(1)
    else:
        logging.info("All tests passed!")

if __name__ == "__main__":
    main() 