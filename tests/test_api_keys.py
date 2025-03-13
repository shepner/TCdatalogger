"""Test script for API key functionality."""

import os
import sys
import json
import logging

# Add app directory to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
app_path = os.path.join(project_root, 'app', 'src')
sys.path.insert(0, app_path)

from app.svcProviders.TornCity.TornCity import tc_load_api_key
from app.common.common import process_api_endpoint

def test_api_keys():
    """Test loading and using API keys."""
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Load API keys
    api_key_path = os.path.join(project_root, 'app', 'config', 'TC_API_key.json')
    api_keys = tc_load_api_key(api_key_path)
    if not api_keys:
        print("Failed to load API keys")
        return False
        
    print("API keys loaded successfully:", list(api_keys.keys()))
    
    # Load API configuration
    config_path = os.path.join(project_root, 'app', 'config', 'TC_API_config.json')
    with open(config_path, 'r') as f:
        api_configs = json.load(f)['endpoints']
        
    # Find an endpoint that uses a specific API key
    test_endpoint = None
    for endpoint in api_configs:
        if endpoint.get('api_key') in api_keys:
            test_endpoint = endpoint
            break
            
    if not test_endpoint:
        print("No suitable endpoint found for testing")
        return False
        
    # Test the endpoint
    config = {'config_dir': os.path.join(project_root, 'app', 'config')}
    print("\nTesting endpoint:", test_endpoint['name'])
    result = process_api_endpoint(config, test_endpoint, api_keys)
    print("Endpoint test result:", result)
    
    return result

if __name__ == '__main__':
    test_api_keys()  # Main entry point for running the test