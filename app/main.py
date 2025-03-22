"""Main entry point for TCdatalogger application.

This module serves as the main entry point for the TCdatalogger application.
It handles:
- Command line argument parsing
- Configuration loading and validation
- Endpoint processor initialization and execution
- Error handling and logging
"""

import sys
import logging
import argparse
from pathlib import Path

from app.core.config import Config
from app.services.torncity.registry import EndpointRegistry

def main() -> None:
    """Main entry point for TCdatalogger.
    
    This function:
    1. Parses command line arguments
    2. Loads and validates configuration
    3. Initializes the endpoint registry
    4. Processes the specified endpoint
    
    Command line arguments:
        --config-dir: Path to configuration directory containing:
            - TC_API_config.json: Endpoint configurations
            - TC_API_key.json: API key configuration
            - credentials.json: Google Cloud credentials
            - app_config.json: Application settings
        --endpoint: Name of the endpoint to process (must match an endpoint
                   name in TC_API_config.json)
    
    Exit codes:
        0: Success
        1: Error (configuration, processing, or runtime error)
    """
    parser = argparse.ArgumentParser(description="TCdatalogger - Torn City data collection")
    parser.add_argument("--config-dir", type=Path, default=Path("config"),
                       help="Path to configuration directory containing necessary config files")
    parser.add_argument("--endpoint", type=str, required=True,
                       help="Name of the endpoint to process (must match config)")
    args = parser.parse_args()

    try:
        # Initialize configuration
        config = Config(args.config_dir)
        
        # Initialize and load registry
        registry = EndpointRegistry()
        registry.load_processors()
        
        # Run specific endpoint
        endpoint_name = args.endpoint
        if endpoint_name not in config.endpoints:
            logging.error(f"Endpoint not found: {endpoint_name}")
            sys.exit(1)
            
        endpoint_config = config.endpoints[endpoint_name]
        logging.info(f"Processing endpoint: {endpoint_name}")
        
        # Get the processor directly from registry
        processor_class = registry.get_processor(endpoint_name)
        
        # Create processor config
        processor_config = {
            'gcp_credentials_file': str(config.config_dir / 'credentials.json'),
            'endpoint': endpoint_name,
            'storage_mode': endpoint_config.get('storage_mode', 'append'),
            'tc_api_key_file': str(config.config_dir / 'TC_API_key.json'),
            'endpoint_config': endpoint_config,  # Add the full endpoint config
            'url': endpoint_config.get('url'),  # Add the URL from endpoint config
            'table': endpoint_config.get('table'),  # Add the table from endpoint config
            'frequency': endpoint_config.get('frequency'),  # Add the frequency from endpoint config
            'api_key': endpoint_config.get('api_key', 'default'),  # Add the API key name
            'app_config': {  # Add app config settings
                'log_level': config.app.log_level,
                'enable_metrics': config.app.enable_metrics,
                'metric_prefix': config.app.metric_prefix,
                'google_cloud': {
                    'credentials_file': str(config.google.credentials_file)
                }
            }
        }
        
        # Create processor with config
        processor = processor_class(processor_config)
        
        # Process the endpoint
        logging.info(f"Starting {endpoint_name} processing...")
        try:
            processor.run()
            logging.info(f"Successfully processed {endpoint_name}")
        except Exception as e:
            logging.error(f"Error processing {endpoint_name}: {str(e)}")
            raise
        
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Application error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 