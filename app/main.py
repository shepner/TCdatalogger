"""Main entry point for TCdatalogger application."""

import sys
import logging
import argparse
from pathlib import Path

from app.core.config import Config
from app.core.scheduler import EndpointScheduler
from app.services.torncity.registry import EndpointRegistry

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="TCdatalogger - Torn City data collection")
    parser.add_argument("--config-dir", type=Path, default=Path("config"),
                       help="Path to configuration directory")
    parser.add_argument("--run-once", action="store_true",
                       help="Run all endpoints once without scheduling")
    parser.add_argument("--endpoint", type=str,
                       help="Run specific endpoint once")
    args = parser.parse_args()

    try:
        # Initialize configuration
        config = Config(args.config_dir)
        
        # Create scheduler config dictionary with GCP settings
        scheduler_config = {
            'tc_api_config_file': str(config.config_dir / 'TC_API_config.json'),
            'tc_api_key_file': str(config.config_dir / 'TC_API_key.json'),
            'gcp_credentials_file': str(config.config_dir / 'credentials.json'),
            'endpoints': config.endpoints
        }
        
        # Initialize and load registry
        registry = EndpointRegistry()
        registry.load_processors()
        
        if args.endpoint:
            # Run specific endpoint once without scheduler
            endpoint_name = args.endpoint
            if endpoint_name not in config.endpoints:
                logging.error(f"Endpoint not found: {endpoint_name}")
                sys.exit(1)
                
            endpoint_config = config.endpoints[endpoint_name]
            logging.info(f"Processing single endpoint: {endpoint_name}")
            
            # Get the processor directly from registry
            processor_class = registry.get_processor(endpoint_name)
            
            # Create processor config by combining scheduler config and endpoint config
            processor_config = {
                'gcp_credentials_file': scheduler_config['gcp_credentials_file'],
                'endpoint': endpoint_name,
                'storage_mode': endpoint_config.get('storage_mode', 'append'),
                'tc_api_key_file': scheduler_config['tc_api_key_file']
            }
            
            processor = processor_class(processor_config, endpoint_config)
            
            # Process the endpoint
            logging.info(f"Starting {endpoint_name} processing...")
            try:
                processor.run()
                logging.info(f"Successfully processed {endpoint_name}")
            except Exception as e:
                logging.error(f"Error processing {endpoint_name}: {str(e)}")
                raise
            
            logging.info("Single endpoint run completed")
            
        else:
            # Initialize scheduler for normal operation
            scheduler = EndpointScheduler(scheduler_config, registry)
            
            if args.run_once:
                # Run all endpoints once
                logging.info("Running all endpoints once...")
                for endpoint_name, endpoint_config in config.endpoints.items():
                    logging.info(f"Processing endpoint: {endpoint_name}")
                    correlation_id = scheduler.process_endpoint(endpoint_config)
                    logging.info(f"Started processing {endpoint_name} (correlation_id: {correlation_id})")
                    process = scheduler.processes[endpoint_name]
                    process.join()
                logging.info("One-time run completed")
            else:
                # Start the scheduler
                logging.info("Starting scheduler...")
                scheduler.run()
        
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Application error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 