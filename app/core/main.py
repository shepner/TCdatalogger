"""Main application entry point for TCdatalogger.

This module orchestrates the data pipeline by:
1. Setting up logging and configuration
2. Initializing services
3. Starting the scheduler
"""

import sys
import logging
import argparse
from typing import NoReturn

from app.core.common import setup_logging, load_config
from app.core.scheduler import EndpointScheduler
from app.services.torncity.registry import EndpointRegistry
from app.services.torncity.client import TornClient

def initialize_services(config: dict) -> tuple[EndpointRegistry, EndpointScheduler]:
    """Initialize application services.
    
    Args:
        config: Application configuration
        
    Returns:
        tuple: (registry, scheduler) initialized services
        
    Raises:
        RuntimeError: If service initialization fails
    """
    try:
        # Initialize registry
        registry = EndpointRegistry()
        registry.load_processors()  # Auto-discovers and registers processors
        
        # Initialize scheduler with registry
        scheduler = EndpointScheduler(config, registry)
        
        return registry, scheduler
        
    except Exception as e:
        raise RuntimeError(f"Failed to initialize services: {str(e)}")

def main() -> NoReturn:
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(description="TCdatalogger - Torn City data collection")
    parser.add_argument("--config-dir", help="Configuration directory")
    args = parser.parse_args()
    
    try:
        # Set up logging first
        setup_logging()
        
        # Load configuration
        config = load_config(args.config_dir)
        if not config:
            logging.error("Failed to load configuration")
            sys.exit(1)
        
        # Initialize services
        registry, scheduler = initialize_services(config)
        
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
