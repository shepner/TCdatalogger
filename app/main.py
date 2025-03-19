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
    args = parser.parse_args()

    try:
        # Initialize configuration
        config = Config(args.config_dir)
        
        # Initialize services
        registry = EndpointRegistry()
        scheduler = EndpointScheduler(config, registry)
        
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