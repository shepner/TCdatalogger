"""Main application entry point for TCdatalogger."""

import logging
from dotenv import load_dotenv
from core.config import Config
from services.torncity.endpoints.members import MembersEndpointProcessor

def main():
    """Main application entry point."""
    # Load environment variables
    load_dotenv()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = Config.load()
        
        # Initialize members processor
        logger.info("Initializing members processor...")
        processor = MembersEndpointProcessor(config)
        
        # Process members data
        logger.info("Processing members data...")
        processor.process()
        
        logger.info("Data processing completed successfully")
        
    except Exception as e:
        logger.error("Application failed: %s", str(e))
        raise

if __name__ == "__main__":
    main() 