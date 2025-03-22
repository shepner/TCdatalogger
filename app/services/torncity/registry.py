"""Endpoint processor registry service.

This module provides automatic discovery and registration of endpoint processors.
It maintains a mapping of endpoint types to their processor classes and handles
processor lookup and instantiation.
"""

import importlib
import inspect
import logging
import pkgutil
import re
from typing import Dict, Type, Optional

from app.services.torncity.base import BaseEndpointProcessor


class EndpointRegistry:
    """Registry for endpoint processors.
    
    This class:
    - Maintains a mapping of endpoint types to processor classes
    - Auto-discovers processors in the endpoints package
    - Provides lookup of appropriate processors for endpoints
    """
    
    def __init__(self):
        """Initialize the registry."""
        self._processors: Dict[str, Type[BaseEndpointProcessor]] = {}
        
    def register(self, endpoint_type: str, processor_class: Type[BaseEndpointProcessor]) -> None:
        """Register a processor class for an endpoint type.
        
        Args:
            endpoint_type: Type of endpoint (e.g., 'crimes', 'members')
            processor_class: Class that processes this endpoint type
        """
        if not issubclass(processor_class, BaseEndpointProcessor):
            raise ValueError(
                f"Processor class {processor_class.__name__} must inherit from BaseEndpointProcessor"
            )
        
        self._processors[endpoint_type] = processor_class
        logging.debug(f"Registered processor {processor_class.__name__} for endpoint type '{endpoint_type}'")
    
    def get_processor(self, endpoint_name: str) -> Optional[Type[BaseEndpointProcessor]]:
        """Get the appropriate processor class for an endpoint.
        
        Args:
            endpoint_name: Name of the endpoint (e.g., 'v2_torn_items', 'v2_faction_40832_crimes')
            
        Returns:
            Type[BaseEndpointProcessor]: The processor class to use
            
        Raises:
            ValueError: If no processor is found for the endpoint
        """
        # Extract base endpoint type from versioned name
        # Example: 'v2_torn_items' -> 'items', 'v2_faction_40832_crimes' -> 'crimes'
        parts = endpoint_name.lower().split('_')
        
        # For faction endpoints, look for the type after the faction ID
        if 'faction' in parts:
            faction_index = parts.index('faction')
            if faction_index + 2 < len(parts):  # Ensure we have parts after faction ID
                endpoint_type = parts[faction_index + 2]  # Skip the faction ID
                if endpoint_type in self._processors:
                    return self._processors[endpoint_type]
        
        # Try each part as a fallback
        for part in parts:
            if part in self._processors:
                return self._processors[part]
        
        raise ValueError(f"No processor found for endpoint: {endpoint_name}")
    
    def load_processors(self) -> None:
        """Auto-discover and load all endpoint processors.
        
        This method:
        1. Scans the endpoints package for processor modules
        2. Imports each module
        3. Finds all classes that inherit from BaseEndpointProcessor
        4. Registers each processor with its endpoint type
        """
        try:
            from app.services.torncity import endpoints
            
            # Scan the endpoints package
            for _, name, _ in pkgutil.iter_modules(endpoints.__path__):
                try:
                    # Import the module
                    module = importlib.import_module(f"app.services.torncity.endpoints.{name}")
                    
                    # Find processor classes
                    for item_name, item in inspect.getmembers(module):
                        if (inspect.isclass(item) and 
                            issubclass(item, BaseEndpointProcessor) and 
                            item != BaseEndpointProcessor):
                            
                            # Register the processor using the module name as the type
                            endpoint_type = name
                            self.register(endpoint_type, item)
                            
                except Exception as e:
                    logging.error(f"Error loading processor module {name}: {str(e)}")
        
        except Exception as e:
            logging.error(f"Error discovering processors: {str(e)}")
            raise
    
    def list_processors(self) -> Dict[str, str]:
        """List all registered processors.
        
        Returns:
            Dict[str, str]: Mapping of endpoint types to processor class names
        """
        return {
            endpoint_type: processor_class.__name__
            for endpoint_type, processor_class in self._processors.items()
        } 