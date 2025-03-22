"""Torn City API integration package.

This package provides a comprehensive integration with the Torn City API,
including:

Components:
    - API client with rate limiting and error handling
    - Base processor for endpoint data processing
    - Specialized endpoint processors for different data types
    - Registry for automatic processor discovery

Endpoint Processors:
    - BasicFactionEndpointProcessor: Basic faction data
    - CrimesEndpointProcessor: Faction crimes data
    - CurrencyEndpointProcessor: Currency market data
    - ItemsEndpointProcessor: Items market data
    - MembersEndpointProcessor: Faction members data

Usage:
    from app.services.torncity import TornClient, BaseEndpointProcessor
    
    # Create API client
    client = TornClient(api_key="your_key")
    
    # Use endpoint processors
    processor = ItemsEndpointProcessor(config)
    processor.process()
"""

from .client import TornClient
from .base import BaseEndpointProcessor
from .endpoints.basic import BasicFactionEndpointProcessor
from .endpoints.crimes import CrimesEndpointProcessor
from .endpoints.currency import CurrencyEndpointProcessor
from .endpoints.items import ItemsEndpointProcessor
from .endpoints.members import MembersEndpointProcessor

__all__ = [
    'TornClient',
    'BaseEndpointProcessor',
    'BasicFactionEndpointProcessor',
    'CrimesEndpointProcessor',
    'CurrencyEndpointProcessor',
    'ItemsEndpointProcessor',
    'MembersEndpointProcessor',
] 