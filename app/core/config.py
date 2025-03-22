"""Configuration management for TCdatalogger.

This module provides centralized configuration management with validation
for all application settings.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class GoogleConfig:
    """Google Cloud configuration settings."""
    credentials_file: Optional[Path] = None

    @classmethod
    def from_file(cls, credentials_file: Path) -> 'GoogleConfig':
        """Create GoogleConfig from credentials file."""
        if not credentials_file.exists():
            raise FileNotFoundError(f"Google credentials file not found: {credentials_file}")
            
        try:
            with open(credentials_file) as f:
                credentials = json.load(f)
            
            return cls(
                credentials_file=credentials_file
            )
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid Google credentials file: {str(e)}")

@dataclass
class TornConfig:
    """Torn City API configuration settings."""
    api_key: str
    rate_limit: int
    timeout: int

    @classmethod
    def from_file(cls, api_key_file: Path) -> 'TornConfig':
        """Create TornConfig from API key file."""
        if not api_key_file.exists():
            raise FileNotFoundError(f"Torn API key file not found: {api_key_file}")
            
        try:
            with open(api_key_file) as f:
                api_keys = json.load(f)
            
            return cls(
                api_key=api_keys['default'],
                rate_limit=60,  # Default values from previous env vars
                timeout=30
            )
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid Torn API key file: {str(e)}")

@dataclass
class AppConfig:
    """Application configuration settings."""
    log_level: str
    config_dir: Path
    enable_metrics: bool
    metric_prefix: str

    @classmethod
    def from_defaults(cls, config_dir: Path) -> 'AppConfig':
        """Create AppConfig with default values."""
        return cls(
            log_level='INFO',
            config_dir=config_dir,
            enable_metrics=True,
            metric_prefix='custom.googleapis.com/tcdatalogger'
        )

class Config:
    """Central configuration manager."""
    
    def __init__(self, config_dir: Path = Path('config')):
        """Initialize configuration."""
        self.config_dir = config_dir
        if not self.config_dir.exists():
            raise FileNotFoundError(f"Configuration directory not found: {self.config_dir}")
        
        # Load app config first
        self.app_config = self._load_app_config()
        
        # Initialize components
        self.google = GoogleConfig.from_file(self.config_dir / 'credentials.json')
        self.torn = TornConfig.from_file(self.config_dir / 'TC_API_key.json')
        self.app = AppConfig.from_defaults(self.config_dir)
        
        # Set up logging
        self._setup_logging()
        
        # Load endpoint configurations
        self.endpoints = self._load_endpoint_configs()
        
    def _load_app_config(self) -> Dict[str, Any]:
        """Load application configuration from app_config.json."""
        config_file = self.config_dir / 'app_config.json'
        if not config_file.exists():
            raise FileNotFoundError(f"Application configuration file not found: {config_file}")
            
        try:
            with open(config_file) as f:
                config = json.load(f)
            return config
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid application configuration JSON: {e}")
            
    def _setup_logging(self) -> None:
        """Configure logging based on settings."""
        logging.basicConfig(
            level=getattr(logging, self.app.log_level),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
    
    def _load_endpoint_configs(self) -> Dict[str, Any]:
        """Load endpoint configurations from JSON files."""
        config_file = self.app.config_dir / 'TC_API_config.json'
        if not config_file.exists():
            raise FileNotFoundError(f"Endpoint configuration file not found: {config_file}")
            
        try:
            with open(config_file) as f:
                config = json.load(f)
            
            # Convert the endpoints list to a dictionary
            configs = {}
            for endpoint in config.get('endpoints', []):
                name = endpoint.get('name')
                if not name:
                    raise ValueError("Endpoint missing required 'name' field")
                
                # Store the endpoint config as is
                configs[name] = endpoint
            
            return configs
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid endpoint configuration JSON: {e}")
    
    @classmethod
    def load(cls) -> 'Config':
        """Load and validate configuration.
        
        Returns:
            Config: Validated configuration instance
            
        Raises:
            ValueError: If configuration is invalid
        """
        try:
            return cls()
        except Exception as e:
            logging.error("Failed to load configuration: %s", str(e))
            raise 