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
from dotenv import load_dotenv

@dataclass
class GoogleConfig:
    """Google Cloud configuration settings."""
    project_id: str
    dataset: str
    credentials_file: Optional[Path] = None

    @classmethod
    def from_env(cls) -> 'GoogleConfig':
        """Create GoogleConfig from environment variables."""
        project_id = os.getenv('GCP_PROJECT_ID')
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
            
        dataset = os.getenv('BIGQUERY_DATASET')
        if not dataset:
            raise ValueError("BIGQUERY_DATASET environment variable is required")
            
        credentials_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        return cls(
            project_id=project_id,
            dataset=dataset,
            credentials_file=Path(credentials_file) if credentials_file else None
        )

@dataclass
class TornConfig:
    """Torn City API configuration settings."""
    api_key: str
    rate_limit: int
    timeout: int

    @classmethod
    def from_env(cls) -> 'TornConfig':
        """Create TornConfig from environment variables."""
        api_key = os.getenv('TORN_API_KEY')
        if not api_key:
            raise ValueError("TORN_API_KEY environment variable is required")
            
        rate_limit = int(os.getenv('API_RATE_LIMIT', '60'))
        timeout = int(os.getenv('API_TIMEOUT', '30'))
        
        return cls(
            api_key=api_key,
            rate_limit=rate_limit,
            timeout=timeout
        )

@dataclass
class AppConfig:
    """Application configuration settings."""
    log_level: str
    config_dir: Path
    enable_metrics: bool
    metric_prefix: str

    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Create AppConfig from environment variables."""
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        if log_level not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
            raise ValueError(f"Invalid log level: {log_level}")
            
        config_dir = os.getenv('CONFIG_DIR', '/config')
        enable_metrics = os.getenv('ENABLE_METRICS', 'true').lower() == 'true'
        metric_prefix = os.getenv('METRIC_PREFIX', 'custom.googleapis.com/tcdatalogger')
        
        return cls(
            log_level=log_level,
            config_dir=Path(config_dir),
            enable_metrics=enable_metrics,
            metric_prefix=metric_prefix
        )

class Config:
    """Central configuration manager."""
    
    def __init__(self):
        """Initialize configuration."""
        # Initialize components
        self.google = GoogleConfig.from_env()
        self.torn = TornConfig.from_env()
        self.app = AppConfig.from_env()
        
        # Set up logging
        self._setup_logging()
        
        # Load endpoint configurations
        self.endpoints = self._load_endpoint_configs()
        
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
        config_file = self.app.config_dir / 'endpoints.json'
        if not config_file.exists():
            raise FileNotFoundError(f"Endpoint configuration file not found: {config_file}")
            
        try:
            with open(config_file) as f:
                configs = json.load(f)
            
            # Validate endpoint configurations
            for name, config in configs.items():
                required_fields = {'table', 'frequency', 'storage_mode'}
                missing_fields = required_fields - set(config.keys())
                if missing_fields:
                    raise ValueError(f"Endpoint {name} missing required fields: {missing_fields}")
            
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