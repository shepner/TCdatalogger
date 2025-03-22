"""Integration tests for the TCdatalogger system.

These tests verify that the entire system works together correctly,
including configuration loading, data processing, and storage.
"""

import os
import json
import tempfile
from pathlib import Path
import pytest
import logging
from app.core.config import Config
from app.services.torncity.client import TornClient
from app.services.torncity.endpoints.members import MembersEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient

@pytest.fixture
def test_config_dir():
    """Create a temporary configuration directory with test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test credentials file
        credentials = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key": "test-key",
            "client_email": "test@example.com"
        }
        with open(temp_path / "credentials.json", "w") as f:
            json.dump(credentials, f)
        
        # Create test API key file
        api_keys = {
            "default": "test-api-key",
            "faction_40832": "test-api-key"
        }
        with open(temp_path / "TC_API_key.json", "w") as f:
            json.dump(api_keys, f)
        
        # Create test endpoints file
        endpoints = {
            "members": {
                "table": "test_members",
                "frequency": "daily",
                "storage_mode": "append",
                "selection": ["basic"],
                "batch_size": 10,
                "max_retries": 1,
                "retry_delay": 1
            }
        }
        with open(temp_path / "endpoints.json", "w") as f:
            json.dump(endpoints, f)
        
        yield temp_path

@pytest.fixture(scope="session")
def setup_test_env(test_config_dir):
    """Set up test environment."""
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    yield test_config_dir

@pytest.fixture(scope="session")
def config(test_config_dir):
    """Load configuration for integration tests."""
    return Config(test_config_dir)

def test_config_loading(test_config_dir):
    """Test that configuration is loaded correctly from files."""
    config = Config(test_config_dir)
    
    # Verify Google Cloud configuration
    assert config.google.project_id == "test-project"
    assert config.google.dataset == "torn_data"
    assert config.google.credentials_file == test_config_dir / "credentials.json"
    
    # Verify Torn API configuration
    assert config.torn.api_key == "test-api-key"
    assert config.torn.rate_limit == 60
    assert config.torn.timeout == 30
    
    # Verify application configuration
    assert config.app.log_level == "INFO"
    assert config.app.config_dir == test_config_dir
    assert config.app.enable_metrics is True
    assert config.app.metric_prefix == "custom.googleapis.com/tcdatalogger"
    
    # Verify endpoint configuration
    assert "members" in config.endpoints
    assert config.endpoints["members"]["table"] == "test_members"
    assert config.endpoints["members"]["frequency"] == "daily"
    assert config.endpoints["members"]["storage_mode"] == "append"

def test_configuration(config):
    """Test configuration loading."""
    assert config is not None
    assert config.app.log_level is not None
    assert config.app.config_dir is not None
    assert config.google.dataset is not None
    logging.info("Configuration loaded successfully")
    logging.info(f"Log Level: {config.app.log_level}")
    logging.info(f"Config Dir: {config.app.config_dir}")
    logging.info(f"Dataset: {config.google.dataset}")

def test_torn_client(config):
    """Test Torn API client."""
    try:
        client = TornClient(config.torn.api_key)
        assert client is not None
        logging.info("Torn client initialized successfully")
    except Exception as e:
        pytest.fail(f"Torn client failed: {str(e)}")

def test_bigquery_client(config):
    """Test BigQuery client."""
    try:
        client = BigQueryClient(config.google.project_id, config.google.credentials_file)
        assert client is not None
        logging.info("BigQuery client initialized successfully")
    except Exception as e:
        pytest.fail(f"BigQuery client failed: {str(e)}")

def test_members_processor(config):
    """Test members endpoint processor."""
    try:
        config_dict = {
            "gcp_project_id": config.google.project_id,
            "gcp_credentials_file": config.google.credentials_file,
            "dataset": config.google.dataset,
            "api_key": config.torn.api_key,
            "storage_mode": "append",
            "endpoint": "members",
            "selection": "default"
        }
        processor = MembersEndpointProcessor(config_dict)
        assert processor is not None
        logging.info("Members processor initialized successfully")
    except Exception as e:
        pytest.fail(f"Members processor failed: {str(e)}") 