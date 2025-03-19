"""Integration tests for the TCdatalogger system."""

import os
import json
import pytest
import logging
from pathlib import Path
from dotenv import load_dotenv
from app.core.config import Config
from app.services.torncity.client import TornClient
from app.services.torncity.endpoints.members import MembersEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient

@pytest.fixture(scope="session")
def test_config_dir(tmp_path_factory):
    """Create a temporary config directory with test endpoints."""
    config_dir = tmp_path_factory.mktemp("config")
    
    # Create mock endpoints.json
    endpoints = {
        "members": {
            "description": "Torn City faction members data",
            "table": "members",
            "frequency": 3600,
            "storage_mode": "append",
            "schema": {
                "fields": [
                    {"name": "server_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "level", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "days_in_faction", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "last_action", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "status", "type": "STRING", "mode": "REQUIRED"}
                ]
            }
        }
    }
    
    with open(config_dir / "endpoints.json", "w") as f:
        json.dump(endpoints, f)
        
    return config_dir

@pytest.fixture(scope="session")
def setup_test_env(test_config_dir):
    """Set up test environment variables."""
    # Load test environment variables
    env_file = Path(__file__).parent.parent / "fixtures" / ".env.test"
    load_dotenv(env_file)
    
    # Override config dir with temp directory
    os.environ["CONFIG_DIR"] = str(test_config_dir)
    
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    
    yield
    
    # Cleanup
    os.environ.pop("CONFIG_DIR", None)

@pytest.fixture(scope="session")
def config(setup_test_env):
    """Load configuration for integration tests."""
    return Config.load()

def test_config_loading(setup_test_env):
    """Test that configuration loads correctly."""
    config = Config.load()
    
    # Test Google Cloud settings
    assert config.google.project_id == "test-project-id"
    assert config.google.dataset == "torn"
    
    # Test Torn API settings
    assert config.torn.api_key == "abcd1234efgh5678"
    assert config.torn.rate_limit == 60
    assert config.torn.timeout == 30
    
    # Test App settings
    assert config.app.enable_metrics is True
    assert "members" in config.endpoints
    
    # Test endpoint configuration
    members = config.endpoints["members"]
    assert members["table"] == "members"
    assert members["frequency"] == 3600
    assert members["storage_mode"] == "append"
    assert len(members["schema"]["fields"]) == 7

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