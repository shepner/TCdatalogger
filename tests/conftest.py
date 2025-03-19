"""Test configuration and shared fixtures."""

import json
import os
from pathlib import Path
from typing import Dict, Any

import pytest
from google.cloud import bigquery
from unittest.mock import MagicMock, patch
import google.auth.credentials
import google.cloud.bigquery
import requests

# Constants
TEST_DATA_DIR = Path(__file__).parent / "fixtures"
TEST_CONFIG = {
    "tc_api_key_file": str(TEST_DATA_DIR / "api_keys.json"),
    "gcp_project_id": "test-project",
    "gcp_credentials_file": str(TEST_DATA_DIR / "test_credentials.json"),
    "dataset": "test_dataset",
    "storage_mode": "append",
    "endpoint": "user",
    "selection": "default",
    "api_key": "test_key_1"  # Direct API key for testing
}

@pytest.fixture
def mock_api_keys():
    """Provide mock API keys for testing."""
    return {
        "default": "test_key_1",
        "secondary": "test_key_2"
    }

@pytest.fixture
def sample_config(mock_credentials, mock_api_keys) -> Dict[str, Any]:
    """Provide sample configuration for testing."""
    config = TEST_CONFIG.copy()
    config["gcp_credentials_file"] = mock_credentials
    config["api_keys"] = mock_api_keys
    return config

@pytest.fixture
def mock_api_keys(tmp_path) -> str:
    """Create a temporary API keys file for testing."""
    api_keys = {
        "default": "test_key_1",
        "secondary": "test_key_2"
    }
    keys_file = tmp_path / "api_keys.json"
    with open(keys_file, "w") as f:
        json.dump(api_keys, f)
    return str(keys_file)

@pytest.fixture
def mock_credentials(tmp_path) -> str:
    """Create a temporary service account credentials file for testing."""
    credentials = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "test_key_id",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC9QFbR1T6TBwZ5\n-----END PRIVATE KEY-----\n",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test@test-project.iam.gserviceaccount.com"
    }
    creds_file = tmp_path / "test_credentials.json"
    with open(creds_file, "w") as f:
        json.dump(credentials, f)
    return str(creds_file)

@pytest.fixture
def sample_bigquery_schema() -> list:
    """Provide a sample BigQuery schema for testing."""
    return [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("value", "FLOAT", mode="NULLABLE")
    ]

@pytest.fixture
def sample_endpoint_config() -> Dict[str, Any]:
    """Provide sample endpoint configuration for testing."""
    return {
        "name": "test_endpoint",
        "url": "https://api.torn.com/user/{API_KEY}",
        "table": "test_dataset.test_table",
        "api_key": "default",
        "storage_mode": "append",
        "frequency": "PT15M",
        "required_fields": ["player_id", "name", "level", "gender"],
        "numeric_fields": ["player_id", "level", "energy", "max_energy", "nerve", "max_nerve", "happy", "max_happy", "life", "max_life", "chain", "max_chain", "money", "points"],
        "timestamp_fields": ["last_action", "timestamp"]
    }

def load_fixture(name: str) -> Dict[str, Any]:
    """Load a JSON fixture file.
    
    Args:
        name: Name of the fixture file (without .json extension)
        
    Returns:
        Dict[str, Any]: Loaded fixture data
    """
    fixture_path = TEST_DATA_DIR / f"{name}.json"
    with open(fixture_path) as f:
        return json.load(f)

@pytest.fixture(autouse=True)
def mock_google_auth(monkeypatch):
    """Mock Google Auth credentials."""
    mock_creds = MagicMock(spec=google.auth.credentials.Credentials)
    
    def mock_from_service_account_file(*args, **kwargs):
        return mock_creds
    
    monkeypatch.setattr(
        'google.oauth2.service_account.Credentials.from_service_account_file',
        mock_from_service_account_file
    )
    
    # Mock os.path.exists to return True for credential files
    original_exists = os.path.exists
    def mock_exists(path):
        if 'test_credentials.json' in str(path):
            return True
        return original_exists(path)
    
    monkeypatch.setattr('os.path.exists', mock_exists)
    return mock_creds

@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client for testing."""
    with patch('google.cloud.bigquery.Client') as mock_client:
        mock_client.return_value.project = 'test-project'
        mock_client.return_value.get_table.return_value = None
        mock_client.return_value.create_table.return_value = None
        mock_client.return_value.load_table_from_dataframe.return_value = None
        mock_client.return_value.dataset.return_value = None
        mock_client.return_value.table.return_value = None
        mock_client.return_value.query.return_value = None
        mock_client.return_value.insert_rows_json.return_value = []
        return mock_client.return_value

@pytest.fixture
def mock_credentials():
    """Mock Google Cloud credentials."""
    with patch('google.oauth2.service_account.Credentials') as mock_creds:
        mock_creds.from_service_account_file.return_value = mock_creds
        return mock_creds

@pytest.fixture
def mock_torn_api(monkeypatch):
    """Mock Torn API responses"""
    def mock_get(*args, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": True, "data": {"test": "data"}}
        return mock_response
        
    def mock_get_rate_limit(*args, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.json.return_value = {"error": {"code": 5, "error": "Too many requests"}}
        return mock_response
        
    def mock_get_timeout(*args, **kwargs):
        raise requests.exceptions.Timeout("Request timed out")
        
    monkeypatch.setattr(requests, "get", mock_get)
    return {
        "normal": mock_get,
        "rate_limit": mock_get_rate_limit,
        "timeout": mock_get_timeout
    } 