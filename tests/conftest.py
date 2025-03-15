"""Test configuration and shared fixtures."""

import json
import os
from pathlib import Path
from typing import Dict, Any

import pytest
from google.cloud import bigquery

# Constants
TEST_DATA_DIR = Path(__file__).parent / "fixtures"
TEST_CONFIG = {
    "tc_api_key_file": str(TEST_DATA_DIR / "api_keys.json"),
    "gcp_project_id": "test-project",
    "dataset": "test_dataset",
    "storage_mode": "append"
}

@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """Provide sample configuration for testing."""
    return TEST_CONFIG.copy()

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
        "table": "project.dataset.test_table",
        "api_key": "default",
        "storage_mode": "append",
        "frequency": "PT15M"
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