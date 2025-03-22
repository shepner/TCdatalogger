"""Unit tests for crimes endpoint processor."""

import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, mock_open, Mock
import time
from typing import Any
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from google.cloud import bigquery, monitoring_v3
from google.oauth2 import service_account
import numpy as np
import unittest

from app.services.torncity.client import TornClient
from app.services.torncity.endpoints.crimes import CrimesEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.client import TornAPIError


class TestCrimesEndpointProcessor(CrimesEndpointProcessor):
    """Test implementation of CrimesEndpointProcessor."""

    def get_schema(self) -> list:
        """Get schema for crimes data."""
        return [
            bigquery.SchemaField("server_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("difficulty", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("planning_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("executed_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("ready_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("expired_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("slots_position", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("slots_item_requirement_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("slots_item_requirement_is_reusable", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("slots_item_requirement_is_available", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("slots_user_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("slots_user_joined_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("slots_user_progress", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("slots_success_chance", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("slots_crime_pass_rate", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rewards_money", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rewards_items_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rewards_items_quantity", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rewards_respect", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rewards_payout_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rewards_payout_percentage", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rewards_payout_paid_by", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rewards_payout_paid_at", "TIMESTAMP", mode="NULLABLE")
        ]


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
            "default": "test_api_key",
            "faction_40832": "test_api_key"
        }
        with open(temp_path / "TC_API_key.json", "w") as f:
            json.dump(api_keys, f)
        
        # Create test endpoints file
        endpoints = {
            "crimes": {
                "table": "test_crimes",
                "frequency": "daily",
                "storage_mode": "append",
                "selection": ["basic"],
                "max_retries": 1,
                "retry_delay": 1
            }
        }
        with open(temp_path / "endpoints.json", "w") as f:
            json.dump(endpoints, f)
        
        yield temp_path


@pytest.fixture
def crimes_processor(test_config_dir):
    """Create a test crimes processor instance."""
    config = {
        'gcp_project_id': 'test-project',
        'gcp_credentials_file': str(test_config_dir / 'credentials.json'),
        'dataset': 'test_dataset',
        'endpoint': 'crimes',
        'selection': 'default',
        'storage_mode': 'append'
    }
    return TestCrimesEndpointProcessor(config=config)


@pytest.fixture
def mock_monitoring_client():
    """Mock Google Cloud Monitoring client."""
    mock_client = MagicMock(spec=monitoring_v3.MetricServiceClient)
    mock_client.common_project_path.return_value = "projects/test-project"
    with patch('google.cloud.monitoring_v3.MetricServiceClient', return_value=mock_client):
        yield mock_client


@pytest.fixture(scope='function')
def mock_credentials(monkeypatch):
    """Mock Google Cloud credentials."""
    mock_creds = MagicMock(spec=service_account.Credentials)
    mock_creds.project_id = "test-project"
    
    # Mock the credentials file content
    mock_creds_content = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "test_key_id",
        "private_key": "test_private_key",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "client_id": "test_client_id"
    }
    
    # Mock file operations
    mock_file = mock_open(read_data=json.dumps(mock_creds_content))
    monkeypatch.setattr('builtins.open', mock_file)
    monkeypatch.setattr(os.path, 'exists', lambda x: True)
    
    # Mock the from_service_account_file method
    def mock_from_service_account_file(*args, **kwargs):
        return mock_creds
    monkeypatch.setattr(
        service_account.Credentials,
        'from_service_account_file',
        mock_from_service_account_file
    )
    
    # Mock google.auth.default
    def mock_auth_default(*args, **kwargs):
        return mock_creds, 'test-project'
    monkeypatch.setattr('google.auth.default', mock_auth_default)
    
    return mock_creds


@pytest.fixture
def sample_config(test_config_dir):
    """Create sample config for testing."""
    return {
        "dataset": "test_dataset",
        "endpoint_config": {
            "crimes": {
                "name": "crimes",
                "table": "test_crimes",
                "endpoint": "/v2/faction/crimes"
            }
        },
        "gcp_credentials_file": str(test_config_dir / "credentials.json"),
        "tc_api_key_file": str(test_config_dir / "TC_API_key.json")
    }


@pytest.fixture
def mock_api_keys():
    """Mock Torn API keys."""
    return {
        'crimes': 'test_key_2',
        'default': 'test_key_1'
    }


@pytest.fixture
def torn_client(mock_api_keys):
    """Create TornClient with mocked API keys."""
    with patch.object(TornClient, "_load_api_keys", return_value=mock_api_keys):
        client = TornClient("dummy_path")
        return client


@pytest.fixture(scope='function')
def bq_client(mock_credentials, sample_config):
    """Create a BigQuery client for testing."""
    return BigQueryClient(sample_config)


@pytest.fixture
def mock_crimes_response():
    """Mock response from crimes endpoint."""
    return {
        "crimes": {
            "1": {
                "id": 1,
                "name": "Test Crime",
                "difficulty": 2,
                "status": "completed",
                "created_at": 1646956800,
                "planning_at": 1646956800,
                "executed_at": 1646960400,
                "ready_at": 1646960400,
                "expired_at": 1646960400,
                "participants": [
                    {"id": 12345, "name": "Player1"},
                    {"id": 67890, "name": "Player2"}
                ],
                "rewards": {
                    "money": 1000000,
                    "respect": 100,
                    "items": [
                        {"id": 1, "quantity": 2},
                        {"id": 2, "quantity": 1}
                    ]
                }
            }
        },
        "timestamp": 1646960400,
        "fetched_at": datetime.now()
    }


class TestCrimesPull(unittest.TestCase):
    """Test cases for crimes endpoint processor."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_client.get_endpoint_data.return_value = {}
        self.mock_client.get_endpoint_name.return_value = 'crimes'
        self.mock_client.get_endpoint_version.return_value = 'v2'
        self.mock_client.get_endpoint_path.return_value = '/faction/crimes'
        self.mock_client.get_endpoint_params.return_value = {}
        self.mock_client.get_endpoint_interval.return_value = 60
        self.mock_client.get_endpoint_timeout.return_value = 30
        self.mock_client.get_endpoint_retries.return_value = 3

    def _get_processor(self):
        """Get a test processor instance."""
        config = {
            'gcp_project_id': 'test-project',
            'gcp_credentials_file': 'test_credentials.json',
            'dataset': 'test_dataset',
            'endpoint': 'crimes',
            'selection': 'default',
            'storage_mode': 'append'
        }
        return TestCrimesEndpointProcessor(config=config)

    def test_empty_records_list(self):
        """Test handling of empty records list."""
        crimes_processor = self._get_processor()
        
        data = {
            "data": {
                "crimes": {}  # Empty crimes dictionary
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert df.empty

    def test_crimes_bigquery_integration(self):
        """Test integration with BigQuery."""
        # Mock credentials and clients
        mock_credentials = MagicMock(spec=service_account.Credentials)
        mock_bigquery_client = MagicMock(spec=bigquery.Client)
        mock_monitoring_client = MagicMock(spec=monitoring_v3.MetricServiceClient)
        mock_table = Mock()
        mock_bigquery_client.get_table.return_value = mock_table

        # Set up test configuration
        config = {
            'api_key': 'test_api_key',
            'gcp_project_id': 'test-project',
            'gcp_credentials_file': 'test_credentials.json',
            'dataset': 'test_dataset',
            'endpoint': 'v2/faction/crimes',
            'selection': 'default',
            'storage_mode': 'append',
            'table': 'crimes',
            'bigquery_client': mock_bigquery_client,
            'monitoring_client': mock_monitoring_client
        }

        # Create processor instance
        crimes_processor = TestCrimesEndpointProcessor(config=config)

        # Test data
        test_data = {
            "data": {
                "crimes": {
                    "123": {
                        "id": 123,
                        "name": "Test Crime",
                        "status": "completed",
                        "created_at": 1710633600
                    }
                }
            }
        }

        # Process data
        crimes_processor.process_data(test_data)

        # Verify BigQuery interactions
        mock_bigquery_client.get_table.assert_called_once()
        mock_bigquery_client.load_table_from_dataframe.assert_called_once()

    def test_safe_int_convert_edge_cases(self):
        """Test edge cases for _safe_int_convert method."""
        crimes_processor = self._get_processor()
        
        test_cases = [
            (None, 0),
            ('', 0),
            ('invalid', 0),
            ('123.45', 123),
            (123.45, 123),
            (True, 1),
            (False, 0),
            ([1, 2, 3], 0),
            ({'a': 1}, 0),
            (pd.NA, 0),
            (pd.NaT, 0),
            (float('nan'), 0),
            (float('inf'), 0),
            (float('-inf'), 0),
            (2147483647, 2147483647),  # Max 32-bit int
            (-2147483648, -2147483648),  # Min 32-bit int
            (9223372036854775807, 9223372036854775807),  # Max 64-bit int
            (-9223372036854775808, -9223372036854775808)  # Min 64-bit int
        ]
        
        for input_value, expected_output in test_cases:
            result = crimes_processor._safe_int_convert(input_value)
            assert result == expected_output, f"Failed for input {input_value}"

    def test_convert_timestamps_method(self):
        """Test the convert_timestamps method directly."""
        crimes_processor = self._get_processor()
        df = pd.DataFrame({
            'created_at': ['2024-03-16T12:00:00', 'invalid_date'],
            'planning_at': [1742170362, 'invalid_timestamp'],
            'not_a_timestamp': ['a', 'b']
        })
        
        result = crimes_processor.convert_timestamps(df, exclude_cols=['not_a_timestamp'])
        assert pd.api.types.is_datetime64_dtype(result['created_at'])
        assert pd.api.types.is_datetime64_dtype(result['planning_at'])
        assert result['not_a_timestamp'].dtype == object
        assert pd.isna(result.loc[1, 'created_at'])
        assert pd.isna(result.loc[1, 'planning_at'])

    def test_timestamp_formats(self):
        """Test handling of different timestamp formats."""
        crimes_processor = self._get_processor()
        
        # Test data with various timestamp formats
        data = {
            'timestamp': 1710633600,  # Unix timestamp
            'fetched_at': '2024-03-17T00:00:00',  # ISO format
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'created_at': 1710633600,  # Unix timestamp
                    'planning_at': None,  # None value
                    'executed_at': 'invalid_date',  # Invalid string
                    'ready_at': '2024-03-17T00:00:00',  # String timestamp
                    'expired_at': 1710720000,  # Unix timestamp
                    'difficulty': 2,
                    'status': 'completed'
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert pd.api.types.is_datetime64_dtype(df['server_timestamp'])
        assert pd.api.types.is_datetime64_dtype(df['created_at'])
        assert pd.api.types.is_datetime64_dtype(df['planning_at'])
        assert pd.api.types.is_datetime64_dtype(df['executed_at'])
        assert pd.api.types.is_datetime64_dtype(df['ready_at'])
        assert pd.api.types.is_datetime64_dtype(df['expired_at'])
        assert pd.api.types.is_datetime64_dtype(df['fetched_at'])
        assert pd.notna(df['created_at'].iloc[0])
        assert pd.isna(df['planning_at'].iloc[0])
        assert pd.isna(df['executed_at'].iloc[0])
        assert pd.notna(df['ready_at'].iloc[0])
        assert pd.notna(df['expired_at'].iloc[0])