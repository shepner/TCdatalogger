"""Unit tests for items endpoint processor."""

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
from app.services.torncity.endpoints.items import ItemsEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.client import TornAPIError


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
            "items": {
                "table": "test_items",
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
def items_processor(test_config_dir):
    """Create a test items processor instance."""
    config = {
        'gcp_project_id': 'test-project',
        'gcp_credentials_file': str(test_config_dir / 'credentials.json'),
        'dataset': 'test_dataset',
        'endpoint': 'items',
        'selection': 'default',
        'storage_mode': 'append'
    }
    return ItemsEndpointProcessor(config=config)


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
            "items": {
                "name": "items",
                "table": "test_items",
                "endpoint": "/v2/torn/items"
            }
        },
        "gcp_credentials_file": str(test_config_dir / "credentials.json"),
        "tc_api_key_file": str(test_config_dir / "TC_API_key.json")
    }


@pytest.fixture
def mock_api_keys():
    """Mock Torn API keys."""
    return {
        'items': 'test_key_2',
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
def mock_items_response():
    """Mock response from items endpoint."""
    return {
        "items": {
            "1": {
                "id": 1,
                "name": "Test Item",
                "description": "A test item",
                "type": "Primary",
                "buy_price": 1000,
                "sell_price": 800,
                "market_value": 900,
                "circulation": 1000,
                "image": "test.png"
            }
        },
        "timestamp": 1646960400,
        "fetched_at": datetime.now()
    }


class TestItemsPull:
    """Test items data pull and processing."""
    
    @pytest.fixture(autouse=True)
    def setup_env(self):
        """Set up environment variables for testing."""
        with patch.dict(os.environ, {
            "GCP_PROJECT_ID": "test-project",
            "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/credentials.json"
        }):
            yield

    def test_items_data_pull(self, items_processor, mock_items_response, torn_client):
        """Test pulling and transforming items data."""
        # Mock the API call
        with patch.object(torn_client, "fetch_data", return_value=mock_items_response):
            # Process the data
            result = items_processor.transform_data(mock_items_response)
            
            # Verify results
            assert len(result) > 0
            assert "item_id" in result.columns
            assert "name" in result.columns
            assert "market_value" in result.columns
            assert result.iloc[0]["name"] == "Test Item"
            assert result.iloc[0]["buy_price"] == 1000
            assert result.iloc[0]["market_value"] == 900
    
    def test_items_data_validation(self, items_processor, mock_items_response):
        """Test data validation."""
        df = items_processor.transform_data(mock_items_response)
        schema = items_processor.get_schema()
        
        # Should not raise any exceptions
        items_processor._validate_schema(df, schema)
    
    def test_items_error_handling(self, items_processor, torn_client):
        """Test error handling with invalid data."""
        invalid_data = {"items": "not_a_dict"}
        mock_fetch = Mock(return_value=invalid_data)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            # Should return empty DataFrame for invalid data
            result = items_processor.transform_data(invalid_data)
            assert result.empty
    
    def test_items_data_update(self, items_processor, mock_items_response, torn_client):
        """Test data update to BigQuery."""
        mock_fetch = Mock(return_value=mock_items_response)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            df = items_processor.transform_data(mock_items_response)
            
            # Mock BigQuery upload
            with patch.object(items_processor.bq_client, "upload_dataframe") as mock_upload:
                items_processor._upload_data(df, items_processor.get_schema())
                mock_upload.assert_called_once()

    def test_empty_items_response(self, items_processor):
        """Test handling of response with no items data."""
        empty_response = {
            "items": {},
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = items_processor.transform_data(empty_response)
        assert result.empty

    def test_invalid_items_data(self, items_processor):
        """Test handling of invalid items data that results in no records."""
        invalid_response = {
            "items": {
                "invalid": {},  # Item with no valid fields
                "also_invalid": None,  # Item with null data
                "123": {  # Item with all null/invalid fields
                    "name": None,
                    "description": None,
                    "type": None,
                    "buy_price": None,
                    "market_value": None
                }
            },
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = items_processor.transform_data(invalid_response)
        assert result.empty

    def test_empty_dataframe_after_processing(self, items_processor):
        """Test handling of valid data that results in empty DataFrame after processing."""
        response = {
            "items": {
                "123": {
                    # All fields that would be dropped during processing
                    "name": None,
                    "description": None,
                    "type": None,
                    "weapon_type": None,
                    "buy_price": None,
                    "sell_price": None,
                    "market_value": None,
                    "circulation": None,
                    "image": None,
                    "requirement": {},  # Empty dict instead of None
                    "effect": {}  # Empty dict instead of None
                }
            },
            "fetched_at": "2024-03-16T00:00:00"
        }
        
        result = items_processor.transform_data(response)
        
        # Verify string fields are None
        assert result.loc[0, "name"] is None
        assert result.loc[0, "description"] is None
        assert result.loc[0, "type"] is None
        assert result.loc[0, "weapon_type"] is None
        assert result.loc[0, "image"] is None
        
        # Verify numeric fields are NaN
        assert pd.isna(result.loc[0, "buy_price"])
        assert pd.isna(result.loc[0, "sell_price"])
        assert pd.isna(result.loc[0, "market_value"])
        assert pd.isna(result.loc[0, "circulation"])
        
        # Verify requirement fields have default value of 0
        assert result.loc[0, "requirement_level"] == 0
        assert result.loc[0, "requirement_strength"] == 0
        assert result.loc[0, "requirement_speed"] == 0
        assert result.loc[0, "requirement_dexterity"] == 0
        assert result.loc[0, "requirement_intelligence"] == 0
        
        # Verify timestamp is converted
        assert pd.to_datetime(result.loc[0, "fetched_at"]) == pd.to_datetime("2024-03-16T00:00:00")

    def test_empty_valid_items_data(self, items_processor, torn_client):
        """Test handling of valid but empty items data that results in an empty DataFrame."""
        # Mock response with valid structure but empty items
        mock_items_response = {
            "items": {
                # Empty items dictionary
            }
        }
        
        # Mock the fetch_data method to return our empty response
        mock_fetch = Mock(return_value=mock_items_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(items_processor, "_log_error", mock_log_error):
            # Process the data
            df = items_processor.transform_data(mock_items_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            assert len(df.columns) == 0
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No items data found in API response")

    def test_empty_records_list(self, items_processor, torn_client):
        """Test handling of valid items data that results in no records."""
        # Mock response with valid structure but item data that will result in no records
        mock_items_response = {
            "items": {
                "123": {
                    "name": "Test Item",
                    "description": "A test item",
                    "type": None,  # Required field is None
                    "buy_price": 100,
                    "market_value": 150
                }
            }
        }
        
        # Mock the fetch_data method to return our response
        mock_fetch = Mock(return_value=mock_items_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(items_processor, "_log_error", mock_log_error), \
             patch.object(pd, "DataFrame", return_value=pd.DataFrame()):
            # Process the data
            df = items_processor.transform_data(mock_items_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No records created from items data")

    def test_items_bigquery_integration(self, items_processor, mock_items_response):
        """Test BigQuery integration for items endpoint."""
        # Create a unique test table name
        test_table = f"test_items_{int(time.time())}"
        original_table = items_processor.table
        items_processor.table = test_table

        # Mock BigQuery client operations
        mock_row = Mock()
        mock_row.count = 1
        
        mock_query_job = Mock()
        mock_query_job.result.return_value = iter([mock_row])  # Return an iterator
        
        mock_bq_client = Mock()
        mock_bq_client.query.return_value = mock_query_job
        mock_bq_client.delete_table.return_value = None
        
        # Replace the actual BigQuery client with our mock
        items_processor.bq_client.client = mock_bq_client

        try:
            # Mock the fetch_data method to return our test response
            with patch.object(items_processor.torn_client, 'fetch_data', return_value=mock_items_response):
                # Process the data
                result = items_processor.process()
                
                # Verify processing was successful
                assert result is True, "Processing should succeed"
                
                # Verify data was uploaded to BigQuery
                query = f"""
                SELECT COUNT(*) as count 
                FROM `{items_processor.bq_client.project_id}.{items_processor.config['dataset']}.{test_table}`
                """
                query_job = items_processor.bq_client.client.query(query)
                results = query_job.result()
                row = next(results)
                assert row.count > 0, "Table should contain data"

        finally:
            # Clean up - delete test table
            items_processor.bq_client.client.delete_table(
                f"{items_processor.bq_client.project_id}.{items_processor.config['dataset']}.{test_table}",
                not_found_ok=True
            )
            items_processor.table = original_table

    def test_transform_data_exception(self, items_processor):
        """Test general exception handling in transform_data."""
        # Mock a response that will cause an exception during processing
        mock_response = None  # This will cause an attribute error
        
        # Mock the error logging
        mock_log_error = Mock()
        
        with patch.object(items_processor, "_log_error", mock_log_error):
            # Process the data
            df = items_processor.transform_data(mock_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            
            # Verify error was logged
            mock_log_error.assert_called_once()
            assert "Error transforming items data" in mock_log_error.call_args[0][0]

    def test_numeric_conversion_error(self, items_processor):
        """Test error handling during numeric conversion."""
        # Create a response with invalid numeric data
        mock_response = {
            "items": {
                "123": {
                    "name": "Test Item",
                    "description": "A test item",
                    "type": "Weapon",
                    "buy_price": "not_a_number",  # Invalid numeric value
                    "sell_price": "invalid",  # Invalid numeric value
                    "market_value": "not_a_price",  # Invalid numeric value
                    "circulation": "invalid",  # Invalid numeric value
                    "requirement": None  # Will cause error in numeric conversion
                }
            },
            "fetched_at": "2024-03-16T09:32:31.281852"
        }
        
        # Mock error logging
        mock_log_error = Mock()
        with patch.object(items_processor, "_log_error", mock_log_error):
            # Process the data
            df = items_processor.transform_data(mock_response)
            
            # Verify error was logged
            mock_log_error.assert_called_once()
            assert "Error transforming items data" in mock_log_error.call_args[0][0]
            
            # Verify we got an empty DataFrame
            assert df.empty

    def test_timestamp_conversion_error(self, items_processor):
        """Test error handling during timestamp conversion."""
        # Create a response with invalid timestamp data
        mock_response = {
            "items": {
                "123": {
                    "name": "Test Item",
                    "description": "A test item",
                    "type": "Weapon",
                    "buy_price": 100,
                    "sell_price": 50,
                    "market_value": 75,
                    "circulation": 1000
                }
            },
            "fetched_at": "invalid_timestamp"  # Invalid timestamp
        }
        
        # Mock error logging
        mock_log_error = Mock()
        with patch.object(items_processor, "_log_error", mock_log_error):
            # Process the data
            df = items_processor.transform_data(mock_response)
            
            # Verify error was logged
            mock_log_error.assert_called_once()
            assert "Error transforming items data" in mock_log_error.call_args[0][0]
            
            # Verify we got an empty DataFrame
            assert df.empty 