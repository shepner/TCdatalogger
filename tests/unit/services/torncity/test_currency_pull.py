"""Unit tests for currency endpoint processor."""

# Standard library imports
import json
import os
from typing import List, Dict
import time

# Third-party imports
import pytest
from unittest.mock import Mock, mock_open, patch
from google.oauth2 import service_account
from google.cloud import bigquery, monitoring_v3
import pandas as pd

# Application imports
from app.services.torncity.endpoints.currency import CurrencyEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.client import TornClient, TornAPIError

class TestCurrencyEndpointProcessor(CurrencyEndpointProcessor):
    """Test implementation of CurrencyEndpointProcessor."""
    
    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the schema for test data."""
        return [
            bigquery.SchemaField("server_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("faction_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("points", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("money", "INTEGER", mode="REQUIRED")
        ]

    def convert_timestamps(self, df: pd.DataFrame, exclude_cols: list[str] = None) -> pd.DataFrame:
        """Convert timestamp columns to datetime.
        
        Args:
            df: DataFrame to process
            exclude_cols: Columns to exclude from conversion
            
        Returns:
            pd.DataFrame: DataFrame with converted timestamps
        """
        if exclude_cols is None:
            exclude_cols = []
            
        timestamp_cols = [
            col for col in df.columns 
            if "timestamp" in col.lower() and col not in exclude_cols
        ]
        
        for col in timestamp_cols:
            df[col] = pd.to_datetime(df[col], unit='s')
            
        if "fetched_at" in df.columns and "fetched_at" not in exclude_cols:
            df["fetched_at"] = pd.to_datetime(df["fetched_at"])
            
        return df

    def convert_numerics(self, df: pd.DataFrame, exclude_cols: list[str] = None) -> pd.DataFrame:
        """Convert numeric columns to appropriate types.
        
        Args:
            df: DataFrame to process
            exclude_cols: Columns to exclude from conversion
            
        Returns:
            pd.DataFrame: DataFrame with converted numeric types
        """
        if exclude_cols is None:
            exclude_cols = []
            
        numeric_cols = [
            col for col in df.columns 
            if any(t in col.lower() for t in ["id", "balance", "accumulated", "total"])
            and col not in exclude_cols
        ]
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df

@pytest.fixture(scope='function')
def mock_monitoring_client():
    """Mock Google Cloud Monitoring client."""
    mock_client = Mock(spec=monitoring_v3.MetricServiceClient)
    mock_client.common_project_path.return_value = "projects/test-project"
    with patch('google.cloud.monitoring_v3.MetricServiceClient', return_value=mock_client):
        yield mock_client

@pytest.fixture(scope='function')
def mock_credentials(monkeypatch):
    """Mock Google Cloud credentials."""
    mock_creds = Mock(spec=service_account.Credentials)
    mock_creds.project_id = 'test-project'
    
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

@pytest.fixture(scope='function')
def sample_config():
    """Sample configuration for testing."""
    return {
        'dataset': 'test_dataset',
        'gcp_project_id': 'test-project',
        'gcp_credentials_file': '/path/to/credentials.json',
        'tc_api_key_file': '/path/to/api_keys.json',
        'storage_mode': 'append',
        'endpoint_config': {
            'endpoint': 'v2/faction/currency',
            'faction_id': 'faction_17991',
            'name': 'v2_faction_17991_currency',
            'table': 'v2_faction_17991_currency'
        }
    }

@pytest.fixture(scope='function')
def mock_api_keys():
    """Mock Torn API keys."""
    keys = {
        "default": "test_key_1",
        "currency": "test_key_2"
    }
    m = mock_open(read_data=json.dumps(keys))
    with patch("builtins.open", m):
        yield keys

@pytest.fixture(scope='function')
def torn_client(mock_api_keys):
    """Create TornClient with mocked API keys."""
    with patch.object(TornClient, "_load_api_keys", return_value=mock_api_keys):
        client = TornClient("dummy_path")
        return client

@pytest.fixture(scope='function')
def bq_client(mock_credentials, sample_config):
    """Create a BigQuery client for testing."""
    return BigQueryClient(sample_config)

@pytest.fixture(scope='function')
def currency_processor(mock_credentials, mock_monitoring_client, sample_config):
    """Create a CurrencyEndpointProcessor for testing."""
    with patch("google.oauth2.service_account.Credentials.from_service_account_file",
              return_value=mock_credentials):
        processor = TestCurrencyEndpointProcessor(sample_config, sample_config['endpoint_config'])
        processor.torn_client = torn_client
        return processor

@pytest.fixture(scope='function')
def mock_currency_response():
    """Mock response from currency endpoint."""
    return {
        "currency": {
            "points": 1000,
            "money": 5000000
        },
        "timestamp": 1647432000
    }

class TestCurrencyPull:
    """Test currency data pull and processing."""
    
    @pytest.fixture(autouse=True)
    def setup_env(self):
        """Set up environment variables for testing."""
        with patch.dict(os.environ, {
            "GCP_PROJECT_ID": "test-project",
            "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/credentials.json"
        }):
            yield

    def test_currency_data_pull(self, currency_processor, mock_currency_response, torn_client):
        """Test pulling and transforming currency data."""
        # Mock the API call
        with patch.object(torn_client, "fetch_data", return_value=mock_currency_response):
            # Process the data
            result = currency_processor.transform_data(mock_currency_response)
            
            # Verify results
            assert not result.empty
            assert "faction_id" in result.columns
            assert "points" in result.columns
            assert "money" in result.columns
            assert result.iloc[0]["points"] == 1000
            assert result.iloc[0]["money"] == 5000000
    
    def test_currency_data_validation(self, currency_processor, mock_currency_response):
        """Test data validation."""
        df = currency_processor.transform_data(mock_currency_response)
        schema = currency_processor.get_schema()
        
        # Verify required fields are present
        required_fields = [field.name for field in schema if field.mode == "REQUIRED"]
        for field in required_fields:
            assert field in df.columns
            assert not df[field].isnull().any()
    
    def test_currency_error_handling(self, currency_processor, torn_client):
        """Test error handling with invalid data."""
        invalid_data = {"currency": "not_a_dict"}
        mock_fetch = Mock(return_value=invalid_data)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            # Should return empty DataFrame for invalid data
            result = currency_processor.transform_data(invalid_data)
            assert result.empty
    
    def test_currency_data_update(self, currency_processor, mock_currency_response, torn_client):
        """Test data update to BigQuery."""
        mock_fetch = Mock(return_value=mock_currency_response)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            df = currency_processor.transform_data(mock_currency_response)
            
            # Mock BigQuery upload
            with patch.object(currency_processor.bq_client, "upload_dataframe") as mock_upload:
                currency_processor._upload_data(df, currency_processor.get_schema())
                mock_upload.assert_called_once()

    def test_empty_currency_response(self, currency_processor):
        """Test handling of response with no currency data."""
        empty_response = {
            "currency": {},
            "timestamp": 1647432000,
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = currency_processor.transform_data(empty_response)
        assert result.empty

    def test_invalid_currency_data(self, currency_processor):
        """Test handling of invalid currency data that results in no records."""
        invalid_response = {
            "currency": {
                # All fields are invalid or null
                "points": None,
                "money": None
            },
            "timestamp": 1647432000,
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = currency_processor.transform_data(invalid_response)
        assert result.empty

    def test_empty_dataframe_after_processing(self, currency_processor):
        """Test handling of valid data that results in empty DataFrame after processing."""
        response = {
            "currency": {
                # All fields that would be dropped during processing
                "points": "invalid",
                "money": "invalid"
            },
            "timestamp": "invalid",
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = currency_processor.transform_data(response)
        assert result.empty

    def test_empty_valid_currency_data(self, currency_processor, torn_client):
        """Test handling of valid but empty currency data that results in an empty DataFrame."""
        # Mock response with valid structure but empty currency data
        mock_currency_response = {
            "currency": None,
            "timestamp": 1647432000,
            "fetched_at": "2024-03-16T00:00:00"
        }
        
        # Mock the fetch_data method to return our empty response
        mock_fetch = Mock(return_value=mock_currency_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(currency_processor, "_log_error", mock_log_error):
            # Process the data
            df = currency_processor.transform_data(mock_currency_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No currency data found in API response")

    def test_empty_records_list(self, currency_processor, torn_client):
        """Test handling of valid currency data that results in no records."""
        # Mock response with valid structure but currency data that will result in no records
        mock_currency_response = {
            "currency": {
                "points": 1000,  # Valid field
                "money": None,  # Required field is None
            },
            "timestamp": 1647432000,
            "fetched_at": "2024-03-16T00:00:00"
        }
        
        # Mock the fetch_data method to return our response
        mock_fetch = Mock(return_value=mock_currency_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(currency_processor, "_log_error", mock_log_error):
            # Process the data
            df = currency_processor.transform_data(mock_currency_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No records created from currency data")

    def test_currency_bigquery_integration(self, mock_api_keys, mock_credentials, mock_monitoring_client):
        """Test BigQuery integration."""
        # Set up configs
        base_config = {
            'dataset': 'test_dataset',
            'gcp_project_id': 'test-project',
            'gcp_credentials_file': '/path/to/credentials.json',
            'tc_api_key_file': '/path/to/api_keys.json',
            'storage_mode': 'append'
        }
        
        endpoint_config = {
            'endpoint': 'v2/faction/currency',
            'faction_id': 'faction_17991',
            'name': 'v2_faction_17991_currency',
            'table': 'v2_faction_17991_currency',
            'url': 'https://api.torn.com/faction/17991',
            'api_key': 'default'  # Specify which API key to use
        }

        # Create mock response
        mock_response = {
            "currency": {
                "points": 1000,
                "money": 5000000
            },
            "timestamp": 1647432000,
            "fetched_at": "2024-03-16T09:32:31.281852"
        }

        # Create processor with mocked clients
        processor = TestCurrencyEndpointProcessor(base_config, endpoint_config)
        
        # Mock TornClient
        mock_torn_client = Mock()
        mock_torn_client.fetch_data.return_value = mock_response
        mock_torn_client._load_api_keys.return_value = mock_api_keys
        processor.torn_client = mock_torn_client
        
        # Mock BigQueryClient
        mock_bq_client = Mock()
        mock_bq_client.project_id = 'test-project'
        mock_bq_client.upload_dataframe = Mock()
        processor.bq_client = mock_bq_client
        
        # Process data
        result = processor.process()
        
        # Verify processing succeeded
        assert result is True
        
        # Verify BigQuery upload was called
        mock_bq_client.upload_dataframe.assert_called_once()

    def test_transform_data_exception(self, currency_processor):
        """Test general exception handling in transform_data."""
        # Mock a response that will cause an exception during processing
        mock_response = None  # This will cause an attribute error
        
        # Mock the error logging
        mock_log_error = Mock()
        
        with patch.object(currency_processor, "_log_error", mock_log_error):
            # Process the data
            df = currency_processor.transform_data(mock_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            
            # Verify error was logged
            mock_log_error.assert_called_once()
            assert "Error transforming currency data" in mock_log_error.call_args[0][0]

    def test_numeric_conversion_error(self, mock_api_keys, mock_credentials, mock_monitoring_client):
        """Test handling of invalid numeric data."""
        # Set up configs
        base_config = {
            'dataset': 'test_dataset',
            'gcp_project_id': 'test-project',
            'gcp_credentials_file': '/path/to/credentials.json',
            'tc_api_key_file': '/path/to/api_keys.json',
            'storage_mode': 'append'
        }
        
        endpoint_config = {
            'endpoint': 'v2/faction/currency',
            'faction_id': 'faction_17991',
            'name': 'v2_faction_17991_currency',
            'table': 'v2_faction_17991_currency',
            'url': 'https://api.torn.com/faction/17991'
        }

        # Create mock response with invalid numeric data
        mock_response = {
            "currency": {
                "points": "invalid",
                "money": "not_a_number"
            },
            "timestamp": 1647432000,
            "fetched_at": "2024-03-16T09:32:31.281852"
        }

        # Create processor
        processor = TestCurrencyEndpointProcessor(base_config, endpoint_config)

        # Process the invalid data
        result = processor.transform_data(mock_response)

        # Verify DataFrame contains NaN values for numeric columns
        assert not result.empty
        assert pd.isna(result['points'].iloc[0])
        assert pd.isna(result['money'].iloc[0])

        # Verify non-numeric columns are still present and valid
        assert result['faction_id'].iloc[0] == 17991
        assert not pd.isna(result['server_timestamp'].iloc[0])
        assert not pd.isna(result['fetched_at'].iloc[0])

    def test_timestamp_conversion_error(self, currency_processor):
        """Test error handling during timestamp conversion."""
        # Create a response with invalid timestamp data
        mock_response = {
            "currency": {
                "points": 1000,
                "money": 5000000
            },
            "timestamp": "invalid_timestamp",  # Invalid timestamp
            "fetched_at": "invalid_datetime"  # Invalid datetime
        }
        
        # Mock the error logging
        mock_log_error = Mock()
        
        with patch.object(currency_processor, "_log_error", mock_log_error):
            # Process the data
            df = currency_processor.transform_data(mock_response)
            
            # Verify error was logged
            mock_log_error.assert_called_once()
            assert "Error transforming currency data" in mock_log_error.call_args[0][0]
            
            # Verify we got an empty DataFrame
            assert df.empty 