"""Unit tests for crimes endpoint processor."""

import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, mock_open, Mock

import pandas as pd
import pytest
from google.cloud import bigquery, monitoring_v3
from google.oauth2 import service_account

from app.services.torncity.client import TornClient
from app.services.torncity.endpoints.crimes import CrimesEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.client import TornAPIError


class TestCrimesEndpointProcessor(CrimesEndpointProcessor):
    """Test implementation of CrimesEndpointProcessor."""

    def get_schema(self) -> list:
        """Get test schema for crimes data."""
        return [
            bigquery.SchemaField("crime_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("crime_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("participants", "INTEGER"),
            bigquery.SchemaField("time_started", "INTEGER"),
            bigquery.SchemaField("time_completed", "INTEGER"),
            bigquery.SchemaField("initiated_by", "INTEGER"),
            bigquery.SchemaField("planned_by", "INTEGER"),
            bigquery.SchemaField("success", "BOOLEAN"),
            bigquery.SchemaField("money_gain", "INTEGER"),
            bigquery.SchemaField("respect_gain", "INTEGER"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP")
        ]

    def pull_data(self) -> pd.DataFrame:
        """Pull and transform crimes data."""
        try:
            data = self.torn_client.fetch_data(self.endpoint_config['endpoint'])
            return self.transform_data(data)
        except Exception as e:
            self._log_error(f"Error pulling crimes data: {str(e)}")
            return pd.DataFrame()

    def convert_timestamps(self, df: pd.DataFrame, exclude_cols: list = None) -> pd.DataFrame:
        """Convert timestamp columns in test data."""
        if exclude_cols is None:
            exclude_cols = []
        timestamp_cols = [col for col in df.columns if col.endswith('_at') or col.endswith('_timestamp')]
        timestamp_cols = [col for col in timestamp_cols if col not in exclude_cols]
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        return df

    def convert_numerics(self, df: pd.DataFrame, exclude_cols: list = None) -> pd.DataFrame:
        """Convert numeric columns in test data."""
        if exclude_cols is None:
            exclude_cols = []
        numeric_cols = df.select_dtypes(include=['object', 'int64', 'float64']).columns
        numeric_cols = [col for col in numeric_cols if col not in exclude_cols]
        for col in numeric_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except (ValueError, TypeError):
                    continue
        return df

    def transform_data(self, data: dict) -> pd.DataFrame:
        """Transform crimes data into a normalized DataFrame."""
        try:
            # Extract crimes data
            crimes_data = data.get("crimes", {})
            if not crimes_data:
                self._log_error("No crimes data found in API response")
                return pd.DataFrame()
            
            # Convert crimes dict to list of records
            records = []
            for crime_id, crime_info in crimes_data.items():
                record = {
                    "crime_id": crime_id,  # Will be converted to numeric later
                    "crime_name": crime_info.get("crime_name"),
                    "participants": crime_info.get("participants"),
                    "time_started": crime_info.get("time_started"),
                    "time_completed": crime_info.get("time_completed"),
                    "initiated_by": crime_info.get("initiated_by"),
                    "planned_by": crime_info.get("planned_by"),
                    "success": crime_info.get("success"),
                    "money_gain": crime_info.get("money_gain"),
                    "respect_gain": crime_info.get("respect_gain"),
                    "fetched_at": datetime.now()
                }
                records.append(record)
            
            # Create DataFrame
            df = pd.DataFrame(records)
            if df.empty:
                self._log_error("No records created from crimes data")
                return df
            
            # Convert timestamps
            df = self.convert_timestamps(df, exclude_cols=[
                "crime_id", "crime_name", "participants", "initiated_by", "planned_by",
                "success", "money_gain", "respect_gain"
            ])
            
            # Convert numeric columns
            df = self.convert_numerics(df, exclude_cols=[
                "crime_name", "success", "fetched_at"
            ])
            
            return df
            
        except Exception as e:
            self._log_error(f"Error transforming crimes data: {str(e)}")
            return pd.DataFrame()


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
def sample_config():
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
        "gcp_credentials_file": "test_creds.json",
        "tc_api_key_file": "test_api_keys.txt"
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


@pytest.fixture(autouse=True)
def setup_env():
    """Set up environment variables for testing."""
    with patch.dict(os.environ, {
        "GCP_PROJECT_ID": "test-project",
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/credentials.json"
    }):
        yield


@pytest.fixture
def crimes_processor(mock_credentials, mock_monitoring_client, sample_config, mock_api_keys):
    """Create CrimesEndpointProcessor for testing."""
    with patch('google.oauth2.service_account.Credentials.from_service_account_file') as mock_creds:
        mock_creds.return_value = mock_credentials
        processor = TestCrimesEndpointProcessor(sample_config, sample_config['endpoint_config']['crimes'])
        with patch.object(TornClient, "_load_api_keys", return_value=mock_api_keys):
            processor.torn_client = TornClient("dummy_path")
            return processor


@pytest.fixture
def mock_crimes_response():
    """Mock response from crimes endpoint."""
    return {
        "crimes": {
            "1": {
                "crime_id": 1,
                "crime_name": "Test Crime",
                "participants": 5,
                "time_started": 1646956800,
                "time_completed": 1646960400,
                "initiated_by": 12345,
                "planned_by": 67890,
                "success": True,
                "money_gain": 1000000,
                "respect_gain": 100
            }
        }
    }


class TestCrimesPull:
    """Test cases for crimes endpoint processor."""

    def test_crimes_data_pull(self, crimes_processor, mock_crimes_response, torn_client):
        """Test pulling and transforming crimes data."""
        # Mock the API call
        with patch.object(torn_client, "fetch_data", return_value=mock_crimes_response):
            # Process the data
            result = crimes_processor.transform_data(mock_crimes_response)
            
            # Verify results
            assert len(result) > 0
            assert "crime_id" in result.columns
            assert "crime_name" in result.columns
            assert result.iloc[0]["crime_id"] == 1
            assert result.iloc[0]["crime_name"] == "Test Crime"
            assert result.iloc[0]["participants"] == 5
            assert result.iloc[0]["money_gain"] == 1000000
            assert result.iloc[0]["respect_gain"] == 100

    def test_crimes_data_validation(self, crimes_processor, mock_crimes_response):
        """Test data validation against schema."""
        df = crimes_processor.transform_data(mock_crimes_response)
        schema = crimes_processor.get_schema()
        
        # Should not raise any exceptions
        crimes_processor._validate_schema(df, schema)

    def test_crimes_error_handling(self, crimes_processor, torn_client):
        """Test error handling with invalid data."""
        invalid_data = {"crimes": "not_a_dict"}
        mock_fetch = Mock(return_value=invalid_data)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            # Should return empty DataFrame for invalid data
            result = crimes_processor.transform_data(invalid_data)
            assert result.empty

    def test_crimes_data_update(self, crimes_processor, mock_crimes_response, torn_client):
        """Test data update to BigQuery."""
        mock_fetch = Mock(return_value=mock_crimes_response)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            df = crimes_processor.transform_data(mock_crimes_response)
            
            # Mock BigQuery upload
            with patch.object(crimes_processor.bq_client, "upload_dataframe") as mock_upload:
                crimes_processor._upload_data(df, crimes_processor.get_schema())
                
                # Verify upload was called with correct parameters
                mock_upload.assert_called_once()
                _, kwargs = mock_upload.call_args
                
                # Verify DataFrame was passed
                assert isinstance(kwargs['df'], pd.DataFrame)
                # Verify table_id format
                assert kwargs['table_id'] == "test-project.test_dataset.test_crimes"
                # Verify write disposition
                assert kwargs['write_disposition'] == "append"

    def test_empty_crimes_response(self, crimes_processor):
        """Test handling of response with no crimes data."""
        empty_response = {
            "crimes": {},
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = crimes_processor.transform_data(empty_response)
        assert result.empty

    def test_invalid_crimes_data(self, crimes_processor):
        """Test handling of invalid crimes data that results in no records."""
        invalid_response = {
            "crimes": {
                "invalid": {},  # Crime with no valid fields
                "also_invalid": None,  # Crime with null data
                "123": {  # Crime with all null/invalid fields
                    "crime_name": None,
                    "participants": None,
                    "time_started": None,
                    "time_completed": None,
                    "initiated_by": None,
                    "planned_by": None,
                    "success": None,
                    "money_gain": None,
                    "respect_gain": None
                }
            },
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = crimes_processor.transform_data(invalid_response)
        assert result.empty

    def test_empty_dataframe_after_processing(self, crimes_processor):
        """Test handling of valid data that results in empty DataFrame after processing."""
        response = {
            "crimes": {
                "123": {
                    # All fields that would be dropped during processing
                    "crime_name": "",
                    "participants": "invalid",
                    "time_started": "invalid",
                    "time_completed": "invalid",
                    "initiated_by": "invalid",
                    "planned_by": "invalid",
                    "success": "invalid",
                    "money_gain": "invalid",
                    "respect_gain": "invalid"
                }
            },
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = crimes_processor.transform_data(response)
        assert result.empty

    def test_empty_valid_crimes_data(self, crimes_processor, torn_client):
        """Test handling of valid but empty crimes data that results in an empty DataFrame."""
        # Mock response with valid structure but empty crimes
        mock_crimes_response = {
            "crimes": {
                # Empty crimes dictionary
            }
        }
        
        # Mock the fetch_data method to return our empty response
        mock_fetch = Mock(return_value=mock_crimes_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(crimes_processor, "_log_error", mock_log_error):
            # Process the data
            df = crimes_processor.transform_data(mock_crimes_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            assert len(df.columns) == 0
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No crimes data found in API response")

    def test_empty_records_list(self, crimes_processor, torn_client):
        """Test handling of valid crimes data that results in no records."""
        # Mock response with valid structure but crime data that will result in no records
        mock_crimes_response = {
            "crimes": {
                "123": {
                    "crime_name": "Test Crime",
                    "participants": 5,
                    "time_started": None,  # Required field is None
                    "time_completed": 1646960400,
                    "initiated_by": 12345,
                    "planned_by": 67890,
                    "success": True,
                    "money_gain": 1000000,
                    "respect_gain": 100
                }
            }
        }
        
        # Mock the fetch_data method to return our response
        mock_fetch = Mock(return_value=mock_crimes_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(crimes_processor, "_log_error", mock_log_error), \
             patch.object(pd, "DataFrame", return_value=pd.DataFrame()):
            # Process the data
            df = crimes_processor.transform_data(mock_crimes_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No records created from crimes data") 