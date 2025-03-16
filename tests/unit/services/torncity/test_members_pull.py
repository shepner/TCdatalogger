"""Unit tests for members endpoint processor."""

# Standard library imports
import json
import os
from typing import List, Dict

# Third-party imports
import pytest
from unittest.mock import Mock, mock_open, patch
from google.oauth2 import service_account
from google.cloud import bigquery, monitoring_v3
import pandas as pd

# Application imports
from app.services.torncity.endpoints.members import MembersEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.client import TornClient, TornAPIError

class TestMembersEndpointProcessor(MembersEndpointProcessor):
    """Test implementation of MembersEndpointProcessor."""
    
    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the schema for test data."""
        return [
            bigquery.SchemaField("member_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("faction_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("level", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("status_state", "STRING"),
            bigquery.SchemaField("status_until", "INTEGER"),
            bigquery.SchemaField("last_action", "STRING"),
            bigquery.SchemaField("last_action_timestamp", "INTEGER"),
            bigquery.SchemaField("last_login", "INTEGER"),
            bigquery.SchemaField("position", "STRING"),
            bigquery.SchemaField("days_in_faction", "INTEGER"),
            bigquery.SchemaField("forum_posts", "INTEGER"),
            bigquery.SchemaField("karma", "INTEGER"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP")
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
            if any(t in col.lower() for t in ["id", "level", "count", "until", "timestamp", "days", "posts", "karma"])
            and col not in exclude_cols
        ]
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df

    def process_data(self) -> pd.DataFrame:
        """Process members data."""
        try:
            # Fetch data from the API
            data = self.torn_client.fetch_data(
                f"v2/faction/members?faction_id={self.endpoint_config['faction_id']}",
                self.torn_client._load_api_keys()['members']
            )
            
            # Transform the data
            df = self.transform_data(data)
            
            # Convert timestamps
            df = self.convert_timestamps(df)
            
            # Convert numerics
            df = self.convert_numerics(df)
            
            return df
        except Exception as e:
            self._log_error(f"Error processing members data: {str(e)}")
            return pd.DataFrame()

    def update_data(self, df: pd.DataFrame) -> None:
        """Update data in BigQuery."""
        try:
            self.bq_client.upload_dataframe(
                df=df,
                table_id=self.table,
                schema=self.get_schema(),
                write_mode=self.storage_mode
            )
        except Exception as e:
            self._log_error(f"Upload failed: {str(e)}")
            raise

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
            'endpoint': 'v2/faction/members',
            'faction_id': 'faction_17991',
            'name': 'members',
            'table': 'v2_faction_17991_members'
        }
    }

@pytest.fixture(scope='function')
def mock_api_keys():
    """Mock Torn API keys."""
    keys = {
        "default": "test_key_1",
        "members": "test_key_2"
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
def members_processor(mock_credentials, mock_monitoring_client, sample_config):
    """Create a MembersEndpointProcessor for testing."""
    with patch("google.oauth2.service_account.Credentials.from_service_account_file",
              return_value=mock_credentials):
        processor = TestMembersEndpointProcessor(sample_config, sample_config['endpoint_config'])
        processor.torn_client = torn_client
        return processor

@pytest.fixture(scope='function')
def mock_members_response():
    """Mock response from members endpoint."""
    return {
        "members": {
            "12345": {
                "name": "Test User",
                "level": 50,
                "faction": {
                    "faction_id": "17991"
                },
                "status": {
                    "description": "Online",
                    "state": "online",
                    "until": 0
                },
                "last_action": {
                    "relative": "1 hour ago",
                    "timestamp": 1647432000
                }
            }
        },
        "fetched_at": "2024-03-16T09:32:31.281852"
    }

class TestMembersPull:
    """Test members data pull and processing."""
    
    @pytest.fixture(autouse=True)
    def setup_env(self):
        """Set up environment variables for testing."""
        with patch.dict(os.environ, {
            "GCP_PROJECT_ID": "test-project",
            "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/credentials.json"
        }):
            yield

    def test_members_data_pull(self, members_processor, mock_members_response, torn_client):
        """Test pulling and transforming members data."""
        # Mock the API call
        with patch.object(torn_client, "fetch_data", return_value=mock_members_response):
            # Process the data
            result = members_processor.transform_data(mock_members_response)
            
            # Verify results
            assert len(result) > 0
            assert "member_id" in result.columns
            assert "faction_id" in result.columns
            assert result.iloc[0]["name"] == "Test User"
            assert result.iloc[0]["level"] == 50
            assert result.iloc[0]["status"] == "Online"
    
    def test_members_data_validation(self, members_processor, mock_members_response):
        """Test data validation."""
        df = members_processor.transform_data(mock_members_response)
        schema = members_processor.get_schema()
        
        # Should not raise any exceptions
        members_processor._validate_schema(df, schema)
    
    def test_members_error_handling(self, members_processor, torn_client):
        """Test error handling with invalid data."""
        invalid_data = {"members": "not_a_dict"}
        mock_fetch = Mock(return_value=invalid_data)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            # Should return empty DataFrame for invalid data
            result = members_processor.transform_data(invalid_data)
            assert result.empty
    
    def test_members_data_update(self, members_processor, mock_members_response, torn_client):
        """Test data update to BigQuery."""
        mock_fetch = Mock(return_value=mock_members_response)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            df = members_processor.transform_data(mock_members_response)
            
            # Mock BigQuery upload
            with patch.object(members_processor.bq_client, "upload_dataframe") as mock_upload:
                members_processor._upload_data(df, members_processor.get_schema())
                mock_upload.assert_called_once()

    def test_empty_members_response(self, members_processor):
        """Test handling of response with no members data."""
        empty_response = {
            "members": {},
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = members_processor.transform_data(empty_response)
        assert result.empty

    def test_invalid_members_data(self, members_processor):
        """Test handling of invalid members data that results in no records."""
        invalid_response = {
            "members": {
                "invalid": {},  # Member with no valid fields
                "also_invalid": None,  # Member with null data
                "123": {  # Member with all null/invalid fields
                    "name": None,
                    "level": None,
                    "faction": None,
                    "status": None,
                    "last_action": None
                }
            },
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = members_processor.transform_data(invalid_response)
        assert result.empty

    def test_empty_dataframe_after_processing(self, members_processor):
        """Test handling of valid data that results in empty DataFrame after processing."""
        response = {
            "members": {
                "123": {
                    # All fields that would be dropped during processing
                    "name": "",
                    "level": "invalid",
                    "faction": {"faction_id": "invalid"},
                    "status": {"description": "", "state": ""},
                    "last_action": {"relative": "", "timestamp": "invalid"}
                }
            },
            "fetched_at": "2024-03-16T00:00:00"
        }
        result = members_processor.transform_data(response)
        assert result.empty

    def test_empty_valid_members_data(self, members_processor, torn_client):
        """Test handling of valid but empty members data that results in an empty DataFrame."""
        # Mock response with valid structure but empty members
        mock_members_response = {
            "members": {
                # Empty members dictionary
            }
        }
        
        # Mock the fetch_data method to return our empty response
        mock_fetch = Mock(return_value=mock_members_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(members_processor, "_log_error", mock_log_error):
            # Process the data
            df = members_processor.transform_data(mock_members_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            assert len(df.columns) == 0
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No members data found in API response")

    def test_empty_records_list(self, members_processor, torn_client):
        """Test handling of valid members data that results in no records."""
        # Mock response with valid structure but member data that will result in no records
        mock_members_response = {
            "members": {
                "123": {
                    "name": "Test User",
                    "level": 50,
                    "faction": {
                        "faction_id": None
                    },
                    "status": {
                        "description": "Online",
                        "state": "online",
                        "until": 0
                    },
                    "last_action": {
                        "relative": "1 hour ago",
                        "timestamp": 1647432000
                    }
                }
            }
        }
        
        # Mock the fetch_data method to return our response
        mock_fetch = Mock(return_value=mock_members_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(members_processor, "_log_error", mock_log_error), \
             patch.object(pd, "DataFrame", return_value=pd.DataFrame()):
            # Process the data
            df = members_processor.transform_data(mock_members_response)
            
            # Assert that we got an empty DataFrame
            assert df.empty
            
            # Verify error was logged
            mock_log_error.assert_called_once_with("No records created from members data") 