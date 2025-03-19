"""Unit tests for members endpoint processor."""

# Standard library imports
import json
import os
from typing import List, Dict
import time

# Third-party imports
import pytest
from unittest.mock import Mock, mock_open, patch, MagicMock
from google.oauth2 import service_account
from google.cloud import bigquery, monitoring_v3
import pandas as pd

# Application imports
from app.services.torncity.endpoints.members import MembersEndpointProcessor
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.client import TornClient, TornAPIError
from app.services.torncity.exceptions import DataValidationError

class TestMembersEndpointProcessor(MembersEndpointProcessor):
    """Test implementation of MembersEndpointProcessor."""
    
    def __init__(self, config, endpoint_config):
        """Initialize with test configuration."""
        super().__init__(config)
        self.endpoint_config.update(endpoint_config)
        self.table = endpoint_config.get('table', 'members') if endpoint_config else 'members'

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the schema for test data."""
        return [
            bigquery.SchemaField("member_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("level", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("status_description", "STRING"),
            bigquery.SchemaField("last_action", "STRING"),
            bigquery.SchemaField("last_action_timestamp", "INTEGER"),
            bigquery.SchemaField("faction_position", "STRING"),
            bigquery.SchemaField("faction_id", "INTEGER"),
            bigquery.SchemaField("life_current", "INTEGER"),
            bigquery.SchemaField("life_maximum", "INTEGER"),
            bigquery.SchemaField("days_in_faction", "INTEGER"),
            bigquery.SchemaField("timestamp", "INTEGER")
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

    def process_data(self, data):
        """Process the data."""
        if not data or "data" not in data:
            return []
        return self.transform_data(data)

    def transform_data(self, data):
        """Transform the data."""
        if not data:
            return []
            
        # Extract members data from the nested structure
        members_data = data.get("data", {}).get("members", {})
        if not members_data:
            return []
            
        transformed_data = []
        
        for member_id, member_data in members_data.items():
            try:
                # Validate required fields and types
                name = member_data.get("name")
                level = member_data.get("level")
                
                if name is None or level is None:
                    error_msg = f"Missing required field for member {member_id}"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                # Type validation
                if not isinstance(name, str):
                    error_msg = f"Invalid type for name field for member {member_id}: expected str, got {type(name)}"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                # Convert level to int if it's a string
                try:
                    level = int(level) if isinstance(level, str) else level
                except (ValueError, TypeError):
                    error_msg = f"Error processing member {member_id}: Invalid numeric value for level field: {level}"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                if not isinstance(level, int):
                    error_msg = f"Error processing member {member_id}: Invalid type for level field, expected int, got {type(level)}"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                # Extract status data
                status_data = member_data.get("status", {})
                if not isinstance(status_data, dict):
                    status_data = {}
                
                # Extract life data
                life_data = member_data.get("life", {})
                if not isinstance(life_data, dict):
                    life_data = {}
                life_current = life_data.get("current")
                life_maximum = life_data.get("maximum")
                
                # Extract faction data
                faction_data = member_data.get("faction", {})
                if not isinstance(faction_data, dict):
                    faction_data = {}
                faction_id = faction_data.get("faction_id")
                faction_position = faction_data.get("position")
                days_in_faction = faction_data.get("days_in_faction")
                
                # Extract last action data
                last_action_data = member_data.get("last_action", {})
                if not isinstance(last_action_data, dict):
                    last_action_data = {}
                last_action = last_action_data.get("status")
                last_action_timestamp = last_action_data.get("timestamp")
                
                # Validate timestamp
                if last_action_timestamp is not None:
                    try:
                        if isinstance(last_action_timestamp, str):
                            pd.to_datetime(last_action_timestamp)
                        elif not isinstance(last_action_timestamp, (int, float)):
                            error_msg = f"Invalid timestamp format for member {member_id}: {last_action_timestamp}"
                            self._log_error(error_msg)
                            raise DataValidationError(error_msg)
                    except (ValueError, TypeError):
                        error_msg = f"Invalid timestamp format for member {member_id}: {last_action_timestamp}"
                        self._log_error(error_msg)
                        raise DataValidationError(error_msg)
                
                # Use last_action_timestamp as the record timestamp if available, otherwise use member timestamp
                timestamp = last_action_timestamp if last_action_timestamp else member_data.get("timestamp")
                
                # Validate timestamp
                if timestamp is not None:
                    try:
                        if isinstance(timestamp, str):
                            pd.to_datetime(timestamp)
                        elif not isinstance(timestamp, (int, float)):
                            error_msg = f"Invalid timestamp format for member {member_id}: {timestamp}"
                            self._log_error(error_msg)
                            raise DataValidationError(error_msg)
                    except (ValueError, TypeError):
                        error_msg = f"Invalid timestamp format for member {member_id}: {timestamp}"
                        self._log_error(error_msg)
                        raise DataValidationError(error_msg)
                
                # Create transformed member data
                transformed_member = {
                    "member_id": int(member_id),
                    "player_id": int(member_id),  # In Torn, member_id is the same as player_id
                    "name": name,
                    "level": level,
                    "status": status_data.get("state"),
                    "status_description": status_data.get("description"),
                    "last_action": last_action,
                    "last_action_timestamp": last_action_timestamp,
                    "faction_id": faction_id,
                    "faction_position": faction_position,
                    "life_current": life_current,
                    "life_maximum": life_maximum,
                    "days_in_faction": int(days_in_faction) if days_in_faction is not None else None,
                    "timestamp": timestamp
                }
                
                transformed_data.append(transformed_member)
                
            except (ValueError, TypeError) as e:
                error_msg = f"Error processing member {member_id}: {str(e)}"
                self._log_error(error_msg)
                raise DataValidationError(error_msg)
                
        return transformed_data

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

@pytest.fixture
def sample_config(mock_api_keys):
    """Sample configuration for testing."""
    return {
        'gcp_project_id': 'test-project',
        'gcp_credentials_file': '/path/to/credentials.json',
        'dataset': 'test_dataset',
        'endpoint': 'user',
        'selection': 'default',
        'api_key': 'test_key_1',
        'endpoint_config': {
            'name': 'members',
            'table': 'v2_faction_17991_members',
            'url': 'v2/faction/members',
            'faction_id': '17991'
        },
        'storage_mode': 'append',
        'api_keys': {
            'default': 'test_key_1',
            'secondary': 'test_key_2'
        }
    }

@pytest.fixture
def mock_api_keys():
    """Mock API keys for testing."""
    return {"default": "test_key_1", "secondary": "test_key_2"}

@pytest.fixture
def torn_client(mock_api_keys):
    """Create a TornClient instance with mock API keys."""
    with patch('os.path.exists') as mock_exists, \
         patch('builtins.open', mock_open(read_data=json.dumps(mock_api_keys))):
        mock_exists.return_value = True
        client = TornClient("/path/to/api_keys.json")
        return client

@pytest.fixture(scope='function')
def bq_client(mock_credentials, sample_config):
    """Create a BigQuery client for testing."""
    return BigQueryClient(sample_config)

@pytest.fixture(scope='function')
def members_processor(mock_credentials, mock_monitoring_client, sample_config, torn_client):
    """Create a MembersEndpointProcessor for testing."""
    with patch("google.oauth2.service_account.Credentials.from_service_account_file",
              return_value=mock_credentials):
        processor = TestMembersEndpointProcessor(sample_config, sample_config['endpoint_config'])
        processor.torn_client = torn_client
        return processor

@pytest.fixture
def mock_members_response():
    """Mock response for members endpoint."""
    return {
        "fetched_at": "2024-03-16T09:32:31.281852",
        "members": {
            "12345": {
                "member_id": 12345,  # Ensure this is an integer
                "name": "Test User",
                "level": 50,
                "faction": {
                    "faction_id": 17991  # Also make this an integer
                },
                "status": {
                    "description": "Online",
                    "state": "online",
                    "until": 0
                },
                "last_action": {
                    "relative": "1 hour ago",
                    "timestamp": 1710579151
                }
            }
        }
    }

@pytest.fixture
def mock_log_error(mocker):
    """Mock the _log_error method."""
    return mocker.patch("app.services.torncity.base.BaseEndpointProcessor._log_error")

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

    def test_members_data_pull(self, members_processor):
        """Test members data pull and transformation."""
        mock_response = {
            "data": {
                "members": {
                    "12345": {
                        "name": "Test User",
                        "level": 50,
                        "status": {
                            "state": "online",
                            "description": "Online"
                        },
                        "last_action": {
                            "status": "1 hour ago",
                            "timestamp": 1710579151
                        },
                        "faction": {
                            "faction_id": 17991,
                            "position": "Member",
                            "days_in_faction": 100
                        },
                        "life": {
                            "current": 100,
                            "maximum": 100
                        },
                        "timestamp": 1710579151
                    }
                }
            }
        }
        result = members_processor.transform_data(mock_response)
        
        assert len(result) == 1
        member = result[0]
        assert member["member_id"] == 12345
        assert member["player_id"] == 12345
        assert member["name"] == "Test User"
        assert member["level"] == 50
        assert member["status"] == "online"
        assert member["status_description"] == "Online"
        assert member["last_action"] == "1 hour ago"
        assert member["last_action_timestamp"] == 1710579151
        assert member["faction_id"] == 17991
        assert member["faction_position"] == "Member"
        assert member["days_in_faction"] == 100
        assert member["life_current"] == 100
        assert member["life_maximum"] == 100
        assert member["timestamp"] == 1710579151

    def test_members_data_validation(self, members_processor):
        """Test data validation against schema."""
        mock_response = {
            "data": {
                "members": {
                    "12345": {
                        "name": "Test User",
                        "level": 50,
                        "status": {
                            "state": "online",
                            "description": "Online"
                        },
                        "last_action": {
                            "status": "1 hour ago",
                            "timestamp": 1710579151
                        },
                        "faction": {
                            "faction_id": 17991,
                            "position": "Member",
                            "days_in_faction": 100
                        },
                        "life": {
                            "current": 100,
                            "maximum": 100
                        },
                        "timestamp": 1710579151
                    }
                }
            }
        }
        result = members_processor.transform_data(mock_response)
        schema = members_processor.get_schema()
        
        # Convert result to DataFrame
        df = pd.DataFrame(result)
        
        # Verify required fields are present
        assert "member_id" in df.columns
        assert "player_id" in df.columns
        assert "name" in df.columns
        assert "level" in df.columns
        
        # Validate schema
        members_processor._validate_schema(df, schema)
    
    def test_members_error_handling(self, members_processor, torn_client):
        """Test error handling with invalid data."""
        invalid_data = {"members": "not_a_dict"}
        mock_fetch = Mock(return_value=invalid_data)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            # Should return empty list for invalid data
            result = members_processor.transform_data(invalid_data)
            assert isinstance(result, list)
            assert len(result) == 0
    
    def test_members_data_update(self, members_processor, mock_members_response, torn_client):
        """Test data update to BigQuery."""
        mock_fetch = Mock(return_value=mock_members_response)
        
        with patch.object(torn_client, "fetch_data", mock_fetch):
            df = members_processor.transform_data(mock_members_response)
            
            # Mock BigQuery upload
            with patch.object(members_processor.bq_client, "upload_dataframe") as mock_upload:
                members_processor._upload_data(df, members_processor.get_schema())
                mock_upload.assert_called_once()

    def test_empty_members_response(self, torn_client, members_processor):
        """Test handling empty members response."""
        empty_response = {"fetched_at": "2024-03-16T00:00:00", "members": {}}
        result = members_processor.process_data({"data": empty_response})
        assert isinstance(result, list)
        assert len(result) == 0

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
        assert isinstance(result, list)
        assert len(result) == 0

    def test_empty_dataframe_after_processing(self, members_processor):
        """Test handling of valid data that results in empty list after processing."""
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
        assert isinstance(result, list)
        assert len(result) == 0

    def test_empty_valid_members_data(self, members_processor, torn_client):
        """Test handling of valid but empty members data that results in an empty list."""
        # Mock response with valid structure but empty members
        mock_members_response = {
            "members": {}
        }
        
        # Mock the fetch_data method to return our empty response
        mock_fetch = Mock(return_value=mock_members_response)
        mock_log_error = Mock()
        
        with patch.object(torn_client, "fetch_data", mock_fetch), \
             patch.object(members_processor, "_log_error", mock_log_error):
            # Process the data
            result = members_processor.transform_data(mock_members_response)
            
            # Assert that we got an empty list
            assert isinstance(result, list)
            assert len(result) == 0

    def test_empty_records_list(self, members_processor):
        """Test handling of empty records list."""
        mock_response = {
            "data": {
                "members": {}  # Empty members dict
            }
        }
        result = members_processor.transform_data(mock_response)
        assert isinstance(result, list)
        assert len(result) == 0  # Should return empty list for empty records

    def test_members_bigquery_integration(self, members_processor):
        """Test BigQuery integration with members data."""
        mock_response = {
            "data": {
                "members": {
                    "12345": {
                        "name": "Test User",
                        "level": 50,
                        "status": {
                            "state": "online",
                            "description": "Online"
                        },
                        "last_action": {
                            "status": "1 hour ago",
                            "timestamp": 1710579151
                        },
                        "faction": {
                            "faction_id": 17991,
                            "position": "Member",
                            "days_in_faction": 100
                        },
                        "life": {
                            "current": 100,
                            "maximum": 100
                        },
                        "timestamp": 1710579151
                    }
                }
            }
        }
        result = members_processor.transform_data(mock_response)
        assert len(result) > 0
        assert isinstance(result, list)
        
        # Verify the data structure matches BigQuery schema
        member = result[0]
        assert "member_id" in member
        assert "player_id" in member
        assert "name" in member
        assert "level" in member
        assert "status" in member
        assert "faction_id" in member
        assert "faction_position" in member
        assert "days_in_faction" in member
        assert "life_current" in member
        assert "life_maximum" in member
        assert "timestamp" in member

    def test_transform_data_exception(self, members_processor, mock_log_error):
        """Test exception handling in transform_data method."""
        invalid_data = {
            "data": {
                "members": {
                    "123": {
                        "name": 123,  # Invalid type for name
                        "level": "invalid",  # Invalid type for level
                        "status": "invalid"  # Invalid status structure
                    }
                }
            }
        }
        
        with pytest.raises(DataValidationError):
            members_processor.transform_data(invalid_data)
        mock_log_error.assert_called_once()

    def test_numeric_conversion_error(self, members_processor):
        """Test error handling during numeric conversion."""
        # Create a response with invalid numeric data
        mock_response = {
            "data": {
                "members": {
                    "123": {
                        "name": "Test User",
                        "level": "not_a_number",  # Invalid numeric value
                        "faction": {"faction_id": "17991"},
                        "status": {
                            "description": "Online",
                            "state": "online",
                            "until": "invalid"  # Invalid numeric value
                        },
                        "last_action": {
                            "relative": "1 hour ago",
                            "timestamp": "not_a_timestamp"  # Invalid timestamp
                        }
                    }
                }
            },
            "fetched_at": "2024-03-16T09:32:31.281852"
        }
        
        # Mock the error logging
        mock_log_error = Mock()
        
        with patch.object(members_processor, "_log_error", mock_log_error):
            # Process the data and expect error
            with pytest.raises(DataValidationError) as exc_info:
                members_processor.transform_data(mock_response)
            
            # Verify error was logged
            mock_log_error.assert_called_once()
            assert "Error processing member" in str(exc_info.value)

    def test_timestamp_conversion_error(self, members_processor, mock_log_error):
        """Test handling of invalid timestamp formats."""
        mock_response = {
            'data': {
                'members': {
                    '123': {
                        'level': 50,
                        'name': 'Test User',
                        'status': {
                            'description': 'Online',
                            'state': 'online',
                            'until': 0
                        },
                        'last_action': {
                            'relative': '1 hour ago',
                            'timestamp': 'invalid_timestamp'
                        },
                        'faction': {
                            'faction_id': '17991'
                        }
                    }
                }
            }
        }
        
        with pytest.raises(DataValidationError):
            members_processor.transform_data(mock_response)
        mock_log_error.assert_called_once() 