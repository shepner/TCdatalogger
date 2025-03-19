"""Unit tests for crimes endpoint processor."""

import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch, mock_open, Mock
import time
from typing import Any

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
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('difficulty', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('created_at', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('planning_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('executed_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('ready_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('expired_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('rewards_money', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('rewards_respect', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('rewards_payout_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_percentage', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_paid_by', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_paid_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('rewards_items_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('rewards_items_quantity', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('slots_position', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_success_chance', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('slots_crime_pass_rate', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_is_reusable', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_is_available', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_joined_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_progress', 'FLOAT', mode='NULLABLE')
        ]


@pytest.fixture
def crimes_processor():
    """Create a test crimes processor instance."""
    config = {
        'api_key': 'test_api_key',
        'gcp_project_id': 'test-project',
        'gcp_credentials_file': 'test_credentials.json',
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
def mock_crimes_response():
    """Mock response from crimes endpoint."""
    return {
        "crimes": {
            "1": {
                "id": 1,
                "name": "Test Crime",
                "difficulty": "medium",
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
        self.mock_client.get_endpoint_retry_delay.return_value = 5
        self.mock_client.get_endpoint_retry_backoff.return_value = 2
        self.mock_client.get_endpoint_retry_max_delay.return_value = 60
        self.mock_client.get_endpoint_retry_on_status.return_value = [500, 502, 503, 504]
        self.mock_client.get_endpoint_retry_on_methods.return_value = ['GET']
        self.mock_client.get_endpoint_retry_on_exceptions.return_value = [Exception]
        self.mock_client.get_endpoint_retry_on_result.return_value = None
        self.mock_client.get_endpoint_retry_on_result_callback.return_value = None
        self.mock_client.get_endpoint_retry_on_result_callback_args.return_value = None
        self.mock_client.get_endpoint_retry_on_result_callback_kwargs.return_value = None
        self.mock_client._log_error = MagicMock()

    def _get_processor(self):
        """Get a test crimes processor instance."""
        config = {
            'dataset': 'test_dataset',
            'gcp_credentials_file': 'test_creds.json',
            'tc_api_key_file': 'test_api_keys.txt',
            'gcp_project_id': 'test-project',
            'selection': 'default',
            'storage_mode': 'append'
        }
        endpoint_config = {
            'endpoint': '/v2/faction/crimes',
            'name': 'crimes',
            'table': 'test_crimes'
        }
        
        # Create mock clients
        mock_torn_client = MagicMock(spec=TornClient)
        mock_torn_client.api_keys = {'crimes': 'test_key_2', 'default': 'test_key_1'}
        mock_torn_client.fetch_data = MagicMock(return_value={})
        
        mock_bq_client = MagicMock(spec=BigQueryClient)
        mock_bq_client.client = MagicMock()
        mock_bq_client.dataset = 'test_dataset'
        mock_bq_client.project_id = 'test-project'
        
        mock_monitoring_client = MagicMock(spec=monitoring_v3.MetricServiceClient)
        mock_monitoring_client.common_project_path.return_value = "projects/test-project"
        
        # Merge configs
        config.update(endpoint_config)
        
        # Patch all three clients
        with patch('app.services.torncity.base.TornClient') as mock_torn_client_class, \
             patch('app.services.torncity.base.BigQueryClient') as mock_bq_client_class, \
             patch('app.services.torncity.base.monitoring_v3.MetricServiceClient') as mock_monitoring_client_class:
            mock_torn_client_class.return_value = mock_torn_client
            mock_bq_client_class.return_value = mock_bq_client
            mock_monitoring_client_class.return_value = mock_monitoring_client
            processor = TestCrimesEndpointProcessor(config)
            return processor

    def test_crimes_data_pull(self, crimes_processor, mock_crimes_response, torn_client):
        """Test pulling and transforming crimes data."""
        # Mock the API call
        with patch.object(torn_client, "fetch_data", return_value=mock_crimes_response):
            # Process the data
            result = crimes_processor.transform_data(mock_crimes_response)
            
            # Verify results
            assert not result.empty
            assert 'id' in result.columns
            assert 'name' in result.columns
            assert result.iloc[0]['id'] == 1
            assert result.iloc[0]['name'] == 'Test Crime'
            assert result.iloc[0]['difficulty'] == 'medium'
            assert result.iloc[0]['status'] == 'completed'
            assert result.iloc[0]['participant_count'] == 2
            assert result.iloc[0]['participant_ids'] == '12345,67890'
            assert result.iloc[0]['participant_names'] == 'Player1,Player2'
            assert result.iloc[0]['reward_money'] == 1000000
            assert result.iloc[0]['reward_respect'] == 100
            assert result.iloc[0]['reward_item_count'] == 2
            assert result.iloc[0]['reward_item_ids'] == '1,2'
            assert result.iloc[0]['reward_item_quantities'] == '2,1'

    @patch('app.services.torncity.base.TornClient')
    @patch('app.services.torncity.base.BigQueryClient')
    @patch('app.services.torncity.base.monitoring_v3.MetricServiceClient')
    def test_crimes_data_validation(self, mock_metric_client, mock_bq_client, mock_torn_client):
        """Test validation of crimes data against schema."""
        # Create test data
        mock_crimes_response = {
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'difficulty': 'medium',
                    'status': 'completed',
                    'created_at': 1646956800,
                    'planning_at': 1646956800,
                    'executed_at': 1646960400,
                    'ready_at': 1646960400,
                    'expired_at': 1646960400,
                    'participants': [
                        {'id': 12345, 'name': 'Player1'},
                        {'id': 67890, 'name': 'Player2'}
                    ],
                    'rewards': {
                        'money': 1000000,
                        'respect': 100,
                        'items': [
                            {'id': 1, 'quantity': 2},
                            {'id': 2, 'quantity': 1}
                        ]
                    }
                }
            },
            'timestamp': 1646960400,
            'fetched_at': datetime.now()
        }
        
        # Create test configuration
        config = {
            'dataset': 'test_dataset',
            'gcp_credentials_file': 'test_creds.json',
            'tc_api_key_file': 'test_api_keys.txt'
        }
        endpoint_config = {
            'name': 'crimes',
            'table': 'test_crimes',
            'endpoint': '/v2/faction/crimes'
        }
        
        # Process test data
        crimes_processor = TestCrimesEndpointProcessor(config, endpoint_config)
        df = crimes_processor.transform_data(mock_crimes_response)
        
        # Get schema
        schema = crimes_processor.get_schema()
        
        # Validate schema
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

    def test_empty_records_list(self):
        """Test handling of empty records list."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {}  # Empty crimes dictionary
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
            'endpoint': 'crimes',
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
            'crimes': {
                '123': {
                    'id': 123,
                    'name': 'Test Crime',
                    'status': 'completed',
                    'timestamp': 1710633600
                }
            }
        }

        # Process data
        crimes_processor.process_data({'data': test_data})

        # Verify BigQuery interactions
        mock_bigquery_client.get_table.assert_called_once()
        mock_bigquery_client.load_table_from_dataframe.assert_called_once()

    def test_rewards_data_processing(self):
        """Test processing of rewards data."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'rewards': {
                        'money': 1000000,
                        'respect': 100,
                        'items': [
                            {'id': 1, 'quantity': 2},
                            {'id': 2, 'quantity': 3},
                            {'id': 3, 'quantity': 1}
                        ]
                    }
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert df['reward_money'].iloc[0] == 1000000
        assert df['reward_respect'].iloc[0] == 100
        assert df['reward_item_count'].iloc[0] == 3
        assert df['reward_item_ids'].iloc[0] == '1,2,3'
        assert df['reward_item_quantities'].iloc[0] == '2,3,1'

    def test_participants_data_processing(self):
        """Test processing of participants data."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'participants': [
                        {'id': 123, 'name': 'Player1'},
                        {'id': 456, 'name': 'Player2'},
                        {'id': 789, 'name': 'Player3'}
                    ]
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert df['participant_count'].iloc[0] == 3
        assert df['participant_ids'].iloc[0] == '123,456,789'
        assert df['participant_names'].iloc[0] == 'Player1,Player2,Player3'

    def test_server_timestamp_handling(self):
        """Test handling of server timestamps."""
        crimes_processor = self._get_processor()
        
        test_cases = [
            (1710633600, pd.Timestamp('2024-03-17 00:00:00')),  # Unix timestamp
            ('2024-03-17T00:00:00', pd.Timestamp('2024-03-17 00:00:00')),  # ISO format
            (None, pd.Timestamp('now').floor('S')),  # None value should use current time
            ('invalid', pd.Timestamp('now').floor('S'))  # Invalid value should use current time
        ]
        
        for input_ts, expected_ts in test_cases:
            data = {
                'timestamp': input_ts,
                'fetched_at': '2024-03-17T00:00:00',
                'crimes': {
                    '1': {
                        'id': 1,
                        'name': 'Test Crime'
                    }
                }
            }
            
            df = crimes_processor.transform_data(data)
            assert not df.empty
            if input_ts in (None, 'invalid'):
                assert pd.Timestamp(df['server_timestamp'].iloc[0]).floor('S') >= expected_ts
            else:
                assert pd.Timestamp(df['server_timestamp'].iloc[0]) == expected_ts

    def test_old_field_name_mapping(self):
        """Test mapping of old field names to new ones."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'crime_name': 'Old Name',  # Should map to 'name'
                    'time_started': 1710633600,  # Should map to 'created_at'
                    'time_completed': 1710637200,  # Should map to 'executed_at'
                    'initiated_by': 123,  # Should be added to participants
                    'planned_by': 456,  # Should be added to participants
                    'money_gain': 1000000,  # Should map to rewards.money
                    'respect_gain': 100  # Should map to rewards.respect
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert df['name'].iloc[0] == 'Old Name'
        assert pd.Timestamp(df['created_at'].iloc[0]) == pd.Timestamp('2024-03-17 00:00:00')
        assert pd.Timestamp(df['executed_at'].iloc[0]) == pd.Timestamp('2024-03-17 01:00:00')
        assert df['participant_count'].iloc[0] == 2
        assert df['participant_ids'].iloc[0] == '123,456'
        assert df['reward_money'].iloc[0] == 1000000
        assert df['reward_respect'].iloc[0] == 100

    def test_invalid_participant_types(self):
        """Test handling of invalid participant types."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'participants': 'not_a_list'  # Invalid type
                },
                '2': {
                    'id': 2,
                    'name': 'Test Crime 2',
                    'participants': [
                        'not_a_dict',  # Invalid type
                        {'wrong_field': 123},  # Missing required fields
                        {'id': 'not_a_number', 'name': 'Player1'},  # Invalid ID
                        {'id': 123, 'name': 'Valid Player'}  # Valid participant
                    ]
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert len(df) == 2
        assert df.loc[df['id'] == 1, 'participant_count'].iloc[0] == 0
        assert df.loc[df['id'] == 1, 'participant_ids'].iloc[0] == ''
        assert df.loc[df['id'] == 2, 'participant_count'].iloc[0] == 1
        assert df.loc[df['id'] == 2, 'participant_ids'].iloc[0] == '123'

    def test_invalid_rewards_types(self):
        """Test handling of invalid reward types."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'rewards': 'not_a_dict'  # Invalid type
                },
                '2': {
                    'id': 2,
                    'name': 'Test Crime 2',
                    'rewards': {
                        'money': 'not_a_number',  # Invalid money
                        'respect': None,  # Invalid respect
                        'items': 'not_a_list'  # Invalid items
                    }
                },
                '3': {
                    'id': 3,
                    'name': 'Test Crime 3',
                    'rewards': {
                        'money': 1000000,
                        'respect': 100,
                        'items': [
                            'not_a_dict',  # Invalid type
                            {'wrong_field': 123},  # Missing required fields
                            {'id': 'not_a_number', 'quantity': 2},  # Invalid ID
                            {'id': 123, 'quantity': 'not_a_number'},  # Invalid quantity
                            {'id': 456, 'quantity': 3}  # Valid item
                        ]
                    }
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert len(df) == 3
        # Check first crime (invalid rewards)
        assert df.loc[df['id'] == 1, 'reward_money'].iloc[0] == 0
        assert df.loc[df['id'] == 1, 'reward_respect'].iloc[0] == 0
        assert df.loc[df['id'] == 1, 'reward_item_count'].iloc[0] == 0
        # Check second crime (invalid money/respect/items)
        assert df.loc[df['id'] == 2, 'reward_money'].iloc[0] == 0
        assert df.loc[df['id'] == 2, 'reward_respect'].iloc[0] == 0
        assert df.loc[df['id'] == 2, 'reward_item_count'].iloc[0] == 0
        # Check third crime (mixed valid/invalid items)
        assert df.loc[df['id'] == 3, 'reward_money'].iloc[0] == 1000000
        assert df.loc[df['id'] == 3, 'reward_respect'].iloc[0] == 100
        assert df.loc[df['id'] == 3, 'reward_item_count'].iloc[0] == 1  # Only count valid items
        assert df.loc[df['id'] == 3, 'reward_item_ids'].iloc[0] == '456'
        assert df.loc[df['id'] == 3, 'reward_item_quantities'].iloc[0] == '3'

    def test_convert_numerics_method(self):
        """Test the convert_numerics method directly."""
        crimes_processor = self._get_processor()
        
        df = pd.DataFrame({
            'id': ['123', 'invalid'],
            'count': ['456', 'also_invalid'],
            'not_a_number': ['a', 'b']
        })
        
        result = crimes_processor.convert_numerics(df, exclude_cols=['not_a_number'])
        assert pd.api.types.is_numeric_dtype(result['id'])
        assert pd.api.types.is_numeric_dtype(result['count'])
        assert result['not_a_number'].dtype == object
        assert pd.isna(result.loc[1, 'id'])
        assert pd.isna(result.loc[1, 'count'])

    def test_convert_timestamps_method(self):
        """Test the convert_timestamps method directly."""
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
                    'difficulty': 'medium',
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

    def test_column_type_conversion(self):
        """Test conversion of column types."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'id': None,  # Should default to 0
                    'name': None,  # Should default to empty string
                    'difficulty': None,  # Should default to empty string
                    'status': None,  # Should default to empty string
                    'participant_count': '5',  # String number
                    'reward_money': 1000.5,  # Float
                    'reward_respect': '100',  # String number
                    'reward_item_count': None  # Should default to 0
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert df['id'].iloc[0] == 0
        assert df['name'].iloc[0] == ''
        assert df['difficulty'].iloc[0] == ''
        assert df['status'].iloc[0] == ''
        assert df['participant_count'].iloc[0] == 5
        assert df['reward_money'].iloc[0] == 1000
        assert df['reward_respect'].iloc[0] == 100
        assert df['reward_item_count'].iloc[0] == 0

    def test_mixed_participant_data(self):
        """Test handling of mixed participant data types."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'participants': [
                        {'id': 123, 'name': 'Valid User'},  # Valid participant
                        None,  # None value
                        'invalid',  # Invalid type
                        {'id': None, 'name': None},  # None values
                        {'id': 456, 'name': 'Another User'},  # Valid participant
                        {'invalid': 'structure'}  # Missing required fields
                    ]
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert df['participant_count'].iloc[0] == 2
        assert df['participant_ids'].iloc[0] == '123,456'
        assert df['participant_names'].iloc[0] == 'Valid User,Another User'

    def test_mixed_rewards_data(self):
        """Test handling of mixed rewards data types."""
        crimes_processor = self._get_processor()
        
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '1': {
                    'id': 1,
                    'name': 'Test Crime',
                    'rewards': {
                        'money': None,  # None value
                        'respect': 'invalid',  # Invalid value
                        'items': [
                            {'id': 123, 'quantity': 2},  # Valid item
                            None,  # None value
                            'invalid',  # Invalid type
                            {'id': 456},  # Missing quantity
                            {'id': None, 'quantity': None},  # None values
                            {'invalid': 'structure'}  # Missing required fields
                        ]
                    }
                }
            }
        }
        
        df = crimes_processor.transform_data(data)
        assert not df.empty
        assert df['reward_money'].iloc[0] == 0
        assert df['reward_respect'].iloc[0] == 0
        assert df['reward_item_count'].iloc[0] == 2
        assert df['reward_item_ids'].iloc[0] == '123,456'
        assert df['reward_item_quantities'].iloc[0] == '2,1'

    def test_process_data_valid(self, processor, sample_crimes_data):
        """Test processing valid crimes data."""
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '123': {
                    'id': 123,
                    'name': 'Test Crime',
                    'difficulty': 'medium',
                    'status': 'completed',
                    'created_at': 1710633600,
                    'planning_at': 1710633600,
                    'executed_at': 1710633600,
                    'ready_at': 1710633600,
                    'expired_at': 1710633600,
                    'rewards': {
                        'money': 1000,
                        'respect': 10.5,
                        'payout': {
                            'type': 'percentage',
                            'percentage': 50.0,
                            'paid_by': 456,
                            'paid_at': '2024-03-17T01:00:00'
                        },
                        'items': [
                            {'id': 1, 'quantity': 2},
                            {'id': 2, 'quantity': 1}
                        ]
                    },
                    'slots': {
                        'position': 1,
                        'user_id': 789,
                        'success_chance': 75.5,
                        'crime_pass_rate': 80.0,
                        'item_requirement_id': 123,
                        'item_requirement_is_reusable': True,
                        'item_requirement_is_available': True,
                        'user_joined_at': '2024-03-17T00:30:00',
                        'user_progress': 50.0
                    }
                }
            }
        }
        
        processed_data = processor.process_data(data)
        assert len(processed_data) == 1
        
        crime = processed_data[0]
        assert isinstance(crime['server_timestamp'], pd.Timestamp)
        assert crime['id'] == 123
        assert crime['name'] == 'Test Crime'
        assert crime['difficulty'] == 'medium'
        assert crime['status'] == 'completed'
        assert isinstance(crime['created_at'], pd.Timestamp)
        assert isinstance(crime['planning_at'], pd.Timestamp)
        assert isinstance(crime['executed_at'], pd.Timestamp)
        assert isinstance(crime['ready_at'], pd.Timestamp)
        assert isinstance(crime['expired_at'], pd.Timestamp)
        assert crime['rewards_money'] == 1000
        assert crime['rewards_respect'] == 10.5
        assert crime['rewards_payout_type'] == 'percentage'
        assert crime['rewards_payout_percentage'] == 50.0
        assert crime['rewards_payout_paid_by'] == 456
        assert isinstance(crime['rewards_payout_paid_at'], pd.Timestamp)
        assert crime['rewards_items_id'] == '1,2'
        assert crime['rewards_items_quantity'] == '2,1'
        assert crime['slots_position'] == 1
        assert crime['slots_user_id'] == 789
        assert crime['slots_success_chance'] == 75.5
        assert crime['slots_crime_pass_rate'] == 80.0
        assert crime['slots_item_requirement_id'] == 123
        assert crime['slots_item_requirement_is_reusable'] is True
        assert crime['slots_item_requirement_is_available'] is True
        assert isinstance(crime['slots_user_joined_at'], pd.Timestamp)
        assert crime['slots_user_progress'] == 50.0

    def test_process_data_empty(self, crimes_processor):
        """Test processing empty data."""
        with pytest.raises(DataValidationError):
            crimes_processor.process_data({'data': {}})

    def test_process_data_missing_fields(self, crimes_processor):
        """Test handling of missing required fields."""
        invalid_data = {
            "crimes": {
                "1": {
                    "name": "Test Crime",  # Missing required fields
                    "status": "completed"
                }
            }
        }
        with pytest.raises(DataValidationError) as exc:
            crimes_processor.process_data({"data": invalid_data})
        assert "Missing required field" in str(exc.value)

    def test_process_data_invalid_types(self, crimes_processor):
        """Test processing data with invalid types."""
        data = {
            'timestamp': 1710633600,
            'fetched_at': '2024-03-17T00:00:00',
            'crimes': {
                '123': {
                    'id': '123abc',  # Invalid integer
                    'name': 123,  # Invalid string
                    'rewards': {
                        'money': 'invalid',  # Invalid integer
                        'respect': 'invalid',  # Invalid float
                        'payout': {
                            'percentage': 'invalid'  # Invalid float
                        }
                    },
                    'slots': {
                        'success_chance': 'invalid',  # Invalid float
                        'user_joined_at': 'invalid'  # Invalid timestamp
                    }
                }
            }
        }
        
        processed_data = crimes_processor.process_data(data)
        assert len(processed_data) == 1
        
        crime = processed_data[0]
        assert crime['id'] == 123  # Should handle invalid integer
        assert crime['name'] == '123'  # Should convert to string
        assert crime['rewards_money'] == 0  # Should default to 0
        assert crime['rewards_respect'] == 0.0  # Should default to 0.0
        assert crime['rewards_payout_percentage'] == 0.0  # Should default to 0.0
        assert crime['slots_success_chance'] is None  # Should be None for invalid float
        assert crime['slots_user_joined_at'] is None  # Should be None for invalid timestamp 