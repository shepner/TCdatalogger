"""Integration tests for the data pipeline between Torn City API and BigQuery."""

from unittest.mock import Mock, patch, call
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import pytest
from google.cloud import bigquery
from google.api_core import exceptions

from app.services.torncity.client import TornClient, TornAPIError
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.processors import (
    UserProcessor,
    ItemsProcessor,
    CrimeProcessor,
    CurrencyProcessor,
    MembersProcessor
)

class TestDataPipelineIntegration:
    """Test suite for data pipeline integration."""

    @pytest.fixture
    def torn_client(self, mock_api_keys):
        """Create a TornClient instance."""
        return TornClient(mock_api_keys)

    @pytest.fixture
    def mock_bigquery_client(self, mocker):
        """Create a mock BigQuery client."""
        mock_client = mocker.patch('google.cloud.bigquery.Client')
        mock_client.return_value.get_table.return_value = mocker.Mock()
        mock_client.return_value.load_table_from_dataframe.return_value = mocker.Mock()
        mock_client.return_value.load_table_from_dataframe.return_value.result.return_value = None
        return mock_client

    @pytest.fixture
    def mock_credentials(self):
        """Mock Google Cloud credentials."""
        with patch('google.oauth2.service_account.Credentials') as mock_creds:
            mock_creds.from_service_account_file.return_value = mock_creds
            return mock_creds

    @pytest.fixture
    def mock_monitoring_client(self):
        """Mock Google Cloud monitoring client."""
        with patch('google.cloud.monitoring_v3.MetricServiceClient') as mock_client:
            return mock_client.return_value

    @pytest.fixture
    def sample_config(self):
        """Create a sample configuration for testing."""
        return {
            "storage_mode": "append",
            "endpoint": "user",
            "table_id": "test_table",
            "dataset_id": "test_dataset",
            "project_id": "test-project",
            "credentials_path": "test_credentials.json",
            "gcp_project_id": "test-project",
            "gcp_credentials_file": "test_credentials.json",
            "dataset": "test_dataset",
            "selection": "default",
            "api_key": "test_api_key"
        }

    @pytest.fixture
    def user_processor(self, sample_config):
        """Create a UserProcessor instance."""
        return UserProcessor(sample_config)

    @pytest.fixture
    def members_processor(self, sample_config):
        """Create a MembersProcessor instance."""
        return MembersProcessor(sample_config)

    @classmethod
    @pytest.fixture
    def mock_torn_response(cls):
        """Mock response from Torn API."""
        return {
            "data": {
                "members": {
                    "12345": {
                        "name": "TestUser",
                        "level": 10,
                        "status": {
                            "state": "Okay",
                            "description": "Online"
                        },
                        "last_action": {
                            "status": "Online",
                            "timestamp": 1710766800
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
                        "timestamp": 1710766800
                    }
                }
            }
        }

    def test_end_to_end_data_pipeline(self, mock_bigquery_client, mock_torn_response, sample_config):
        """Test end-to-end data pipeline processing."""
        # Create and run processor
        processor = MembersProcessor(sample_config)
        result = processor.run()

        # Verify result
        assert result is not None
        assert len(result) == 1
        member = result[0]
        assert member["member_id"] == 12345
        assert member["player_id"] == 12345
        assert member["name"] == "TestUser"
        assert member["level"] == 10
        assert member["status"] == "Okay"
        assert member["status_description"] == "Online"
        assert member["last_action"] == "Online"
        assert member["last_action_timestamp"] == 1710766800
        assert member["faction_id"] == 17991
        assert member["faction_position"] == "Member"
        assert member["life_current"] == 100
        assert member["life_maximum"] == 100
        assert member["days_in_faction"] == 100
        assert member["timestamp"] == 1710766800

        # Verify mock calls
        mock_torn_response.assert_called_once()
        mock_bigquery_client.write_data_to_table.assert_called_once()

    def test_data_validation(self, torn_client, mock_bigquery_client, members_processor, mock_torn_response):
        """Test data validation through the pipeline."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            # Fetch and transform data
            data = torn_client.fetch_data("user", "default")
            transformed = members_processor.transform_data(data)
            
            # Verify data validation
            assert len(transformed) == 1
            record = transformed[0]
            assert record["member_id"] == 12345
            assert record["player_id"] == 12345
            assert record["name"] == "TestUser"
            assert record["level"] == 10
            assert record["status"] == "Okay"
            assert record["status_description"] == "Online"
            assert record["last_action"] == "Online"
            assert record["last_action_timestamp"] == 1710766800
            assert record["faction_id"] == 17991
            assert record["faction_position"] == "Member"
            assert record["life_current"] == 100
            assert record["life_maximum"] == 100
            assert record["days_in_faction"] == 100
            assert record["timestamp"] == 1710766800

    def test_pipeline_error_handling(self, torn_client, mock_bigquery_client, members_processor):
        """Test error handling throughout the pipeline."""
        # Test API error handling
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = TornAPIError("API Error")
            
            with pytest.raises(TornAPIError):
                torn_client.fetch_data("user", "default")
        
        # Test transformation error handling
        invalid_data = {
            "data": {
                "members": {
                    "invalid": {
                        "name": 123,  # Invalid type for name
                        "level": "10",  # Invalid type for level
                        "status": "Okay"
                    }
                }
            }
        }
        with pytest.raises(ValueError):
            members_processor.transform_data(invalid_data)
        
        # Test BigQuery error handling
        with patch.object(mock_bigquery_client, 'write_data_to_table') as mock_write:
            mock_write.side_effect = exceptions.BadRequest("Invalid data")
            
            with pytest.raises(exceptions.BadRequest):
                mock_bigquery_client.write_data_to_table("test_table", [{"invalid": "data"}])

    def test_data_type_consistency(self, torn_client, mock_bigquery_client, members_processor, mock_torn_response):
        """Test data type consistency through the pipeline."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            # Fetch and transform data
            data = torn_client.fetch_data("user", "default")
            transformed = members_processor.transform_data(data)
            
            # Verify data types
            record = transformed[0]
            assert isinstance(record["member_id"], int)
            assert isinstance(record["player_id"], int)
            assert isinstance(record["name"], str)
            assert isinstance(record["level"], int)
            assert isinstance(record["status"], str)
            assert isinstance(record["status_description"], str)
            assert isinstance(record["last_action"], str)
            assert isinstance(record["last_action_timestamp"], int)
            assert isinstance(record["faction_id"], int)
            assert isinstance(record["faction_position"], str)
            assert isinstance(record["life_current"], int)
            assert isinstance(record["life_maximum"], int)
            assert isinstance(record["days_in_faction"], int)
            assert isinstance(record["timestamp"], int)

    def test_batch_processing(self, torn_client, mock_bigquery_client, members_processor):
        """Test batch processing of data."""
        # Generate batch of mock responses
        batch_size = 5
        mock_responses = [
            {
                "data": {
                    "members": {
                        str(i): {
                            "name": f"User{i}",
                            "level": 10 + i,
                            "status": {"state": "Okay"},
                            "last_action": {
                                "status": "Online",
                                "timestamp": 1646956800 + i
                            },
                            "faction": {
                                "faction_id": 17991,
                                "position": "Member"
                            },
                            "life": {
                                "current": 100,
                                "maximum": 100
                            },
                            "timestamp": 1646956800 + i
                        }
                    }
                }
            }
            for i in range(batch_size)
        ]
        
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = mock_responses
            
            with patch.object(mock_bigquery_client, 'batch_write_data') as mock_batch_write:
                mock_batch_write.return_value = []  # No errors
                
                transformed_batch = []
                for i in range(batch_size):
                    data = torn_client.fetch_data("user", "default")
                    transformed = members_processor.transform_data(data)
                    transformed_batch.extend(transformed)
                
                assert len(transformed_batch) == batch_size
                
                # Write batch to BigQuery
                mock_bigquery_client.batch_write_data("users", transformed_batch)
                mock_batch_write.assert_called_once()

    def test_schema_evolution_handling(self, torn_client, mock_bigquery_client, members_processor, mock_torn_response):
        """Test handling of schema evolution."""
        # Add new field to API response
        mock_torn_response["data"]["members"]["12345"]["new_field"] = "new_value"
        
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            with patch.object(mock_bigquery_client, 'update_table_schema') as mock_update_schema:
                # Fetch and transform data
                data = torn_client.fetch_data("user", "default")
                transformed = members_processor.transform_data(data)
                
                # Verify new field is handled
                assert len(transformed) == 1
                assert transformed[0]["member_id"] == 12345
                assert transformed[0]["name"] == "TestUser"
                
                # Simulate schema update
                new_schema = members_processor.get_schema()
                mock_bigquery_client.update_table_schema("users", new_schema)
                mock_update_schema.assert_called_once()

    def test_retry_and_recovery(self, torn_client, mock_bigquery_client, members_processor, mock_torn_response):
        """Test retry and recovery mechanisms."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            # Simulate transient failure then success
            mock_fetch.side_effect = [
                TornAPIError("Temporary error"),
                mock_torn_response
            ]
            
            with patch.object(mock_bigquery_client, 'write_data_with_retry') as mock_write_retry:
                # First attempt fails
                with pytest.raises(TornAPIError):
                    data = torn_client.fetch_data("user", "default")
                
                # Second attempt succeeds
                data = torn_client.fetch_data("user", "default")
                transformed = members_processor.transform_data(data)
                
                # Write with retry
                mock_bigquery_client.write_data_with_retry("users", transformed)
                mock_write_retry.assert_called_once()

    def test_concurrent_processing(self, torn_client, mock_bigquery_client, members_processor, mock_torn_response):
        """Test concurrent processing of data."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch, \
             patch.object(mock_bigquery_client, 'write_data_to_table') as mock_write:
            
            mock_fetch.return_value = mock_torn_response
            mock_write.return_value = None
            
            def process_endpoint(endpoint):
                data = torn_client.fetch_data(endpoint, "default")
                transformed = members_processor.transform_data(data)
                mock_bigquery_client.write_data_to_table(f"{endpoint}_table", transformed)
                return transformed
            
            # Process multiple endpoints concurrently
            endpoints = ['members', 'items', 'crimes', 'currency']
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(process_endpoint, endpoint) for endpoint in endpoints]
                results = [f.result() for f in futures]
            
            # Verify results
            for result in results:
                assert len(result) == 1
                assert result[0]["member_id"] == 12345
                assert result[0]["name"] == "TestUser"

    def test_error_recovery_and_partial_success(self, torn_client, mock_bigquery_client, members_processor):
        """Test error recovery and partial success handling."""
        # Generate batch of mock responses with some invalid data
        batch_size = 3
        mock_responses = [
            {
                "data": {
                    "members": {
                        str(i): {
                            "name": f"User{i}",
                            "level": 10 + i,
                            "status": {"state": "Okay"},
                            "last_action": {
                                "status": "Online",
                                "timestamp": 1646956800 + i
                            },
                            "faction": {
                                "faction_id": 17991,
                                "position": "Member"
                            },
                            "life": {
                                "current": 100,
                                "maximum": 100
                            },
                            "timestamp": 1646956800 + i
                        }
                    }
                }
            }
            for i in range(batch_size)
        ]
        
        with patch.object(torn_client, 'fetch_data') as mock_fetch, \
             patch.object(mock_bigquery_client, 'write_data_to_table') as mock_write:
            
            mock_fetch.side_effect = mock_responses
            mock_write.return_value = None
            
            successful_records = []
            failed_records = []
            
            for i in range(batch_size):
                try:
                    data = torn_client.fetch_data("user", "default")
                    transformed = members_processor.transform_data(data)
                    successful_records.extend(transformed)
                except Exception as e:
                    failed_records.append({"index": i, "error": str(e)})
            
            # Verify results
            assert len(successful_records) > 0
            assert len(successful_records) + len(failed_records) == batch_size 