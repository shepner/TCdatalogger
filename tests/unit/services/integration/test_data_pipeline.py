"""Integration tests for the data pipeline between Torn City API and BigQuery."""

from unittest.mock import Mock, patch, call
import json
from datetime import datetime, timedelta

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
    def bq_client(self, sample_config):
        """Create a BigQueryClient instance."""
        return BigQueryClient(sample_config)

    @pytest.fixture
    def user_processor(self, sample_config):
        """Create a UserProcessor instance."""
        return UserProcessor(sample_config)

    @pytest.fixture
    def mock_torn_response(self):
        """Create a mock Torn API response."""
        return {
            "level": 15,
            "gender": "Male",
            "player_id": 12345,
            "name": "TestUser",
            "status": {
                "state": "Okay",
                "description": "Test status"
            },
            "last_action": {
                "status": "Online",
                "timestamp": 1646956800
            }
        }

    def test_end_to_end_data_pipeline(self, torn_client, bq_client, user_processor, mock_torn_response):
        """Test complete data pipeline from API fetch to BigQuery storage."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            with patch.object(bq_client, 'write_data_to_table') as mock_write:
                mock_write.return_value = []  # No errors
                
                # Fetch data from API
                data = torn_client.fetch_data("user", "default")
                assert data == mock_torn_response
                
                # Transform data
                transformed = user_processor.transform_data(data)
                assert len(transformed) == 1
                assert transformed[0]["player_id"] == 12345
                
                # Write to BigQuery
                user_processor.write_to_bigquery(transformed, "users")
                mock_write.assert_called_once()

    def test_pipeline_error_handling(self, torn_client, bq_client, user_processor):
        """Test error handling throughout the pipeline."""
        # Test API error handling
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = TornAPIError("API Error")
            
            with pytest.raises(TornAPIError):
                torn_client.fetch_data("user", "default")
        
        # Test transformation error handling
        invalid_data = {"invalid": "data"}
        with pytest.raises(ValueError):
            user_processor.transform_data(invalid_data)
        
        # Test BigQuery error handling
        with patch.object(bq_client, 'write_data_to_table') as mock_write:
            mock_write.side_effect = exceptions.BadRequest("Invalid data")
            
            with pytest.raises(exceptions.BadRequest):
                bq_client.write_data_to_table("test_table", [{"invalid": "data"}])

    def test_data_type_consistency(self, torn_client, bq_client, user_processor, mock_torn_response):
        """Test data type consistency through the pipeline."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            # Fetch and transform data
            data = torn_client.fetch_data("user", "default")
            transformed = user_processor.transform_data(data)
            
            # Verify data types
            record = transformed[0]
            assert isinstance(record["player_id"], int)
            assert isinstance(record["name"], str)
            assert isinstance(record["level"], int)
            assert isinstance(record["status_state"], str)
            
            # Verify timestamp format
            timestamp = record.get("timestamp") or record.get("last_action_timestamp")
            assert timestamp is not None
            # Should be ISO format string
            datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

    def test_batch_processing(self, torn_client, bq_client, user_processor):
        """Test batch processing of data."""
        # Generate batch of mock responses
        batch_size = 5
        mock_responses = [
            {
                "player_id": i,
                "name": f"User{i}",
                "level": 10 + i,
                "status": {"state": "Okay"},
                "last_action": {
                    "status": "Online",
                    "timestamp": 1646956800 + i
                }
            }
            for i in range(batch_size)
        ]
        
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = mock_responses
            
            with patch.object(bq_client, 'batch_write_data') as mock_batch_write:
                mock_batch_write.return_value = []  # No errors
                
                transformed_batch = []
                for i in range(batch_size):
                    data = torn_client.fetch_data("user", "default")
                    transformed = user_processor.transform_data(data)
                    transformed_batch.extend(transformed)
                
                assert len(transformed_batch) == batch_size
                
                # Write batch to BigQuery
                bq_client.batch_write_data("users", transformed_batch)
                mock_batch_write.assert_called_once()

    def test_schema_evolution_handling(self, torn_client, bq_client, user_processor, mock_torn_response):
        """Test handling of schema evolution."""
        # Add new field to API response
        mock_torn_response["new_field"] = "new_value"
        
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            with patch.object(bq_client, 'update_table_schema') as mock_update_schema:
                # Fetch and transform data
                data = torn_client.fetch_data("user", "default")
                transformed = user_processor.transform_data(data)
                
                # Verify new field is handled
                assert "new_field" in transformed[0]
                
                # Simulate schema update
                new_schema = user_processor.get_schema()
                bq_client.update_table_schema("users", new_schema)
                mock_update_schema.assert_called_once()

    def test_retry_and_recovery(self, torn_client, bq_client, user_processor, mock_torn_response):
        """Test retry and recovery mechanisms."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            # Simulate transient failure then success
            mock_fetch.side_effect = [
                TornAPIError("Temporary error"),
                mock_torn_response
            ]
            
            with patch.object(bq_client, 'write_data_with_retry') as mock_write_retry:
                # First attempt fails
                with pytest.raises(TornAPIError):
                    data = torn_client.fetch_data("user", "default")
                
                # Second attempt succeeds
                data = torn_client.fetch_data("user", "default")
                transformed = user_processor.transform_data(data)
                
                # Write with retry
                bq_client.write_data_with_retry("users", transformed)
                mock_write_retry.assert_called_once()

    def test_data_validation(self, torn_client, bq_client, user_processor, mock_torn_response):
        """Test data validation at each stage."""
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            # 1. Validate API response
            data = torn_client.fetch_data("user", "default")
            assert "player_id" in data
            assert "name" in data
            
            # 2. Validate transformed data
            transformed = user_processor.transform_data(data)
            record = transformed[0]
            
            # Required fields
            assert "player_id" in record
            assert "name" in record
            assert "timestamp" in record
            
            # Field types
            assert isinstance(record["player_id"], int)
            assert isinstance(record["name"], str)
            
            # 3. Validate against BigQuery schema
            schema = user_processor.get_schema()
            user_processor.validate_data(transformed, schema)

    def test_concurrent_processing(self, torn_client, bq_client, user_processor, mock_torn_response):
        """Test concurrent processing of multiple endpoints."""
        endpoints = ["user", "items", "crimes", "currency", "members"]
        
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.return_value = mock_torn_response
            
            with patch.object(bq_client, 'write_data_to_table') as mock_write:
                from concurrent.futures import ThreadPoolExecutor
                
                def process_endpoint(endpoint):
                    data = torn_client.fetch_data(endpoint, "default")
                    transformed = user_processor.transform_data(data)
                    bq_client.write_data_to_table(f"{endpoint}_table", transformed)
                    return endpoint
                
                with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
                    futures = [
                        executor.submit(process_endpoint, endpoint)
                        for endpoint in endpoints
                    ]
                    
                    results = [f.result() for f in futures]
                    assert len(results) == len(endpoints)

    def test_error_recovery_and_partial_success(self, torn_client, bq_client, user_processor):
        """Test handling of partial failures and recovery."""
        batch_size = 3
        mock_responses = [
            {
                "player_id": i,
                "name": f"User{i}",
                "level": 10 + i,
                "status": {"state": "Okay"},
                "last_action": {
                    "status": "Online",
                    "timestamp": 1646956800 + i
                }
            }
            for i in range(batch_size)
        ]
        
        with patch.object(torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = mock_responses
            
            with patch.object(bq_client, 'write_data_to_table') as mock_write:
                # Simulate failure for second record
                mock_write.side_effect = [
                    [],  # Success
                    exceptions.BadRequest("Error"),  # Failure
                    []   # Success
                ]
                
                successful_records = []
                failed_records = []
                
                for i in range(batch_size):
                    try:
                        data = torn_client.fetch_data("user", "default")
                        transformed = user_processor.transform_data(data)
                        bq_client.write_data_to_table("users", transformed)
                        successful_records.extend(transformed)
                    except Exception as e:
                        failed_records.extend(transformed)
                
                assert len(successful_records) == 2
                assert len(failed_records) == 1 