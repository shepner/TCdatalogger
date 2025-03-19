"""Unit tests for Torn City base processor error handling and validation."""

from unittest.mock import Mock, patch
import pytest
from google.cloud import bigquery
from google.api_core import exceptions

from app.services.torncity.base import BaseEndpointProcessor
from app.services.torncity.client import (
    TornAPIError,
    TornAPIKeyError,
    TornAPIRateLimitError,
    TornAPIConnectionError,
    TornAPITimeoutError
)

class TestTornBaseProcessorErrorHandling:
    """Test suite for TornBaseProcessor error handling."""

    @pytest.fixture
    def processor(self, sample_config, mock_api_keys):
        """Create a test processor instance."""
        class TestProcessor(BaseEndpointProcessor):
            def process_data(self, data):
                return data

        return TestProcessor(sample_config)

    def test_api_key_error_handling(self, processor):
        """Test handling of API key errors."""
        with patch.object(processor.torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = TornAPIKeyError("Invalid API key")
            
            with pytest.raises(TornAPIKeyError) as exc:
                processor.fetch_torn_data("test_url", "default")
            assert "Invalid API key" in str(exc.value)

    def test_rate_limit_error_handling(self, processor):
        """Test handling of rate limit errors."""
        with patch.object(processor.torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = TornAPIRateLimitError("Rate limit exceeded")
            
            with pytest.raises(TornAPIRateLimitError) as exc:
                processor.fetch_torn_data("test_url", "default")
            assert "Rate limit exceeded" in str(exc.value)

    def test_connection_error_handling(self, processor):
        """Test handling of connection errors."""
        with patch.object(processor.torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = TornAPIConnectionError("Connection failed")
            
            with pytest.raises(TornAPIConnectionError) as exc:
                processor.fetch_torn_data("test_url", "default")
            assert "Connection failed" in str(exc.value)

    def test_timeout_error_handling(self, processor):
        """Test handling of timeout errors."""
        with patch.object(processor.torn_client, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = TornAPITimeoutError("Request timed out")
            
            with pytest.raises(TornAPITimeoutError) as exc:
                processor.fetch_torn_data("test_url", "default")
            assert "Request timed out" in str(exc.value)

    def test_bigquery_error_handling(self, processor):
        """Test handling of BigQuery errors."""
        test_data = [{"id": 1}]
        test_schema = [bigquery.SchemaField("id", "INTEGER")]
        
        with patch.object(processor.bq_client, 'write_data') as mock_write:
            mock_write.side_effect = exceptions.BadRequest("Invalid data")
            
            with pytest.raises(exceptions.BadRequest) as exc:
                processor.write_to_bigquery(test_data, "test_table", test_schema)
            assert "Invalid data" in str(exc.value)

    def test_empty_data_handling(self, processor):
        """Test handling of empty data."""
        with pytest.raises(ValueError) as exc:
            processor.write_to_bigquery([], "test_table", [])
        assert "No data to write" in str(exc.value)

    def test_invalid_schema_handling(self, processor):
        """Test handling of invalid schema."""
        test_data = [{"id": 1}]
        invalid_schema = []  # Empty schema
        
        with pytest.raises(ValueError) as exc:
            processor.write_to_bigquery(test_data, "test_table", invalid_schema)
        assert "Invalid schema" in str(exc.value)

    def test_data_schema_mismatch_handling(self, processor):
        """Test handling of data-schema mismatches."""
        test_data = [{"id": "not_an_integer"}]
        test_schema = [bigquery.SchemaField("id", "INTEGER")]
        
        with patch.object(processor.bq_client, 'write_data') as mock_write:
            mock_write.side_effect = exceptions.BadRequest("Type mismatch")
            
            with pytest.raises(exceptions.BadRequest) as exc:
                processor.write_to_bigquery(test_data, "test_table", test_schema)
            assert "Type mismatch" in str(exc.value)

class TestTornBaseProcessorValidation:
    """Test suite for TornBaseProcessor validation."""

    def test_config_validation_missing_fields(self):
        """Test validation of configuration with missing fields."""
        invalid_configs = [
            {},  # Empty config
            {"tc_api_key_file": "test.json"},  # Missing project_id
            {"gcp_project_id": "test-project"},  # Missing api_key_file
            {"tc_api_key_file": "test.json", "gcp_project_id": "test-project"}  # Missing storage_mode
        ]
        
        for config in invalid_configs:
            with pytest.raises(ValueError) as exc:
                BaseEndpointProcessor.validate_config(config)
            assert "Missing required configuration" in str(exc.value)

    def test_config_validation_invalid_storage_mode(self, sample_config):
        """Test validation of invalid storage modes."""
        sample_config["storage_mode"] = "invalid_mode"
        
        with pytest.raises(ValueError) as exc:
            BaseEndpointProcessor.validate_config(sample_config)
        assert "Invalid storage mode" in str(exc.value)

    def test_config_validation_invalid_project_id(self, sample_config):
        """Test validation of invalid project IDs."""
        invalid_project_ids = [
            "",  # Empty
            "invalid-chars-#$%",  # Invalid characters
            "a" * 100  # Too long
        ]
        
        for project_id in invalid_project_ids:
            sample_config["gcp_project_id"] = project_id
            with pytest.raises(ValueError) as exc:
                BaseEndpointProcessor.validate_config(sample_config)
            assert "Invalid project ID" in str(exc.value)

    def test_config_validation_invalid_api_key_file(self, sample_config):
        """Test validation of invalid API key file paths."""
        invalid_paths = [
            "",  # Empty
            "/invalid/path/with/special/chars/#$%",  # Invalid characters
            "a" * 1000  # Too long
        ]
        
        for path in invalid_paths:
            sample_config["tc_api_key_file"] = path
            with pytest.raises(ValueError) as exc:
                BaseEndpointProcessor.validate_config(sample_config)
            assert "Invalid API key file path" in str(exc.value)

    def test_config_validation_success(self, sample_config):
        """Test successful configuration validation."""
        # Should not raise any exceptions
        BaseEndpointProcessor.validate_config(sample_config)

    def test_schema_validation(self, processor):
        """Test validation of BigQuery schema."""
        valid_schema = [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE")
        ]
        
        invalid_schemas = [
            [],  # Empty schema
            None,  # None schema
            [{"not": "a schema field"}],  # Invalid schema field
            [bigquery.SchemaField("id", "INVALID_TYPE")]  # Invalid field type
        ]
        
        # Valid schema should not raise exception
        processor.validate_schema(valid_schema)
        
        # Invalid schemas should raise ValueError
        for schema in invalid_schemas:
            with pytest.raises(ValueError):
                processor.validate_schema(schema)

    def test_data_validation(self, processor):
        """Test validation of data against schema."""
        schema = [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE")
        ]
        
        valid_data = [
            {"id": 1, "name": "test"},
            {"id": 2, "name": None}
        ]
        
        invalid_data_cases = [
            [],  # Empty data
            [{"wrong_field": "value"}],  # Missing required field
            [{"id": "not_an_integer", "name": "test"}],  # Wrong type
            [{"id": 1, "name": "test", "extra": "field"}]  # Extra field
        ]
        
        # Valid data should not raise exception
        processor.validate_data(valid_data, schema)
        
        # Invalid data should raise ValueError
        for data in invalid_data_cases:
            with pytest.raises(ValueError):
                processor.validate_data(data, schema) 