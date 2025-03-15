"""Unit tests for Torn City base processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery

from app.services.torncity.base import TornBaseProcessor
from app.services.torncity.client import TornClient
from app.services.google.bigquery.client import BigQueryClient

class TestTornBaseProcessor:
    """Test suite for TornBaseProcessor."""

    @pytest.fixture
    def processor(self, sample_config, mock_api_keys):
        """Create a TornBaseProcessor instance for testing."""
        class TestProcessor(TornBaseProcessor):
            def process_data(self, data):
                return data

        return TestProcessor(sample_config)

    def test_init_with_valid_config(self, sample_config, mock_api_keys):
        """Test processor initialization with valid configuration."""
        class TestProcessor(TornBaseProcessor):
            def process_data(self, data):
                return data

        processor = TestProcessor(sample_config)
        assert isinstance(processor.torn_client, TornClient)
        assert isinstance(processor.bq_client, BigQueryClient)
        assert processor.config == sample_config

    def test_init_with_invalid_config(self):
        """Test processor initialization with invalid configuration."""
        class TestProcessor(TornBaseProcessor):
            def process_data(self, data):
                return data

        with pytest.raises(ValueError, match="Missing required configuration"):
            TestProcessor({})

    @patch.object(TornClient, "fetch_data")
    def test_fetch_torn_data(self, mock_fetch, processor):
        """Test fetching data from Torn API."""
        mock_fetch.return_value = {"success": True, "data": {"id": 1}}
        
        data = processor.fetch_torn_data("https://api.torn.com/user/{API_KEY}", "default")
        assert data == {"success": True, "data": {"id": 1}}
        mock_fetch.assert_called_once_with(
            "https://api.torn.com/user/{API_KEY}",
            "default"
        )

    @patch.object(BigQueryClient, "write_data")
    def test_write_to_bigquery(self, mock_write, processor, sample_bigquery_schema):
        """Test writing data to BigQuery."""
        test_data = [{"id": 1, "name": "test"}]
        processor.write_to_bigquery(test_data, "test_table", sample_bigquery_schema)
        
        mock_write.assert_called_once_with(
            test_data,
            "test_table",
            sample_bigquery_schema,
            "append"
        )

    def test_process_data_not_implemented(self, sample_config, mock_api_keys):
        """Test that base class raises NotImplementedError."""
        processor = TornBaseProcessor(sample_config)
        with pytest.raises(NotImplementedError):
            processor.process_data({"data": {}})

    @patch.object(TornClient, "fetch_data")
    @patch.object(BigQueryClient, "write_data")
    def test_run_processor(self, mock_write, mock_fetch, processor, sample_bigquery_schema):
        """Test running the processor end-to-end."""
        # Setup mocks
        mock_fetch.return_value = {"success": True, "data": {"id": 1}}
        
        # Configure processor
        processor.endpoint_url = "https://api.torn.com/user/{API_KEY}"
        processor.api_key = "default"
        processor.table = "test_table"
        processor.schema = sample_bigquery_schema
        
        # Run processor
        processor.run()
        
        # Verify data flow
        mock_fetch.assert_called_once()
        mock_write.assert_called_once()

    def test_validate_config(self, sample_config):
        """Test configuration validation."""
        # Test with valid config
        TornBaseProcessor.validate_config(sample_config)
        
        # Test with missing required fields
        invalid_config = {}
        with pytest.raises(ValueError):
            TornBaseProcessor.validate_config(invalid_config)

    @patch.object(TornClient, "fetch_data")
    def test_error_handling(self, mock_fetch, processor):
        """Test error handling during processing."""
        mock_fetch.side_effect = Exception("Test error")
        
        with pytest.raises(Exception, match="Test error"):
            processor.fetch_torn_data("https://api.torn.com/user/{API_KEY}", "default")

    def test_storage_mode_validation(self, sample_config):
        """Test storage mode validation."""
        # Test with valid storage mode
        sample_config["storage_mode"] = "append"
        TornBaseProcessor.validate_config(sample_config)
        
        # Test with invalid storage mode
        sample_config["storage_mode"] = "invalid_mode"
        with pytest.raises(ValueError, match="Invalid storage mode"):
            TornBaseProcessor.validate_config(sample_config) 