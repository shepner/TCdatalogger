"""Unit tests for Torn City base processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery
import pandas as pd
from typing import Dict, List, Any

from app.services.torncity.base import BaseEndpointProcessor
from app.services.torncity.client import TornClient
from app.services.google.bigquery.client import BigQueryClient

class TestProcessor(BaseEndpointProcessor):
    """Test implementation of BaseEndpointProcessor."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize test processor."""
        super().__init__(config)
        self.endpoint_config.update({
            'name': 'test',
            'endpoint': 'user',
            'selection': 'default',
            'table': 'test_table'
        })

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get test schema."""
        return [
            bigquery.SchemaField('id', 'INTEGER', 'REQUIRED'),
            bigquery.SchemaField('name', 'STRING', 'NULLABLE'),
            bigquery.SchemaField('timestamp', 'DATETIME', 'REQUIRED'),
            bigquery.SchemaField('value', 'FLOAT', 'NULLABLE')
        ]

    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform test data."""
        return pd.DataFrame([{
            'id': 1,
            'name': 'test',
            'timestamp': '2025-03-17T10:00:00',
            'value': 1.0
        }])

    def process_data(self, data: dict) -> pd.DataFrame:
        """Process the test data.
        
        Args:
            data: Raw data from the API response
            
        Returns:
            pd.DataFrame: Processed data records
        """
        transformed_data = self.transform_data(data)
        self.validate_schema(transformed_data)
        return transformed_data

class TestBaseProcessor:
    """Test cases for BaseEndpointProcessor."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a test processor instance."""
        return TestProcessor(sample_config)

    def test_init_with_valid_config(self, sample_config, mock_api_keys, mock_bigquery_client):
        """Test processor initialization with valid configuration."""
        processor = TestProcessor(sample_config)
        
        # Verify TornClient initialization
        assert isinstance(processor.torn_client, TornClient)
        assert processor.torn_client.api_keys.get('default') == "abcd1234efgh5678"  # From mock_api_keys
        
        # Verify BigQueryClient initialization
        assert isinstance(processor.bq_client, BigQueryClient)
        assert processor.bq_client.project_id == "test-project"
        assert processor.bq_client.dataset == "test_dataset"
        
        # Verify config
        assert processor.config == sample_config

    def test_init_with_invalid_config(self):
        """Test processor initialization with invalid configuration."""
        with pytest.raises(ValueError, match="Missing required configuration"):
            TestProcessor({})

    def test_init_with_invalid_storage_mode(self, sample_config):
        """Test processor initialization with invalid storage mode."""
        sample_config["storage_mode"] = "invalid"
        with pytest.raises(ValueError, match="Invalid storage mode"):
            TestProcessor(sample_config)

    @patch.object(TornClient, "make_request")
    def test_fetch_torn_data(self, mock_request, processor):
        """Test fetching data from Torn API."""
        expected_response = {"success": True, "data": {"id": 1}}
        mock_request.return_value = expected_response
        
        data = processor.fetch_torn_data()
        assert data == expected_response
        mock_request.assert_called_once_with(
            processor.endpoint_config['endpoint'],
            processor.endpoint_config['selection']
        )

    @patch.object(BigQueryClient, "write_data")
    def test_write_to_bigquery(self, mock_write, processor, mock_bigquery_client):
        """Test writing data to BigQuery."""
        test_data = pd.DataFrame([{
            'id': 1,
            'timestamp': '2025-03-17T10:00:00',
            'name': 'test',
            'value': 1.0
        }])
        
        # Write data to BigQuery
        processor.write_to_bigquery(test_data)
        
        # Verify write_data was called with correct arguments
        mock_write.assert_called_once_with(
            processor.endpoint_config['table'],
            test_data.to_dict('records'),
            write_disposition='APPEND'
        )
        
        # Verify BigQuery client methods were called
        assert mock_bigquery_client.load_table_from_dataframe.called

    def test_process_data(self, processor):
        """Test process_data method."""
        test_data = {"id": 1, "name": "test"}
        result = processor.process_data(test_data)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert all(col in result.columns for col in ['id', 'name', 'timestamp', 'value'])

    @patch.object(TornClient, "make_request")
    @patch.object(BigQueryClient, "write_data")
    def test_run_processor(self, mock_write, mock_request, processor, sample_bigquery_schema):
        """Test running the processor end-to-end."""
        # Setup mocks
        mock_request.return_value = {"success": True, "data": {"id": 1}}
        
        # Run processor
        processor.run()
        
        # Verify data flow
        mock_request.assert_called_once_with(
            processor.endpoint_config['endpoint'],
            processor.endpoint_config['selection']
        )
        mock_write.assert_called_once()

    def test_validate_config(self, sample_config):
        """Test configuration validation."""
        # Test with valid config
        BaseEndpointProcessor.validate_config(sample_config)
        
        # Test with missing required fields
        invalid_config = {}
        with pytest.raises(ValueError):
            BaseEndpointProcessor.validate_config(invalid_config)

    @patch.object(TornClient, "make_request")
    def test_error_handling(self, mock_request, processor):
        """Test error handling during processing."""
        mock_request.side_effect = Exception("Test error")
        
        with pytest.raises(Exception, match="Test error"):
            processor.fetch_torn_data()

    def test_storage_mode_validation(self, sample_config):
        """Test storage mode validation."""
        # Test with valid storage mode
        sample_config["storage_mode"] = "append"
        BaseEndpointProcessor.validate_config(sample_config)
        
        # Test with invalid storage mode
        sample_config["storage_mode"] = "invalid_mode"
        with pytest.raises(ValueError, match="Invalid storage mode"):
            BaseEndpointProcessor.validate_config(sample_config)

    def test_write_to_bigquery(self, mocker, mock_api_keys, tmp_path):
        """Test writing data to BigQuery."""
        # Create a temporary credentials file
        creds_file = tmp_path / "test-creds.json"
        creds_file.write_text('{"type": "service_account"}')
        
        config = {
            'gcp_project_id': 'test-project',
            'gcp_credentials_file': str(creds_file),
            'dataset': 'test_dataset',
            'storage_mode': 'append',
            'tc_api_key_file': mock_api_keys,
            'endpoint': 'user',
            'selection': 'default'
        }
        
        processor = TestProcessor(config)
        mock_write = mocker.patch.object(processor.bq_client, 'write_data')
        
        test_data = [{
            'id': 1,
            'name': 'test',
            'timestamp': '2025-03-17T10:00:00',
            'value': 1.0
        }]
        
        processor.write_to_bigquery(test_data, 'test_table')
        
        # Get the actual DataFrame that was passed to write_data
        actual_df = mock_write.call_args[0][0]
        
        # Compare DataFrame contents instead of the DataFrame objects directly
        expected_df = pd.DataFrame(test_data)
        pd.testing.assert_frame_equal(actual_df, expected_df)
        
        # Verify the other arguments
        assert mock_write.call_args[0][1] == 'test_table'
        assert mock_write.call_args[1]['write_disposition'] == 'WRITE_APPEND' 