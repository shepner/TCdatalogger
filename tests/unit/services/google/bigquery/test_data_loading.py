"""Unit tests for BigQuery data loading and writing."""

from unittest.mock import Mock, patch, call
import json
from datetime import datetime, date, time

import pytest
from google.cloud import bigquery
from google.api_core import exceptions

from app.services.google.bigquery.client import BigQueryClient

class TestBigQueryDataLoading:
    """Test suite for BigQuery data loading operations."""

    @pytest.fixture
    def client(self, sample_config):
        """Create a BigQueryClient instance for testing."""
        return BigQueryClient(sample_config)

    @pytest.fixture
    def sample_data(self):
        """Provide sample data for testing."""
        return [
            {
                "id": 1,
                "timestamp": "2024-03-08T12:00:00",
                "name": "Test Item 1",
                "value": 42.5
            },
            {
                "id": 2,
                "timestamp": "2024-03-08T12:01:00",
                "name": "Test Item 2",
                "value": 37.8
            }
        ]

    def test_load_data_from_json(self, client, sample_data, tmp_path):
        """Test loading data from JSON file."""
        # Create temporary JSON file
        json_file = tmp_path / "test_data.json"
        with open(json_file, "w") as f:
            json.dump(sample_data, f)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True
        )

        with patch.object(client.client, 'load_table_from_file') as mock_load:
            client.load_data_from_json("test_table", str(json_file))
            mock_load.assert_called_once()

    def test_write_data_to_table(self, client, sample_data):
        """Test writing data directly to table."""
        with patch.object(client.client, 'insert_rows_json') as mock_insert:
            mock_insert.return_value = []  # Empty list indicates success
            client.write_data_to_table("test_table", sample_data)
            
            mock_insert.assert_called_once_with(
                "test_table",
                sample_data,
                row_ids=[None] * len(sample_data)
            )

    def test_write_data_with_row_ids(self, client, sample_data):
        """Test writing data with custom row IDs."""
        row_ids = ["row1", "row2"]
        
        with patch.object(client.client, 'insert_rows_json') as mock_insert:
            mock_insert.return_value = []
            client.write_data_to_table("test_table", sample_data, row_ids=row_ids)
            
            mock_insert.assert_called_once_with(
                "test_table",
                sample_data,
                row_ids=row_ids
            )

    def test_write_data_error_handling(self, client, sample_data):
        """Test error handling during data writing."""
        errors = [
            {
                'index': 0,
                'errors': [{'message': 'Invalid data type', 'reason': 'invalid'}]
            }
        ]
        
        with patch.object(client.client, 'insert_rows_json') as mock_insert:
            mock_insert.return_value = errors
            
            with pytest.raises(ValueError) as exc:
                client.write_data_to_table("test_table", sample_data)
            assert "Failed to insert rows" in str(exc.value)

    def test_batch_write_data(self, client):
        """Test writing data in batches."""
        # Generate large dataset
        large_dataset = [
            {
                "id": i,
                "value": f"test_{i}"
            }
            for i in range(1000)
        ]
        
        with patch.object(client.client, 'insert_rows_json') as mock_insert:
            mock_insert.return_value = []
            client.batch_write_data("test_table", large_dataset, batch_size=100)
            
            # Should have been called 10 times with batches of 100
            assert mock_insert.call_count == 10
            
            # Verify first and last batch
            first_batch = mock_insert.call_args_list[0][0][1]
            assert len(first_batch) == 100
            assert first_batch[0]["id"] == 0
            
            last_batch = mock_insert.call_args_list[-1][0][1]
            assert len(last_batch) == 100
            assert last_batch[-1]["id"] == 999

    def test_write_data_type_conversion(self, client):
        """Test automatic type conversion during data writing."""
        data = [
            {
                "datetime_field": datetime(2024, 3, 8, 12, 0),
                "date_field": date(2024, 3, 8),
                "time_field": time(12, 0),
                "float_field": 42.5,
                "int_field": 42,
                "bool_field": True,
                "none_field": None
            }
        ]
        
        with patch.object(client.client, 'insert_rows_json') as mock_insert:
            mock_insert.return_value = []
            client.write_data_to_table("test_table", data)
            
            # Verify data was converted to expected format
            written_data = mock_insert.call_args[0][1]
            assert written_data[0]["datetime_field"] == "2024-03-08T12:00:00"
            assert written_data[0]["date_field"] == "2024-03-08"
            assert written_data[0]["time_field"] == "12:00:00"
            assert isinstance(written_data[0]["float_field"], float)
            assert isinstance(written_data[0]["int_field"], int)
            assert isinstance(written_data[0]["bool_field"], bool)
            assert written_data[0]["none_field"] is None

    def test_write_data_with_template(self, client):
        """Test writing data using a template for missing fields."""
        template = {
            "id": None,
            "timestamp": None,
            "name": "",
            "value": 0.0,
            "status": "unknown"
        }
        
        data = [
            {"id": 1, "name": "Test 1"},  # Missing timestamp, value, status
            {"id": 2, "timestamp": "2024-03-08T12:00:00"}  # Missing name, value, status
        ]
        
        with patch.object(client.client, 'insert_rows_json') as mock_insert:
            mock_insert.return_value = []
            client.write_data_with_template("test_table", data, template)
            
            written_data = mock_insert.call_args[0][1]
            assert written_data[0]["value"] == 0.0
            assert written_data[0]["status"] == "unknown"
            assert written_data[1]["name"] == ""
            assert written_data[1]["value"] == 0.0

    def test_write_data_validation(self, client):
        """Test data validation before writing."""
        # Test missing required fields
        invalid_data = [
            {"optional_field": "value"}  # Missing required fields
        ]
        
        schema = [
            bigquery.SchemaField("required_field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("optional_field", "STRING", mode="NULLABLE")
        ]
        
        with pytest.raises(ValueError) as exc:
            client.validate_data_against_schema(invalid_data, schema)
        assert "Missing required field" in str(exc.value)
        
        # Test invalid data types
        invalid_types = [
            {
                "required_field": 123,  # Should be string
                "optional_field": "value"
            }
        ]
        
        with pytest.raises(ValueError) as exc:
            client.validate_data_against_schema(invalid_types, schema)
        assert "Invalid data type" in str(exc.value)

    def test_write_data_with_retry(self, client, sample_data):
        """Test writing data with retry logic."""
        with patch.object(client.client, 'insert_rows_json') as mock_insert:
            # Simulate failures then success
            mock_insert.side_effect = [
                exceptions.InternalServerError("Server Error"),
                exceptions.InternalServerError("Server Error"),
                []  # Success on third try
            ]
            
            client.write_data_with_retry("test_table", sample_data, max_retries=3)
            assert mock_insert.call_count == 3

            # Should fail if max retries exceeded
            mock_insert.reset_mock()
            mock_insert.side_effect = exceptions.InternalServerError("Server Error")
            
            with pytest.raises(exceptions.InternalServerError):
                client.write_data_with_retry("test_table", sample_data, max_retries=2) 