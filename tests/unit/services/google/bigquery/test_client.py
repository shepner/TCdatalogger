"""Unit tests for BigQuery client."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery
from google.api_core import exceptions

from app.services.google.bigquery.client import BigQueryClient

class TestBigQueryClient:
    """Test suite for BigQueryClient."""

    @pytest.fixture
    def client(self, sample_config):
        """Create a BigQueryClient instance for testing."""
        return BigQueryClient(sample_config)

    def test_init_with_valid_config(self, sample_config):
        """Test client initialization with valid configuration."""
        client = BigQueryClient(sample_config)
        assert client.project_id == sample_config["gcp_project_id"]
        assert client.dataset == sample_config["dataset"]
        assert client.storage_mode == sample_config["storage_mode"]
        assert isinstance(client.client, bigquery.Client)

    def test_init_with_invalid_config(self):
        """Test client initialization with invalid configuration."""
        with pytest.raises(ValueError, match="Missing required configuration"):
            BigQueryClient({})

    @patch("google.cloud.bigquery.Client")
    def test_write_data_append_mode(self, mock_bq_client, client, sample_bigquery_schema):
        """Test writing data in append mode."""
        test_data = [{"id": 1, "name": "test"}]
        mock_job = Mock()
        mock_bq_client.return_value.load_table_from_json.return_value = mock_job
        
        client.write_data(test_data, "test_table", sample_bigquery_schema, "append")
        
        mock_bq_client.return_value.load_table_from_json.assert_called_once()
        mock_job.result.assert_called_once()

    @patch("google.cloud.bigquery.Client")
    def test_write_data_overwrite_mode(self, mock_bq_client, client, sample_bigquery_schema):
        """Test writing data in overwrite mode."""
        test_data = [{"id": 1, "name": "test"}]
        mock_job = Mock()
        mock_bq_client.return_value.load_table_from_json.return_value = mock_job
        
        client.write_data(test_data, "test_table", sample_bigquery_schema, "overwrite")
        
        mock_bq_client.return_value.load_table_from_json.assert_called_once()
        mock_job.result.assert_called_once()

    @patch("google.cloud.bigquery.Client")
    def test_write_data_invalid_mode(self, mock_bq_client, client, sample_bigquery_schema):
        """Test writing data with invalid storage mode."""
        test_data = [{"id": 1, "name": "test"}]
        
        with pytest.raises(ValueError, match="Invalid storage mode"):
            client.write_data(test_data, "test_table", sample_bigquery_schema, "invalid")

    @patch("google.cloud.bigquery.Client")
    def test_write_data_empty_data(self, mock_bq_client, client, sample_bigquery_schema):
        """Test writing empty data."""
        with pytest.raises(ValueError, match="No data to write"):
            client.write_data([], "test_table", sample_bigquery_schema, "append")

    @patch("google.cloud.bigquery.Client")
    def test_write_data_api_error(self, mock_bq_client, client, sample_bigquery_schema):
        """Test handling of BigQuery API errors."""
        test_data = [{"id": 1, "name": "test"}]
        mock_job = Mock()
        mock_job.result.side_effect = exceptions.BadRequest("Invalid data")
        mock_bq_client.return_value.load_table_from_json.return_value = mock_job
        
        with pytest.raises(exceptions.BadRequest, match="Invalid data"):
            client.write_data(test_data, "test_table", sample_bigquery_schema, "append")

    @patch("google.cloud.bigquery.Client")
    def test_table_exists(self, mock_bq_client, client):
        """Test checking if table exists."""
        # Test when table exists
        mock_bq_client.return_value.get_table.return_value = Mock()
        assert client.table_exists("test_table") is True
        
        # Test when table doesn't exist
        mock_bq_client.return_value.get_table.side_effect = exceptions.NotFound("Table not found")
        assert client.table_exists("test_table") is False

    @patch("google.cloud.bigquery.Client")
    def test_delete_table(self, mock_bq_client, client):
        """Test deleting a table."""
        # Test successful deletion
        client.delete_table("test_table")
        mock_bq_client.return_value.delete_table.assert_called_once()
        
        # Test deletion of non-existent table
        mock_bq_client.return_value.delete_table.side_effect = exceptions.NotFound("Table not found")
        with pytest.raises(exceptions.NotFound):
            client.delete_table("nonexistent_table")

    @patch("google.cloud.bigquery.Client")
    def test_create_table(self, mock_bq_client, client, sample_bigquery_schema):
        """Test creating a table."""
        client.create_table("test_table", sample_bigquery_schema)
        mock_bq_client.return_value.create_table.assert_called_once()

    @patch("google.cloud.bigquery.Client")
    def test_get_table_schema(self, mock_bq_client, client):
        """Test getting table schema."""
        mock_table = Mock()
        mock_table.schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING")
        ]
        mock_bq_client.return_value.get_table.return_value = mock_table
        
        schema = client.get_table_schema("test_table")
        assert len(schema) == 2
        assert schema[0].name == "id"
        assert schema[1].name == "name"

    @patch("google.cloud.bigquery.Client")
    def test_validate_schema_compatibility(self, mock_bq_client, client, sample_bigquery_schema):
        """Test schema compatibility validation."""
        # Test with compatible schema
        mock_table = Mock()
        mock_table.schema = sample_bigquery_schema
        mock_bq_client.return_value.get_table.return_value = mock_table
        
        client.validate_schema_compatibility("test_table", sample_bigquery_schema)
        
        # Test with incompatible schema
        incompatible_schema = [
            bigquery.SchemaField("different", "STRING")
        ]
        with pytest.raises(ValueError, match="Schema mismatch"):
            client.validate_schema_compatibility("test_table", incompatible_schema) 