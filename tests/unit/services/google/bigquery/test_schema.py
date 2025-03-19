"""Unit tests for BigQuery schema management."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery
from google.api_core import exceptions

from app.services.google.bigquery.client import BigQueryClient

class TestBigQuerySchemaManagement:
    """Test suite for BigQuery schema management."""

    @pytest.fixture
    def client(self, sample_config):
        """Create a BigQueryClient instance for testing."""
        return BigQueryClient(sample_config)

    @pytest.fixture
    def sample_schema(self):
        """Provide a sample schema for testing."""
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "FLOAT", mode="NULLABLE")
        ]

    def test_create_table_with_schema(self, client, sample_schema):
        """Test creating a table with schema."""
        with patch.object(client.client, 'create_table') as mock_create:
            client.create_table("test_table", sample_schema)
            
            mock_create.assert_called_once()
            table_arg = mock_create.call_args[0][0]
            assert table_arg.schema == sample_schema

    def test_update_table_schema(self, client, sample_schema):
        """Test updating table schema."""
        updated_schema = sample_schema + [
            bigquery.SchemaField("new_field", "STRING", mode="NULLABLE")
        ]
        
        mock_table = Mock()
        mock_table.schema = sample_schema
        mock_table.num_rows = 0  # Mock empty table
        
        with patch.object(client.client, 'get_table', return_value=mock_table), \
             patch.object(client.client, 'update_table') as mock_update:
            client.update_table_schema("test_table", updated_schema)
            
            mock_update.assert_called_once()
            table_arg = mock_update.call_args[0][0]
            assert table_arg.schema == updated_schema

    def test_get_table_schema(self, client, sample_schema):
        """Test retrieving table schema."""
        mock_table = Mock()
        mock_table.schema = sample_schema
        mock_table.num_rows = 0
        
        with patch.object(client.client, 'get_table') as mock_get:
            mock_get.return_value = mock_table
            
            schema = client.get_table_schema("test_table")
            assert schema == sample_schema

    def test_validate_schema_compatibility(self, client, sample_schema):
        """Test schema compatibility validation."""
        # Test compatible schema (same fields, different order)
        compatible_schema = [
            sample_schema[1],  # timestamp
            sample_schema[0],  # id
            sample_schema[2],  # name
            sample_schema[3]   # value
        ]
        
        mock_table = Mock()
        mock_table.schema = sample_schema
        mock_table.num_rows = 100  # Mock existing data
        
        with patch.object(client.client, 'get_table', return_value=mock_table):
            # Should not raise exception
            client.validate_schema_compatibility("test_table", compatible_schema)
        
        # Test incompatible schema (missing required field)
        incompatible_schema = [
            bigquery.SchemaField("timestamp", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE")
        ]
        
        with patch.object(client.client, 'get_table', return_value=mock_table):
            with pytest.raises(ValueError) as exc:
                client.validate_schema_compatibility("test_table", incompatible_schema)
            assert "Schema mismatch" in str(exc.value)

    def test_schema_evolution(self, client, sample_schema):
        """Test schema evolution scenarios."""
        mock_table = Mock()
        mock_table.schema = sample_schema
        mock_table.num_rows = 100  # Mock existing data
        
        with patch.object(client.client, 'get_table', return_value=mock_table):
            # Test adding nullable field (should be compatible)
            evolved_schema = sample_schema + [
                bigquery.SchemaField("new_field", "STRING", mode="NULLABLE")
            ]
            client.validate_schema_compatibility("test_table", evolved_schema)
            
            # Test changing field mode (should be incompatible)
            incompatible_schema = [
                bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),  # Changed from REQUIRED
                bigquery.SchemaField("timestamp", "DATETIME", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value", "FLOAT", mode="NULLABLE")
            ]
            
            with pytest.raises(ValueError) as exc:
                client.validate_schema_compatibility("test_table", incompatible_schema)
            assert "Schema mismatch" in str(exc.value)

    def test_schema_type_validation(self, client):
        """Test validation of schema field types."""
        valid_types = [
            ("INTEGER", 42),
            ("FLOAT", 3.14),
            ("STRING", "test"),
            ("BOOLEAN", True),
            ("DATETIME", "2024-03-08T12:00:00"),
            ("DATE", "2024-03-08"),
            ("TIME", "12:00:00"),
            ("TIMESTAMP", "2024-03-08T12:00:00Z")
        ]
        
        for field_type, test_value in valid_types:
            schema = [bigquery.SchemaField("test_field", field_type)]
            data = [{"test_field": test_value}]
            
            # Should not raise exception
            client.validate_data_types(data, schema)
        
        # Test invalid type combinations
        invalid_types = [
            ("INTEGER", "not_an_integer"),
            ("FLOAT", "not_a_float"),
            ("BOOLEAN", "not_a_boolean"),
            ("DATETIME", "invalid_datetime"),
            ("DATE", "invalid_date"),
            ("TIME", "invalid_time"),
            ("TIMESTAMP", "invalid_timestamp")
        ]
        
        for field_type, test_value in invalid_types:
            schema = [bigquery.SchemaField("test_field", field_type)]
            data = [{"test_field": test_value}]
            
            with pytest.raises(ValueError):
                client.validate_data_types(data, schema)

    def test_schema_mode_validation(self, client):
        """Test validation of schema field modes."""
        schema = [
            bigquery.SchemaField("required_field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("nullable_field", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("repeated_field", "STRING", mode="REPEATED")
        ]
        
        # Test valid data
        valid_data = [
            {
                "required_field": "value",
                "nullable_field": None,
                "repeated_field": ["value1", "value2"]
            }
        ]
        client.validate_field_modes(valid_data, schema)
        
        # Test missing required field
        invalid_data = [
            {
                "nullable_field": "value",
                "repeated_field": ["value"]
            }
        ]
        with pytest.raises(ValueError):
            client.validate_field_modes(invalid_data, schema)
        
        # Test invalid repeated field
        invalid_repeated = [
            {
                "required_field": "value",
                "nullable_field": None,
                "repeated_field": "not_a_list"  # Should be a list
            }
        ]
        with pytest.raises(ValueError):
            client.validate_field_modes(invalid_repeated, schema)

    def test_schema_field_name_validation(self, client):
        """Test validation of schema field names."""
        valid_names = [
            "field_name",
            "field123",
            "FIELD_NAME",
            "_field_name"
        ]
        
        for name in valid_names:
            schema = [bigquery.SchemaField(name, "STRING")]
            # Should not raise exception
            client.validate_field_names(schema)
        
        invalid_names = [
            "123field",  # Cannot start with number
            "field-name",  # Cannot contain hyphen
            "field name",  # Cannot contain space
            "field@name",  # Cannot contain special characters
            ""  # Cannot be empty
        ]
        
        for name in invalid_names:
            schema = [bigquery.SchemaField(name, "STRING")]
            with pytest.raises(ValueError):
                client.validate_field_names(schema)

    def test_schema_description_validation(self, client):
        """Test validation of schema field descriptions."""
        # Test valid descriptions
        valid_schema = [
            bigquery.SchemaField("field1", "STRING", description="A valid description"),
            bigquery.SchemaField("field2", "INTEGER", description=None),  # Optional
            bigquery.SchemaField("field3", "FLOAT", description="")  # Empty is ok
        ]
        client.validate_field_descriptions(valid_schema)
        
        # Test invalid description (non-string)
        invalid_schema = [
            bigquery.SchemaField("field1", "STRING", description=123)  # Must be string
        ]
        with pytest.raises(ValueError):
            client.validate_field_descriptions(invalid_schema) 