import pytest
from unittest.mock import Mock, patch
from google.cloud import bigquery
from google.api_core import exceptions
from app.services.google.bigquery.client import BigQueryClient, BigQueryError
from datetime import datetime

@pytest.fixture
def mock_bigquery_client():
    with patch('google.cloud.bigquery.Client') as mock_client:
        yield mock_client

@pytest.fixture
def bq_client(mock_bigquery_client):
    return BigQueryClient(project_id='test-project')

def test_init_failure(mocker):
    """Test client initialization failure."""
    mock_client = mocker.patch('google.cloud.bigquery.Client')
    mock_client.side_effect = Exception("Auth failed")
    
    with pytest.raises(BigQueryError, match="Failed to authenticate with BigQuery: Auth failed"):
        BigQueryClient(project_id='test-project')

def test_table_exists_success(bq_client, mock_bigquery_client):
    """Test successful table existence check."""
    mock_bigquery_client.return_value.get_table.return_value = Mock()
    assert bq_client.table_exists('test_table') is True

def test_table_exists_not_found(bq_client, mock_bigquery_client):
    """Test table not found case."""
    mock_bigquery_client.return_value.get_table.side_effect = exceptions.NotFound('not found')
    assert bq_client.table_exists('test_table') is False

def test_table_exists_auth_error(bq_client, mock_bigquery_client):
    """Test authentication error handling."""
    mock_bigquery_client.return_value.get_table.side_effect = exceptions.Forbidden('forbidden')
    with pytest.raises(ValueError, match='Authentication error'):
        bq_client.table_exists('test_table')

def test_write_data_success(bq_client, mock_bigquery_client):
    """Test successful data write."""
    mock_job = Mock()
    mock_job.result.return_value = None
    mock_bigquery_client.return_value.load_table_from_dataframe.return_value = mock_job
    
    data = [{'col1': 'val1'}]
    bq_client.write_data(data, 'test_table', write_disposition='WRITE_APPEND')
    
    mock_bigquery_client.return_value.load_table_from_dataframe.assert_called_once()

def test_write_data_invalid_disposition(bq_client):
    """Test invalid write disposition."""
    with pytest.raises(ValueError, match='Invalid write disposition'):
        bq_client.write_data([], 'test_table', write_disposition='INVALID')

def test_write_data_empty(bq_client, mock_bigquery_client):
    """Test write with empty data."""
    with pytest.raises(ValueError, match='No data to write'):
        bq_client.write_data([], 'test_table')

def test_create_table(bq_client, mock_bigquery_client):
    """Test table creation."""
    schema = [
        {'name': 'col1', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'col2', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
    bq_client.create_table('test_table', schema)
    mock_bigquery_client.return_value.create_table.assert_called_once()

def test_validate_schema_compatibility_success(bq_client, mock_bigquery_client):
    """Test successful schema compatibility validation."""
    existing_schema = [
        bigquery.SchemaField('col1', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('col2', 'INTEGER', mode='NULLABLE')
    ]
    new_schema = [
        {'name': 'col1', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'col2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'col3', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
    
    mock_bigquery_client.return_value.get_table.return_value.schema = existing_schema
    mock_bigquery_client.return_value.get_table.return_value.num_rows = 0
    
    # Should not raise any exceptions
    bq_client.validate_schema_compatibility('test_table', new_schema)

def test_validate_schema_compatibility_type_mismatch(bq_client, mock_bigquery_client):
    """Test schema compatibility with type mismatch."""
    existing_schema = [
        bigquery.SchemaField('col1', 'STRING', mode='REQUIRED')
    ]
    new_schema = [
        {'name': 'col1', 'type': 'INTEGER', 'mode': 'REQUIRED'}
    ]
    
    mock_bigquery_client.return_value.get_table.return_value.schema = existing_schema
    
    with pytest.raises(ValueError, match='type changed from STRING to INTEGER'):
        bq_client.validate_schema_compatibility('test_table', new_schema)

def test_validate_schema_compatibility_required_field_missing(bq_client, mock_bigquery_client):
    """Test schema compatibility with missing required field."""
    existing_schema = [
        bigquery.SchemaField('col1', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('col2', 'INTEGER', mode='REQUIRED')
    ]
    new_schema = [
        {'name': 'col1', 'type': 'STRING', 'mode': 'REQUIRED'}
    ]
    
    mock_bigquery_client.return_value.get_table.return_value.schema = existing_schema
    
    with pytest.raises(ValueError, match='Required field .* is missing'):
        bq_client.validate_schema_compatibility('test_table', new_schema)

def test_validate_data_types_success(bq_client):
    """Test successful data type validation."""
    schema = [
        bigquery.SchemaField('str_col', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('int_col', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('float_col', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('bool_col', 'BOOLEAN', mode='REQUIRED'),
        bigquery.SchemaField('datetime_col', 'DATETIME', mode='NULLABLE')
    ]
    
    data = [{
        'str_col': 'test',
        'int_col': 123,
        'float_col': 123.45,
        'bool_col': True,
        'datetime_col': '2024-03-15T12:00:00'
    }]
    
    # Should not raise any exceptions
    bq_client.validate_data_types(data, schema)

def test_validate_data_types_invalid_type(bq_client):
    """Test data type validation with invalid type."""
    schema = [
        bigquery.SchemaField('int_col', 'INTEGER', mode='REQUIRED')
    ]
    
    data = [{
        'int_col': 'not an integer'
    }]
    
    with pytest.raises(ValueError, match='validation failed'):
        bq_client.validate_data_types(data, schema)

def test_validate_data_types_required_null(bq_client):
    """Test data type validation with null in required field."""
    schema = [
        bigquery.SchemaField('str_col', 'STRING', mode='REQUIRED')
    ]
    
    data = [{
        'str_col': None
    }]
    
    with pytest.raises(ValueError, match='Required field .* cannot be null'):
        bq_client.validate_data_types(data, schema)

def test_delete_table_success(bq_client, mock_bigquery_client):
    """Test successful table deletion."""
    bq_client.delete_table('test_table')
    mock_bigquery_client.return_value.delete_table.assert_called_once()

def test_delete_table_failure(bq_client, mock_bigquery_client):
    """Test table deletion failure."""
    mock_bigquery_client.return_value.delete_table.side_effect = Exception('Delete failed')
    with pytest.raises(ValueError, match='Failed to delete table'):
        bq_client.delete_table('test_table')

def test_get_table_schema_success(bq_client, mock_bigquery_client):
    """Test successful schema retrieval."""
    expected_schema = [
        bigquery.SchemaField('col1', 'STRING', mode='REQUIRED')
    ]
    mock_bigquery_client.return_value.get_table.return_value.schema = expected_schema
    
    schema = bq_client.get_table_schema('test_table')
    assert schema == expected_schema

def test_get_table_schema_not_found(bq_client, mock_bigquery_client):
    """Test schema retrieval for non-existent table."""
    mock_bigquery_client.return_value.get_table.side_effect = exceptions.NotFound('not found')
    with pytest.raises(ValueError, match='Table .* does not exist'):
        bq_client.get_table_schema('test_table')

def test_write_data_with_retry_success(bq_client, mock_bigquery_client):
    """Test successful data write with retry."""
    mock_job = Mock()
    mock_job.result.return_value = None
    mock_bigquery_client.return_value.load_table_from_dataframe.return_value = mock_job
    
    data = [{'col1': 'val1'}]
    bq_client.write_data_with_retry('test_table', data)
    
    assert mock_bigquery_client.return_value.load_table_from_dataframe.call_count == 1

def test_write_data_with_retry_failure(bq_client, mock_bigquery_client):
    """Test data write with retry exhaustion."""
    mock_bigquery_client.return_value.load_table_from_dataframe.side_effect = Exception('Write failed')
    
    data = [{'col1': 'val1'}]
    with pytest.raises(Exception, match='Write failed'):
        bq_client.write_data_with_retry('test_table', data, max_retries=2)
    
    assert mock_bigquery_client.return_value.load_table_from_dataframe.call_count == 2

def test_write_data_in_batches(bq_client, mock_bigquery_client):
    """Test batch data writing."""
    mock_job = Mock()
    mock_job.result.return_value = None
    mock_bigquery_client.return_value.load_table_from_dataframe.return_value = mock_job
    
    data = [{'col1': f'val{i}'} for i in range(2500)]  # Create 2500 records
    bq_client.write_data_in_batches('test_table', data, batch_size=1000)
    
    # Should have made 3 calls (2 full batches + 1 partial)
    assert mock_bigquery_client.return_value.load_table_from_dataframe.call_count == 3 