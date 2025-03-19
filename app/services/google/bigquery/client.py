"""BigQuery client for data storage operations.

This module provides functionality for interacting with Google BigQuery:
- Table management
- Data upload
- Schema management
- Query execution
"""

from typing import Dict, List, Optional, Union, Any
import re
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.api_core import retry
import time
import logging
from google.api_core import exceptions
from google.oauth2 import service_account

from app.services.google.base import BaseGoogleClient
from app.services.common.types import SchemaType, DataType, ConfigType, validate_schema

# Remove the import from torncity.exceptions
class BigQueryError(Exception):
    """Base exception for BigQuery operations."""
    pass

class BigQueryClient(BaseGoogleClient):
    """Client for Google BigQuery operations.
    
    This class provides methods for:
    - Managing BigQuery tables
    - Uploading data
    - Managing schemas
    - Executing queries
    """
    
    def __init__(self, project_id: Union[str, Dict[str, Any]], credentials: Optional[str] = None):
        """Initialize the BigQuery client.
        
        Args:
            project_id: The GCP project ID or a config dictionary containing 'gcp_project_id'.
            credentials: Path to the service account credentials file.
        
        Raises:
            BigQueryError: If authentication fails.
        """
        try:
            # Store the config if project_id is a dictionary
            self.config = project_id if isinstance(project_id, dict) else None
            
            # Handle project_id being a config dictionary
            if isinstance(project_id, dict):
                self.project_id = project_id.get('gcp_project_id')
                credentials = credentials or project_id.get('gcp_credentials_file')
                self.dataset = project_id.get('dataset')
                self.storage_mode = project_id.get('storage_mode', 'append')
            else:
                self.project_id = project_id
                self.dataset = None
                self.storage_mode = 'append'

            if not self.project_id:
                raise BigQueryError("Project ID is required")

            if credentials:
                self.client = bigquery.Client(
                    project=self.project_id,
                    credentials=service_account.Credentials.from_service_account_file(credentials)
                )
            else:
                self.client = bigquery.Client(project=self.project_id)
        except Exception as e:
            raise BigQueryError(f"Failed to authenticate with BigQuery: {str(e)}")
        
    def _get_full_table_id(self, table_id: str) -> str:
        """Get the fully qualified table ID.
        
        Args:
            table_id: Table name or full table ID
            
        Returns:
            str: Fully qualified table ID (project.dataset.table)
        """
        if '.' in table_id:
            return table_id
        return f"{self.project_id}.{self.dataset}.{table_id}"
        
    def get_table(self, table_id: str) -> bigquery.Table:
        """Get a BigQuery table reference.
        
        Args:
            table_id: Table name or full table ID
            
        Returns:
            Table: BigQuery table reference
        """
        full_table_id = self._get_full_table_id(table_id)
        return self.client.get_table(full_table_id)
        
    def create_table(self, table_id: str, schema: List[Dict[str, str]]) -> None:
        """Create a new BigQuery table.
        
        Args:
            table_id: The table to create
            schema: The table schema as a list of dictionaries
            
        Raises:
            ValueError: If schema is invalid
        """
        full_table_id = self._get_full_table_id(table_id)
        schema_fields = self._convert_schema(schema)
        table = bigquery.Table(full_table_id, schema=schema_fields)
        self.client.create_table(table)

    def _convert_schema(self, schema: List[Dict[str, str]]) -> List[bigquery.SchemaField]:
        """Convert schema dict to BigQuery SchemaField objects.
        
        Args:
            schema: List of schema field definitions
            
        Returns:
            List[SchemaField]: Converted schema fields
            
        Raises:
            ValueError: If schema is invalid
        """
        schema_fields = []
        for field in schema:
            if not isinstance(field, dict) and not isinstance(field, bigquery.SchemaField):
                raise ValueError(f"Invalid schema field: {field}")
                
            if isinstance(field, bigquery.SchemaField):
                schema_fields.append(field)
                continue
                
            name = field.get('name')
            field_type = field.get('type', 'STRING').upper()
            mode = field.get('mode', 'NULLABLE').upper()
            description = field.get('description', '')
            
            if not name:
                raise ValueError("Schema field must have a name")
                
            if field_type not in self.VALID_TYPES:
                raise ValueError(f"Invalid field type: {field_type}")
                
            if mode not in ['NULLABLE', 'REQUIRED', 'REPEATED']:
                raise ValueError(f"Invalid field mode: {mode}")
                
            schema_fields.append(
                bigquery.SchemaField(
                    name=name,
                    field_type=field_type,
                    mode=mode,
                    description=description or None
                )
            )
        return schema_fields

    def upload_dataframe(self, df: pd.DataFrame, table_id: str, write_disposition: str = 'WRITE_APPEND') -> None:
        """Upload a pandas DataFrame to BigQuery.
        
        Args:
            df: The DataFrame to upload
            table_id: The table to write to
            write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY)
        """
        full_table_id = self._get_full_table_id(table_id)
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        
        try:
            job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # Wait for the job to complete
        except Exception as e:
            raise ValueError(f"Failed to upload data to {table_id}: {str(e)}")

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a BigQuery SQL query.
        
        Args:
            query: The SQL query to execute
            params: Optional query parameters
            
        Returns:
            List[Dict[str, Any]]: Query results as a list of dictionaries
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(key, 'STRING', value)
                for key, value in (params or {}).items()
            ]
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            return [dict(row.items()) for row in results]
        except Exception as e:
            raise ValueError(f"Query execution failed: {str(e)}")
        
    def table_exists(self, table_id: str) -> bool:
        """Check if a table exists.
        
        Args:
            table_id: The ID of the table to check
        
        Returns:
            bool: True if the table exists, False otherwise
        
        Raises:
            ValueError: If there is an authentication error
        """
        try:
            full_table_id = self._get_full_table_id(table_id)
            self.client.get_table(full_table_id)
            return True
        except exceptions.NotFound:
            return False
        except Exception as e:
            self._handle_auth_error(e, f"checking existence of table {table_id}")
            return False
            
    def get_schema(self, table_id: str) -> List[bigquery.SchemaField]:
        """Get the schema of a table.
        
        Args:
            table_id: Table name or full table ID
            
        Returns:
            List[SchemaField]: Table schema
        """
        full_table_id = self._get_full_table_id(table_id)
        table = self.client.get_table(full_table_id)
        return table.schema

    def write_data(self, data: Union[List[Dict], pd.DataFrame], table_id: str, write_disposition: str = 'WRITE_APPEND', schema: List[bigquery.SchemaField] = None) -> None:
        """Write data to a BigQuery table.
        
        Args:
            data: List of dictionaries or pandas DataFrame containing the data
            table_id: The table to write to
            write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY). Defaults to WRITE_APPEND
            schema: Optional schema to validate against
            
        Raises:
            ValueError: If data is invalid or write disposition is invalid
        """
        valid_dispositions = {'WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'}
        write_disposition = write_disposition.upper()
        if write_disposition not in valid_dispositions:
            raise ValueError("Invalid write disposition. Must be one of: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY")
            
        # Convert to DataFrame if necessary
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame.from_records(data)
        
        if df.empty:
            raise ValueError("No data to write - empty DataFrame")
            
        if schema:
            self.validate_data_types(df.to_dict('records'), schema)
            
        self.upload_dataframe(df, table_id, write_disposition)

    def batch_write_data(self, data: List[Dict], table_id: str, batch_size: int = 1000) -> None:
        """Write data to a BigQuery table in batches.
        
        Args:
            data: List of dictionaries containing the data
            table_id: The table to write to
            batch_size: Number of records to write in each batch
            
        Raises:
            ValueError: If data is invalid
        """
        if not isinstance(data, list) or len(data) == 0:
            return
            
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            self.write_data(batch, table_id)

    def write_data_with_retry(self, data: List[Dict], table_id: str, max_retries: int = 3) -> None:
        """Write data to a BigQuery table with retry on failure.
        
        Args:
            data: List of dictionaries containing the data
            table_id: The table to write to
            max_retries: Maximum number of retries
            
        Raises:
            ValueError: If data is invalid
            Exception: If all retries fail
        """
        if not isinstance(data, list) or len(data) == 0:
            return
            
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.write_data(data, table_id)
                return
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    raise Exception(f"Failed to write data after {max_retries} retries: {str(e)}")
                time.sleep(2 ** retry_count)  # Exponential backoff

    def validate_field_names(self, schema: List[bigquery.SchemaField]) -> None:
        """Validate field names in a schema.
        
        Args:
            schema: List of schema fields to validate
            
        Raises:
            ValueError: If any field names are invalid
        """
        invalid_fields = []
        for field in schema:
            if not field.name.isidentifier():
                invalid_fields.append(field.name)
        
        if invalid_fields:
            raise ValueError(f"Invalid field names: {', '.join(invalid_fields)}")

    def validate_schema_compatibility(self, table_id: str, new_schema: List[Dict[str, str]]) -> None:
        """Validate schema compatibility between existing and new schema.
        
        Args:
            table_id: The table ID
            new_schema: New schema to validate
            
        Raises:
            ValueError: If schemas are incompatible
        """
        if not self.table_exists(table_id):
            return
            
        try:
            existing_schema = self.get_table_schema(table_id)
            new_schema_fields = self._convert_schema(new_schema)
            
            existing_fields = {field.name: field for field in existing_schema}
            new_fields = {field.name: field for field in new_schema_fields}
            
            # Check for missing required fields
            for name, field in existing_fields.items():
                if field.mode == 'REQUIRED' and name not in new_fields:
                    raise ValueError(f"Schema mismatch: Required field '{name}' is missing in new schema")
            
            # Check field compatibility
            for name, new_field in new_fields.items():
                if name in existing_fields:
                    existing_field = existing_fields[name]
                    
                    # Check type compatibility
                    if new_field.field_type != existing_field.field_type:
                        raise ValueError(f"Schema mismatch: Field '{name}' type changed from {existing_field.field_type} to {new_field.field_type}")
                    
                    # Check mode compatibility
                    if existing_field.mode == 'REQUIRED' and new_field.mode != 'REQUIRED':
                        raise ValueError(f"Schema mismatch: Cannot change required field '{name}' to {new_field.mode}")
                else:
                    # New field being added
                    if new_field.mode == 'REQUIRED':
                        table = self.client.get_table(self._get_full_table_id(table_id))
                        if table.num_rows > 0:
                            raise ValueError(f"Schema mismatch: Cannot add required field '{name}' to table with existing data")
        except Exception as e:
            if "Schema mismatch" not in str(e):
                raise ValueError(f"Failed to validate schema compatibility: {str(e)}")
            raise

    VALID_TYPES = {
        'STRING',
        'BYTES',
        'INTEGER',
        'FLOAT',
        'NUMERIC',
        'BOOLEAN',
        'TIMESTAMP',
        'DATE',
        'TIME',
        'DATETIME',
        'GEOGRAPHY',
        'RECORD',
    }

    def validate_data_types(self, data: List[Dict], schema: List[bigquery.SchemaField]) -> None:
        """Validate that data types match schema.
        
        Args:
            data: List of dictionaries containing the data
            schema: List of SchemaField objects defining the schema
            
        Raises:
            ValueError: If data types don't match schema
        """
        if not data:
            return
            
        # Create a mapping of field names to their SchemaField objects
        schema_map = {field.name: field for field in schema}
        
        for i, record in enumerate(data):
            for field_name, value in record.items():
                if field_name not in schema_map:
                    continue  # Skip fields not in schema
                    
                field = schema_map[field_name]
                
                # Skip validation for None values in NULLABLE fields
                if value is None:
                    if field.mode == 'REQUIRED':
                        raise ValueError(f"Record {i}: Required field '{field_name}' cannot be null")
                    continue
                
                try:
                    # Validate based on field type
                    if field.field_type == 'INTEGER':
                        int(value)
                    elif field.field_type == 'FLOAT':
                        float(value)
                    elif field.field_type == 'BOOLEAN':
                        if not isinstance(value, bool):
                            raise ValueError()
                    elif field.field_type == 'STRING':
                        str(value)
                    elif field.field_type == 'DATETIME':
                        if isinstance(value, str):
                            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                    elif field.field_type == 'DATE':
                        if isinstance(value, str):
                            datetime.strptime(value, "%Y-%m-%d")
                    elif field.field_type == 'TIME':
                        if isinstance(value, str):
                            datetime.strptime(value, "%H:%M:%S")
                    elif field.field_type == 'TIMESTAMP':
                        if isinstance(value, str):
                            # Handle 'Z' timezone designator
                            if value.endswith('Z'):
                                value = value[:-1] + '+00:00'
                            datetime.fromisoformat(value)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Record {i}: Field '{field_name}' validation failed: {str(e)}")
                except Exception as e:
                    raise ValueError(f"Record {i}: Field '{field_name}' validation failed: Unexpected error: {str(e)}")

    def validate_field_modes(self, data: List[Dict], schema: List[bigquery.SchemaField]) -> None:
        """Validate that data matches the expected field modes.
        
        Args:
            data: List of data records to validate
            schema: The expected schema
            
        Raises:
            ValueError: If field modes don't match schema
        """
        if not data:
            return
            
        schema_by_name = {field.name: field for field in schema}
        
        for i, record in enumerate(data):
            for name, value in record.items():
                if name not in schema_by_name:
                    continue
                    
                field = schema_by_name[name]
                
                # Check REQUIRED fields
                if field.mode == 'REQUIRED':
                    if value is None:
                        raise ValueError(f"Record {i}: Required field '{name}' cannot be null")
                
                # Check REPEATED fields
                if field.mode == 'REPEATED':
                    if value is not None and not isinstance(value, (list, tuple)):
                        raise ValueError(f"Record {i}: Repeated field '{name}' must be a list or tuple")
                    if value:
                        for j, item in enumerate(value):
                            if item is None:
                                raise ValueError(f"Record {i}: Repeated field '{name}' item {j} cannot be null")
                            
                # Check NULLABLE fields
                if field.mode == 'NULLABLE':
                    # No additional validation needed as null is allowed
                    continue
                    
            # Check for missing required fields
            for name, field in schema_by_name.items():
                if field.mode == 'REQUIRED' and name not in record:
                    raise ValueError(f"Record {i}: Required field '{name}' is missing")

    def validate_field_descriptions(self, schema: List[bigquery.SchemaField]) -> None:
        """Validate field descriptions in a schema.
        
        Args:
            schema: List of schema field definitions
            
        Raises:
            ValueError: If any field descriptions are invalid
        """
        max_length = 1024
        
        for field in schema:
            if field.description is not None:
                if not isinstance(field.description, str):
                    raise ValueError(f"Field description for '{field.name}' must be a string, got {type(field.description).__name__}")
                if len(field.description) > max_length:
                    raise ValueError(f"Field description for '{field.name}' exceeds maximum length of {max_length}")

    def _is_valid_datetime(self, value: Union[str, datetime]) -> bool:
        """Check if a value is a valid datetime.
        
        Args:
            value: Value to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if isinstance(value, datetime):
            return True
            
        if not isinstance(value, str):
            return False
            
        try:
            # Handle ISO format with 'Z' timezone
            if value.endswith('Z'):
                value = value[:-1] + '+00:00'
            datetime.fromisoformat(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_valid_date(self, value: Union[str, datetime]) -> bool:
        """Check if a value is a valid date.
        
        Args:
            value: Value to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if isinstance(value, datetime):
            return True
            
        if not isinstance(value, str):
            return False
            
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return True
        except (ValueError, TypeError):
            return False

    def _is_valid_time(self, value: Union[str, datetime.time]) -> bool:
        """Check if a value is a valid time.
        
        Args:
            value: Value to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if isinstance(value, datetime.time):
            return True
            
        if not isinstance(value, str):
            return False
            
        try:
            datetime.strptime(value, '%H:%M:%S')
            return True
        except (ValueError, TypeError):
            return False

    def _is_valid_timestamp(self, value: Union[str, datetime]) -> bool:
        """Check if a value is a valid timestamp.
        
        Args:
            value: Value to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if isinstance(value, datetime):
            return True
            
        if not isinstance(value, str):
            return False
            
        try:
            # Handle ISO format with 'Z' timezone
            if value.endswith('Z'):
                value = value[:-1] + '+00:00'
            datetime.fromisoformat(value)
            return True
        except (ValueError, TypeError):
            return False

    def update_table_schema(self, table_id: str, schema: List[bigquery.SchemaField]) -> None:
        """Update the schema of an existing table.
        
        Args:
            table_id: Table name or full table ID
            schema: New schema to apply
        """
        full_table_id = self._get_full_table_id(table_id)
        self.validate_schema_compatibility(table_id, schema)
        
        table = self.client.get_table(full_table_id)
        table.schema = schema
        self.client.update_table(table, ['schema'])

    def get_table_schema(self, table_id: str) -> List[bigquery.SchemaField]:
        """Get the schema of a table.
        
        Args:
            table_id: Table name or full table ID
            
        Returns:
            List[SchemaField]: Table schema
            
        Raises:
            ValueError: If the table does not exist
        """
        try:
            full_table_id = self._get_full_table_id(table_id)
            table = self.client.get_table(full_table_id)
            return table.schema
        except exceptions.NotFound:
            raise ValueError(f"Table {table_id} does not exist")
        except Exception as e:
            raise ValueError(f"Failed to get schema for table {table_id}: {str(e)}")

    def write_data_with_retry(self, table_name: str, data: List[Dict[str, Any]], max_retries: int = 3) -> None:
        """Write data to a table with retries on failure.
        
        Args:
            table_name: The table to write to
            data: The data to write
            max_retries: Maximum number of retry attempts
        """
        retries = 0
        while retries < max_retries:
            try:
                self.write_data(data, table_name)
                return
            except Exception as e:
                retries += 1
                if retries == max_retries:
                    raise e
                time.sleep(2 ** retries)  # Exponential backoff

    def write_data_in_batches(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Write data to a table in batches.
        
        Args:
            table_name: The table to write to
            data: The data to write
            batch_size: Number of rows per batch
        """
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            self.write_data(batch, table_name)

    def write_data_to_table(self, table_id: str, data: List[Dict[str, Any]], schema: Optional[List[Dict[str, str]]] = None) -> None:
        """Write data to a table, creating it if it doesn't exist.
        
        Args:
            table_id: The table to write to
            data: The data to write
            schema: Optional schema for table creation
        """
        if not data:
            return

        full_table_id = self._get_full_table_id(table_id)
        
        if not self.table_exists(table_id):
            if not schema:
                raise ValueError(f"Schema required to create table {table_id}")
            self.create_table(table_id, schema)
        
        write_disposition = 'WRITE_TRUNCATE' if self.storage_mode == 'replace' else 'WRITE_APPEND'
        self.write_data(data, table_id, write_disposition)

    def _infer_schema_from_data(self, sample_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Infer BigQuery schema from sample data.
        
        Args:
            sample_data: Sample data dictionary
            
        Returns:
            List[Dict[str, str]]: Inferred schema
        """
        schema = []
        for field_name, value in sample_data.items():
            field_type = 'STRING'
            if isinstance(value, bool):
                field_type = 'BOOLEAN'
            elif isinstance(value, int):
                field_type = 'INTEGER'
            elif isinstance(value, float):
                field_type = 'FLOAT'
            
            schema.append({
                'name': field_name,
                'type': field_type,
                'mode': 'NULLABLE'
            })
        
        return schema

    def delete_table(self, table_id: str) -> None:
        """Delete a BigQuery table.
        
        Args:
            table_id: The table to delete
            
        Raises:
            ValueError: If table does not exist or deletion fails
        """
        try:
            full_table_id = self._get_full_table_id(table_id)
            self.client.delete_table(full_table_id)
        except Exception as e:
            raise ValueError(f"Failed to delete table {table_id}: {str(e)}")

    def _handle_auth_error(self, e: Exception, operation: str) -> None:
        """Handle authentication and permission errors.
        
        Args:
            e: The exception that was raised
            operation: The operation being performed when the error occurred
        
        Raises:
            ValueError: With a descriptive error message
        """
        if isinstance(e, (exceptions.Unauthorized, exceptions.Forbidden)):
            raise ValueError(f"Authentication error during {operation}: {str(e)}")
        raise e
