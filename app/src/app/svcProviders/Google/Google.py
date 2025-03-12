"""Google BigQuery integration module.

This module provides functionality for:
- Schema generation from pandas DataFrames
- Table creation and schema updates in BigQuery
- Data upload to BigQuery with proper type handling
- Schema evolution management
- Deduplication of records based on timestamp

The module handles automatic schema updates when new columns are added
or when data types change, ensuring data consistency in BigQuery.
"""

# initial setup steps:
# BigQuery API Client Libraries: https://cloud.google.com/bigquery/docs/reference/libraries

# From inside the .venv:
# pip install --upgrade pip
# pip install --upgrade google-cloud-bigquery
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# pip install --upgrade pyarrow pandas-gbq


from google.cloud import bigquery
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import logging

def get_bigquery_schema(df: pd.DataFrame) -> List[bigquery.SchemaField]:
    """Generate BigQuery schema from a pandas DataFrame.

    This function analyzes the DataFrame's structure and data types to create
    an appropriate BigQuery schema. It handles various data types including
    timestamps, integers, floats, and strings.

    Args:
        df: The pandas DataFrame to analyze.

    Returns:
        List[bigquery.SchemaField]: List of BigQuery schema field definitions.

    Example:
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        >>> schema = get_bigquery_schema(df)
        >>> print(schema[0].name, schema[0].field_type)
        'id' 'INTEGER'
    """
    schema = []
    
    for col in df.columns:
        dtype = df[col].dtype
        
        # Map pandas dtypes to BigQuery types
        if pd.api.types.is_integer_dtype(dtype):
            field_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "DATETIME"
        else:
            field_type = "STRING"
            
        schema.append(bigquery.SchemaField(col, field_type))
    
    return schema

def upload_to_bigquery(config: Dict[str, Any], df: pd.DataFrame, table_id: str, storage_mode: str = "append") -> None:
    """Upload DataFrame to BigQuery, handling table creation and schema updates.

    This function:
    1. Creates the dataset if it doesn't exist
    2. Creates the table if it doesn't exist
    3. Updates the schema if new columns are added
    4. Uploads data to BigQuery according to storage_mode:
       - append: Adds new records, preventing duplicates based on timestamp
       - replace: Replaces all existing data with new data

    Args:
        config: Configuration dictionary containing GCP credentials.
        df: The pandas DataFrame to upload.
        table_id: Full BigQuery table ID (project.dataset.table).
        storage_mode: How to store the data ("append" or "replace"). Defaults to "append".

    Raises:
        google.api_core.exceptions.GoogleAPIError: If there's an error with BigQuery operations.
        ValueError: If the table_id format is invalid or storage_mode is invalid.
    """
    if storage_mode not in ["append", "replace"]:
        raise ValueError(f"Invalid storage_mode: {storage_mode}. Must be 'append' or 'replace'")

    client = bigquery.Client.from_service_account_json(config["gcp_credentials_file"])

    # Extract project, dataset, and table name
    try:
        project_id, dataset_id, table_name = table_id.split(".")
    except ValueError as e:
        raise ValueError(f"Invalid table_id format. Expected 'project.dataset.table', got '{table_id}'") from e

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_name)
    temp_table_ref = dataset_ref.table(f"{table_name}_temp")

    # Create dataset if it does not exist
    try:
        client.get_dataset(dataset_ref)
        logging.info("Dataset %s exists", dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset, exists_ok=True)
        logging.info("Dataset %s created", dataset_id)

    # Check if table exists and handle schema
    try:
        table = client.get_table(table_ref)
        logging.info("Table %s exists, checking schema...", table_name)
        
        # Get existing schema
        existing_schema = {field.name: field.field_type for field in table.schema}
        logging.info("Existing schema types: %s", existing_schema)
        
        # Log DataFrame types before conversion
        logging.info("DataFrame types before conversion: %s", df.dtypes.to_dict())
        
        # Log raw values for timestamp fields before conversion
        for col, field_type in existing_schema.items():
            if field_type == "TIMESTAMP" and col in df.columns:
                sample_values = df[col].head().tolist()
                logging.info("Raw values for %s before conversion: %s", col, sample_values)
        
        # Ensure DataFrame columns match existing schema types
        for col, field_type in existing_schema.items():
            if col in df.columns:
                if field_type == "TIMESTAMP":
                    try:
                        # Skip conversion if already datetime
                        if not pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                            # Handle None, empty strings, and other invalid values
                            df[col] = df[col].apply(lambda x: None if pd.isna(x) or x == '' or x == 'None' else x)
                            # Convert valid timestamps
                            df[col] = pd.to_datetime(df[col], format='mixed', errors='coerce')
                            
                        logging.info("Column '%s' is ready for upload as TIMESTAMP", col)
                        # Log sample values
                        sample_values = df[col].head().tolist()
                        logging.info("Values for %s: %s", col, sample_values)
                    except Exception as e:
                        logging.error("Failed to handle column %s. Raw values: %s. Error: %s", 
                                    col, df[col].head().tolist(), str(e))
                        raise
                elif field_type == "INTEGER":
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        logging.info("Converted column '%s' to INTEGER", col)
                    except Exception as e:
                        logging.error("Failed to convert column %s to INTEGER: %s", col, str(e))
                        raise
                elif field_type == "FLOAT":
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        logging.info("Converted column '%s' to FLOAT", col)
                    except Exception as e:
                        logging.error("Failed to convert column %s to FLOAT: %s", col, str(e))
                        raise
        
        # Log DataFrame types after conversion
        logging.info("DataFrame types after conversion: %s", df.dtypes.to_dict())
        
        # Generate new schema
        new_schema = get_bigquery_schema(df)
        new_schema_dict = {field.name: field.field_type for field in new_schema}
        logging.info("New schema types: %s", new_schema_dict)
        
        # Check for type mismatches
        type_mismatches = {
            col: (existing_schema[col], new_schema_dict[col])
            for col in set(existing_schema.keys()) & set(new_schema_dict.keys())
            if existing_schema[col] != new_schema_dict[col]
        }
        if type_mismatches:
            logging.info("Schema type mismatches detected. Dropping and recreating table...")
            client.delete_table(table_ref, not_found_ok=True)
            # Create table with new schema
            table = bigquery.Table(table_ref, schema=new_schema)
            client.create_table(table)
            logging.info("Table %s recreated with new schema", table_name)
        else:
            # Identify new columns (ignore type changes)
            new_columns = set(new_schema_dict.keys()) - set(existing_schema.keys())
            
            if new_columns:
                logging.info("New columns detected: %s", new_columns)
                
                # Create combined schema preserving existing types
                combined_schema = []
                for field in table.schema:
                    combined_schema.append(field)
                
                # Add new columns
                for col in new_columns:
                    combined_schema.append(bigquery.SchemaField(col, new_schema_dict[col]))
                
                # Update schema
                table.schema = combined_schema
                client.update_table(table, ["schema"])
                logging.info("Schema updated for %s", table_name)
            
    except Exception as e:
        if "Not found" in str(e):
            logging.info("Table %s does not exist. Creating it now...", table_name)
            # Create table with initial schema
            schema = get_bigquery_schema(df)
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logging.info("Table %s created", table_name)
        else:
            raise e

    # Configure the load job for temporary table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    # Load data into temporary table
    logging.info("Loading data into temporary table...")
    job = client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    if storage_mode == "replace":
        # For replace mode, simply copy temp table to main table
        logging.info("Replacing existing data with new data...")
        copy_job_config = bigquery.job.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        copy_job = client.copy_table(
            temp_table_ref,
            table_ref,
            job_config=copy_job_config
        )
        copy_job.result()  # Wait for the job to complete
        logging.info("Data replacement complete")
    else:  # append mode
        # Check if table has a timestamp column
        timestamp_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col].dtype)]
        if not timestamp_columns:
            logging.warning("No timestamp columns found in DataFrame")
            timestamp_columns = ['server_timestamp']  # Default to server_timestamp if no other timestamp found

        # Construct MERGE query that appends new records based on timestamp
        merge_query = f"""
        MERGE `{table_id}` T
        USING `{project_id}.{dataset_id}.{table_name}_temp` S
        ON {' AND '.join(f'T.{col} = S.{col}' for col in timestamp_columns)}
        WHEN NOT MATCHED THEN
            INSERT ROW
        """

        # Execute MERGE operation
        logging.info("Executing MERGE operation to append new records...")
        query_job = client.query(merge_query)
        query_job.result()  # Wait for the query to complete
        logging.info("Data append complete")
    
    # Clean up temporary table
    logging.info("Cleaning up temporary table...")
    client.delete_table(temp_table_ref, not_found_ok=True)
    logging.info("Temporary table deleted")
    logging.info("Data successfully uploaded to %s using %s mode", table_name, storage_mode)

def drop_tables(config: Dict[str, Any], table_id: str) -> None:
    """Drop a table in BigQuery if it exists.
    
    Args:
        config: Configuration dictionary containing GCP credentials.
        table_id: Full BigQuery table ID (project.dataset.table).
    """
    client = bigquery.Client.from_service_account_json(config["gcp_credentials_file"])
    
    # Extract project, dataset, and table name
    try:
        project_id, dataset_id, table_name = table_id.split(".")
    except ValueError as e:
        raise ValueError(f"Invalid table_id format. Expected 'project.dataset.table', got '{table_id}'") from e
    
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_name)
    
    # Drop the table if it exists
    client.delete_table(table_ref, not_found_ok=True)
    logging.info("Dropped table %s", table_id)
