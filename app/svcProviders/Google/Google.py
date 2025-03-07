"""Google BigQuery integration module.

This module provides functionality for:
- Schema generation from pandas DataFrames
- Table creation and schema updates in BigQuery
- Data upload to BigQuery with proper type handling
- Schema evolution management

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
        'id' 'INT64'
    """
    schema = []
    
    for col in df.columns:
        dtype = df[col].dtype
        
        # Map pandas dtypes to BigQuery types
        if pd.api.types.is_integer_dtype(dtype):
            field_type = "INT64"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT64"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "TIMESTAMP"
        else:
            field_type = "STRING"
            
        schema.append(bigquery.SchemaField(col, field_type))
    
    return schema

def upload_to_bigquery(config: Dict[str, Any], df: pd.DataFrame, table_id: str) -> None:
    """Upload DataFrame to BigQuery, handling table creation and schema updates.

    This function:
    1. Creates the dataset if it doesn't exist
    2. Creates the table if it doesn't exist
    3. Updates the schema if new columns are added or types change
    4. Uploads the data to BigQuery

    Args:
        config: Configuration dictionary containing GCP credentials.
        df: The pandas DataFrame to upload.
        table_id: Full BigQuery table ID (project.dataset.table).

    Raises:
        google.api_core.exceptions.GoogleAPIError: If there's an error with BigQuery operations.
        ValueError: If the table_id format is invalid.

    Example:
        >>> config = {"gcp_credentials_file": "path/to/credentials.json"}
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        >>> upload_to_bigquery(config, df, "project.dataset.table")
    """
    client = bigquery.Client.from_service_account_json(config["gcp_credentials_file"])

    # Extract project, dataset, and table name
    try:
        project_id, dataset_id, table_name = table_id.split(".")
    except ValueError as e:
        raise ValueError(f"Invalid table_id format. Expected 'project.dataset.table', got '{table_id}'") from e

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_name)

    try:
        # Get existing table
        table = client.get_table(table_ref)
        existing_schema = {field.name: field.field_type for field in table.schema}
        
        # Generate new schema
        new_schema = get_bigquery_schema(df)
        new_schema_dict = {field.name: field.field_type for field in new_schema}
        
        # Identify schema changes
        new_columns = set(new_schema_dict.keys()) - set(existing_schema.keys())
        type_changes = {
            col: new_schema_dict[col] 
            for col in set(new_schema_dict.keys()) & set(existing_schema.keys())
            if new_schema_dict[col] != existing_schema[col]
        }
        
        if new_columns or type_changes:
            logging.info("Schema changes detected:")
            if new_columns:
                logging.info("New columns: %s", new_columns)
            if type_changes:
                logging.info("Type changes: %s", type_changes)
                
            # Update schema
            table.schema = new_schema
            client.update_table(table, ["schema"])
            logging.info("Schema updated for %s", table_name)

    except Exception as e:
        logging.info("Table %s does not exist. Creating it now...", table_name)

        # Create dataset if it does not exist
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            client.create_dataset(dataset, exists_ok=True)
            logging.info("Dataset %s created", dataset_id)

        # Create table with initial schema
        schema = get_bigquery_schema(df)
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info("Table %s created", table_name)

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ]
    )
    
    # Load the data
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    logging.info("Data successfully uploaded to %s", table_name)
