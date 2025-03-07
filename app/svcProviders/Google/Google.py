# initial setup steps:
# BigQuery API Client Libraries: https://cloud.google.com/bigquery/docs/reference/libraries

# From inside the .venv:
# pip install --upgrade pip
# pip install --upgrade google-cloud-bigquery
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# pip install --upgrade pyarrow pandas-gbq


from google.cloud import bigquery
import pandas as pd
from typing import Dict, List
from datetime import datetime

def get_bigquery_schema(df: pd.DataFrame) -> List[bigquery.SchemaField]:
    """Generate BigQuery schema from DataFrame."""
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

def upload_to_bigquery(config: Dict, df: pd.DataFrame, table_id: str) -> None:
    """Upload dataframe to BigQuery, handling table creation and schema updates."""
    client = bigquery.Client.from_service_account_json(config["gcp_credentials_file"])

    # Extract project, dataset, and table name
    project_id, dataset_id, table_name = table_id.split(".")
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_name)

    try:
        # Get existing table
        table = client.get_table(table_ref)
        existing_schema = {field.name: field.field_type for field in table.schema}
        
        # Generate new schema
        new_schema = get_bigquery_schema(df)
        new_schema_dict = {field.name: field.field_type for field in new_schema}
        
        # Identify new columns and type changes
        new_columns = set(new_schema_dict.keys()) - set(existing_schema.keys())
        type_changes = {
            col: new_schema_dict[col] 
            for col in set(new_schema_dict.keys()) & set(existing_schema.keys())
            if new_schema_dict[col] != existing_schema[col]
        }
        
        if new_columns or type_changes:
            print(f"Schema changes detected:")
            if new_columns:
                print(f"New columns: {new_columns}")
            if type_changes:
                print(f"Type changes: {type_changes}")
                
            # Update schema
            table.schema = new_schema
            client.update_table(table, ["schema"])
            print(f"Schema updated for {table_name}")

    except Exception as e:
        print(f"Table {table_name} does not exist. Creating it now...")

        # Create dataset if it does not exist
        try:
            client.get_dataset(dataset_ref)
        except:
            dataset = bigquery.Dataset(dataset_ref)
            client.create_dataset(dataset, exists_ok=True)
            print(f"Dataset {dataset_id} created.")

        # Create table with initial schema
        schema = get_bigquery_schema(df)
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_name} created.")

    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ]
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Data successfully uploaded to {table_name}")
