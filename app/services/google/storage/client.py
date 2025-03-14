"""Google BigQuery integration module.

This module provides core BigQuery connectivity and transport functionality:
- Basic table and dataset operations
- Data upload transport
- Connection management

All data processing, schema management, and business logic should be handled by endpoint processors.
"""

# initial setup steps:
# BigQuery API Client Libraries: https://cloud.google.com/bigquery/docs/reference/libraries

# From inside the .venv:
# pip install --upgrade pip
# pip install --upgrade google-cloud-bigquery
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# pip install --upgrade pyarrow pandas-gbq


from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from typing import Dict, List, Any, Optional
import logging

class BigQueryClient:
    """Generic BigQuery client for basic transport operations."""
    
    def __init__(self, project_id: str, credentials_file: Optional[str] = None):
        """Initialize BigQuery client.
        
        Args:
            project_id: GCP project ID
            credentials_file: Optional path to credentials file
        """
        self.project_id = project_id
        if credentials_file:
            self.client = bigquery.Client.from_service_account_json(credentials_file)
        else:
            self.client = bigquery.Client(project=project_id)

    def ensure_dataset(self, dataset_id: str) -> None:
        """Ensure dataset exists, create if not.
        
        Args:
            dataset_id: Dataset ID to check/create
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            logging.info(f"Dataset {dataset_id} exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            self.client.create_dataset(dataset)
            logging.info(f"Dataset {dataset_id} created")

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_id: str,
        schema: List[bigquery.SchemaField],
        write_mode: str = "append"
    ) -> None:
        """Upload DataFrame to BigQuery table.
        
        Pure transport function - assumes data is already processed and validated.
        
        Args:
            df: Processed DataFrame ready for upload
            table_id: Full table ID (project.dataset.table)
            schema: BigQuery table schema
            write_mode: How to handle existing data ('append' or 'replace')
        """
        try:
            # Validate table_id format
            parts = table_id.split('.')
            if len(parts) != 3:
                raise ValueError(f"Invalid table_id format: {table_id}. Expected: project.dataset.table")
            
            # Ensure dataset exists
            self.ensure_dataset(parts[1])
            
            # Configure upload job
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE if write_mode == 'replace'
                    else bigquery.WriteDisposition.WRITE_APPEND
                )
            )
            
            # Upload data
            load_job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            load_job.result()  # Wait for completion
            
            logging.info(f"Uploaded {len(df)} rows to {table_id}")
            
        except Exception as e:
            logging.error(f"Upload failed for {table_id}: {str(e)}")
            raise

    def delete_table(self, table_id: str) -> None:
        """Delete a table if it exists.
        
        Args:
            table_id: Full table ID (project.dataset.table)
        """
        try:
            self.client.delete_table(table_id, not_found_ok=True)
            logging.info(f"Deleted table {table_id}")
        except Exception as e:
            logging.error(f"Failed to delete table {table_id}: {str(e)}")
            raise

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
