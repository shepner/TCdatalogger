"""BigQuery client for data storage operations.

This module provides functionality for interacting with Google BigQuery:
- Table management
- Data upload
- Schema management
- Query execution
"""

from typing import Dict, List, Optional, Union
import pandas as pd
from google.cloud import bigquery
from google.api_core import retry

from app.services.google.base.client import BaseGoogleClient


class BigQueryClient(BaseGoogleClient):
    """Client for Google BigQuery operations.
    
    This class provides methods for:
    - Managing BigQuery tables
    - Uploading data
    - Managing schemas
    - Executing queries
    """
    
    def __init__(self, config: Dict):
        """Initialize the BigQuery client.
        
        Args:
            config: Application configuration
        """
        super().__init__(config)
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.project_id
        )
        
    def get_table(self, table_id: str) -> bigquery.Table:
        """Get a BigQuery table reference.
        
        Args:
            table_id: Full table ID (project.dataset.table)
            
        Returns:
            Table: BigQuery table reference
        """
        return self.client.get_table(table_id)
        
    def create_table(self, table_id: str, schema: List[bigquery.SchemaField],
                    exists_ok: bool = True) -> None:
        """Create a BigQuery table.
        
        Args:
            table_id: Full table ID (project.dataset.table)
            schema: List of schema field definitions
            exists_ok: Whether to ignore "already exists" errors
        """
        table = bigquery.Table(table_id, schema=schema)
        self.client.create_table(table, exists_ok=exists_ok)
        
    def upload_dataframe(self, df: pd.DataFrame, table_id: str,
                        write_disposition: str = 'WRITE_APPEND') -> None:
        """Upload a pandas DataFrame to BigQuery.
        
        Args:
            df: DataFrame to upload
            table_id: Full table ID (project.dataset.table)
            write_disposition: How to handle existing data
                             (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
        """
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition
        )
        
        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Wait for job to complete
        
    def execute_query(self, query: str,
                     retry_count: Optional[int] = 3) -> pd.DataFrame:
        """Execute a BigQuery SQL query.
        
        Args:
            query: SQL query to execute
            retry_count: Number of retry attempts
            
        Returns:
            DataFrame: Query results as a pandas DataFrame
        """
        retry_config = retry.Retry(deadline=30)
        query_job = self.client.query(
            query,
            retry=retry_config
        )
        return query_job.result().to_dataframe()
        
    def table_exists(self, table_id: str) -> bool:
        """Check if a table exists.
        
        Args:
            table_id: Full table ID (project.dataset.table)
            
        Returns:
            bool: Whether the table exists
        """
        try:
            self.client.get_table(table_id)
            return True
        except Exception:
            return False
            
    def get_schema(self, table_id: str) -> List[bigquery.SchemaField]:
        """Get the schema of a table.
        
        Args:
            table_id: Full table ID (project.dataset.table)
            
        Returns:
            List[SchemaField]: Table schema
        """
        table = self.client.get_table(table_id)
        return table.schema
