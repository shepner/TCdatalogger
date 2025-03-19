"""Processor for Torn City server timestamp endpoint."""

from typing import Dict, List, Any
from google.cloud import bigquery
from ..base import BaseEndpointProcessor

class ServerTimestampEndpointProcessor(BaseEndpointProcessor):
    """Processor for Torn City server timestamp data."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the server timestamp processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for server timestamp data.

        Returns:
            List of BigQuery SchemaField objects defining the table schema.
        """
        return [
            bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED')
        ] 