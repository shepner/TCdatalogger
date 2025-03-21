"""Processor for Torn City basic faction endpoint."""

import logging
from datetime import datetime
from typing import Dict, List, Any
import json

import pandas as pd
from google.cloud import bigquery

from app.services.torncity.base import BaseEndpointProcessor
from app.services.torncity.exceptions import DataValidationError

class BasicFactionEndpointProcessor(BaseEndpointProcessor):
    """Processor for Torn City basic faction data."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the basic faction processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)
        
        # Update endpoint config with defaults
        endpoint_config = {
            'name': 'basic',
            'url': config.get('url', 'https://api.torn.com/v2/faction/40832/basic'),
            'table': config.get('table', 'torncity-402423.torn_data.v2_faction_40832_basic'),
            'api_key': config.get('api_key', 'faction_40832'),
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': config.get('frequency', 'P1D')
        }
        self.endpoint_config.update(endpoint_config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for basic faction data.

        Returns:
            List of BigQuery SchemaField objects defining the table schema.
        """
        return [
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('tag', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('tag_image', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('leader_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('co_leader_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('respect', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('days_old', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('capacity', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('members', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('is_enlisted', 'BOOLEAN', mode='REQUIRED'),
            bigquery.SchemaField('rank_level', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('rank_name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('rank_division', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('rank_position', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('rank_wins', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('best_chain', 'INTEGER', mode='REQUIRED')
        ]

    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform basic faction data into a DataFrame.

        Args:
            data: Raw API response data.

        Returns:
            DataFrame containing transformed faction data.

        Raises:
            DataValidationError: If the data is invalid or missing required fields.
        """
        logging.info(f"API response structure: {json.dumps({k: type(v).__name__ for k, v in data.items()})}")
        
        # Extract basic data from response
        basic_data = data.get('basic')
        if not basic_data:
            raise DataValidationError("No basic faction data found in API response")
            
        # Log raw data for debugging
        logging.info(f"Raw basic data: {json.dumps(basic_data, indent=2)}")
            
        # Create a record with all required fields
        record = {
            'server_timestamp': datetime.now(),
            'id': basic_data.get('id', 0),
            'name': basic_data.get('name', ''),
            'tag': basic_data.get('tag', ''),
            'tag_image': basic_data.get('tag_image', ''),
            'leader_id': basic_data.get('leader_id', 0),
            'co_leader_id': basic_data.get('co-leader_id', 0),
            'respect': basic_data.get('respect', 0),
            'days_old': basic_data.get('days_old', 0),
            'capacity': basic_data.get('capacity', 0),
            'members': basic_data.get('members', 0),
            'is_enlisted': basic_data.get('is_enlisted', False),
            'rank_level': basic_data.get('rank', {}).get('level', 0),
            'rank_name': basic_data.get('rank', {}).get('name', ''),
            'rank_division': basic_data.get('rank', {}).get('division', 0),
            'rank_position': basic_data.get('rank', {}).get('position', 0),
            'rank_wins': basic_data.get('rank', {}).get('wins', 0),
            'best_chain': basic_data.get('best_chain', 0)
        }
        
        # Log transformed record for debugging
        logging.info(f"Transformed record: {json.dumps(record, default=str, indent=2)}")
        
        # Convert to DataFrame
        df = pd.DataFrame([record])
        
        # Ensure all numeric columns are integers
        numeric_columns = [
            'id', 'leader_id', 'co_leader_id', 'respect', 'days_old', 'capacity',
            'members', 'rank_level', 'rank_division', 'rank_position', 'rank_wins',
            'best_chain'
        ]
        for col in numeric_columns:
            df[col] = df[col].astype('Int64')
            
        # Ensure boolean columns are boolean
        df['is_enlisted'] = df['is_enlisted'].astype('bool')
        
        logging.info(f"Successfully transformed basic faction data: {len(df)} rows")
        return df 