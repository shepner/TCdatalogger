"""Processor for the Torn City items endpoint."""

import logging
from datetime import datetime
from typing import Dict, Optional, List, Any

import pandas as pd
from google.cloud import bigquery

from app.services.torncity.base import BaseEndpointProcessor, DataValidationError

class ItemsEndpointProcessor(BaseEndpointProcessor):
    """Processor for the items endpoint.
    
    This processor handles data from the /v2/torn/items endpoint.
    It transforms the items data into normalized rows with proper
    data types and timestamps.
    """

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any] = None):
        """Initialize the items endpoint processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
            endpoint_config: Optional endpoint-specific configuration.
        """
        super().__init__(config)
        
        # Initialize endpoint configuration with defaults
        self.endpoint_config.update({
            'name': config['endpoint'],
            'url': config.get('url'),
            'table': config.get('table'),  # Get table from config
            'storage_mode': config.get('storage_mode', 'replace'),
            'frequency': config.get('frequency')
        })
        
        # Update endpoint configuration with any provided overrides
        if endpoint_config:
            self.endpoint_config.update(endpoint_config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for items data."""
        return [
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('item_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('description', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('effect', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('requirement', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('weapon_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('buy_price', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('sell_price', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('market_value', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('circulation', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('image', 'STRING', mode='NULLABLE')
        ]

    def transform_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform the raw API response data into the required format.

        Args:
            data: Raw API response data containing items information

        Returns:
            DataFrame containing transformed items data

        Raises:
            DataValidationError: If data validation fails
        """
        # Validate input data
        if not data or not isinstance(data, dict):
            logging.warning("Invalid data format received")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])
        
        # Extract items data
        items_data = data.get("items", [])  # Changed to handle list format
        if not items_data:
            logging.warning("No items data found in response")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])
        
        # Log detailed information about the items data
        logging.info(f"Processing items data:")
        logging.info(f"Total items in response: {len(items_data)}")
        logging.info(f"Sample item IDs: {[item.get('id') for item in items_data[:5] if item]}")
        
        transformed_data = []
        server_timestamp = pd.Timestamp.now()
        
        for item in items_data:  # Iterate directly over the list
            try:
                # Validate item object
                if not item or not isinstance(item, dict):
                    logging.warning(f"Invalid item data format")
                    continue

                # Extract value data
                value_data = item.get('value', {})
                
                # Create item record
                item_record = {
                    'server_timestamp': server_timestamp,
                    'item_id': item.get('id'),
                    'name': item.get('name'),
                    'description': item.get('description'),
                    'effect': item.get('effect'),
                    'requirement': item.get('requirement'),
                    'type': item.get('type'),
                    'weapon_type': item.get('sub_type'),
                    'buy_price': value_data.get('buy_price'),
                    'sell_price': value_data.get('sell_price'),
                    'market_value': value_data.get('market_price'),
                    'circulation': item.get('circulation'),
                    'image': item.get('image')
                }
                
                transformed_data.append(item_record)
                
            except Exception as e:
                logging.error(f"Error processing item data: {str(e)}")
                continue
        
        # Convert to DataFrame
        df = pd.DataFrame(transformed_data)
        
        # Log summary of transformed data
        logging.info(f"Successfully transformed {len(df)} items")
        
        return df

    def process_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Process the items data.
        
        Args:
            data: Raw API response data
            
        Returns:
            DataFrame containing processed items data
        """
        try:
            # Transform the data
            df = self.transform_data(data)
            
            if df.empty:
                logging.warning("No data to process")
                return df
                
            # Validate the transformed data
            self.validate_data(df)
            
            return df
            
        except Exception as e:
            logging.error(f"Failed to process item data: {str(e)}")
            raise DataValidationError(f"Failed to process item data: {str(e)}") 