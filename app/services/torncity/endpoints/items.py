"""Processor for items data from Torn City API."""

import logging
from datetime import datetime
from typing import Dict, List, Any
import time

import pandas as pd
from google.cloud import bigquery

from app.services.torncity.base import BaseEndpointProcessor, DataValidationError

class ItemsEndpointProcessor(BaseEndpointProcessor):
    """Processor for Torn City items endpoint."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the items endpoint processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        endpoint_config = {
            'name': 'items',
            'url': 'https://api.torn.com/torn/{API_KEY}?selections=items',
            'table': f"{config.get('dataset', 'torn')}.items",
            'api_key': config.get('tc_api_key', 'default'),
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': 'PT1H'
        }
        super().__init__(config)
        self.endpoint_config.update(endpoint_config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for items data.

        Returns:
            List of BigQuery SchemaField objects defining the table schema.
        """
        return [
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('description', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('effect', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('requirement', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('image', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sub_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_masked', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('is_tradable', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('is_found_in_city', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('value_vendor_country', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('value_vendor_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('value_buy_price', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('value_sell_price', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('value_market_price', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('circulation', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_coverage_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('details_coverage_value', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('details_category', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('details_stealth_level', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('details_base_stats_damage', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_base_stats_accuracy', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_base_stats_armor', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_ammo_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_ammo_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('details_ammo_magazine_rounds', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_ammo_rate_of_fire_minimum', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_ammo_rate_of_fire_maximum', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('details_mods', 'INTEGER', mode='NULLABLE')
        ]

    def transform_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform items data into a list of dictionaries.

        Args:
            data: Dictionary containing items data from the API response.

        Returns:
            List of dictionaries containing transformed items data.

        Raises:
            DataValidationError: If the response is empty or missing required fields.
        """
        if not data:
            raise DataValidationError("Empty response from API")

        transformed_data = []
        for item_id, item_data in data.items():
            if not isinstance(item_data, dict):
                continue

            transformed_item = {
                'item_id': int(item_id),
                'name': item_data.get('name', ''),
                'description': item_data.get('description', ''),
                'type': item_data.get('type', ''),
                'buy_price': item_data.get('buy_price', 0),
                'sell_price': item_data.get('sell_price', 0),
                'market_value': item_data.get('market_value', 0),
                'circulation': item_data.get('circulation', 0),
                'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            }
            transformed_data.append(transformed_item)

        if not transformed_data:
            raise DataValidationError("No valid items data found")

        return transformed_data

    def process_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process the items data.
        
        Args:
            data: Raw data from the API response
            
        Returns:
            List[Dict[str, Any]]: List of processed data records
        """
        transformed_data = self.transform_data(data)
        self.validate_schema(transformed_data)
        return transformed_data 