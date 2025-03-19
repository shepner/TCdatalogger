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
        """Get the BigQuery schema for items data."""
        return [
            bigquery.SchemaField("item_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("buy_price", "INTEGER"),
            bigquery.SchemaField("sell_price", "INTEGER"),
            bigquery.SchemaField("market_value", "INTEGER"),
            bigquery.SchemaField("circulation", "INTEGER"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")
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