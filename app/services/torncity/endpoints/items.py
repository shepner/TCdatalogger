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
        # Initialize with base config first
        super().__init__(config)
        
        # Update endpoint config with defaults
        endpoint_config = {
            'name': 'items',
            'url': 'https://api.torn.com/v2/torn/items',
            'table': f"torncity-402423.torn_data.v2_torn_items",
            'api_key': config.get('tc_api_key', 'default'),
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': 'PT24H'
        }
        self.endpoint_config.update(endpoint_config)
        
        # Override with any provided endpoint config
        if 'table' in config:
            self.endpoint_config['table'] = config['table']
        if 'url' in config:
            self.endpoint_config['url'] = config['url']

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
        logging.info(f"Raw API response type: {type(data)}")
        logging.info(f"Raw API response structure: {data}")

        if not data or not isinstance(data, dict):
            raise DataValidationError("Empty or invalid response from API")

        # Get the items list from the response
        items_data = data.get('items', [])
        logging.info(f"Items data type: {type(items_data)}")
        logging.info(f"Items data structure: {items_data}")

        if not items_data:
            raise DataValidationError("No items data found in API response")

        transformed_data = []
        for item_data in items_data:
            if not isinstance(item_data, dict):
                continue

            try:
                value = item_data.get('value', {})
                if isinstance(value, dict):
                    vendor = value.get('vendor', {})
                    if isinstance(vendor, dict):
                        vendor_country = vendor.get('country', '')
                        vendor_name = vendor.get('name', '')
                    else:
                        vendor_country = ''
                        vendor_name = ''
                    buy_price = value.get('buy_price', 0)
                    sell_price = value.get('sell_price', 0)
                    market_price = value.get('market_price', 0)
                else:
                    vendor_country = ''
                    vendor_name = ''
                    buy_price = 0
                    sell_price = 0
                    market_price = 0

                transformed_item = {
                    'server_timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                    'id': int(item_data.get('id', 0)),
                    'name': str(item_data.get('name', '')),
                    'description': str(item_data.get('description', '')),
                    'effect': str(item_data.get('effect', '')),
                    'requirement': str(item_data.get('requirement', '')),
                    'image': str(item_data.get('image', '')),
                    'type': str(item_data.get('type', '')),
                    'sub_type': str(item_data.get('sub_type', '')),
                    'is_masked': bool(item_data.get('is_masked', False)),
                    'is_tradable': bool(item_data.get('is_tradable', True)),
                    'is_found_in_city': bool(item_data.get('is_found_in_city', True)),
                    'value_vendor_country': str(vendor_country),
                    'value_vendor_name': str(vendor_name),
                    'value_buy_price': int(buy_price or 0),
                    'value_sell_price': int(sell_price or 0),
                    'value_market_price': int(market_price or 0),
                    'circulation': int(item_data.get('circulation', 0)),
                }

                details = item_data.get('details', {})
                if details and isinstance(details, dict):
                    coverage = details.get('coverage', [])
                    if coverage and isinstance(coverage, list) and len(coverage) > 0:
                        transformed_item.update({
                            'details_coverage_name': str(coverage[0].get('name', '')),
                            'details_coverage_value': float(coverage[0].get('value', 0.0))
                        })
                    else:
                        transformed_item.update({
                            'details_coverage_name': '',
                            'details_coverage_value': 0.0
                        })

                    base_stats = details.get('base_stats', {})
                    if base_stats and isinstance(base_stats, dict):
                        transformed_item.update({
                            'details_base_stats_damage': int(base_stats.get('damage', 0)),
                            'details_base_stats_accuracy': int(base_stats.get('accuracy', 0)),
                            'details_base_stats_armor': int(base_stats.get('armor', 0))
                        })
                    else:
                        transformed_item.update({
                            'details_base_stats_damage': 0,
                            'details_base_stats_accuracy': 0,
                            'details_base_stats_armor': 0
                        })

                    transformed_item.update({
                        'details_category': str(details.get('category', '')),
                        'details_stealth_level': float(details.get('stealth_level', 0.0))
                    })

                    ammo = details.get('ammo', {})
                    if ammo and isinstance(ammo, dict):
                        rate_of_fire = ammo.get('rate_of_fire', {})
                        if rate_of_fire and isinstance(rate_of_fire, dict):
                            transformed_item.update({
                                'details_ammo_rate_of_fire_minimum': int(rate_of_fire.get('minimum', 0)),
                                'details_ammo_rate_of_fire_maximum': int(rate_of_fire.get('maximum', 0))
                            })
                        else:
                            transformed_item.update({
                                'details_ammo_rate_of_fire_minimum': 0,
                                'details_ammo_rate_of_fire_maximum': 0
                            })

                        transformed_item.update({
                            'details_ammo_id': int(ammo.get('id', 0)),
                            'details_ammo_name': str(ammo.get('name', '')),
                            'details_ammo_magazine_rounds': int(ammo.get('magazine_rounds', 0))
                        })
                    else:
                        transformed_item.update({
                            'details_ammo_id': 0,
                            'details_ammo_name': '',
                            'details_ammo_magazine_rounds': 0,
                            'details_ammo_rate_of_fire_minimum': 0,
                            'details_ammo_rate_of_fire_maximum': 0
                        })

                    mods = details.get('mods', [])
                    transformed_item['details_mods'] = len(mods) if isinstance(mods, list) else 0
                else:
                    transformed_item.update({
                        'details_coverage_name': '',
                        'details_coverage_value': 0.0,
                        'details_category': '',
                        'details_stealth_level': 0.0,
                        'details_base_stats_damage': 0,
                        'details_base_stats_accuracy': 0,
                        'details_base_stats_armor': 0,
                        'details_ammo_id': 0,
                        'details_ammo_name': '',
                        'details_ammo_magazine_rounds': 0,
                        'details_ammo_rate_of_fire_minimum': 0,
                        'details_ammo_rate_of_fire_maximum': 0,
                        'details_mods': 0
                    })

                transformed_data.append(transformed_item)
                logging.info(f"Successfully transformed item {transformed_item['id']}")

            except Exception as e:
                logging.warning(f"Error transforming item: {e}")
                continue

        if not transformed_data:
            raise DataValidationError("No valid items data after transformation")

        return transformed_data

    def process_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Process the items data.
        
        Args:
            data: Raw data from the API response
            
        Returns:
            pd.DataFrame: DataFrame containing processed data records
        """
        transformed_data = self.transform_data(data)
        df = pd.DataFrame(transformed_data)
        
        # Convert server_timestamp to datetime
        df['server_timestamp'] = pd.to_datetime(df['server_timestamp'])
        
        # Ensure all required columns are present
        schema_fields = {field.name: field for field in self.get_schema()}
        for field_name, field in schema_fields.items():
            if field_name not in df.columns:
                # Add missing column with appropriate default value based on field type
                if field.field_type == 'STRING':
                    df[field_name] = ''
                elif field.field_type == 'INTEGER':
                    df[field_name] = 0
                elif field.field_type == 'FLOAT':
                    df[field_name] = 0.0
                elif field.field_type == 'BOOLEAN':
                    df[field_name] = False
                elif field.field_type == 'TIMESTAMP':
                    df[field_name] = pd.Timestamp.now()
        
        # Validate schema
        self.validate_schema(df)
        
        return df 