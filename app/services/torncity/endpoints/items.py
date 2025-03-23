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
        """Get the BigQuery schema for items data.
        
        Returns:
            List of BigQuery SchemaField objects defining the table schema.
            Schema matches the specification in ARCHITECTURE.md.
        """
        return [
            # Required fields
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED',
                               description='Server time when data was fetched'),
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED',
                               description='Item identifier'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED',
                               description='Item name'),
            
            # Basic item information (NULLABLE)
            bigquery.SchemaField('description', 'STRING', mode='NULLABLE',
                               description='Item description'),
            bigquery.SchemaField('effect', 'STRING', mode='NULLABLE',
                               description='Item effect description'),
            bigquery.SchemaField('requirement', 'STRING', mode='NULLABLE',
                               description='Item usage requirements'),
            bigquery.SchemaField('image', 'STRING', mode='NULLABLE',
                               description='Item image filename'),
            bigquery.SchemaField('type', 'STRING', mode='NULLABLE',
                               description='Item type'),
            bigquery.SchemaField('sub_type', 'STRING', mode='NULLABLE',
                               description='Item subtype'),
            
            # Item flags (NULLABLE)
            bigquery.SchemaField('is_masked', 'BOOLEAN', mode='NULLABLE',
                               description='Whether item details are hidden'),
            bigquery.SchemaField('is_tradable', 'BOOLEAN', mode='NULLABLE',
                               description='Whether item can be traded'),
            bigquery.SchemaField('is_found_in_city', 'BOOLEAN', mode='NULLABLE',
                               description='Whether item can be found in city'),
            
            # Value information (NULLABLE)
            bigquery.SchemaField('value_vendor_country', 'STRING', mode='NULLABLE',
                               description='Country where item is sold'),
            bigquery.SchemaField('value_vendor_name', 'STRING', mode='NULLABLE',
                               description='Name of vendor selling item'),
            bigquery.SchemaField('value_buy_price', 'INTEGER', mode='NULLABLE',
                               description='Price to buy from vendor'),
            bigquery.SchemaField('value_sell_price', 'INTEGER', mode='NULLABLE',
                               description='Price to sell to vendor'),
            bigquery.SchemaField('value_market_price', 'INTEGER', mode='NULLABLE',
                               description='Current market price'),
            bigquery.SchemaField('circulation', 'INTEGER', mode='NULLABLE',
                               description='Amount in circulation'),
            
            # Details and coverage (NULLABLE)
            bigquery.SchemaField('details_coverage_name', 'STRING', mode='NULLABLE',
                               description='Name of the coverage type'),
            bigquery.SchemaField('details_coverage_value', 'FLOAT', mode='NULLABLE',
                               description='Value of the coverage'),
            bigquery.SchemaField('details_category', 'STRING', mode='NULLABLE',
                               description='Item category'),
            bigquery.SchemaField('details_stealth_level', 'FLOAT', mode='NULLABLE',
                               description='Required stealth level'),
            
            # Base stats (NULLABLE)
            bigquery.SchemaField('details_base_stats_damage', 'INTEGER', mode='NULLABLE',
                               description='Base damage stat'),
            bigquery.SchemaField('details_base_stats_accuracy', 'INTEGER', mode='NULLABLE',
                               description='Base accuracy stat'),
            bigquery.SchemaField('details_base_stats_armor', 'INTEGER', mode='NULLABLE',
                               description='Base armor stat'),
            
            # Ammo details (NULLABLE)
            bigquery.SchemaField('details_ammo_id', 'INTEGER', mode='NULLABLE',
                               description='ID of compatible ammo'),
            bigquery.SchemaField('details_ammo_name', 'STRING', mode='NULLABLE',
                               description='Name of compatible ammo'),
            bigquery.SchemaField('details_ammo_magazine_rounds', 'INTEGER', mode='NULLABLE',
                               description='Number of rounds per magazine'),
            bigquery.SchemaField('details_ammo_rate_of_fire_minimum', 'INTEGER', mode='NULLABLE',
                               description='Minimum rate of fire'),
            bigquery.SchemaField('details_ammo_rate_of_fire_maximum', 'INTEGER', mode='NULLABLE',
                               description='Maximum rate of fire'),
            
            # Modification slots (NULLABLE)
            bigquery.SchemaField('details_mods', 'INTEGER', mode='NULLABLE',
                               description='Number of modification slots')
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
        server_timestamp = datetime.utcnow()  # Use UTC timestamp for BigQuery compatibility
        
        for item in items_data:  # Iterate directly over the list
            try:
                # Validate item object
                if not item or not isinstance(item, dict):
                    logging.warning(f"Invalid item data format")
                    continue

                # Extract nested data with safe defaults
                value_data = item.get('value') if isinstance(item.get('value'), dict) else {}
                details_data = item.get('details') if isinstance(item.get('details'), dict) else {}
                base_stats = details_data.get('base_stats') if isinstance(details_data.get('base_stats'), dict) else {}
                ammo_data = details_data.get('ammo') if isinstance(item.get('ammo'), dict) else {}
                
                # Helper function to safely convert values
                def safe_convert(value: Any, convert_type: type, default: Any = None) -> Any:
                    """Safely convert a value to the specified type."""
                    if value is None:
                        return default
                    if isinstance(value, list):
                        # If value is a list, try to convert its first element
                        return convert_type(value[0]) if value else default
                    try:
                        return convert_type(value)
                    except (ValueError, TypeError):
                        return default

                # Create item record with UTC timestamp and safe type conversions
                item_record = {
                    'server_timestamp': server_timestamp,  # UTC timestamp for BigQuery
                    'id': safe_convert(item.get('id'), int, 0),
                    'name': str(item.get('name', '')),
                    'description': str(item.get('description', '')),
                    'effect': str(item.get('effect', '')),
                    'requirement': str(item.get('requirement', '')),
                    'image': str(item.get('image', '')),
                    'type': str(item.get('type', '')),
                    'sub_type': str(item.get('sub_type', '')),
                    'is_masked': bool(item.get('is_masked', False)),
                    'is_tradable': bool(item.get('is_tradable', False)),
                    'is_found_in_city': bool(item.get('is_found_in_city', False)),
                    'value_vendor_country': str(value_data.get('vendor_country', '')),
                    'value_vendor_name': str(value_data.get('vendor_name', '')),
                    'value_buy_price': safe_convert(value_data.get('buy_price'), int),
                    'value_sell_price': safe_convert(value_data.get('sell_price'), int),
                    'value_market_price': safe_convert(value_data.get('market_price'), int),
                    'circulation': safe_convert(item.get('circulation'), int),
                    'details_coverage_name': str(details_data.get('coverage_name', '')),
                    'details_coverage_value': safe_convert(details_data.get('coverage_value'), float),
                    'details_category': str(details_data.get('category', '')),
                    'details_stealth_level': safe_convert(details_data.get('stealth_level'), float),
                    'details_base_stats_damage': safe_convert(base_stats.get('damage'), int),
                    'details_base_stats_accuracy': safe_convert(base_stats.get('accuracy'), int),
                    'details_base_stats_armor': safe_convert(base_stats.get('armor'), int),
                    'details_ammo_id': safe_convert(ammo_data.get('id'), int),
                    'details_ammo_name': str(ammo_data.get('name', '')),
                    'details_ammo_magazine_rounds': safe_convert(ammo_data.get('magazine_rounds'), int),
                    'details_ammo_rate_of_fire_minimum': safe_convert(ammo_data.get('rate_of_fire_minimum'), int),
                    'details_ammo_rate_of_fire_maximum': safe_convert(ammo_data.get('rate_of_fire_maximum'), int),
                    'details_mods': safe_convert(details_data.get('mods'), int)
                }
                
                transformed_data.append(item_record)
                
            except Exception as e:
                logging.error(f"Error processing item data: {str(e)}")
                continue
        
        # Convert to DataFrame with explicit dtypes
        df = pd.DataFrame(transformed_data)
        
        # Set explicit dtypes for columns that PyArrow has trouble with
        if not df.empty:
            dtype_map = {
                'details_coverage_value': 'float64',
                'details_stealth_level': 'float64',
                'details_ammo_id': 'Int64',  # Nullable integer type
                'details_ammo_magazine_rounds': 'Int64',
                'details_ammo_rate_of_fire_minimum': 'Int64',
                'details_ammo_rate_of_fire_maximum': 'Int64',
                'details_mods': 'Int64'
            }
            df = df.astype(dtype_map)
        
        # Log summary of transformed data
        logging.info(f"Successfully transformed {len(df)} items")
        
        return df

    def validate_data(self, df: pd.DataFrame) -> None:
        """Validate the transformed data against schema requirements.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            DataValidationError: If validation fails
        """
        if df.empty:
            return
            
        schema = self.get_schema()
        required_fields = [field.name for field in schema if field.mode == 'REQUIRED']
        
        # Check for missing required fields
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            raise DataValidationError(f"Missing required fields: {', '.join(missing_fields)}")
            
        # Check for null values in required fields
        for field in required_fields:
            if df[field].isnull().any():
                raise DataValidationError(f"Found null values in required field: {field}")

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