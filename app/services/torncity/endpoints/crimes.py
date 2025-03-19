"""Processor for the Torn City crimes endpoint."""

import logging
from datetime import datetime
from typing import Dict, Optional, List, Any

import pandas as pd
import numpy as np
import time
from google.cloud import bigquery

from app.services.torncity.base import BaseEndpointProcessor, DataValidationError

class CrimesEndpointProcessor(BaseEndpointProcessor):
    """Processor for the crimes endpoint.
    
    This processor handles data from the /v2/faction/crimes endpoint.
    It transforms the nested crime data into normalized rows with proper
    data types and timestamps.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the crimes endpoint processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)
        self.endpoint_config.update({
            'name': 'crimes',
            'url': 'https://api.torn.com/faction/{API_KEY}?selections=crimes',
            'table': f"{config.get('dataset', 'torn')}.crimes",
            'api_key': config.get('tc_api_key', 'default'),
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': 'PT15M'
        })

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for crimes data."""
        return [
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('difficulty', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('created_at', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('planning_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('executed_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('ready_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('expired_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('rewards_money', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('rewards_respect', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('rewards_payout_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_percentage', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_paid_by', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_paid_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('rewards_items_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('rewards_items_quantity', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('slots_position', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_success_chance', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('slots_crime_pass_rate', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_is_reusable', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_is_available', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_joined_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_progress', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('reward_item_count', 'INTEGER', mode='REQUIRED')
        ]

    def convert_timestamps(self, df: pd.DataFrame, exclude_cols: List[str] = None) -> pd.DataFrame:
        """Convert timestamp columns to datetime format.
        
        Args:
            df: DataFrame containing timestamp columns
            exclude_cols: List of column names to exclude from conversion
            
        Returns:
            DataFrame with converted timestamps
        """
        exclude_cols = exclude_cols or []
        timestamp_cols = [col for col in df.columns if 'timestamp' in col.lower() or '_at' in col.lower()]
        timestamp_cols = [col for col in timestamp_cols if col not in exclude_cols]
        
        for col in timestamp_cols:
            try:
                # Try multiple timestamp formats
                timestamps = []
                for val in df[col]:
                    try:
                        if pd.isna(val):
                            timestamps.append(pd.NaT)
                        elif isinstance(val, (int, float)):
                            timestamps.append(pd.Timestamp.fromtimestamp(val))
                        elif isinstance(val, str):
                            try:
                                # Try ISO format first
                                timestamps.append(pd.Timestamp(val))
                            except ValueError:
                                # Try Unix timestamp as string
                                timestamps.append(pd.Timestamp.fromtimestamp(float(val)))
                        else:
                            timestamps.append(pd.NaT)
                    except (ValueError, TypeError):
                        timestamps.append(pd.NaT)
                
                df[col] = timestamps
            except Exception as e:
                logging.warning(f"Failed to convert timestamps for column {col}: {str(e)}")
                continue
        
        return df

    def convert_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert column types to their expected types.
        
        Args:
            df: DataFrame to convert
            
        Returns:
            DataFrame with converted types
        """
        type_conversions = {
            'id': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int),
            'reward_money': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int),
            'reward_respect': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(float),
            'reward_item_count': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int),
            'participant_count': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int)
        }
        
        for col, conversion in type_conversions.items():
            if col in df.columns:
                try:
                    df[col] = conversion(df[col])
                except Exception as e:
                    logging.warning(f"Failed to convert column {col}: {str(e)}")
                    # Set default values based on column type
                    if col in ['id', 'reward_money', 'reward_item_count', 'participant_count']:
                        df[col] = 0
                    elif col == 'reward_respect':
                        df[col] = 0.0
        
        return df

    def transform_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform crimes data into a list of dictionaries.

        Args:
            data: Dictionary containing crimes data.

        Returns:
            List of dictionaries containing transformed data.

        Raises:
            DataValidationError: If data validation fails
        """
        if not data or not isinstance(data, dict):
            raise DataValidationError("No crimes data found in API response")

        # Handle both direct crimes data and wrapped format
        crimes_data = data.get('crimes', data)
        if not crimes_data:
            raise DataValidationError("No crimes data found in API response")

        # Parse server timestamp
        server_timestamp = data.get('timestamp', pd.Timestamp.now())
        try:
            if isinstance(server_timestamp, (int, float)):
                server_timestamp = pd.Timestamp.fromtimestamp(server_timestamp)
            elif isinstance(server_timestamp, str):
                try:
                    # Try parsing as ISO format first
                    server_timestamp = pd.Timestamp(server_timestamp)
                except ValueError:
                    # Try parsing as Unix timestamp
                    server_timestamp = pd.Timestamp.fromtimestamp(float(server_timestamp))
            else:
                server_timestamp = pd.Timestamp.now()
        except (ValueError, TypeError):
            server_timestamp = pd.Timestamp.now()

        # Parse fetched_at timestamp
        fetched_at = data.get('fetched_at', pd.Timestamp.now())
        try:
            if isinstance(fetched_at, (int, float)):
                fetched_at = pd.Timestamp.fromtimestamp(fetched_at)
            elif isinstance(fetched_at, str):
                try:
                    # Try parsing as ISO format first
                    fetched_at = pd.Timestamp(fetched_at)
                except ValueError:
                    # Try parsing as Unix timestamp
                    fetched_at = pd.Timestamp.fromtimestamp(float(fetched_at))
            else:
                fetched_at = pd.Timestamp.now()
        except (ValueError, TypeError):
            fetched_at = pd.Timestamp.now()

        def parse_timestamp(value):
            """Parse timestamp value to pandas Timestamp."""
            if value is None:
                return None
            try:
                if isinstance(value, (int, float)):
                    return pd.Timestamp.fromtimestamp(value)
                elif isinstance(value, str):
                    try:
                        # Try parsing as ISO format first
                        return pd.Timestamp(value)
                    except ValueError:
                        # Try parsing as Unix timestamp
                        return pd.Timestamp.fromtimestamp(float(value))
                elif isinstance(value, pd.Timestamp):
                    return value
                return None
            except (ValueError, TypeError):
                return None

        processed_crimes = []
        for crime_id, crime_data in crimes_data.items():
            try:
                if not isinstance(crime_data, dict):
                    continue

                # Create base crime record
                processed_crime = {
                    'server_timestamp': server_timestamp,
                    'id': int(crime_id) if str(crime_id).isdigit() else 0,
                    'name': crime_data.get('name', ''),
                    'difficulty': crime_data.get('difficulty', 'unknown'),
                    'status': crime_data.get('status', 'unknown'),
                    'created_at': parse_timestamp(crime_data.get('created_at')) or server_timestamp,
                    'planning_at': parse_timestamp(crime_data.get('planning_at')),
                    'ready_at': parse_timestamp(crime_data.get('ready_at')),
                    'executed_at': parse_timestamp(crime_data.get('executed_at')),
                    'expired_at': parse_timestamp(crime_data.get('expired_at')),
                    'rewards_money': int(crime_data.get('rewards', {}).get('money', 0)),
                    'rewards_respect': float(crime_data.get('rewards', {}).get('respect', 0.0)),
                    'rewards_payout_type': crime_data.get('rewards', {}).get('payout', {}).get('type'),
                    'rewards_payout_percentage': float(crime_data.get('rewards', {}).get('payout', {}).get('percentage', 0.0)),
                    'rewards_payout_paid_by': int(crime_data.get('rewards', {}).get('payout', {}).get('paid_by', 0)),
                    'rewards_payout_paid_at': parse_timestamp(crime_data.get('rewards', {}).get('payout', {}).get('paid_at')),
                    'fetched_at': fetched_at,
                    'reward_item_count': 0  # Initialize counter for valid reward items
                }

                # Process reward items
                reward_items = crime_data.get('rewards', {}).get('items', [])
                if isinstance(reward_items, list):
                    item_ids = []
                    item_quantities = []
                    valid_item_count = 0
                    for item in reward_items:
                        if isinstance(item, dict):
                            try:
                                item_id = int(item.get('id', 0))
                                quantity = int(item.get('quantity', 0))
                                if item_id > 0 and quantity > 0:
                                    item_ids.append(str(item_id))
                                    item_quantities.append(str(quantity))
                                    valid_item_count += 1
                            except (ValueError, TypeError):
                                continue
                    processed_crime['rewards_items_id'] = ','.join(item_ids) if item_ids else None
                    processed_crime['rewards_items_quantity'] = ','.join(item_quantities) if item_quantities else None
                    processed_crime['reward_item_count'] = valid_item_count

                # Process slots data
                slots = crime_data.get('slots', {})
                if isinstance(slots, dict):
                    processed_crime.update({
                        'slots_position': int(slots.get('position', 0)),
                        'slots_user_id': int(slots.get('user_id', 0)),
                        'slots_success_chance': float(slots.get('success_chance', 0.0)),
                        'slots_crime_pass_rate': float(slots.get('crime_pass_rate', 0.0)),
                        'slots_item_requirement_id': int(slots.get('item_requirement', {}).get('id', 0)),
                        'slots_item_requirement_is_reusable': bool(slots.get('item_requirement', {}).get('is_reusable', False)),
                        'slots_item_requirement_is_available': bool(slots.get('item_requirement', {}).get('is_available', False)),
                        'slots_user_joined_at': parse_timestamp(slots.get('user_joined_at')),
                        'slots_user_progress': float(slots.get('user_progress', 0.0))
                    })

                processed_crimes.append(processed_crime)

            except Exception as e:
                self._log_error(f"Error processing crime {crime_id}: {str(e)}")
                continue

        return processed_crimes

    def process_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process the crimes data.
        
        Args:
            data: Raw data from the API response
            
        Returns:
            List[Dict[str, Any]]: List of processed data records
            
        Raises:
            DataValidationError: If data validation fails
        """
        try:
            transformed_data = self.transform_data(data)
            self.validate_schema(transformed_data)
            return transformed_data
        except Exception as e:
            error_msg = f"Error transforming crimes data: {str(e)}"
            self._log_error(error_msg)
            raise DataValidationError(error_msg) 