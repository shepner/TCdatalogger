"""Processor for the Torn City crimes endpoint."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
import json

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

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any] = None):
        """Initialize the crimes endpoint processor.

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
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': config.get('frequency')
        })
        
        # Update endpoint configuration with any provided overrides
        if endpoint_config:
            self.endpoint_config.update(endpoint_config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for crimes data."""
        return [
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('difficulty', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('created_at', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('planning_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('executed_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('ready_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('expired_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('slots_position', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_is_reusable', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('slots_item_requirement_is_available', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_joined_at', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('slots_user_progress', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('slots_success_chance', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('slots_crime_pass_rate', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_money', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_items_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_items_quantity', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_respect', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_percentage', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_paid_by', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('rewards_payout_paid_at', 'TIMESTAMP', mode='NULLABLE')
        ]

    def transform_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform the raw data into the required format.

        Args:
            data: Raw API response data containing crimes information

        Returns:
            DataFrame containing transformed crimes data

        Raises:
            DataValidationError: If data validation fails
        """
        # Validate input data
        if not data or not isinstance(data, dict):
            logging.warning("Invalid data format received")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])
        
        # Extract crimes data - it's directly in the root of the response
        crimes_data = data.get("crimes", [])
        if not crimes_data:
            logging.warning("No crimes data found in response")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])
        
        # Log detailed information about the crimes data
        logging.info(f"Processing crimes data:")
        logging.info(f"Total crimes in response: {len(crimes_data)}")
        logging.info(f"Sample crime IDs: {[c.get('id') for c in list(crimes_data)[:5]]}")
        logging.info(f"Response data structure: {json.dumps({k: type(v).__name__ for k, v in data.items()}, indent=2)}")
        
        transformed_data = []
        server_timestamp = pd.Timestamp.now()
        
        for crime in crimes_data:
            try:
                # Validate crime object
                if not crime or not isinstance(crime, dict):
                    logging.warning(f"Invalid crime data format: {crime}")
                    continue

                # Extract nested data with safe defaults and validation
                slots = []
                if isinstance(crime.get('slots'), list):
                    slots = crime['slots']

                rewards = {}
                items = []
                payout = {}  # Initialize payout with empty dict
                if isinstance(crime.get('rewards'), dict):
                    rewards = crime['rewards']
                    if isinstance(rewards.get('items'), list):
                        items = rewards['items']
                    if isinstance(rewards.get('payout'), dict):
                        payout = rewards['payout']

                # Convert timestamps with error handling
                def safe_timestamp(ts):
                    if not ts:
                        return None
                    try:
                        return pd.Timestamp.fromtimestamp(ts)
                    except (ValueError, TypeError, OSError) as e:
                        logging.warning(f"Failed to convert timestamp {ts}: {str(e)}")
                        return None

                # Create base crime dictionary with common fields
                base_crime = {
                    'server_timestamp': server_timestamp,
                    'id': int(crime.get('id', 0)),
                    'name': str(crime.get('name', 'Unknown')),
                    'difficulty': int(crime.get('difficulty', 0)),
                    'status': str(crime.get('status', 'Unknown')),
                    'created_at': safe_timestamp(crime.get('created_at')) or server_timestamp,
                    'planning_at': safe_timestamp(crime.get('planning_at')),
                    'executed_at': safe_timestamp(crime.get('executed_at')),
                    'ready_at': safe_timestamp(crime.get('ready_at')),
                    'expired_at': safe_timestamp(crime.get('expired_at')),
                    'rewards_money': int(rewards.get('money')) if rewards.get('money') is not None else None,
                    'rewards_respect': int(rewards.get('respect')) if rewards.get('respect') is not None else None,
                    'rewards_payout_type': str(payout.get('type', '')),
                    'rewards_payout_percentage': int(payout.get('percentage')) if payout.get('percentage') is not None else None,
                    'rewards_payout_paid_by': int(payout.get('paid_by')) if payout.get('paid_by') is not None else None,
                    'rewards_payout_paid_at': safe_timestamp(payout.get('paid_at'))
                }

                # If no slots, create one row with base crime data
                if not slots:
                    transformed_data.append({
                        **base_crime,
                        'slots_position': '',
                        'slots_item_requirement_id': None,
                        'slots_item_requirement_is_reusable': None,
                        'slots_item_requirement_is_available': None,
                        'slots_user_id': None,
                        'slots_user_joined_at': None,
                        'slots_user_progress': None,
                        'slots_success_chance': None,
                        'slots_crime_pass_rate': None,
                        'rewards_items_id': None,
                        'rewards_items_quantity': None
                    })
                    continue

                # Create a row for each slot
                for slot in slots:
                    if not isinstance(slot, dict):
                        continue

                    item_req = slot.get('item_requirement', {}) if isinstance(slot.get('item_requirement'), dict) else {}
                    user = slot.get('user', {}) if isinstance(slot.get('user'), dict) else {}

                    # Create a row for each item in rewards (or one row with no item if no rewards)
                    items_to_process = items if items else [{}]
                    for item in items_to_process:
                        if not isinstance(item, dict):
                            continue

                        transformed_crime = {
                            **base_crime,
                            'slots_position': str(slot.get('position', '')),
                            'slots_item_requirement_id': int(item_req.get('id')) if item_req.get('id') is not None else None,
                            'slots_item_requirement_is_reusable': bool(item_req.get('is_reusable', False)),
                            'slots_item_requirement_is_available': bool(item_req.get('is_available', False)),
                            'slots_user_id': int(slot.get('user_id')) if slot.get('user_id') is not None else None,
                            'slots_user_joined_at': safe_timestamp(user.get('joined_at')),
                            'slots_user_progress': float(user.get('progress', 0)),
                            'slots_success_chance': int(slot.get('success_chance', 0)),
                            'slots_crime_pass_rate': int(slot.get('crime_pass_rate', 0)),
                            'rewards_items_id': int(item.get('id')) if item.get('id') is not None else None,
                            'rewards_items_quantity': int(item.get('quantity')) if item.get('quantity') is not None else None
                        }
                        transformed_data.append(transformed_crime)

            except Exception as e:
                crime_id = crime.get('id', 'unknown') if isinstance(crime, dict) else 'unknown'
                logging.error(f"Error processing crime {crime_id}: {str(e)}")
                continue

        if not transformed_data:
            logging.warning("No valid crimes data after transformation")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])

        df = pd.DataFrame(transformed_data)
        
        # Convert types to match schema
        for field in self.get_schema():
            if field.field_type == 'TIMESTAMP':
                if field.name not in df.columns:
                    df[field.name] = pd.NaT
                else:
                    df[field.name] = pd.to_datetime(df[field.name], errors='coerce')
            elif field.field_type == 'INTEGER':
                if field.name not in df.columns:
                    df[field.name] = pd.NA if field.mode == 'NULLABLE' else 0
                else:
                    df[field.name] = pd.to_numeric(df[field.name], errors='coerce')
                    if field.mode == 'REQUIRED':
                        df[field.name] = df[field.name].fillna(0).astype('Int64')
                    else:
                        df[field.name] = df[field.name].astype('Int64')
            elif field.field_type == 'FLOAT':
                if field.name not in df.columns:
                    df[field.name] = pd.NA if field.mode == 'NULLABLE' else 0.0
                else:
                    df[field.name] = pd.to_numeric(df[field.name], errors='coerce')
                    if field.mode == 'REQUIRED':
                        df[field.name] = df[field.name].fillna(0.0)
            elif field.field_type == 'BOOLEAN':
                if field.name not in df.columns:
                    df[field.name] = pd.NA if field.mode == 'NULLABLE' else False
                else:
                    df[field.name] = df[field.name].fillna(False).astype('boolean')
            elif field.field_type == 'STRING':
                if field.name not in df.columns:
                    df[field.name] = pd.NA if field.mode == 'NULLABLE' else ''
                else:
                    df[field.name] = df[field.name].fillna('').astype(str)

        return df

    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert DataFrame columns to their proper types.
        
        Args:
            df: DataFrame to convert
            
        Returns:
            DataFrame with converted types
        """
        # Integer columns
        int_columns = [
            'id', 'difficulty', 'slots_item_requirement_id', 'slots_user_id',
            'slots_success_chance', 'slots_crime_pass_rate', 'rewards_money',
            'rewards_items_id', 'rewards_items_quantity', 'rewards_respect',
            'rewards_payout_percentage', 'rewards_payout_paid_by'
        ]
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')

        # Float columns
        float_columns = ['slots_user_progress']
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float64')

        # Boolean columns
        bool_columns = ['slots_item_requirement_is_reusable', 'slots_item_requirement_is_available']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype('boolean')

        # String columns
        string_columns = ['name', 'status', 'slots_position', 'rewards_payout_type']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)

        # Timestamp columns
        timestamp_columns = [
            'server_timestamp', 'created_at', 'planning_at', 'executed_at',
            'ready_at', 'expired_at', 'slots_user_joined_at', 'rewards_payout_paid_at'
        ]
        for col in timestamp_columns:
            if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

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

    def fetch_data(self) -> Dict[str, Any]:
        """Fetch crimes data using sliding 7-day windows.
        
        Returns:
            Dictionary containing all crimes data.
        """
        logger = logging.getLogger(__name__)
        logger.info("Fetching crimes data using sliding time windows")
        
        all_crimes = {}
        window_count = 1
        
        # Get the API key from the client
        api_key_selection = self.endpoint_config.get('api_key', 'default')
        api_key = self.torn_client.api_keys.get(api_key_selection)
        if not api_key:
            raise ValueError(f"API key not found for selection: {api_key_selection}")
        
        # Remove any existing key= prefix
        api_key = api_key.replace('key=', '')
        
        # Start from current time
        end_time = datetime.now()
        
        while True:
            # Calculate start time for this window
            start_time = end_time - timedelta(days=7)
            
            # Format timestamps for API
            from_ts = int(start_time.timestamp())
            to_ts = int(end_time.timestamp())
            
            # Construct URL for this time window
            params = {
                "key": api_key,
                "cat": "all",
                "sort": "DESC",
                "from": from_ts,
                "to": to_ts
            }
            
            logger.info(f"Fetching window {window_count}: {start_time.date()} to {end_time.date()}")
            logger.info(f"Request URL: {self.endpoint_config['url']}")
            logger.info(f"Request params: {params}")
            
            try:
                # Use the session from the base class
                response = self.torn_client.session.get(
                    self.endpoint_config['url'],
                    params=params
                )
                data = response.json()
                logger.info(f"Response status code: {response.status_code}")
                logger.info(f"Response data: {data}")
                
                # Check for API errors
                if 'error' in data:
                    error_msg = data['error'].get('error', 'Unknown API error')
                    logger.error(f"API returned an error: {error_msg}")
                    break
                
                if not data.get("crimes"):
                    logger.info("No crimes found in this time window")
                    break
                    
                crimes = data["crimes"]
                if not crimes:
                    logger.info("No crimes found in this time window")
                    break
                    
                crime_ids = [crime['id'] for crime in crimes]
                logger.info(f"First crime in window: ID {min(crime_ids)}")
                logger.info(f"Last crime in window: ID {max(crime_ids)}")
                logger.info(f"Retrieved {len(crimes)} crimes from window {window_count}")
                
                # Track new crimes added
                new_crimes = {str(crime['id']): crime for crime in crimes if str(crime['id']) not in all_crimes}
                logger.info(f"Added {len(new_crimes)} new crimes")
                
                if new_crimes:
                    logger.info(f"New crime IDs: {sorted(map(int, new_crimes.keys()))}")
                    all_crimes.update(new_crimes)
                    
                logger.info(f"Total crimes so far: {len(all_crimes)}")
                
                # Move window back in time
                end_time = start_time
                window_count += 1
                
                # If we didn't add any new crimes, we're done
                if not new_crimes:
                    logger.info("No new crimes found, stopping time window progression")
                    break
                
                # Add a small delay to avoid hitting rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching crimes data: {str(e)}")
                break
        
        return {"crimes": list(all_crimes.values())} 