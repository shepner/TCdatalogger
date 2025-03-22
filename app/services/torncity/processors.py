"""Torn City API endpoint processors."""

from typing import Dict, List, Any, Optional, Union
import pandas as pd
from google.cloud import bigquery
import time
from datetime import datetime
import logging

from .base import BaseEndpointProcessor, DataValidationError
from .endpoints.crimes import CrimesEndpointProcessor
from .endpoints.currency import CurrencyEndpointProcessor
from .endpoints.items import ItemsEndpointProcessor
from .endpoints.members import MembersEndpointProcessor

class UserProcessor(BaseEndpointProcessor):
    """Processor for Torn City user data."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the user processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)
        self.endpoint_config.update({
            'name': 'user',
            'url': 'https://api.torn.com/user/{API_KEY}',
            'table': f"{config.get('dataset', 'torn')}.users",
            'api_key': config.get('tc_api_key', 'default'),
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': 'PT15M'
        })

    def transform_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform user data into the required format.
        
        Args:
            data: Raw user data from the API
            
        Returns:
            List[Dict[str, Any]]: Transformed data
            
        Raises:
            ValueError: If data is invalid
        """
        if not data:
            raise ValueError("Empty response")

        if not isinstance(data, dict):
            raise ValueError("Invalid data format")

        # Validate required fields
        required_fields = ['player_id', 'name']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")

        # Validate data types
        if not isinstance(data.get('player_id'), int):
            raise ValueError("Invalid data type: player_id must be an integer")
        if not isinstance(data.get('name'), str):
            raise ValueError("Invalid data type: name must be a string")
        if 'level' in data and not isinstance(data['level'], int):
            raise ValueError("Invalid data type: level must be an integer")

        try:
            # Handle nested data structures
            inventory_items = {}
            if 'inventory' in data and 'items' in data['inventory']:
                for item_id, item_data in data['inventory']['items'].items():
                    inventory_items[f"item_{item_id}"] = {
                        'name': item_data.get('name', ''),
                        'quantity': item_data.get('quantity', 0),
                        'equipped': item_data.get('equipped', False)
                    }

            transformed = {
                # Required fields
                'user_id': data['player_id'],
                'player_id': data['player_id'],
                'name': data['name'],
                'level': data.get('level', 0),
                'gender': data.get('gender', ''),
                'status': data.get('status', {}).get('state', ''),
                'status_state': data.get('status', {}).get('state', ''),
                
                # Optional fields with defaults
                'life': data.get('life', 0),
                'max_life': data.get('max_life', 0),
                'energy': data.get('energy', 0),
                'max_energy': data.get('max_energy', 0),
                'nerve': data.get('nerve', 0),
                'max_nerve': data.get('max_nerve', 0),
                'happy': data.get('happy', 0),
                'max_happy': data.get('max_happy', 0),
                'money': data.get('money', 0),
                'points': data.get('points', 0),
                
                # Faction info
                'faction_id': data.get('faction', {}).get('faction_id', 0),
                'faction_name': data.get('faction', {}).get('faction_name', ''),
                
                # Company info
                'company_id': data.get('job', {}).get('company_id', 0),
                'company_name': data.get('job', {}).get('company_name', ''),
                'job': data.get('job', {}).get('position', ''),
                
                # Chain info
                'chain': data.get('chain', {}).get('current', 0),
                'max_chain': data.get('chain', {}).get('maximum', 0),
                
                # Last action
                'last_action': self._format_timestamp(data.get('last_action', {}).get('timestamp', 0)),
                'last_action_status': data.get('last_action', {}).get('status', ''),
                
                # Timestamp
                'timestamp': datetime.now().isoformat()
            }
            
            # Add inventory items if present
            if inventory_items:
                transformed['inventory_items'] = inventory_items

            # Handle schema evolution - preserve any new fields
            for key, value in data.items():
                if key not in transformed and not isinstance(value, (dict, list)):
                    transformed[key] = value

            return [transformed]

        except Exception as e:
            error_msg = f"Error transforming user data: {str(e)}"
            logging.error(error_msg)
            raise ValueError(error_msg)

    def _format_timestamp(self, timestamp: Union[int, str]) -> str:
        """Format Unix timestamp to ISO format string.
        
        Args:
            timestamp: Unix timestamp or string
            
        Returns:
            str: ISO format datetime string
            
        Raises:
            ValueError: If timestamp is invalid
        """
        try:
            if not timestamp:
                return datetime.now().isoformat()
                
            if isinstance(timestamp, str):
                try:
                    timestamp = int(timestamp)
                except ValueError:
                    raise ValueError(f"Invalid timestamp format: {timestamp}")
                    
            return datetime.fromtimestamp(timestamp).isoformat()
        except Exception as e:
            raise ValueError(f"Invalid timestamp: {str(e)}")

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for user data.

        Returns:
            List of BigQuery SchemaField objects defining the table schema.
        """
        return [
            bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('player_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('level', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('gender', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('status', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('status_state', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('last_action', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('last_action_status', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('energy', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('max_energy', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('nerve', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('max_nerve', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('happy', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('max_happy', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('life', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('max_life', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('chain', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('max_chain', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('money', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('points', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('job', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('company_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('company_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('faction_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('faction_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED')
        ]

    def process_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process the user data.
        
        Args:
            data: Raw data from the API response
            
        Returns:
            List[Dict[str, Any]]: List of processed data records
        """
        transformed_data = self.transform_data(data)
        self.validate_schema(transformed_data)
        return transformed_data

    def validate_data(self, data: List[Dict[str, Any]], schema: List[bigquery.SchemaField]) -> None:
        """Validate data against the schema."""
        for record in data:
            for field in schema:
                if field.mode == 'REQUIRED' and field.name not in record:
                    raise ValueError(f"Required field {field.name} missing from record")
                if field.name in record:
                    value = record[field.name]
                    if field.field_type == 'INTEGER' and not isinstance(value, (int, type(None))):
                        raise ValueError(f"Field {field.name} must be an integer")
                    elif field.field_type == 'STRING' and not isinstance(value, (str, type(None))):
                        raise ValueError(f"Field {field.name} must be a string")
                    elif field.field_type == 'TIMESTAMP' and not isinstance(value, (str, datetime, type(None))):
                        raise ValueError(f"Field {field.name} must be a timestamp")

class ItemsProcessor(ItemsEndpointProcessor):
    """Processor for Torn City items data."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the items processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)

class CrimeProcessor(CrimesEndpointProcessor):
    """Processor for Torn City crime data."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the crime processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)

    def transform_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform crime data into a list of dictionaries.

        Args:
            data: Dictionary containing crime data.

        Returns:
            List of dictionaries containing transformed data.
        """
        if not data or not isinstance(data, dict):
            raise DataValidationError("No crime data found in API response")

        server_timestamp = pd.Timestamp.now()
        transformed_crimes = []

        for crime_id, crime_data in data.items():
            try:
                if not isinstance(crime_data, dict):
                    continue

                # Get participants
                participants = crime_data.get('participants', [])
                if not isinstance(participants, list):
                    participants = []

                # Map old field names to new schema
                crime = {
                    'server_timestamp': server_timestamp,
                    'id': int(crime_id),
                    'name': str(crime_data.get('crime_name', '')),
                    'difficulty': '',  # Not available in old schema
                    'status': 'completed' if crime_data.get('success') else 'failed',
                    'created_at': pd.Timestamp.fromtimestamp(crime_data.get('time_started')) if crime_data.get('time_started') else None,
                    'planning_at': None,  # Not available in old schema
                    'executed_at': pd.Timestamp.fromtimestamp(crime_data.get('time_completed')) if crime_data.get('time_completed') else None,
                    'ready_at': None,  # Not available in old schema
                    'expired_at': None,  # Not available in old schema
                    'rewards_money': int(crime_data.get('rewards_money', 0)),
                    'rewards_respect': 0.0,  # Not available in old schema
                    'rewards_payout_type': '',  # Not available in old schema
                    'rewards_payout_percentage': 0.0,  # Not available in old schema
                    'rewards_payout_paid_by': 0,  # Not available in old schema
                    'rewards_payout_paid_at': None,  # Not available in old schema
                    'rewards_items_id': None,  # Not available in old schema
                    'rewards_items_quantity': None,  # Not available in old schema
                    'slots_position': None,  # Not available in old schema
                    'slots_user_id': None,  # Not available in old schema
                    'slots_success_chance': None,  # Not available in old schema
                    'slots_crime_pass_rate': None,  # Not available in old schema
                    'slots_item_requirement_id': None,
                    'slots_item_requirement_is_reusable': False,  # Not available in old schema
                    'slots_item_requirement_is_available': False,  # Not available in old schema
                    'slots_user_joined_at': None,  # Not available in old schema
                    'slots_user_progress': None,  # Not available in old schema
                    'participant_count': len(participants),
                    'participant_ids': ','.join(str(p) for p in participants) if participants else ''
                }

                transformed_crimes.append(crime)

            except Exception as e:
                logging.error(f"Error processing crime {crime_id}: {str(e)}")
                continue

        if not transformed_crimes:
            raise DataValidationError("No valid crime data found after processing")

        return transformed_crimes

class CurrencyProcessor(CurrencyEndpointProcessor):
    """Processor for Torn City currency data."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the currency processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)

class MembersProcessor(MembersEndpointProcessor):
    """Processor for Torn City members data."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the members processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
        """
        super().__init__(config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for members data."""
        return [
            bigquery.SchemaField("member_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("level", "INTEGER"),
            bigquery.SchemaField("days_in_faction", "INTEGER"),
            bigquery.SchemaField("last_action", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("position", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")
        ] 