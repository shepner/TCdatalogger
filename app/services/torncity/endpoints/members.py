"""Processor for the Torn City faction members endpoint."""

import logging
from datetime import datetime
from typing import Dict, Optional, List, Any
import json

import pandas as pd
from google.cloud import bigquery

from app.services.torncity.base import BaseEndpointProcessor, DataValidationError

class MembersEndpointProcessor(BaseEndpointProcessor):
    """Processor for the faction members endpoint.
    
    This processor handles data from the /v2/faction/members endpoint.
    It transforms the member data into normalized rows with proper
    data types and timestamps.
    """

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any] = None):
        """Initialize the members endpoint processor.

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
        """Get the BigQuery schema for faction members data."""
        return [
            bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('level', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('days_in_faction', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('revive_setting', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('position', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_revivable', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('is_on_wall', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('is_in_oc', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('has_early_discharge', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('last_action_status', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('last_action_timestamp', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('last_action_relative', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('status_description', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('status_details', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('status_state', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('status_until', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('life_current', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('life_maximum', 'INTEGER', mode='NULLABLE')
        ]

    def transform_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform the raw data into the required format.

        Args:
            data: Raw API response data containing member information

        Returns:
            DataFrame containing transformed member data

        Raises:
            DataValidationError: If data validation fails
        """
        # Validate input data
        if not data or not isinstance(data, dict):
            logging.warning("Invalid data format received")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])
        
        # Extract members data - it's a list in the members field
        members_data = data.get("members", [])
        if not members_data:
            logging.warning("No members data found in response")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])
        
        # Log detailed information about the members data
        logging.info(f"Processing members data:")
        logging.info(f"Total members in response: {len(members_data)}")
        logging.info(f"Sample member IDs: {[m.get('id') for m in members_data[:5] if m]}")
        
        transformed_data = []
        server_timestamp = pd.Timestamp.now()
        
        for member in members_data:
            try:
                # Validate member object
                if not member or not isinstance(member, dict):
                    logging.warning(f"Invalid member data format")
                    continue

                # Convert timestamps with error handling
                def safe_timestamp(ts):
                    if not ts:
                        return None
                    try:
                        return pd.Timestamp.fromtimestamp(ts)
                    except (ValueError, TypeError, OSError) as e:
                        logging.warning(f"Failed to convert timestamp {ts}: {str(e)}")
                        return None

                # Extract status information
                status = member.get('status', {})
                if not isinstance(status, dict):
                    status = {}

                # Create member record
                member_record = {
                    'server_timestamp': server_timestamp,
                    'id': int(member.get('id', 0)),
                    'name': str(member.get('name', 'Unknown')),
                    'level': int(member.get('level', 0)),
                    'days_in_faction': int(member.get('days_in_faction', 0)),
                    'revive_setting': str(member.get('revive_setting', '')),
                    'position': str(member.get('position', '')),
                    'is_revivable': bool(member.get('is_revivable', False)),
                    'is_on_wall': bool(member.get('is_on_wall', False)),
                    'is_in_oc': bool(member.get('is_in_oc', False)),
                    'has_early_discharge': bool(member.get('has_early_discharge', False)),
                    'last_action_status': str(member.get('last_action', {}).get('status', 'Unknown')),
                    'last_action_timestamp': safe_timestamp(member.get('last_action', {}).get('timestamp')) or server_timestamp,
                    'last_action_relative': str(member.get('last_action', {}).get('relative', '')),
                    'status_description': str(status.get('description', '')),
                    'status_details': str(status.get('details', '')),
                    'status_state': str(status.get('state', '')),
                    'status_until': str(status.get('until', '')),
                    'life_current': int(member.get('life', {}).get('current')) if member.get('life') else None,
                    'life_maximum': int(member.get('life', {}).get('maximum')) if member.get('life') else None
                }

                transformed_data.append(member_record)

            except Exception as e:
                logging.error(f"Error processing member: {str(e)}")
                continue

        if not transformed_data:
            logging.warning("No valid members data after transformation")
            return pd.DataFrame(columns=[field.name for field in self.get_schema()])

        df = pd.DataFrame(transformed_data)
        
        # Convert types to match schema
        for field in self.get_schema():
            if field.field_type == 'TIMESTAMP':
                if field.name not in df.columns:
                    df[field.name] = pd.NaT
                else:
                    df[field.name] = pd.to_datetime(df[field.name], errors='coerce')
                    if field.mode == 'REQUIRED':
                        df[field.name] = df[field.name].fillna(pd.Timestamp.now())
            elif field.field_type == 'INTEGER':
                if field.name not in df.columns:
                    df[field.name] = pd.NA if field.mode == 'NULLABLE' else 0
                else:
                    df[field.name] = pd.to_numeric(df[field.name], errors='coerce')
                    if field.mode == 'REQUIRED':
                        df[field.name] = df[field.name].fillna(0).astype('Int64')
                    else:
                        df[field.name] = df[field.name].astype('Int64')
            elif field.field_type == 'STRING':
                if field.name not in df.columns:
                    df[field.name] = pd.NA if field.mode == 'NULLABLE' else ''
                else:
                    df[field.name] = df[field.name].fillna('').astype(str)

        return df

    def process_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Process the raw API response data.

        Args:
            data: Raw API response data

        Returns:
            DataFrame: DataFrame containing processed member records

        Raises:
            DataValidationError: If data validation fails
        """
        try:
            if not data:
                logging.warning("Empty data received in process_data")
                return pd.DataFrame()
            
            # Log raw API response
            logging.info(f"Raw API response: {data}")
            
            # Transform data to DataFrame
            df = self.transform_data(data)
            
            # Validate against schema
            schema = self.get_schema()
            df = self._validate_schema(df, schema)
            
            if df.empty:
                logging.warning("No valid member data found in API response")
                
            return df
            
        except Exception as e:
            self._log_error(f"Error processing member data: {str(e)}")
            raise DataValidationError(f"Failed to process member data: {str(e)}")