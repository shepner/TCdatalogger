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
            bigquery.SchemaField('crime_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('crime_name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('participants', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('success', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('money_gained', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('respect_gained', 'FLOAT', mode='NULLABLE')
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
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except (ValueError, TypeError):
                pass
        
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

        server_timestamp = data.get('timestamp', pd.Timestamp.now())
        fetched_at = data.get('fetched_at', pd.Timestamp.now())

        try:
            if isinstance(server_timestamp, (int, float)):
                server_timestamp = pd.Timestamp.fromtimestamp(server_timestamp)
            else:
                server_timestamp = pd.Timestamp(server_timestamp)
        except (ValueError, TypeError):
            server_timestamp = pd.Timestamp.now()

        try:
            fetched_at = pd.Timestamp(fetched_at)
        except (ValueError, TypeError):
            fetched_at = pd.Timestamp.now()

        processed_crimes = []
        for crime_id, crime_data in crimes_data.items():
            try:
                if not isinstance(crime_data, dict):
                    continue

                # Map old field names to new ones
                crime_data = {
                    'name': crime_data.get('crime_name', crime_data.get('name', '')),
                    'created_at': crime_data.get('time_started', crime_data.get('created_at')),
                    'executed_at': crime_data.get('time_completed', crime_data.get('executed_at')),
                    'status': 'completed' if crime_data.get('success') else crime_data.get('status', ''),
                    'reward_money': crime_data.get('money_gained', crime_data.get('reward_money', 0)),
                    'participants': crime_data.get('participants', []),
                    'difficulty': crime_data.get('difficulty', ''),
                    'planning_at': crime_data.get('planning_at'),
                    'ready_at': crime_data.get('ready_at'),
                    'expired_at': crime_data.get('expired_at'),
                    'rewards': crime_data.get('rewards', {})
                }

                # Process timestamps
                timestamps = {
                    'created_at': crime_data.get('created_at'),
                    'planning_at': crime_data.get('planning_at'),
                    'ready_at': crime_data.get('ready_at'),
                    'executed_at': crime_data.get('executed_at'),
                    'expired_at': crime_data.get('expired_at')
                }
                
                for key, value in timestamps.items():
                    try:
                        if isinstance(value, (int, float)):
                            timestamps[key] = pd.Timestamp.fromtimestamp(value)
                        elif isinstance(value, str):
                            timestamps[key] = pd.Timestamp(value)
                        else:
                            timestamps[key] = None
                    except (ValueError, TypeError):
                        timestamps[key] = None

                # Process rewards
                rewards = crime_data.get('rewards', {})
                if not isinstance(rewards, dict):
                    rewards = {}

                reward_items = rewards.get('items', [])
                if not isinstance(reward_items, list):
                    reward_items = []

                item_ids = []
                item_quantities = []
                for item in reward_items:
                    if isinstance(item, dict):
                        item_id = item.get('id')
                        quantity = item.get('quantity')
                        if item_id is not None and quantity is not None:
                            try:
                                item_ids.append(str(int(item_id)))
                                item_quantities.append(str(int(quantity)))
                            except (ValueError, TypeError):
                                continue

                # Process participants
                participants = crime_data.get('participants', [])
                if not isinstance(participants, list):
                    participants = []

                participant_ids = []
                participant_names = []
                for participant in participants:
                    if isinstance(participant, dict):
                        p_id = participant.get('id')
                        p_name = participant.get('name')
                        if p_id is not None and p_name is not None:
                            try:
                                participant_ids.append(str(int(p_id)))
                                participant_names.append(str(p_name))
                            except (ValueError, TypeError):
                                continue
                    elif isinstance(participant, str):
                        # Handle old format where participants is just a list of IDs
                        try:
                            participant_ids.append(str(int(participant)))
                        except (ValueError, TypeError):
                            continue

                processed_crime = {
                    'id': int(crime_id) if isinstance(crime_id, (int, str)) else 0,
                    'name': str(crime_data.get('name', '')),
                    'created_at': timestamps['created_at'],
                    'planning_at': timestamps['planning_at'],
                    'ready_at': timestamps['ready_at'],
                    'executed_at': timestamps['executed_at'],
                    'expired_at': timestamps['expired_at'],
                    'status': str(crime_data.get('status', '')),
                    'difficulty': str(crime_data.get('difficulty', '')),
                    'reward_money': int(float(rewards.get('money', 0))),
                    'reward_respect': float(rewards.get('respect', 0.0)),
                    'reward_item_count': len(reward_items),
                    'reward_item_ids': ','.join(item_ids) if item_ids else None,
                    'reward_item_quantities': ','.join(item_quantities) if item_quantities else None,
                    'participant_count': len(participants),
                    'participant_ids': ','.join(participant_ids) if participant_ids else None,
                    'participant_names': ','.join(participant_names) if participant_names else None,
                    'server_timestamp': server_timestamp,
                    'fetched_at': fetched_at
                }
                processed_crimes.append(processed_crime)

            except (ValueError, TypeError) as e:
                self._log_error(f"Error processing crime {crime_id}: {str(e)}")
                continue

        if not processed_crimes:
            raise DataValidationError("No valid crimes data found in API response")

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