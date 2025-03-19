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

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any] = None):
        """Initialize the crimes endpoint processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
            endpoint_config: Optional endpoint-specific configuration.
        """
        super().__init__(config)
        if endpoint_config is None:
            endpoint_config = {
                'name': 'crimes',
                'url': 'https://api.torn.com/faction/{API_KEY}?selections=crimes',
                'table': f"{config.get('dataset', 'torn')}.crimes",
                'api_key': config.get('tc_api_key', 'default'),
                'storage_mode': config.get('storage_mode', 'append'),
                'frequency': 'PT15M'
            }
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

    def transform_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform the raw data into the required format.

        Args:
            data: Raw API response data containing crimes information

        Returns:
            DataFrame containing transformed crimes data

        Raises:
            DataValidationError: If data validation fails
        """
        if not data:
            return pd.DataFrame()
            
        # Extract crimes data from the nested structure
        crimes_data = data.get("data", {}).get("crimes", {})
        if not crimes_data:
            return pd.DataFrame()
            
        transformed_data = []
        server_timestamp = pd.Timestamp.now()
        
        for crime_id, crime_data in crimes_data.items():
            try:
                # Create transformed crime data matching schema exactly
                transformed_crime = {
                    "server_timestamp": server_timestamp,
                    "crime_id": crime_id,
                    "crime_name": crime_data.get("crime_name"),
                    "participants": crime_data.get("participants"),
                    "time_started": crime_data.get("time_started"),
                    "time_completed": crime_data.get("time_completed"),
                    "time_ready": crime_data.get("time_ready"),
                    "initiated_by": crime_data.get("initiated_by"),
                    "planned_by": crime_data.get("planned_by"),
                    "success": crime_data.get("success"),
                    "money_gain": crime_data.get("money_gain"),
                    "respect_gain": crime_data.get("respect_gain"),
                    "initiated_by_name": crime_data.get("initiated_by_name"),
                    "planned_by_name": crime_data.get("planned_by_name"),
                    "crime_type": crime_data.get("crime_type"),
                    "state": crime_data.get("state")
                }
                
                transformed_data.append(transformed_crime)
                
            except Exception as e:
                logging.error(f"Error processing crime {crime_id}: {str(e)}")
                continue
                
        # Create DataFrame and validate
        df = pd.DataFrame(transformed_data)
        return df if not df.empty else pd.DataFrame(columns=self.get_schema())

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