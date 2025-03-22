"""Processor for Torn City faction currency endpoint."""

import logging
import re
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
from google.cloud import bigquery

from app.services.torncity.base import BaseEndpointProcessor, DataValidationError

class CurrencyEndpointProcessor(BaseEndpointProcessor):
    """Processor for Torn City currency endpoint."""

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any] = None):
        """Initialize the currency processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
            endpoint_config: Optional endpoint-specific configuration.
        """
        super().__init__(config)
        
        # Use endpoint config from the main config if not provided separately
        self.endpoint_config = endpoint_config or config.get('endpoint_config', {})
            
        # Determine if this is a faction endpoint
        self.is_faction_endpoint = 'faction' in config.get('endpoint', '')

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for currency data.

        Returns:
            List of BigQuery SchemaField objects defining the table schema.
        """
        if self.is_faction_endpoint:
            return [
                bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('faction_id', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('points', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('money', 'INTEGER', mode='REQUIRED')
            ]
        else:
            return [
                bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('currency_id', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('buy_price', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('sell_price', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('circulation', 'INTEGER', mode='NULLABLE')
            ]

    def transform_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform currency data into a normalized DataFrame.
        
        Args:
            data: Raw API response containing currency data
            
        Returns:
            pd.DataFrame: Normalized currency data
        """
        try:
            if self.is_faction_endpoint:
                return self._transform_faction_currency(data)
            else:
                return self._transform_base_currency(data)
                
        except Exception as e:
            self._log_error(f"Error transforming currency data: {str(e)}")
            return pd.DataFrame()

    def _transform_base_currency(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform base currency endpoint data.
        
        Args:
            data: Raw API response containing currency data
            
        Returns:
            pd.DataFrame: Normalized currency data
        """
        if not data or not isinstance(data, dict):
            self._log_error("Invalid or empty currency data")
            return pd.DataFrame()

        records = []
        
        # Process points data if available
        points_data = data.get('points', {})
        if points_data:
            try:
                records.append({
                    'currency_id': 1,  # Points are always ID 1
                    'name': 'Points',
                    'buy_price': float(points_data.get('buy', 0.0)),
                    'sell_price': float(points_data.get('sell', 0.0)),
                    'circulation': int(points_data.get('total', 0)),
                    'timestamp': self._format_timestamp(points_data.get('timestamp')) or self._get_current_timestamp()
                })
            except (ValueError, TypeError) as e:
                self._log_error(f"Invalid points data: {str(e)}")

        # Process items data if available
        items_data = data.get('items', {})
        for item_id, item_data in items_data.items():
            try:
                if not item_data.get('name'):
                    continue

                records.append({
                    'currency_id': int(item_id),
                    'name': str(item_data.get('name', '')),
                    'buy_price': 0.0,  # Items don't have buy prices
                    'sell_price': float(item_data.get('value', 0.0)),
                    'circulation': 0,  # Items don't have circulation data
                    'timestamp': self._format_timestamp(item_data.get('timestamp')) or self._get_current_timestamp()
                })
            except (ValueError, TypeError) as e:
                self._log_error(f"Invalid item data for {item_id}: {str(e)}")

        if not records:
            self._log_error("No valid currency data found in API response")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        
        # Round numeric values to 2 decimal places
        numeric_cols = ['buy_price', 'sell_price']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].round(2)
        
        return df

    def _transform_faction_currency(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform faction currency endpoint data.
        
        Args:
            data: Raw API response containing currency data
            
        Returns:
            pd.DataFrame: Normalized currency data
        """
        try:
            # Extract faction ID from endpoint name
            faction_id = None
            match = re.search(r'v2_faction_(\d+)_currency', self.config.get('endpoint', ''))
            if match:
                faction_id = int(match.group(1))
            
            # Create record with current balances
            record = {
                "faction_id": faction_id,
                "points": int(data.get("points", 0)),
                "money": int(data.get("money", 0)),
                "server_timestamp": datetime.now().isoformat()
            }
            
            # Create DataFrame
            df = pd.DataFrame([record])
            if df.empty:
                self._log_error("No records created from currency data")
                return df
            
            # Convert timestamps
            df = self.convert_timestamps(df, exclude_cols=["faction_id"])
            
            # Convert numeric columns
            df = self.convert_numerics(df, exclude_cols=["server_timestamp"])
            
            # Ensure faction_id is integer
            if "faction_id" in df.columns:
                df["faction_id"] = df["faction_id"].astype("Int64")
            
            # Log success
            logging.info({
                "event": "currency_transform_success",
                "record_count": len(df),
                "columns": list(df.columns)
            })
            
            return df
            
        except Exception as e:
            self._log_error(f"Error transforming currency data: {str(e)}")
            return pd.DataFrame()

    def convert_timestamps(self, df: pd.DataFrame, exclude_cols: List[str] = None) -> pd.DataFrame:
        """Convert timestamp columns to datetime.
        
        Args:
            df: DataFrame to process
            exclude_cols: Columns to exclude from conversion
            
        Returns:
            DataFrame with converted timestamps
        """
        exclude_cols = exclude_cols or []
        timestamp_cols = [col for col in df.columns if 'timestamp' in col.lower() and col not in exclude_cols]
        
        for col in timestamp_cols:
            df[col] = pd.to_datetime(df[col])
            
        return df

    def convert_numerics(self, df: pd.DataFrame, exclude_cols: List[str] = None) -> pd.DataFrame:
        """Convert numeric columns to appropriate types.
        
        Args:
            df: DataFrame to process
            exclude_cols: Columns to exclude from conversion
            
        Returns:
            DataFrame with converted numeric columns
        """
        exclude_cols = exclude_cols or []
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        numeric_cols = [col for col in numeric_cols if col not in exclude_cols]
        
        for col in numeric_cols:
            if df[col].dtype == 'float64':
                df[col] = df[col].astype('float64')
            else:
                df[col] = df[col].astype('Int64')
                
        return df

    def process_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process the currency data.
        
        Args:
            data: Raw data from the API response
            
        Returns:
            List[Dict[str, Any]]: List of processed data records
        """
        transformed_data = self.transform_data(data)
        self.validate_schema(transformed_data)
        return transformed_data 