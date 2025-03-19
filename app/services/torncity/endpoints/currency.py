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
        
        # Default endpoint config for base currency endpoint
        default_config = {
            'name': 'currency',
            'url': 'https://api.torn.com/torn/{API_KEY}?selections=currency',
            'table': f"{config.get('dataset', 'torn')}.currency",
            'api_key': config.get('tc_api_key', 'default'),
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': 'PT1H'
        }
        
        # Update with endpoint-specific config if provided
        if endpoint_config:
            default_config.update(endpoint_config)
        
        self.endpoint_config.update(default_config)
        
        # Determine if this is a faction currency endpoint
        self.is_faction_endpoint = 'faction' in self.endpoint_config.get('endpoint', '')

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for currency data.

        Returns:
            List of BigQuery SchemaField objects defining the table schema.
        """
        if self.is_faction_endpoint:
            return [
                bigquery.SchemaField('server_timestamp', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('faction_id', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('points_balance', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('money_balance', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('points_accumulated', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('points_total', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('money_accumulated', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('money_total', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('fetched_at', 'TIMESTAMP', mode='NULLABLE')
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
            # Extract currency data
            currency_data = data.get("currency", {})
            if not currency_data:
                self._log_error("No currency data found in API response")
                return pd.DataFrame()
            
            # Extract faction ID from endpoint name
            faction_id = None
            match = re.search(r'v2_faction_(\d+)_currency', self.endpoint_config.get('name', ''))
            if match:
                faction_id = int(match.group(1))
            
            # Create record with current balances
            record = {
                "faction_id": faction_id,
                "points_balance": currency_data.get("points_balance", 0),
                "money_balance": currency_data.get("money_balance", 0),
                "points_accumulated": currency_data.get("points", {}).get("accumulated", 0),
                "points_total": currency_data.get("points", {}).get("total", 0),
                "money_accumulated": currency_data.get("money", {}).get("accumulated", 0),
                "money_total": currency_data.get("money", {}).get("total", 0),
                "server_timestamp": data.get("timestamp"),
                "fetched_at": data.get("fetched_at")
            }
            
            # Create DataFrame
            df = pd.DataFrame([record])
            if df.empty:
                self._log_error("No records created from currency data")
                return df
            
            # Convert timestamps
            df = self.convert_timestamps(df, exclude_cols=["faction_id"])
            
            # Convert numeric columns
            df = self.convert_numerics(df, exclude_cols=["server_timestamp", "fetched_at"])
            
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

    def convert_timestamps(self, df: pd.DataFrame, exclude_cols: list[str] = None) -> pd.DataFrame:
        """Convert timestamp columns to datetime.
        
        Args:
            df: DataFrame to process
            exclude_cols: Columns to exclude from conversion
            
        Returns:
            pd.DataFrame: DataFrame with converted timestamps
        """
        if exclude_cols is None:
            exclude_cols = []
            
        timestamp_cols = [
            col for col in df.columns 
            if "timestamp" in col.lower() and col not in exclude_cols
        ]
        
        for col in timestamp_cols:
            df[col] = pd.to_datetime(df[col], unit='s')
            
        if "fetched_at" in df.columns and "fetched_at" not in exclude_cols:
            df["fetched_at"] = pd.to_datetime(df["fetched_at"])
            
        return df

    def convert_numerics(self, df: pd.DataFrame, exclude_cols: list[str] = None) -> pd.DataFrame:
        """Convert numeric columns to appropriate types.
        
        Args:
            df: DataFrame to process
            exclude_cols: Columns to exclude from conversion
            
        Returns:
            pd.DataFrame: DataFrame with converted numeric types
        """
        if exclude_cols is None:
            exclude_cols = []
            
        numeric_cols = [
            col for col in df.columns 
            if any(t in col.lower() for t in ["id", "balance", "accumulated", "total"])
            and col not in exclude_cols
        ]
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
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