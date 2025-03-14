"""Processor for Torn City faction currency endpoint."""

import logging
import re
from datetime import datetime
from typing import Dict

import pandas as pd

from app.services.torncity.base import BaseEndpointProcessor

class CurrencyEndpointProcessor(BaseEndpointProcessor):
    """Process data from the /v2/faction/currency endpoint.
    
    This processor handles faction currency data including:
    - Points balance
    - Money balance
    - Transaction history
    - Balance changes
    """
    
    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform currency data into a normalized DataFrame.
        
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
            match = re.search(r'v2_faction_(\d+)_currency', self.name)
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