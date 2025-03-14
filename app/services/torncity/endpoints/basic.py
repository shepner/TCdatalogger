"""Processor for Torn City faction basic endpoint."""

import logging
from datetime import datetime
from typing import Dict

import pandas as pd

from app.services.torncity.base import BaseEndpointProcessor

class BasicEndpointProcessor(BaseEndpointProcessor):
    """Process data from the /v2/faction/basic endpoint.
    
    This processor handles basic faction data including:
    - Faction information (ID, name, tag)
    - Leader and co-leader information
    - Age and creation date
    - Respect earned
    - Territory and capacity stats
    """
    
    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform basic faction data into a normalized DataFrame.
        
        Args:
            data: Raw API response containing basic faction data
            
        Returns:
            pd.DataFrame: Normalized faction data
        """
        try:
            # Extract basic data
            basic_data = data.get("basic", {})
            if not basic_data:
                self._log_error("No basic data found in API response")
                return pd.DataFrame()
            
            # Create record with faction information
            record = {
                "faction_id": basic_data.get("ID"),
                "name": basic_data.get("name"),
                "tag": basic_data.get("tag"),
                "leader_id": basic_data.get("leader"),
                "co_leader_id": basic_data.get("co-leader"),
                "age": basic_data.get("age"),
                "best_chain": basic_data.get("best_chain"),
                "total_respect": basic_data.get("respect"),
                "capacity": basic_data.get("capacity"),
                "territory_count": basic_data.get("territory_count"),
                "territory_respect": basic_data.get("territory_respect"),
                "raid_won": basic_data.get("raid_won", 0),
                "raid_lost": basic_data.get("raid_lost", 0),
                "peace_expiry": basic_data.get("peace", {}).get("expiry"),
                "peace_faction_id": basic_data.get("peace", {}).get("faction_id"),
                "server_timestamp": data.get("timestamp"),
                "fetched_at": data.get("fetched_at")
            }
            
            # Create DataFrame
            df = pd.DataFrame([record])
            if df.empty:
                self._log_error("No records created from basic data")
                return df
            
            # Convert timestamps
            df = self.convert_timestamps(df, exclude_cols=[
                "faction_id", "name", "tag", "leader_id", "co_leader_id"
            ])
            
            # Convert numeric columns
            df = self.convert_numerics(df, exclude_cols=[
                "name", "tag", "server_timestamp", "fetched_at"
            ])
            
            # Ensure ID columns are integers
            for col in ["faction_id", "leader_id", "co_leader_id", "peace_faction_id"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            # Log success
            logging.info({
                "event": "basic_transform_success",
                "record_count": len(df),
                "columns": list(df.columns)
            })
            
            return df
            
        except Exception as e:
            self._log_error(f"Error transforming basic data: {str(e)}")
            return pd.DataFrame() 