"""Processor for Torn City faction members endpoint."""

import logging
from datetime import datetime
from typing import Dict

import pandas as pd

from app.services.torncity.base import BaseEndpointProcessor

class MembersEndpointProcessor(BaseEndpointProcessor):
    """Process data from the /v2/faction/members endpoint.
    
    This processor handles member data including:
    - Basic member information (ID, name, level)
    - Status information (last action, last login)
    - Position and role details
    - Activity metrics (forum posts, karma)
    - Faction-specific data (days in faction, status)
    """
    
    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform members data into a normalized DataFrame.
        
        Args:
            data: Raw API response containing member data
            
        Returns:
            pd.DataFrame: Normalized member data
        """
        try:
            # Extract members data
            members_data = data.get("members", {})
            if not members_data:
                self._log_error("No members data found in API response")
                return pd.DataFrame()
            
            # Convert members dict to list of records
            records = []
            for member_id, member_info in members_data.items():
                record = {
                    "member_id": member_id,
                    "faction_id": member_info.get("faction", {}).get("faction_id"),
                    "name": member_info.get("name"),
                    "level": member_info.get("level"),
                    "status": member_info.get("status", {}).get("description"),
                    "status_state": member_info.get("status", {}).get("state"),
                    "status_until": member_info.get("status", {}).get("until"),
                    "last_action": member_info.get("last_action", {}).get("relative"),
                    "last_action_timestamp": member_info.get("last_action", {}).get("timestamp"),
                    "last_login": member_info.get("last_login", 0),
                    "position": member_info.get("position"),
                    "days_in_faction": member_info.get("days_in_faction"),
                    "forum_posts": member_info.get("forum_posts"),
                    "karma": member_info.get("karma"),
                    "fetched_at": data.get("fetched_at")
                }
                records.append(record)
            
            # Create DataFrame
            df = pd.DataFrame(records)
            if df.empty:
                self._log_error("No records created from members data")
                return df
            
            # Convert timestamps
            df = self.convert_timestamps(df, exclude_cols=["member_id", "faction_id", "name", "position"])
            
            # Convert numeric columns
            df = self.convert_numerics(df, exclude_cols=["name", "status", "status_state", "position", "fetched_at"])
            
            # Ensure member_id and faction_id are integers
            for col in ["member_id", "faction_id"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            # Log success
            logging.info({
                "event": "members_transform_success",
                "record_count": len(df),
                "columns": list(df.columns)
            })
            
            return df
            
        except Exception as e:
            self._log_error(f"Error transforming members data: {str(e)}")
            return pd.DataFrame() 