"""Processor for Torn City items endpoint."""

import logging
from datetime import datetime
from typing import Dict

import pandas as pd

from app.services.torncity.base import BaseEndpointProcessor

class ItemsEndpointProcessor(BaseEndpointProcessor):
    """Process data from the /v2/torn/items endpoint.
    
    This processor handles item data including:
    - Basic item information (ID, name, description)
    - Market data (buy price, sell price, market value)
    - Item properties (type, weapon type, requirements)
    - Effect information (damage, accuracy, etc.)
    """
    
    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform items data into a normalized DataFrame.
        
        Args:
            data: Raw API response containing item data
            
        Returns:
            pd.DataFrame: Normalized item data
        """
        try:
            # Extract items data
            items_data = data.get("items", {})
            if not items_data:
                self._log_error("No items data found in API response")
                return pd.DataFrame()
            
            # Convert items dict to list of records
            records = []
            for item_id, item_info in items_data.items():
                # Base item information
                record = {
                    "item_id": item_id,
                    "name": item_info.get("name"),
                    "description": item_info.get("description"),
                    "type": item_info.get("type"),
                    "weapon_type": item_info.get("weapon_type"),
                    "buy_price": item_info.get("buy_price"),
                    "sell_price": item_info.get("sell_price"),
                    "market_value": item_info.get("market_value"),
                    "circulation": item_info.get("circulation"),
                    "image": item_info.get("image"),
                    "requirement_level": item_info.get("requirement", {}).get("level", 0),
                    "requirement_strength": item_info.get("requirement", {}).get("strength", 0),
                    "requirement_speed": item_info.get("requirement", {}).get("speed", 0),
                    "requirement_dexterity": item_info.get("requirement", {}).get("dexterity", 0),
                    "requirement_intelligence": item_info.get("requirement", {}).get("intelligence", 0),
                    "fetched_at": data.get("fetched_at")
                }
                
                # Add effect information if present
                effect = item_info.get("effect", {})
                if effect:
                    record.update({
                        "damage": effect.get("damage", 0),
                        "accuracy": effect.get("accuracy", 0),
                        "damage_bonus": effect.get("damage_bonus", 0),
                        "accuracy_bonus": effect.get("accuracy_bonus", 0)
                    })
                
                records.append(record)
            
            # Create DataFrame
            df = pd.DataFrame(records)
            if df.empty:
                self._log_error("No records created from items data")
                return df
            
            # Convert timestamps
            df = self.convert_timestamps(df, exclude_cols=["item_id", "name", "description", "type", "weapon_type", "image"])
            
            # Convert numeric columns
            df = self.convert_numerics(df, exclude_cols=[
                "name", "description", "type", "weapon_type", "image", "fetched_at"
            ])
            
            # Ensure item_id is integer
            df["item_id"] = df["item_id"].astype("Int64")
            
            # Log success
            logging.info({
                "event": "items_transform_success",
                "record_count": len(df),
                "columns": list(df.columns)
            })
            
            return df
            
        except Exception as e:
            self._log_error(f"Error transforming items data: {str(e)}")
            return pd.DataFrame() 