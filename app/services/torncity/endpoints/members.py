"""Members endpoint processor."""

from typing import Dict, List, Any, Optional
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import time
import logging

from ..base import BaseEndpointProcessor, DataValidationError

class MembersEndpointProcessor(BaseEndpointProcessor):
    """Processor for members endpoint data."""

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any] = None):
        """Initialize the members endpoint processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
            endpoint_config: Optional endpoint-specific configuration. If not provided,
                           will be constructed from the main config.
        """
        if endpoint_config is None:
            endpoint_config = {
                'name': 'members',
                'url': 'https://api.torn.com/faction/{API_KEY}?selections=basic',
                'table': f"{config.get('dataset', 'torn')}.members",
                'api_key': config.get('tc_api_key', 'default'),
                'storage_mode': config.get('storage_mode', 'append'),
                'frequency': 'PT15M'
            }
        super().__init__(config)
        self.endpoint_config.update(endpoint_config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for faction members data."""
        return [
            bigquery.SchemaField("server_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("level", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("days_in_faction", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("revive_setting", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("position", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_revivable", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("is_on_wall", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("is_in_oc", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("has_early_discharge", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("last_action_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_action_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("last_action_relative", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status_description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status_details", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status_state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status_until", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("life_current", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("life_maximum", "INTEGER", mode="NULLABLE")
        ]

    def transform_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform the raw data into the required format."""
        if not data:
            return []
            
        # Extract members data from the nested structure
        members_data = data.get("data", {}).get("members", {})
        if not members_data:
            return []
            
        transformed_data = []
        
        for member_id, member_data in members_data.items():
            try:
                # Validate required fields and types
                name = member_data.get("name")
                level = member_data.get("level")
                
                if name is None or level is None:
                    error_msg = f"Error processing member {member_id}: Missing required field"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                # Convert name to string if it's not already
                name = str(name) if name is not None else None
                
                # Type validation
                if not isinstance(name, str):
                    error_msg = f"Error processing member {member_id}: Invalid type for name field, expected str, got {type(name)}"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                # Convert level to int if it's a string
                try:
                    level = int(level) if isinstance(level, str) else level
                except (ValueError, TypeError):
                    error_msg = f"Error processing member {member_id}: Invalid numeric value for level field: {level}"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                if not isinstance(level, int):
                    error_msg = f"Error processing member {member_id}: Invalid type for level field, expected int, got {type(level)}"
                    self._log_error(error_msg)
                    raise DataValidationError(error_msg)
                
                # Extract status data
                status_data = member_data.get("status", {})
                if not isinstance(status_data, dict):
                    status_data = {}
                
                # Extract life data
                life_data = member_data.get("life", {})
                if not isinstance(life_data, dict):
                    life_data = {}
                life_current = life_data.get("current")
                life_maximum = life_data.get("maximum")
                
                # Extract faction data
                faction_data = member_data.get("faction", {})
                if not isinstance(faction_data, dict):
                    faction_data = {}
                faction_id = faction_data.get("faction_id")
                faction_position = faction_data.get("position")
                days_in_faction = faction_data.get("days_in_faction")
                
                # Extract last action data
                last_action_data = member_data.get("last_action", {})
                if not isinstance(last_action_data, dict):
                    last_action_data = {}
                last_action = last_action_data.get("status")
                last_action_timestamp = last_action_data.get("timestamp")
                
                # Validate timestamp
                if last_action_timestamp is not None:
                    try:
                        if isinstance(last_action_timestamp, str):
                            pd.to_datetime(last_action_timestamp)
                        elif not isinstance(last_action_timestamp, (int, float)):
                            error_msg = f"Error processing member {member_id}: Invalid timestamp format: {last_action_timestamp}"
                            self._log_error(error_msg)
                            raise DataValidationError(error_msg)
                    except (ValueError, TypeError):
                        error_msg = f"Error processing member {member_id}: Invalid timestamp format: {last_action_timestamp}"
                        self._log_error(error_msg)
                        raise DataValidationError(error_msg)
                
                # Use last_action_timestamp as the record timestamp if available, otherwise use member timestamp
                timestamp = last_action_timestamp if last_action_timestamp else member_data.get("timestamp")
                
                # Validate timestamp
                if timestamp is not None:
                    try:
                        if isinstance(timestamp, str):
                            pd.to_datetime(timestamp)
                        elif not isinstance(timestamp, (int, float)):
                            error_msg = f"Error processing member {member_id}: Invalid timestamp format: {timestamp}"
                            self._log_error(error_msg)
                            raise DataValidationError(error_msg)
                    except (ValueError, TypeError):
                        error_msg = f"Error processing member {member_id}: Invalid timestamp format: {timestamp}"
                        self._log_error(error_msg)
                        raise DataValidationError(error_msg)
                
                # Create transformed member data
                transformed_member = {
                    "member_id": int(member_id),
                    "player_id": int(member_id),  # In Torn, member_id is the same as player_id
                    "name": name,
                    "level": level,
                    "status": status_data.get("state"),
                    "status_description": status_data.get("description"),
                    "last_action": last_action,
                    "last_action_timestamp": last_action_timestamp,
                    "faction_id": faction_id,
                    "faction_position": faction_position,
                    "life_current": life_current,
                    "life_maximum": life_maximum,
                    "days_in_faction": int(days_in_faction) if days_in_faction is not None else None,
                    "timestamp": timestamp
                }
                
                transformed_data.append(transformed_member)
            except (ValueError, TypeError) as e:
                error_msg = f"Error processing member {member_id}: {str(e)}"
                self._log_error(error_msg)
                raise DataValidationError(error_msg)
                
        return transformed_data

    def process_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process the raw API response data.

        Args:
            data: Raw API response data

        Returns:
            List[Dict[str, Any]]: List of processed member records

        Raises:
            DataValidationError: If data validation fails
        """
        try:
            if not data:
                return []
            
            return self.transform_data(data)
            
        except Exception as e:
            self._log_error(f"Error processing member data: {str(e)}")
            raise