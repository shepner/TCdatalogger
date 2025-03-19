"""Members endpoint processor."""

from typing import Dict, List, Any, Optional
import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime, timezone

from ..base import BaseEndpointProcessor, DataValidationError

class MembersEndpointProcessor(BaseEndpointProcessor):
    """Processor for members endpoint data."""

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any] = None):
        """Initialize the members endpoint processor.

        Args:
            config: Configuration dictionary containing API and storage settings.
            endpoint_config: Optional endpoint-specific configuration.
        """
        super().__init__(config)
        if endpoint_config is None:
            endpoint_config = {
                'name': 'members',
                'url': 'https://api.torn.com/faction/?selections=basic&key={key}',
                'table': f"{config.get('dataset', 'torn')}.members",
                'api_key': config.get('tc_api_key', 'default'),
                'storage_mode': config.get('storage_mode', 'append'),
                'frequency': 'PT15M'
            }
        self.endpoint_config.update(endpoint_config)

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for faction members data."""
        return [
            bigquery.SchemaField("server_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("level", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("days_in_faction", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("position", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_revivable", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("is_on_wall", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("is_in_oc", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("has_early_discharge", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("last_action_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_action_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("last_action_relative", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status_description", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status_details", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status_state", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status_until", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("life_current", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("life_maximum", "INTEGER", mode="REQUIRED")
        ]

    def validate_required_fields(self, member_data: Dict[str, Any], member_id: str) -> bool:
        """Validate required fields are present and of correct type.

        Args:
            member_data: Raw member data
            member_id: Member ID for error reporting

        Returns:
            bool: True if validation passes, False otherwise
        """
        required_fields = {
            'name': str,
            'level': int
        }

        for field, field_type in required_fields.items():
            value = member_data.get(field)
            if value is None:
                logging.error(f"Missing required field '{field}' for member {member_id}")
                return False
            try:
                if not isinstance(value, field_type):
                    field_type(value)  # Try to convert
            except (ValueError, TypeError):
                logging.error(f"Invalid type for field '{field}' for member {member_id}. Expected {field_type.__name__}")
                return False
        return True

    def transform_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform the raw data into the required format.

        Args:
            data: Raw API response data containing member information

        Returns:
            DataFrame containing transformed member data

        Raises:
            DataValidationError: If data validation fails
        """
        if not data:
            return pd.DataFrame()
            
        # Extract members data directly from response
        members_data = data.get("members", {})
        if not members_data:
            return pd.DataFrame()
            
        transformed_data = []
        server_timestamp = datetime.now(timezone.utc)
        
        for member_id, member_data in members_data.items():
            try:
                # Validate required fields
                if not self.validate_required_fields(member_data, member_id):
                    continue
                
                # Extract and validate nested data
                status_data = member_data.get("status", {}) if isinstance(member_data.get("status"), dict) else {}
                life_data = member_data.get("life", {}) if isinstance(member_data.get("life"), dict) else {}
                last_action_data = member_data.get("last_action", {}) if isinstance(member_data.get("last_action"), dict) else {}
                faction_data = member_data.get("faction", {}) if isinstance(member_data.get("faction"), dict) else {}
                
                # Convert timestamps
                last_action_timestamp = None
                if last_action_ts := last_action_data.get("timestamp"):
                    try:
                        last_action_timestamp = datetime.fromtimestamp(int(last_action_ts), timezone.utc)
                    except (ValueError, TypeError):
                        logging.warning(f"Invalid last_action_timestamp for member {member_id}: {last_action_ts}")
                
                # Create transformed member data matching schema exactly
                transformed_member = {
                    "server_timestamp": server_timestamp,
                    "id": int(member_id),
                    "name": str(member_data["name"]),
                    "level": int(member_data["level"]),
                    "days_in_faction": int(faction_data["days_in_faction"]) if faction_data.get("days_in_faction") else None,
                    "revive_setting": str(member_data["revive_setting"]) if member_data.get("revive_setting") else None,
                    "position": str(faction_data["position"]) if faction_data.get("position") else None,
                    "is_revivable": bool(member_data.get("is_revivable")),
                    "is_on_wall": bool(member_data.get("is_on_wall")),
                    "is_in_oc": bool(member_data.get("is_in_oc")),
                    "has_early_discharge": bool(member_data.get("has_early_discharge")),
                    "last_action_status": str(last_action_data["status"]) if last_action_data.get("status") else None,
                    "last_action_timestamp": last_action_timestamp,
                    "last_action_relative": str(last_action_data["relative"]) if last_action_data.get("relative") else None,
                    "status_description": str(status_data["description"]) if status_data.get("description") else None,
                    "status_details": str(status_data["details"]) if status_data.get("details") else None,
                    "status_state": str(status_data["state"]) if status_data.get("state") else None,
                    "status_until": str(status_data["until"]) if status_data.get("until") else None,
                    "life_current": int(life_data["current"]) if life_data.get("current") else None,
                    "life_maximum": int(life_data["maximum"]) if life_data.get("maximum") else None
                }
                
                transformed_data.append(transformed_member)
                
            except Exception as e:
                logging.error(f"Error processing member {member_id}: {str(e)}")
                continue
                
        # Create DataFrame with schema-defined column order
        schema_fields = self.get_schema()
        columns = [field.name for field in schema_fields]
        df = pd.DataFrame(transformed_data, columns=columns)
        
        # Fill numeric fields with 0 instead of None
        numeric_columns = ['days_in_faction', 'level', 'life_current', 'life_maximum']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        # Explicitly set data types
        dtype_mapping = {
            'server_timestamp': 'datetime64[ns, UTC]',
            'id': 'int64',
            'name': 'object',
            'level': 'int64',
            'days_in_faction': 'int64',
            'revive_setting': 'object',
            'position': 'object',
            'is_revivable': 'bool',
            'is_on_wall': 'bool',
            'is_in_oc': 'bool',
            'has_early_discharge': 'bool',
            'last_action_status': 'object',
            'last_action_timestamp': 'datetime64[ns, UTC]',
            'last_action_relative': 'object',
            'status_description': 'object',
            'status_details': 'object',
            'status_state': 'object',
            'status_until': 'object',
            'life_current': 'int64',
            'life_maximum': 'int64'
        }
        
        for col, dtype in dtype_mapping.items():
            if col in df.columns:
                try:
                    if dtype.startswith('datetime64'):
                        df[col] = pd.to_datetime(df[col], utc=True)
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    logging.warning(f"Failed to convert column {col} to {dtype}: {str(e)}")
        
        return df if not df.empty else pd.DataFrame(columns=columns)

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
                return pd.DataFrame()
            
            df = self.transform_data(data)
            if df.empty:
                logging.warning("No valid member data found in API response")
            return df
            
        except Exception as e:
            self._log_error(f"Error processing member data: {str(e)}")
            raise DataValidationError(f"Failed to process member data: {str(e)}")