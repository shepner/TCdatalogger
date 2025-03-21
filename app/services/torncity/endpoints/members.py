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
            raise ValueError("endpoint_config is required for MembersEndpointProcessor")
            
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
            bigquery.SchemaField("status_details", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status_state", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status_until", "INTEGER", mode="REQUIRED"),
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
        """Transform raw member data into a DataFrame.

        Args:
            data: Raw member data from the API response.

        Returns:
            DataFrame containing transformed member data.
        """
        if not data:
            logging.warning("Empty data received in transform_data")
            return pd.DataFrame()

        # Log raw data structure
        logging.info(f"Raw data structure: {type(data)}")
        logging.info(f"Raw data keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")

        # Get current server timestamp
        server_timestamp = datetime.now(timezone.utc)

        # Handle both v1 and v2 API response formats
        if isinstance(data, dict):
            if "members" in data:
                # v1 API returns a dict with member IDs as keys
                members_dict = data.get("members", {})
            else:
                # Try to get data from v2 API format
                members_dict = data.get("data", {}).get("members", {})
            
            if not members_dict:
                logging.warning(f"No members found in data: {data}")
                return pd.DataFrame()
                
            # Convert dict values to list for v1 format
            if isinstance(members_dict, dict):
                members_list = [
                    {**member, "id": int(member_id)} 
                    for member_id, member in members_dict.items()
                ]
            else:
                members_list = members_dict
        else:
            # v2 API returns a list directly
            members_list = data if isinstance(data, list) else []

        if not members_list:
            logging.warning(f"No members list found after processing data: {data}")
            return pd.DataFrame()

        transformed_members = []
        for member in members_list:
            if not member:
                continue

            # Extract and validate required fields
            member_id = member.get("id")
            if not member_id:
                continue

            # Extract nested data
            status = member.get("status", {})
            last_action = member.get("last_action", {})
            life = member.get("life", {})

            # Convert timestamps
            last_action_timestamp = last_action.get("timestamp")
            if last_action_timestamp:
                try:
                    last_action_timestamp = pd.to_datetime(last_action_timestamp, unit="s")
                except (ValueError, TypeError):
                    last_action_timestamp = pd.Timestamp.now(tz="UTC")
            else:
                last_action_timestamp = pd.Timestamp.now(tz="UTC")

            # Create transformed member data
            transformed_member = {
                "server_timestamp": server_timestamp,
                "id": member_id,
                "name": member.get("name", ""),
                "level": member.get("level", 0),
                "days_in_faction": member.get("days_in_faction", 0),
                "position": member.get("position", ""),
                "is_revivable": member.get("is_revivable", False),
                "is_on_wall": member.get("is_on_wall", False),
                "is_in_oc": member.get("is_in_oc", False),
                "has_early_discharge": member.get("has_early_discharge", False),
                "last_action_status": last_action.get("status", ""),
                "last_action_timestamp": last_action_timestamp,
                "last_action_relative": last_action.get("relative", ""),
                "status_description": status.get("description", ""),
                "status_details": status.get("details", ""),
                "status_state": status.get("state", ""),
                "status_until": status.get("until", 0),
                "life_current": life.get("current", 0),
                "life_maximum": life.get("maximum", 0)
            }
            transformed_members.append(transformed_member)

        if not transformed_members:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=[
                "server_timestamp", "id", "name", "level", "days_in_faction",
                "position", "is_revivable", "is_on_wall", "is_in_oc", "has_early_discharge",
                "last_action_status", "last_action_timestamp", "last_action_relative",
                "status_description", "status_details", "status_state", "status_until",
                "life_current", "life_maximum"
            ])

        # Create DataFrame and set data types
        df = pd.DataFrame(transformed_members)
        df["level"] = df["level"].fillna(0).astype(int)
        df["days_in_faction"] = df["days_in_faction"].fillna(0).astype(int)
        df["status_until"] = df["status_until"].fillna(0).astype(int)
        df["life_current"] = df["life_current"].fillna(0).astype(int)
        df["life_maximum"] = df["life_maximum"].fillna(0).astype(int)

        return df

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
                logging.warning("Empty data received in process_data")
                return pd.DataFrame()
            
            # Log raw API response
            logging.info(f"Raw API response: {data}")
            
            # Transform data to DataFrame
            df = self.transform_data(data)
            
            # Validate against schema
            schema = self.get_schema()
            df = self._validate_schema(df, schema)
            
            if df.empty:
                logging.warning("No valid member data found in API response")
                
            return df
            
        except Exception as e:
            self._log_error(f"Error processing member data: {str(e)}")
            raise DataValidationError(f"Failed to process member data: {str(e)}")