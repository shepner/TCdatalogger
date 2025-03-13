"""Common utilities for data processing and configuration management.

This module provides utilities for:
- Configuration directory management
- Data type inference and conversion
- Data flattening and processing
- Timestamp handling
- Logging setup and configuration
- API endpoint processing

Configuration Notes:
    The API configuration uses ISO 8601 duration format for frequencies:
    - Format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
    - P is the duration designator (period)
    - T is the time designator
    - Examples:
        PT15M = 15 minutes
        PT1H = 1 hour
        P1D = 1 day
        PT1H30M = 1 hour and 30 minutes

    Storage modes:
    - append: Add new records to existing data (default)
    - replace: Replace all existing data with new data

The main functionality is focused on processing nested JSON data structures
and preparing them for BigQuery upload, with proper type handling and
data structure flattening.
"""

import os
import pandas as pd
import json
import logging
import numpy as np
import sys
import re
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
import time

from app.svcProviders.TornCity.TornCity import tc_fetch_api_data
from app.svcProviders.Google.Google import upload_to_bigquery

# Global variable to store server timestamp
_server_timestamp = None

def parse_iso_duration(duration: str) -> timedelta:
    """Parse an ISO 8601 duration string into a timedelta object.
    
    This function parses duration strings in the format P[n]Y[n]M[n]DT[n]H[n]M[n]S
    into a Python timedelta object. Note that years and months are converted to days
    (365 days for a year, 30 days for a month).
    
    Args:
        duration: ISO 8601 duration string (e.g., "PT15M", "PT1H", "P1D")
        
    Returns:
        timedelta: The parsed duration as a timedelta object
        
    Raises:
        ValueError: If the duration string is invalid
        
    Example:
        >>> parse_iso_duration("PT15M")
        timedelta(minutes=15)
        >>> parse_iso_duration("PT1H")
        timedelta(hours=1)
        >>> parse_iso_duration("P1D")
        timedelta(days=1)
    """
    if not isinstance(duration, str):
        raise ValueError(f"Duration must be a string, got {type(duration)}")
    
    if not duration or not duration.startswith('P'):
        raise ValueError(f"Invalid duration format: {duration}. Must start with 'P'")
    
    duration = duration[1:]  # Remove P
    
    # Initialize timedelta components
    days = 0
    hours = 0
    minutes = 0
    seconds = 0
    
    # Split into date and time parts if T is present
    if 'T' in duration:
        date_part, time_part = duration.split('T')
    else:
        date_part = duration
        time_part = ''
    
    # Parse date part
    remaining_date = date_part
    if date_part:
        # Years
        if 'Y' in remaining_date:
            parts = remaining_date.split('Y')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid years format in duration: {duration}")
            days += int(parts[0]) * 365
            remaining_date = parts[1]
        
        # Months
        if 'M' in remaining_date:
            parts = remaining_date.split('M')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid months format in duration: {duration}")
            days += int(parts[0]) * 30
            remaining_date = parts[1]
        
        # Days
        if 'D' in remaining_date:
            parts = remaining_date.split('D')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid days format in duration: {duration}")
            days += int(parts[0])
            remaining_date = parts[1]
        
        if remaining_date:
            raise ValueError(f"Invalid date format in duration: {duration}")
    
    # Parse time part
    remaining_time = time_part
    if time_part:
        # Hours
        if 'H' in remaining_time:
            parts = remaining_time.split('H')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid hours format in duration: {duration}")
            hours = int(parts[0])
            remaining_time = parts[1]
        
        # Minutes
        if 'M' in remaining_time:
            parts = remaining_time.split('M')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid minutes format in duration: {duration}")
            minutes = int(parts[0])
            remaining_time = parts[1]
        
        # Seconds
        if 'S' in remaining_time:
            parts = remaining_time.split('S')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid seconds format in duration: {duration}")
            seconds = int(parts[0])
            remaining_time = parts[1]
        
        if remaining_time:
            raise ValueError(f"Invalid time format in duration: {duration}")
    
    # Ensure at least one valid component was found
    if days == 0 and hours == 0 and minutes == 0 and seconds == 0:
        raise ValueError(f"Duration must specify at least one time component: {duration}")
    
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def wait_for_next_poll(api_config: Dict) -> None:
    """Wait until the next polling interval based on the endpoint's frequency.
    
    This function calculates the next polling time based on the endpoint's frequency
    and waits until that time. It uses the ISO 8601 duration format to determine
    the interval.
    
    Args:
        api_config: API endpoint configuration dictionary containing the frequency
        
    Raises:
        ValueError: If frequency is missing or invalid
    """
    if 'frequency' not in api_config:
        raise ValueError(f"Missing frequency for endpoint {api_config['name']}")
        
    frequency = api_config['frequency']
    if not frequency:
        raise ValueError(f"Empty frequency for endpoint {api_config['name']}")
    
    try:
        interval = parse_iso_duration(frequency)
        interval_seconds = int(interval.total_seconds())  # Convert to integer seconds
        
        if interval_seconds > 0:
            logging.info("Waiting %d seconds until next poll for %s", 
                        interval_seconds, api_config['name'])
            time.sleep(interval_seconds)
    except ValueError as e:
        raise ValueError(f"Invalid frequency format '{frequency}' for endpoint {api_config['name']}: {str(e)}")

def setup_logging() -> None:
    """Configure logging for the application.
    
    Sets up logging to both console and file with appropriate format
    and log level.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('tcdata.log')
        ]
    )

def load_config(config_directories: List[str]) -> Optional[Dict]:
    """Load configuration from the first valid directory.
    
    Args:
        config_directories: List of directory paths to search.
        
    Returns:
        Dict: Configuration dictionary if successful, None otherwise.
    """
    config_dir = find_config_directory(config_directories)
    
    if not config_dir:
        logging.error("No configuration directory found.")
        return None
        
    config = {
        "config_dir": config_dir,
        "gcp_credentials_file": os.path.join(config_dir, "credentials.json"),
        "tc_api_key_file": os.path.join(config_dir, "TC_API_key.txt"),
        "tc_api_config_file": os.path.join(config_dir, "TC_API_config.json"),
    }
    
    # Verify all required files exist
    for key, filepath in config.items():
        if key != "config_dir" and not os.path.exists(filepath):
            logging.error(f"Required configuration file not found: {filepath}")
            return None
            
    return config

def process_api_endpoint(config: Dict, api_config: Dict, api_keys: Dict[str, str]) -> bool:
    """Process a single API endpoint and upload data to BigQuery.
    
    Args:
        config: Application configuration dictionary.
        api_config: API endpoint configuration dictionary.
        api_keys: Dictionary mapping API key identifiers to their values.
        
    Returns:
        bool: True if processing was successful, False otherwise.
    """
    global _server_timestamp
    
    try:
        # Get the API key identifier for this endpoint
        api_key_id = api_config.get('api_key', 'default')
        if api_key_id not in api_keys:
            logging.error("API key identifier '%s' not found for endpoint %s", 
                         api_key_id, api_config['name'])
            return False
            
        # Mask sensitive data in logs
        masked_config = {
            **api_config,
            'url': re.sub(r'key=[^&]+', 'key=***', api_config['url'])
        }
        logging.info("Processing endpoint: %s", masked_config['name'])
        logging.debug("Endpoint configuration: %s", 
                     {k: v for k, v in masked_config.items() if k != 'url'})
        
        # Fetch data from API
        data = tc_fetch_api_data(api_config["url"], api_key_id, api_keys)
        if not data:
            logging.error("Failed to fetch data for %s", api_config['name'])
            return False
            
        # For timestamp endpoint, store the timestamp for other endpoints
        if api_config['name'] == 'server_timestamp' and 'timestamp' in data:
            _server_timestamp = pd.to_datetime(data['timestamp'], unit='s')
            logging.info("Stored server timestamp: %s", _server_timestamp)
            
        # Process the data
        logging.info("Processing data for %s", api_config['name'])
        df = process_data(api_config["name"], data, _server_timestamp)
        if df.empty:
            logging.error("No data processed for %s", api_config['name'])
            return False
            
        # Upload to BigQuery with specified storage mode
        storage_mode = api_config.get("storage_mode", "append")  # Default to append if not specified
        logging.info("Uploading data to BigQuery table: %s (storage mode: %s)", 
                    api_config['table'], storage_mode)
        upload_to_bigquery(config, df, api_config["table"], storage_mode)
        
        logging.info("Successfully processed %s", api_config['name'])
        logging.info("Records processed: %d", len(df))
        logging.info("Columns: %d", len(df.columns))
        logging.info("Data types: %s", df.dtypes.value_counts().to_dict())
        
        return True
        
    except Exception as e:
        logging.error("Error processing %s: %s", api_config['name'], str(e))
        return False

def find_config_directory(directories: List[str]) -> Optional[str]:
    """Find the first valid configuration directory from a list of candidates.

    Args:
        directories: List of directory paths to search.

    Returns:
        str: Path to the first valid directory found, or None if no valid directory exists.
    """
    for directory in directories:
        if os.path.isdir(directory):
            return directory
    return None

def infer_bigquery_type(value: Any) -> str:
    """Infer the appropriate BigQuery data type for a given value.

    This function analyzes the input value and determines the most appropriate
    BigQuery data type. It handles special cases like timestamps and nested structures.

    Args:
        value: The value to analyze for type inference.

    Returns:
        str: The inferred BigQuery data type (e.g., 'STRING', 'INT64', 'TIMESTAMP', etc.).
    """
    # Handle None/NaN values
    if value is None:
        return "STRING"
    
    # Handle pandas NA values
    try:
        if pd.isna(value):
            return "STRING"
    except (ValueError, TypeError):
        pass
    
    if isinstance(value, bool):
        return "BOOLEAN"
        
    if isinstance(value, int):
        # Check if it might be a timestamp (Unix epoch)
        if 1000000000 <= value <= 2000000000:  # Reasonable range for Unix timestamps
            return "TIMESTAMP"
        return "INT64"
        
    if isinstance(value, float):
        return "FLOAT64"
        
    if isinstance(value, str):
        return "STRING"
        
    if isinstance(value, list):
        # Empty list defaults to STRING
        if not value:
            return "STRING"
        # Check if all elements are numeric
        if all(isinstance(x, (int, float)) for x in value):
            return "FLOAT64"
        return "STRING"
        
    if isinstance(value, dict):
        return "STRING"  # Will be converted to JSON string
        
    if isinstance(value, datetime):
        return "TIMESTAMP"
        
    return "STRING"

def convert_timestamp(value: Any) -> Optional[datetime]:
    """Convert a value to a timestamp if possible.

    This function attempts to convert various formats of timestamp data
    (Unix epoch, string dates, etc.) into Python datetime objects.

    Args:
        value: The value to attempt to convert to a timestamp.

    Returns:
        datetime: Converted timestamp, or None if conversion is not possible.
    """
    if pd.isna(value) or value is None:
        return None
        
    try:
        # If it's already a datetime, return it
        if isinstance(value, (datetime, pd.Timestamp)):
            return value
            
        # Handle Unix timestamps (as string or number)
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            timestamp = float(value)
            if 1000000000 <= timestamp <= 2000000000:  # Reasonable range for Unix timestamps
                return pd.to_datetime(timestamp, unit='s')
                
        # Try parsing as ISO format if it's a string
        if isinstance(value, str):
            try:
                return pd.to_datetime(value)
            except (ValueError, TypeError):
                return None
                
        return None
    except (ValueError, TypeError):
        return None

def process_data(api_name: str, data: Dict[str, Any], server_timestamp: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """Process and flatten API response data for BigQuery upload.

    This function handles:
    - Flattening nested JSON structures
    - Converting timestamps
    - Type inference and conversion
    - List expansion for faction crimes data
    - JSON string conversion for other list data
    - Schema preparation
    - Adding server timestamp to each record

    Args:
        api_name: Name of the API endpoint being processed.
        data: Raw API response data.
        server_timestamp: Optional server timestamp to add to each record.

    Returns:
        pd.DataFrame: Processed and flattened data ready for BigQuery upload.
    """
    logging.debug("Raw data before processing (first 500 chars): %s", 
                 json.dumps(data, indent=2)[:500])

    # Create empty DataFrame with server_timestamp as first column
    if server_timestamp is not None:
        df = pd.DataFrame({'server_timestamp': [server_timestamp]})
        logging.info("Added server timestamp: %s", server_timestamp)
    else:
        df = pd.DataFrame()
        logging.warning("No server timestamp provided")

    # Handle simple key-value responses (like timestamp endpoint)
    if len(data) == 1 and isinstance(list(data.values())[0], (int, float, str)):
        temp_df = pd.DataFrame([data])
    else:
        # Get the key where the main data is stored
        main_key = list(data.keys())[0]
        records = data[main_key]

        # First normalization to flatten top-level structure
        temp_df = pd.json_normalize(records, sep='_')

        # Handle list columns
        list_columns = [col for col in temp_df.columns 
                       if temp_df[col].apply(lambda x: isinstance(x, list)).any()]
        logging.info("List columns before processing: %s", list_columns)

        if api_name == "v2_faction_crimes" and list_columns:
            # For faction crimes, explode list columns and flatten their contents
            logging.info("Processing list columns for faction crimes data")
            
            # Save non-list columns
            non_list_columns = [col for col in temp_df.columns if col not in list_columns]
            base_df = temp_df[non_list_columns].copy()
            
            # Process each list column
            exploded_dfs = []
            for col in list_columns:
                logging.info("Processing list column: %s", col)
                # Create a DataFrame with the list column exploded
                exploded_df = temp_df.explode(col).reset_index()
                
                # If the exploded column contains dictionaries, normalize them
                if exploded_df[col].apply(lambda x: isinstance(x, dict)).any():
                    # Normalize the dictionaries and prefix with original column name
                    normalized = pd.json_normalize(exploded_df[col].tolist(), sep='_')
                    normalized.columns = [f"{col}_{c}" for c in normalized.columns]
                    
                    # Combine with the index to maintain relationships
                    exploded_df = pd.concat([
                        exploded_df['index'],
                        normalized
                    ], axis=1)
                else:
                    # If not dictionaries, just keep the values with original column name
                    exploded_df = exploded_df[['index', col]]
                    
                exploded_dfs.append(exploded_df)
            
            # Merge all exploded DataFrames back with the base DataFrame
            for exploded_df in exploded_dfs:
                base_df = base_df.reset_index().merge(
                    exploded_df,
                    left_on='index',
                    right_on='index',
                    how='left'
                ).drop('index', axis=1)
            
            temp_df = base_df
            logging.info("Completed processing list columns for faction crimes")
        else:
            # For other endpoints, convert lists to JSON strings
            for col in list_columns:
                logging.info("Converting list to JSON string in column: %s", col)
                temp_df[col] = temp_df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

        # Second normalization for any remaining nested structures
        temp_df = pd.json_normalize(temp_df.to_dict(orient='records'), sep='_')

    # If we have a server timestamp, expand the DataFrame to match the number of records
    if not df.empty:
        df = pd.concat([df] * len(temp_df), ignore_index=True)

    # Handle dictionary columns
    for col in temp_df.columns:
        if temp_df[col].apply(lambda x: isinstance(x, dict)).any():
            logging.info("Converting dictionary to JSON string in column: %s", col)
            temp_df[col] = temp_df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    # Convert timestamp columns
    timestamp_patterns = ['timestamp', 'time', 'date', 'until', 'at', 
                         'created', 'updated', 'executed', 'expired']
    timestamp_columns = [
        col for col in temp_df.columns 
        if any(pattern in col.lower() for pattern in timestamp_patterns)
    ]
    logging.info("Potential timestamp columns found: %s", timestamp_columns)
    
    for col in timestamp_columns:
        logging.info("Attempting timestamp conversion for column: %s", col)
        # Convert the column to timestamps, keeping track of which values converted successfully
        converted_series = temp_df[col].apply(convert_timestamp)
        
        # If any values were successfully converted to timestamps, use those
        # Otherwise, leave the column as is (likely STRING type)
        if not converted_series.dropna().empty:
            temp_df[col] = converted_series
            logging.info("Successfully converted column '%s' to TIMESTAMP", col)
        else:
            logging.info("Column '%s' contains no valid timestamps, keeping as STRING", col)

    # Infer and set appropriate data types
    for col in temp_df.columns:
        if temp_df[col].isna().all():
            continue
            
        sample_values = temp_df[col].dropna().head(100)
        if len(sample_values) == 0:
            continue
            
        inferred_type = infer_bigquery_type(sample_values.iloc[0])
        
        try:
            if inferred_type == "INT64":
                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce').astype('Int64')
            elif inferred_type == "FLOAT64":
                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')
            elif inferred_type == "BOOLEAN":
                temp_df[col] = temp_df[col].astype(bool)
            elif inferred_type == "TIMESTAMP":
                if not pd.api.types.is_datetime64_any_dtype(temp_df[col].dtype):
                    temp_df[col] = pd.to_datetime(temp_df[col], errors='ignore')
            logging.info("Converted column '%s' to %s", col, inferred_type)
        except Exception as e:
            logging.error("Error converting column '%s' to %s: %s", 
                        col, inferred_type, str(e))

    # Combine the server timestamp with the rest of the data
    if not df.empty:
        df = pd.concat([df, temp_df], axis=1)
    else:
        df = temp_df

    logging.info("Final DataFrame shape: %s", df.shape)
    logging.debug("Final DataFrame types:\n%s", df.dtypes)

    return df
