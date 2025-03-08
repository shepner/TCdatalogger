"""Common utilities for data processing and configuration management.

This module provides utilities for:
- Configuration directory management
- Data type inference and conversion
- Data flattening and processing
- Timestamp handling
- Logging setup and configuration
- API endpoint processing

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
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

from app.svcProviders.TornCity.TornCity import tc_fetch_api_data
from app.svcProviders.Google.Google import upload_to_bigquery

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
        "gcp_credentials_file": os.path.join(config_dir, "credentials.json"),
        "tc_api_key_file": os.path.join(config_dir, "TC_API_key.txt"),
        "tc_api_config_file": os.path.join(config_dir, "TC_API_config.json"),
    }
    
    # Verify all required files exist
    for key, filepath in config.items():
        if not os.path.exists(filepath):
            logging.error(f"Required configuration file not found: {filepath}")
            return None
            
    return config

def process_api_endpoint(config: Dict, api_config: Dict, tc_api_key: str) -> bool:
    """Process a single API endpoint and upload data to BigQuery.
    
    Args:
        config: Application configuration dictionary.
        api_config: API endpoint configuration dictionary.
        tc_api_key: Torn City API key.
        
    Returns:
        bool: True if processing was successful, False otherwise.
    """
    try:
        logging.info(f"Processing endpoint: {api_config['name']}")
        
        # Fetch data from API
        data = tc_fetch_api_data(api_config["url"], tc_api_key)
        if not data:
            logging.error(f"Failed to fetch data for {api_config['name']}")
            return False
            
        # Process the data
        logging.info(f"Processing data for {api_config['name']}")
        df = process_data(api_config["name"], data)
        if df.empty:
            logging.error(f"No data processed for {api_config['name']}")
            return False
            
        # Upload to BigQuery
        logging.info(f"Uploading data to BigQuery table: {api_config['table']}")
        upload_to_bigquery(config, df, api_config["table"])
        
        logging.info(f"Successfully processed {api_config['name']}")
        logging.info(f"Records processed: {len(df)}")
        logging.info(f"Columns: {len(df.columns)}")
        logging.info(f"Data types: {df.dtypes.value_counts().to_dict()}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing {api_config['name']}: {str(e)}")
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
    if pd.isna(value):
        return None
    try:
        if isinstance(value, str):
            value = int(value)
        if isinstance(value, (int, float)):
            # Check if it's a reasonable Unix timestamp
            if 1000000000 <= value <= 2000000000:
                return pd.to_datetime(value, unit='s')
        return value
    except (ValueError, TypeError):
        return value

def process_data(api_name: str, data: Dict[str, Any]) -> pd.DataFrame:
    """Process and flatten API response data for BigQuery upload.

    This function handles:
    - Flattening nested JSON structures
    - Converting timestamps
    - Type inference and conversion
    - List expansion
    - Schema preparation

    Args:
        api_name: Name of the API endpoint being processed.
        data: Raw API response data.

    Returns:
        pd.DataFrame: Processed and flattened data ready for BigQuery upload.

    Example:
        >>> data = {"members": [{"id": 1, "stats": {"strength": 100}}]}
        >>> df = process_data("faction_members", data)
        >>> print(df.columns)
        ['id', 'stats_strength']
    """
    logging.debug("Raw data before processing (first 500 chars): %s", 
                 json.dumps(data, indent=2)[:500])

    # Get the key where the main data is stored
    main_key = list(data.keys())[0]
    records = data[main_key]

    # First normalization to flatten top-level structure
    df = pd.json_normalize(records, sep='_')

    # Handle list columns
    list_columns = [col for col in df.columns 
                   if df[col].apply(lambda x: isinstance(x, list)).any()]
    logging.info("List columns before exploding: %s", list_columns)

    # Expand list columns while preserving relationships
    for col in list_columns:
        logging.info("Exploding list column: %s", col)
        df = df.explode(col)

    # Second normalization for any remaining nested structures
    df = pd.json_normalize(df.to_dict(orient='records'), sep='_')

    # Handle dictionary columns
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            logging.info("Converting dictionary to JSON string in column: %s", col)
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    # Convert timestamp columns
    timestamp_patterns = ['timestamp', 'time', 'date', 'until', 'at', 
                         'created', 'updated', 'executed', 'expired']
    timestamp_columns = [
        col for col in df.columns 
        if any(pattern in col.lower() for pattern in timestamp_patterns)
    ]
    logging.info("Potential timestamp columns found: %s", timestamp_columns)
    
    for col in timestamp_columns:
        logging.info("Attempting timestamp conversion for column: %s", col)
        df[col] = df[col].apply(convert_timestamp)

    # Infer and set appropriate data types
    for col in df.columns:
        if df[col].isna().all():
            continue
            
        sample_values = df[col].dropna().head(100)
        if len(sample_values) == 0:
            continue
            
        inferred_type = infer_bigquery_type(sample_values.iloc[0])
        
        try:
            if inferred_type == "INT64":
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif inferred_type == "FLOAT64":
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif inferred_type == "BOOLEAN":
                df[col] = df[col].astype(bool)
            elif inferred_type == "TIMESTAMP":
                if not pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                    df[col] = pd.to_datetime(df[col])
            logging.info("Converted column '%s' to %s", col, inferred_type)
        except Exception as e:
            logging.error("Error converting column '%s' to %s: %s", 
                        col, inferred_type, str(e))

    logging.info("Final DataFrame shape: %s", df.shape)
    logging.debug("Final DataFrame types:\n%s", df.dtypes)

    return df
