import os
import pandas as pd
import json
import numpy as np
from typing import Dict, Any, List
from datetime import datetime

def find_config_directory(directories):
    for directory in directories:
        if os.path.isdir(directory):
            return directory
    return None

def infer_bigquery_type(value: Any) -> str:
    """Infer BigQuery data type from a value."""
    if pd.isna(value):
        return "STRING"
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
        if all(isinstance(x, (int, float)) for x in value):
            return "FLOAT64"
        return "STRING"
    if isinstance(value, dict):
        return "STRING"  # Will be converted to JSON
    if isinstance(value, datetime):
        return "TIMESTAMP"
    return "STRING"

def convert_timestamp(value: Any) -> Any:
    """Convert Unix timestamp to datetime object."""
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

def process_data(api_name: str, data: Dict) -> pd.DataFrame:
    """Flatten nested structures, expand lists, and prepare data for BigQuery."""
    print("\n🔹 Raw data before processing (first 500 chars):", json.dumps(data, indent=2)[:500])

    # Get the key where the main data is stored
    main_key = list(data.keys())[0]
    records = data[main_key]

    # First normalization to flatten top-level structure
    df = pd.json_normalize(records, sep='_')

    # Identify and handle list columns
    list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
    print("\n🔹 List columns before exploding:", list_columns)

    # Expand list columns while preserving relationships
    for col in list_columns:
        print(f"🔸 Exploding list column: {col}")
        df = df.explode(col)

    # Second normalization for any remaining nested structures
    df = pd.json_normalize(df.to_dict(orient='records'), sep='_')

    # Handle dictionary columns by converting to JSON strings
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            print(f"🔸 Converting dictionary to JSON string in column: {col}")
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    # First pass: Convert all potential timestamp columns
    # This includes both explicit timestamp fields and numeric fields that might be timestamps
    timestamp_patterns = ['timestamp', 'time', 'date', 'until', 'at', 'created', 'updated', 'executed', 'expired']
    timestamp_columns = [
        col for col in df.columns 
        if any(pattern in col.lower() for pattern in timestamp_patterns)
    ]
    print("\n🔹 Potential timestamp columns found:", timestamp_columns)
    
    for col in timestamp_columns:
        print(f"🔸 Attempting timestamp conversion for column: {col}")
        df[col] = df[col].apply(convert_timestamp)

    # Infer and set appropriate data types
    for col in df.columns:
        # Skip empty columns
        if df[col].isna().all():
            continue
            
        # Get sample of non-null values for type inference
        sample_values = df[col].dropna().head(100)
        
        # Skip if no valid samples
        if len(sample_values) == 0:
            continue
            
        # Infer type from sample values
        inferred_type = infer_bigquery_type(sample_values.iloc[0])
        
        try:
            if inferred_type == "INT64":
                # Try to preserve integers even with NaN values
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif inferred_type == "FLOAT64":
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif inferred_type == "BOOLEAN":
                df[col] = df[col].astype(bool)
            elif inferred_type == "TIMESTAMP":
                if not pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                    df[col] = pd.to_datetime(df[col])
            print(f"🔸 Converted column '{col}' to {inferred_type}")
        except Exception as e:
            print(f"⚠️ Error converting column '{col}' to {inferred_type}: {e}")

    print("\n🔹 Final DataFrame shape:", df.shape)
    print("🔹 Final DataFrame types:\n", df.dtypes)

    return df
