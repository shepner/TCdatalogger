"""Test suite for data processing functionality.

This module contains tests for:
- Data flattening
- Type inference
- Timestamp handling
- List expansion
- Schema generation
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Any

import pytest
import pandas as pd
import numpy as np

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.common.common import process_data, infer_bigquery_type, convert_timestamp


@pytest.fixture
def sample_crime_data() -> Dict[str, Any]:
    """Provide sample crime data for testing."""
    return {
        "crimes": [
            {
                "id": 1,
                "name": "Test Crime",
                "difficulty": 1,
                "status": "Successful",
                "created_at": 1738695616,
                "slots": [
                    {
                        "position": "Looter",
                        "item_requirement": {
                            "id": 568,
                            "is_reusable": True,
                            "is_available": True
                        }
                    }
                ],
                "rewards": {
                    "money": 1000,
                    "respect": 10
                }
            }
        ]
    }


@pytest.fixture
def sample_member_data() -> Dict[str, Any]:
    """Provide sample member data for testing."""
    return {
        "members": [
            {
                "id": 1,
                "name": "Test Member",
                "level": 10,
                "days_in_faction": 100,
                "last_action": {
                    "status": "Offline",
                    "timestamp": 1741335699,
                    "relative": "11 hours ago"
                },
                "status": {
                    "description": "Okay",
                    "details": None,
                    "state": "Okay",
                    "until": None
                }
            }
        ]
    }


def test_infer_bigquery_type():
    """Test type inference for BigQuery schema generation."""
    assert infer_bigquery_type(1) == "INT64"
    assert infer_bigquery_type(1.0) == "FLOAT64"
    assert infer_bigquery_type("test") == "STRING"
    assert infer_bigquery_type(True) == "BOOLEAN"
    assert infer_bigquery_type(None) == "STRING"
    assert infer_bigquery_type([1, 2, 3]) == "FLOAT64"
    assert infer_bigquery_type({"key": "value"}) == "STRING"
    assert infer_bigquery_type(datetime.now()) == "TIMESTAMP"


def test_convert_timestamp():
    """Test timestamp conversion functionality."""
    # Test Unix timestamp
    assert convert_timestamp(1738695616) is not None
    assert isinstance(convert_timestamp(1738695616), pd.Timestamp)
    
    # Test invalid values
    assert convert_timestamp(None) is None
    assert convert_timestamp("invalid") == "invalid"
    assert convert_timestamp(0) == 0  # Too early to be a valid timestamp


def test_process_crime_data(sample_crime_data):
    """Test processing of crime data."""
    df = process_data("crimes", sample_crime_data)
    
    # Check basic DataFrame properties
    assert not df.empty
    assert "id" in df.columns
    assert "created_at" in df.columns
    assert "slots_position" in df.columns
    
    # Check data types
    assert pd.api.types.is_integer_dtype(df["id"].dtype)
    assert pd.api.types.is_datetime64_dtype(df["created_at"].dtype)
    assert pd.api.types.is_string_dtype(df["slots_position"].dtype)
    
    # Check flattened nested structures
    assert "slots_item_requirement_id" in df.columns
    assert "slots_item_requirement_is_reusable" in df.columns


def test_process_member_data(sample_member_data):
    """Test processing of member data."""
    df = process_data("members", sample_member_data)
    
    # Check basic DataFrame properties
    assert not df.empty
    assert "id" in df.columns
    assert "last_action_timestamp" in df.columns
    
    # Check data types
    assert pd.api.types.is_integer_dtype(df["id"].dtype)
    assert pd.api.types.is_datetime64_dtype(df["last_action_timestamp"].dtype)
    assert pd.api.types.is_string_dtype(df["last_action_status"].dtype)
    
    # Check values
    assert df["id"].iloc[0] == 1
    assert df["name"].iloc[0] == "Test Member"
    assert df["level"].iloc[0] == 10


def test_process_data_empty():
    """Test handling of empty data."""
    empty_data = {"test": []}
    df = process_data("test", empty_data)
    assert df.empty


def test_process_data_invalid():
    """Test handling of invalid data structures."""
    with pytest.raises(Exception):
        process_data("test", {})  # Empty dict
    
    with pytest.raises(Exception):
        process_data("test", {"test": None})  # None value
        
    with pytest.raises(Exception):
        process_data("test", {"test": "not a list"})  # Not a list 