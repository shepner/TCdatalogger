"""Unit tests for the crimes endpoint processor."""

import pytest
import pandas as pd
from datetime import datetime
from app.services.torncity.endpoints.crimes import CrimesEndpointProcessor

@pytest.fixture
def sample_crimes_response():
    """Sample API response for crimes endpoint."""
    return {
        "crimes": {
            "123456": {
                "crime_id": 123456,
                "crime_name": "Armed Robbery",
                "participants": [
                    {
                        "id": 789,
                        "name": "TestCriminal",
                        "rank": "Mastermind",
                        "status": "Success",
                        "share": 50000
                    }
                ],
                "time_started": 1647123456,
                "time_completed": 1647123556,
                "time_ready": 1647123656,
                "initiated": 1,
                "success": 1,
                "money_gained": 100000,
                "respect_gained": 10,
                "details": "Successfully robbed a bank"
            }
        }
    }

@pytest.fixture
def processor():
    """Create a crimes endpoint processor instance."""
    config = {
        "endpoint": "faction/crimes",
        "table": "project.dataset.crimes",
        "frequency": "PT15M"
    }
    return CrimesEndpointProcessor(config)

def test_transform_data(processor, sample_crimes_response):
    """Test data transformation from API response to DataFrame."""
    df = processor.transform_data(sample_crimes_response)
    
    # Verify DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Verify required columns exist
    required_columns = [
        'server_timestamp',
        'crime_id',
        'crime_name',
        'time_started',
        'time_completed',
        'time_ready',
        'initiated',
        'success',
        'money_gained',
        'respect_gained',
        'details',
        'participant_id',
        'participant_name',
        'participant_rank',
        'participant_status',
        'participant_share'
    ]
    for col in required_columns:
        assert col in df.columns
    
    # Verify data types
    assert df['crime_id'].dtype == 'int64'
    assert df['time_started'].dtype == 'int64'
    assert df['time_completed'].dtype == 'int64'
    assert df['time_ready'].dtype == 'int64'
    assert df['initiated'].dtype == 'int64'
    assert df['success'].dtype == 'int64'
    assert df['money_gained'].dtype == 'int64'
    assert df['respect_gained'].dtype == 'int64'
    
    # Verify sample data values
    row = df.iloc[0]
    assert row['crime_id'] == 123456
    assert row['crime_name'] == "Armed Robbery"
    assert row['participant_id'] == 789
    assert row['participant_name'] == "TestCriminal"
    assert row['participant_rank'] == "Mastermind"
    assert row['participant_status'] == "Success"
    assert row['participant_share'] == 50000
    assert row['money_gained'] == 100000
    assert row['respect_gained'] == 10

def test_transform_data_empty_response(processor):
    """Test handling of empty API response."""
    empty_response = {"crimes": {}}
    df = processor.transform_data(empty_response)
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_transform_data_missing_fields(processor):
    """Test handling of response with missing fields."""
    incomplete_response = {
        "crimes": {
            "123456": {
                "crime_id": 123456,
                "crime_name": "Armed Robbery",
                "participants": [
                    {
                        "id": 789,
                        "name": "TestCriminal"
                        # Missing other participant fields
                    }
                ],
                "time_started": 1647123456
                # Missing other fields
            }
        }
    }
    
    df = processor.transform_data(incomplete_response)
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.iloc[0]['crime_id'] == 123456
    assert df.iloc[0]['crime_name'] == "Armed Robbery"
    assert df.iloc[0]['participant_id'] == 789
    assert df.iloc[0]['participant_name'] == "TestCriminal"
    # Other fields should be null
    assert pd.isna(df.iloc[0]['participant_rank'])
    assert pd.isna(df.iloc[0]['time_completed']) 