"""Unit tests for the crimes endpoint processor."""

import pytest
import pandas as pd
from datetime import datetime
from app.services.torncity.endpoints.crimes import CrimesEndpointProcessor

@pytest.fixture
def sample_crimes_response():
    """Sample API response for crimes endpoint."""
    return {
        "data": {
            "crimes": {
                "123456": {
                    "id": 123456,
                    "name": "Armed Robbery",
                    "difficulty": 3,
                    "status": "completed",
                    "created_at": 1647123456,
                    "planning_at": 1647123456,
                    "executed_at": 1647123556,
                    "ready_at": 1647123656,
                    "expired_at": 1647123756,
                    "slots": {
                        "position": "leader",
                        "item_requirement": {
                            "id": 789,
                            "is_reusable": True,
                            "is_available": True
                        },
                        "user_id": 12345,
                        "user_joined_at": 1647123456,
                        "user_progress": 100.0,
                        "success_chance": 85,
                        "crime_pass_rate": 90
                    },
                    "rewards": {
                        "money": 100000,
                        "respect": 10,
                        "items": [
                            {
                                "id": 456,
                                "quantity": 2
                            }
                        ],
                        "payout": {
                            "type": "percentage",
                            "percentage": 50,
                            "paid_by": 67890,
                            "paid_at": 1647123856
                        }
                    }
                }
            }
        }
    }

@pytest.fixture
def processor():
    """Create a crimes endpoint processor instance."""
    config = {
        "endpoint": "v2/faction/crimes",
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
        'id',
        'name',
        'difficulty',
        'status',
        'created_at',
        'planning_at',
        'executed_at',
        'ready_at',
        'expired_at',
        'slots_position',
        'slots_item_requirement_id',
        'slots_item_requirement_is_reusable',
        'slots_item_requirement_is_available',
        'slots_user_id',
        'slots_user_joined_at',
        'slots_user_progress',
        'slots_success_chance',
        'slots_crime_pass_rate',
        'rewards_money',
        'rewards_items_id',
        'rewards_items_quantity',
        'rewards_respect',
        'rewards_payout_type',
        'rewards_payout_percentage',
        'rewards_payout_paid_by',
        'rewards_payout_paid_at'
    ]
    for col in required_columns:
        assert col in df.columns
    
    # Verify data types
    assert df['id'].dtype == 'Int64'
    assert df['difficulty'].dtype == 'Int64'
    assert df['slots_success_chance'].dtype == 'Int64'
    assert df['slots_crime_pass_rate'].dtype == 'Int64'
    assert df['rewards_money'].dtype == 'Int64'
    assert df['rewards_respect'].dtype == 'Int64'
    assert df['slots_user_progress'].dtype == 'float64'
    assert df['slots_item_requirement_is_reusable'].dtype == 'boolean'
    assert df['slots_item_requirement_is_available'].dtype == 'boolean'
    
    # Verify sample data values
    row = df.iloc[0]
    assert row['id'] == 123456
    assert row['name'] == "Armed Robbery"
    assert row['difficulty'] == 3
    assert row['status'] == "completed"
    assert row['slots_position'] == "leader"
    assert row['slots_item_requirement_id'] == 789
    assert row['slots_item_requirement_is_reusable'] == True
    assert row['slots_item_requirement_is_available'] == True
    assert row['slots_user_id'] == 12345
    assert row['slots_user_progress'] == 100.0
    assert row['slots_success_chance'] == 85
    assert row['slots_crime_pass_rate'] == 90
    assert row['rewards_money'] == 100000
    assert row['rewards_respect'] == 10
    assert row['rewards_items_id'] == "456"
    assert row['rewards_items_quantity'] == "2"
    assert row['rewards_payout_type'] == "percentage"
    assert row['rewards_payout_percentage'] == 50
    assert row['rewards_payout_paid_by'] == 67890

def test_transform_data_empty_response(processor):
    """Test handling of empty API response."""
    empty_response = {"data": {"crimes": {}}}
    df = processor.transform_data(empty_response)
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_transform_data_missing_fields(processor):
    """Test handling of response with missing fields."""
    incomplete_response = {
        "data": {
            "crimes": {
                "123456": {
                    "id": 123456,
                    "name": "Armed Robbery",
                    "difficulty": 3,
                    "status": "completed",
                    "slots": {
                        "position": "leader",
                        "user_id": 12345
                    },
                    "rewards": {
                        "money": 100000
                    }
                }
            }
        }
    }
    
    df = processor.transform_data(incomplete_response)
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.iloc[0]['id'] == 123456
    assert df.iloc[0]['name'] == "Armed Robbery"
    assert df.iloc[0]['difficulty'] == 3
    assert df.iloc[0]['status'] == "completed"
    assert df.iloc[0]['slots_position'] == "leader"
    assert df.iloc[0]['slots_user_id'] == 12345
    assert df.iloc[0]['rewards_money'] == 100000
    # Other fields should be null or default values
    assert pd.isna(df.iloc[0]['slots_user_joined_at'])
    assert pd.isna(df.iloc[0]['slots_user_progress'])
    assert pd.isna(df.iloc[0]['rewards_respect'])
    assert pd.isna(df.iloc[0]['rewards_items_id'])
    assert pd.isna(df.iloc[0]['rewards_payout_type']) 