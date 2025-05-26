"""Unit tests for the crimes endpoint processor."""

import pytest
import pandas as pd
from datetime import datetime
from app.services.torncity.endpoints.crimes import CrimesEndpointProcessor
import json
import os
import pathlib

@pytest.fixture
def sample_crimes_response():
    """Sample API response for crimes endpoint."""
    return {
        "crimes": [
            {
                "id": 123456,
                "name": "Armed Robbery",
                "difficulty": 3,
                "status": "completed",
                "created_at": 1647123456,
                "planning_at": 1647123456,
                "executed_at": 1647123556,
                "ready_at": 1647123656,
                "expired_at": 1647123756,
                "slots": [
                    {
                        "position": "leader",
                        "item_requirement": {
                            "id": 789,
                            "is_reusable": True,
                            "is_available": True
                        },
                        "user": {
                            "id": 12345,
                            "joined_at": 1647123456,
                            "progress": 100.0
                        },
                        "checkpoint_pass_rate": 90
                    }
                ],
                "rewards": {
                    "money": 100000,
                    "respect": 10,
                    "items": [
                        {"id": 456, "quantity": 2}
                    ],
                    "payout": {
                        "type": "percentage",
                        "percentage": 50,
                        "paid_by": 67890,
                        "paid_at": 1647123856
                    }
                }
            }
        ]
    }

@pytest.fixture
def processor():
    """Create a crimes endpoint processor instance."""
    config = {
        "endpoint": "v2/faction/crimes",
        "table": "project.dataset.crimes",
        "frequency": "PT15M",
        "gcp_credentials_file": "config/credentials.json.example",
        "tc_api_key_file": "config/TC_API_key.json"
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
    assert row['slots_crime_pass_rate'] == 90
    assert row['rewards_money'] == 100000
    assert row['rewards_respect'] == 10
    assert row['rewards_items_id'] == 456
    assert row['rewards_items_quantity'] == 2
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
        "crimes": [
            {
                "id": 123456,
                "name": "Armed Robbery",
                "difficulty": 3,
                "status": "completed",
                "slots": [
                    {
                        "position": "leader",
                        "user": {"id": 12345}
                    }
                ],
                "rewards": {"money": 100000}
            }
        ]
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
    # Other fields should be default values
    assert pd.isna(df.iloc[0]['slots_user_joined_at'])
    assert df.iloc[0]['slots_user_progress'] == 0.0
    assert df.iloc[0]['slots_crime_pass_rate'] == 0
    assert pd.isna(df.iloc[0]['rewards_respect'])
    assert pd.isna(df.iloc[0]['rewards_items_id'])
    assert df.iloc[0]['slots_item_requirement_is_reusable'] == False
    assert df.iloc[0]['slots_item_requirement_is_available'] == False
    assert df.iloc[0]['rewards_payout_type'] == ""

def test_transform_data_with_real_json(processor):
    """Test transform_data with the real crimes.json file and check slots_user_id population."""
    crimes_json_path = pathlib.Path(__file__).parent.parent.parent.parent.parent / 'fixtures' / 'crimes.json'
    with open(crimes_json_path, 'r') as f:
        crimes_data = json.load(f)
    # Wrap as expected by transform_data if needed
    if isinstance(crimes_data, dict) and 'crimes' not in crimes_data:
        data = {"crimes": list(crimes_data.values())}
    else:
        data = crimes_data
    df = processor.transform_data(data)
    # Check that slots_user_id is correct for a few known rows
    # Example: first crime, first slot
    first_crime = data['crimes'][0]
    first_slot = first_crime['slots'][0] if 'slots' in first_crime and first_crime['slots'] else None
    expected_user_id = first_slot['user']['id'] if first_slot and 'user' in first_slot and first_slot['user'] else None
    if first_slot:
        row = df[(df['id'] == first_crime['id']) & (df['slots_position'] == first_slot['position'])].iloc[0]
        assert row['slots_user_id'] == expected_user_id
    # Check a slot with user=None (should be pd.NA or None)
    for crime in data['crimes']:
        for slot in crime.get('slots', []):
            if slot.get('user') is None:
                row = df[(df['id'] == crime['id']) & (df['slots_position'] == slot['position'])].iloc[0]
                assert pd.isna(row['slots_user_id']) or row['slots_user_id'] is None
                break
        else:
            continue
        break

def test_crimes_processor_slots_user_id(monkeypatch, tmp_path):
    """Integration-style test: ensure slots_user_id is present and correct before upload."""
    import pandas as pd
    from app.services.torncity.endpoints.crimes import CrimesEndpointProcessor

    # Sample API response with multiple slots and users
    sample_response = {
        "crimes": [
            {
                "id": 1,
                "name": "Test Crime",
                "difficulty": 2,
                "status": "completed",
                "created_at": 1700000000,
                "slots": [
                    {"position": "leader", "user": {"id": 111, "joined_at": 1700000000, "progress": 100}},
                    {"position": "hacker", "user": {"id": 222, "joined_at": 1700000100, "progress": 80}},
                    {"position": "driver", "user": None}
                ],
                "rewards": {"money": 5000}
            }
        ]
    }

    # Minimal config
    config = {
        "endpoint": "v2/faction/crimes",
        "table": "project.dataset.crimes",
        "frequency": "PT15M",
        "gcp_credentials_file": str(tmp_path / "credentials.json"),
        "tc_api_key_file": str(tmp_path / "TC_API_key.json")
    }
    processor = CrimesEndpointProcessor(config)

    # Patch upload to BigQuery to capture the DataFrame
    uploaded = {}
    def fake_upload_dataframe(df, table_id, write_disposition=None):
        uploaded['df'] = df.copy()
        uploaded['table_id'] = table_id
    processor.bq_client.upload_dataframe = fake_upload_dataframe

    # Patch fetch_data to return our sample response
    processor.fetch_data = lambda: sample_response

    # Patch run to use our fetch_data and upload
    def fake_run():
        data = processor.fetch_data()
        df = processor.transform_data(data)
        # Simulate upload
        processor.bq_client.upload_dataframe(df, config['table'])
    processor.run = fake_run

    # Run the processor
    processor.run()

    # Check the uploaded DataFrame
    df = uploaded['df']
    assert 'slots_user_id' in df.columns
    # There should be 3 rows (one per slot)
    assert len(df) == 3
    # Check values for slots_user_id
    assert df.loc[df['slots_position'] == 'leader', 'slots_user_id'].iloc[0] == 111
    assert df.loc[df['slots_position'] == 'hacker', 'slots_user_id'].iloc[0] == 222
    # For the slot with user=None, should be pd.NA or None
    driver_val = df.loc[df['slots_position'] == 'driver', 'slots_user_id'].iloc[0]
    assert pd.isna(driver_val) or driver_val is None 