"""Unit tests for Torn City members endpoint processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

from app.services.torncity.endpoints.members import MembersEndpointProcessor
from app.services.torncity.exceptions import TornAPIRateLimitError, DataValidationError

@pytest.fixture
def processor(sample_config, mock_api_keys):
    """Create a MembersEndpointProcessor instance for testing."""
    return MembersEndpointProcessor(sample_config)

class TestMembersProcessor:
    """Test suite for MembersEndpointProcessor."""

    @pytest.fixture
    def sample_members_data(self):
        """Sample members data for testing."""
        return {
            "members": {
                "1": {
                    "name": "TestUser1",
                    "level": 10,
                    "status": {"state": "Okay", "description": ""},
                    "last_action": {"status": "Online", "timestamp": 1234567890},
                    "faction": {"position": "Member", "faction_id": 123},
                    "life": {"current": 100, "maximum": 100},
                    "timestamp": 1234567890
                },
                "2": {
                    "name": "TestUser2",
                    "level": 20,
                    "status": {"state": "Hospital", "description": "Hospitalized"},
                    "last_action": {"status": "Offline", "timestamp": 1234567880},
                    "faction": None,
                    "life": {"current": 50, "maximum": 100},
                    "timestamp": 1234567880
                }
            }
        }

    @pytest.fixture
    def mock_members_response(self):
        return {
            "fetched_at": "2024-03-16T09:32:31.281852",
            "members": {
                "12345": {
                    "name": "Test User",
                    "level": 50,
                    "status": {
                        "description": "Online",
                        "state": "online",
                        "until": 0
                    },
                    "last_action": {
                        "status": "Online",
                        "timestamp": 1710579151
                    },
                    "faction": {
                        "faction_id": "17991",
                        "position": "Member",
                        "days_in_faction": 365
                    },
                    "life": {
                        "current": 100,
                        "maximum": 100
                    }
                }
            }
        }

    @pytest.fixture
    def mock_torn_response(self):
        """Mock response from Torn API."""
        return {
            "data": {
                "members": {
                    "1": {
                        "name": "TestUser1",
                        "level": 10,
                        "status": {
                            "description": "",
                            "state": "Okay"
                        },
                        "last_action": {
                            "status": "Online",
                            "timestamp": 1234567890
                        },
                        "faction": {
                            "faction_id": 123,
                            "position": "Member",
                            "days_in_faction": 0
                        },
                        "life": {
                            "current": 100,
                            "maximum": 100
                        },
                        "timestamp": 1234567890
                    },
                    "2": {
                        "name": "TestUser2",
                        "level": 20,
                        "status": {
                            "description": "Hospitalized",
                            "state": "Hospital"
                        },
                        "last_action": {
                            "status": "Offline",
                            "timestamp": 1234567880
                        },
                        "life": {
                            "current": 50,
                            "maximum": 100
                        },
                        "timestamp": 1234567880
                    }
                }
            }
        }

    def test_init(self, mock_api_keys, tmp_path):
        """Test initialization of MembersProcessor."""
        config = {
            'gcp_project_id': 'test-project',
            'gcp_credentials_file': str(tmp_path / 'test-creds.json'),
            'dataset': 'test_dataset',
            'endpoint': 'user',
            'tc_api_key_file': mock_api_keys,
            'storage_mode': 'append',
            'selection': 'default'
        }
        processor = MembersEndpointProcessor(config)
        assert processor.endpoint_config['table'] == "test_dataset.members"

    def test_process_data_valid(self, processor, mock_torn_response):
        """Test processing valid members data."""
        processed_data = processor.process_data(mock_torn_response)
        
        assert len(processed_data) == 2
        assert all(isinstance(item, dict) for item in processed_data)
        
        # Verify first member
        member1 = next(item for item in processed_data if item["player_id"] == 1)
        assert member1["name"] == "TestUser1"
        assert member1["level"] == 10
        assert member1["status"] == "Okay"
        assert member1["status_description"] == ""
        assert member1["last_action"] == "Online"
        assert member1["last_action_timestamp"] == 1234567890
        assert member1["faction_position"] == "Member"
        assert member1["faction_id"] == 123
        assert member1["life_current"] == 100
        assert member1["life_maximum"] == 100
        assert member1["timestamp"] == 1234567890
        
        # Verify second member
        member2 = next(item for item in processed_data if item["player_id"] == 2)
        assert member2["name"] == "TestUser2"
        assert member2["level"] == 20
        assert member2["status"] == "Hospital"
        assert member2["status_description"] == "Hospitalized"
        assert member2["last_action"] == "Offline"
        assert member2["last_action_timestamp"] == 1234567880
        assert member2["faction_position"] is None
        assert member2["faction_id"] is None
        assert member2["life_current"] == 50
        assert member2["life_maximum"] == 100
        assert member2["timestamp"] == 1234567880

    def test_process_data_empty(self, processor):
        """Test processing empty data."""
        processed_data = processor.process_data({"data": {}})
        assert len(processed_data) == 0

    def test_process_data_missing_fields(self, processor):
        """Test processing data with missing fields."""
        data = {
            "members": {
                "1": {
                    "name": "TestUser1",
                    "level": 10,
                    # Missing status, last_action, life
                    "timestamp": 1234567890
                }
            }
        }
        
        processed_data = processor.process_data(data)
        assert len(processed_data) == 1
        
        # Verify basic fields
        assert processed_data.iloc[0]["id"] == 1
        assert processed_data.iloc[0]["name"] == "TestUser1"
        assert processed_data.iloc[0]["level"] == 10
        
        # Verify missing fields are None
        assert pd.isna(processed_data.iloc[0]["status_description"])
        assert pd.isna(processed_data.iloc[0]["status_details"])
        assert pd.isna(processed_data.iloc[0]["status_state"])
        assert pd.isna(processed_data.iloc[0]["status_until"])
        assert pd.isna(processed_data.iloc[0]["last_action_status"])
        assert pd.isna(processed_data.iloc[0]["last_action_timestamp"])
        assert pd.isna(processed_data.iloc[0]["last_action_relative"])
        assert pd.isna(processed_data.iloc[0]["position"])
        assert pd.isna(processed_data.iloc[0]["days_in_faction"])
        assert pd.isna(processed_data.iloc[0]["life_current"])
        assert pd.isna(processed_data.iloc[0]["life_maximum"])

    def test_process_data_invalid_types(self, processor):
        """Test handling of invalid data types."""
        invalid_data = {
            "members": {
                "1": {
                    "name": 123,  # Invalid: should be string
                    "level": "invalid",  # Invalid: should be integer
                    "status": {"state": True},  # Invalid: should be string
                    "last_action": {"status": "Online", "timestamp": "invalid"},  # Invalid: timestamp should be integer
                    "faction": {"position": 123, "faction_id": "invalid"},  # Invalid types
                    "life": {"current": "50", "maximum": True},  # Invalid types
                    "timestamp": "invalid"  # Invalid: should be integer
                }
            }
        }

        with pytest.raises(DataValidationError) as exc:
            processor.process_data({"data": invalid_data})

        assert "Invalid numeric value for level field: invalid" in str(exc.value)

    @patch("app.services.torncity.client.TornClient.fetch_data")
    @patch("app.services.google.bigquery.client.BigQueryClient.write_data")
    def test_run_end_to_end(self, mock_write, mock_fetch):
        """Test end-to-end run of the processor."""
        # Set up mock response
        mock_response = {
            "data": {
                "1": {
                    "name": "TestUser",
                    "level": 10,
                    "status": {"state": "Okay"},
                    "timestamp": 1710766800
                }
            }
        }
        mock_fetch.return_value = mock_response
        mock_write.return_value = None

        # Create and run processor
        processor = MembersEndpointProcessor(self.sample_config)
        result = processor.run()

        # Verify mocks were called correctly
        mock_fetch.assert_called_once()
        mock_write.assert_called_once()

        # Verify result
        assert result is not None
        assert len(result) == 1
        assert result[0]["name"] == "TestUser"
        assert result[0]["level"] == 10
        assert result[0]["status"] == "Okay"

    def test_run_end_to_end(self, processor):
        """Test end-to-end processing."""
        # Set up mock response
        mock_data = {
            "data": {
                "1": {
                    "name": "TestUser",
                    "level": 10,
                    "status": {"state": "Okay"},
                    "timestamp": 1710766800,
                    "member_id": 1,
                    "last_action": {
                        "status": "Online",
                        "timestamp": 1710766800
                    }
                }
            }
        }

        # Mock the fetch_data method
        with patch.object(processor.torn_client, 'fetch_data', return_value=mock_data):
            # Run processor
            result = processor.run()

            # Verify result
            assert result is not None
            assert len(result) == 1
            assert result[0]["name"] == "TestUser"
            assert result[0]["level"] == 10
            assert result[0]["status"] == "Okay"
            assert result[0]["timestamp"] == 1710766800

            # Test rate limit handling
            with patch.object(processor.torn_client, 'fetch_data', side_effect=TornAPIRateLimitError("Rate limit exceeded")):
                with pytest.raises(TornAPIRateLimitError):
                    processor.run()

@pytest.fixture
def sample_members_response():
    """Sample API response for members endpoint."""
    return {
        "members": {
            "123": {
                "name": "TestUser",
                "level": 50,
                "days_in_faction": 100,
                "position": "Member",
                "status": {
                    "description": "Online",
                    "details": "",
                    "state": "Online",
                    "until": 0
                },
                "last_action": {
                    "status": "Online",
                    "timestamp": 1647123456,
                    "relative": "1 minute ago"
                },
                "life": {
                    "current": 1000,
                    "maximum": 1000
                }
            }
        }
    }

def test_transform_data(processor, sample_members_response):
    """Test data transformation from API response to DataFrame."""
    df = processor.transform_data(sample_members_response)
    
    # Verify DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Verify required columns exist
    required_columns = [
        'server_timestamp',
        'id',
        'name',
        'level',
        'days_in_faction',
        'position',
        'status_description',
        'status_details',
        'status_state',
        'status_until',
        'last_action_status',
        'last_action_timestamp',
        'last_action_relative',
        'life_current',
        'life_maximum'
    ]
    for col in required_columns:
        assert col in df.columns
    
    # Verify data types
    assert df['id'].dtype == 'int64'
    assert df['level'].dtype == 'int64'
    assert df['days_in_faction'].dtype == 'int64'
    assert df['life_current'].dtype == 'int64'
    assert df['life_maximum'].dtype == 'int64'
    
    # Verify sample data values
    row = df.iloc[0]
    assert row['id'] == 123
    assert row['name'] == "TestUser"
    assert row['level'] == 50
    assert row['days_in_faction'] == 100
    assert row['position'] == "Member"
    assert row['status_description'] == "Online"
    assert row['life_current'] == 1000
    assert row['life_maximum'] == 1000

def test_transform_data_empty_response(processor):
    """Test handling of empty API response."""
    empty_response = {"members": {}}
    df = processor.transform_data(empty_response)
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_transform_data_missing_fields(processor):
    """Test handling of response with missing fields."""
    incomplete_response = {
        "members": {
            "123": {
                "name": "TestUser",
                "level": 50
                # Missing other fields
            }
        }
    }
    
    df = processor.transform_data(incomplete_response)
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.iloc[0]['name'] == "TestUser"
    assert df.iloc[0]['level'] == 50
    # Other fields should be null
    assert pd.isna(df.iloc[0]['days_in_faction']) 