"""Unit tests for Torn City members endpoint processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery

from app.services.torncity.endpoints.members import MembersProcessor

class TestMembersProcessor:
    """Test suite for MembersProcessor."""

    @pytest.fixture
    def processor(self, sample_config, mock_api_keys):
        """Create a MembersProcessor instance for testing."""
        return MembersProcessor(sample_config)

    @pytest.fixture
    def sample_members_data(self):
        """Sample members data for testing."""
        return {
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
                "timestamp": 1234567890
            }
        }

    def test_init(self, processor):
        """Test processor initialization."""
        assert processor.endpoint_url == "https://api.torn.com/user/{API_KEY}"
        assert processor.api_key == "default"
        assert processor.table == "project.dataset.test_table"
        assert isinstance(processor.schema, list)
        assert len(processor.schema) > 0

    def test_process_data_valid(self, processor, sample_members_data):
        """Test processing valid members data."""
        processed_data = processor.process_data({"data": sample_members_data})
        
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
        assert member2["timestamp"] == 1234567890

    def test_process_data_empty(self, processor):
        """Test processing empty data."""
        processed_data = processor.process_data({"data": {}})
        assert len(processed_data) == 0

    def test_process_data_missing_fields(self, processor):
        """Test processing data with missing fields."""
        data = {
            "1": {
                "name": "TestUser1",
                "level": 10,
                # Missing status, last_action, faction, life
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": data})
        assert len(processed_data) == 1
        member = processed_data[0]
        
        assert member["player_id"] == 1
        assert member["name"] == "TestUser1"
        assert member["level"] == 10
        assert member["status"] is None
        assert member["status_description"] is None
        assert member["last_action"] is None
        assert member["last_action_timestamp"] is None
        assert member["faction_position"] is None
        assert member["faction_id"] is None
        assert member["life_current"] is None
        assert member["life_maximum"] is None
        assert member["timestamp"] == 1234567890

    def test_process_data_invalid_types(self, processor):
        """Test processing data with invalid types."""
        data = {
            "1": {
                "name": 123,  # Should be string
                "level": "10",  # Should be integer
                "status": {"state": True},  # Should be string
                "timestamp": "invalid"  # Should be integer
            }
        }
        
        with pytest.raises(ValueError):
            processor.process_data({"data": data})

    @patch("app.services.torncity.client.TornClient.fetch_data")
    @patch("app.services.google.bigquery.client.BigQueryClient.write_data")
    def test_run_end_to_end(self, mock_write, mock_fetch, processor, sample_members_data):
        """Test running the processor end-to-end."""
        mock_fetch.return_value = {"data": sample_members_data}
        
        processor.run()
        
        mock_fetch.assert_called_once()
        mock_write.assert_called_once()
        
        # Verify the processed data matches the schema
        written_data = mock_write.call_args[0][0]
        assert len(written_data) == 2
        assert all(isinstance(item, dict) for item in written_data)
        
        # Verify all required fields are present
        required_fields = {field.name for field in processor.schema if field.mode == "REQUIRED"}
        assert all(all(field in item for field in required_fields) for item in written_data) 