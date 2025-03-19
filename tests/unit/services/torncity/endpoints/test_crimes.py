"""Unit tests for Torn City crimes endpoint processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery

from app.services.torncity.endpoints.crimes import CrimesEndpointProcessor

class TestCrimesProcessor:
    """Test suite for CrimesEndpointProcessor."""

    @pytest.fixture
    def processor(self, sample_config, mock_api_keys):
        """Create a CrimesEndpointProcessor instance for testing."""
        return CrimesEndpointProcessor(sample_config)

    @pytest.fixture
    def sample_crimes_data(self):
        """Sample crimes data for testing."""
        return {
            "1": {
                "crime_name": "Shoplift",
                "crime_type": "Property",
                "participants": 1,
                "time": 30,
                "level_required": 1,
                "money_reward": {"minimum": 100, "maximum": 500},
                "respect_reward": {"minimum": 0.5, "maximum": 1.5},
                "nerve": 2,
                "success_rate": {"minimum": 50, "maximum": 95},
                "timestamp": 1234567890
            },
            "2": {
                "crime_name": "Bank Robbery",
                "crime_type": "Organized",
                "participants": 4,
                "time": 300,
                "level_required": 10,
                "money_reward": {"minimum": 10000, "maximum": 50000},
                "respect_reward": {"minimum": 5.0, "maximum": 15.0},
                "nerve": 10,
                "success_rate": {"minimum": 30, "maximum": 75},
                "timestamp": 1234567890
            }
        }

    def test_init(self, processor):
        """Test processor initialization."""
        assert processor.endpoint_url == "https://api.torn.com/torn/{API_KEY}"
        assert processor.api_key == "default"
        assert processor.table == "project.dataset.test_table"
        assert isinstance(processor.schema, list)
        assert len(processor.schema) > 0

    def test_process_data_valid(self, processor, sample_crimes_data):
        """Test processing valid crimes data."""
        processed_data = processor.process_data({"data": sample_crimes_data})
        
        assert len(processed_data) == 2
        assert all(isinstance(crime, dict) for crime in processed_data)
        
        # Verify first crime
        crime1 = next(crime for crime in processed_data if crime["crime_id"] == 1)
        assert crime1["crime_name"] == "Shoplift"
        assert crime1["crime_type"] == "Property"
        assert crime1["participants"] == 1
        assert crime1["time"] == 30
        assert crime1["level_required"] == 1
        assert crime1["money_reward_min"] == 100
        assert crime1["money_reward_max"] == 500
        assert crime1["respect_reward_min"] == 0.5
        assert crime1["respect_reward_max"] == 1.5
        assert crime1["nerve"] == 2
        assert crime1["success_rate_min"] == 50
        assert crime1["success_rate_max"] == 95
        assert crime1["timestamp"] == 1234567890
        
        # Verify second crime
        crime2 = next(crime for crime in processed_data if crime["crime_id"] == 2)
        assert crime2["crime_name"] == "Bank Robbery"
        assert crime2["crime_type"] == "Organized"
        assert crime2["participants"] == 4
        assert crime2["time"] == 300
        assert crime2["level_required"] == 10
        assert crime2["money_reward_min"] == 10000
        assert crime2["money_reward_max"] == 50000
        assert crime2["respect_reward_min"] == 5.0
        assert crime2["respect_reward_max"] == 15.0
        assert crime2["nerve"] == 10
        assert crime2["success_rate_min"] == 30
        assert crime2["success_rate_max"] == 75
        assert crime2["timestamp"] == 1234567890

    def test_process_data_empty(self, processor):
        """Test processing empty data."""
        processed_data = processor.process_data({"data": {}})
        assert len(processed_data) == 0

    def test_process_data_missing_fields(self, processor):
        """Test processing data with missing fields."""
        data = {
            "1": {
                "crime_name": "Test Crime",
                "crime_type": "Test",
                # Missing other fields
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": data})
        assert len(processed_data) == 1
        crime = processed_data[0]
        
        assert crime["crime_id"] == 1
        assert crime["crime_name"] == "Test Crime"
        assert crime["crime_type"] == "Test"
        assert crime["participants"] is None
        assert crime["time"] is None
        assert crime["level_required"] is None
        assert crime["money_reward_min"] is None
        assert crime["money_reward_max"] is None
        assert crime["respect_reward_min"] is None
        assert crime["respect_reward_max"] is None
        assert crime["nerve"] is None
        assert crime["success_rate_min"] is None
        assert crime["success_rate_max"] is None
        assert crime["timestamp"] == 1234567890

    def test_process_data_invalid_types(self, processor):
        """Test processing data with invalid types."""
        data = {
            "1": {
                "crime_name": 123,  # Should be string
                "participants": "1",  # Should be integer
                "money_reward": {"minimum": "100"},  # Should be integer
                "timestamp": "invalid"  # Should be integer
            }
        }
        
        with pytest.raises(ValueError):
            processor.process_data({"data": data})

    @patch("app.services.torncity.client.TornClient.fetch_data")
    @patch("app.services.google.bigquery.client.BigQueryClient.write_data")
    def test_run_end_to_end(self, mock_write, mock_fetch, processor, sample_crimes_data):
        """Test running the processor end-to-end."""
        mock_fetch.return_value = {"data": sample_crimes_data}
        
        processor.run()
        
        mock_fetch.assert_called_once()
        mock_write.assert_called_once()
        
        # Verify the processed data matches the schema
        written_data = mock_write.call_args[0][0]
        assert len(written_data) == 2
        assert all(isinstance(crime, dict) for crime in written_data)
        
        # Verify all required fields are present
        required_fields = {field.name for field in processor.schema if field.mode == "REQUIRED"}
        assert all(all(field in crime for field in required_fields) for crime in written_data)

    def test_reward_calculations(self, processor):
        """Test reward calculations with various scenarios."""
        test_data = {
            "1": {
                "crime_name": "Test Crime 1",
                "money_reward": {"minimum": 100, "maximum": 500},
                "respect_reward": None,  # Missing respect reward
                "timestamp": 1234567890
            },
            "2": {
                "crime_name": "Test Crime 2",
                "money_reward": None,  # Missing money reward
                "respect_reward": {"minimum": 1.0, "maximum": 2.0},
                "timestamp": 1234567890
            },
            "3": {
                "crime_name": "Test Crime 3",
                "money_reward": {"minimum": 0, "maximum": 0},  # Zero rewards
                "respect_reward": {"minimum": 0, "maximum": 0},
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": test_data})
        
        # Verify reward calculations
        crime1 = next(crime for crime in processed_data if crime["crime_id"] == 1)
        assert crime1["money_reward_min"] == 100
        assert crime1["money_reward_max"] == 500
        assert crime1["respect_reward_min"] is None
        assert crime1["respect_reward_max"] is None
        
        crime2 = next(crime for crime in processed_data if crime["crime_id"] == 2)
        assert crime2["money_reward_min"] is None
        assert crime2["money_reward_max"] is None
        assert crime2["respect_reward_min"] == 1.0
        assert crime2["respect_reward_max"] == 2.0
        
        crime3 = next(crime for crime in processed_data if crime["crime_id"] == 3)
        assert crime3["money_reward_min"] == 0
        assert crime3["money_reward_max"] == 0
        assert crime3["respect_reward_min"] == 0
        assert crime3["respect_reward_max"] == 0 