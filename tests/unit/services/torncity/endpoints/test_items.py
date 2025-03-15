"""Unit tests for Torn City items endpoint processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery

from app.services.torncity.endpoints.items import ItemsProcessor

class TestItemsProcessor:
    """Test suite for ItemsProcessor."""

    @pytest.fixture
    def processor(self, sample_config, mock_api_keys):
        """Create an ItemsProcessor instance for testing."""
        return ItemsProcessor(sample_config)

    @pytest.fixture
    def sample_items_data(self):
        """Sample items data for testing."""
        return {
            "1": {
                "name": "Flower",
                "description": "A beautiful flower",
                "effect": "No effect",
                "requirement": "None",
                "type": "Flower",
                "weapon_type": None,
                "buy_price": 100,
                "sell_price": 50,
                "market_value": 75,
                "circulation": 1000,
                "image": "flower.png",
                "timestamp": 1234567890
            },
            "2": {
                "name": "Baseball Bat",
                "description": "A wooden baseball bat",
                "effect": "+5 Attack",
                "requirement": "Level 1",
                "type": "Melee",
                "weapon_type": "Clubbing",
                "buy_price": 250,
                "sell_price": 100,
                "market_value": 200,
                "circulation": 5000,
                "image": "bat.png",
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

    def test_process_data_valid(self, processor, sample_items_data):
        """Test processing valid items data."""
        processed_data = processor.process_data({"data": sample_items_data})
        
        assert len(processed_data) == 2
        assert all(isinstance(item, dict) for item in processed_data)
        
        # Verify first item
        item1 = next(item for item in processed_data if item["item_id"] == 1)
        assert item1["name"] == "Flower"
        assert item1["description"] == "A beautiful flower"
        assert item1["effect"] == "No effect"
        assert item1["requirement"] == "None"
        assert item1["type"] == "Flower"
        assert item1["weapon_type"] is None
        assert item1["buy_price"] == 100
        assert item1["sell_price"] == 50
        assert item1["market_value"] == 75
        assert item1["circulation"] == 1000
        assert item1["image"] == "flower.png"
        assert item1["timestamp"] == 1234567890
        
        # Verify second item
        item2 = next(item for item in processed_data if item["item_id"] == 2)
        assert item2["name"] == "Baseball Bat"
        assert item2["description"] == "A wooden baseball bat"
        assert item2["effect"] == "+5 Attack"
        assert item2["requirement"] == "Level 1"
        assert item2["type"] == "Melee"
        assert item2["weapon_type"] == "Clubbing"
        assert item2["buy_price"] == 250
        assert item2["sell_price"] == 100
        assert item2["market_value"] == 200
        assert item2["circulation"] == 5000
        assert item2["image"] == "bat.png"
        assert item2["timestamp"] == 1234567890

    def test_process_data_empty(self, processor):
        """Test processing empty data."""
        processed_data = processor.process_data({"data": {}})
        assert len(processed_data) == 0

    def test_process_data_missing_fields(self, processor):
        """Test processing data with missing fields."""
        data = {
            "1": {
                "name": "Test Item",
                "type": "Misc",
                # Missing other fields
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": data})
        assert len(processed_data) == 1
        item = processed_data[0]
        
        assert item["item_id"] == 1
        assert item["name"] == "Test Item"
        assert item["type"] == "Misc"
        assert item["description"] is None
        assert item["effect"] is None
        assert item["requirement"] is None
        assert item["weapon_type"] is None
        assert item["buy_price"] is None
        assert item["sell_price"] is None
        assert item["market_value"] is None
        assert item["circulation"] is None
        assert item["image"] is None
        assert item["timestamp"] == 1234567890

    def test_process_data_invalid_types(self, processor):
        """Test processing data with invalid types."""
        data = {
            "1": {
                "name": 123,  # Should be string
                "buy_price": "100",  # Should be integer
                "type": True,  # Should be string
                "timestamp": "invalid"  # Should be integer
            }
        }
        
        with pytest.raises(ValueError):
            processor.process_data({"data": data})

    @patch("app.services.torncity.client.TornClient.fetch_data")
    @patch("app.services.google.bigquery.client.BigQueryClient.write_data")
    def test_run_end_to_end(self, mock_write, mock_fetch, processor, sample_items_data):
        """Test running the processor end-to-end."""
        mock_fetch.return_value = {"data": sample_items_data}
        
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

    def test_market_value_calculation(self, processor):
        """Test market value calculation with various price scenarios."""
        test_data = {
            "1": {
                "name": "Test Item 1",
                "buy_price": 100,
                "sell_price": 50,
                "market_value": None,  # Missing market value
                "timestamp": 1234567890
            },
            "2": {
                "name": "Test Item 2",
                "buy_price": None,  # Missing buy price
                "sell_price": None,  # Missing sell price
                "market_value": 200,
                "timestamp": 1234567890
            },
            "3": {
                "name": "Test Item 3",
                "buy_price": 0,  # Zero prices
                "sell_price": 0,
                "market_value": 0,
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": test_data})
        
        # Verify market value calculations
        item1 = next(item for item in processed_data if item["item_id"] == 1)
        assert item1["market_value"] is None  # Should use provided market value or None
        
        item2 = next(item for item in processed_data if item["item_id"] == 2)
        assert item2["market_value"] == 200  # Should use provided market value
        
        item3 = next(item for item in processed_data if item["item_id"] == 3)
        assert item3["market_value"] == 0  # Should handle zero values 