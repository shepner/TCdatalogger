"""Unit tests for Torn City API endpoint processors."""

from unittest.mock import Mock, patch, call
import json
from datetime import datetime, timedelta

import pytest
from google.cloud import bigquery

from app.services.torncity.processors import (
    UserProcessor,
    ItemsProcessor,
    CrimeProcessor,
    CurrencyProcessor,
    MembersProcessor
)
from app.services.torncity.exceptions import TornAPIError

class TestBaseProcessor:
    """Test suite for base processor functionality."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a processor instance for testing."""
        return UserProcessor(sample_config)

    @pytest.fixture
    def sample_data(self):
        """Provide sample API response data."""
        return {
            "level": 15,
            "gender": "Male",
            "player_id": 12345,
            "name": "TestUser",
            "status": {
                "state": "Okay",
                "description": "Test status"
            },
            "last_action": {
                "status": "Online",
                "timestamp": 1646956800
            }
        }

    def test_data_transformation(self, processor, sample_data):
        """Test data transformation from API response to BigQuery format."""
        transformed = processor.transform_data(sample_data)
        
        assert isinstance(transformed, list)
        assert len(transformed) == 1
        assert transformed[0]["player_id"] == 12345
        assert transformed[0]["name"] == "TestUser"
        assert "timestamp" in transformed[0]
        assert isinstance(transformed[0]["timestamp"], str)

    def test_schema_validation(self, processor, sample_data):
        """Test schema validation of transformed data."""
        transformed = processor.transform_data(sample_data)
        schema = processor.get_schema()
        
        # Verify all required fields are present
        required_fields = [
            field.name for field in schema 
            if field.mode == "REQUIRED"
        ]
        for field in required_fields:
            assert field in transformed[0]
        
        # Verify data types match schema
        for field in schema:
            if field.name in transformed[0]:
                value = transformed[0][field.name]
                if field.field_type == "STRING":
                    assert isinstance(value, str)
                elif field.field_type == "INTEGER":
                    assert isinstance(value, int)
                elif field.field_type == "FLOAT":
                    assert isinstance(value, float)
                elif field.field_type == "BOOLEAN":
                    assert isinstance(value, bool)
                elif field.field_type in ["DATETIME", "TIMESTAMP"]:
                    # Should be ISO format string
                    datetime.fromisoformat(value.replace('Z', '+00:00'))

class TestUserProcessor:
    """Test suite for user endpoint processor."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a UserProcessor instance."""
        return UserProcessor(sample_config)

    def test_user_data_processing(self, processor):
        """Test processing of user endpoint data."""
        sample_data = {
            "level": 15,
            "gender": "Male",
            "player_id": 12345,
            "name": "TestUser",
            "status": {
                "state": "Okay",
                "description": "Test status"
            },
            "last_action": {
                "status": "Online",
                "timestamp": 1646956800
            }
        }
        
        transformed = processor.transform_data(sample_data)
        assert transformed[0]["level"] == 15
        assert transformed[0]["gender"] == "Male"
        assert transformed[0]["status_state"] == "Okay"
        assert transformed[0]["last_action_status"] == "Online"

class TestItemsProcessor:
    """Test suite for items endpoint processor."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create an ItemsProcessor instance."""
        return ItemsProcessor(sample_config)

    def test_items_data_processing(self, processor):
        """Test processing of items endpoint data."""
        sample_data = {
            "123": {
                "name": "Test Item",
                "description": "A test item",
                "effect": "Test effect",
                "requirement": "Level 10",
                "type": "Primary",
                "weapon_type": "Melee",
                "buy_price": 1000,
                "sell_price": 800,
                "market_value": 950,
                "circulation": 1500
            }
        }
        
        transformed = processor.transform_data(sample_data)
        assert len(transformed) == 1
        assert transformed[0]["item_id"] == 123
        assert transformed[0]["name"] == "Test Item"
        assert transformed[0]["buy_price"] == 1000
        assert transformed[0]["market_value"] == 950

class TestCrimeProcessor:
    """Test suite for crime endpoint processor."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a CrimeProcessor instance."""
        return CrimeProcessor(sample_config)

    def test_crime_data_processing(self, processor):
        """Test processing of crime endpoint data."""
        sample_data = {
            "456": {
                "crime_id": 456,
                "crime_name": "Test Crime",
                "participants": ["12345", "67890"],
                "time_started": 1646956800,
                "time_completed": 1646960400,
                "initiated_by": "12345",
                "success": True,
                "money_gained": 5000
            }
        }
        
        transformed = processor.transform_data(sample_data)
        assert len(transformed) == 1
        assert transformed[0]["crime_id"] == 456
        assert transformed[0]["crime_name"] == "Test Crime"
        assert transformed[0]["success"] is True
        assert transformed[0]["money_gained"] == 5000

class TestCurrencyProcessor:
    """Test suite for currency endpoint processor."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a CurrencyProcessor instance."""
        return CurrencyProcessor(sample_config)

    def test_currency_data_processing(self, processor):
        """Test processing of currency endpoint data."""
        sample_data = {
            "items": {
                "123": {
                    "name": "Test Item",
                    "value": 1000,
                    "timestamp": 1646956800
                }
            },
            "points": {
                "buy": 45.5,
                "sell": 44.8,
                "total": 1000000,
                "timestamp": 1646956800
            }
        }
        
        transformed = processor.transform_data(sample_data)
        assert len(transformed) > 0
        # Verify item price data
        item_record = next(r for r in transformed if r.get("item_id") == 123)
        assert item_record["value"] == 1000
        # Verify points data
        points_record = next(r for r in transformed if r.get("type") == "points")
        assert points_record["buy_price"] == 45.5
        assert points_record["sell_price"] == 44.8

class TestMembersProcessor:
    """Test suite for members endpoint processor."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a MembersProcessor instance."""
        return MembersProcessor(sample_config)

    def test_members_data_processing(self, processor):
        """Test processing of members endpoint data."""
        sample_data = {
            "12345": {
                "name": "Test Member",
                "level": 20,
                "status": {
                    "state": "Okay",
                    "description": "Test status"
                },
                "faction": {
                    "position": "Member",
                    "faction_id": 789,
                    "days_in_faction": 30
                },
                "last_action": {
                    "status": "Offline",
                    "timestamp": 1646956800
                }
            }
        }
        
        transformed = processor.transform_data(sample_data)
        assert len(transformed) == 1
        assert transformed[0]["player_id"] == 12345
        assert transformed[0]["name"] == "Test Member"
        assert transformed[0]["level"] == 20
        assert transformed[0]["faction_position"] == "Member"
        assert transformed[0]["faction_id"] == 789

class TestProcessorErrorHandling:
    """Test suite for processor error handling."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a processor instance for testing."""
        return UserProcessor(sample_config)

    def test_missing_required_fields(self, processor):
        """Test handling of missing required fields."""
        invalid_data = {
            "name": "Test User"
            # Missing required fields like player_id
        }
        
        with pytest.raises(ValueError) as exc:
            processor.transform_data(invalid_data)
        assert "Missing required field" in str(exc.value)

    def test_invalid_data_types(self, processor):
        """Test handling of invalid data types."""
        invalid_data = {
            "player_id": "not_an_integer",  # Should be integer
            "name": "Test User",
            "level": "15"  # Should be integer
        }
        
        with pytest.raises(ValueError) as exc:
            processor.transform_data(invalid_data)
        assert "Invalid data type" in str(exc.value)

    def test_invalid_timestamp(self, processor):
        """Test handling of invalid timestamps."""
        invalid_data = {
            "player_id": 12345,
            "name": "Test User",
            "last_action": {
                "status": "Online",
                "timestamp": "invalid_timestamp"  # Should be integer
            }
        }
        
        with pytest.raises(ValueError) as exc:
            processor.transform_data(invalid_data)
        assert "Invalid timestamp" in str(exc.value)

    def test_empty_response_handling(self, processor):
        """Test handling of empty API responses."""
        with pytest.raises(ValueError) as exc:
            processor.transform_data({})
        assert "Empty response" in str(exc.value)

    def test_nested_data_handling(self, processor):
        """Test handling of deeply nested data structures."""
        nested_data = {
            "player_id": 12345,
            "name": "Test User",
            "inventory": {
                "items": {
                    "123": {
                        "name": "Test Item",
                        "quantity": 5,
                        "equipped": False
                    }
                }
            }
        }
        
        # Should handle nested structures without error
        transformed = processor.transform_data(nested_data)
        assert transformed[0]["player_id"] == 12345
        # Verify nested data is flattened appropriately
        assert "inventory_items" in transformed[0] or "items" in transformed[0] 