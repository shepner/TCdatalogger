"""Unit tests for Torn City currency endpoint processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery
import pandas as pd

from app.services.torncity.endpoints.currency import CurrencyEndpointProcessor

class TestCurrencyProcessor:
    """Test suite for CurrencyEndpointProcessor."""

    @pytest.fixture
    def processor(self, sample_config, mock_api_keys):
        """Create a CurrencyEndpointProcessor instance for testing."""
        # Use base currency endpoint config
        base_config = sample_config.copy()
        base_config.pop('endpoint_config', None)  # Remove faction endpoint config
        return CurrencyEndpointProcessor(base_config)

    @pytest.fixture
    def sample_currency_data(self):
        """Sample currency data for testing."""
        return {
            "points": {
                "buy": 45000,
                "sell": 44500,
                "total": 1000000,
                "timestamp": 1234567890
            },
            "items": {
                "1": {
                    "name": "Swiss Franc",
                    "value": 1.05,
                    "timestamp": 1234567890
                }
            }
        }

    def test_init(self, processor):
        """Test processor initialization."""
        assert processor.endpoint_config['url'] == "https://api.torn.com/torn/{API_KEY}?selections=currency"
        assert processor.endpoint_config['api_key'] == "default"
        assert processor.endpoint_config['table'].endswith(".currency")
        assert isinstance(processor.get_schema(), list)
        assert len(processor.get_schema()) > 0
        assert not processor.is_faction_endpoint

    def test_process_data_valid(self, processor, sample_currency_data):
        """Test processing valid currency data."""
        df = processor.transform_data(sample_currency_data)
        
        assert not df.empty
        assert len(df) == 2  # One for points, one for item
        
        # Verify points data
        points_row = df[df['currency_id'] == 1].iloc[0]
        assert points_row['name'] == "Points"
        assert points_row['buy_price'] == 45000.0
        assert points_row['sell_price'] == 44500.0
        assert points_row['circulation'] == 1000000
        assert not pd.isna(points_row['timestamp'])
        
        # Verify item data
        item_row = df[df['currency_id'] == 1].iloc[0]
        assert item_row['name'] == "Swiss Franc"
        assert item_row['buy_price'] == 0.0  # Items don't have buy prices
        assert item_row['sell_price'] == 1.05
        assert item_row['circulation'] == 0  # Items don't have circulation data
        assert not pd.isna(item_row['timestamp'])

    def test_process_data_empty(self, processor):
        """Test processing empty data."""
        df = processor.transform_data({})
        assert df.empty

    def test_process_data_missing_fields(self, processor):
        """Test processing data with missing fields."""
        data = {
            "points": {
                "name": "Points",
                # Missing other fields
                "timestamp": 1234567890
            }
        }
        
        df = processor.transform_data(data)
        assert not df.empty
        assert len(df) == 1
        
        row = df.iloc[0]
        assert row['currency_id'] == 1
        assert row['name'] == "Points"
        assert row['buy_price'] == 0.0
        assert row['sell_price'] == 0.0
        assert row['circulation'] == 0
        assert not pd.isna(row['timestamp'])

    def test_process_data_invalid_types(self, processor):
        """Test processing data with invalid types."""
        data = {
            "points": {
                "buy": "not_a_number",
                "sell": "invalid",
                "total": "error",
                "timestamp": "invalid"
            }
        }
        
        df = processor.transform_data(data)
        assert not df.empty
        assert len(df) == 1
        
        row = df.iloc[0]
        assert row['currency_id'] == 1
        assert row['name'] == "Points"
        assert pd.isna(row['buy_price'])
        assert pd.isna(row['sell_price'])
        assert pd.isna(row['circulation'])
        assert not pd.isna(row['timestamp'])  # Should use current timestamp as fallback

    @patch("app.services.torncity.client.TornClient.fetch_data")
    @patch("app.services.google.bigquery.client.BigQueryClient.write_data")
    def test_run_end_to_end(self, mock_write, mock_fetch, processor, sample_currency_data):
        """Test running the processor end-to-end."""
        mock_fetch.return_value = sample_currency_data
        
        processor.run()
        
        mock_fetch.assert_called_once()
        mock_write.assert_called_once()
        
        # Verify the processed data matches the schema
        written_data = mock_write.call_args[0][0]
        assert len(written_data) == 2
        assert all(isinstance(currency, dict) for currency in written_data)
        
        # Verify all required fields are present
        required_fields = {field.name for field in processor.get_schema() if field.mode == "REQUIRED"}
        assert all(all(field in currency for field in required_fields) for currency in written_data)

    def test_price_calculations(self, processor):
        """Test price calculations with various scenarios."""
        test_data = {
            "points": {
                "buy": 100.0,
                "sell": 90.0,
                "total": 1000000,
                "timestamp": 1234567890
            },
            "items": {
                "1": {
                    "name": "Test Item 1",
                    "value": 95.0,
                    "timestamp": 1234567890
                },
                "2": {
                    "name": "Test Item 2",
                    "value": 0.0,
                    "timestamp": 1234567890
                }
            }
        }
        
        df = processor.transform_data(test_data)
        assert not df.empty
        assert len(df) == 3  # Points + 2 items
        
        # Verify points data
        points_row = df[df['currency_id'] == 1].iloc[0]
        assert points_row['buy_price'] == 100.0
        assert points_row['sell_price'] == 90.0
        assert points_row['circulation'] == 1000000
        
        # Verify first item
        item1_row = df[df['name'] == "Test Item 1"].iloc[0]
        assert item1_row['buy_price'] == 0.0
        assert item1_row['sell_price'] == 95.0
        assert item1_row['circulation'] == 0
        
        # Verify second item
        item2_row = df[df['name'] == "Test Item 2"].iloc[0]
        assert item2_row['buy_price'] == 0.0
        assert item2_row['sell_price'] == 0.0
        assert item2_row['circulation'] == 0

    def test_decimal_precision(self, processor):
        """Test handling of decimal values."""
        test_data = {
            "points": {
                "buy": 1.23456789,  # More than 2 decimal places
                "sell": 1.23456789,
                "total": 1000000,
                "timestamp": 1234567890
            }
        }
        
        df = processor.transform_data(test_data)
        assert not df.empty
        
        row = df.iloc[0]
        # Verify values are rounded to 2 decimal places
        assert row['buy_price'] == 1.23
        assert row['sell_price'] == 1.23

    def test_timestamp_handling(self, processor):
        """Test handling of timestamps."""
        test_data = {
            "points": {
                "buy": 100.0,
                "sell": 90.0,
                "total": 1000000,
                "timestamp": 1234567890
            },
            "items": {
                "1": {
                    "name": "Test Item",
                    "value": 95.0,
                    # Missing timestamp
                }
            }
        }
        
        df = processor.transform_data(test_data)
        assert not df.empty
        
        # Points should use provided timestamp
        points_row = df[df['currency_id'] == 1].iloc[0]
        assert not pd.isna(points_row['timestamp'])
        assert pd.Timestamp(points_row['timestamp']).timestamp() == 1234567890
        
        # Item should use current timestamp as fallback
        item_row = df[df['name'] == "Test Item"].iloc[0]
        assert not pd.isna(item_row['timestamp'])
        assert pd.Timestamp(item_row['timestamp']).timestamp() > 0

    def test_invalid_response_structure(self, processor):
        """Test handling of invalid response structure."""
        invalid_data = {
            "points": "not_a_dict",
            "items": None
        }
        
        df = processor.transform_data(invalid_data)
        assert df.empty

    def test_empty_points_and_items(self, processor):
        """Test handling of empty points and items data."""
        empty_data = {
            "points": {},
            "items": {}
        }
        
        df = processor.transform_data(empty_data)
        assert df.empty

    def test_schema_validation(self, processor):
        """Test schema validation."""
        schema = processor.get_schema()
        
        # Verify required fields
        required_fields = [field.name for field in schema if field.mode == "REQUIRED"]
        assert "currency_id" in required_fields
        assert "name" in required_fields
        assert "timestamp" in required_fields
        
        # Verify field types
        field_types = {field.name: field.field_type for field in schema}
        assert field_types["currency_id"] == "INTEGER"
        assert field_types["name"] == "STRING"
        assert field_types["buy_price"] == "FLOAT"
        assert field_types["sell_price"] == "FLOAT"
        assert field_types["circulation"] == "INTEGER"
        assert field_types["timestamp"] == "TIMESTAMP" 