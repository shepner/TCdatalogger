"""Unit tests for Torn City currency endpoint processor."""

from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery

from app.services.torncity.endpoints.currency import CurrencyProcessor

class TestCurrencyProcessor:
    """Test suite for CurrencyProcessor."""

    @pytest.fixture
    def processor(self, sample_config, mock_api_keys):
        """Create a CurrencyProcessor instance for testing."""
        return CurrencyProcessor(sample_config)

    @pytest.fixture
    def sample_currency_data(self):
        """Sample currency data for testing."""
        return {
            "1": {
                "name": "Points",
                "buy_price": 45000,
                "sell_price": 44500,
                "circulation": 1000000,
                "market_value": 44750,
                "timestamp": 1234567890
            },
            "2": {
                "name": "Swiss Franc",
                "buy_price": 1.05,
                "sell_price": 0.95,
                "circulation": 500000,
                "market_value": 1.00,
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

    def test_process_data_valid(self, processor, sample_currency_data):
        """Test processing valid currency data."""
        processed_data = processor.process_data({"data": sample_currency_data})
        
        assert len(processed_data) == 2
        assert all(isinstance(currency, dict) for currency in processed_data)
        
        # Verify first currency
        currency1 = next(currency for currency in processed_data if currency["currency_id"] == 1)
        assert currency1["name"] == "Points"
        assert currency1["buy_price"] == 45000
        assert currency1["sell_price"] == 44500
        assert currency1["circulation"] == 1000000
        assert currency1["market_value"] == 44750
        assert currency1["timestamp"] == 1234567890
        
        # Verify second currency
        currency2 = next(currency for currency in processed_data if currency["currency_id"] == 2)
        assert currency2["name"] == "Swiss Franc"
        assert currency2["buy_price"] == 1.05
        assert currency2["sell_price"] == 0.95
        assert currency2["circulation"] == 500000
        assert currency2["market_value"] == 1.00
        assert currency2["timestamp"] == 1234567890

    def test_process_data_empty(self, processor):
        """Test processing empty data."""
        processed_data = processor.process_data({"data": {}})
        assert len(processed_data) == 0

    def test_process_data_missing_fields(self, processor):
        """Test processing data with missing fields."""
        data = {
            "1": {
                "name": "Test Currency",
                # Missing other fields
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": data})
        assert len(processed_data) == 1
        currency = processed_data[0]
        
        assert currency["currency_id"] == 1
        assert currency["name"] == "Test Currency"
        assert currency["buy_price"] is None
        assert currency["sell_price"] is None
        assert currency["circulation"] is None
        assert currency["market_value"] is None
        assert currency["timestamp"] == 1234567890

    def test_process_data_invalid_types(self, processor):
        """Test processing data with invalid types."""
        data = {
            "1": {
                "name": 123,  # Should be string
                "buy_price": "45000",  # Should be number
                "circulation": "1000000",  # Should be integer
                "timestamp": "invalid"  # Should be integer
            }
        }
        
        with pytest.raises(ValueError):
            processor.process_data({"data": data})

    @patch("app.services.torncity.client.TornClient.fetch_data")
    @patch("app.services.google.bigquery.client.BigQueryClient.write_data")
    def test_run_end_to_end(self, mock_write, mock_fetch, processor, sample_currency_data):
        """Test running the processor end-to-end."""
        mock_fetch.return_value = {"data": sample_currency_data}
        
        processor.run()
        
        mock_fetch.assert_called_once()
        mock_write.assert_called_once()
        
        # Verify the processed data matches the schema
        written_data = mock_write.call_args[0][0]
        assert len(written_data) == 2
        assert all(isinstance(currency, dict) for currency in written_data)
        
        # Verify all required fields are present
        required_fields = {field.name for field in processor.schema if field.mode == "REQUIRED"}
        assert all(all(field in currency for field in required_fields) for currency in written_data)

    def test_price_calculations(self, processor):
        """Test price calculations with various scenarios."""
        test_data = {
            "1": {
                "name": "Test Currency 1",
                "buy_price": 100.0,
                "sell_price": 90.0,
                "market_value": None,  # Missing market value
                "timestamp": 1234567890
            },
            "2": {
                "name": "Test Currency 2",
                "buy_price": None,  # Missing buy price
                "sell_price": None,  # Missing sell price
                "market_value": 95.0,
                "timestamp": 1234567890
            },
            "3": {
                "name": "Test Currency 3",
                "buy_price": 0.0,  # Zero prices
                "sell_price": 0.0,
                "market_value": 0.0,
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": test_data})
        
        # Verify price calculations
        currency1 = next(currency for currency in processed_data if currency["currency_id"] == 1)
        assert currency1["buy_price"] == 100.0
        assert currency1["sell_price"] == 90.0
        assert currency1["market_value"] is None
        
        currency2 = next(currency for currency in processed_data if currency["currency_id"] == 2)
        assert currency2["buy_price"] is None
        assert currency2["sell_price"] is None
        assert currency2["market_value"] == 95.0
        
        currency3 = next(currency for currency in processed_data if currency["currency_id"] == 3)
        assert currency3["buy_price"] == 0.0
        assert currency3["sell_price"] == 0.0
        assert currency3["market_value"] == 0.0

    def test_decimal_precision(self, processor):
        """Test handling of decimal values."""
        test_data = {
            "1": {
                "name": "Test Currency",
                "buy_price": 1.23456789,  # More than 2 decimal places
                "sell_price": 1.23456789,
                "market_value": 1.23456789,
                "timestamp": 1234567890
            }
        }
        
        processed_data = processor.process_data({"data": test_data})
        currency = processed_data[0]
        
        # Verify values are rounded to 2 decimal places
        assert currency["buy_price"] == 1.23
        assert currency["sell_price"] == 1.23
        assert currency["market_value"] == 1.23 