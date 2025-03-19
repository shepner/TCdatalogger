"""Unit tests for Torn City API endpoint processors."""

from unittest.mock import Mock, patch, call
import json
from datetime import datetime, timedelta

import pytest
from google.cloud import bigquery
import pandas as pd

from app.services.torncity.processors import (
    UserProcessor,
    ItemsProcessor,
    CrimeProcessor,
    CurrencyProcessor,
    MembersProcessor
)
from app.services.torncity.exceptions import TornAPIError, SchemaError, DataValidationError

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

    def test_config_validation(self, sample_config):
        """Test configuration validation."""
        # Test missing required fields
        invalid_config = {}
        with pytest.raises(ValueError, match="Missing required configuration fields"):
            UserProcessor(invalid_config)

        # Test invalid storage mode
        invalid_config = sample_config.copy()
        invalid_config['storage_mode'] = 'invalid_mode'
        with pytest.raises(ValueError, match="Invalid storage mode"):
            UserProcessor(invalid_config)

    def test_validate_column_type_conversions(self, processor):
        """Test column type validation and conversion."""
        # Test string conversion
        string_field = bigquery.SchemaField("test_str", "STRING")
        series = pd.Series(['test', 123, None])
        converted = processor._validate_column_type(series, string_field)
        assert converted.dtype == 'object'
        assert converted.iloc[0] == 'test'
        assert converted.iloc[1] == '123'
        assert converted.iloc[2] == ''

        # Test integer conversion
        int_field = bigquery.SchemaField("test_int", "INTEGER")
        series = pd.Series([123, 456, 789])  # All valid integers
        converted = processor._validate_column_type(series, int_field)
        assert converted.dtype == 'int64'
        assert converted.iloc[0] == 123
        assert converted.iloc[1] == 456
        assert converted.iloc[2] == 789

        # Test float conversion
        float_field = bigquery.SchemaField("test_float", "FLOAT")
        series = pd.Series(['123.45', 456, None])
        converted = processor._validate_column_type(series, float_field)
        assert converted.dtype == 'float64'
        assert converted.iloc[0] == 123.45
        assert converted.iloc[1] == 456.0
        assert converted.iloc[2] == 0.0

        # Test boolean conversion
        bool_field = bigquery.SchemaField("test_bool", "BOOLEAN")
        series = pd.Series([True, 'true', 1, 0, None])
        converted = processor._validate_column_type(series, bool_field)
        assert converted.dtype == 'bool'
        assert bool(converted.iloc[0]) is True  # Convert numpy.bool_ to Python bool
        assert bool(converted.iloc[1]) is True
        assert bool(converted.iloc[2]) is True
        assert bool(converted.iloc[3]) is False
        assert bool(converted.iloc[4]) is False

        # Test datetime conversion
        datetime_field = bigquery.SchemaField("test_datetime", "DATETIME")
        series = pd.Series(['2024-03-17', '1646956800', None])
        converted = processor._validate_column_type(series, datetime_field)
        assert pd.api.types.is_datetime64_any_dtype(converted)
        assert converted.iloc[0].strftime('%Y-%m-%d') == '2024-03-17'
        assert pd.isna(converted.iloc[2])

    def test_validate_schema_required_fields(self, processor):
        """Test schema validation for required fields."""
        schema = [
            bigquery.SchemaField("required_field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("optional_field", "STRING", mode="NULLABLE")
        ]
        
        # Test missing required field
        df = pd.DataFrame({"optional_field": ["test"]})
        with pytest.raises(SchemaError, match="Missing required columns"):
            processor._validate_schema(df, schema)

        # Test null in required field
        df = pd.DataFrame({
            "required_field": [None],
            "optional_field": ["test"]
        })
        with pytest.raises(SchemaError, match="Invalid type for column"):
            processor._validate_schema(df, schema)

        # Test valid data
        df = pd.DataFrame({
            "required_field": ["value"],
            "optional_field": ["test"]
        })
        validated = processor._validate_schema(df, schema)
        assert validated is not None

    @patch('logging.error')
    def test_error_logging(self, mock_log, processor):
        """Test error logging functionality."""
        error_msg = "Test error message"
        processor._log_error(error_msg)
        
        mock_log.assert_called_once()
        log_args = mock_log.call_args[0][0]
        assert isinstance(log_args, dict)
        assert log_args["event"] == "endpoint_error"
        assert log_args["error"] == error_msg
        assert "timestamp" in log_args

    @patch('logging.info')
    def test_completion_logging(self, mock_log, processor):
        """Test completion logging functionality."""
        processor._log_completion(True, 1.234)
        
        mock_log.assert_called_once()
        log_args = mock_log.call_args[0][0]
        assert isinstance(log_args, dict)
        assert log_args["event"] == "endpoint_completion"
        assert log_args["success"] is True
        assert log_args["duration_seconds"] == 1.234
        assert log_args["error"] is None

    @patch('app.services.google.bigquery.client.BigQueryClient.upload_dataframe')
    def test_upload_data(self, mock_upload, processor, sample_config):
        """Test data upload to BigQuery."""
        df = pd.DataFrame({
            "player_id": [12345],
            "name": ["TestUser"],
            "level": [15]
        })
        schema = [
            bigquery.SchemaField("player_id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("level", "INTEGER")
        ]
        
        # Test successful upload
        processor._upload_data(df, schema)
        
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args[1]
        assert isinstance(call_args['df'], pd.DataFrame)
        assert call_args['table_id'] == f"{sample_config['gcp_project_id']}.{sample_config['dataset']}.{processor.endpoint_config['table']}"
        assert call_args['write_disposition'] == processor.endpoint_config['storage_mode']

        # Test upload failure
        mock_upload.side_effect = Exception("Upload failed")
        with pytest.raises(Exception, match="Upload failed"):
            processor._upload_data(df, schema)

    @patch('app.services.torncity.client.TornClient.make_request')
    def test_fetch_torn_data(self, mock_request, processor):
        """Test fetching data from Torn API."""
        expected_data = {"success": True, "data": {"player_id": 12345}}
        mock_request.return_value = expected_data
        
        # Test successful fetch
        result = processor.fetch_torn_data()
        assert result == expected_data
        mock_request.assert_called_once_with(
            processor.endpoint_config['endpoint'],
            processor.endpoint_config['selection']
        )
        
        # Test fetch failure
        mock_request.side_effect = TornAPIError("API error")
        with pytest.raises(TornAPIError, match="API error"):
            processor.fetch_torn_data()

    def test_record_metrics(self, processor):
        """Test metrics recording functionality."""
        metrics = {
            "upload_size": 100,
            "processing_time": 1.234,
            "success": True
        }
        
        # Since _record_metrics logs on failure, we just verify it doesn't raise
        try:
            processor._record_metrics(**metrics)
        except Exception as e:
            pytest.fail(f"record_metrics raised an exception: {e}")

    def test_process_method(self, processor, sample_data):
        """Test the main process method."""
        # Test successful processing
        with patch.object(processor, 'transform_data') as mock_transform, \
             patch.object(processor, '_upload_data') as mock_upload, \
             patch.object(processor, 'get_schema') as mock_schema, \
             patch.object(processor, '_validate_schema') as mock_validate:
            
            df = pd.DataFrame({
                "player_id": [12345],
                "name": ["TestUser"]
            })
            mock_transform.return_value = [{"player_id": 12345, "name": "TestUser"}]
            mock_schema.return_value = [
                bigquery.SchemaField("player_id", "INTEGER"),
                bigquery.SchemaField("name", "STRING")
            ]
            mock_validate.return_value = df
            
            result = processor.process(sample_data)
            assert result is True
            mock_transform.assert_called_once_with(sample_data)
            mock_validate.assert_called_once()
            mock_upload.assert_called_once()
        
        # Test empty data handling
        with patch.object(processor, 'transform_data') as mock_transform:
            mock_transform.return_value = []
            result = processor.process(sample_data)
            assert result is False
        
        # Test transformation error
        with patch.object(processor, 'transform_data') as mock_transform:
            mock_transform.side_effect = DataValidationError("Invalid data")
            result = processor.process(sample_data)
            assert result is False

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
        assert transformed[0]["id"] == 456
        assert transformed[0]["name"] == "Test Crime"
        assert transformed[0]["status"] == "completed"
        assert transformed[0]["reward_money"] == 5000
        assert transformed[0]["participant_count"] == 2
        assert transformed[0]["participant_ids"] == "12345,67890"

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
        assert len(transformed) == 2  # One for points, one for item
        
        # Verify points data (currency_id = 1)
        points_record = transformed[transformed['currency_id'] == 1].iloc[0]
        assert points_record['name'] == "Points"
        assert points_record['buy_price'] == 45.5
        assert points_record['sell_price'] == 44.8
        assert points_record['circulation'] == 1000000
        assert pd.notnull(points_record['timestamp'])
        
        # Verify item data
        item_record = transformed[transformed['currency_id'] == 123].iloc[0]
        assert item_record['name'] == "Test Item"
        assert item_record['buy_price'] == 0.0  # Items don't have buy prices
        assert item_record['sell_price'] == 1000.0
        assert item_record['circulation'] == 0  # Items don't have circulation data
        assert pd.notnull(item_record['timestamp'])

class TestMembersProcessor:
    """Test suite for members endpoint processor."""

    @pytest.fixture
    def processor(self, sample_config):
        """Create a MembersProcessor instance."""
        return MembersProcessor(sample_config)

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return {
            "members": {
                "1": {
                    "name": "TestUser",
                    "level": 10,
                    "status": {"state": "Okay"},
                    "last_action": {"timestamp": 1710766800},
                    "faction": {"position": "Member"}
                }
            }
        }

    def test_members_data_processing(self, processor, sample_data):
        """Test processing members data."""
        transformed = processor.process_data({"data": sample_data})
        assert len(transformed) == 1
        assert transformed[0]["faction_position"] == "Member"

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