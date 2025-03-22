"""Unit tests for common utilities."""

import os
import json
import pytest
import tempfile
import logging
from datetime import timedelta
from app.core.common import (
    setup_logging,
    find_config_directory,
    load_config,
    parse_iso_duration,
    wait_for_next_poll
)

def test_imports():
    """Test that all required imports are available."""
    pass  # Will be implemented

def test_setup_logging(caplog):
    """Test logging setup."""
    setup_logging()
    
    # Verify log level is set
    assert logging.getLogger().level == logging.INFO
    
    # Verify handlers are set up
    root_logger = logging.getLogger()
    assert any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers)
    assert any(isinstance(h, logging.FileHandler) for h in root_logger.handlers)
    
    # Test logging output
    logging.info("Test message")
    assert "Test message" in caplog.text

class TestConfigManagement:
    """Test suite for configuration management functions."""
    
    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary config directory with test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test config files
            with open(os.path.join(temp_dir, "credentials.json"), "w") as f:
                json.dump({"test": "credentials"}, f)
            with open(os.path.join(temp_dir, "TC_API_key.json"), "w") as f:
                json.dump({"key": "test_key"}, f)
            with open(os.path.join(temp_dir, "TC_API_config.json"), "w") as f:
                json.dump({"config": "test_config"}, f)
            yield temp_dir
    
    def test_find_config_directory(self, temp_config_dir):
        """Test finding configuration directory."""
        # Test with valid directory
        directories = ["/nonexistent", temp_config_dir]
        assert find_config_directory(directories) == temp_config_dir
        
        # Test with no valid directories
        directories = ["/nonexistent1", "/nonexistent2"]
        assert find_config_directory(directories) is None
        
        # Test with empty list
        assert find_config_directory([]) is None
    
    def test_load_config(self, temp_config_dir):
        """Test loading configuration."""
        # Test with valid config directory
        config = load_config([temp_config_dir])
        assert config is not None
        assert config["config_dir"] == temp_config_dir
        assert os.path.exists(config["gcp_credentials_file"])
        assert os.path.exists(config["tc_api_key_file"])
        assert os.path.exists(config["tc_api_config_file"])
        
        # Test with invalid directory
        config = load_config(["/nonexistent"])
        assert config is None
        
        # Test with missing required file
        os.remove(os.path.join(temp_config_dir, "credentials.json"))
        config = load_config([temp_config_dir])
        assert config is None

class TestDurationParsing:
    """Test suite for ISO duration parsing."""
    
    @pytest.mark.parametrize("duration,expected", [
        ("PT15M", timedelta(minutes=15)),
        ("PT1H", timedelta(hours=1)),
        ("P1D", timedelta(days=1)),
        ("PT1H30M", timedelta(hours=1, minutes=30)),
        ("P1DT12H", timedelta(days=1, hours=12)),
        ("P1Y", timedelta(days=365)),
        ("P1M", timedelta(days=30)),
        ("PT1S", timedelta(seconds=1)),
    ])
    def test_valid_durations(self, duration, expected):
        """Test parsing valid ISO durations."""
        assert parse_iso_duration(duration) == expected
    
    @pytest.mark.parametrize("invalid_duration", [
        "",  # Empty string
        "P",  # Only period designator
        "15M",  # Missing period designator
        "PT",  # Missing time value
        "PTM",  # Missing number
        "P1H",  # Time designator in wrong place
        "PT1X",  # Invalid designator
        None,  # Not a string
        "P0D",  # Zero duration
    ])
    def test_invalid_durations(self, invalid_duration):
        """Test parsing invalid ISO durations."""
        with pytest.raises(ValueError):
            parse_iso_duration(invalid_duration)

class TestPolling:
    """Test suite for polling functionality."""
    
    def test_wait_for_next_poll(self, mocker):
        """Test wait_for_next_poll function."""
        # Mock time.sleep to avoid actual waiting
        mock_sleep = mocker.patch('time.sleep')
        
        # Test valid frequency
        api_config = {"name": "test", "frequency": "PT1M"}
        wait_for_next_poll(api_config)
        mock_sleep.assert_called_once_with(60)
        
        # Test missing frequency
        with pytest.raises(ValueError):
            wait_for_next_poll({"name": "test"})
        
        # Test empty frequency
        with pytest.raises(ValueError):
            wait_for_next_poll({"name": "test", "frequency": ""})
        
        # Test invalid frequency format
        with pytest.raises(ValueError):
            wait_for_next_poll({"name": "test", "frequency": "1M"})

class TestCommonUtilities:
    """Test suite for common utilities."""
    
    def setup_method(self):
        """Set up test fixtures."""
        pass
    
    def teardown_method(self):
        """Clean up after tests."""
        pass
    
    def test_placeholder(self):
        """Placeholder for actual tests."""
        pass  # Will be implemented 