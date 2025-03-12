"""Test polling functionality and ISO duration parsing.

This module tests:
1. ISO 8601 duration parsing
2. Polling interval calculation
3. Wait timing accuracy
"""

import unittest
from datetime import timedelta
import time
from app.common.common import parse_iso_duration, wait_for_next_poll
import logging

class TestPolling(unittest.TestCase):
    """Test cases for polling functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        logging.basicConfig(level=logging.INFO)
    
    def test_parse_iso_duration(self):
        """Test ISO 8601 duration parsing."""
        test_cases = [
            ("PT15M", timedelta(minutes=15)),
            ("PT1H", timedelta(hours=1)),
            ("P1D", timedelta(days=1)),
            ("PT1H30M", timedelta(hours=1, minutes=30)),
            ("P1DT12H", timedelta(days=1, hours=12))
        ]
        
        for duration_str, expected in test_cases:
            with self.subTest(duration=duration_str):
                result = parse_iso_duration(duration_str)
                self.assertEqual(result, expected)
    
    def test_invalid_duration(self):
        """Test invalid duration formats."""
        invalid_durations = [
            "15M",  # Missing P prefix
            "PT",   # No time values
            "P",    # No values
            "PTM",  # Missing number
            "PT15X" # Invalid unit
        ]
        
        for duration in invalid_durations:
            with self.subTest(duration=duration):
                with self.assertRaises(ValueError):
                    parse_iso_duration(duration)
    
    def test_wait_timing(self):
        """Test wait timing accuracy with a short duration."""
        # Test with a 2-second wait
        api_config = {
            "name": "test_endpoint",
            "frequency": "PT2S"
        }
        
        start_time = time.time()
        wait_for_next_poll(api_config)
        elapsed = time.time() - start_time
        
        # Allow for small timing variations (between 1.9 and 2.1 seconds)
        self.assertTrue(1.9 <= elapsed <= 2.1, 
                       f"Wait time was {elapsed:.2f} seconds, expected ~2.0 seconds")
    
    def test_default_frequency(self):
        """Test default frequency when none is specified."""
        api_config = {
            "name": "test_endpoint"
            # No frequency specified
        }
        
        # Patch time.sleep to verify the default duration
        original_sleep = time.sleep
        try:
            sleep_duration = None
            def mock_sleep(seconds):
                nonlocal sleep_duration
                sleep_duration = seconds
            
            time.sleep = mock_sleep
            wait_for_next_poll(api_config)
            
            # Default should be 15 minutes (900 seconds)
            self.assertEqual(sleep_duration, 900)
        finally:
            time.sleep = original_sleep

if __name__ == '__main__':
    unittest.main() 