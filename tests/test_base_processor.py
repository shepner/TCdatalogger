"""Tests for the BaseEndpointProcessor class."""

import os
import json
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
from google.cloud import monitoring_v3

from app.services.torncity.base import BaseEndpointProcessor

class TestProcessor(BaseEndpointProcessor):
    """Test implementation of BaseEndpointProcessor."""
    
    def transform_data(self, data: dict) -> pd.DataFrame:
        """Transform test data into a DataFrame."""
        return pd.DataFrame([data])

class TestBaseEndpointProcessor(unittest.TestCase):
    """Test cases for BaseEndpointProcessor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "config_dir": "/config",
            "gcp_credentials_file": "/config/credentials.json"
        }
        self.endpoint_config = {
            "name": "test_endpoint",
            "url": "https://api.test.com/endpoint?key={API_KEY}",
            "table": "project.dataset.table",
            "storage_mode": "replace"
        }
        self.api_keys = {"default": "test_key"}
        
        # Create processor instance
        with patch('google.cloud.monitoring_v3.MetricServiceClient'):
            self.processor = TestProcessor(
                self.config,
                self.endpoint_config,
                self.api_keys
            )

    @patch('requests.get')
    def test_fetch_data_success(self, mock_get):
        """Test successful data fetch."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_get.return_value = mock_response
        
        # Test fetch_data
        data = self.processor.fetch_data()
        
        # Verify results
        self.assertIsNotNone(data)
        self.assertEqual(data["data"], "test")
        self.assertIn("fetched_at", data)

    @patch('requests.get')
    def test_fetch_data_failure(self, mock_get):
        """Test failed data fetch."""
        # Setup mock to raise exception
        mock_get.side_effect = Exception("API Error")
        
        # Test fetch_data
        data = self.processor.fetch_data()
        
        # Verify results
        self.assertIsNone(data)

    def test_convert_timestamps(self):
        """Test timestamp conversion."""
        # Create test DataFrame
        df = pd.DataFrame({
            "timestamp": ["2024-03-14T12:00:00"],
            "created_at": [1710417600],
            "name": ["test"],
            "value": [100]
        })
        
        # Convert timestamps
        result = self.processor.convert_timestamps(df)
        
        # Verify results
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["timestamp"]))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["created_at"]))
        self.assertEqual(result["name"].iloc[0], "test")
        self.assertEqual(result["value"].iloc[0], 100)

    def test_convert_numerics(self):
        """Test numeric conversion."""
        # Create test DataFrame
        df = pd.DataFrame({
            "string_num": ["100", "200"],
            "text": ["abc", "def"],
            "mixed": ["100", "invalid"]
        })
        
        # Convert numerics
        result = self.processor.convert_numerics(df)
        
        # Verify results
        self.assertTrue(pd.api.types.is_numeric_dtype(result["string_num"]))
        self.assertFalse(pd.api.types.is_numeric_dtype(result["text"]))
        self.assertFalse(pd.api.types.is_numeric_dtype(result["mixed"]))

    @patch('app.services.google.client.upload_to_bigquery')
    def test_upload_data(self, mock_upload):
        """Test data upload."""
        # Create test DataFrame
        df = pd.DataFrame({"test": [1, 2, 3]})
        
        # Upload data
        self.processor.upload_data(df)
        
        # Verify upload was called
        mock_upload.assert_called_once_with(
            self.config,
            df,
            self.endpoint_config["table"],
            self.endpoint_config["storage_mode"]
        )

    @patch('app.services.torncity.base.BaseEndpointProcessor.fetch_data')
    @patch('app.services.torncity.base.BaseEndpointProcessor.upload_data')
    def test_process_success(self, mock_upload, mock_fetch):
        """Test successful process flow."""
        # Setup mocks
        mock_fetch.return_value = {"test": "data"}
        
        # Process data
        result = self.processor.process()
        
        # Verify results
        self.assertTrue(result)
        mock_upload.assert_called_once()

    @patch('app.services.torncity.base.BaseEndpointProcessor.fetch_data')
    def test_process_failure(self, mock_fetch):
        """Test process failure handling."""
        # Setup mock to return None (failure)
        mock_fetch.return_value = None
        
        # Process data
        result = self.processor.process()
        
        # Verify results
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main() 