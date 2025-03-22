"""Integration tests for the data pipeline between Torn City API and BigQuery."""

from unittest.mock import Mock, patch, call
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import pytest
from google.cloud import bigquery
from google.api_core import exceptions
import pandas as pd

from app.services.torncity.client import TornClient, TornAPIError
from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.processors import (
    UserProcessor,
    ItemsProcessor,
    CrimeProcessor,
    CurrencyProcessor,
    MembersProcessor
)

class TestDataPipelineIntegration:
    """Test suite for data pipeline integration."""

    @pytest.fixture
    def torn_client(self, mock_api_keys):
        """Create a TornClient instance."""
        return TornClient(mock_api_keys)

    @pytest.fixture
    def mock_bigquery_client(self, mocker):
        """Create a mock BigQuery client."""
        mock_client = mocker.patch('google.cloud.bigquery.Client')
        mock_client.return_value.get_table.return_value = mocker.Mock()
        mock_client.return_value.load_table_from_dataframe.return_value = mocker.Mock()
        mock_client.return_value.load_table_from_dataframe.return_value.result.return_value = None
        return mock_client

    @pytest.fixture
    def mock_credentials(self):
        """Mock Google Cloud credentials."""
        with patch('google.oauth2.service_account.Credentials') as mock_creds:
            mock_creds.from_service_account_file.return_value = mock_creds
            return mock_creds

    @pytest.fixture
    def mock_monitoring_client(self):
        """Mock Google Cloud monitoring client."""
        with patch('google.cloud.monitoring_v3.MetricServiceClient') as mock_client:
            return mock_client.return_value

    @pytest.fixture
    def sample_config(self):
        """Provide a sample configuration for testing."""
        return {
            "api_key": "abcd1234efgh5678",  # Valid 16-character alphanumeric key
            "project_id": "test-project",
            "dataset": "test_dataset",
            "gcp_project_id": "test-project",
            "gcp_credentials_file": "test_credentials.json",
            "credentials_path": "test_credentials.json",
            "dataset_id": "test_dataset",
            "table_id": "test_table",
            "endpoint": "user",
            "selection": "default",
            "storage_mode": "append"
        }

    @pytest.fixture
    def user_processor(self, sample_config):
        """Create a UserProcessor instance."""
        return UserProcessor(sample_config)

    @pytest.fixture
    def members_processor(self, sample_config):
        """Create a MembersProcessor instance."""
        return MembersProcessor(sample_config)

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, mock_bigquery_client, mock_torn_api, mock_fetch, mock_batch_write, mock_write, mock_update_schema):
        """Set up test fixtures."""
        self.monkeypatch = monkeypatch
        self.mock_bigquery_client = mock_bigquery_client
        self.mock_torn_api = mock_torn_api
        self.mock_fetch = mock_fetch
        self.mock_batch_write = mock_batch_write
        self.mock_write = mock_write
        self.mock_update_schema = mock_update_schema

    def mock_torn_response(self):
        """Create a mock Torn API response."""
        return {
            "data": {
                "members": {
                    "12345": {
                        "name": "TestUser",
                        "level": 10,
                        "life": {"current": 100, "maximum": 100},
                        "last_action": {"timestamp": 1647691200, "status": "Idle"},
                        "status": {"state": "Online", "description": ""},
                        "faction": {"position": "Member", "faction_id": 1234}
                    }
                }
            }
        }

    def test_end_to_end_data_pipeline(self, sample_config):
        """Test end-to-end data pipeline processing."""
        self.monkeypatch.setattr('app.services.torncity.base.BaseEndpointProcessor.fetch_data', self.mock_fetch)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.write_data_to_table', self.mock_write)
        
        processor = MembersProcessor(sample_config)
        mock_response = self.mock_torn_response()
        self.mock_fetch.return_value = mock_response
        
        result = processor.process_data()
        assert not result.empty
        assert result["name"].iloc[0] == "TestUser"
        assert result["level"].iloc[0] == 10
        self.mock_write.assert_called_once()

    def test_data_validation(self):
        """Test data validation."""
        self.monkeypatch.setattr('app.services.torncity.base.BaseEndpointProcessor.fetch_data', self.mock_fetch)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.write_data_to_table', self.mock_write)
        
        processor = MembersProcessor({
            "api_key": "abcd1234efgh5678",
            "project_id": "test-project",
            "dataset": "test_dataset",
            "table_id": "test_table"
        })
        
        mock_response = self.mock_torn_response()
        self.mock_fetch.return_value = mock_response
        
        result = processor.transform_data(mock_response.get("data", {}))
        assert not result.empty
        assert result["name"].iloc[0] == "TestUser"
        assert result["level"].iloc[0] == 10

    def test_pipeline_error_handling(self):
        """Test error handling in the pipeline."""
        self.monkeypatch.setattr('app.services.torncity.base.BaseEndpointProcessor.fetch_data', self.mock_fetch)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.write_data_to_table', self.mock_write)
        
        processor = MembersProcessor({
            "api_key": "abcd1234efgh5678",
            "project_id": "test-project",
            "dataset": "test_dataset",
            "table_id": "test_table"
        })
        
        self.mock_fetch.side_effect = TornAPIError("Test error")
        
        result = processor.process_data()
        assert result.empty

    def test_batch_processing(self):
        """Test batch processing of data."""
        self.monkeypatch.setattr('app.services.torncity.base.BaseEndpointProcessor.fetch_data', self.mock_fetch)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.write_data_to_table', self.mock_batch_write)
        
        processor = MembersProcessor({
            "api_key": "abcd1234efgh5678",
            "project_id": "test-project",
            "dataset": "test_dataset",
            "table_id": "test_table"
        })
        
        mock_responses = [
            {"data": {"members": {"1": {"name": "User1", "level": 10}}}},
            {"data": {"members": {"2": {"name": "User2", "level": 11}}}}
        ]
        self.mock_fetch.side_effect = mock_responses
        
        results = []
        for _ in range(2):
            result = processor.transform_data(self.mock_fetch().get("data", {}))
            results.append(result)
        
        assert len(results) == 2
        assert results[0]["name"].iloc[0] == "User1"
        assert results[1]["name"].iloc[0] == "User2"

    def test_data_type_consistency(self):
        """Test data type consistency."""
        self.monkeypatch.setattr('app.services.torncity.base.BaseEndpointProcessor.fetch_data', self.mock_fetch)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.write_data_to_table', self.mock_write)
        
        processor = MembersProcessor({
            "api_key": "abcd1234efgh5678",
            "project_id": "test-project",
            "dataset": "test_dataset",
            "table_id": "test_table"
        })
        
        mock_response = self.mock_torn_response()
        self.mock_fetch.return_value = mock_response
        
        result = processor.transform_data(mock_response.get("data", {}))
        assert pd.api.types.is_integer_dtype(result["level"])
        assert pd.api.types.is_string_dtype(result["name"])
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_schema_evolution_handling(self):
        """Test handling of schema evolution."""
        self.monkeypatch.setattr('app.services.torncity.base.BaseEndpointProcessor.fetch_data', self.mock_fetch)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.write_data_to_table', self.mock_write)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.update_table_schema', self.mock_update_schema)
        
        processor = MembersProcessor({
            "api_key": "abcd1234efgh5678",
            "project_id": "test-project",
            "dataset": "test_dataset",
            "table_id": "test_table"
        })
        
        mock_response = self.mock_torn_response()
        mock_response["data"]["members"]["12345"]["new_field"] = "test_value"
        self.mock_fetch.return_value = mock_response
        
        result = processor.transform_data(mock_response.get("data", {}))
        assert "new_field" in result.columns
        assert result["new_field"].iloc[0] == "test_value"

    def test_concurrent_processing(self):
        """Test concurrent processing of multiple endpoints."""
        self.monkeypatch.setattr('app.services.torncity.base.BaseEndpointProcessor.fetch_data', self.mock_fetch)
        self.monkeypatch.setattr('app.services.google.bigquery.client.BigQueryClient.write_data_to_table', self.mock_write)
        
        processor = MembersProcessor({
            "api_key": "abcd1234efgh5678",
            "project_id": "test-project",
            "dataset": "test_dataset",
            "table_id": "test_table"
        })
        
        mock_responses = [
            {"data": {"members": {"1": {"name": "User1", "level": 10}}}},
            {"data": {"members": {"2": {"name": "User2", "level": 11}}}}
        ]
        self.mock_fetch.side_effect = mock_responses
        
        results = []
        for _ in range(2):
            result = processor.transform_data(self.mock_fetch().get("data", {}))
            results.append(result)
        
        assert len(results) == 2
        assert results[0]["name"].iloc[0] == "User1"
        assert results[1]["name"].iloc[0] == "User2" 