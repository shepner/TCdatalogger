#!/usr/bin/env python3
"""Test runner script for TCdatalogger."""

import os
import sys
import pytest
import json
from pathlib import Path

def setup_test_environment():
    """Set up the test environment with necessary configurations."""
    # Set environment variables for testing
    os.environ["TORN_API_KEY"] = "test_api_key"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
        Path(__file__).parent / "config" / "mock_credentials.json"
    )
    
    # Load test configuration
    config_path = Path(__file__).parent / "config" / "test_config.json"
    with open(config_path) as f:
        test_config = json.load(f)
    
    # Set additional environment variables from config if needed
    os.environ["BIGQUERY_PROJECT_ID"] = test_config["bigquery"]["project_id"]
    os.environ["BIGQUERY_DATASET_ID"] = test_config["bigquery"]["dataset_id"]

def run_tests():
    """Run the test suite."""
    # Add the application root to Python path
    app_root = str(Path(__file__).parent.parent)
    sys.path.insert(0, app_root)
    
    # Set up test environment
    setup_test_environment()
    
    # Run pytest with coverage
    args = [
        "--verbose",
        "--cov=app",
        "--cov-report=term-missing",
        "--cov-report=html:coverage_report",
        "tests/unit"
    ]
    
    return pytest.main(args)

if __name__ == "__main__":
    sys.exit(run_tests()) 