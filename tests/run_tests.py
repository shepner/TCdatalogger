#!/usr/bin/env python3
"""Test runner script for TCdatalogger."""

import os
import sys
import pytest
import json
import shutil
import tempfile
from pathlib import Path

def setup_test_environment():
    """Set up the test environment with necessary configurations."""
    # Create temporary test config directory
    temp_dir = Path(tempfile.mkdtemp())
    
    # Copy test configuration files
    test_config_dir = Path(__file__).parent / "config"
    
    # Copy credentials
    shutil.copy2(
        test_config_dir / "mock_credentials.json",
        temp_dir / "credentials.json"
    )
    
    # Create test API key file
    api_keys = {
        "default": "test_api_key",
        "faction_40832": "test_api_key"
    }
    with open(temp_dir / "TC_API_key.json", "w") as f:
        json.dump(api_keys, f)
    
    # Load test configuration
    with open(test_config_dir / "test_config.json") as f:
        test_config = json.load(f)
    
    # Create test endpoints file
    endpoints = {
        "members": {
            "table": test_config["bigquery"]["dataset_id"] + ".members",
            "frequency": "daily",
            "storage_mode": "append",
            "selection": ["basic"],
            "batch_size": 10,
            "max_retries": 1,
            "retry_delay": 1
        }
    }
    with open(temp_dir / "endpoints.json", "w") as f:
        json.dump(endpoints, f)
    
    return temp_dir

def run_tests():
    """Run the test suite."""
    # Add the application root to Python path
    app_root = str(Path(__file__).parent.parent)
    sys.path.insert(0, app_root)
    
    # Set up test environment
    config_dir = setup_test_environment()
    
    # Run pytest with coverage
    args = [
        "--verbose",
        "--cov=app",
        "--cov-report=term-missing",
        "--cov-report=html:coverage_report",
        f"--config-dir={config_dir}",
        "tests/unit"
    ]
    
    try:
        return pytest.main(args)
    finally:
        # Clean up temporary directory
        shutil.rmtree(config_dir)

if __name__ == "__main__":
    sys.exit(run_tests()) 