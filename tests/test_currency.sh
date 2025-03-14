#!/bin/bash

# Change to project root directory
cd "$(dirname "$0")/.."

# Create and activate virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    echo "Installing dependencies..."
    pip install pandas google-cloud-bigquery pyarrow
else
    echo "Activating existing virtual environment..."
    source venv/bin/activate
fi

# Run the test script from the tests directory
python3 tests/test_currency_endpoint.py

# Deactivate virtual environment
deactivate 