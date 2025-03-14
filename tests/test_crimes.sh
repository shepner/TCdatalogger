#!/bin/bash

# Set Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS=~/credentials.json

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    echo "Installing dependencies..."
    pip install pandas google-cloud-bigquery pyarrow
else
    source venv/bin/activate
fi

# Run the test script
python tests/test_crimes_endpoint.py

# Deactivate virtual environment
deactivate 