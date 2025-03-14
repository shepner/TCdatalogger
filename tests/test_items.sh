#!/bin/bash

# Set the environment variables for Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS=~/credentials.json

# Activate the virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Change to the project root directory
cd "$(dirname "$0")/.."

# Run the test script
python3 tests/test_items_endpoint.py

# Get the exit code
exit_code=$?

# Deactivate the virtual environment if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

# Exit with the test script's exit code
exit $exit_code 