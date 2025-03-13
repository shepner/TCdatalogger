#!/bin/bash

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a /var/log/tcdatalogger/app.log
}

# Function to check required files
check_required_files() {
    local missing_files=0
    
    # Check for required config files
    for file in "credentials.json" "TC_API_key.txt" "TC_API_config.json"; do
        if [ ! -f "/app/config/$file" ]; then
            log "ERROR: Required file /app/config/$file not found"
            missing_files=$((missing_files + 1))
        else
            log "INFO: Found config file: $file"
        fi
    done
    
    return $missing_files
}

# Create log directory if it doesn't exist
mkdir -p /var/log/tcdatalogger

# Initialize log file
log "INFO: Starting TCdatalogger container"

# Check for required files
log "INFO: Checking for required configuration files..."
if ! check_required_files; then
    log "ERROR: Missing required configuration files. Please check the mounted config volume."
    exit 1
fi

# Activate virtual environment and start the Python application
log "INFO: Starting TCdatalogger application..."
cd /app && exec /app/venv/bin/python /app/src/main.py 