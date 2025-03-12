#!/bin/bash

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a /var/log/tcdatalogger/app.log
}

# Function to check required files
check_required_files() {
    local missing_files=0
    
    # Check for required config files
    for file in "crontab" "credentials.json" "TC_API_key.txt" "TC_API_config.json"; do
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

# Copy crontab from mounted config volume
log "INFO: Setting up cron job..."
cp /app/config/crontab /etc/cron.d/tcdatalogger
chmod 0644 /etc/cron.d/tcdatalogger

# Update crontab to use correct paths and logging
sed -i 's|python3|/usr/local/bin/python|g' /etc/cron.d/tcdatalogger
sed -i 's|/app/main.py|/app/src/main.py|g' /etc/cron.d/tcdatalogger
sed -i 's|/var/log/cron.log|/var/log/tcdatalogger/app.log|g' /etc/cron.d/tcdatalogger

# Apply crontab
if ! crontab /etc/cron.d/tcdatalogger; then
    log "ERROR: Failed to apply crontab"
    exit 1
fi

# Start cron
log "INFO: Starting cron service..."
if ! service cron start; then
    log "ERROR: Failed to start cron service"
    exit 1
fi

# Run the initial job
log "INFO: Running initial job..."
cd /app && /usr/local/bin/python /app/src/main.py >> /var/log/tcdatalogger/app.log 2>&1

# Monitor logs and cron service
log "INFO: Container startup complete. Monitoring logs..."
exec tail -f /var/log/tcdatalogger/app.log 