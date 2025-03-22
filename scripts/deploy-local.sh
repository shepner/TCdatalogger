#!/bin/bash
set -e

# Configuration
LOCAL_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"  # Get absolute path to project root

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Create required directories
log "Creating required directories..."
mkdir -p "${LOCAL_PATH}"/{config,var/{log,data}}

# Set correct permissions
log "Setting permissions..."
chmod -R 755 "${LOCAL_PATH}"

# Check for required configuration files
log "Checking configuration files..."
for file in "credentials.json" "TC_API_key.json" "TC_API_config.json"; do
    if [ ! -f "${LOCAL_PATH}/config/$file" ]; then
        log "ERROR: Required file ${file} not found in config directory"
        log "Please create ${LOCAL_PATH}/config/${file} with appropriate content"
        exit 1
    else
        log "Found config file: ${file}"
    fi
done

# Stop any existing container
log "Stopping existing containers..."
if docker compose -f "${LOCAL_PATH}/docker-compose.yaml" ps | grep -q "tcdatalogger"; then
    docker compose -f "${LOCAL_PATH}/docker-compose.yaml" down
fi

# Build and start containers
log "Building and starting container..."
cd "${LOCAL_PATH}"
docker compose build --no-cache --pull
docker compose up -d

# Check container status
log "Checking container status..."
sleep 5
if ! docker compose ps | grep -q "tcdatalogger.*Up"; then
    log "ERROR: Container failed to start"
    log "Showing last 50 lines of logs:"
    docker compose logs --tail 50
    exit 1
fi

log "Deployment completed successfully!"
log "To view logs in real-time, run:"
log "cd ${LOCAL_PATH} && docker compose logs -f"
log ""
log "To check container status:"
log "cd ${LOCAL_PATH} && docker compose ps"
log ""
log "To stop the container:"
log "cd ${LOCAL_PATH} && docker compose down" 