#!/bin/bash
set -e

# Configuration
REMOTE_HOST="d03.asyla.org"
REMOTE_PATH="/mnt/docker/tcdatalogger"
REMOTE_USER="docker"
REMOTE_GROUP="asyla"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Create required directories on remote host
log "Creating directories on ${REMOTE_HOST}..."
ssh ${REMOTE_HOST} << ENDSSH
doas mkdir -p ${REMOTE_PATH}/{config,var/{log,data}}
doas chown -R ${REMOTE_USER}:${REMOTE_GROUP} ${REMOTE_PATH}
doas chmod -R 775 ${REMOTE_PATH}
ENDSSH

# Sync application files to remote host
log "Syncing files to ${REMOTE_HOST}..."
rsync -av --force \
    --exclude '.git' \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude 'var' \
    --exclude 'config' \
    --rsync-path="doas rsync" \
    app \
    docker \
    scripts \
    docker-compose.yaml \
    .env.example \
    ${REMOTE_HOST}:${REMOTE_PATH}/

# Deploy and start the application
log "Deploying application on ${REMOTE_HOST}..."
ssh ${REMOTE_HOST} << ENDSSH
# Change to application directory
cd ${REMOTE_PATH}

# Ensure correct permissions
doas chown -R ${REMOTE_USER}:${REMOTE_GROUP} .
doas chmod -R 775 .

# Check for required configuration files
if [ ! -f "config/credentials.json" ] || [ ! -f "config/TC_API_key.json" ] || [ ! -f "config/TC_API_config.json" ]; then
    echo "ERROR: Missing required configuration files in config directory"
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file from example..."
    cp .env.example .env
    echo "Please update .env file with your settings"
    exit 1
fi

# Stop existing containers
doas docker compose down

# Build and start containers
doas docker compose build --no-cache --pull
doas docker compose up -d

# Check container status
sleep 5
if ! doas docker compose ps | grep -q "tcdatalogger.*Up"; then
    echo "ERROR: Container failed to start. Check logs with: docker compose logs"
    exit 1
fi

echo "Deployment completed successfully"
ENDSSH

log "Deployment completed. Container logs can be viewed with:"
log "ssh ${REMOTE_HOST} 'cd ${REMOTE_PATH} && doas docker compose logs -f'"
