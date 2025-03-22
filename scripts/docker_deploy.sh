#!/bin/bash
set -e

# Configuration
REMOTE_HOST="d03.asyla.org"
REMOTE_PATH="/mnt/docker/tcdatalogger"
REMOTE_USER="docker"
REMOTE_GROUP="asyla"
LOCAL_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"  # Get absolute path to project root

# Function to display usage
usage() {
    echo "Usage: $0 [--local|--remote]"
    echo "  --local   Deploy to local system"
    echo "  --remote  Deploy to remote system (${REMOTE_HOST})"
    exit 1
}

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check required configuration files
check_config_files() {
    local config_path="$1"
    log "Checking configuration files..."
    for file in "credentials.json" "TC_API_key.json" "TC_API_config.json"; do
        if [ ! -f "${config_path}/config/$file" ]; then
            log "ERROR: Required file ${file} not found in config directory"
            log "Please create ${config_path}/config/${file} with appropriate content"
            exit 1
        else
            log "Found config file: ${file}"
        fi
    done
}

# Function to create required directories
create_directories() {
    local base_path="$1"
    local is_remote="$2"
    local cmd_prefix=""
    
    if [ "$is_remote" = true ]; then
        log "Creating directories on ${REMOTE_HOST}..."
        ssh ${REMOTE_HOST} << ENDSSH
        doas mkdir -p ${base_path}/{config,var/{log,data}}
        doas chown -R ${REMOTE_USER}:${REMOTE_GROUP} ${base_path}
        doas chmod -R 775 ${base_path}
ENDSSH
    else
        log "Creating required directories..."
        mkdir -p "${base_path}"/{config,var/{log,data}}
        chmod -R 755 "${base_path}"
    fi
}

# Function to deploy application
deploy_application() {
    local base_path="$1"
    local is_remote="$2"
    local docker_cmd=""
    
    if [ "$is_remote" = true ]; then
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
            ${REMOTE_HOST}:${base_path}/
            
        docker_cmd="doas docker"
    else
        docker_cmd="docker"
    fi
    
    # Deploy and start the application
    if [ "$is_remote" = true ]; then
        log "Deploying application on ${REMOTE_HOST}..."
        ssh ${REMOTE_HOST} << ENDSSH
        # Change to application directory
        cd ${base_path}

        # Ensure correct permissions
        doas chown -R ${REMOTE_USER}:${REMOTE_GROUP} .
        doas chmod -R 775 .

        # Stop existing containers
        ${docker_cmd} compose down

        # Build and start containers
        ${docker_cmd} compose build --no-cache --pull
        ${docker_cmd} compose up -d

        # Check container status
        sleep 5
        if ! ${docker_cmd} compose ps | grep -q "tcdatalogger.*Up"; then
            echo "ERROR: Container failed to start. Check logs with: docker compose logs"
            exit 1
        fi

        echo "Deployment completed successfully"
ENDSSH
    else
        log "Deploying application locally..."
        cd "${base_path}"
        
        # Stop existing containers
        if ${docker_cmd} compose ps | grep -q "tcdatalogger"; then
            ${docker_cmd} compose down
        fi
        
        # Build and start containers
        ${docker_cmd} compose build --no-cache --pull
        ${docker_cmd} compose up -d
        
        # Check container status
        sleep 5
        if ! ${docker_cmd} compose ps | grep -q "tcdatalogger.*Up"; then
            log "ERROR: Container failed to start"
            log "Showing last 50 lines of logs:"
            ${docker_cmd} compose logs --tail 50
            exit 1
        fi
    fi
}

# Parse command line arguments
if [ $# -ne 1 ]; then
    usage
fi

case "$1" in
    --local)
        IS_REMOTE=false
        DEPLOY_PATH="${LOCAL_PATH}"
        ;;
    --remote)
        IS_REMOTE=true
        DEPLOY_PATH="${REMOTE_PATH}"
        ;;
    *)
        usage
        ;;
esac

# Main deployment process
create_directories "${DEPLOY_PATH}" "${IS_REMOTE}"
check_config_files "${DEPLOY_PATH}"
deploy_application "${DEPLOY_PATH}" "${IS_REMOTE}"

# Show final instructions
if [ "${IS_REMOTE}" = true ]; then
    log "Deployment completed. Container logs can be viewed with:"
    log "ssh ${REMOTE_HOST} 'cd ${REMOTE_PATH} && doas docker compose logs -f'"
else
    log "Deployment completed successfully!"
    log "To view logs in real-time, run:"
    log "cd ${DEPLOY_PATH} && docker compose logs -f"
    log ""
    log "To check container status:"
    log "cd ${DEPLOY_PATH} && docker compose ps"
    log ""
    log "To stop the container:"
    log "cd ${DEPLOY_PATH} && docker compose down"
fi
