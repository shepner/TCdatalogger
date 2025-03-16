#!/bin/bash

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if a container exists
check_container() {
    local container_name="tcdatalogger"
    local count=$(docker ps -a --filter "name=${container_name}" --format '{{.Names}}' | wc -l)
    if [ "$count" -gt 0 ]; then
        return 0  # Container exists
    else
        return 1  # Container does not exist
    fi
}

# Function to stop and remove existing containers
cleanup_containers() {
    local container_name="tcdatalogger"
    
    # Check for running containers
    if docker ps -q --filter "name=${container_name}" | grep -q .; then
        log "INFO: Stopping running container..."
        docker stop $(docker ps -q --filter "name=${container_name}")
    fi
    
    # Check for existing containers (including stopped ones)
    if docker ps -aq --filter "name=${container_name}" | grep -q .; then
        log "INFO: Removing existing container..."
        docker rm $(docker ps -aq --filter "name=${container_name}")
    fi
}

# Function to start the container
start_container() {
    log "INFO: Building and starting container..."
    docker compose up --build
}

# Main script
log "INFO: Starting TCdatalogger management script..."

# Check for existing containers
if check_container; then
    log "INFO: Found existing container(s)"
    cleanup_containers
else
    log "INFO: No existing containers found"
fi

# Start the container
start_container 