#!/bin/sh
set -e

# Create destination directory on d03.asyla.org with proper permissions
ssh d03.asyla.org << 'ENDSSH'
doas mkdir -p /mnt/docker/tcdatalogger
doas chown -R docker:asyla /mnt/docker/tcdatalogger
doas chmod -R 775 /mnt/docker/tcdatalogger
ENDSSH

# Sync only the necessary files to d03.asyla.org
rsync -av \
  --rsync-path="doas rsync" \
  app \
  docker \
  docker-compose.yaml \
  d03.asyla.org:/mnt/docker/tcdatalogger/

# SSH to d03.asyla.org to set up permissions and start the container
ssh d03.asyla.org << 'ENDSSH'
cd /mnt/docker/tcdatalogger
doas chown -R docker:asyla .
doas chmod -R 775 .
doas docker compose up -d --build
ENDSSH
