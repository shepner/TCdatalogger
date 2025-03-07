#!/bin/bash

# Copy crontab from mounted config volume
if [ -f /app/config/crontab ]; then
    cp /app/config/crontab /etc/cron.d/tcdatalogger
    chmod 0644 /etc/cron.d/tcdatalogger
    crontab /etc/cron.d/tcdatalogger
else
    echo "Warning: No crontab file found in config volume"
    exit 1
fi

# Start cron
service cron start

# Run the initial job
cd /app && /usr/local/bin/python /app/main.py >> /var/log/cron.log 2>&1

# Tail the logs
tail -f /var/log/cron.log 