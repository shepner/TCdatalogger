#!/bin/bash
# Ensure cron PID directory exists with correct permissions
mkdir -p /var/run/crond
chown tcdatalogger:tcdatalogger /var/run/crond
chmod 755 /var/run/crond

# Start cron in the foreground
exec cron -f 