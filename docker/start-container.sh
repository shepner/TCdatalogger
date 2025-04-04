#!/bin/bash
set -e

# Function to log messages
log() {
    echo "[$(date "+%Y-%m-%d %H:%M:%S")] $1"
}

# Ensure timezone is correct
log "Setting timezone to UTC"
cp /etc/localtime /etc/localtime.orig
ln -sf /usr/share/zoneinfo/UTC /etc/localtime

# Set up logging
log "Setting up log directory"
mkdir -p /app/var/log
chown tcdatalogger:tcdatalogger /app/var/log
chmod 755 /app/var/log

# Ensure cron directories exist with correct permissions
log "Setting up cron directories"
mkdir -p /var/run/crond
chown root:root /var/run/crond
chmod 755 /var/run/crond

# Start rsyslog
log "Starting rsyslog daemon"
rsyslogd

# Generate crontab from config
log "Generating crontab from config"
cd /opt/tcdatalogger
su -s /bin/bash tcdatalogger -c "PYTHONPATH=/opt/tcdatalogger ./scripts/create_crontab.py"

# Verify crontab was installed
if ! su -s /bin/bash tcdatalogger -c "crontab -l" > /dev/null 2>&1; then
    log "ERROR: Failed to install crontab"
    exit 1
fi

# Ensure log files exist with correct permissions
log "Setting up log files"
touch /app/var/log/cron.log /app/var/log/syslog
chown tcdatalogger:tcdatalogger /app/var/log/cron.log /app/var/log/syslog
chmod 644 /app/var/log/cron.log /app/var/log/syslog

# Start cron daemon with debugging enabled
log "Starting cron daemon"
exec cron -f -L 15 