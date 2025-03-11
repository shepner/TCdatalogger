#!/bin/bash

# Start the cron service
service cron start

# Create the log file if it doesn't exist
touch /var/log/cron.log

# Start logging
tail -f /var/log/cron.log &

# Run the initial data collection
python3 main.py

# Keep the container running
while true; do
    sleep 60
done 