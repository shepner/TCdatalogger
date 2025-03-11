FROM python:3.11-slim

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ .
COPY start.sh .
COPY config/crontab /etc/cron.d/app-cron

# Make the shell script executable
RUN chmod +x start.sh

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/app-cron

# Create log file
RUN touch /var/log/cron.log

# Run the command on container startup
CMD ["./start.sh"] 