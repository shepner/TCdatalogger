FROM python:3.13-slim-bullseye

# Install required packages
RUN apt-get update && \
    apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy application files from src directory
COPY src/app/ /app/app/
COPY src/tests/ /app/tests/
COPY src/main.py src/requirements.txt /app/

# Create directory for config
RUN mkdir -p /app/config

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy start script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Set environment variable for Python to buffer stdout/stderr
ENV PYTHONUNBUFFERED=1

# Create volume mount points
VOLUME ["/app/config", "/var/log/tcdatalogger"]

# Start cron and tail logs
CMD ["/app/start.sh"]