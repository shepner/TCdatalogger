FROM python:3.13-slim-bullseye

# Install required packages
RUN apt-get update && \
    apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY app/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app/src/ /app/src/
COPY queries/ /app/queries/

# Create directories
RUN mkdir -p /app/config /app/var /var/log/tcdatalogger

# Copy start script
COPY docker/start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Set environment variable for Python to buffer stdout/stderr
ENV PYTHONUNBUFFERED=1

# Create volume mount points
VOLUME ["/app/config", "/app/var", "/var/log/tcdatalogger"]

# Start cron and tail logs
CMD ["/app/start.sh"]