# Build stage
FROM python:3.13-slim-bullseye AS builder

# Set build environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Create app directory and venv, install dependencies
WORKDIR /build
COPY app/requirements.txt .
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        && \
    python -m venv /opt/venv && \
    . /opt/venv/bin/activate && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    # Cleanup
    apt-get purge -y gcc python3-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /build/* && \
    rm -rf /root/.cache/pip/*

# Runtime stage
FROM python:3.13-slim-bullseye

# Set runtime environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    VIRTUAL_ENV="/opt/venv" \
    PYTHONPATH=/opt/tcdatalogger

# Copy virtual environment and application files
COPY --from=builder /opt/venv /opt/venv
COPY --chown=tcdatalogger:tcdatalogger --chmod=755 app /opt/tcdatalogger/app
COPY --chown=tcdatalogger:tcdatalogger --chmod=755 scripts /opt/tcdatalogger/scripts
COPY --chown=root:root --chmod=755 docker/start-container.sh /opt/tcdatalogger/scripts/

# Install runtime dependencies, setup cron, create user and directories
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        cron \
        procps \
        rsyslog \
        && \
    rm -rf /var/lib/apt/lists/* && \
    # Setup cron directories and logging
    rm -rf /etc/cron.*/* && \
    mkdir -p /var/spool/cron/crontabs && \
    chown root:crontab /var/spool/cron/crontabs && \
    chmod 1730 /var/spool/cron/crontabs && \
    # Configure rsyslog for cron
    echo "cron.*    /var/log/cron.log" > /etc/rsyslog.d/cron.conf && \
    touch /var/log/cron.log && \
    # Create user and setup directories
    groupadd -r tcdatalogger && \
    useradd -r -g tcdatalogger tcdatalogger && \
    usermod -a -G crontab tcdatalogger && \
    mkdir -p /app/{config,var/log,var/data} && \
    # Set ownership and permissions for /app directory only
    chown -R tcdatalogger:tcdatalogger /app && \
    chmod -R 755 /app

# Set working directory
WORKDIR /opt/tcdatalogger

# Health check
HEALTHCHECK --interval=5m --timeout=3s \
  CMD ps aux | grep "[c]ron" || exit 1

# Define volume mount points for external data
VOLUME ["/app/config"]     # For configuration files (mounted read-only)
VOLUME ["/app/var/log"]    # For log files
VOLUME ["/app/var/data"]   # For persistent data

CMD ["/opt/tcdatalogger/scripts/start-container.sh"]