# Build stage
FROM python:3.13-slim-bullseye AS builder

# Set build environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Create app directory and venv, install dependencies
WORKDIR /opt/tcdatalogger
COPY app/requirements.txt .
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        && \
    python -m venv .venv && \
    . .venv/bin/activate && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    # Cleanup
    apt-get purge -y gcc python3-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /root/.cache/pip/*

# Runtime stage
FROM python:3.13-slim-bullseye

# Set runtime environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/tcdatalogger/.venv/bin:$PATH" \
    VIRTUAL_ENV="/opt/tcdatalogger/.venv" \
    PYTHONPATH=/opt/tcdatalogger \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# Create user and group first
RUN groupadd -r tcdatalogger && \
    useradd -r -g tcdatalogger tcdatalogger && \
    mkdir -p /app/{config,var/log,var/data} && \
    chown -R tcdatalogger:tcdatalogger /app && \
    chmod -R 755 /app

# Install runtime dependencies and setup cron
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        cron \
        procps \
        rsyslog \
        locales \
        && \
    # Setup locales
    echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && \
    locale-gen && \
    # Create locale environment file
    mkdir -p /etc/default && \
    echo "LANG=C.UTF-8\nLC_ALL=C.UTF-8" > /etc/default/locale && \
    # Cleanup
    rm -rf /var/lib/apt/lists/* && \
    # Setup cron directories with proper permissions
    mkdir -p /var/run/crond && \
    chown root:root /var/run/crond && \
    chmod 755 /var/run/crond && \
    mkdir -p /var/spool/cron/crontabs && \
    chown root:crontab /var/spool/cron/crontabs && \
    chmod 1730 /var/spool/cron/crontabs && \
    usermod -a -G crontab tcdatalogger && \
    # Setup logging directories
    rm -f /etc/rsyslog.conf

# Copy rsyslog configuration
COPY --chown=root:root --chmod=644 docker/rsyslog.conf /etc/rsyslog.conf

# Copy virtual environment and application files
COPY --from=builder /opt/tcdatalogger/.venv /opt/tcdatalogger/.venv
COPY --chown=tcdatalogger:tcdatalogger --chmod=755 app /opt/tcdatalogger/app
COPY --chown=tcdatalogger:tcdatalogger --chmod=755 scripts /opt/tcdatalogger/scripts
COPY --chown=root:root --chmod=755 docker/start-container.sh /opt/tcdatalogger/scripts/

# Set ownership of .venv directory
RUN chown -R tcdatalogger:tcdatalogger /opt/tcdatalogger/.venv

# Set working directory
WORKDIR /opt/tcdatalogger

# Disable any inherited health check
HEALTHCHECK NONE

CMD ["/opt/tcdatalogger/scripts/start-container.sh"]