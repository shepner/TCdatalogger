FROM python:3.13-slim-bullseye

# Set environment variables and create non-root user
ENV PYTHONUNBUFFERED=1 \
    PATH="/app/venv/bin:$PATH" \
    VIRTUAL_ENV="/app/venv"

# Create user, directories, and set up Python environment
RUN useradd -m -r -s /bin/bash tcdatalogger && \
    mkdir -p /app/config /app/var /var/log/tcdatalogger && \
    python -m venv /app/venv && \
    /app/venv/bin/pip install --upgrade pip && \
    chown -R tcdatalogger:tcdatalogger /app /var/log/tcdatalogger && \
    chmod -R 755 /app /var/log/tcdatalogger

# Set working directory and switch to non-root user
WORKDIR /app
USER tcdatalogger

# Install dependencies and clean up
COPY --chown=tcdatalogger:tcdatalogger app/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf ~/.cache/pip && \
    find /app/venv -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true && \
    find /app/venv -type f -name "*.pyc" -delete

# Copy application files and set up start script
COPY --chown=tcdatalogger:tcdatalogger app/src/ /app/src/
COPY --chown=tcdatalogger:tcdatalogger docker/start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Create volume mount points and start the application
VOLUME ["/app/config", "/app/var", "/var/log/tcdatalogger"]
CMD ["/app/start.sh"]