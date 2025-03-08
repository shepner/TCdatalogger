"""TCdatalogger application package.

This package contains modules for:
- Data processing and configuration management
- Service provider integrations (Google, TornCity)
- Common utilities and helpers

## Features

- Fetches data from Torn City API v2 endpoints
- Handles nested JSON structures and list data
- Automatically flattens complex data structures
- Proper timestamp handling for all date/time fields
- Automatic schema updates in BigQuery
- Type inference and preservation
- Comprehensive error handling and logging
- Containerized deployment with scheduled execution

## Prerequisites

- Docker
- Google Cloud Platform account with BigQuery enabled
- Torn City API key with appropriate permissions
- Google Cloud credentials file

## Project Structure

```
TCdatalogger/
├── config/                  # Configuration files (not in repo)
│   ├── credentials.json     # GCP credentials
│   ├── TC_API_key.txt      # Torn City API key
│   ├── TC_API_config.json  # API endpoint configuration
│   └── crontab             # Cron schedule configuration
├── var/
│   └── log/
│       └── tcdatalogger/   # Application logs (not in repo)
│           └── app.log     # Combined application and cron logs
├── Dockerfile              # Container definition
├── start.sh               # Container startup script
└── src/                    # Source code
    ├── app/                # Application package
    │   ├── common/        # Common utilities
    │   └── svcProviders/  # Service integrations
    ├── tests/             # Unit tests
    ├── requirements.txt   # Python dependencies
    └── main.py           # Application entry point
```

## Docker Deployment

1. Build the Docker image:
```bash
docker build -t tcdatalogger .
```

2. Create required directories:
```bash
# Create config directory
mkdir -p config

# Create log directory with proper structure and permissions
mkdir -p var/log/tcdatalogger
chmod -R 777 var/log  # Ensure container can write to logs
```

3. Set up configuration:
```bash
# Copy your configuration files to ./config/
cp path/to/your/credentials.json config/
cp path/to/your/TC_API_key.txt config/
cp path/to/your/TC_API_config.json config/
```

4. Run the container:
```bash
docker run -d \
  --name tcdatalogger \
  --restart unless-stopped \
  -v "$(pwd)/config:/app/config:ro" \
  -v "$(pwd)/var/log/tcdatalogger:/var/log/tcdatalogger" \
  tcdatalogger
```

The container will:
- Validate all required configuration files on startup
- Run the data pipeline immediately
- Execute the pipeline every 15 minutes (configurable via crontab)
- Log all operations to var/log/tcdatalogger/app.log
- Automatically restart on failure

View logs:
```bash
# Follow logs in real-time
docker logs -f tcdatalogger

# Or view the log file directly
tail -f var/log/tcdatalogger/app.log
```