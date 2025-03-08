# TCdatalogger

A Python application for collecting Torn City API data and storing it in Google BigQuery.

## Overview

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

### Option 1: Building from GitHub

1. Clone the repository:
```bash
git clone https://github.com/shepner/TCdatalogger.git
cd TCdatalogger
```

2. Build the Docker image:
```bash
docker build -t tcdatalogger .
```

3. Create required directories:
```bash
# Create config directory
mkdir -p config

# Create log directory with proper structure and permissions
mkdir -p var/log/tcdatalogger
chmod -R 777 var/log  # Ensure container can write to logs
```

4. Set up configuration:
```bash
# Copy your configuration files to ./config/
cp path/to/your/credentials.json config/
cp path/to/your/TC_API_key.txt config/
cp path/to/your/TC_API_config.json config/
```

5. Run the container:
```bash
docker run -d \
  --name tcdatalogger \
  --restart unless-stopped \
  -v "$(pwd)/config:/app/config:ro" \
  -v "$(pwd)/var/log/tcdatalogger:/var/log/tcdatalogger" \
  tcdatalogger
```

### Option 2: Using Pre-built Image

If you prefer to use a pre-built image (when available):

```bash
# Create project directory
mkdir TCdatalogger && cd TCdatalogger

# Create required directories
mkdir -p config var/log/tcdatalogger
chmod -R 777 var/log

# Set up your configuration files in ./config/
# ... copy your config files as described in Configuration Setup ...

# Pull and run the container
docker run -d \
  --name tcdatalogger \
  --restart unless-stopped \
  -v "$(pwd)/config:/app/config:ro" \
  -v "$(pwd)/var/log/tcdatalogger:/var/log/tcdatalogger" \
  ghcr.io/your-username/tcdatalogger:latest
```

The container will:
- Validate all required configuration files on startup
- Run the data pipeline immediately
- Execute the pipeline every 15 minutes (configurable via crontab)
- Log all operations to var/log/tcdatalogger/app.log
- Automatically restart on failure

### Managing the Container

View logs:
```bash
# Follow logs in real-time
docker logs -f tcdatalogger

# Or view the log file directly
tail -f var/log/tcdatalogger/app.log
```

Other useful commands:
```bash
# Stop the container
docker stop tcdatalogger

# Start the container
docker start tcdatalogger

# Remove the container
docker rm tcdatalogger

# View container status
docker ps -a | grep tcdatalogger

# View container resource usage
docker stats tcdatalogger
```

## Configuration Setup

The application requires several configuration files to be placed in the `config/` directory:

### 1. Google Cloud Credentials (`credentials.json`)
1. Go to the [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project or select an existing one
3. Enable the BigQuery API for your project
4. Create a service account:
   - Navigate to IAM & Admin > Service Accounts
   - Click "Create Service Account"
   - Grant the following roles:
     - BigQuery Data Editor
     - BigQuery Job User
5. Create and download the service account key:
   - Select your service account
   - Go to the "Keys" tab
   - Click "Add Key" > "Create New Key"
   - Choose JSON format
   - Save the downloaded file as `config/credentials.json`

### 2. Torn City API Key (`TC_API_key.txt`)
1. Log into [Torn City](https://www.torn.com)
2. Go to your API Key page: [https://www.torn.com/preferences.php#tab=api](https://www.torn.com/preferences.php#tab=api)
3. Generate a new API key or use an existing one
4. Create `config/TC_API_key.txt` and paste your API key:
   ```bash
   echo "your-api-key-here" > config/TC_API_key.txt
   ```
   The API key requires the following permissions:
   - Faction access
   - Basic user info

### 3. API Configuration (`TC_API_config.json`)
Create `config/TC_API_config.json` with your desired endpoints:
```json
[
  {
    "name": "v2_faction_crimes",
    "url": "https://api.torn.com/v2/faction/crimes?key={API_KEY}&cat=all&offset=0&sort=ASC",
    "table": "your-project.your_dataset.v2_faction_crimes"
  },
  {
    "name": "v2_faction_members",
    "url": "https://api.torn.com/v2/faction/members?key={API_KEY}&striptags=true",
    "table": "your-project.your_dataset.v2_faction_members"
  }
]
```
Replace:
- `your-project` with your Google Cloud project ID
- `your_dataset` with your BigQuery dataset name

### 4. Cron Schedule (`crontab`)
Create `config/crontab` to define the data collection schedule:
```bash
# Run TCdatalogger every 15 minutes
*/15 * * * * cd /app && /usr/local/bin/python /app/main.py >> /var/log/tcdatalogger/app.log 2>&1
```
Adjust the schedule as needed using [crontab syntax](https://crontab.guru/).

### Configuration Checklist
Before running the container, ensure:
- [ ] `credentials.json` exists and contains valid GCP service account credentials
- [ ] `TC_API_key.txt` exists and contains a valid Torn City API key
- [ ] `TC_API_config.json` exists and contains properly configured endpoints
- [ ] `crontab` exists and contains the desired schedule
- [ ] BigQuery API is enabled in your Google Cloud project
- [ ] The specified BigQuery dataset exists in your project

### File Permissions
Set appropriate permissions for configuration files:
```bash
chmod 600 config/credentials.json config/TC_API_key.txt
chmod 644 config/TC_API_config.json config/crontab
```

### Testing Configuration
To verify your configuration:
1. Create the required directories:
   ```bash
   mkdir -p config var/log/tcdatalogger
   chmod -R 777 var/log
   ```

2. Run a test container:
   ```bash
   docker run --rm \
     -v "$(pwd)/config:/app/config:ro" \
     -v "$(pwd)/var/log/tcdatalogger:/var/log/tcdatalogger" \
     tcdatalogger
   ```

3. Check the logs for any errors:
   ```bash
   tail -f var/log/tcdatalogger/app.log
   ```