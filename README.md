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

## Configuration

1. Create a `config` directory with the following files:
   - `credentials.json`: Google Cloud service account credentials
   - `TC_API_key.txt`: Your Torn City API key
   - `TC_API_config.json`: API endpoint configuration
   - `crontab`: Cron schedule configuration (provided)

Example `TC_API_config.json`:
```json
[
  {
    "name": "v2_faction_crimes",
    "url": "https://api.torn.com/v2/faction/crimes?key={API_KEY}&cat=all&offset=0&sort=ASC",
    "table": "your-project.dataset.v2_faction_crimes"
  },
  {
    "name": "v2_faction_members",
    "url": "https://api.torn.com/v2/faction/members?key={API_KEY}&striptags=true",
    "table": "your-project.dataset.v2_faction_members"
  }
]
```

## Docker Deployment

1. Build the Docker image:
```bash
docker build -t tcdatalogger .
```

2. Run the container:
```bash
docker run -d \
  --name tcdatalogger \
  --restart unless-stopped \
  -v /path/to/your/config:/app/config \
  tcdatalogger
```

The container will:
- Run the data pipeline immediately on startup
- Execute the pipeline every 15 minutes (configurable via crontab)
- Log all operations to the container's log
- Automatically restart on failure

View logs:
```bash
docker logs -f tcdatalogger
```

## Local Development

1. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the data pipeline:
```bash
python main.py
```

## Project Structure

```
TCdatalogger/
├── app/
│   ├── common/
│   │   ├── __init__.py
│   │   └── common.py         # Common utilities and data processing
│   └── svcProviders/
│       ├── Google/
│       │   ├── __init__.py
│       │   └── Google.py     # Google BigQuery integration
│       └── TornCity/
│           ├── __init__.py
│           └── TornCity.py   # Torn City API integration
├── config/
│   ├── TC_API_config.json    # API endpoint configuration
│   ├── credentials.json      # GCP credentials (not in repo)
│   ├── TC_API_key.txt       # Torn City API key (not in repo)
│   └── crontab              # Cron schedule configuration
├── tests/                    # Unit tests
├── Dockerfile               # Container definition
├── start.sh                # Container startup script
├── .gitignore
├── requirements.txt         # Project dependencies
├── main.py                 # Main application entry point
└── README.md               # Project documentation
```

## Development

### Running Tests
```bash
pytest tests/
```

### Adding New Endpoints

1. Add the endpoint configuration to `TC_API_config.json`
2. The data processing pipeline will automatically:
   - Handle nested structures
   - Convert timestamps
   - Infer and preserve data types
   - Create or update BigQuery tables

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Torn City API Documentation](https://api.torn.com/)
- [Google BigQuery Documentation](https://cloud.google.com/bigquery/docs)


