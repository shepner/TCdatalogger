# TCdatalogger

A Python-based data pipeline that fetches data from the [Torn City API](https://api.torn.com/) and loads it into Google BigQuery for analysis.

## Features

- Fetches data from Torn City API v2 endpoints
- Handles nested JSON structures and list data
- Automatically flattens complex data structures
- Proper timestamp handling for all date/time fields
- Automatic schema updates in BigQuery
- Type inference and preservation
- Comprehensive error handling and logging

## Prerequisites

- Python 3.8+
- Google Cloud Platform account with BigQuery enabled
- Torn City API key with appropriate permissions
- Google Cloud credentials file

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd TCdatalogger
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `config` directory in the project root if it doesn't exist
2. Add the following configuration files:
   - `config/credentials.json`: Google Cloud service account credentials
   - `config/TC_API_key.txt`: Your Torn City API key
   - `config/TC_API_config.json`: API endpoint configuration (example provided)

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

## Usage

Run the data pipeline:
```bash
python main.py
```

The script will:
1. Load configuration from the config directory
2. Fetch data from configured Torn City API endpoints
3. Process and flatten the data
4. Upload the data to BigQuery
5. Log all operations to both console and `tcdata.log`

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
│   └── TC_API_key.txt       # Torn City API key (not in repo)
├── tests/                    # Unit tests
├── .gitignore
├── requirements.txt          # Project dependencies
├── main.py                  # Main application entry point
└── README.md                # Project documentation
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


