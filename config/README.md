# Configuration Directory

This directory contains configuration files for the TCdatalogger application. Each file serves a specific purpose and should be properly configured before running the application.

## Configuration Files

### TC_API_config.json
Main configuration file for Torn City API endpoints. This file defines:
- API endpoints and their configurations
- Data collection frequencies
- Storage modes
- Default retry and rate limit settings

Copy `TC_API_config.json.example` to `TC_API_config.json` and update with your settings.

### TC_API_key.json
Contains API keys for accessing the Torn City API. This file defines:
- Default API key
- Faction-specific API keys

Copy `TC_API_key.json.example` to `TC_API_key.json` and update with your API keys.

### app_config.json
Application-wide configuration settings. This file defines:
- Google Cloud credentials location
- Logging configuration
- Monitoring settings

Copy `app_config.json.example` to `app_config.json` and update with your settings.

### credentials.json
Google Cloud Service Account credentials for BigQuery access. This file should be obtained from the Google Cloud Console.

Copy `credentials.json.example` to `credentials.json` and replace with your actual service account credentials file.

## Setup Instructions

1. Copy each `.example` file to create the actual configuration file:
   ```bash
   cp TC_API_config.json.example TC_API_config.json
   cp TC_API_key.json.example TC_API_key.json
   cp app_config.json.example app_config.json
   cp credentials.json.example credentials.json
   ```

2. Update each file with your specific settings:
   - Add your Torn City API keys to `TC_API_key.json`
   - Configure your endpoints in `TC_API_config.json`
   - Set your application preferences in `app_config.json`
   - Replace `credentials.json` with your Google Cloud service account key file

3. Verify file permissions:
   ```bash
   chmod 600 TC_API_key.json credentials.json
   ```

## Configuration Details

### Frequency Format
The application uses ISO 8601 duration format for scheduling:
- `PT15M` = 15 minutes
- `PT1H` = 1 hour
- `P1D` = 1 day
- `PT1H30M` = 1 hour and 30 minutes

### Storage Modes
- `append`: Add new records to existing data
- `replace`: Replace all existing data with new data

### Log Levels
Valid log levels in order of verbosity:
- `DEBUG`: Detailed information for debugging
- `INFO`: General information about program execution
- `WARNING`: Indicate a potential problem
- `ERROR`: A more serious problem
- `CRITICAL`: Program may be unable to continue

### Security Notes
- Keep API keys and credentials secure
- Don't commit sensitive files to version control
- Use appropriate file permissions
- Monitor API usage and rate limits 