# TCdatalogger Scripts Directory

This directory contains utility scripts for managing, deploying, and automating tasks for the TCdatalogger project. Below is a summary of each script and its usage.

---

## Shell Scripts

### `manage.sh`
Manages the TCdatalogger Docker container lifecycle.
- **Functions:**
  - Stops and removes any existing `tcdatalogger` containers
  - Builds and starts a new container using `docker compose`
- **Usage:**
  ```bash
  ./manage.sh
  ```

### `docker_deploy.sh`
Automates deployment of the application locally or to a remote server.
- **Functions:**
  - Creates required directories and sets permissions
  - Checks for required config files
  - Syncs files to remote host (if `--remote`)
  - Builds and starts containers (local or remote)
- **Usage:**
  ```bash
  ./docker_deploy.sh --local   # Deploy locally
  ./docker_deploy.sh --remote  # Deploy to remote server
  ```

### `drop_tables.sh`
Drops a predefined list of BigQuery tables from the `torn_data` dataset after user confirmation.
- **Usage:**
  ```bash
  ./drop_tables.sh
  ```
  The script will prompt for confirmation before proceeding.

---

## Python Scripts

### `setup.py`
Sets up the development environment or runs the main application.
- **Functions:**
  - Creates virtual environment and installs dependencies
  - Ensures required directory structure and config files
  - Can run tests, activate the venv, or run the main app
- **Usage:**
  ```bash
  ./setup.py setup         # Full environment setup
  ./setup.py test          # Run tests
  ./setup.py activate      # Activate the virtual environment
  ./setup.py run <cmd>     # Run a command in the venv
  ./setup.py main [args]   # Run the main application
  ```

### `create_crontab.py`
Generates and installs crontab entries for all endpoints defined in `TC_API_config.json` using only standard library functions.
- **Functions:**
  - Reads endpoint config and frequency
  - Converts ISO 8601 durations to cron expressions
  - Installs the generated crontab for the `tcdatalogger` user
- **Usage:**
  ```bash
  ./create_crontab.py
  ```

### `setup_cron.py`
Sets up cron jobs for all endpoints using the `isodate` library for ISO 8601 duration parsing.
- **Functions:**
  - Reads endpoint config and frequency
  - Converts ISO 8601 durations to cron expressions
  - Installs the generated cron jobs after user confirmation
- **Usage:**
  ```bash
  ./setup_cron.py
  ```

---

## Notes
- Some scripts require configuration files in the `config/` directory (e.g., `TC_API_config.json`).
- For deployment and cron setup, ensure you have the necessary permissions and dependencies installed.
- See each script's comments or `--help` (if available) for more details. 