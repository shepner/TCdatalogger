#!/usr/bin/env python3
"""Set up cron jobs for TCdatalogger endpoints.

This script:
1. Reads the endpoint configurations
2. Creates appropriate cron entries for each endpoint
3. Installs the cron jobs
"""

import json
import os
import sys
import logging
from pathlib import Path
import subprocess
import isodate
from typing import Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_api_config(config_file: str) -> Dict:
    """Load the API configuration file."""
    try:
        with open(config_file) as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load API config: {e}")
        sys.exit(1)

def iso_duration_to_cron(duration: str) -> str:
    """Convert ISO 8601 duration to cron schedule.
    
    Args:
        duration: ISO duration string (e.g., "PT15M", "PT1H", "P1D")
        
    Returns:
        str: Cron schedule expression
    """
    try:
        td = isodate.parse_duration(duration)
        minutes = int(td.total_seconds() / 60)
        
        if minutes < 1:
            raise ValueError("Duration must be at least 1 minute")
        
        if minutes == 1440:  # Daily
            return "0 0 * * *"
        elif minutes == 720:  # 12 hours
            return "0 */12 * * *"
        elif minutes == 60:  # Hourly
            return "0 * * * *"
        elif minutes < 60:  # Minutes
            return f"*/{minutes} * * * *"
        else:  # Convert to hours if possible
            hours = minutes / 60
            if hours.is_integer():
                return f"0 */{int(hours)} * * *"
            return f"*/{minutes} * * *"
            
    except Exception as e:
        logging.error(f"Failed to parse duration {duration}: {e}")
        return None

def generate_cron_jobs(workspace_dir: Path, config: Dict) -> List[str]:
    """Generate cron job entries for all endpoints."""
    cron_jobs = []
    
    # Add comment header
    cron_jobs.extend([
        "# TCdatalogger Automated Cron Jobs",
        "# DO NOT EDIT MANUALLY - Managed by setup_cron.py",
        "SHELL=/bin/zsh",
        f"PATH=/usr/local/bin:/usr/bin:/bin:{workspace_dir}/scripts",
        f"WORKSPACE_DIR={workspace_dir}",
        ""
    ])
    
    # Add jobs for each endpoint
    for endpoint in config.get("endpoints", []):
        name = endpoint["name"]
        frequency = endpoint.get("frequency", "PT15M")
        schedule = iso_duration_to_cron(frequency)
        
        if schedule:
            # Create the cron command
            cmd = f"cd {workspace_dir} && ./scripts/setup.py main --endpoint {name} >> {workspace_dir}/logs/{name}.log 2>&1"
            cron_jobs.append(f"{schedule} {cmd}")
    
    return cron_jobs

def setup_cron_jobs(cron_jobs: List[str]) -> None:
    """Install the cron jobs."""
    try:
        # Write cron jobs to temporary file
        temp_file = "/tmp/tcdatalogger_cron"
        with open(temp_file, "w") as f:
            f.write("\n".join(cron_jobs) + "\n")
        
        # Install cron jobs
        result = subprocess.run(["crontab", temp_file], capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(f"Failed to install cron jobs: {result.stderr}")
            sys.exit(1)
        
        # Clean up
        os.unlink(temp_file)
        logging.info("Successfully installed cron jobs")
        
    except Exception as e:
        logging.error(f"Failed to setup cron jobs: {e}")
        sys.exit(1)

def main():
    """Main entry point."""
    try:
        # Get workspace directory
        workspace_dir = Path(__file__).parent.parent.absolute()
        
        # Ensure logs directory exists
        logs_dir = workspace_dir / "logs"
        logs_dir.mkdir(exist_ok=True)
        
        # Load configuration
        config_file = workspace_dir / "config" / "TC_API_config.json"
        config = load_api_config(str(config_file))
        
        # Generate cron jobs
        cron_jobs = generate_cron_jobs(workspace_dir, config)
        
        # Show preview
        logging.info("Generated cron jobs:")
        for job in cron_jobs:
            logging.info(job)
            
        # Confirm with user
        response = input("\nInstall these cron jobs? [y/N] ").lower()
        if response != 'y':
            logging.info("Aborted by user")
            sys.exit(0)
        
        # Install cron jobs
        setup_cron_jobs(cron_jobs)
        
    except Exception as e:
        logging.error(f"Setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 