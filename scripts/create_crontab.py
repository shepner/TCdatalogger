#!/usr/bin/env python3
"""Create crontab entries for TCdatalogger endpoints.

This script reads the TC_API_config.json file and creates appropriate
crontab entries for each endpoint using only standard library functions.
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path

def parse_iso_duration(duration: str) -> int:
    """Parse ISO 8601 duration string into minutes.
    
    Args:
        duration: ISO duration string (e.g., "PT15M", "PT1H", "P1D")
        
    Returns:
        int: Duration in minutes
    """
    # Extract numbers and units using regex
    pattern = r'P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?'
    match = re.match(pattern, duration)
    if not match:
        raise ValueError(f"Invalid duration format: {duration}")
    
    days, hours, minutes = match.groups()
    total_minutes = 0
    
    if days:
        total_minutes += int(days) * 24 * 60
    if hours:
        total_minutes += int(hours) * 60
    if minutes:
        total_minutes += int(minutes)
        
    return total_minutes

def duration_to_cron(minutes: int) -> str:
    """Convert duration in minutes to cron schedule expression.
    
    Args:
        minutes: Duration in minutes
        
    Returns:
        str: Cron schedule expression
    """
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

def main():
    """Generate crontab entries for all endpoints."""
    # Use container paths
    app_dir = Path("/opt/tcdatalogger")
    config_dir = Path("/app/config")
    scripts_dir = app_dir / "scripts"
    log_dir = Path("/app/var/log")
    
    # Load configuration
    config_file = config_dir / "TC_API_config.json"
    with open(config_file) as f:
        config = json.load(f)
    
    # Generate crontab entries
    cron_jobs = [
        "# TCdatalogger Automated Cron Jobs",
        "# DO NOT EDIT MANUALLY - Generated by create_crontab.py",
        "SHELL=/bin/bash",
        f"PATH=/usr/local/bin:/usr/bin:/bin:{scripts_dir}",
        f"PYTHONPATH={app_dir}",
        ""
    ]
    
    # Add jobs for each endpoint
    for endpoint in config.get("endpoints", []):
        name = endpoint["name"]
        frequency = endpoint.get("frequency", "PT15M")
        
        try:
            minutes = parse_iso_duration(frequency)
            schedule = duration_to_cron(minutes)
            
            # Create the cron command with environment setup and config directory
            cmd = f"cd {app_dir} && {scripts_dir}/setup.py main --endpoint {name} --config-dir {config_dir} >> {log_dir}/{name}.log 2>&1"
            cron_jobs.append(f"{schedule} {cmd}")
            
        except Exception as e:
            print(f"Error processing endpoint {name}: {e}")
    
    # Write crontab to standard location
    crontab_user = os.environ.get("CRON_USER", "tcdatalogger")
    crontab_file = Path(f"/var/spool/cron/crontabs/{crontab_user}")
    
    # Ensure proper permissions
    crontab_content = "\n".join(cron_jobs) + "\n"
    
    # Write to a temporary file first
    temp_file = Path("/tmp/crontab.tmp")
    with open(temp_file, "w") as f:
        f.write(crontab_content)
    
    # Use crontab command to install (handles permissions correctly)
    os.system(f"crontab -u {crontab_user} {temp_file}")
    
    # Cleanup
    temp_file.unlink()
    
    print(f"Installed crontab for user: {crontab_user}")

if __name__ == "__main__":
    main() 