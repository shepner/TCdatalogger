"""Common utilities for configuration management and scheduling.

This module provides utilities for:
- Configuration directory management
- Logging setup and configuration
- Schedule interval parsing and management
"""

import os
import json
import logging
from typing import Dict, List, Optional
from datetime import timedelta
import time

def setup_logging() -> None:
    """Configure logging for the application.
    
    Sets up logging to both console and file with appropriate format
    and log level.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('tcdata.log')
        ]
    )

def find_config_directory(directories: List[str]) -> Optional[str]:
    """Find the first valid configuration directory from a list of candidates.

    Args:
        directories: List of directory paths to search.

    Returns:
        str: Path to the first valid directory found, or None if no valid directory exists.
    """
    for directory in directories:
        if os.path.isdir(directory):
            return directory
    return None

def load_config(config_directories: List[str]) -> Optional[Dict]:
    """Load configuration from the first valid directory.
    
    Args:
        config_directories: List of directory paths to search.
        
    Returns:
        Dict: Configuration dictionary if successful, None otherwise.
    """
    config_dir = find_config_directory(config_directories)
    
    if not config_dir:
        logging.error("No configuration directory found.")
        return None
        
    config = {
        "config_dir": config_dir,
        "gcp_credentials_file": os.path.join(config_dir, "credentials.json"),
        "tc_api_key_file": os.path.join(config_dir, "TC_API_key.json"),
        "tc_api_config_file": os.path.join(config_dir, "TC_API_config.json"),
    }
    
    # Verify all required files exist
    for key, filepath in config.items():
        if key != "config_dir" and not os.path.exists(filepath):
            logging.error(f"Required configuration file not found: {filepath}")
            return None
            
    return config

def parse_iso_duration(duration: str) -> timedelta:
    """Parse an ISO 8601 duration string into a timedelta object.
    
    This function parses duration strings in the format P[n]Y[n]M[n]DT[n]H[n]M[n]S
    into a Python timedelta object. Note that years and months are converted to days
    (365 days for a year, 30 days for a month).
    
    Args:
        duration: ISO 8601 duration string (e.g., "PT15M", "PT1H", "P1D")
        
    Returns:
        timedelta: The parsed duration as a timedelta object
        
    Raises:
        ValueError: If the duration string is invalid
        
    Example:
        >>> parse_iso_duration("PT15M")
        timedelta(minutes=15)
        >>> parse_iso_duration("PT1H")
        timedelta(hours=1)
        >>> parse_iso_duration("P1D")
        timedelta(days=1)
    """
    if not isinstance(duration, str):
        raise ValueError(f"Duration must be a string, got {type(duration)}")
    
    if not duration or not duration.startswith('P'):
        raise ValueError(f"Invalid duration format: {duration}. Must start with 'P'")
    
    duration = duration[1:]  # Remove P
    
    # Initialize timedelta components
    days = 0
    hours = 0
    minutes = 0
    seconds = 0
    
    # Split into date and time parts if T is present
    if 'T' in duration:
        date_part, time_part = duration.split('T')
    else:
        date_part = duration
        time_part = ''
    
    # Parse date part
    remaining_date = date_part
    if date_part:
        # Years
        if 'Y' in remaining_date:
            parts = remaining_date.split('Y')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid years format in duration: {duration}")
            days += int(parts[0]) * 365
            remaining_date = parts[1]
        
        # Months
        if 'M' in remaining_date:
            parts = remaining_date.split('M')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid months format in duration: {duration}")
            days += int(parts[0]) * 30
            remaining_date = parts[1]
        
        # Days
        if 'D' in remaining_date:
            parts = remaining_date.split('D')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid days format in duration: {duration}")
            days += int(parts[0])
            remaining_date = parts[1]
        
        if remaining_date:
            raise ValueError(f"Invalid date format in duration: {duration}")
    
    # Parse time part
    remaining_time = time_part
    if time_part:
        # Hours
        if 'H' in remaining_time:
            parts = remaining_time.split('H')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid hours format in duration: {duration}")
            hours = int(parts[0])
            remaining_time = parts[1]
        
        # Minutes
        if 'M' in remaining_time:
            parts = remaining_time.split('M')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid minutes format in duration: {duration}")
            minutes = int(parts[0])
            remaining_time = parts[1]
        
        # Seconds
        if 'S' in remaining_time:
            parts = remaining_time.split('S')
            if len(parts) != 2 or not parts[0].isdigit():
                raise ValueError(f"Invalid seconds format in duration: {duration}")
            seconds = int(parts[0])
            remaining_time = parts[1]
        
        if remaining_time:
            raise ValueError(f"Invalid time format in duration: {duration}")
    
    # Ensure at least one valid component was found
    if days == 0 and hours == 0 and minutes == 0 and seconds == 0:
        raise ValueError(f"Duration must specify at least one time component: {duration}")
    
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def wait_for_next_poll(api_config: Dict) -> None:
    """Wait until the next polling interval based on the endpoint's frequency.
    
    This function calculates the next polling time based on the endpoint's frequency
    and waits until that time. It uses the ISO 8601 duration format to determine
    the interval.
    
    Args:
        api_config: API endpoint configuration dictionary containing the frequency
        
    Raises:
        ValueError: If frequency is missing or invalid
    """
    if 'frequency' not in api_config:
        raise ValueError(f"Missing frequency for endpoint {api_config['name']}")
    
    frequency = api_config['frequency']
    if not frequency:
        raise ValueError(f"Empty frequency for endpoint {api_config['name']}")
    
    try:
        interval = parse_iso_duration(frequency)
        interval_seconds = int(interval.total_seconds())  # Convert to integer seconds
        
        if interval_seconds > 0:
            logging.info("Waiting %d seconds until next poll for %s", 
                        interval_seconds, api_config['name'])
            time.sleep(interval_seconds)
    except ValueError as e:
        raise ValueError(f"Invalid frequency format '{frequency}' for endpoint {api_config['name']}: {str(e)}")
