#!/usr/bin/env python3

import json
import os
import requests
import logging
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_existing_response(filename):
    """Load the existing response file and extract crime IDs."""
    with open(filename, 'r') as f:
        data = json.load(f)
    return {str(crime['id']): crime for crime in data['crimes']}

def format_timestamp(dt):
    """Format datetime object to Unix timestamp."""
    return int(dt.timestamp())

def fetch_crimes_data(api_key):
    """Fetch crimes data from the API using sliding 7-day windows."""
    logger = logging.getLogger(__name__)
    logger.info("Fetching new data from API")
    
    all_crimes = {}
    window_count = 1
    
    # Remove any 'key=' prefix from the API key if present
    api_key = api_key.replace('key=', '')
    
    # Start from current time
    end_time = datetime.now()
    
    while True:
        # Calculate start time for this window
        start_time = end_time - timedelta(days=7)
        
        # Format timestamps for API
        from_ts = format_timestamp(start_time)
        to_ts = format_timestamp(end_time)
        
        # Construct URL for this time window
        current_url = (
            f"https://api.torn.com/v2/faction/crimes?"
            f"key={api_key}&cat=all&sort=DESC"
            f"&from={from_ts}&to={to_ts}"
        )
        
        logger.info(f"Fetching window {window_count}: {start_time.date()} to {end_time.date()}")
        logger.info(f"Making request to: {current_url}")
        
        response = requests.get(current_url)
        data = response.json()
        
        if not data.get("crimes"):
            logger.info("No crimes found in this time window")
            break
            
        crimes = data["crimes"]
        if not crimes:
            logger.info("No crimes found in this time window")
            break
            
        crime_ids = [crime['id'] for crime in crimes]
        logger.info(f"First crime in window: ID {min(crime_ids)}")
        logger.info(f"Last crime in window: ID {max(crime_ids)}")
        logger.info(f"Retrieved {len(crimes)} crimes from window {window_count}")
        
        # Track new crimes added
        new_crimes = {str(crime['id']): crime for crime in crimes if str(crime['id']) not in all_crimes}
        logger.info(f"Added {len(new_crimes)} new crimes")
        
        if new_crimes:
            logger.info(f"New crime IDs: {sorted(map(int, new_crimes.keys()))}")
            all_crimes.update(new_crimes)
            
        logger.info(f"Total crimes so far: {len(all_crimes)}")
        
        # Move window back in time
        end_time = start_time
        window_count += 1
        
        # If we didn't add any new crimes, we're done
        if not new_crimes:
            logger.info("No new crimes found, stopping time window progression")
            break
        
        # Add a small delay to avoid hitting rate limits
        time.sleep(1)
    
    return all_crimes

def compare_responses(existing_crimes, new_crimes):
    """Compare existing and new crime responses."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Existing response has {len(existing_crimes)} crimes")
    logger.info(f"New response has {len(new_crimes)} crimes")
    
    # Convert all IDs to integers for comparison
    existing_ids = set(map(int, existing_crimes.keys()))
    new_ids = set(map(int, new_crimes.keys()))
    
    # Print ID ranges
    logger.info(f"Existing response ID range: {min(existing_ids)} to {max(existing_ids)}")
    logger.info(f"New response ID range: {min(new_ids)} to {max(new_ids)}")
    
    # Check for missing crimes
    missing_crimes = [str(id) for id in existing_ids if id not in new_ids]
    if missing_crimes:
        logger.warning(f"Crimes in existing but missing in new: {sorted(missing_crimes)}")
    
    # Check for new crimes
    new_found_crimes = [str(id) for id in new_ids if id not in existing_ids]
    if new_found_crimes:
        logger.warning(f"Crimes in new but missing in existing: {sorted(new_found_crimes)}")
    
    # Check for gaps in crime IDs
    all_ids = sorted(new_ids)  # Already integers
    for i in range(len(all_ids) - 1):
        if all_ids[i + 1] - all_ids[i] > 1:
            logger.warning(f"Gap in crime IDs between {all_ids[i]} and {all_ids[i + 1]}")

def main():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('FACTION_40832_API_KEY')
    
    if not api_key:
        logger.error("API key not found in environment variables")
        return
        
    # API key should already include 'key=' prefix
    if not api_key.startswith('key='):
        logger.error("API key must start with 'key='")
        return
    
    # Load existing response
    existing_file = 'response_1742470596429.json'
    logger.info(f"Loading existing response from {existing_file}")
    existing_crimes = load_existing_response(existing_file)
    
    # Fetch new data
    logger.info("Fetching new data from API")
    new_crimes = fetch_crimes_data(api_key)
    
    # Compare responses
    logger.info("Comparing responses")
    compare_responses(existing_crimes, new_crimes)

if __name__ == '__main__':
    main() 