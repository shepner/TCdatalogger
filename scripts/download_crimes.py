#!/usr/bin/env python3

import json
import requests
import logging
from pathlib import Path
from typing import Dict, List, Set

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_api_key():
    """Load API key from config file."""
    config_path = Path("config/TC_API_key.json")
    if not config_path.exists():
        raise FileNotFoundError(f"API key file not found: {config_path}")
    
    with open(config_path) as f:
        config = json.load(f)
        return config.get("faction_40832")

def fetch_all_crimes(api_key):
    """Fetch all crimes data from the API."""
    base_url = "https://api.torn.com/v2/faction/crimes"
    params = {
        "key": api_key,
        "cat": "all",
        "sort": "ASC"
    }
    
    all_crimes = []
    next_url = base_url
    page = 1
    
    while next_url:
        logger.info(f"Fetching page {page} from: {next_url}")
        # If it's not the base URL, append the API key
        if next_url != base_url:
            next_url = f"{next_url}&key={api_key}"
        
        response = requests.get(next_url, params=params if next_url == base_url else None)
        response.raise_for_status()
        
        data = response.json()
        crimes = data.get("crimes", [])
        all_crimes.extend(crimes)
        
        logger.info(f"Fetched {len(crimes)} crimes on page {page} (Total: {len(all_crimes)})")
        
        # Get next page URL from metadata
        metadata = data.get("_metadata", {})
        next_url = metadata.get("next")
        # Clear params as they're included in the next_url
        params = None
        page += 1
    
    return all_crimes

def analyze_crimes(crimes: List[Dict]) -> Dict:
    """Analyze a list of crimes and return statistics."""
    return {
        "total": len(crimes),
        "id_range": {
            "min": min(c["id"] for c in crimes),
            "max": max(c["id"] for c in crimes)
        },
        "unique_ids": len({c["id"] for c in crimes}),
        "id_gaps": find_id_gaps([c["id"] for c in crimes])
    }

def find_id_gaps(ids: List[int]) -> List[Dict]:
    """Find gaps in sequential IDs."""
    sorted_ids = sorted(ids)
    gaps = []
    for i in range(len(sorted_ids) - 1):
        if sorted_ids[i + 1] - sorted_ids[i] > 1:
            gaps.append({
                "start": sorted_ids[i],
                "end": sorted_ids[i + 1],
                "size": sorted_ids[i + 1] - sorted_ids[i] - 1
            })
    return gaps

def compare_crime_sets(current_crimes: List[Dict], ref_crimes: List[Dict]) -> None:
    """Compare two sets of crimes and log detailed analysis."""
    current_ids = {c["id"] for c in current_crimes}
    ref_ids = {c["id"] for c in ref_crimes}
    
    # Analyze both sets
    current_analysis = analyze_crimes(current_crimes)
    ref_analysis = analyze_crimes(ref_crimes)
    
    logger.info("\nAnalysis of current API response:")
    logger.info(f"Total crimes: {current_analysis['total']}")
    logger.info(f"ID range: {current_analysis['id_range']['min']} to {current_analysis['id_range']['max']}")
    logger.info(f"Unique IDs: {current_analysis['unique_ids']}")
    if current_analysis['id_gaps']:
        logger.info("Found gaps in current IDs:")
        for gap in current_analysis['id_gaps']:
            logger.info(f"  Gap from {gap['start']} to {gap['end']} (size: {gap['size']})")
    
    logger.info("\nAnalysis of reference file:")
    logger.info(f"Total crimes: {ref_analysis['total']}")
    logger.info(f"ID range: {ref_analysis['id_range']['min']} to {ref_analysis['id_range']['max']}")
    logger.info(f"Unique IDs: {ref_analysis['unique_ids']}")
    if ref_analysis['id_gaps']:
        logger.info("Found gaps in reference IDs:")
        for gap in ref_analysis['id_gaps']:
            logger.info(f"  Gap from {gap['start']} to {gap['end']} (size: {gap['size']})")
    
    # Compare the sets
    new_ids = current_ids - ref_ids
    missing_ids = ref_ids - current_ids
    
    logger.info("\nComparison results:")
    if new_ids:
        logger.info(f"Found {len(new_ids)} new crimes not in reference:")
        logger.info(f"New IDs: {sorted(new_ids)}")
    if missing_ids:
        logger.info(f"Found {len(missing_ids)} crimes from reference missing in current data:")
        logger.info(f"Missing IDs: {sorted(missing_ids)}")
    if not new_ids and not missing_ids:
        logger.info("Current data matches reference data exactly")

def main():
    try:
        api_key = load_api_key()
        crimes = fetch_all_crimes(api_key)
        
        # Save to file
        output_file = Path("data/response_current.json")
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, "w") as f:
            json.dump({"crimes": crimes}, f, indent=2)
        
        logger.info(f"\nSaved {len(crimes)} crimes to {output_file}")
        
        # Compare with reference file if it exists
        ref_file = Path("data/response_1742470596429.json")
        if ref_file.exists():
            with open(ref_file) as f:
                content = f.read()
                # Count JSON statements (objects/arrays)
                json_count = content.count('{') - content.count('{{')  # Count opening braces, excluding escaped
                logger.info(f"\nNumber of JSON statements in reference file: {json_count}")
                
                ref_data = json.load(f)
                ref_crimes = ref_data.get("crimes", [])
            
            compare_crime_sets(crimes, ref_crimes)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 