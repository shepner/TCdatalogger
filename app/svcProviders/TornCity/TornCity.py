
def tc_load_api_key(tc_api_key_file):

    """Read the API key from the file."""
    try:
        with open(tc_api_key_file, "r") as f:
            return f.read().strip()  # Ensure no extra spaces/newlines
    except FileNotFoundError:
        print(f"Error: API key file '{tc_api_key_file}' not found.")
        return None


import json
import requests

def tc_fetch_api_data(url, tc_api_key):
    """Fetch data from Torn City API, inserting the API key dynamically."""
    if tc_api_key is None:
        print("Error: API key is missing. Cannot proceed with API call.")
        return None

    # Replace placeholder `{API_KEY}` with the actual key
    formatted_url = url.replace("{API_KEY}", tc_api_key)
    print(f"Fetching data from: {formatted_url}")  # Debugging output

    try:
        response = requests.get(formatted_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

