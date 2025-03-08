import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
from app.common.common import process_data
from app.svcProviders.TornCity.TornCity import tc_load_api_key, tc_fetch_api_data

def test_faction_members():
    # Load API key
    config_dir = "./config"  # Adjust if needed
    api_key = tc_load_api_key(os.path.join(config_dir, "TC_API_key.txt"))
    
    if not api_key:
        print("Error: Could not load API key")
        return

    # Fetch faction members data
    url = "https://api.torn.com/v2/faction/members?key={API_KEY}&striptags=true"
    data = tc_fetch_api_data(url, api_key)
    
    if not data:
        print("Error: Could not fetch faction members data")
        return

    # Process the data
    print("\n🔹 Processing faction members data...")
    df = process_data("faction_members", data)
    
    # Print DataFrame info
    print("\n🔹 DataFrame Info:")
    print(df.info())
    
    print("\n🔹 DataFrame Head:")
    print(df.head())
    
    print("\n🔹 Column Types:")
    print(df.dtypes)
    
    print("\n🔹 Sample of nested data:")
    # Find columns that might contain nested data
    nested_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, str) and x.startswith('{')).any()]
    if nested_cols:
        print("\nSample of nested data in columns:")
        for col in nested_cols[:3]:  # Show first 3 nested columns
            print(f"\n{col}:")
            print(df[col].iloc[0][:200] + "..." if len(df[col].iloc[0]) > 200 else df[col].iloc[0])

if __name__ == "__main__":
    test_faction_members() 