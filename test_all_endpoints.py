import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
from app.common.common import process_data
from app.svcProviders.TornCity.TornCity import tc_load_api_key, tc_fetch_api_data

def load_api_config():
    """Load API configuration from file."""
    with open("./config/TC_API_config.json", "r") as f:
        return json.load(f)

def test_endpoint(api_config, api_key):
    """Test data flattening for a single endpoint."""
    print(f"\n{'='*80}")
    print(f"Testing endpoint: {api_config['name']}")
    print(f"{'='*80}")
    
    # Fetch data
    data = tc_fetch_api_data(api_config['url'], api_key)
    
    if not data:
        print(f"Error: Could not fetch data for {api_config['name']}")
        return
    
    # Process the data
    print(f"\n🔹 Processing {api_config['name']} data...")
    df = process_data(api_config['name'], data)
    
    # Print DataFrame info
    print("\n🔹 DataFrame Info:")
    print(df.info())
    
    print("\n🔹 DataFrame Head:")
    print(df.head())
    
    print("\n🔹 Column Types:")
    print(df.dtypes)
    
    # Print sample of any remaining nested data
    nested_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, str) and x.startswith('{')).any()]
    if nested_cols:
        print("\n🔹 Sample of nested data in columns:")
        for col in nested_cols[:3]:  # Show first 3 nested columns
            print(f"\n{col}:")
            print(df[col].iloc[0][:200] + "..." if len(df[col].iloc[0]) > 200 else df[col].iloc[0])
    
    return df

def main():
    # Load API key
    config_dir = "./config"
    api_key = tc_load_api_key(os.path.join(config_dir, "TC_API_key.txt"))
    
    if not api_key:
        print("Error: Could not load API key")
        return

    # Load API configurations
    api_configs = load_api_config()
    
    # Test each endpoint
    for api_config in api_configs:
        df = test_endpoint(api_config, api_key)
        
        if df is not None:
            print(f"\n✅ Successfully processed {api_config['name']}")
            print(f"   Shape: {df.shape}")
            print(f"   Columns: {len(df.columns)}")
            print(f"   Data types: {df.dtypes.value_counts().to_dict()}")

if __name__ == "__main__":
    main() 