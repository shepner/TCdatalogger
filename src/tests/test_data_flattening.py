import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
from app.common.common import process_data
from app.svcProviders.Google.Google import upload_to_bigquery

def test_data_flattening():
    # Test data with various data types and nested structures
    test_data = {
        "test_data": [
            {
                "id": 1,
                "name": "Test 1",
                "score": 95.5,
                "is_active": True,
                "tags": ["tag1", "tag2"],
                "metadata": {
                    "created_at": "2024-01-01",
                    "updated_at": "2024-01-02"
                },
                "stats": {
                    "views": 100,
                    "likes": 50
                }
            },
            {
                "id": 2,
                "name": "Test 2",
                "score": 88.0,
                "is_active": False,
                "tags": ["tag3"],
                "metadata": {
                    "created_at": "2024-01-03",
                    "updated_at": "2024-01-04"
                },
                "stats": {
                    "views": 200,
                    "likes": 75
                }
            }
        ]
    }

    # Process the data
    print("\n🔹 Testing data processing...")
    df = process_data("test", test_data)
    
    # Print DataFrame info
    print("\n🔹 DataFrame Info:")
    print(df.info())
    
    print("\n🔹 DataFrame Head:")
    print(df.head())
    
    print("\n🔹 Column Types:")
    print(df.dtypes)

if __name__ == "__main__":
    test_data_flattening() 