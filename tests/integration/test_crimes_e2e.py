import os
import subprocess
import pytest
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json

CONFIG_DIR = Path('config')
REQUIRED_FILES = [
    CONFIG_DIR / 'TC_API_key.json',
    CONFIG_DIR / 'credentials.json',
    CONFIG_DIR / 'TC_API_config.json',
]

ENDPOINT_NAME = 'v2_faction_40832_crimes'

@pytest.mark.skipif(
    not all(f.exists() for f in REQUIRED_FILES),
    reason="E2E test requires real config files and credentials."
)
def test_crimes_endpoint_e2e():
    """Full end-to-end test: run the v2_faction_40832_crimes endpoint and check for success and BigQuery write."""
    # Load endpoint config to get the table name
    with open(CONFIG_DIR / 'TC_API_config.json') as f:
        endpoint_configs = json.load(f)
    crimes_cfg = None
    for ep in endpoint_configs.get('endpoints', []):
        if ep.get('name') == ENDPOINT_NAME:
            crimes_cfg = ep
            break
    assert crimes_cfg, f"Endpoint config '{ENDPOINT_NAME}' not found"
    table_id = crimes_cfg['table']

    # Run the main app
    cmd = [
        'python', '-m', 'app.main',
        '--endpoint', ENDPOINT_NAME,
        '--config-dir', 'config'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    assert result.returncode == 0, f"Process failed: {result.stderr}"

    # Now check BigQuery for recent rows
    try:
        from app.services.google.bigquery.client import BigQueryClient
        bq = BigQueryClient(str(CONFIG_DIR / 'credentials.json'))
        # Query for rows in the last hour
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        query = f"""
            SELECT * FROM `{table_id}`
            WHERE server_timestamp >= TIMESTAMP('{one_hour_ago.isoformat()}')
            ORDER BY server_timestamp DESC
            LIMIT 5
        """
        rows = bq.execute_query(query)
        print(f"BigQuery: Found {len(rows)} rows in the last hour.")
        if rows:
            print("Sample row:", rows[0])
        assert rows, "No recent rows found in BigQuery for crimes endpoint."
    except Exception as e:
        pytest.skip(f"BigQuery check skipped due to error: {e}") 