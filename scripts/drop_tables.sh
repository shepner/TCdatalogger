#!/bin/bash

# Script to drop all BigQuery tables in the torn_data dataset

# List of tables to drop
TABLES=(
    "server_timestamp"
    "v2_faction_40832_crimes"
    "v2_faction_17991_crimes"
    "v2_faction_40832_members"
    "v2_faction_17991_members"
    "v2_torn_items"
    "v2_faction_40832_basic"
    "v2_faction_17991_basic"
    "v2_faction_40832_currency"
    "v2_faction_17991_currency"
)

# Project and dataset configuration
PROJECT="torncity-402423"
DATASET="torn_data"

# Print what we're about to do
echo "About to drop the following tables from ${PROJECT}.${DATASET}:"
for table in "${TABLES[@]}"; do
    echo "  - $table"
done

# Ask for confirmation
read -p "Are you sure you want to proceed? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operation cancelled"
    exit 1
fi

# Drop each table
for table in "${TABLES[@]}"; do
    echo "Dropping table: $table"
    bq rm -f "${PROJECT}:${DATASET}.${table}"
    if [ $? -eq 0 ]; then
        echo "  Success"
    else
        echo "  Failed to drop table: $table"
    fi
done

echo "All tables dropped successfully" 