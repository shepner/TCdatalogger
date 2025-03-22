"""Common types and utilities shared across services.

This module contains shared type definitions and utilities to avoid circular imports.
"""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from google.cloud import bigquery

# Type aliases
SchemaType = List[bigquery.SchemaField]
DataType = List[Dict[str, Any]]
ConfigType = Dict[str, Any]

# Common validation functions
def validate_schema_field(field: bigquery.SchemaField) -> None:
    """Validate a single schema field.
    
    Args:
        field: The schema field to validate
        
    Raises:
        ValueError: If the field is invalid
    """
    if not field.name.isidentifier():
        raise ValueError(f"Invalid field name: {field.name}")
        
    if field.field_type not in {
        'STRING', 'INTEGER', 'FLOAT', 'BOOLEAN', 'TIMESTAMP',
        'DATE', 'TIME', 'DATETIME', 'NUMERIC', 'BYTES'
    }:
        raise ValueError(f"Invalid field type: {field.field_type}")
        
    if field.mode not in {'NULLABLE', 'REQUIRED', 'REPEATED'}:
        raise ValueError(f"Invalid field mode: {field.mode}")

def validate_schema(schema: SchemaType) -> None:
    """Validate a complete schema.
    
    Args:
        schema: The schema to validate
        
    Raises:
        ValueError: If the schema is invalid
    """
    if not schema:
        raise ValueError("Schema cannot be empty")
        
    for field in schema:
        validate_schema_field(field) 