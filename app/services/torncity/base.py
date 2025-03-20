"""Base classes for Torn City API endpoint processors.

This module provides base classes for processing data from Torn City API endpoints:
- BaseEndpointProcessor: Abstract base class for all endpoint processors
- EndpointRegistry: Registry for managing endpoint processors
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Type, Union
import pandas as pd
from datetime import datetime
import time
import json
import numpy as np
import re

from google.cloud import bigquery
from google.cloud import monitoring_v3

from app.services.common.types import SchemaType, DataType, ConfigType, validate_schema
from app.services.torncity.client import TornClient
from app.services.torncity.exceptions import (
    EndpointError,
    SchemaError,
    ProcessingError,
    StorageError,
    DataValidationError
)
from app.services.google.bigquery.client import BigQueryClient


class BaseEndpointProcessor(ABC):
    """Base class for Torn City API endpoint processors."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize the BaseEndpointProcessor.

        Args:
            config: Configuration dictionary containing:
                - gcp_credentials_file: Path to Google Cloud credentials file
                - endpoint: Torn City API endpoint name
                - selection: Fields to select from the API response (optional)
                - storage_mode: Storage mode (either 'append' or 'replace')
                - api_key: Torn City API key (optional)
                - tc_api_key_file: Path to Torn City API keys file (optional)
                At least one of api_key or tc_api_key_file must be provided.

        Raises:
            ValueError: If the configuration is invalid.
        """
        self.validate_config(config)
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize endpoint configuration with default values
        self.endpoint_config = {
            'name': None,  # Set by child classes
            'url': None,   # Set by child classes
            'table': None, # Set by child classes
            'endpoint': config['endpoint'],
            'storage_mode': config.get('storage_mode', 'append'),
            'frequency': None  # Set by child classes
        }

        # Add selection to endpoint config if present
        if 'selection' in config:
            self.endpoint_config['selection'] = config['selection']

        # Initialize Torn client with API key
        self.torn_client = TornClient(
            api_key_or_file=config.get('api_key') or config.get('tc_api_key_file')
        )
        
        # Initialize BigQuery client
        self._bq_client = None
        self.gcp_credentials_file = config['gcp_credentials_file']
        
        self.schema_validator = None
        
    @property
    def bq_client(self) -> BigQueryClient:
        """Get or create the BigQuery client.

        Returns:
            BigQueryClient: The BigQuery client instance.
        """
        if self._bq_client is None:
            self._bq_client = BigQueryClient(self.gcp_credentials_file)
        return self._bq_client

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """Validate the configuration.

        Args:
            config: Configuration dictionary.

        Raises:
            ValueError: If the configuration is invalid.
        """
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")

        required_fields = [
            "gcp_credentials_file",
            "endpoint"
        ]

        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")

        # Validate storage mode if provided
        if "storage_mode" in config:
            if config["storage_mode"] not in ["append", "replace"]:
                raise ValueError("Invalid storage mode. Must be either 'append' or 'replace'")

        # Validate API key configuration
        if not config.get('api_key') and not config.get('tc_api_key_file'):
            raise ValueError("Either 'api_key' or 'tc_api_key_file' must be provided")

    def fetch_torn_data(self) -> Union[Dict[str, Any], List[Any]]:
        """Fetch data from the Torn City API using the configured endpoint and selection.

        Returns:
            Dictionary or List containing the API response data.

        Raises:
            Exception: If the API request fails.
        """
        try:
            # Use the API key specified in the endpoint config, fallback to default
            api_key_selection = self.endpoint_config.get('api_key', 'default')
            api_key = self.torn_client.api_keys.get(api_key_selection)
            if not api_key:
                raise ValueError(f"API key not found for selection: {api_key_selection}")

            # Get pagination settings from config
            pagination_config = self.config.get('defaults', {}).get('pagination', {})
            pagination_enabled = pagination_config.get('enabled', True)
            max_pages = pagination_config.get('max_pages')
            metadata_field = pagination_config.get('metadata_field', '_metadata')
            next_url_field = pagination_config.get('next_url_field', 'next')

            # Get the initial URL with API key
            url = self.endpoint_config['url'].replace('{API_KEY}', api_key)

            # Initialize data storage
            all_data = None
            current_url = url
            page_count = 0

            while current_url:
                # Check page limit if set
                if max_pages is not None and page_count >= max_pages:
                    logging.info(f"Reached maximum page limit of {max_pages}")
                    break

                # Fetch data from API
                data = self.torn_client.fetch_data(current_url)
                logging.info(f"API Response: {json.dumps(data, indent=2)}")

                if all_data is None:
                    all_data = data
                else:
                    # Merge data from subsequent pages
                    if 'data' in data and 'data' in all_data:
                        # For v2 API responses
                        for key in data['data']:
                            if isinstance(data['data'][key], list):
                                all_data['data'][key].extend(data['data'][key])
                            elif isinstance(data['data'][key], dict):
                                all_data['data'][key].update(data['data'][key])
                    else:
                        # For v1 API responses
                        if isinstance(data, dict):
                            all_data.update(data)
                        elif isinstance(data, list):
                            all_data.extend(data)

                # Check for next page if pagination is enabled
                current_url = None
                if pagination_enabled and 'error' not in data:
                    metadata = data.get(metadata_field, {})
                    next_url = metadata.get(next_url_field) if metadata else None
                    if next_url:
                        # Replace or add API key to next URL
                        if 'key=' in next_url:
                            current_url = re.sub(r'key=[^&]+', f'key={api_key}', next_url)
                        else:
                            current_url = f"{next_url}&key={api_key}"
                        page_count += 1
                        logging.info(f"Fetching page {page_count + 1}")

            return all_data

        except Exception as e:
            self._log_error(f"Failed to fetch data from Torn API: {str(e)}")
            raise

    def write_to_bigquery(self, data: Union[List[Dict], pd.DataFrame], table: Optional[str] = None) -> None:
        """Write data to BigQuery.
        
        Args:
            data: Data to write, either as a list of dictionaries or a DataFrame
            table: Optional table name. If not provided, uses the one from endpoint_config
            
        Raises:
            ValueError: If no table is specified and none is found in endpoint_config
        """
        if table is None:
            if 'table' not in self.endpoint_config:
                raise ValueError("No table specified and no table found in endpoint_config")
            table = self.endpoint_config['table']
            
        # Map storage modes to BigQuery write dispositions
        storage_mode = self.endpoint_config.get('storage_mode', 'append').lower()
        write_disposition_map = {
            'append': 'WRITE_APPEND',
            'truncate': 'WRITE_TRUNCATE',
            'empty': 'WRITE_EMPTY'
        }
        write_disposition = write_disposition_map.get(storage_mode, 'WRITE_APPEND')
            
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)
            
        try:
            self.bq_client.write_data(data, table, write_disposition=write_disposition)
        except Exception as e:
            self.logger.error(f"Failed to write data to BigQuery: {str(e)}")
            raise

    def run(self) -> None:
        """Run the processor to fetch and process data."""
        try:
            # Fetch data from the API
            data = self.fetch_torn_data()
            if not data:
                raise ValueError("No data received from API")
            
            # Log the raw API response for debugging
            logging.debug(f"Raw API response: {json.dumps(data, indent=2)}")
            
            # Process the data
            processed_data = self.process_data(data)
            if processed_data.empty:
                raise ValueError("No data to write - empty DataFrame")
            
            # Log the processed data for debugging
            logging.debug(f"Processed data shape: {processed_data.shape}")
            logging.debug(f"Processed data columns: {processed_data.columns.tolist()}")
            
            # Write the data to BigQuery
            self.write_to_bigquery(processed_data, self.endpoint_config['table'])
            logging.info(f"Successfully wrote {len(processed_data)} rows to {self.endpoint_config['table']}")
            
        except Exception as e:
            self.logger.error(f"Error running processor: {str(e)}")
            raise

    def _get_current_timestamp(self) -> str:
        """Get the current timestamp in ISO format.
        
        Returns:
            str: Current timestamp in ISO format
        """
        return datetime.fromtimestamp(int(time.time())).isoformat()

    def _format_timestamp(self, timestamp) -> Optional[str]:
        """Format a Unix timestamp as an ISO format string.
        
        Args:
            timestamp: Unix timestamp to format
            
        Returns:
            Optional[str]: Formatted timestamp or None if invalid
        """
        try:
            if timestamp is None:
                return datetime.now().isoformat()
            
            if isinstance(timestamp, str):
                try:
                    # Try parsing as ISO format first
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    return dt.isoformat()
                except ValueError:
                    # Try parsing as Unix timestamp
                    timestamp = float(timestamp)
            
            if isinstance(timestamp, (int, float)):
                if timestamp <= 0:
                    return datetime.now().isoformat()
                return datetime.fromtimestamp(timestamp).isoformat()
            
            return datetime.now().isoformat()
        except (ValueError, TypeError):
            return datetime.now().isoformat()

    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for this endpoint.
        
        Returns:
            List of SchemaField objects defining the table schema
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement get_schema()")

    def get_validator(self) -> 'SchemaValidator':
        """Get or create the schema validator.
        
        Returns:
            SchemaValidator instance for this endpoint's schema
        """
        if self.schema_validator is None:
            self.schema_validator = SchemaValidator(self.get_schema())
        return self.schema_validator

    def validate_data(self, data: Union[Dict[str, Any], pd.DataFrame]) -> Union[Dict[str, Any], pd.DataFrame]:
        """Validate data against the schema.
        
        Args:
            data: Data to validate (either a single record or DataFrame)
            
        Returns:
            Validated data with correct types
            
        Raises:
            DataValidationError: If validation fails
        """
        validator = self.get_validator()
        
        if isinstance(data, pd.DataFrame):
            return validator.validate_dataframe(data)
        else:
            return validator.validate_record(data)

    def get_quality_metrics(self, data: pd.DataFrame) -> Dict[str, float]:
        """Get data quality metrics.
        
        Args:
            data: DataFrame to analyze
            
        Returns:
            Dictionary of quality metrics
        """
        return self.get_validator().get_quality_metrics(data)

    def transform_data(self, data: Union[Dict[str, Any], List[Any]]) -> pd.DataFrame:
        """Transform raw API data into DataFrame format.
        
        Args:
            data: Raw API response data
            
        Returns:
            DataFrame containing transformed data
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement transform_data()")

    def process_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Process the raw API response data.

        Args:
            data: Raw API response data

        Returns:
            DataFrame containing processed data

        Raises:
            DataValidationError: If data validation fails
        """
        try:
            # Check for empty data
            if not data:
                logging.warning("No data received from API")
                return pd.DataFrame()

            # Check for API errors
            if 'error' in data:
                error_msg = data['error'].get('error', 'Unknown API error')
                raise DataValidationError(f"API returned an error: {error_msg}")

            # Handle v2 API response structure
            data_content = data.get('data', data)

            # Transform the data
            transformed_data = self.transform_data(data_content)
            if transformed_data.empty:
                logging.warning("No data after transformation")
                return pd.DataFrame()

            # Log validation results
            logging.info(f"Validated {len(transformed_data)} rows")
            logging.debug(f"DataFrame shape: {transformed_data.shape}")
            logging.debug(f"DataFrame columns: {transformed_data.columns.tolist()}")

            return transformed_data

        except Exception as e:
            self._log_error(f"Error processing data: {str(e)}")
            raise

    def process(self, data: Union[Dict[str, Any], List[Any]]) -> bool:
        """Process data from the endpoint.
        
        Args:
            data: Raw data from the API
            
        Returns:
            bool: True if processing succeeded
            
        Raises:
            DataValidationError: If data cannot be transformed
        """
        try:
            # Transform data
            transformed = self.transform_data(data)
            
            if not transformed:
                self._log_error("No data to process")
                return False
            
            # Convert to DataFrame if necessary
            if isinstance(transformed, list):
                df = pd.DataFrame(transformed)
            else:
                df = transformed
            
            if df.empty:
                self._log_error("No data to process")
                return False
            
            # Validate against schema
            schema = self.get_schema()
            df = self._validate_schema(df, schema)
            
            # Upload to BigQuery
            self._upload_data(df, schema)
            
            return True
            
        except SchemaError as e:
            self._log_error(f"Processing failed: {str(e)}")
            return False
            
        except Exception as e:
            self._log_error(str(e))
            return False

    def _validate_schema(self, df: pd.DataFrame, schema: List[bigquery.SchemaField]) -> pd.DataFrame:
        """Validate DataFrame against schema.
        
        Args:
            df: DataFrame to validate
            schema: List of SchemaField objects defining the schema
            
        Returns:
            DataFrame with validated and converted data types
            
        Raises:
            SchemaError: If validation fails
        """
        if df.empty:
            logging.warning("Empty DataFrame received for validation")
            return pd.DataFrame(columns=[field.name for field in schema])
        
        # Create mapping of field names to schema fields
        schema_fields = {field.name: field for field in schema}
        
        # Check required columns
        missing_cols = [
            field.name for field in schema 
            if field.mode == "REQUIRED" and field.name not in df.columns
        ]
        if missing_cols:
            logging.error(f"Missing required columns: {missing_cols}")
            raise SchemaError(f"Missing required columns: {missing_cols}")
        
        # Check for null values in required fields and fill with defaults
        for field in schema:
            if field.mode == "REQUIRED" and field.name in df.columns:
                if df[field.name].isnull().any():
                    logging.warning(f"Null values found in required field {field.name}, filling with defaults")
                    if field.field_type == "TIMESTAMP":
                        df[field.name] = df[field.name].fillna(pd.Timestamp.now())
                    elif field.field_type == "INTEGER":
                        df[field.name] = df[field.name].fillna(0).astype('int64')
                    elif field.field_type == "STRING":
                        df[field.name] = df[field.name].fillna("")
                    elif field.field_type == "FLOAT":
                        df[field.name] = df[field.name].fillna(0.0)
                    elif field.field_type == "BOOLEAN":
                        df[field.name] = df[field.name].fillna(False)
        
        # Validate data types
        for col in df.columns:
            if col in schema_fields:
                field = schema_fields[col]
                try:
                    df[col] = self._validate_column_type(df[col], field)
                except (ValueError, TypeError) as e:
                    logging.error(f"Invalid type for column {col}: {str(e)}")
                    raise SchemaError(f"Invalid type for column {col}: {str(e)}")
        
        return df

    def _validate_column_type(self, series: pd.Series, field: bigquery.SchemaField) -> pd.Series:
        """Validate a column's data type.
        
        Args:
            series: Column data to validate
            field: Expected schema field
            
        Returns:
            pd.Series: The validated and converted series
            
        Raises:
            ValueError: If validation fails
        """
        try:
            if field.field_type == "STRING":
                return series.fillna('').astype(str)
            elif field.field_type == "INTEGER":
                converted = pd.to_numeric(series, errors='coerce')
                if field.mode == 'REQUIRED':
                    converted = converted.fillna(0).astype('int64')
                    if not converted.dtype.kind in ['i', 'u']:  # Check if integer type
                        raise ValueError("Non-integer values found")
                    return converted.astype('int64')
                else:
                    # Use Int64 for nullable integers
                    return converted.astype('Int64')
            elif field.field_type == "FLOAT":
                converted = pd.to_numeric(series, errors='coerce')
                if field.mode == 'REQUIRED':
                    converted = converted.fillna(0.0)
                if not converted.dtype.kind in ['f', 'i', 'u']:  # Check if numeric type
                    raise ValueError("Non-numeric values found")
                return converted.astype('float64')
            elif field.field_type == "BOOLEAN":
                return series.fillna(False).astype(bool)
            elif field.field_type in ["DATETIME", "TIMESTAMP"]:
                if pd.api.types.is_datetime64_any_dtype(series):
                    return series
                
                # Convert various timestamp formats
                converted = pd.to_datetime(series, format='mixed', errors='coerce')
                
                # Only fill NaT with current time if field is required
                if field.mode == 'REQUIRED':
                    current_time = pd.Timestamp.now()
                    converted = converted.fillna(current_time)
                
                # Log any invalid timestamps
                invalid_mask = pd.isna(converted)
                if invalid_mask.any():
                    invalid_values = series[invalid_mask].tolist()
                    self._log_error(f"Invalid timestamp values found: {invalid_values}")
                
                return converted
            else:
                raise ValueError(f"Unsupported field type: {field.field_type}")
        except Exception as e:
            raise ValueError(f"Validation failed: {str(e)}")

    def _upload_data(self, df: pd.DataFrame, schema: List[bigquery.SchemaField]) -> None:
        """Upload data to BigQuery with proper schema management.
        
        Args:
            df: Processed DataFrame to upload
            schema: BigQuery table schema
        """
        try:
            # Use the table ID directly since it should be fully qualified
            table_id = self.endpoint_config['table']
            
            self.bq_client.upload_dataframe(
                df=df,
                table_id=table_id,
                write_disposition=self.endpoint_config['storage_mode']
            )
            
            # Record upload metrics
            self._record_metrics(
                upload_size=len(df),
                table_name=self.endpoint_config['table']
            )
            
        except Exception as e:
            self._log_error(f"Upload failed: {str(e)}")
            raise

    def _record_metrics(self, **metrics: Dict[str, Any]) -> None:
        """Record metrics to Cloud Monitoring.
        
        Metrics recorded:
        - Processing time
        - Success/failure counts
        - Error counts and types
        - Data volumes
        - API latency
        - Schema validation results
        """
        try:
            series = monitoring_v3.TimeSeries()
            series.metric.type = "custom.googleapis.com/tcdatalogger/endpoint"
            series.metric.labels.update({
                "endpoint": self.endpoint_config['name'],
                "storage_mode": self.endpoint_config['storage_mode'],
                "metric_type": "processing"
            })
            
            point = series.points.add()
            now = int(time.time())
            
            # Add all metrics as labels
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    point.value.double_value = value
                    series.metric.labels["metric_name"] = key
                elif isinstance(value, bool):
                    point.value.bool_value = value
                    series.metric.labels["metric_name"] = key
                else:
                    series.metric.labels[key] = str(value)
            
            point.interval.end_time.seconds = now
            
            self.monitoring_client.create_time_series(
                request={"name": self.project_path, "time_series": [series]}
            )
            
        except Exception as e:
            logging.warning(f"Failed to record metrics: {str(e)}")

    def _log_error(self, message: str) -> None:
        """Log an error with context."""
        logging.error({
            "event": "endpoint_error",
            "endpoint": self.endpoint_config['name'],
            "error": message,
            "timestamp": datetime.now().isoformat()
        })

    def _log_completion(self, success: bool, duration: float, error: Optional[str] = None) -> None:
        """Log completion status with metrics."""
        logging.info({
            "event": "endpoint_completion",
            "endpoint": self.endpoint_config['name'],
            "success": success,
            "duration_seconds": round(duration, 3),
            "error": error,
            "timestamp": datetime.now().isoformat()
        })

    def validate_schema(self, data: pd.DataFrame) -> None:
        """Validate that the data matches the schema.
        
        Args:
            data: DataFrame to validate
            
        Raises:
            SchemaError: If schema validation fails
        """
        schema = self.get_schema()
        required_fields = [field.name for field in schema if field.mode == "REQUIRED"]
        
        # Check for missing required fields
        missing_fields = [field for field in required_fields if field not in data.columns]
        if missing_fields:
            raise SchemaError(f"Missing required fields in DataFrame: {missing_fields}")
        
        # Check for null values in required fields
        for field in required_fields:
            if data[field].isnull().any():
                # Fill null values with defaults based on field type
                if field == 'server_timestamp':
                    data[field] = data[field].fillna(pd.Timestamp.now())
                elif field == 'id':
                    data[field] = data[field].fillna(0).astype('int64')
                elif field == 'name':
                    data[field] = data[field].fillna('Unknown')
                elif field == 'difficulty':
                    data[field] = data[field].fillna(0).astype('int64')
                elif field == 'status':
                    data[field] = data[field].fillna('Unknown')
                elif field == 'created_at':
                    data[field] = data[field].fillna(pd.Timestamp.now())
                else:
                    raise SchemaError(f"Found null values in required field: {field}")
        
        # Validate data types
        for field in schema:
            if field.name not in data.columns:
                continue
                
            values = data[field.name].dropna()
            if len(values) == 0:
                continue
                
            if field.field_type == "INTEGER":
                if not pd.api.types.is_numeric_dtype(values):
                    if field.mode == 'REQUIRED':
                        data[field.name] = pd.to_numeric(data[field.name], errors='coerce').fillna(0).astype('int64')
                    else:
                        data[field.name] = pd.to_numeric(data[field.name], errors='coerce').astype('Int64')
            elif field.field_type == "STRING":
                if not pd.api.types.is_string_dtype(values):
                    data[field.name] = data[field.name].fillna('').astype(str)
            elif field.field_type == "TIMESTAMP":
                if not pd.api.types.is_datetime64_any_dtype(values):
                    data[field.name] = pd.to_datetime(data[field.name], errors='coerce')
            elif field.field_type == "BOOLEAN":
                if not pd.api.types.is_bool_dtype(values):
                    data[field.name] = data[field.name].fillna(False).astype('boolean')

class SchemaValidator:
    """Handles schema validation for BigQuery data."""
    
    def __init__(self, schema: List[bigquery.SchemaField]):
        """Initialize the schema validator.
        
        Args:
            schema: List of BigQuery SchemaField objects defining the table schema
        """
        self.schema = {field.name: field for field in schema}
        self.required_fields = {
            name: field for name, field in self.schema.items() 
            if field.mode == 'REQUIRED'
        }
    
    def validate_field(self, name: str, value: Any) -> Any:
        """Validate and convert a single field value.
        
        Args:
            name: Field name
            value: Field value
            
        Returns:
            Converted value matching schema type
            
        Raises:
            DataValidationError: If validation fails
        """
        if name not in self.schema:
            raise DataValidationError(f"Unknown field: {name}")
            
        field = self.schema[name]
        
        # Handle NULL values
        if pd.isna(value) or value is None:
            if field.mode == 'REQUIRED':
                raise DataValidationError(f"Required field {name} cannot be NULL")
            return None
            
        try:
            # Convert based on BigQuery type
            if field.field_type == 'STRING':
                return str(value) if value is not None else None
                
            elif field.field_type == 'INTEGER':
                if isinstance(value, bool):
                    raise DataValidationError(f"Cannot convert boolean to integer for field {name}")
                if isinstance(value, str):
                    # Direct string to int conversion
                    value = int(value.strip())
                elif isinstance(value, float):
                    # Check if float is actually an integer
                    if value.is_integer():
                        value = int(value)
                    else:
                        raise DataValidationError(f"Float value {value} cannot be converted to integer for field {name}")
                return int(value)
                
            elif field.field_type == 'FLOAT':
                return float(value)
                
            elif field.field_type == 'BOOLEAN':
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes')
                return bool(value)
                
            elif field.field_type == 'TIMESTAMP':
                if isinstance(value, (int, float)):
                    return pd.Timestamp.fromtimestamp(value)
                elif isinstance(value, str):
                    try:
                        return pd.Timestamp(value)
                    except ValueError:
                        return pd.Timestamp.fromtimestamp(float(value))
                elif isinstance(value, datetime):
                    return pd.Timestamp(value)
                elif isinstance(value, pd.Timestamp):
                    return value
                else:
                    raise DataValidationError(f"Invalid timestamp format for field {name}: {value}")
                    
            else:
                raise DataValidationError(f"Unsupported field type: {field.field_type}")
                
        except (ValueError, TypeError) as e:
            raise DataValidationError(f"Invalid value for field {name} ({field.field_type}): {str(e)}")
    
    def validate_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a complete data record.
        
        Args:
            record: Dictionary containing field values
            
        Returns:
            Dictionary with validated and converted values
            
        Raises:
            DataValidationError: If validation fails
        """
        # Check for required fields
        missing_fields = set(self.required_fields) - set(record)
        if missing_fields:
            raise DataValidationError(f"Missing required fields: {', '.join(missing_fields)}")
        
        validated = {}
        for name, value in record.items():
            if name in self.schema:
                validated[name] = self.validate_field(name, value)
        
        return validated
    
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate a complete DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame with correct types
            
        Raises:
            DataValidationError: If validation fails
        """
        if df.empty:
            return df
            
        # Check for required columns
        missing_cols = set(self.required_fields) - set(df.columns)
        if missing_cols:
            raise DataValidationError(f"Missing required columns: {', '.join(missing_cols)}")
        
        # Validate each column
        for col in df.columns:
            if col in self.schema:
                df[col] = df[col].apply(lambda x: self.validate_field(col, x))
        
        return df
    
    def get_quality_metrics(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate data quality metrics.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary of quality metrics
        """
        metrics = {
            'total_rows': len(df),
            'null_percentage': df.isnull().mean().mean() * 100,
            'duplicate_rows': df.duplicated().sum(),
        }
        
        # Field-specific metrics
        for name, field in self.schema.items():
            if name in df.columns:
                metrics[f'{name}_null_count'] = df[name].isnull().sum()
                
                if field.field_type in ('INTEGER', 'FLOAT'):
                    non_null = df[name].dropna()
                    if not non_null.empty:
                        metrics[f'{name}_min'] = float(non_null.min())
                        metrics[f'{name}_max'] = float(non_null.max())
                        metrics[f'{name}_mean'] = float(non_null.mean())
                
                elif field.field_type == 'STRING':
                    non_null = df[name].dropna()
                    if not non_null.empty:
                        metrics[f'{name}_empty_count'] = (non_null == '').sum()
                        metrics[f'{name}_unique_count'] = non_null.nunique()
        
        return metrics 