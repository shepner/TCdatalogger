"""Base class for Torn City API endpoint processors.

This class provides common functionality for endpoint processors:
- Configuration management
- Data transformation pipeline
- Schema management
- Monitoring and metrics
- Error handling and logging
"""

import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Optional, Any, List

import pandas as pd
from google.cloud import monitoring_v3, bigquery

from app.services.google.bigquery.client import BigQueryClient
from app.services.torncity.client import TornClient


class ProcessingError(Exception):
    """Base exception for processing errors."""
    pass


class SchemaError(ProcessingError):
    """Raised when there are schema-related issues."""
    pass


class DataValidationError(ProcessingError):
    """Raised when data validation fails."""
    pass


class BaseEndpointProcessor(ABC):
    """Base class for processing Torn City API endpoints."""

    def __init__(self, config: Dict[str, Any], endpoint_config: Dict[str, Any]):
        """Initialize the endpoint processor.
        
        Args:
            config: Application configuration dictionary
            endpoint_config: Endpoint-specific configuration dictionary
        """
        self.config = config
        self.endpoint_config = endpoint_config
        self.name = endpoint_config["name"]
        self.table = endpoint_config["table"]
        self.storage_mode = endpoint_config.get("storage_mode", "append")
        
        # Initialize clients
        self.torn_client = TornClient(config["tc_api_key_file"])
        self.bq_client = BigQueryClient(config)
        
        # Initialize monitoring
        self.monitoring_client = monitoring_v3.MetricServiceClient()
        self.project_path = self.monitoring_client.common_project_path(
            os.getenv('GCP_PROJECT_ID')
        )

    @abstractmethod
    def get_schema(self) -> List[bigquery.SchemaField]:
        """Get the BigQuery schema for this endpoint.
        
        Returns:
            List[bigquery.SchemaField]: The table schema
        """
        pass

    def process(self) -> bool:
        """Process the endpoint data.
        
        Orchestrates the entire process:
        1. Fetch data from API
        2. Transform and validate data
        3. Upload to BigQuery with schema management
        
        Returns:
            bool: True if processing was successful
        """
        start_time = time.time()
        success = False
        error_message = None
        
        try:
            # Fetch data using the client
            data = self.torn_client.fetch_data(
                self.endpoint_config["url"],
                self.endpoint_config.get("api_key", "default")
            )
            
            # Transform data
            df = self.transform_data(data)
            if df.empty:
                raise DataValidationError("No data transformed")
            
            # Validate against schema
            schema = self.get_schema()
            self._validate_schema(df, schema)
            
            # Upload to BigQuery
            self._upload_data(df, schema)
            
            success = True
            return True

        except Exception as e:
            error_message = str(e)
            self._log_error(f"Processing failed: {error_message}")
            
            # Record error metrics
            self._record_metrics(
                error_count=1,
                error_type=type(e).__name__
            )
            return False
        
        finally:
            duration = time.time() - start_time
            
            # Record final metrics
            self._record_metrics(
                processing_time=duration,
                success=success
            )
            
            # Log completion
            self._log_completion(
                success=success,
                duration=duration,
                error=error_message
            )

    @abstractmethod
    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform API response data into a DataFrame.
        
        Args:
            data: Raw API response data
            
        Returns:
            pd.DataFrame: Transformed data matching the schema
            
        Raises:
            DataValidationError: If data cannot be transformed
        """
        pass

    def _validate_schema(self, df: pd.DataFrame, schema: List[bigquery.SchemaField]) -> None:
        """Validate DataFrame against BigQuery schema.
        
        Args:
            df: DataFrame to validate
            schema: Expected BigQuery schema
            
        Raises:
            SchemaError: If validation fails
        """
        schema_fields = {field.name: field for field in schema}
        
        # Check required columns
        missing_cols = [
            field.name for field in schema 
            if not field.is_nullable and field.name not in df.columns
        ]
        if missing_cols:
            raise SchemaError(f"Missing required columns: {missing_cols}")
        
        # Validate data types
        for col in df.columns:
            if col in schema_fields:
                field = schema_fields[col]
                try:
                    self._validate_column_type(df[col], field)
                except (ValueError, TypeError) as e:
                    raise SchemaError(f"Invalid type for column {col}: {str(e)}")

    def _validate_column_type(self, series: pd.Series, field: bigquery.SchemaField) -> None:
        """Validate a column's data type.
        
        Args:
            series: Column data to validate
            field: Expected schema field
            
        Raises:
            ValueError: If validation fails
        """
        if field.field_type == "STRING":
            series.astype(str)
        elif field.field_type == "INTEGER":
            pd.to_numeric(series, errors='raise')
        elif field.field_type == "FLOAT":
            pd.to_numeric(series, errors='raise')
        elif field.field_type == "BOOLEAN":
            series.astype(bool)
        elif field.field_type == "DATETIME":
            pd.to_datetime(series, errors='raise')

    def _upload_data(self, df: pd.DataFrame, schema: List[bigquery.SchemaField]) -> None:
        """Upload data to BigQuery with proper schema management.
        
        Args:
            df: Processed DataFrame to upload
            schema: BigQuery table schema
        """
        try:
            # Construct fully qualified table ID
            table_id = f"{self.bq_client.project_id}.{self.config['dataset']}.{self.table}"
            
            self.bq_client.upload_dataframe(
                df=df,
                table_id=table_id,
                write_disposition=self.storage_mode
            )
            
            # Record upload metrics
            self._record_metrics(
                upload_size=len(df),
                table_name=self.table
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
                "endpoint": self.name,
                "storage_mode": self.storage_mode,
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
            "endpoint": self.name,
            "error": message,
            "timestamp": datetime.now().isoformat()
        })

    def _log_completion(self, success: bool, duration: float, error: Optional[str] = None) -> None:
        """Log completion status with metrics."""
        logging.info({
            "event": "endpoint_completion",
            "endpoint": self.name,
            "success": success,
            "duration_seconds": round(duration, 3),
            "error": error,
            "timestamp": datetime.now().isoformat()
        }) 