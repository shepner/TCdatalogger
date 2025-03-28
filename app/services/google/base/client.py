"""Base client for Google Cloud services.

This module provides a base client class for interacting with Google Cloud services.
It handles authentication and common functionality used by specific service clients.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from google.oauth2 import service_account
from google.auth.credentials import Credentials
import json
from google.cloud import storage, bigquery


class GoogleClient:
    """Base client for Google Cloud services."""
    
    def __init__(self, project_id: str, credentials_file: Optional[Path] = None):
        """Initialize the Google Cloud client.
        
        Args:
            project_id: Google Cloud project ID
            credentials_file: Path to service account credentials file
            
        Raises:
            ValueError: If credentials cannot be loaded
        """
        self.project_id = project_id
        self.credentials_file = credentials_file
        self.credentials = self._load_credentials()
        
    def _load_credentials(self) -> service_account.Credentials:
        """Load Google Cloud credentials.
        
        Returns:
            Credentials: Google Cloud credentials
            
        Raises:
            ValueError: If credentials cannot be loaded
        """
        try:
            if not self.credentials_file:
                raise ValueError("No credentials file provided")
                
            if not self.credentials_file.exists():
                raise FileNotFoundError(f"Credentials file not found: {self.credentials_file}")
                
            return service_account.Credentials.from_service_account_file(
                str(self.credentials_file)
            )
            
        except Exception as e:
            raise ValueError(f"Failed to load Google Cloud credentials: {str(e)}")
            
    def get_storage_client(self) -> storage.Client:
        """Get a Google Cloud Storage client.
        
        Returns:
            Client: Storage client
        """
        return storage.Client(
            project=self.project_id,
            credentials=self.credentials
        )
        
    def get_bigquery_client(self) -> bigquery.Client:
        """Get a Google BigQuery client.
        
        Returns:
            Client: BigQuery client
        """
        return bigquery.Client(
            project=self.project_id,
            credentials=self.credentials
        )

class BaseGoogleClient:
    """Base client for Google Cloud services.
    
    This class provides common functionality for all Google Cloud services:
    - Credentials management
    - Project configuration
    - Common utility functions
    
    All Google service clients should inherit from this class.
    """
    
    def __init__(self, project_id: str, credentials: Optional[Dict[str, Any]] = None):
        """Initialize the base Google Cloud client.

        Args:
            project_id: Google Cloud project ID
            credentials: Optional credentials dictionary or path to credentials file
        """
        self.project_id = project_id
        self.credentials = self._load_credentials(credentials)
        
    def _load_credentials(self, credentials: Optional[Dict[str, Any]]) -> Optional[service_account.Credentials]:
        """Load Google Cloud credentials.

        Args:
            credentials: Credentials dictionary or path to credentials file

        Returns:
            Credentials object or None if using default credentials

        Raises:
            ValueError: If credentials are invalid
        """
        if not credentials:
            # Try to load from environment variable
            creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_path and os.path.exists(creds_path):
                try:
                    return service_account.Credentials.from_service_account_file(creds_path)
                except Exception as e:
                    raise ValueError(f"Failed to load credentials from {creds_path}: {str(e)}")
            return None

        if isinstance(credentials, dict):
            try:
                return service_account.Credentials.from_service_account_info(credentials)
            except Exception as e:
                raise ValueError(f"Invalid credentials dictionary: {str(e)}")

        if isinstance(credentials, str) and os.path.exists(credentials):
            try:
                with open(credentials, 'r') as f:
                    creds_dict = json.load(f)
                return service_account.Credentials.from_service_account_info(creds_dict)
            except Exception as e:
                raise ValueError(f"Failed to load credentials from {credentials}: {str(e)}")

        raise ValueError("Invalid credentials format. Must be a dictionary, path to file, or None")
        
    def validate_config(self, required_fields: Optional[list] = None) -> None:
        """Validate service configuration.
        
        Args:
            required_fields: Optional list of required configuration fields
            
        Raises:
            ValueError: If required fields are missing
        """
        if required_fields:
            missing = [f for f in required_fields if f not in self.config]
            if missing:
                raise ValueError(f"Missing required configuration fields: {missing}")
                
    def get_project_id(self) -> str:
        """Get the Google Cloud project ID.
        
        Returns:
            str: Project ID
        """
        return self.project_id
