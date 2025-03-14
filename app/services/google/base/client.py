"""Base client for Google Cloud services.

This module provides the base functionality for interacting with Google Cloud services:
- Authentication and credentials management
- Project configuration
- Common utility functions
"""

import os
from typing import Dict, Optional
from google.oauth2 import service_account
from google.auth.credentials import Credentials


class BaseGoogleClient:
    """Base client for Google Cloud services.
    
    This class provides common functionality for all Google Cloud services:
    - Credentials management
    - Project configuration
    - Common utility functions
    
    All Google service clients should inherit from this class.
    """
    
    def __init__(self, config: Dict):
        """Initialize the base client.
        
        Args:
            config: Application configuration containing Google credentials
        """
        self.config = config
        self.project_id = os.getenv('GCP_PROJECT_ID')
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable not set")
            
        self.credentials = self._get_credentials()
        
    def _get_credentials(self) -> Credentials:
        """Get Google Cloud credentials.
        
        Returns:
            Credentials: Google Cloud credentials
            
        Raises:
            ValueError: If credentials file is not found or invalid
        """
        credentials_file = self.config.get('gcp_credentials_file')
        if not credentials_file:
            raise ValueError("Google Cloud credentials file not specified")
            
        if not os.path.exists(credentials_file):
            raise ValueError(f"Credentials file not found: {credentials_file}")
            
        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            return credentials
        except Exception as e:
            raise ValueError(f"Failed to load credentials: {str(e)}")
            
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
