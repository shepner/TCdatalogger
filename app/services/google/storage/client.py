"""Google Cloud Storage client for file operations.

This module provides functionality for interacting with Google Cloud Storage:
- Bucket management
- File upload/download
- File metadata management
- Access control
"""

from typing import Dict, Optional, BinaryIO
from google.cloud import storage
from google.api_core import retry

from app.services.google.base.client import BaseGoogleClient


class StorageClient(BaseGoogleClient):
    """Client for Google Cloud Storage operations.
    
    This class provides methods for:
    - Managing buckets
    - Uploading/downloading files
    - Managing file metadata
    - Controlling access
    """
    
    def __init__(self, config: Dict):
        """Initialize the Storage client.
        
        Args:
            config: Application configuration
        """
        super().__init__(config)
        self.client = storage.Client(
            credentials=self.credentials,
            project=self.project_id
        )
        
    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """Get a bucket reference.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Bucket: Storage bucket reference
        """
        return self.client.get_bucket(bucket_name)
        
    def upload_file(self, bucket_name: str, source_file: BinaryIO,
                   destination_blob_name: str,
                   content_type: Optional[str] = None) -> None:
        """Upload a file to Cloud Storage.
        
        Args:
            bucket_name: Name of the bucket
            source_file: File-like object to upload
            destination_blob_name: Name to give the uploaded file
            content_type: Content type of the file
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        if content_type:
            blob.content_type = content_type
            
        blob.upload_from_file(source_file)
        
    def download_file(self, bucket_name: str, source_blob_name: str,
                     destination_file: BinaryIO) -> None:
        """Download a file from Cloud Storage.
        
        Args:
            bucket_name: Name of the bucket
            source_blob_name: Name of the file to download
            destination_file: File-like object to write to
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_file(destination_file)
        
    def delete_file(self, bucket_name: str, blob_name: str) -> None:
        """Delete a file from Cloud Storage.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the file to delete
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        
    def file_exists(self, bucket_name: str, blob_name: str) -> bool:
        """Check if a file exists.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the file to check
            
        Returns:
            bool: Whether the file exists
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
        
    def list_files(self, bucket_name: str, prefix: Optional[str] = None) -> list:
        """List files in a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Filter results to files that begin with this prefix
            
        Returns:
            list: List of file names
        """
        bucket = self.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs] 