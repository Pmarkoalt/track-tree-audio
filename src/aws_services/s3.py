"""S3/R2 service with tenacity retry logic."""

import os
import logging
from typing import Optional, List, Dict, Any
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
    retry_if_exception_type,
    retry_if_exception,
    before_sleep_log,
    after_log
)

from .env import settings
from aws_services.constants import (
    S3_DEFAULT_MAX_RETRIES,
    S3_DEFAULT_MIN_DELAY,
    S3_DEFAULT_MAX_DELAY,
    S3_DEFAULT_EXPONENTIAL_BASE,
    S3_DEFAULT_JITTER,
    S3_RETRYABLE_EXCEPTIONS,
    S3_RETRYABLE_ERROR_CODES
)

logger = logging.getLogger(__name__)

# Retryable exception types
RETRYABLE_EXCEPTIONS = (
    ClientError,
    BotoCoreError,
    ConnectionError,
    TimeoutError
)


def _should_retry_s3_error(retry_state):
    """Custom retry condition for S3-specific errors."""
    exception = retry_state.outcome.exception()
    
    if isinstance(exception, (ClientError, BotoCoreError)):
        error_code = getattr(exception, 'response', {}).get('Error', {}).get('Code', '')
        return error_code in S3_RETRYABLE_ERROR_CODES
    
    return False


class S3Service:
    """S3/R2 service with tenacity retry logic."""
    
    def __init__(
        self,
        max_retries: int = S3_DEFAULT_MAX_RETRIES,
        min_delay: float = S3_DEFAULT_MIN_DELAY,
        max_delay: float = S3_DEFAULT_MAX_DELAY,
        exponential_base: float = S3_DEFAULT_EXPONENTIAL_BASE,
        jitter: bool = S3_DEFAULT_JITTER
    ):
        """
        Initialize S3 service with retry configuration.
        
        Args:
            max_retries: Maximum number of retry attempts
            min_delay: Minimum delay in seconds for exponential backoff
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential backoff calculation
            jitter: Whether to add random jitter to delays
        """
        self.max_retries = max_retries
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self._client = None
    
    @property
    def client(self):
        """Get S3 client with lazy initialization."""
        if self._client is None:
            self._client = self._create_client()
        return self._client
    
    def _create_client(self):
        """Create configured S3 client."""
        try:
            if settings.s3_access_key_id and settings.s3_secret_access_key:
                # Use provided credentials
                client = boto3.client(
                    's3',
                    endpoint_url=settings.s3_endpoint if settings.s3_endpoint else None,
                    region_name=settings.s3_region,
                    aws_access_key_id=settings.s3_access_key_id,
                    aws_secret_access_key=settings.s3_secret_access_key
                )
            else:
                # Use default credentials (IAM role, environment, etc.)
                client = boto3.client(
                    's3',
                    endpoint_url=settings.s3_endpoint if settings.s3_endpoint else None,
                    region_name=settings.s3_region
                )
            
            return client
            
        except NoCredentialsError:
            logger.error("No AWS credentials found")
            raise
        except Exception as e:
            logger.error(f"Failed to create S3 client: {e}")
            raise
    
    def _get_bucket(self, bucket: Optional[str] = None) -> str:
        """Get bucket name, using default if not provided."""
        if not bucket:
            bucket = settings.s3_bucket
        
        if not bucket:
            raise ValueError("S3 bucket not configured")
        
        return bucket
    
    def _construct_url(self, s3_key: str, bucket: str) -> str:
        """Construct S3 URL for object."""
        if settings.s3_endpoint:
            # Custom endpoint (e.g., R2)
            return f"{settings.s3_endpoint.rstrip('/')}/{bucket}/{s3_key}"
        else:
            # Standard S3
            return f"https://{bucket}.s3.{settings.s3_region}.amazonaws.com/{s3_key}"
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def upload_file(self, file_path: str, s3_key: str, bucket: Optional[str] = None) -> str:
        """
        Upload file to S3/R2 with retry logic.
        
        Args:
            file_path: Local file path to upload
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            S3 URL of uploaded file
        """
        bucket = self._get_bucket(bucket)
        self.client.upload_file(file_path, bucket, s3_key)
        url = self._construct_url(s3_key, bucket)
        logger.info(f"Uploaded {file_path} to {url}")
        return url
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def download_file(self, s3_key: str, local_path: str, bucket: Optional[str] = None) -> str:
        """
        Download file from S3/R2 with retry logic.
        
        Args:
            s3_key: S3 object key
            local_path: Local file path to save to
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            Local file path
        """
        bucket = self._get_bucket(bucket)
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self.client.download_file(bucket, s3_key, local_path)
        logger.info(f"Downloaded {s3_key} to {local_path}")
        return local_path
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def generate_presigned_put_url(self, s3_key: str, bucket: Optional[str] = None, expires_in: int = 3600) -> str:
        """
        Generate presigned PUT URL for direct upload with retry logic.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            expires_in: URL expiration time in seconds
            
        Returns:
            Presigned PUT URL
        """
        bucket = self._get_bucket(bucket)
        url = self.client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expires_in
        )
        logger.info(f"Generated presigned PUT URL for {s3_key}")
        return url
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def generate_presigned_get_url(self, s3_key: str, bucket: Optional[str] = None, expires_in: int = 3600) -> str:
        """
        Generate presigned GET URL for direct download with retry logic.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            expires_in: URL expiration time in seconds
            
        Returns:
            Presigned GET URL
        """
        bucket = self._get_bucket(bucket)
        url = self.client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expires_in
        )
        logger.info(f"Generated presigned GET URL for {s3_key}")
        return url
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def object_exists(self, s3_key: str, bucket: Optional[str] = None) -> bool:
        """
        Check if object exists in S3/R2 with retry logic.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            True if object exists, False otherwise
        """
        bucket = self._get_bucket(bucket)
        try:
            self.client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def delete_object(self, s3_key: str, bucket: Optional[str] = None) -> bool:
        """
        Delete object from S3/R2 with retry logic.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            True if deleted successfully
        """
        bucket = self._get_bucket(bucket)
        self.client.delete_object(Bucket=bucket, Key=s3_key)
        logger.info(f"Deleted object {s3_key}")
        return True
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def delete_objects(self, s3_keys: List[str], bucket: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete multiple objects from S3/R2 with retry logic.
        
        Args:
            s3_keys: List of S3 object keys
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            Response containing deleted and failed objects
        """
        bucket = self._get_bucket(bucket)
        
        if not s3_keys:
            return {"Deleted": [], "Errors": []}
        
        delete_objects = [{'Key': key} for key in s3_keys]
        response = self.client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': delete_objects}
        )
        
        deleted_count = len(response.get('Deleted', []))
        error_count = len(response.get('Errors', []))
        logger.info(f"Deleted {deleted_count} objects, {error_count} errors")
        return response
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def copy_object(self, source_key: str, dest_key: str, source_bucket: Optional[str] = None, dest_bucket: Optional[str] = None) -> bool:
        """
        Copy object within S3/R2 with retry logic.
        
        Args:
            source_key: Source S3 object key
            dest_key: Destination S3 object key
            source_bucket: Source bucket (uses settings.s3_bucket if not provided)
            dest_bucket: Destination bucket (uses settings.s3_bucket if not provided)
            
        Returns:
            True if copied successfully
        """
        source_bucket = self._get_bucket(source_bucket)
        dest_bucket = self._get_bucket(dest_bucket)
        
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        self.client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        logger.info(f"Copied {source_key} to {dest_key}")
        return True
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_metadata(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """
        Get object metadata from S3/R2 with retry logic.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            Object metadata dictionary
        """
        bucket = self._get_bucket(bucket)
        response = self.client.head_object(Bucket=bucket, Key=s3_key)
        
        metadata = {
            'size': response.get('ContentLength'),
            'last_modified': response.get('LastModified'),
            'etag': response.get('ETag'),
            'storage_class': response.get('StorageClass', 'STANDARD'),
            'content_type': response.get('ContentType'),
            'metadata': response.get('Metadata', {})
        }
        
        logger.info(f"Retrieved metadata for {s3_key}")
        return metadata
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def change_storage_class(self, s3_key: str, storage_class: str, bucket: Optional[str] = None) -> bool:
        """
        Change object storage class in S3/R2 with retry logic.
        
        Args:
            s3_key: S3 object key
            storage_class: New storage class (STANDARD, STANDARD_IA, GLACIER, etc.)
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            True if changed successfully
        """
        bucket = self._get_bucket(bucket)
        
        copy_source = {'Bucket': bucket, 'Key': s3_key}
        self.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket,
            Key=s3_key,
            StorageClass=storage_class
        )
        logger.info(f"Changed storage class for {s3_key} to {storage_class}")
        return True
    
    def get_storage_class(self, s3_key: str, bucket: Optional[str] = None) -> str:
        """Get object storage class from S3/R2."""
        metadata = self.get_object_metadata(s3_key, bucket)
        return metadata.get('storage_class', 'STANDARD')
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def list_objects(self, prefix: str = "", bucket: Optional[str] = None, max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in S3/R2 bucket with retry logic.
        
        Args:
            prefix: Object key prefix to filter by
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            max_keys: Maximum number of objects to return
            
        Returns:
            List of object metadata dictionaries
        """
        bucket = self._get_bucket(bucket)
        response = self.client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=max_keys
        )
        objects = response.get('Contents', [])
        logger.info(f"Listed {len(objects)} objects with prefix '{prefix}'")
        return objects
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def list_object_versions(self, s3_key: str, bucket: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List object versions in S3/R2 bucket with retry logic.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            List of object version metadata dictionaries
        """
        bucket = self._get_bucket(bucket)
        response = self.client.list_object_versions(
            Bucket=bucket,
            Prefix=s3_key
        )
        versions = response.get('Versions', [])
        logger.info(f"Listed {len(versions)} versions for {s3_key}")
        return versions
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_tags(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, str]:
        """
        Get object tags from S3/R2 with retry logic.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            Dictionary of tag key-value pairs
        """
        bucket = self._get_bucket(bucket)
        response = self.client.get_object_tagging(Bucket=bucket, Key=s3_key)
        tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        logger.info(f"Retrieved {len(tags)} tags for {s3_key}")
        return tags
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def set_object_tags(self, s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
        """
        Set object tags in S3/R2 with retry logic.
        
        Args:
            s3_key: S3 object key
            tags: Dictionary of tag key-value pairs
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            
        Returns:
            True if tags set successfully
        """
        bucket = self._get_bucket(bucket)
        
        if len(tags) > 10:
            raise ValueError("S3 supports maximum 10 tags per object")
        
        for key, value in tags.items():
            if len(key) > 50 or len(value) > 50:
                raise ValueError("Tag keys and values must be 50 characters or less")
        
        tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
        self.client.put_object_tagging(
            Bucket=bucket,
            Key=s3_key,
            Tagging={'TagSet': tag_set}
        )
        logger.info(f"Set {len(tags)} tags for {s3_key}")
        return True
    
    def update_object_tags(self, s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
        """Update object tags in S3/R2 (merges with existing tags) with retry logic."""
        try:
            # Get existing tags
            existing_tags = self.get_object_tags(s3_key, bucket)
            
            # Merge with new tags
            existing_tags.update(tags)
            
            # Set all tags
            return self.set_object_tags(s3_key, existing_tags, bucket)
            
        except Exception as e:
            logger.error(f"Failed to update object tags: {e}")
            raise
    
    def delete_object_tags(self, s3_key: str, tag_keys: List[str], bucket: Optional[str] = None) -> bool:
        """Delete specific tags from S3/R2 object with retry logic."""
        try:
            # Get existing tags
            existing_tags = self.get_object_tags(s3_key, bucket)
            
            # Remove specified tags
            for key in tag_keys:
                existing_tags.pop(key, None)
            
            # Set remaining tags
            return self.set_object_tags(s3_key, existing_tags, bucket)
            
        except Exception as e:
            logger.error(f"Failed to delete object tags: {e}")
            raise
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def list_objects_by_tags(self, tag_filters: Dict[str, str], bucket: Optional[str] = None, prefix: str = "") -> List[Dict[str, Any]]:
        """
        List objects filtered by tags with retry logic.
        
        Note: This is a simplified implementation that scans objects.
        For production, consider using S3 Inventory or CloudTrail.
        
        Args:
            tag_filters: Dictionary of tag key-value pairs to filter by
            bucket: S3 bucket name (uses settings.s3_bucket if not provided)
            prefix: Object key prefix to limit search
            
        Returns:
            List of objects matching tag filters
        """
        bucket = self._get_bucket(bucket)
        matching_objects = []
        
        # List all objects with prefix
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            for obj in page.get('Contents', []):
                obj_key = obj['Key']
                
                try:
                    # Get tags for this object
                    obj_tags = self.get_object_tags(obj_key, bucket)
                    
                    # Check if all filter tags match
                    if all(obj_tags.get(k) == v for k, v in tag_filters.items()):
                        obj['Tags'] = obj_tags
                        matching_objects.append(obj)
                        
                except Exception as e:
                    # Skip objects that can't be tagged or don't exist
                    logger.debug(f"Could not get tags for {obj_key}: {e}")
                    continue
        
        logger.info(f"Found {len(matching_objects)} objects matching tag filters")
        return matching_objects
    
    @retry(
        retry=(
            retry_if_exception_type(RETRYABLE_EXCEPTIONS) |
            retry_if_exception(_should_retry_s3_error)
        ),
        wait=wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY, exp_base=S3_DEFAULT_EXPONENTIAL_BASE),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES + 1),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def test_connection(self) -> bool:
        """Test S3 connection and permissions with retry logic."""
        self.client.list_objects_v2(Bucket=settings.s3_bucket, MaxKeys=1)
        logger.info("S3 connection test successful")
        return True


# Global service instance for backward compatibility
_s3_service = S3Service()

# Backward compatibility functions
def get_s3_client():
    """Get configured S3 client (backward compatibility)."""
    return _s3_service.client

def upload_to_s3(file_path: str, s3_key: str, bucket: Optional[str] = None) -> str:
    """Upload file to S3/R2 (backward compatibility)."""
    return _s3_service.upload_file(file_path, s3_key, bucket)

def generate_presigned_put_url(s3_key: str, bucket: Optional[str] = None, expires_in: int = 3600) -> str:
    """Generate presigned PUT URL (backward compatibility)."""
    return _s3_service.generate_presigned_put_url(s3_key, bucket, expires_in)

def generate_presigned_get_url(s3_key: str, bucket: Optional[str] = None, expires_in: int = 3600) -> str:
    """Generate presigned GET URL (backward compatibility)."""
    return _s3_service.generate_presigned_get_url(s3_key, bucket, expires_in)

def download_from_s3(s3_key: str, local_path: str, bucket: Optional[str] = None) -> str:
    """Download file from S3/R2 (backward compatibility)."""
    return _s3_service.download_file(s3_key, local_path, bucket)

def object_exists(s3_key: str, bucket: Optional[str] = None) -> bool:
    """Check if object exists (backward compatibility)."""
    return _s3_service.object_exists(s3_key, bucket)

def delete_object(s3_key: str, bucket: Optional[str] = None) -> bool:
    """Delete object (backward compatibility)."""
    return _s3_service.delete_object(s3_key, bucket)

def delete_objects(s3_keys: List[str], bucket: Optional[str] = None) -> Dict[str, Any]:
    """Delete multiple objects (backward compatibility)."""
    return _s3_service.delete_objects(s3_keys, bucket)

def copy_object(source_key: str, dest_key: str, source_bucket: Optional[str] = None, dest_bucket: Optional[str] = None) -> bool:
    """Copy object (backward compatibility)."""
    return _s3_service.copy_object(source_key, dest_key, source_bucket, dest_bucket)

def get_object_metadata(s3_key: str, bucket: Optional[str] = None) -> Dict[str, Any]:
    """Get object metadata (backward compatibility)."""
    return _s3_service.get_object_metadata(s3_key, bucket)

def change_storage_class(s3_key: str, storage_class: str, bucket: Optional[str] = None) -> bool:
    """Change storage class (backward compatibility)."""
    return _s3_service.change_storage_class(s3_key, storage_class, bucket)

def get_storage_class(s3_key: str, bucket: Optional[str] = None) -> str:
    """Get storage class (backward compatibility)."""
    return _s3_service.get_storage_class(s3_key, bucket)

def list_objects(prefix: str = "", bucket: Optional[str] = None, max_keys: int = 1000) -> List[Dict[str, Any]]:
    """List objects (backward compatibility)."""
    return _s3_service.list_objects(prefix, bucket, max_keys)

def list_object_versions(s3_key: str, bucket: Optional[str] = None) -> List[Dict[str, Any]]:
    """List object versions (backward compatibility)."""
    return _s3_service.list_object_versions(s3_key, bucket)

def get_object_tags(s3_key: str, bucket: Optional[str] = None) -> Dict[str, str]:
    """Get object tags (backward compatibility)."""
    return _s3_service.get_object_tags(s3_key, bucket)

def set_object_tags(s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
    """Set object tags (backward compatibility)."""
    return _s3_service.set_object_tags(s3_key, tags, bucket)

def update_object_tags(s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
    """Update object tags (backward compatibility)."""
    return _s3_service.update_object_tags(s3_key, tags, bucket)

def delete_object_tags(s3_key: str, tag_keys: List[str], bucket: Optional[str] = None) -> bool:
    """Delete object tags (backward compatibility)."""
    return _s3_service.delete_object_tags(s3_key, tag_keys, bucket)

def list_objects_by_tags(tag_filters: Dict[str, str], bucket: Optional[str] = None, prefix: str = "") -> List[Dict[str, Any]]:
    """List objects by tags (backward compatibility)."""
    return _s3_service.list_objects_by_tags(tag_filters, bucket, prefix)

def test_s3_connection() -> bool:
    """Test S3 connection (backward compatibility)."""
    return _s3_service.test_connection()

