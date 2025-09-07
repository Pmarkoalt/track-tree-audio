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

from src.aws_services.constants import (
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


def _get_s3_config():
    """Get S3 configuration based on environment."""
    environment = os.getenv("ENVIRONMENT", "local").lower()
    
    if environment == "local":
        return {
            "endpoint_url": "http://localhost:4566",
            "region_name": "us-east-1",
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "bucket": os.getenv("S3_BUCKET", "test-bucket")
        }
    else:
        return {
            "endpoint_url": os.getenv("S3_ENDPOINT"),
            "region_name": os.getenv("S3_REGION", "us-east-1"),
            "aws_access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY"),
            "bucket": os.getenv("S3_BUCKET")
        }


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
        """Create configured S3 client using environment-based configuration."""
        try:
            s3_config = _get_s3_config()
            
            client = boto3.client(
                's3',
                endpoint_url=s3_config['endpoint_url'],
                region_name=s3_config['region_name'],
                aws_access_key_id=s3_config['aws_access_key_id'],
                aws_secret_access_key=s3_config['aws_secret_access_key']
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
            s3_config = _get_s3_config()
            bucket = s3_config['bucket']
        
        if not bucket:
            raise ValueError("S3 bucket not configured")
        
        return bucket
    
    def _construct_url(self, s3_key: str, bucket: str) -> str:
        """Construct S3 URL for object."""
        s3_config = _get_s3_config()
        endpoint_url = s3_config['endpoint_url']
        
        if endpoint_url and 'localhost' in endpoint_url:
            # LocalStack
            return f"{endpoint_url.rstrip('/')}/{bucket}/{s3_key}"
        elif endpoint_url:
            # Custom endpoint (e.g., R2)
            return f"{endpoint_url.rstrip('/')}/{bucket}/{s3_key}"
        else:
            # Standard AWS S3
            region = s3_config['region_name']
            return f"https://{bucket}.s3.{region}.amazonaws.com/{s3_key}"

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def test_connection(self) -> bool:
        """
        Test S3 connection by listing objects.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            bucket = self._get_bucket()
            self.client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            return True
        except Exception as e:
            logger.error(f"S3 connection test failed: {e}")
            return False

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
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
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            str: S3 URL of uploaded file
        """
        bucket = self._get_bucket(bucket)
        
        try:
            self.client.upload_file(file_path, bucket, s3_key)
            url = self._construct_url(s3_key, bucket)
            logger.info(f"File uploaded successfully: {url}")
            return url
        except Exception as e:
            logger.error(f"Failed to upload file {file_path} to {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def download_file(self, s3_key: str, local_path: str, bucket: Optional[str] = None) -> str:
        """
        Download file from S3/R2 to local path.
        
        Args:
            s3_key: S3 object key
            local_path: Local file path to save to
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            str: Local file path
        """
        bucket = self._get_bucket(bucket)
        
        try:
            self.client.download_file(bucket, s3_key, local_path)
            logger.info(f"File downloaded successfully: {local_path}")
            return local_path
        except Exception as e:
            logger.error(f"Failed to download file {s3_key} to {local_path}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def file_exists(self, s3_key: str, bucket: Optional[str] = None) -> bool:
        """
        Check if file exists in S3/R2.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            bool: True if file exists, False otherwise
        """
        bucket = self._get_bucket(bucket)
        
        try:
            self.client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
        except Exception as e:
            logger.error(f"Failed to check if file exists {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def delete_file(self, s3_key: str, bucket: Optional[str] = None) -> bool:
        """
        Delete file from S3/R2.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            bool: True if deleted successfully, False otherwise
        """
        bucket = self._get_bucket(bucket)
        
        try:
            self.client.delete_object(Bucket=bucket, Key=s3_key)
            logger.info(f"File deleted successfully: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def list_objects(self, prefix: str = "", bucket: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List objects in S3/R2 bucket.
        
        Args:
            prefix: Object key prefix to filter by
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            List[Dict]: List of object metadata
        """
        bucket = self._get_bucket(bucket)
        
        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return response.get('Contents', [])
        except Exception as e:
            logger.error(f"Failed to list objects with prefix {prefix}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def generate_presigned_put_url(self, s3_key: str, expiration: int = 3600, bucket: Optional[str] = None) -> str:
        """
        Generate presigned URL for PUT operation.
        
        Args:
            s3_key: S3 object key
            expiration: URL expiration time in seconds
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            str: Presigned URL
        """
        bucket = self._get_bucket(bucket)
        
        try:
            url = self.client.generate_presigned_url(
                'put_object',
                Params={'Bucket': bucket, 'Key': s3_key},
                ExpiresIn=expiration
            )
            logger.info(f"Presigned URL generated for {s3_key}")
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def generate_presigned_get_url(self, s3_key: str, expiration: int = 3600, bucket: Optional[str] = None) -> str:
        """
        Generate presigned URL for GET operation.
        
        Args:
            s3_key: S3 object key
            expiration: URL expiration time in seconds
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            str: Presigned URL
        """
        bucket = self._get_bucket(bucket)
        
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': s3_key},
                ExpiresIn=expiration
            )
            logger.info(f"Presigned GET URL generated for {s3_key}")
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned GET URL for {s3_key}: {e}")
            raise


    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_metadata(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """Get object metadata without downloading the object."""
        bucket = self._get_bucket(bucket)
        try:
            response = self.client.head_object(Bucket=bucket, Key=s3_key)
            return {
                "Key": s3_key,
                "Size": response.get("ContentLength"),
                "LastModified": response.get("LastModified"),
                "ETag": response.get("ETag"),
                "ContentType": response.get("ContentType"),
                "Metadata": response.get("Metadata", {}),
                "StorageClass": response.get("StorageClass")
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get metadata for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_tags(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, str]:
        """Get object tags."""
        bucket = self._get_bucket(bucket)
        try:
            response = self.client.get_object_tagging(Bucket=bucket, Key=s3_key)
            return {tag["Key"]: tag["Value"] for tag in response.get("TagSet", [])}
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get tags for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def set_object_tags(self, s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
        """Set object tags."""
        bucket = self._get_bucket(bucket)
        try:
            tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]
            self.client.put_object_tagging(
                Bucket=bucket,
                Key=s3_key,
                Tagging={"TagSet": tag_set}
            )
            logger.info(f"Tags set successfully for {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to set tags for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def copy_object(self, source_key: str, dest_key: str, source_bucket: Optional[str] = None, dest_bucket: Optional[str] = None) -> str:
        """Copy object within S3 or between buckets."""
        source_bucket = self._get_bucket(source_bucket)
        dest_bucket = self._get_bucket(dest_bucket)
        try:
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            url = self._construct_url(dest_key, dest_bucket)
            logger.info(f"Object copied successfully: {source_key} -> {dest_key}")
            return url
        except Exception as e:
            logger.error(f"Failed to copy object {source_key} to {dest_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object(self, s3_key: str, bucket: Optional[str] = None) -> bytes:
        """Get object content as bytes (download to memory)."""
        bucket = self._get_bucket(bucket)
        try:
            response = self.client.get_object(Bucket=bucket, Key=s3_key)
            content = response["Body"].read()
            logger.info(f"Object downloaded to memory: {s3_key}")
            return content
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get object {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def upload_fileobj(self, file_obj, s3_key: str, bucket: Optional[str] = None, **kwargs) -> str:
        """Upload file-like object to S3."""
        bucket = self._get_bucket(bucket)
        try:
            self.client.upload_fileobj(file_obj, bucket, s3_key, **kwargs)
            url = self._construct_url(s3_key, bucket)
            logger.info(f"File object uploaded successfully: {url}")
            return url
        except Exception as e:
            logger.error(f"Failed to upload file object to {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def delete_objects(self, s3_keys: List[str], bucket: Optional[str] = None) -> Dict[str, Any]:
        """Delete multiple objects in a single request."""
        bucket = self._get_bucket(bucket)
        try:
            objects = [{"Key": key} for key in s3_keys]
            response = self.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects}
            )
            logger.info(f"Deleted {len(s3_keys)} objects")
            return response
        except Exception as e:
            logger.error(f"Failed to delete objects: {e}")
            raise

# Global service instance
_s3_service = S3Service()


# Backward compatibility functions
def upload_to_s3(file_path: str, s3_key: str, bucket: Optional[str] = None) -> str:
    """Upload file to S3/R2 (backward compatibility)."""
    return _s3_service.upload_file(file_path, s3_key, bucket)


def generate_presigned_put_url(s3_key: str, expiration: int = 3600, bucket: Optional[str] = None) -> str:
    """Generate presigned URL for PUT operation (backward compatibility)."""
    return _s3_service.generate_presigned_put_url(s3_key, expiration, bucket)


def test_s3_connection() -> bool:
    """Test S3 connection (backward compatibility)."""
    return _s3_service.test_connection()


def list_objects_v2(prefix: str = "", bucket: Optional[str] = None) -> List[Dict[str, Any]]:
    """List objects in S3/R2 bucket (backward compatibility)."""
    return _s3_service.list_objects(prefix, bucket)

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_metadata(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """
        Get object metadata without downloading the object.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            Dict: Object metadata including size, last_modified, etc.
        """
        bucket = self._get_bucket(bucket)
        
        try:
            response = self.client.head_object(Bucket=bucket, Key=s3_key)
            return {
                'Key': s3_key,
                'Size': response.get('ContentLength'),
                'LastModified': response.get('LastModified'),
                'ETag': response.get('ETag'),
                'ContentType': response.get('ContentType'),
                'Metadata': response.get('Metadata', {}),
                'StorageClass': response.get('StorageClass')
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get metadata for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_tags(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, str]:
        """
        Get object tags.
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            Dict: Object tags as key-value pairs
        """
        bucket = self._get_bucket(bucket)
        
        try:
            response = self.client.get_object_tagging(Bucket=bucket, Key=s3_key)
            return {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get tags for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def set_object_tags(self, s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
        """
        Set object tags.
        
        Args:
            s3_key: S3 object key
            tags: Dictionary of tags to set
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            bool: True if successful
        """
        bucket = self._get_bucket(bucket)
        
        try:
            tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
            self.client.put_object_tagging(
                Bucket=bucket,
                Key=s3_key,
                Tagging={'TagSet': tag_set}
            )
            logger.info(f"Tags set successfully for {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to set tags for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def copy_object(self, source_key: str, dest_key: str, source_bucket: Optional[str] = None, dest_bucket: Optional[str] = None) -> str:
        """
        Copy object within S3 or between buckets.
        
        Args:
            source_key: Source S3 object key
            dest_key: Destination S3 object key
            source_bucket: Source bucket (optional, uses default)
            dest_bucket: Destination bucket (optional, uses default)
            
        Returns:
            str: URL of copied object
        """
        source_bucket = self._get_bucket(source_bucket)
        dest_bucket = self._get_bucket(dest_bucket)
        
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            url = self._construct_url(dest_key, dest_bucket)
            logger.info(f"Object copied successfully: {source_key} -> {dest_key}")
            return url
        except Exception as e:
            logger.error(f"Failed to copy object {source_key} to {dest_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object(self, s3_key: str, bucket: Optional[str] = None) -> bytes:
        """
        Get object content as bytes (download to memory).
        
        Args:
            s3_key: S3 object key
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            bytes: Object content
        """
        bucket = self._get_bucket(bucket)
        
        try:
            response = self.client.get_object(Bucket=bucket, Key=s3_key)
            content = response['Body'].read()
            logger.info(f"Object downloaded to memory: {s3_key}")
            return content
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get object {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def upload_fileobj(self, file_obj, s3_key: str, bucket: Optional[str] = None, **kwargs) -> str:
        """
        Upload file-like object to S3.
        
        Args:
            file_obj: File-like object to upload
            s3_key: S3 object key
            bucket: S3 bucket name (optional, uses default)
            **kwargs: Additional arguments for upload_fileobj
            
        Returns:
            str: S3 URL of uploaded file
        """
        bucket = self._get_bucket(bucket)
        
        try:
            self.client.upload_fileobj(file_obj, bucket, s3_key, **kwargs)
            url = self._construct_url(s3_key, bucket)
            logger.info(f"File object uploaded successfully: {url}")
            return url
        except Exception as e:
            logger.error(f"Failed to upload file object to {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def delete_objects(self, s3_keys: List[str], bucket: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete multiple objects in a single request.
        
        Args:
            s3_keys: List of S3 object keys to delete
            bucket: S3 bucket name (optional, uses default)
            
        Returns:
            Dict: Response with deleted and error information
        """
        bucket = self._get_bucket(bucket)
        
        try:
            objects = [{'Key': key} for key in s3_keys]
            response = self.client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects}
            )
            logger.info(f"Deleted {len(s3_keys)} objects")
            return response
        except Exception as e:
            logger.error(f"Failed to delete objects: {e}")
            raise


    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_metadata(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """Get object metadata without downloading the object."""
        bucket = self._get_bucket(bucket)
        try:
            response = self.client.head_object(Bucket=bucket, Key=s3_key)
            return {
                "Key": s3_key,
                "Size": response.get("ContentLength"),
                "LastModified": response.get("LastModified"),
                "ETag": response.get("ETag"),
                "ContentType": response.get("ContentType"),
                "Metadata": response.get("Metadata", {}),
                "StorageClass": response.get("StorageClass")
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get metadata for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object_tags(self, s3_key: str, bucket: Optional[str] = None) -> Dict[str, str]:
        """Get object tags."""
        bucket = self._get_bucket(bucket)
        try:
            response = self.client.get_object_tagging(Bucket=bucket, Key=s3_key)
            return {tag["Key"]: tag["Value"] for tag in response.get("TagSet", [])}
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get tags for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def set_object_tags(self, s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
        """Set object tags."""
        bucket = self._get_bucket(bucket)
        try:
            tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]
            self.client.put_object_tagging(
                Bucket=bucket,
                Key=s3_key,
                Tagging={"TagSet": tag_set}
            )
            logger.info(f"Tags set successfully for {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to set tags for {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def copy_object(self, source_key: str, dest_key: str, source_bucket: Optional[str] = None, dest_bucket: Optional[str] = None) -> str:
        """Copy object within S3 or between buckets."""
        source_bucket = self._get_bucket(source_bucket)
        dest_bucket = self._get_bucket(dest_bucket)
        try:
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            url = self._construct_url(dest_key, dest_bucket)
            logger.info(f"Object copied successfully: {source_key} -> {dest_key}")
            return url
        except Exception as e:
            logger.error(f"Failed to copy object {source_key} to {dest_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def get_object(self, s3_key: str, bucket: Optional[str] = None) -> bytes:
        """Get object content as bytes (download to memory)."""
        bucket = self._get_bucket(bucket)
        try:
            response = self.client.get_object(Bucket=bucket, Key=s3_key)
            content = response["Body"].read()
            logger.info(f"Object downloaded to memory: {s3_key}")
            return content
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"Object {s3_key} not found")
            raise
        except Exception as e:
            logger.error(f"Failed to get object {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def upload_fileobj(self, file_obj, s3_key: str, bucket: Optional[str] = None, **kwargs) -> str:
        """Upload file-like object to S3."""
        bucket = self._get_bucket(bucket)
        try:
            self.client.upload_fileobj(file_obj, bucket, s3_key, **kwargs)
            url = self._construct_url(s3_key, bucket)
            logger.info(f"File object uploaded successfully: {url}")
            return url
        except Exception as e:
            logger.error(f"Failed to upload file object to {s3_key}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS) | retry_if_exception(_should_retry_s3_error),
        wait=wait_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY) if not S3_DEFAULT_JITTER else wait_random_exponential(multiplier=S3_DEFAULT_MIN_DELAY, max=S3_DEFAULT_MAX_DELAY),
        stop=stop_after_attempt(S3_DEFAULT_MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )
    def delete_objects(self, s3_keys: List[str], bucket: Optional[str] = None) -> Dict[str, Any]:
        """Delete multiple objects in a single request."""
        bucket = self._get_bucket(bucket)
        try:
            objects = [{"Key": key} for key in s3_keys]
            response = self.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects}
            )
            logger.info(f"Deleted {len(s3_keys)} objects")
            return response
        except Exception as e:
            logger.error(f"Failed to delete objects: {e}")
            raise

# Global service instance
_s3_service = S3Service()


# Backward compatibility functions
def upload_to_s3(file_path: str, s3_key: str, bucket: Optional[str] = None) -> str:
    """Upload file to S3/R2 (backward compatibility)."""
    return _s3_service.upload_file(file_path, s3_key, bucket)


def generate_presigned_put_url(s3_key: str, expiration: int = 3600, bucket: Optional[str] = None) -> str:
    """Generate presigned URL for PUT operation (backward compatibility)."""
    return _s3_service.generate_presigned_put_url(s3_key, expiration, bucket)


def test_s3_connection() -> bool:
    """Test S3 connection (backward compatibility)."""
    return _s3_service.test_connection()


def list_objects_v2(prefix: str = "", bucket: Optional[str] = None) -> List[Dict[str, Any]]:
    """List objects in S3/R2 bucket (backward compatibility)."""
    return _s3_service.list_objects(prefix, bucket)
