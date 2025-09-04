"""S3/R2 upload utilities."""

import os
import logging
from typing import Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from .env import settings

logger = logging.getLogger(__name__)


def get_s3_client():
    """Get configured S3 client."""
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


def upload_to_s3(file_path: str, s3_key: str, bucket: Optional[str] = None) -> str:
    """
    Upload file to S3/R2.
    
    Args:
        file_path: Local file path to upload
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        S3 URL of uploaded file
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        # Upload file
        s3_client.upload_file(file_path, bucket, s3_key)
        
        # Construct URL
        if settings.s3_endpoint:
            # Custom endpoint (e.g., R2)
            url = f"{settings.s3_endpoint.rstrip('/')}/{bucket}/{s3_key}"
        else:
            # Standard S3
            url = f"https://{bucket}.s3.{settings.s3_region}.amazonaws.com/{s3_key}"
        
        logger.info(f"Uploaded {file_path} to {url}")
        return url
        
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise


def generate_presigned_put_url(s3_key: str, bucket: Optional[str] = None, expires_in: int = 3600) -> str:
    """
    Generate presigned PUT URL for direct upload.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        expires_in: URL expiration time in seconds
        
    Returns:
        Presigned PUT URL
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        # Generate presigned PUT URL
        url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expires_in
        )
        
        logger.info(f"Generated presigned PUT URL for {s3_key}")
        return url
        
    except ClientError as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        raise


def test_s3_connection() -> bool:
    """Test S3 connection and permissions."""
    try:
        s3_client = get_s3_client()
        
        # Try to list objects in bucket
        s3_client.list_objects_v2(Bucket=settings.s3_bucket, MaxKeys=1)
        
        logger.info("S3 connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"S3 connection test failed: {e}")
        return False
