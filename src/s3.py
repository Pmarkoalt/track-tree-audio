"""S3/R2 upload utilities."""

import os
import logging
from typing import Optional, List, Dict, Any
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


def generate_presigned_get_url(s3_key: str, bucket: Optional[str] = None, expires_in: int = 3600) -> str:
    """
    Generate presigned GET URL for direct download.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        expires_in: URL expiration time in seconds
        
    Returns:
        Presigned GET URL
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        # Generate presigned GET URL
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expires_in
        )
        
        logger.info(f"Generated presigned GET URL for {s3_key}")
        return url
        
    except ClientError as e:
        logger.error(f"Failed to generate presigned GET URL: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to generate presigned GET URL: {e}")
        raise


def download_from_s3(s3_key: str, local_path: str, bucket: Optional[str] = None) -> str:
    """
    Download file from S3/R2.
    
    Args:
        s3_key: S3 object key
        local_path: Local file path to save to
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        Local file path
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download file
        s3_client.download_file(bucket, s3_key, local_path)
        
        logger.info(f"Downloaded {s3_key} to {local_path}")
        return local_path
        
    except ClientError as e:
        logger.error(f"S3 download failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to download from S3: {e}")
        raise


def object_exists(s3_key: str, bucket: Optional[str] = None) -> bool:
    """
    Check if object exists in S3/R2.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        True if object exists, False otherwise
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        logger.error(f"Failed to check object existence: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to check object existence: {e}")
        raise


def delete_object(s3_key: str, bucket: Optional[str] = None) -> bool:
    """
    Delete object from S3/R2.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        True if deleted successfully
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        s3_client.delete_object(Bucket=bucket, Key=s3_key)
        
        logger.info(f"Deleted object {s3_key}")
        return True
        
    except ClientError as e:
        logger.error(f"S3 delete failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to delete from S3: {e}")
        raise


def delete_objects(s3_keys: List[str], bucket: Optional[str] = None) -> Dict[str, Any]:
    """
    Delete multiple objects from S3/R2.
    
    Args:
        s3_keys: List of S3 object keys
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        Response containing deleted and failed objects
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    if not s3_keys:
        return {"Deleted": [], "Errors": []}
    
    try:
        s3_client = get_s3_client()
        
        # Prepare delete request
        delete_objects = [{'Key': key} for key in s3_keys]
        
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': delete_objects}
        )
        
        deleted_count = len(response.get('Deleted', []))
        error_count = len(response.get('Errors', []))
        
        logger.info(f"Deleted {deleted_count} objects, {error_count} errors")
        return response
        
    except ClientError as e:
        logger.error(f"S3 batch delete failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to batch delete from S3: {e}")
        raise


def copy_object(source_key: str, dest_key: str, source_bucket: Optional[str] = None, dest_bucket: Optional[str] = None) -> bool:
    """
    Copy object within S3/R2.
    
    Args:
        source_key: Source S3 object key
        dest_key: Destination S3 object key
        source_bucket: Source bucket (uses settings.s3_bucket if not provided)
        dest_bucket: Destination bucket (uses settings.s3_bucket if not provided)
        
    Returns:
        True if copied successfully
    """
    if not source_bucket:
        source_bucket = settings.s3_bucket
    if not dest_bucket:
        dest_bucket = settings.s3_bucket
    
    if not source_bucket or not dest_bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        
        logger.info(f"Copied {source_key} to {dest_key}")
        return True
        
    except ClientError as e:
        logger.error(f"S3 copy failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to copy in S3: {e}")
        raise


def get_object_metadata(s3_key: str, bucket: Optional[str] = None) -> Dict[str, Any]:
    """
    Get object metadata from S3/R2.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        Object metadata dictionary
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        
        # Extract relevant metadata
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
        
    except ClientError as e:
        logger.error(f"Failed to get object metadata: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to get object metadata: {e}")
        raise


def change_storage_class(s3_key: str, storage_class: str, bucket: Optional[str] = None) -> bool:
    """
    Change object storage class in S3/R2.
    
    Args:
        s3_key: S3 object key
        storage_class: New storage class (STANDARD, STANDARD_IA, GLACIER, etc.)
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        True if changed successfully
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        # Copy object to itself with new storage class
        copy_source = {'Bucket': bucket, 'Key': s3_key}
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket,
            Key=s3_key,
            StorageClass=storage_class
        )
        
        logger.info(f"Changed storage class for {s3_key} to {storage_class}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to change storage class: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to change storage class: {e}")
        raise


def get_storage_class(s3_key: str, bucket: Optional[str] = None) -> str:
    """
    Get object storage class from S3/R2.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        Storage class string
    """
    metadata = get_object_metadata(s3_key, bucket)
    return metadata.get('storage_class', 'STANDARD')


def list_objects(prefix: str = "", bucket: Optional[str] = None, max_keys: int = 1000) -> List[Dict[str, Any]]:
    """
    List objects in S3/R2 bucket.
    
    Args:
        prefix: Object key prefix to filter by
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        max_keys: Maximum number of objects to return
        
    Returns:
        List of object metadata dictionaries
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=max_keys
        )
        
        objects = response.get('Contents', [])
        
        logger.info(f"Listed {len(objects)} objects with prefix '{prefix}'")
        return objects
        
    except ClientError as e:
        logger.error(f"Failed to list objects: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to list objects: {e}")
        raise


def list_object_versions(s3_key: str, bucket: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List object versions in S3/R2 bucket.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        List of object version metadata dictionaries
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        
        response = s3_client.list_object_versions(
            Bucket=bucket,
            Prefix=s3_key
        )
        
        versions = response.get('Versions', [])
        
        logger.info(f"Listed {len(versions)} versions for {s3_key}")
        return versions
        
    except ClientError as e:
        logger.error(f"Failed to list object versions: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to list object versions: {e}")
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


def get_object_tags(s3_key: str, bucket: Optional[str] = None) -> Dict[str, str]:
    """
    Get object tags from S3/R2.
    
    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        Dictionary of tag key-value pairs
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object_tagging(Bucket=bucket, Key=s3_key)
        
        # Convert tag list to dictionary
        tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        
        logger.info(f"Retrieved {len(tags)} tags for {s3_key}")
        return tags
        
    except ClientError as e:
        logger.error(f"Failed to get object tags: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to get object tags: {e}")
        raise


def set_object_tags(s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
    """
    Set object tags in S3/R2.
    
    Args:
        s3_key: S3 object key
        tags: Dictionary of tag key-value pairs
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        True if tags set successfully
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    if len(tags) > 10:
        raise ValueError("S3 supports maximum 10 tags per object")
    
    for key, value in tags.items():
        if len(key) > 50 or len(value) > 50:
            raise ValueError("Tag keys and values must be 50 characters or less")
    
    try:
        s3_client = get_s3_client()
        
        # Convert dictionary to tag list format
        tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
        
        s3_client.put_object_tagging(
            Bucket=bucket,
            Key=s3_key,
            Tagging={'TagSet': tag_set}
        )
        
        logger.info(f"Set {len(tags)} tags for {s3_key}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to set object tags: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to set object tags: {e}")
        raise


def update_object_tags(s3_key: str, tags: Dict[str, str], bucket: Optional[str] = None) -> bool:
    """
    Update object tags in S3/R2 (merges with existing tags).
    
    Args:
        s3_key: S3 object key
        tags: Dictionary of tag key-value pairs to add/update
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        True if tags updated successfully
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        # Get existing tags
        existing_tags = get_object_tags(s3_key, bucket)
        
        # Merge with new tags
        existing_tags.update(tags)
        
        # Set all tags
        return set_object_tags(s3_key, existing_tags, bucket)
        
    except Exception as e:
        logger.error(f"Failed to update object tags: {e}")
        raise


def delete_object_tags(s3_key: str, tag_keys: List[str], bucket: Optional[str] = None) -> bool:
    """
    Delete specific tags from S3/R2 object.
    
    Args:
        s3_key: S3 object key
        tag_keys: List of tag keys to delete
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        
    Returns:
        True if tags deleted successfully
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        # Get existing tags
        existing_tags = get_object_tags(s3_key, bucket)
        
        # Remove specified tags
        for key in tag_keys:
            existing_tags.pop(key, None)
        
        # Set remaining tags
        return set_object_tags(s3_key, existing_tags, bucket)
        
    except Exception as e:
        logger.error(f"Failed to delete object tags: {e}")
        raise


def list_objects_by_tags(tag_filters: Dict[str, str], bucket: Optional[str] = None, prefix: str = "") -> List[Dict[str, Any]]:
    """
    List objects filtered by tags (requires S3 Inventory or manual scanning).
    
    Note: This is a simplified implementation that scans objects.
    For production, consider using S3 Inventory or CloudTrail.
    
    Args:
        tag_filters: Dictionary of tag key-value pairs to filter by
        bucket: S3 bucket name (uses settings.s3_bucket if not provided)
        prefix: Object key prefix to limit search
        
    Returns:
        List of objects matching tag filters
    """
    if not bucket:
        bucket = settings.s3_bucket
    
    if not bucket:
        raise ValueError("S3 bucket not configured")
    
    try:
        s3_client = get_s3_client()
        matching_objects = []
        
        # List all objects with prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            for obj in page.get('Contents', []):
                obj_key = obj['Key']
                
                try:
                    # Get tags for this object
                    obj_tags = get_object_tags(obj_key, bucket)
                    
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
        
    except Exception as e:
        logger.error(f"Failed to list objects by tags: {e}")
        raise
