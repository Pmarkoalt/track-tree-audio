#!/usr/bin/env python3
"""Test S3 operations with LocalStack."""

import os
import tempfile
from pathlib import Path

# Set environment to local before importing
os.environ["ENVIRONMENT"] = "local"

from src.aws_services.s3 import _s3_service


def test_s3_operations():
    """Test basic S3 operations with LocalStack."""
    environment = os.getenv("ENVIRONMENT", "local")
    print(f"Testing S3 operations in {environment} environment")
    
    # Test connection
    print("\n1. Testing S3 connection...")
    try:
        result = _s3_service.test_connection()
        print(f"✅ Connection test: {result}")
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return
    
    # Create a test file
    print("\n2. Creating test file...")
    test_content = "Hello, LocalStack S3!"
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write(test_content)
        test_file_path = f.name
    
    try:
        # Test upload
        print("\n3. Testing file upload...")
        s3_key = "test/hello.txt"
        result = _s3_service.upload_file(test_file_path, s3_key)
        print(f"✅ Upload successful: {result}")
        
        # Test file exists
        print("\n4. Testing file exists check...")
        exists = _s3_service.file_exists(s3_key)
        print(f"✅ File exists: {exists}")
        
        # Test download
        print("\n5. Testing file download...")
        download_path = tempfile.mktemp(suffix='.txt')
        downloaded_path = _s3_service.download_file(s3_key, download_path)
        print(f"✅ Download successful: {downloaded_path}")
        
        # Verify content
        with open(downloaded_path, 'r') as f:
            downloaded_content = f.read()
        print(f"✅ Content matches: {downloaded_content == test_content}")
        
        # Test presigned URL
        print("\n6. Testing presigned URL generation...")
        presigned_url = _s3_service.generate_presigned_put_url(s3_key)
        print(f"✅ Presigned URL generated: {presigned_url[:50]}...")
        
        # Test list objects
        print("\n7. Testing list objects...")
        objects = _s3_service.list_objects("test/")
        print(f"✅ Objects listed: {len(objects)} objects")
        for obj in objects:
            print(f"   - {obj['Key']}")
        
        # Test delete
        print("\n8. Testing file deletion...")
        deleted = _s3_service.delete_file(s3_key)
        print(f"✅ File deleted: {deleted}")
        
        # Verify deletion
        exists_after = _s3_service.file_exists(s3_key)
        print(f"✅ File no longer exists: {not exists_after}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        if os.path.exists(test_file_path):
            os.unlink(test_file_path)
        if 'download_path' in locals() and os.path.exists(download_path):
            os.unlink(download_path)


if __name__ == "__main__":
    test_s3_operations()
