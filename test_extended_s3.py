#!/usr/bin/env python3
"""Test extended S3 operations with LocalStack."""

import os
import tempfile
import io

# Set environment to local before importing
os.environ["ENVIRONMENT"] = "local"

from src.aws_services.s3 import _s3_service


def test_extended_s3_operations():
    """Test extended S3 operations with LocalStack."""
    environment = os.getenv("ENVIRONMENT", "local")
    print(f"Testing extended S3 operations in {environment} environment")
    
    # Create a test file
    test_content = "Hello, Extended S3 Operations!"
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write(test_content)
        test_file_path = f.name
    
    try:
        # Test upload
        print("\n1. Testing file upload...")
        s3_key = "test/extended.txt"
        result = _s3_service.upload_file(test_file_path, s3_key)
        print(f"✅ Upload successful: {result}")
        
        # Test get object metadata
        print("\n2. Testing get object metadata...")
        metadata = _s3_service.get_object_metadata(s3_key)
        print(f"✅ Metadata retrieved: Size={metadata['Size']}, ContentType={metadata['ContentType']}")
        
        # Test set and get tags
        print("\n3. Testing object tags...")
        tags = {"environment": "test", "type": "text", "version": "1.0"}
        _s3_service.set_object_tags(s3_key, tags)
        retrieved_tags = _s3_service.get_object_tags(s3_key)
        print(f"✅ Tags set and retrieved: {retrieved_tags}")
        
        # Test copy object
        print("\n4. Testing copy object...")
        copy_key = "test/extended_copy.txt"
        copy_result = _s3_service.copy_object(s3_key, copy_key)
        print(f"✅ Object copied: {copy_result}")
        
        # Test get object (download to memory)
        print("\n5. Testing get object (memory)...")
        content_bytes = _s3_service.get_object(copy_key)
        content_str = content_bytes.decode('utf-8')
        print(f"✅ Object downloaded to memory: {content_str == test_content}")
        
        # Test upload fileobj
        print("\n6. Testing upload fileobj...")
        file_obj = io.BytesIO(b"Hello from fileobj!")
        fileobj_key = "test/fileobj.txt"
        fileobj_result = _s3_service.upload_fileobj(file_obj, fileobj_key)
        print(f"✅ File object uploaded: {fileobj_result}")
        
        # Test delete multiple objects
        print("\n7. Testing delete multiple objects...")
        keys_to_delete = [s3_key, copy_key, fileobj_key]
        delete_result = _s3_service.delete_objects(keys_to_delete)
        deleted_count = len(delete_result.get('Deleted', []))
        print(f"✅ Deleted {deleted_count} objects")
        
        # Verify deletion
        print("\n8. Verifying deletion...")
        for key in keys_to_delete:
            exists = _s3_service.file_exists(key)
            print(f"   - {key}: {'exists' if exists else 'deleted'}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        if os.path.exists(test_file_path):
            os.unlink(test_file_path)


if __name__ == "__main__":
    test_extended_s3_operations()
