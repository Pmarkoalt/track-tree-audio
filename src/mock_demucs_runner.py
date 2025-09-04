"""Mock Demucs runner for testing and development."""

import asyncio
import json
import time
import uuid
from typing import List, Dict, Any
import httpx
from .models import WebhookPayload, StemInfo
from .security import get_webhook_headers
from .env import settings


async def mock_process_audio_split(
    version_id: str,
    file_key: str,
    stem_types: List[str],
    callback_url: str,
    correlation_id: str = None
) -> Dict[str, Any]:
    """
    Mock implementation of audio stem separation.
    
    This simulates the Demucs processing by:
    1. Waiting for a simulated processing time
    2. Generating mock stem files
    3. Calling the webhook with results
    """
    
    job_id = str(uuid.uuid4())
    start_time = time.time()
    
    print(f"🎵 Mock Demucs: Starting job {job_id} for version {version_id}")
    print(f"   File: {file_key}")
    print(f"   Stem types: {stem_types}")
    print(f"   Callback: {callback_url}")
    
    # Simulate processing time (1-5 seconds for demo)
    processing_delay = min(5, max(1, len(stem_types) * 1.5))
    await asyncio.sleep(processing_delay)
    
    # Generate mock stems
    mock_stems = []
    for stem_type in stem_types:
        stem_info = StemInfo(
            type=stem_type,
            name=f"{stem_type.title()} Track",
            url=f"s3://mock-bucket/stems/{version_id}/{stem_type}.wav",
            size=1024 * 1024 * 2,  # 2MB mock file
            duration=180.0,  # 3 minutes
            checksum=f"mock-checksum-{stem_type}-{uuid.uuid4().hex[:8]}"
        )
        mock_stems.append(stem_info)
    
    processing_time_ms = int((time.time() - start_time) * 1000)
    
    # Create webhook payload
    webhook_payload = WebhookPayload(
        job_id=job_id,
        status="completed",
        stems=mock_stems,
        processing_time=processing_time_ms
    )
    
    # Send webhook
    try:
        payload_json = webhook_payload.model_dump_json()
        headers = get_webhook_headers(payload_json)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                callback_url,
                content=payload_json,
                headers=headers,
                timeout=30.0
            )
            
            if response.status_code == 200:
                print(f"✅ Mock Demucs: Job {job_id} completed successfully")
                print(f"   Generated {len(mock_stems)} stems")
                print(f"   Processing time: {processing_time_ms}ms")
            else:
                print(f"❌ Mock Demucs: Webhook failed with status {response.status_code}")
                print(f"   Response: {response.text}")
                
    except Exception as e:
        print(f"❌ Mock Demucs: Failed to send webhook: {e}")
        
        # Send failure webhook
        try:
            failure_payload = WebhookPayload(
                job_id=job_id,
                status="failed",
                stems=[],
                error=f"Webhook delivery failed: {str(e)}",
                processing_time=processing_time_ms
            )
            
            payload_json = failure_payload.model_dump_json()
            headers = get_webhook_headers(payload_json)
            
            async with httpx.AsyncClient() as client:
                await client.post(
                    callback_url,
                    content=payload_json,
                    headers=headers,
                    timeout=30.0
                )
        except Exception as webhook_error:
            print(f"❌ Mock Demucs: Failed to send failure webhook: {webhook_error}")
    
    return {
        "job_id": job_id,
        "status": "completed",
        "stems": [stem.model_dump() for stem in mock_stems],
        "processing_time": processing_time_ms
    }


async def mock_process_audio_split_failure(
    version_id: str,
    file_key: str,
    stem_types: List[str],
    callback_url: str,
    correlation_id: str = None,
    error_message: str = "Mock processing failure"
) -> Dict[str, Any]:
    """
    Mock implementation that simulates a processing failure.
    """
    
    job_id = str(uuid.uuid4())
    start_time = time.time()
    
    print(f"🎵 Mock Demucs: Starting FAILING job {job_id} for version {version_id}")
    print(f"   Error: {error_message}")
    
    # Simulate processing time before failure
    await asyncio.sleep(2)
    
    processing_time_ms = int((time.time() - start_time) * 1000)
    
    # Create failure webhook payload
    webhook_payload = WebhookPayload(
        job_id=job_id,
        status="failed",
        stems=[],
        error=error_message,
        processing_time=processing_time_ms
    )
    
    # Send failure webhook
    try:
        payload_json = webhook_payload.model_dump_json()
        headers = get_webhook_headers(payload_json)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                callback_url,
                content=payload_json,
                headers=headers,
                timeout=30.0
            )
            
            if response.status_code == 200:
                print(f"❌ Mock Demucs: Job {job_id} failed as expected")
            else:
                print(f"❌ Mock Demucs: Failure webhook failed with status {response.status_code}")
                
    except Exception as e:
        print(f"❌ Mock Demucs: Failed to send failure webhook: {e}")
    
    return {
        "job_id": job_id,
        "status": "failed",
        "error": error_message,
        "processing_time": processing_time_ms
    }
