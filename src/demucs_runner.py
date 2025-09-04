"""Demucs audio separation task implementation."""

import os
import sys
import tempfile
import time
import logging
import hashlib
import subprocess
from typing import List, Dict, Any
from pathlib import Path

# Add user site-packages to path for demucs
sys.path.append('/Users/pmarko.alt/Library/Python/3.9/lib/python/site-packages')

import requests
import torch
from celery import current_task
from demucs import separate
from demucs import pretrained

from .models import StemInfo, WebhookPayload
from .security import get_webhook_headers
from .s3 import upload_to_s3
from .queues import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="src.demucs_runner.process_audio_split")
def process_audio_split(
    self,
    version_id: str,
    audio_url: str,
    ai_model: str,
    webhook: str,
    correlation_id: str = None
) -> Dict[str, Any]:
    """
    Process audio separation using Demucs.
    
    Args:
        version_id: Unique version identifier
        audio_url: Pre-signed URL to download audio
        ai_model: Demucs model name
        webhook: Webhook URL to call on completion
        correlation_id: Optional correlation ID
        
    Returns:
        Dictionary with job results
    """
    start_time = time.time()
    temp_dir = None
    
    try:
        # Update task state
        self.update_state(state="PROGRESS", meta={"status": "Starting audio separation"})
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix="demucs_")
        logger.info(f"Created temp directory: {temp_dir}")
        
        # Download audio file
        self.update_state(state="PROGRESS", meta={"status": "Downloading audio"})
        audio_path = download_audio(audio_url, temp_dir)
        
        # Load Demucs model
        self.update_state(state="PROGRESS", meta={"status": "Loading model"})
        model = load_demucs_model(ai_model)
        
        # Separate audio
        self.update_state(state="PROGRESS", meta={"status": "Separating audio"})
        stems = separate_audio(model, audio_path, temp_dir)
        
        # Upload stems to S3
        self.update_state(state="PROGRESS", meta={"status": "Uploading stems"})
        stem_infos = upload_stems(stems, version_id)
        
        # Calculate processing time
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Create webhook payload
        payload = WebhookPayload(
            version_id=version_id,
            status="completed",
            processing_time_ms=processing_time_ms,
            stems=stem_infos,
            error=None
        )
        
        # Send webhook
        self.update_state(state="PROGRESS", meta={"status": "Sending webhook"})
        send_webhook(webhook, payload)
        
        logger.info(f"Successfully processed version {version_id} in {processing_time_ms}ms")
        
        return {
            "status": "completed",
            "version_id": version_id,
            "processing_time_ms": processing_time_ms,
            "stems_count": len(stem_infos)
        }
        
    except Exception as e:
        processing_time_ms = int((time.time() - start_time) * 1000)
        error_msg = str(e)
        
        logger.error(f"Error processing version {version_id}: {error_msg}")
        
        # Send error webhook
        try:
            payload = WebhookPayload(
                version_id=version_id,
                status="failed",
                processing_time_ms=processing_time_ms,
                stems=[],
                error=error_msg
            )
            send_webhook(webhook, payload)
        except Exception as webhook_error:
            logger.error(f"Failed to send error webhook: {webhook_error}")
        
        # Update task state with error
        self.update_state(
            state="FAILURE",
            meta={"error": error_msg, "processing_time_ms": processing_time_ms}
        )
        
        raise
        
    finally:
        # Cleanup temporary files
        if temp_dir and os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up temp directory: {temp_dir}")


def download_audio(audio_url: str, temp_dir: str) -> str:
    """Download audio file from URL to temporary directory."""
    try:
        response = requests.get(audio_url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Determine file extension from content type or URL
        content_type = response.headers.get('content-type', '')
        if 'audio/wav' in content_type:
            ext = '.wav'
        elif 'audio/mpeg' in content_type or 'audio/mp3' in content_type:
            ext = '.mp3'
        elif 'audio/flac' in content_type:
            ext = '.flac'
        else:
            ext = '.wav'  # Default to WAV
        
        audio_path = os.path.join(temp_dir, f"input{ext}")
        
        with open(audio_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded audio to {audio_path}")
        return audio_path
        
    except Exception as e:
        logger.error(f"Failed to download audio: {e}")
        raise


def load_demucs_model(model_name: str):
    """Load Demucs model."""
    try:
        # Check if CUDA is available
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using device: {device}")
        
        # Load the model
        model = pretrained.get_model(model_name)
        model.to(device)
        
        logger.info(f"Loaded Demucs model: {model_name}")
        return model
        
    except Exception as e:
        logger.error(f"Failed to load Demucs model {model_name}: {e}")
        raise


def separate_audio(model, audio_path: str, temp_dir: str) -> List[str]:
    """Separate audio into stems using Demucs."""
    try:
        # Use Demucs separate function
        # This will create separated files in the temp directory
        separate.apply_model(
            model, 
            audio_path, 
            out=temp_dir,
            device=next(model.parameters()).device
        )
        
        # Find the generated stem files
        stem_files = []
        stem_names = ["drums", "bass", "other", "vocals"]
        
        # Demucs creates a subdirectory with the model name
        model_name = model.name if hasattr(model, 'name') else 'htdemucs'
        output_dir = os.path.join(temp_dir, model_name)
        
        if os.path.exists(output_dir):
            for stem_name in stem_names:
                stem_path = os.path.join(output_dir, f"{stem_name}.wav")
                if os.path.exists(stem_path):
                    stem_files.append(stem_path)
                    logger.info(f"Generated stem: {stem_name}")
        
        return stem_files
        
    except Exception as e:
        logger.error(f"Failed to separate audio: {e}")
        raise


def upload_stems(stem_files: List[str], version_id: str) -> List[StemInfo]:
    """Upload stem files to S3 and return stem information."""
    stem_infos = []
    
    for stem_path in stem_files:
        try:
            stem_name = Path(stem_path).stem
            stem_type = stem_name
            
            # Calculate file checksum
            with open(stem_path, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
            
            # Get file size
            file_size = os.path.getsize(stem_path)
            
            # Get audio duration using ffprobe
            duration = get_audio_duration(stem_path)
            
            # Upload to S3
            s3_url = upload_to_s3(stem_path, f"{version_id}/{stem_name}.wav")
            
            # Create stem info
            stem_info = StemInfo(
                type=stem_type,
                name=stem_name.title(),
                url=s3_url,
                size=file_size,
                duration=duration,
                checksum=f"sha256:{file_hash}"
            )
            
            stem_infos.append(stem_info)
            logger.info(f"Uploaded stem {stem_name} to {s3_url}")
            
        except Exception as e:
            logger.error(f"Failed to upload stem {stem_path}: {e}")
            raise
    
    return stem_infos


def get_audio_duration(audio_path: str) -> float:
    """Get audio duration using ffprobe."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-show_entries", "format=duration",
            "-of", "csv=p=0",
            audio_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return float(result.stdout.strip())
        else:
            logger.warning(f"ffprobe failed for {audio_path}, using default duration")
            return 0.0
            
    except Exception as e:
        logger.warning(f"Failed to get duration for {audio_path}: {e}")
        return 0.0


def send_webhook(webhook_url: str, payload: WebhookPayload):
    """Send webhook with HMAC signature."""
    try:
        payload_json = payload.model_dump_json()
        headers = get_webhook_headers(payload_json)
        
        response = requests.post(
            webhook_url,
            data=payload_json,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        logger.info(f"Successfully sent webhook to {webhook_url}")
        
    except Exception as e:
        logger.error(f"Failed to send webhook to {webhook_url}: {e}")
        raise
