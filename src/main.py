"""FastAPI application for track-tree-audio service."""

import logging
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import JSONResponse

from .env import settings
from .models import SplitRequest, SplitResponse, HealthResponse, QueueStatusResponse
from .security import verify_hmac_signature, is_webhook_url_allowed
from .queues import celery_app
from .demucs_runner import process_audio_split
from .mock_demucs_runner import mock_process_audio_split

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Track Tree Audio Service",
    description="GPU microservice for audio stem separation using Demucs",
    version="1.0.0"
)


def verify_hmac_auth(x_signature: str = Header(..., alias="X-Signature")) -> str:
    """Dependency to verify HMAC authentication."""
    if not x_signature:
        raise HTTPException(status_code=401, detail="Missing X-Signature header")
    return x_signature


@app.get("/healthz", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(ok=True)


@app.get("/queue/status", response_model=QueueStatusResponse)
async def queue_status():
    """Get queue status and worker statistics."""
    try:
        # Get queue statistics
        inspect = celery_app.control.inspect()
        active_tasks = inspect.active()
        scheduled_tasks = inspect.scheduled()
        
        # Count active workers
        active_workers = len(active_tasks) if active_tasks else 0
        
        # Count pending tasks
        queue_depth = 0
        if scheduled_tasks:
            for worker_tasks in scheduled_tasks.values():
                queue_depth += len(worker_tasks)
        
        # Get completed/failed counts from result backend
        # Note: This is a simplified implementation
        completed_jobs = 0
        failed_jobs = 0
        
        return QueueStatusResponse(
            queue_depth=queue_depth,
            active_workers=active_workers,
            completed_jobs=completed_jobs,
            failed_jobs=failed_jobs
        )
    except Exception as e:
        logger.error(f"Error getting queue status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get queue status")


@app.post("/split", response_model=SplitResponse)
async def split_audio(
    request: SplitRequest,
    x_signature: str = Depends(verify_hmac_auth)
):
    """
    Split audio into stems using Demucs.
    
    Validates HMAC signature, enqueues job, and returns job ID.
    """
    try:
        # Verify HMAC signature
        request_body = request.model_dump_json()
        if not verify_hmac_signature(request_body, x_signature):
            raise HTTPException(status_code=401, detail="Invalid HMAC signature")
        
        # Validate webhook URL
        if not is_webhook_url_allowed(request.callback_url):
            raise HTTPException(status_code=400, detail="Webhook URL not allowed")
        
        # For development, use mock implementation
        # In production, this would use the real Demucs processing
        if settings.demucssvc_token == "mock-token-for-development":
            # Use mock implementation for development
            import asyncio
            result = asyncio.run(mock_process_audio_split(
                version_id=request.version_id,
                file_key=request.file_key,
                stem_types=request.stem_types,
                callback_url=request.callback_url,
                correlation_id=request.correlation_id
            ))
            job_id = result["job_id"]
        else:
            # Use real Demucs processing
            job = process_audio_split.delay(
                version_id=request.version_id,
                file_key=request.file_key,
                stem_types=request.stem_types,
                callback_url=request.callback_url,
                correlation_id=request.correlation_id
            )
            job_id = job.id
        
        logger.info(f"Enqueued job {job_id} for version {request.version_id}")
        
        return SplitResponse(job_id=job_id)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing split request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=settings.port,
        reload=True
    )
