"""Pydantic models for request/response validation."""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field
from datetime import datetime


class SplitRequest(BaseModel):
    """Request model for /split endpoint."""
    version_id: str = Field(..., description="Unique version identifier")
    audio_url: str = Field(..., description="Pre-signed URL to download audio")
    ai_model: str = Field(..., description="Demucs model to use (e.g., 'htdemucs')")
    webhook: str = Field(..., description="Webhook URL to call on completion")
    correlation_id: Optional[str] = Field(None, description="Optional correlation ID for tracking")


class SplitResponse(BaseModel):
    """Response model for /split endpoint."""
    job_id: str = Field(..., description="Unique job identifier")


class StemInfo(BaseModel):
    """Information about a separated audio stem."""
    type: str = Field(..., description="Stem type (e.g., 'drums', 'bass', 'other', 'vocals')")
    name: str = Field(..., description="Human-readable stem name")
    url: str = Field(..., description="S3/R2 URL where stem is stored")
    size: int = Field(..., description="File size in bytes")
    duration: float = Field(..., description="Audio duration in seconds")
    checksum: str = Field(..., description="SHA-256 checksum of the file")


class WebhookPayload(BaseModel):
    """Payload sent to webhook on job completion."""
    version_id: str = Field(..., description="Original version ID")
    status: Literal["completed", "failed"] = Field(..., description="Job status")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    stems: List[StemInfo] = Field(default_factory=list, description="Generated stems")
    error: Optional[str] = Field(None, description="Error message if failed")


class HealthResponse(BaseModel):
    """Response model for /healthz endpoint."""
    ok: bool = Field(True, description="Service health status")


class QueueStatusResponse(BaseModel):
    """Response model for /queue/status endpoint."""
    queue_depth: int = Field(..., description="Number of pending jobs")
    active_workers: int = Field(..., description="Number of active workers")
    completed_jobs: int = Field(..., description="Number of completed jobs")
    failed_jobs: int = Field(..., description="Number of failed jobs")
