"""Signed, idempotent enqueue. No heavy model imports or inference in FastAPI."""

import hashlib
import json
from uuid import uuid4
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from .env import settings
from .models import SplitRequest, SplitResponse, HealthResponse
from .security import (
    verify_hmac_signature,
    is_webhook_url_allowed,
    is_media_url_allowed,
)
from . import state

app = FastAPI(title="Track-Tree Audio", version="0.2.0")


@app.exception_handler(HTTPException)
async def problem(request, error):
    return JSONResponse(
        {
            "type": "about:blank",
            "title": "Audio request failed",
            "status": error.status_code,
            "detail": str(error.detail),
            "requestId": str(uuid4()),
        },
        status_code=error.status_code,
        media_type="application/problem+json",
    )


@app.exception_handler(Exception)
async def unexpected_problem(request, error):
    return JSONResponse(
        {
            "type": "about:blank",
            "title": "Internal Server Error",
            "status": 500,
            "detail": "Audio request could not be completed",
            "requestId": str(uuid4()),
        },
        status_code=500,
        media_type="application/problem+json",
    )


@app.get("/healthz", response_model=HealthResponse)
def health():
    return HealthResponse()


@app.get("/readyz")
def ready():
    try:
        state.client().ping()
    except Exception:
        raise HTTPException(503, "Queue is unavailable")
    return {"status": "ready"}


@app.post("/split", response_model=SplitResponse, status_code=202)
async def split_audio(request: Request):
    chunks = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > 65536:
            raise HTTPException(413, "Request is too large")
        chunks.append(chunk)
    body = b"".join(chunks)
    if not settings.demucssvc_token:
        raise HTTPException(503, "Audio signing is not configured")
    if not verify_hmac_signature(
        body,
        request.headers.get("X-Signature", ""),
        request.headers.get("X-Timestamp", ""),
    ):
        raise HTTPException(401, "Invalid signed request")
    try:
        payload = SplitRequest.model_validate_json(body)
    except ValueError:
        raise HTTPException(400, "Invalid audio contract")
    if payload.model != "htdemucs":
        raise HTTPException(400, "Unsupported inference model")
    if (
        not is_webhook_url_allowed(payload.callbackUrl)
        or not is_media_url_allowed(payload.audioUrl)
        or any(not is_media_url_allowed(output.uploadUrl) for output in payload.outputs)
    ):
        raise HTTPException(400, "Transfer or callback destination is not approved")
    if len({o.type for o in payload.outputs}) != len(payload.outputs) or len(
        {o.fileKey for o in payload.outputs}
    ) != len(payload.outputs):
        raise HTTPException(400, "Duplicate output allocations")
    identity = {
        "jobId": str(payload.jobId),
        "versionId": str(payload.versionId),
        "model": payload.model,
        "callbackUrl": payload.callbackUrl,
        "outputs": [{"type": o.type, "fileKey": o.fileKey} for o in payload.outputs],
    }
    fingerprint = hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode()
    ).hexdigest()
    job_id = str(payload.jobId)
    try:
        db = state.client()
        with db.lock(
            settings.audio_redis_prefix + "state-lock:" + job_id,
            timeout=10,
            blocking_timeout=5,
        ):
            current = state.load(job_id)
            if current and current.get("fingerprint") != fingerprint:
                raise HTTPException(409, "Job ID already belongs to another allocation")
            if not current:
                current = {
                    "fingerprint": fingerprint,
                    "request": payload.model_dump(mode="json", exclude_none=True),
                    "status": "queued",
                    "callbackAttempts": 0,
                }
                state.save(job_id, current)
        # Same task ID + worker lease makes commit-before-enqueue and repeated submissions safe.
        if current.get("status") in ("queued", "processing"):
            from .demucs_runner import process_audio_split

            process_audio_split.apply_async(
                args=[job_id], task_id="inference:" + job_id
            )
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(503, "Queue is unavailable; retry the same job ID")
    return SplitResponse(jobId=payload.jobId)
