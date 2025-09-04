"""Simplified FastAPI app for testing without Demucs."""

import logging
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import JSONResponse

from .env import settings
from .models import SplitRequest, SplitResponse, HealthResponse
from .security import verify_hmac_signature, is_webhook_url_allowed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Track Tree Audio Service (Simple)",
    description="Simplified version for testing",
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
        if not is_webhook_url_allowed(request.webhook):
            raise HTTPException(status_code=400, detail="Webhook URL not allowed")
        
        # For now, just return a mock job ID
        mock_job_id = f"mock_job_{request.version_id}"
        
        logger.info(f"Mock job {mock_job_id} for version {request.version_id}")
        
        return SplitResponse(job_id=mock_job_id)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing split request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main_simple:app",
        host="0.0.0.0",
        port=settings.port,
        reload=True
    )
