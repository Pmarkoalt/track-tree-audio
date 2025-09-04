"""Basic tests for the track-tree-audio service."""

import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock

# Add src to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.models import SplitRequest, SplitResponse, HealthResponse
from src.security import create_hmac_signature, verify_hmac_signature, is_webhook_url_allowed
from src.env import Settings


def test_hmac_signature():
    """Test HMAC signature creation and verification."""
    payload = '{"test": "data"}'
    signature = create_hmac_signature(payload)
    
    # Should be valid
    assert verify_hmac_signature(payload, f"sha256={signature}")
    
    # Should be invalid with wrong payload
    assert not verify_hmac_signature('{"wrong": "data"}', f"sha256={signature}")


def test_webhook_url_validation():
    """Test webhook URL allowlist validation."""
    # Valid URL
    assert is_webhook_url_allowed("https://api.track-tree.com/webhooks/demucs")
    
    # Invalid URL
    assert not is_webhook_url_allowed("https://malicious.com/webhook")
    
    # Invalid format
    assert not is_webhook_url_allowed("not-a-url")


def test_models():
    """Test Pydantic models."""
    # Test SplitRequest
    request = SplitRequest(
        version_id="test-123",
        audio_url="https://example.com/audio.wav",
        ai_model="htdemucs",
        webhook="https://api.track-tree.com/webhooks/demucs"
    )
    assert request.version_id == "test-123"
    
    # Test SplitResponse
    response = SplitResponse(job_id="job-456")
    assert response.job_id == "job-456"
    
    # Test HealthResponse
    health = HealthResponse()
    assert health.ok is True


def test_settings():
    """Test environment settings."""
    with patch.dict(os.environ, {
        'DEMUCSSVC_TOKEN': 'test-token',
        'PORT': '9000'
    }):
        settings = Settings()
        assert settings.demucssvc_token == 'test-token'
        assert settings.port == 9000


if __name__ == "__main__":
    pytest.main([__file__])
