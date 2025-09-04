"""HMAC security utilities for request authentication and webhook signing."""

import hmac
import hashlib
import time
from typing import Optional
from urllib.parse import urlparse

from .env import settings


def verify_hmac_signature(payload: str, signature: str, timestamp: Optional[str] = None) -> bool:
    """
    Verify HMAC signature for incoming requests.
    
    Args:
        payload: The request body as string
        signature: The X-Signature header value (format: sha256=<hex>)
        timestamp: Optional timestamp for replay attack prevention
        
    Returns:
        True if signature is valid, False otherwise
    """
    if not signature.startswith("sha256="):
        return False
    
    # Extract the hex signature
    received_signature = signature[7:]  # Remove "sha256=" prefix
    
    # Create expected signature
    expected_signature = create_hmac_signature(payload, timestamp)
    
    # Use constant-time comparison to prevent timing attacks
    return hmac.compare_digest(received_signature, expected_signature)


def create_hmac_signature(payload: str, timestamp: Optional[str] = None) -> str:
    """
    Create HMAC signature for outgoing webhooks.
    
    Args:
        payload: The payload to sign
        timestamp: Optional timestamp to include in signature
        
    Returns:
        Hex-encoded HMAC signature
    """
    # Include timestamp in payload to prevent replay attacks
    if timestamp is None:
        timestamp = str(int(time.time()))
    
    # Create message to sign (payload + timestamp)
    message = f"{payload}|{timestamp}"
    
    # Create HMAC signature
    signature = hmac.new(
        settings.demucssvc_token.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    return signature


def is_webhook_url_allowed(webhook_url: str) -> bool:
    """
    Check if webhook URL is in the allowlist.
    
    Args:
        webhook_url: The webhook URL to validate
        
    Returns:
        True if URL is allowed, False otherwise
    """
    try:
        parsed_url = urlparse(webhook_url)
        if not parsed_url.scheme or not parsed_url.netloc:
            return False
        
        # Check against allowlist
        for allowed_url in settings.api_webhook_url_allowlist:
            allowed_parsed = urlparse(allowed_url)
            if (parsed_url.scheme == allowed_parsed.scheme and 
                parsed_url.netloc == allowed_parsed.netloc and
                parsed_url.path.startswith(allowed_parsed.path)):
                return True
        
        return False
    except Exception:
        return False


def get_webhook_headers(payload: str, timestamp: Optional[str] = None) -> dict:
    """
    Get headers for webhook requests including HMAC signature.
    
    Args:
        payload: The webhook payload
        timestamp: Optional timestamp
        
    Returns:
        Dictionary of headers including X-Signature
    """
    if timestamp is None:
        timestamp = str(int(time.time()))
    
    signature = create_hmac_signature(payload, timestamp)
    
    return {
        "X-Signature": f"sha256={signature}",
        "X-Timestamp": timestamp,
        "Content-Type": "application/json"
    }
