"""Exact raw-byte timestamp.body HMAC shared with the TypeScript bridge."""

import hashlib
import hmac
import re
import time
from urllib.parse import urlsplit
from uuid import UUID
from .env import settings


def create_hmac_signature(
    payload: str | bytes, timestamp: str, secret: str | None = None
) -> str:
    body = payload.encode("utf-8") if isinstance(payload, str) else payload
    return hmac.new(
        (secret if secret is not None else settings.demucssvc_token).encode(),
        timestamp.encode("ascii") + b"." + body,
        hashlib.sha256,
    ).hexdigest()


def verify_hmac_signature(
    payload: str | bytes,
    signature: str,
    timestamp: str,
    *,
    secret: str | None = None,
    now: float | None = None,
) -> bool:
    token = secret if secret is not None else settings.demucssvc_token
    if (
        not token
        or not re.fullmatch(r"\d{10}", timestamp or "")
        or not re.fullmatch(r"sha256=[0-9a-f]{64}", signature or "")
    ):
        return False
    if abs((time.time() if now is None else now) - int(timestamp)) > 300:
        return False
    return hmac.compare_digest(
        signature[7:], create_hmac_signature(payload, timestamp, token)
    )


def get_webhook_headers(
    payload: str | bytes, timestamp: str | None = None
) -> dict[str, str]:
    timestamp = timestamp or str(int(time.time()))
    return {
        "Content-Type": "application/json",
        "X-Timestamp": timestamp,
        "X-Signature": "sha256=" + create_hmac_signature(payload, timestamp),
    }


def is_webhook_url_allowed(url: str) -> bool:
    value = urlsplit(url)
    return (
        value.scheme in ("http", "https")
        and not value.username
        and not value.password
        and not value.query
        and not value.fragment
        and url in settings.api_webhook_url_allowlist
    )


def is_media_url_allowed(url: str) -> bool:
    value = urlsplit(url)
    return (
        value.scheme in ("http", "https")
        and not value.username
        and not value.password
        and not value.fragment
        and f"{value.scheme}://{value.netloc}" in settings.media_url_origins
    )


def rendition_control_url(job_id: str, purpose: str) -> str:
    """Construct only fixed canonical control paths from operator configuration."""
    base = settings.rendition_api_base_url
    value = urlsplit(base)
    if (
        not base
        or value.scheme not in ("http", "https")
        or not value.hostname
        or value.username is not None
        or value.password is not None
        or "?" in base
        or "#" in base
        or "\\" in base
        or any(ord(char) <= 32 or ord(char) == 127 for char in base)
        or any(segment in (".", "..") for segment in value.path.split("/"))
    ):
        raise ValueError("Rendition API destination is not configured")
    # Match canonical EntityIdSchema's lowercase UUID transform after raw-byte
    # authentication. Datetime strings are handled separately without rewriting.
    try:
        identifier = str(UUID(job_id))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Invalid rendition job ID") from None
    if identifier != job_id.lower():
        raise ValueError("Invalid rendition job ID")
    job_id = identifier
    if purpose == "callback":
        path = "/webhooks/renditions"
    elif purpose in ("refresh", "heartbeat"):
        path = f"/internal/renditions/jobs/{job_id}/" + (
            "urls" if purpose == "refresh" else "heartbeat"
        )
    else:
        raise ValueError("Invalid rendition control purpose")
    return base.rstrip("/") + path


def is_rendition_control_url_allowed(url: str, job_id: str, purpose: str) -> bool:
    try:
        return url == rendition_control_url(job_id, purpose)
    except ValueError:
        return False
