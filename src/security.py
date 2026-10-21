"""Exact raw-byte timestamp.body HMAC shared with the TypeScript bridge."""

import hashlib
import hmac
import re
import time
from urllib.parse import urlsplit
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
