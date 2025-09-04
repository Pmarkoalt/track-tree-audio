"""Webhook utilities for sending callbacks to the API."""

import logging
import requests
from typing import Dict, Any

from .models import WebhookPayload
from .security import get_webhook_headers

logger = logging.getLogger(__name__)


def send_webhook(webhook_url: str, payload: WebhookPayload, timeout: int = 30) -> bool:
    """
    Send webhook payload to the specified URL.
    
    Args:
        webhook_url: URL to send webhook to
        payload: Webhook payload data
        timeout: Request timeout in seconds
        
    Returns:
        True if successful, False otherwise
    """
    try:
        payload_json = payload.model_dump_json()
        headers = get_webhook_headers(payload_json)
        
        logger.info(f"Sending webhook to {webhook_url}")
        logger.debug(f"Webhook payload: {payload_json}")
        
        response = requests.post(
            webhook_url,
            data=payload_json,
            headers=headers,
            timeout=timeout
        )
        
        response.raise_for_status()
        
        logger.info(f"Webhook sent successfully to {webhook_url}")
        return True
        
    except requests.exceptions.Timeout:
        logger.error(f"Webhook timeout to {webhook_url}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Webhook request failed to {webhook_url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending webhook to {webhook_url}: {e}")
        return False


def send_webhook_with_retry(
    webhook_url: str, 
    payload: WebhookPayload, 
    max_retries: int = 3,
    timeout: int = 30
) -> bool:
    """
    Send webhook with retry logic.
    
    Args:
        webhook_url: URL to send webhook to
        payload: Webhook payload data
        max_retries: Maximum number of retry attempts
        timeout: Request timeout in seconds
        
    Returns:
        True if successful, False otherwise
    """
    for attempt in range(max_retries + 1):
        if attempt > 0:
            # Exponential backoff
            import time
            delay = 2 ** attempt
            logger.info(f"Retrying webhook in {delay} seconds (attempt {attempt + 1}/{max_retries + 1})")
            time.sleep(delay)
        
        if send_webhook(webhook_url, payload, timeout):
            return True
        
        if attempt == max_retries:
            logger.error(f"Failed to send webhook after {max_retries + 1} attempts")
            return False
    
    return False
