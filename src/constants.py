"""Application constants."""

# S3 Retry configuration constants
DEFAULT_MAX_RETRIES = 3
DEFAULT_MIN_DELAY = 1.0
DEFAULT_MAX_DELAY = 60.0
DEFAULT_EXPONENTIAL_BASE = 2.0
DEFAULT_JITTER = True

# Retryable exception types for S3 operations
RETRYABLE_EXCEPTIONS = (
    'ClientError',
    'BotoCoreError', 
    'ConnectionError',
    'TimeoutError'
)

# Retryable S3 error codes
RETRYABLE_ERROR_CODES = {
    'ThrottlingException',
    'Throttling',
    'RequestTimeout',
    'RequestTimeoutException',
    'ServiceUnavailable',
    'InternalError',
    'InternalServerError',
    'SlowDown',
    'TooManyRequests',
    'RequestLimitExceeded',
    'BandwidthLimitExceeded',
    'RequestThrottled'
} 