"""AWS services constants."""

# S3 Retry configuration constants
S3_DEFAULT_MAX_RETRIES = 3
S3_DEFAULT_MIN_DELAY = 1.0
S3_DEFAULT_MAX_DELAY = 60.0
S3_DEFAULT_EXPONENTIAL_BASE = 2.0
S3_DEFAULT_JITTER = True

# S3 Retryable exception types
S3_RETRYABLE_EXCEPTIONS = (
    'ClientError',
    'BotoCoreError', 
    'ConnectionError',
    'TimeoutError'
)

# S3 Retryable error codes
S3_RETRYABLE_ERROR_CODES = {
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

# DynamoDB Retry configuration constants (for future use)
DYNAMODB_DEFAULT_MAX_RETRIES = 3
DYNAMODB_DEFAULT_MIN_DELAY = 0.1
DYNAMODB_DEFAULT_MAX_DELAY = 20.0
DYNAMODB_DEFAULT_EXPONENTIAL_BASE = 2.0
DYNAMODB_DEFAULT_JITTER = True

# DynamoDB Retryable exception types (for future use)
DYNAMODB_RETRYABLE_EXCEPTIONS = (
    'ClientError',
    'BotoCoreError',
    'ConnectionError',
    'TimeoutError'
)

# DynamoDB Retryable error codes (for future use)
DYNAMODB_RETRYABLE_ERROR_CODES = {
    'ThrottlingException',
    'Throttling',
    'RequestTimeout',
    'RequestTimeoutException',
    'ServiceUnavailable',
    'InternalError',
    'InternalServerError',
    'ProvisionedThroughputExceededException',
    'RequestLimitExceeded'
} 