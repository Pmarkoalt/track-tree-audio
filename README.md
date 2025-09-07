# Track Tree Audio Service

A GPU microservice for audio stem separation using Demucs. This service pulls audio from pre-signed URLs, processes it using Demucs on GPU, uploads the separated stems to S3/R2, and sends webhook callbacks to the track-tree API.

## Features

- **GPU-accelerated audio separation** using Demucs v4/v5
- **Secure HMAC authentication** for all requests
- **Queue-based processing** with Celery and Redis
- **S3/R2 integration** with presigned URL support
- **Webhook callbacks** with secure HMAC signatures
- **Progress tracking** and comprehensive logging
- **Docker support** with CUDA base image
- **Local development** with LocalStack for S3 testing

## Tech Stack

- **Framework**: FastAPI + Uvicorn
- **Queue**: Celery with Redis
- **Audio Processing**: Demucs (PyTorch, CUDA)
- **Storage**: boto3 → S3/R2
- **Security**: HMAC authentication
- **Container**: Docker with CUDA support
- **Local Testing**: LocalStack for S3 simulation

## Quick Start

### Prerequisites

- Python 3.11+
- CUDA-compatible GPU (for production)
- Redis server
- S3/R2 bucket access (or LocalStack for local development)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd track-tree-audio
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp env.example .env
# Edit .env with your configuration
```

4. Start Redis:
```bash
redis-server
```

5. Start the Celery worker:
```bash
celery -A src.queues worker --loglevel=info
```

6. Start the FastAPI server:
```bash
uvicorn src.main:app --reload --port 8080
```

## Local Development with LocalStack

For local development, you can use LocalStack to simulate AWS S3 without needing real AWS credentials or costs.

### Setting up LocalStack

1. **Install LocalStack:**
```bash
# Option 1: Using pip
pip install localstack

# Option 2: Using Docker (recommended)
docker run -d -p 4566:4566 localstack/localstack
```

2. **Create environment file for local development:**
```bash
# Create .env.local for local development
cat > .env.local << 'ENVEOF'
ENVIRONMENT=local
DEMUCSSVC_TOKEN=test-token-for-local-development
S3_BUCKET=test-bucket
REDIS_URL=redis://localhost:6379/0
CUDA_VISIBLE_DEVICES=0
ENVEOF
```

3. **Create test bucket:**
```bash
# Set environment to local
export ENVIRONMENT=local

# Create bucket using Python
python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
s3.create_bucket(Bucket='test-bucket')
print('✅ Test bucket created successfully')
"
```

4. **Test S3 operations:**
```bash
# Run the S3 test script
python3 test_s3_local.py
```

### LocalStack Configuration

The service automatically detects the environment and configures S3 accordingly:

- **Local environment** (`ENVIRONMENT=local`): Uses LocalStack at `http://localhost:4566`
- **Production environment** (`ENVIRONMENT=prod`): Uses real AWS S3/R2

### Environment Variables for LocalStack

| Variable | Local Value | Description |
|----------|-------------|-------------|
| `ENVIRONMENT` | `local` | Sets environment to local development |
| `S3_BUCKET` | `test-bucket` | LocalStack bucket name |
| `DEMUCSSVC_TOKEN` | `test-token-...` | Test token for local development |

### Testing S3 Operations

The service includes comprehensive S3 testing capabilities:

```python
from src.aws_services.s3 import _s3_service

# Test connection
connection_ok = _s3_service.test_connection()

# Upload a file
url = _s3_service.upload_file("local_file.wav", "audio/file.wav")

# Check if file exists
exists = _s3_service.file_exists("audio/file.wav")

# Download a file
local_path = _s3_service.download_file("audio/file.wav", "/tmp/downloaded.wav")

# Get object metadata
metadata = _s3_service.get_object_metadata("audio/file.wav")

# Set and get tags
_s3_service.set_object_tags("audio/file.wav", {"type": "audio", "processed": "true"})
tags = _s3_service.get_object_tags("audio/file.wav")

# Copy objects
copy_url = _s3_service.copy_object("audio/file.wav", "audio/file_copy.wav")

# Delete files
_s3_service.delete_file("audio/file.wav")
```

### Available S3 Methods

The S3Service class provides these methods with automatic retry logic:

- `test_connection()` - Test S3 connectivity
- `upload_file(file_path, s3_key)` - Upload file from local path
- `download_file(s3_key, local_path)` - Download file to local path
- `file_exists(s3_key)` - Check if file exists
- `delete_file(s3_key)` - Delete a single file
- `delete_objects(s3_keys)` - Delete multiple files
- `list_objects(prefix)` - List objects with prefix
- `get_object_metadata(s3_key)` - Get file metadata without downloading
- `get_object(s3_key)` - Download file content to memory
- `upload_fileobj(file_obj, s3_key)` - Upload file-like object
- `copy_object(source_key, dest_key)` - Copy objects
- `get_object_tags(s3_key)` - Get object tags
- `set_object_tags(s3_key, tags)` - Set object tags
- `generate_presigned_put_url(s3_key)` - Generate presigned upload URL
- `generate_presigned_get_url(s3_key)` - Generate presigned download URL

### Docker

Build and run with Docker:

```bash
# Build the image
docker build -t track-tree-audio .

# Run with GPU support
docker run --gpus all -p 8080:8080 --env-file .env track-tree-audio
```

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `ENVIRONMENT` | Environment (local/prod) | No | local |
| `PORT` | Server port | No | 8080 |
| `DEMUCSSVC_TOKEN` | HMAC secret token | Yes | - |
| `API_WEBHOOK_URL_ALLOWLIST` | Allowed webhook URLs | No | https://api.track-tree.com/webhooks/demucs |
| `S3_ENDPOINT` | S3 endpoint URL | No | - |
| `S3_REGION` | S3 region | No | us-east-1 |
| `S3_BUCKET` | S3 bucket name | No | - |
| `S3_ACCESS_KEY_ID` | S3 access key | No | - |
| `S3_SECRET_ACCESS_KEY` | S3 secret key | No | - |
| `REDIS_URL` | Redis connection URL | No | redis://localhost:6379/0 |
| `CUDA_VISIBLE_DEVICES` | CUDA devices to use | No | 0 |

## API Endpoints

### POST /split

Split audio into stems using Demucs.

**Headers:**
- `X-Signature: sha256=<hex>` - HMAC signature

**Request Body:**
```json
{
  "versionId": "uuid",
  "audioUrl": "https://example.com/audio.wav",
  "aiModel": "htdemucs",
  "webhook": "https://api.track-tree.com/webhooks/demucs",
  "correlationId": "optional-correlation-id"
}
```

**Response:**
```json
{
  "jobId": "celery-task-id"
}
```

### GET /healthz

Health check endpoint.

**Response:**
```json
{
  "ok": true
}
```

### GET /queue/status

Get queue status and worker statistics.

**Response:**
```json
{
  "queue_depth": 5,
  "active_workers": 2,
  "completed_jobs": 100,
  "failed_jobs": 3
}
```

## Webhook Payload

When a job completes, the service sends a webhook to the specified URL:

**Headers:**
- `X-Signature: sha256=<hex>` - HMAC signature
- `X-Timestamp: <unix-timestamp>` - Request timestamp

**Payload:**
```json
{
  "versionId": "uuid",
  "status": "completed",
  "processingTimeMs": 45000,
  "stems": [
    {
      "type": "drums",
      "name": "Drums",
      "url": "s3://bucket/version/drums.wav",
      "size": 1234567,
      "duration": 180.5,
      "checksum": "sha256:abc123..."
    }
  ],
  "error": null
}
```

## Security

### HMAC Authentication

All requests must include a valid HMAC signature in the `X-Signature` header:

```
X-Signature: sha256=<hex-signature>
```

The signature is calculated as:
```
HMAC-SHA256(DEMUCSSVC_TOKEN, payload|timestamp)
```

### Webhook Security

- Webhook URLs must be in the allowlist
- All webhook requests include HMAC signatures
- Timestamps prevent replay attacks

## Development

### Running Tests

```bash
python -m pytest
```

### Code Style

```bash
black src/
isort src/
flake8 src/
```

### Local Development Workflow

1. **Start LocalStack:**
```bash
docker run -d -p 4566:4566 localstack/localstack
```

2. **Set up local environment:**
```bash
export ENVIRONMENT=local
export S3_BUCKET=test-bucket
```

3. **Start Redis:**
```bash
redis-server
```

4. **Start Celery worker:**
```bash
celery -A src.queues worker --loglevel=info
```

5. **Start FastAPI:**
```bash
uvicorn src.main:app --reload --port 8080
```

6. **Test S3 operations:**
```bash
python3 test_s3_local.py
```

## Deployment

### Modal

```python
import modal

image = modal.Image.from_dockerfile("Dockerfile")
app = modal.App("track-tree-audio")

@app.function(
    image=image,
    gpu="A10G",
    secrets=[modal.Secret.from_name("track-tree-secrets")]
)
def run_service():
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080)
```

### AWS/GCP/Azure

Use the provided Dockerfile with GPU-enabled instances.

## Monitoring

- Health checks via `/healthz`
- Queue monitoring via `/queue/status`
- Comprehensive logging with structured output
- Celery task monitoring and retry logic

## Error Handling

- Automatic retries for transient failures
- Graceful error webhooks on job failure
- Timeout protection for long-running jobs
- Cleanup of temporary files

## License

See LICENSE file for details.
