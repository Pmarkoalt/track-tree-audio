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

## Tech Stack

- **Framework**: FastAPI + Uvicorn
- **Queue**: Celery with Redis
- **Audio Processing**: Demucs (PyTorch, CUDA)
- **Storage**: boto3 → S3/R2
- **Security**: HMAC authentication
- **Container**: Docker with CUDA support

## Quick Start

### Prerequisites

- Python 3.11+
- CUDA-compatible GPU
- Redis server
- S3/R2 bucket access

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

### Local Development

1. Start Redis: `redis-server`
2. Start Celery worker: `celery -A src.queues worker --loglevel=info`
3. Start FastAPI: `uvicorn src.main:app --reload --port 8080`

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
