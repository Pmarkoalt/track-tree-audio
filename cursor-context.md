0) TL;DR Mission

GPU microservice that runs Demucs to split stems. Pulls audio from a pre-signed URL, uploads stems to S3/R2, then POSTs a signed webhook to track-tree-api. No DB. No app secrets outside the HMAC token.

Priority: Stable /split with queueing, progress logs, and secure webhook callback.

1) Tech Stack (decided)

Framework: Python 3.11+, FastAPI + Uvicorn

Jobs: Celery (Redis/Rabbit) or RQ (Redis) — pick simplest for host

Audio: Demucs v4/v5 (PyTorch, CUDA), ffmpeg for re-mux if needed

Storage: boto3 → R2/S3 (pre-signed PUT or use server creds—prefer presigned)

Security: HMAC (DEMUCSSVC_TOKEN)

Deploy: Modal / Runpod / OCI / AWS GPU; Docker w/ CUDA base

2) Repo Structure
track-tree-audio/
  src/
    main.py                 # FastAPI app
    env.py                  # pydantic-settings
    security.py             # HMAC sign/verify
    queues.py               # Celery/RQ init
    demucs_runner.py        # actual Demucs invocation
    s3.py                   # upload helpers (or accept presigned PUTs)
    webhook.py              # POST back to API with HMAC
    models.py               # pydantic request/response models
  Dockerfile
  requirements.txt
  .env.example
  README.md

3) Environment Variables
PORT=8080
API_WEBHOOK_URL_ALLOWLIST=https://api.track-tree.com/webhooks/demucs

DEMUCSSVC_TOKEN=   # shared secret for HMAC

# Optional if not using presigned PUT on return:
S3_ENDPOINT=
S3_REGION=
S3_BUCKET=
S3_ACCESS_KEY_ID=
S3_SECRET_ACCESS_KEY=

4) API Endpoints

POST /split

Body: { versionId, audioUrl, aiModel, webhook, correlationId? }

Auth: HMAC header X-Signature: sha256=<hex>

Behavior: enqueue job → return { jobId } (202)

GET /healthz → { ok: true }

GET /queue/status (optional) → simple depth & worker stats

Webhook payload → BFF

{
  "versionId": "uuid",
  "status": "completed",
  "processingTimeMs": 45000,
  "stems": [
    { "type":"drums","name":"Drums","url":"s3://.../drums.wav","size":123,"duration":180.5,"checksum":"sha256:..." }
  ],
  "error": null
}


Header: X-Signature: sha256=<hex> calculated over body with DEMUCSSVC_TOKEN.

5) Job Flow

Validate HMAC + allowlisted webhook URL.

Download source audio from pre-signed GET.

Run Demucs (gpu device env).

For each stem: write WAV → upload via pre-signed PUT (preferred) or S3 creds.

Compute SHA-256; collect durations (ffprobe).

POST webhook to BFF with HMAC.

Collect metrics & logs; cleanup temp files.

6) Error Handling & Retries

Use job retries with backoff for transient S3/timeouts.

If failure: send webhook { status: "failed", error: "...stack/summary..." }.

Enforce per-file size/time caps; kill runaway jobs.

7) Task Graph (Cursor — execute in order)

FastAPI app + /healthz.

env.py + settings validation.

HMAC sign/verify helpers + allowlist check.

/split endpoint: validate → enqueue → 202.

Minimal queue worker that logs and sleeps (fake) → webhook back.

Integrate Demucs + GPU; stream logs/progress.

Add S3 upload + checksum + ffprobe metadata.

Harden: retries, temp dirs, graceful shutdown.

Acceptance: API receives /split, completes job, and BFF logs valid webhook with stems array.

8) Dev Commands
uvicorn src.main:app --reload --port 8080
python -m pytest

9) Notes for Cursor Agent

Never talk to the database. This service is stateless aside from the queue.

Prefer pre-signed URLs for all I/O; only use creds if necessary.

Always send HMAC with timestamp (include in signed payload to avoid replay).