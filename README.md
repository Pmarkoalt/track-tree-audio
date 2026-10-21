# Track-Tree audio service

Signed audio-job execution for private music review. The API owns durable jobs,
identity, quotas and private media grants; this service owns namespaced Redis/Celery
execution and callbacks. Upstream Demucs 4.0.1 `htdemucs` is the only separation
implementation. Health responses are not inference qualification evidence.

## Runtime and installation

Use Python 3.11 and the committed uv lock. Core HTTP dependencies and optional
inference dependencies use separate environments. From this repository:

```sh
uv sync --frozen
uv run --frozen pytest -q
```

Both mutually exclusive `cpu` and `gpu` extras pin Torch 2.14.1, Torchaudio 2.11.0, TorchCodec 0.17.0, Demucs
4.0.1, NumPy 1.26.4 and Soundfile 0.13.1. On Linux, `cpu` selects the official PyTorch CPU wheels and avoids CUDA runtime
downloads; `gpu` retains the standard PyPI runtime. On macOS both use native PyPI
wheels. The same upstream implementation runs in both cases. A successful CPU run does not qualify
CUDA hardware or drivers. See [runtime qualification](../docs/e2e/INFERENCE_RUNTIME_UPGRADE.md)
for exact platform evidence and remaining deployment gates.

Stop an existing inference worker before changing its environment. Never sync
core dependencies into an active inference environment:

```sh
UV_PROJECT_ENVIRONMENT=.venv-gpu uv sync --frozen --extra gpu
```

TorchCodec requires shared FFmpeg libraries, not just a static executable.
The Linux image installs Debian FFmpeg; local macOS qualification used the
FFmpeg 8 Homebrew keg and explicit binary/library paths. Set `FFMPEG_PATH` and
`FFPROBE_PATH` to managed executables. If the dynamic loader needs an explicit
library directory, supply it only to the worker/child runtime.

Copy `env.example` to a private ignored `.env`, choose a unique queue namespace,
and configure the shared signing key and exact approved transfer/callback origins.
The worker needs no PostgreSQL or long-lived S3/Clerk/Stripe credentials.
Celery is a fire-and-forget broker: its unused result backend is disabled.
PostgreSQL and namespaced worker state own durable progress, terminal results
and independent callback retry. Publisher sockets and retries are bounded.

Start HTTP, worker and recovery scheduler as separate supervised processes:

```sh
uv run --frozen uvicorn src.main:app --host 127.0.0.1 --port 8080
UV_PROJECT_ENVIRONMENT=.venv-gpu uv run --frozen --extra gpu celery -A src.queues worker --pool=solo --concurrency=1 --queues track-tree-audio:inference,track-tree-audio:callbacks --loglevel=info
UV_PROJECT_ENVIRONMENT=.venv-gpu uv run --frozen --extra gpu celery -A src.queues beat --schedule /private/path/celerybeat --loglevel=info
```

Change both worker queue names when changing `AUDIO_REDIS_PREFIX`. The enclosing
workspace's `scripts/audio-local.ts` launcher manages these processes and private
logs for isolated local verification.

## Protocol and security

Canonical request/response/callback definitions and exact-byte fixtures live in
[track-tree-contracts](../track-tree-contracts/CONTRACTS_V02.md). Do not invent a
second local HTTP shape.

- `POST /split` accepts a preallocated `jobId`, `versionId`, `audioUrl`, allowlisted
  `model`, configured `callbackUrl` and `outputs` containing stem type, key and PUT
  grant. It returns 202 `{ jobId, status: "accepted" }`.
- Both directions require `X-Timestamp` and `X-Signature: sha256=<hex>` over
  `HMAC-SHA256(key, timestamp + "." + exact_raw_UTF8_body)`. Timestamp skew is
  bounded to 300 seconds. Exact approved destinations and allocation bindings are
  checked independently of the signature.
- Redis stores idempotent state. Leases, process-group termination, a parent-loss
  guardian and a bounded deadline prevent duplicate/orphan compute. Recovery
  re-enqueues pending work without flushing shared Redis.
- Only the reviewed official checkpoint may reach deserialization. Every load
  rehashes a private snapshot against its full SHA256. Caller model paths and
  arbitrary output/callback allocations are forbidden.
- Callback delivery retries its persisted signed payload independently of
  completed inference. Terminal canceled/archived jobs cannot be resurrected.
- Outputs are real WAV files with checked metadata, checksums and bounded total
  bytes. API finalization verifies their immutable private objects again.
- Signed URLs, headers, provider errors and secrets must remain out of public logs
  and browser errors.

`GET /healthz` returns `{ "status": "ok" }` without importing the model.
`GET /readyz` returns readiness only after Redis responds. It does not prove that
an inference worker, checkpoint or GPU is healthy. There is no `/queue/status`
endpoint; application job status comes from the API's durable split-job routes.

## Containers and evidence

```sh
docker build --target core -t track-tree-audio:core .
docker build --target cpu -t track-tree-audio:cpu .
docker build --target gpu -t track-tree-audio:gpu .
```

The default final target is the small core image; inference requires the explicit
`cpu` or `gpu` target. The image runs as a non-root service user. Qualify actual inference,
shared-library loading, signatures, callbacks and recovery in the intended image.
GPU execution additionally requires qualified hardware/driver support. Do not
present a prepared Dockerfile as a deployed service.

The isolated actual worker smoke is `track-tree-api/scripts/audio-smoke.ts` in the
enclosing workspace. It requires an explicit disposable database and storage
prefix, records real outputs and supports separate worker/Redis/callback fault
recovery. See [verification instructions](../scripts/e2e/README.md). Injected smoke
identity is not actual Clerk proof; real browser identity tests are separate.

The queue liveness regression starts its own loopback HTTP process, performs
1,000 actual signed enqueues plus readiness checks under forced garbage collection,
and verifies actual broker messages, durable state and zero result subscriptions.
It starts no inference worker and deletes only its generated Redis namespace:

```sh
TRACK_TREE_AUDIO_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
TRACK_TREE_AUDIO_TEST_HTTP_PORT=8082 \
TRACK_TREE_AUDIO_TEST_ARTIFACT_DIR=/private/path/audio-liveness \
python scripts/check_queue_liveness.py
```

Use the selected locked environment's Python. Missing Redis configuration fails;
there are no skipped integration cases. The selected HTTP port must be free.
Private `http.log` and `result.json` record failures and counts without grants.

License: [LICENSE](LICENSE).
