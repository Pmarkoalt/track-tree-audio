#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
# Run the Celery worker separately; no hidden detached processes.
exec uv run --frozen uvicorn src.main:app --host 0.0.0.0 --port "${PORT:-8080}"
