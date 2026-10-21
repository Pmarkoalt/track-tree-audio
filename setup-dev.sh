#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
[[ -e .env ]] || cp env.example .env
exec uv sync --frozen
