# Core HTTP/runtime image. GPU inference requires the explicitly selected gpu target.
FROM python:3.11.13-slim-bookworm AS core
ENV PYTHONUNBUFFERED=1 UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl libsndfile1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.10.0 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY src ./src
RUN useradd --create-home --uid 1000 appuser && chown -R appuser /app /opt/venv
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://localhost:8080/healthz || exit 1
CMD ["/opt/venv/bin/uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]

# Native CPU deployment uses official CPU wheels rather than CUDA runtime payloads.
FROM core AS cpu
USER root
RUN uv sync --frozen --no-dev --extra cpu && chown -R appuser /opt/venv
USER appuser

FROM core AS gpu
USER root
RUN uv sync --frozen --no-dev --extra gpu && chown -R appuser /opt/venv
USER appuser

# The default image stays lightweight; select --target gpu for inference.
FROM core AS runtime
