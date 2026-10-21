"""Redis worker state is separate from API/Postgres, including durable callback data."""

import json
import redis
from .env import settings


def client():
    return redis.Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


def job_key(job_id: str) -> str:
    return settings.audio_redis_prefix + "job:" + job_id


def load(job_id: str) -> dict:
    value = client().get(job_key(job_id))
    return json.loads(value) if value else {}


def save(job_id: str, value: dict):
    client().set(
        job_key(job_id),
        json.dumps(value, ensure_ascii=False, separators=(",", ":")),
        ex=settings.audio_job_retention_seconds,
    )


def patch(job_id: str, **values):
    db = client()
    with db.lock(
        settings.audio_redis_prefix + "state-lock:" + job_id,
        timeout=10,
        blocking_timeout=5,
    ):
        value = load(job_id)
        value.update(values)
        save(job_id, value)
        return value
