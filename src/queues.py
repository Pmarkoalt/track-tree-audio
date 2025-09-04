"""Celery configuration and task definitions."""

from celery import Celery
from .env import settings

# Initialize Celery app
celery_app = Celery(
    "track_tree_audio",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["src.demucs_runner"]
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=1800,  # 30 minutes max per task
    task_soft_time_limit=1500,  # 25 minutes soft limit
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=False,
    task_reject_on_worker_lost=True,
    task_ignore_result=False,
    result_expires=3600,  # 1 hour
)

# Task routing
celery_app.conf.task_routes = {
    "src.demucs_runner.process_audio_split": {"queue": "demucs_queue"},
}
