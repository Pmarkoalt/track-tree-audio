"""One inference task and a separate callback/recovery queue."""

from celery import Celery
from .env import settings

celery_app = Celery(
    "track_tree_audio",
    broker=settings.redis_url,
    include=["src.demucs_runner", "src.rendition_service"],
)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # PostgreSQL and namespaced state own progress/results. Redis result PubSub is
    # unused and AsyncResult finalizers can re-enter its non-reentrant lock at GC.
    task_ignore_result=True,
    task_track_started=False,
    broker_connection_timeout=5,
    broker_transport_options={
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
        "retry_on_timeout": False,
    },
    task_publish_retry_policy={
        "max_retries": 2,
        "interval_start": 0,
        "interval_step": 0.2,
        "interval_max": 0.5,
    },
    task_time_limit=1800,
    task_soft_time_limit=1700,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=86400,
    broker_connection_retry_on_startup=True,
    task_routes={
        "audio.process": {"queue": settings.audio_redis_prefix + "inference"},
        "audio.callback": {"queue": settings.audio_redis_prefix + "callbacks"},
        "audio.recover": {"queue": settings.audio_redis_prefix + "callbacks"},
        "rendition.execute": {"queue": settings.audio_redis_prefix + "rendition-execution"},
        "rendition.callback": {"queue": settings.audio_redis_prefix + "rendition-callbacks"},
        "rendition.recover": {"queue": settings.audio_redis_prefix + "rendition-callbacks"},
    },
    beat_schedule={
        "recover-persisted-jobs": {"task": "audio.recover", "schedule": 30.0},
        "recover-rendition-jobs": {"task": "rendition.recover", "schedule": 10.0},
    },
)
