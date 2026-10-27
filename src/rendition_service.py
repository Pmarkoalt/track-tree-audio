"""Trusted HTTP/Celery registration for the optional bounded rendition worker."""

import hashlib
import base64
from pathlib import Path
import tempfile

from .env import settings
from .queues import celery_app


def rendition_runtime():
    # These imports are small core modules. No inference/model loading, directory
    # creation, network or Redis access occurs while constructing registration.
    from .rendition_jobs import RenditionRuntime
    from .rendition_runner import encode_rendition

    namespace = hashlib.sha256(settings.audio_redis_prefix.encode()).hexdigest()[:16]
    root = Path(settings.rendition_work_root) if settings.rendition_work_root else (
        Path(tempfile.gettempdir()) / ("track-tree-renditions-" + namespace)
    )
    if not root.is_absolute():
        raise ValueError("Rendition work root must be absolute")
    return RenditionRuntime(
        work_root=root,
        encode=encode_rendition,
        schedule_execution=lambda job_id: execute_task.apply_async(
            args=[job_id], task_id="rendition-execution:" + job_id,
        ),
        schedule_callback=lambda job_id, delay_seconds=0: callback_task.apply_async(
            args=[job_id], countdown=delay_seconds,
        ),
    )


def rendition_evidence(job_id: str) -> dict:
    """Private run-harness selector; never expose capabilities or terminal body."""
    from . import state
    from .rendition_models import identity_hash, parse_model, validate_identity, validate_model

    job_id = validate_model("RenditionAccepted", {
        "kind": "rendition", "jobId": job_id, "status": "accepted",
    })["jobId"]
    current = state.worker_store("rendition").load(job_id)
    if not current:
        raise ValueError("Rendition evidence is unavailable")
    identity = validate_identity(current["identity"])
    if identity["jobId"] != job_id or identity_hash(identity) != current.get("identityHash"):
        raise ValueError("Rendition evidence identity is invalid")
    terminal_status = None
    digest = current.get("terminalSha256")
    if digest is not None:
        raw = base64.b64decode(current["terminalBodyBase64"], validate=True)
        if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("Rendition evidence digest is invalid")
        terminal = parse_model("RenditionCallback", raw)
        if terminal["identity"] != identity:
            raise ValueError("Rendition evidence identity is invalid")
        terminal_status = terminal["status"]
    return {
        "jobId": job_id,
        "versionId": identity["versionId"],
        "allocationId": identity["output"]["id"],
        "status": current.get("status"),
        "executionAttempts": current.get("executionAttempts", 0),
        "callbackAttempts": current.get("callbackAttempts", 0),
        "terminalSha256": digest,
        "terminalStatus": terminal_status,
        "delivered": current.get("delivered") is True,
    }


@celery_app.task(
    name="rendition.execute", max_retries=0, soft_time_limit=195,
    time_limit=200, reject_on_worker_lost=False,
)
def execute_task(job_id: str):
    from .rendition_jobs import execute_rendition

    return execute_rendition(job_id, runtime=rendition_runtime())


@celery_app.task(name="rendition.callback", max_retries=0, soft_time_limit=30, time_limit=40)
def callback_task(job_id: str):
    from .rendition_jobs import deliver_rendition

    return deliver_rendition(job_id, runtime=rendition_runtime())


@celery_app.task(name="rendition.recover", max_retries=0, soft_time_limit=30, time_limit=40)
def recover_task():
    from .rendition_jobs import recover_renditions

    return recover_renditions(runtime=rendition_runtime(), limit=25)
