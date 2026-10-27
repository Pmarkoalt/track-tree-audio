"""Actual Demucs inference, immutable callback payload, and independent delivery retries."""

import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import wave
from urllib.parse import urlsplit, urlunsplit
import requests
from .env import settings
from .models import SplitRequest, WebhookPayload
from .security import get_webhook_headers, is_media_url_allowed, is_webhook_url_allowed
from .queues import celery_app
from .process_runtime import ProcessCanceled, run_supervised
from . import state


class JobCanceled(Exception):
    pass


def refresh(request: SplitRequest) -> dict:
    if not is_webhook_url_allowed(request.callbackUrl):
        raise ValueError("Callback destination is not approved")
    callback = urlsplit(request.callbackUrl)
    url = urlunsplit(
        (
            callback.scheme,
            callback.netloc,
            f"/internal/audio/jobs/{request.jobId}/urls",
            "",
            "",
        )
    )
    body = b"{}"
    response = requests.post(
        url,
        data=body,
        headers=get_webhook_headers(body),
        timeout=15,
        allow_redirects=False,
    )
    if response.status_code in (404, 409):
        raise JobCanceled()
    response.raise_for_status()
    if response.status_code != 200:
        raise RuntimeError("Unexpected refresh response")
    value = response.json()
    if value.get("jobId") != str(request.jobId):
        raise ValueError("Refresh job binding failed")
    if not is_media_url_allowed(value.get("audioUrl", "")):
        raise ValueError("Source destination is not approved")
    expected = {(output.type, output.fileKey) for output in request.outputs}
    actual = {
        (output.get("type"), output.get("fileKey"))
        for output in value.get("outputs", [])
    }
    if actual != expected or any(
        not is_media_url_allowed(o.get("uploadUrl", ""))
        for o in value.get("outputs", [])
    ):
        raise ValueError("Output grant binding failed")
    return value


def download_audio(url: str, path: Path):
    if not is_media_url_allowed(url):
        raise ValueError("Source destination is not approved")
    with requests.get(
        url, stream=True, timeout=(5, 60), allow_redirects=False
    ) as response:
        response.raise_for_status()
        if response.status_code != 200:
            raise RuntimeError("Unexpected source response")
        size = 0
        with path.open("wb") as target:
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > settings.audio_max_input_bytes:
                    raise ValueError("Input exceeds processing limit")
                target.write(chunk)
    if size == 0:
        raise ValueError("Input is empty")


def runtime_environment() -> dict:
    for binary in (settings.ffmpeg_path, settings.ffprobe_path):
        if not Path(binary).is_file() or not os.access(binary, os.X_OK):
            raise ValueError("Configure managed FFmpeg and FFprobe binaries")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("CLERK_", "S3_", "STRIPE_", "DEMUCSSVC_"))
        and key not in ("REDIS_URL", "DATABASE_URL")
    }
    environment["PATH"] = os.pathsep.join(
        [
            str(Path(settings.ffmpeg_path).parent),
            str(Path(settings.ffprobe_path).parent),
            environment.get("PATH", ""),
        ]
    )
    environment["OMP_NUM_THREADS"] = str(settings.audio_worker_threads)
    environment["MKL_NUM_THREADS"] = str(settings.audio_worker_threads)
    return environment


def infer_stems(
    request: SplitRequest, input_path: Path, directory: Path, canceled: threading.Event
):
    # Executed in a killable subprocess; htdemucs is allowlisted, never an arbitrary model/path.
    if request.model != "htdemucs":
        raise ValueError("Unsupported inference model")
    command = [
        sys.executable,
        "-m",
        "src.inference",
        str(os.getpid()),
        "-n",
        "htdemucs",
        "-d",
        settings.audio_device,
        "--shifts",
        "0",
        "--segment",
        "6",
        "--overlap",
        "0.1",
        "-j",
        "1",
        "-o",
        str(directory / "separated"),
        str(input_path),
    ]
    try:
        run_supervised(
            command,
            environment=runtime_environment(),
            canceled=canceled,
            timeout_seconds=settings.audio_job_timeout_seconds,
            on_started=lambda pid: state.patch(str(request.jobId), inferencePid=pid),
            failed_message="Demucs inference failed",
            timeout_message="Inference exceeded time budget",
        )
    except ProcessCanceled:
        raise JobCanceled() from None
    return {
        output.type: directory
        / "separated"
        / "htdemucs"
        / input_path.stem
        / (output.type + ".wav")
        for output in request.outputs
    }


@celery_app.task(bind=True, name="audio.process", max_retries=3)
def process_audio_split(self, job_id: str):
    current = state.load(job_id)
    if not current or current.get("status") in ("completed", "failed", "canceled"):
        return
    if current.get("status") == "callback_pending":
        deliver_callback.apply_async(args=[job_id])
        return
    db = state.client()
    lease = db.lock(
        settings.audio_redis_prefix + "inference-lease:" + job_id,
        timeout=90,
        blocking=False,
        thread_local=False,
    )
    if not lease.acquire(blocking=False):
        return
    started = time.monotonic()
    canceled = threading.Event()
    stop = threading.Event()
    thread = None
    request = SplitRequest.model_validate(current["request"])
    try:
        attempts = int(current.get("inferenceAttempts", 0)) + 1
        if attempts > 3:
            raise RuntimeError("Inference retry budget exhausted")
        state.patch(
            job_id, status="processing", inferenceAttempts=attempts, stage="downloading"
        )
        initial = refresh(request)

        def heartbeat():
            failures = 0
            while not stop.wait(20):
                try:
                    lease.extend(90, replace_ttl=True)
                    fresh = refresh(request)
                    state.patch(job_id, grants=fresh)
                    failures = 0
                except JobCanceled:
                    canceled.set()
                    return
                except Exception:
                    failures += 1
                    if failures >= 3:
                        canceled.set()
                        return

        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        with tempfile.TemporaryDirectory(prefix="track-tree-demucs-") as temporary:
            directory = Path(temporary)
            source = directory / "input.audio"
            download_audio(initial["audioUrl"], source)
            state.patch(job_id, stage="inference")
            paths = infer_stems(request, source, directory, canceled)
            if canceled.is_set():
                raise JobCanceled()
            # Renew destination grants after inference, so queue/model/download delay cannot expire PUTs.
            grants = refresh(request)
            output_grants = {output["type"]: output for output in grants["outputs"]}
            results = []
            total = 0
            for output in request.outputs:
                path = paths[output.type]
                if not path.is_file():
                    raise ValueError("Expected model output is missing")
                size = path.stat().st_size
                total += size
                if total > settings.audio_max_output_bytes:
                    raise ValueError("Outputs exceed processing limit")
                with wave.open(str(path), "rb") as audio:
                    duration_ms = round(
                        audio.getnframes() / audio.getframerate() * 1000
                    )
                checksum = hashlib.sha256(path.read_bytes()).hexdigest()
                if canceled.is_set():
                    raise JobCanceled()
                with path.open("rb") as data:
                    response = requests.put(
                        output_grants[output.type]["uploadUrl"],
                        data=data,
                        headers={"Content-Type": "audio/wav"},
                        timeout=(5, 120),
                        allow_redirects=False,
                    )
                    response.raise_for_status()
                results.append(
                    {
                        "type": output.type,
                        "fileKey": output.fileKey,
                        "sizeBytes": size,
                        "durationMs": duration_ms,
                        "checksumSha256": checksum,
                    }
                )
            payload = {
                "jobId": str(request.jobId),
                "versionId": str(request.versionId),
                "status": "completed",
                "stems": results,
                "processingTimeMs": round((time.monotonic() - started) * 1000),
            }
    except JobCanceled:
        state.patch(job_id, status="canceled", stage="canceled")
        return
    except Exception:
        # No URLs, secrets, provider errors or local paths in browser-visible failures.
        payload = {
            "jobId": str(request.jobId),
            "versionId": str(request.versionId),
            "status": "failed",
            "stems": [],
            "processingTimeMs": round((time.monotonic() - started) * 1000),
            "error": "Audio processing failed; check the worker configuration and retry the job",
        }
    finally:
        stop.set()
        if thread:
            thread.join(timeout=20)
        try:
            lease.release()
        except Exception:
            pass
    WebhookPayload.model_validate(payload)
    # Persist before enqueue. Callback redelivery never re-enters inference.
    state.patch(job_id, status="callback_pending", callback=payload, stage="delivering")
    deliver_callback.apply_async(args=[job_id], task_id="callback:" + job_id)


@celery_app.task(bind=True, name="audio.callback", max_retries=12)
def deliver_callback(self, job_id: str):
    current = state.load(job_id)
    if not current or current.get("status") != "callback_pending":
        return
    payload = current["callback"]
    request = SplitRequest.model_validate(current["request"])
    if not is_webhook_url_allowed(request.callbackUrl):
        raise ValueError("Callback destination is not approved")
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    try:
        response = requests.post(
            request.callbackUrl,
            data=raw,
            headers=get_webhook_headers(raw),
            timeout=60,
            allow_redirects=False,
        )
        if response.status_code in (404, 409):
            state.patch(job_id, status="canceled", stage="canceled")
            return
        response.raise_for_status()
        if response.status_code != 200:
            raise RuntimeError("Unexpected callback response")
    except Exception as error:
        attempts = int(current.get("callbackAttempts", 0)) + 1
        state.patch(job_id, callbackAttempts=attempts)
        if attempts >= 12:
            state.patch(job_id, status="failed", stage="callback_failed")
            return
        raise self.retry(
            exc=RuntimeError("Callback delivery failed"),
            countdown=min(300, 2 ** min(attempts, 8)),
        ) from error
    state.patch(job_id, status=payload["status"], stage="done")


@celery_app.task(name="audio.recover")
def recover_audio_jobs():
    db = state.client()
    count = 0
    for key in db.scan_iter(settings.audio_redis_prefix + "job:*", count=100):
        current = json.loads(db.get(key) or "{}")
        job_id = current.get("request", {}).get("jobId")
        if not job_id:
            continue
        if current.get("status") == "callback_pending":
            deliver_callback.apply_async(args=[job_id])
            count += 1
        elif current.get("status") in ("queued", "processing") and not db.exists(
            settings.audio_redis_prefix + "inference-lease:" + job_id
        ):
            process_audio_split.apply_async(
                args=[job_id], task_id="inference:" + job_id
            )
            count += 1
        if count >= 100:
            break
    return {"recovered": count}
