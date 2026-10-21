"""Actual signed enqueue/GC/readiness regression; never starts an inference worker.

Requires an explicit local TRACK_TREE_AUDIO_TEST_REDIS_URL. Owns only a generated
Redis prefix and a new loopback HTTP process; never flushes Redis or reuses jobs.
"""

import faulthandler
import gc
import hashlib
import hmac
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry


def serve():
    import uvicorn
    from celery.backends.base import DisabledBackend
    from src.main import app
    from src.queues import celery_app

    if not isinstance(celery_app.backend, DisabledBackend):
        raise RuntimeError("Unused Celery result backend must be disabled")
    faulthandler.enable()
    faulthandler.dump_traceback_later(15, repeat=True)
    gc.set_threshold(30, 3, 3)

    @app.get("/_queue_liveness_metrics")
    def metrics():
        gc.collect()
        return {
            "backend": type(celery_app.backend).__name__,
            "collections": sum(row["collections"] for row in gc.get_stats()),
        }

    uvicorn.run(
        app, host="127.0.0.1", port=int(os.environ["LIVENESS_PORT"]),
        access_log=False, log_level="warning",
    )


def main():
    redis_url = os.environ.get("TRACK_TREE_AUDIO_TEST_REDIS_URL")
    if not redis_url:
        raise ValueError("Explicit TRACK_TREE_AUDIO_TEST_REDIS_URL is required")
    parsed = urlsplit(redis_url)
    if parsed.scheme != "redis" or parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
        raise ValueError("Queue regression requires explicit local Redis")
    port = int(os.environ.get("TRACK_TREE_AUDIO_TEST_HTTP_PORT", "8082"))
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    directory = Path(os.environ.get("TRACK_TREE_AUDIO_TEST_ARTIFACT_DIR") or tempfile.mkdtemp(prefix="track-tree-queue-liveness-"))
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    prefix = "track-tree-queue-liveness:" + str(uuid4()) + ":"
    secret = uuid4().hex
    source = Path(__file__).resolve().parents[1]
    environment = {key: os.environ[key] for key in ("PATH", "HOME", "TMPDIR") if key in os.environ}
    environment.update(
        PYTHONPATH=str(source), REDIS_URL=redis_url, AUDIO_REDIS_PREFIX=prefix,
        DEMUCSSVC_TOKEN=secret, LIVENESS_PORT=str(port),
        MEDIA_URL_ORIGINS='["http://127.0.0.1:44098"]',
        API_WEBHOOK_URL_ALLOWLIST='["http://127.0.0.1:44099/webhooks/demucs"]',
    )
    db = redis.Redis.from_url(
        redis_url, socket_connect_timeout=2, socket_timeout=2,
        retry=Retry(NoBackoff(), 0), decode_responses=True,
    )
    result = {"status": "running", "namespace": prefix, "accepted": 0, "readinessChecks": 0}
    process = None
    log = None
    job_ids = []

    def request(path, payload=None):
        headers = {}
        body = None
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            timestamp = str(int(time.time()))
            signature = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
            headers = {"Content-Type": "application/json", "X-Timestamp": timestamp, "X-Signature": "sha256=" + signature}
        with urlopen(Request(f"http://127.0.0.1:{port}" + path, data=body, headers=headers), timeout=5) as response:
            return response.status, json.load(response)

    def own_keys():
        keys = list(db.scan_iter(match=prefix + "*", count=100))
        keys += list(db.scan_iter(match="_kombu.binding." + prefix + "*", count=100))
        if any(not (key.startswith(prefix) or key.startswith("_kombu.binding." + prefix)) for key in keys):
            raise RuntimeError("Redis cleanup escaped its generated namespace")
        return keys

    try:
        if not db.ping():
            raise RuntimeError("Explicit Redis is unavailable")
        log = (directory / "http.log").open("w")
        os.chmod(directory / "http.log", 0o600)
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--serve"], cwd=source,
            env=environment, stdout=log, stderr=log, start_new_session=True,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Owned HTTP process exited during startup")
            try:
                if request("/readyz")[0] == 200:
                    break
            except Exception:
                time.sleep(0.1)
        else:
            raise TimeoutError("Owned HTTP startup timed out")
        start = time.monotonic()
        for _ in range(1000):
            job = str(uuid4())
            status, response = request("/split", {
                "jobId": job, "versionId": str(uuid4()), "model": "htdemucs",
                "audioUrl": "http://127.0.0.1:44098/source",
                "callbackUrl": "http://127.0.0.1:44099/webhooks/demucs",
                "outputs": [{"type": stem, "fileKey": prefix + job + "/" + stem + ".wav",
                             "uploadUrl": "http://127.0.0.1:44098/" + job + "/" + stem}
                            for stem in ("vocals", "drums", "bass", "other")],
            })
            if status != 202 or response.get("jobId") != job:
                raise RuntimeError("Canonical signed enqueue was not accepted")
            job_ids.append(job)
            result["accepted"] += 1
            if request("/readyz")[0] != 200:
                raise RuntimeError("Readiness failed after signed enqueue")
            result["readinessChecks"] += 1
            if len(job_ids) % 25 == 0:
                metrics = request("/_queue_liveness_metrics")[1]
                if metrics["backend"] != "DisabledBackend":
                    raise RuntimeError("Celery result backend unexpectedly active")
        metrics = request("/_queue_liveness_metrics")[1]
        states = db.mget([prefix + "job:" + job for job in job_ids])
        if any(not value or json.loads(value).get("status") != "queued" for value in states):
            raise RuntimeError("Accepted enqueue lacks durable namespaced state")
        if db.llen(prefix + "inference") != len(job_ids):
            raise RuntimeError("Accepted enqueue lacks its actual broker message")
        subscriptions = db.pubsub_numsub(*["celery-task-meta-inference:" + job for job in job_ids])
        if any(count for _, count in subscriptions):
            raise RuntimeError("Unused Celery result subscriptions were created")
        result.update(status="passed", elapsedMs=round((time.monotonic() - start) * 1000),
                      queuedMessages=len(job_ids), durableStates=len(states), resultSubscriptions=0,
                      backend=metrics["backend"], garbageCollections=metrics["collections"])
    except Exception as error:
        result.update(status="failed", failureClass=type(error).__name__)
    finally:
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=3)
            except Exception:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)
        if log is not None:
            log.close()
        try:
            keys = own_keys()
            for offset in range(0, len(keys), 100):
                db.delete(*keys[offset:offset + 100])
            result["ownedRedisKeysRemoved"] = len(keys)
            if own_keys():
                raise RuntimeError("Owned Redis cleanup incomplete")
        except Exception as error:
            result.update(status="failed", cleanupFailureClass=type(error).__name__)
        db.close()
        evidence = directory / "result.json"
        evidence.write_text(json.dumps(result, indent=2) + "\n")
        os.chmod(evidence, 0o600)
    print(json.dumps(result))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["--serve"]:
        serve()
    else:
        raise SystemExit(main())
