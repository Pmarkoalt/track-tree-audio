"""Distinct signed rendition execution and immutable independent delivery.

Registration, configured destinations and public API authority remain parent/API
owned. Broker messages carry only job IDs; Redis stores immutable local work.
"""

import base64
from collections.abc import Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import tempfile
import threading
import time
from typing import Any

from . import state
from .audio_transfer import (
    TransferContext, TransferError, TransferCanceled, TransferDeadline,
    post_control, download_media, upload_media,
)
from .managed_process import ProcessCanceled, ProcessDeadline, ProcessLimit
from .rendition_models import (
    MAX_SIGNED_BYTES, RenditionProtocolError, identity_hash, parse_model,
    timestamp_ms, validate_identity, validate_model, wire_bytes,
)
from .security import (
    verify_hmac_signature, is_media_url_allowed,
    rendition_control_url, is_rendition_control_url_allowed,
)
from .env import settings


@dataclass
class RenditionRuntime:
    work_root: Path
    encode: Callable[..., Any]
    schedule_execution: Callable[[str], None]
    schedule_callback: Callable[[str, float], None]
    store: Any = field(default_factory=lambda: state.worker_store("rendition"))
    control_url: Callable[[str, str], str] = rendition_control_url
    control_allowed: Callable[[str, str, str], bool] = is_rendition_control_url_allowed
    clock: Callable[[], float] = time.time
    monotonic: Callable[[], float] = time.monotonic
    post: Callable[..., Any] = post_control
    download: Callable[..., Any] = download_media
    upload: Callable[..., Any] = upload_media


class RenditionJobError(Exception):
    def __init__(self, code: str, status_code: int):
        # Only canonical pairs are constructed here; provider details stay private.
        self.code = code
        self.status_code = status_code
        super().__init__(code)


def _now(runtime):
    return int(runtime.clock() * 1000)


def _retention(identity, runtime):
    return max(1, math.ceil((timestamp_ms(identity["retainUntil"]) - _now(runtime)) / 1000))


def _stored_identity(current, expected_job_id=None):
    identity = validate_identity(current["identity"])
    if current.get("identityHash") != identity_hash(identity):
        raise RenditionProtocolError()
    if expected_job_id is not None:
        expected = validate_model("RenditionAccepted", {"kind": "rendition", "jobId": expected_job_id, "status": "accepted"})["jobId"]
        if identity["jobId"] != expected:
            raise RenditionProtocolError()
    return identity


def _authenticate(raw, signature, timestamp, schema, runtime):
    if not isinstance(raw, bytes) or len(raw) > MAX_SIGNED_BYTES:
        raise RenditionJobError("RENDITION_BODY_TOO_LARGE", 413)
    if not settings.demucssvc_token:
        raise RenditionJobError("RENDITION_SERVICE_UNAVAILABLE", 503)
    if not verify_hmac_signature(raw, signature, timestamp, now=runtime.clock()):
        raise RenditionJobError("RENDITION_SIGNATURE_INVALID", 401)
    try:
        return parse_model(schema, raw)
    except RenditionProtocolError:
        raise RenditionJobError("RENDITION_PAYLOAD_INVALID", 400) from None


def _destination(identity, runtime):
    try:
        expected = runtime.control_url(identity["jobId"], "callback")
    except Exception:
        raise RenditionJobError("RENDITION_SERVICE_UNAVAILABLE", 503) from None
    if identity["callbackUrl"] != expected or not runtime.control_allowed(expected, identity["jobId"], "callback"):
        raise RenditionJobError("RENDITION_PAYLOAD_INVALID", 400)


def accept_rendition(raw: bytes, signature: str, timestamp: str, *, runtime: RenditionRuntime, enabled: bool = False) -> dict:
    payload = _authenticate(raw, signature, timestamp, "RenditionEnqueue", runtime)
    if not enabled:
        raise RenditionJobError("RENDITION_SERVICE_UNAVAILABLE", 503)
    identity = payload["identity"]
    _destination(identity, runtime)
    if any(not is_media_url_allowed(payload["grants"][kind]["url"]) for kind in ("source", "output")):
        raise RenditionJobError("RENDITION_PAYLOAD_INVALID", 400)
    if timestamp_ms(identity["deadlineAt"]) <= _now(runtime):
        raise RenditionJobError("RENDITION_EXPIRED", 409)
    try:
        _, current = runtime.store.save_new(identity["jobId"], {
            "identity": identity, "status": "queued", "executionAttempts": 0,
            "callbackAttempts": 0,
        }, identity_hash=identity_hash(identity), retention_seconds=_retention(identity, runtime))
        if _stored_identity(current, identity["jobId"]) != identity:
            raise RenditionJobError("RENDITION_IDENTITY_CONFLICT", 409)
        if current["status"] == "canceled":
            raise RenditionJobError("RENDITION_CANCELED", 409)
        if current["status"] not in ("queued", "processing"):
            raise RenditionJobError("RENDITION_TERMINAL", 409)
        if current["status"] == "queued":
            runtime.schedule_execution(identity["jobId"])
    except state.StateConflict:
        raise RenditionJobError("RENDITION_IDENTITY_CONFLICT", 409) from None
    except RenditionJobError:
        raise
    except Exception:
        # Persist-before-publish leaves an identical replay/recovery path.
        raise RenditionJobError("RENDITION_SERVICE_UNAVAILABLE", 503) from None
    return validate_model("RenditionAccepted", {"kind": "rendition", "jobId": identity["jobId"], "status": "accepted"})


def cancel_rendition(raw: bytes, signature: str, timestamp: str, *, path_job_id: str, runtime: RenditionRuntime) -> dict:
    payload = _authenticate(raw, signature, timestamp, "RenditionCancelRequest", runtime)
    identity = payload["identity"]
    try:
        path_job_id = validate_model("RenditionAccepted", {"kind": "rendition", "jobId": path_job_id, "status": "accepted"})["jobId"]
    except RenditionProtocolError:
        raise RenditionJobError("RENDITION_PAYLOAD_INVALID", 400) from None
    if path_job_id != identity["jobId"]:
        raise RenditionJobError("RENDITION_IDENTITY_CONFLICT", 409)
    _destination(identity, runtime)
    if timestamp_ms(identity["retainUntil"]) <= _now(runtime):
        raise RenditionJobError("RENDITION_EXPIRED", 409)
    try:
        created, current = runtime.store.save_new(path_job_id, {
            "identity": identity, "status": "canceled", "executionAttempts": 0,
            "callbackAttempts": 0,
        }, identity_hash=identity_hash(identity), retention_seconds=_retention(identity, runtime))
        if not created and current.get("status") != "canceled":
            for attempt in range(3):
                if current.get("status") not in ("queued", "processing", "callback_pending"):
                    raise RenditionJobError("RENDITION_TERMINAL", 409)
                try:
                    runtime.store.transition(path_job_id, expected_statuses={current["status"]},
                        changes={"status": "canceled"}, retention_seconds=_retention(identity, runtime))
                    break
                except state.StateConflict:
                    if attempt == 2:
                        raise
                    current = runtime.store.load(path_job_id)
                    if current.get("status") == "canceled":
                        break
    except state.StateConflict:
        raise RenditionJobError("RENDITION_IDENTITY_CONFLICT", 409) from None
    except RenditionJobError:
        raise
    except Exception:
        raise RenditionJobError("RENDITION_SERVICE_UNAVAILABLE", 503) from None
    return validate_model("RenditionCancelResponse", {"kind": "rendition", "action": "cancel", "jobId": path_job_id, "status": "canceled"})


@contextmanager
def _directory(runtime, *, parent=None, prefix="rendition-"):
    import os
    import stat

    root = Path(parent if parent is not None else runtime.work_root)
    if not root.is_absolute() or root.is_symlink():
        raise ValueError("Invalid private rendition allocation")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
        raise ValueError("Invalid private rendition allocation")
    with tempfile.TemporaryDirectory(prefix=prefix, dir=root) as name:
        os.chmod(name, 0o700)
        yield Path(name).resolve(strict=True)


class _ControlRejected(Exception):
    def __init__(self, code="RENDITION_PAYLOAD_INVALID"):
        self.code = code
        super().__init__(code)


class _ControlRetry(Exception):
    pass


def _response(response, schema):
    if response.status_code == 200:
        try:
            return parse_model(schema, response.body)
        except RenditionProtocolError:
            raise _ControlRetry() from None
    try:
        problem = parse_model("RenditionControlProblem", response.body)
    except RenditionProtocolError:
        if response.status_code in (408, 429) or response.status_code >= 500 or 200 <= response.status_code < 300:
            raise _ControlRetry() from None
        raise _ControlRejected() from None
    if problem["status"] != response.status_code:
        raise _ControlRejected()
    if response.status_code >= 500:
        raise _ControlRetry()
    raise _ControlRejected(problem["code"])


def _control(identity, action, payload, *, runtime, run_root, deadline, canceled, response_schema):
    url = runtime.control_url(identity["jobId"], action)
    with _directory(runtime, parent=run_root, prefix="control-") as directory:
        response = runtime.post(url, wire_bytes(payload),
            context=TransferContext(directory, deadline, canceled,
                max_process_rss_bytes=identity["policy"]["sampledProcessGroupRssStopBytes"] // 2),
            destination_allowed=lambda candidate: runtime.control_allowed(candidate, identity["jobId"], action))
    return _response(response, response_schema)


def _token(lease):
    return lease.local.token


def _lease(runtime, job_id, purpose, seconds):
    return state.client().lock(runtime.store.lease_key(job_id, purpose),
        timeout=max(1, seconds), blocking=False, thread_local=False)


def _transition(runtime, identity, lease, *, statuses, changes, purpose="execution"):
    return runtime.store.transition(identity["jobId"], expected_statuses=set(statuses), changes=changes,
        lease_key=runtime.store.lease_key(identity["jobId"], purpose), lease_token=_token(lease),
        retention_seconds=_retention(identity, runtime))


def _release(lease):
    try:
        lease.release()
    except Exception:
        pass


class _ActiveExecution:
    def __init__(self, identity, runtime, root, execution, global_execution, deadline):
        self.identity, self.runtime, self.root = identity, runtime, root
        self.execution, self.global_execution = execution, global_execution
        self.deadline = deadline
        self.canceled = threading.Event()
        self.stopped = threading.Event()
        self.heartbeat_canceled = threading.Event()
        self.lock = threading.Lock()
        self.heartbeat_lock = threading.Lock()
        self.phase = "downloading"
        self.api_lease_deadline = deadline
        self.failures = 0
        self.reason = "RENDITION_TERMINAL"
        self.threads = []

    def check(self):
        if self.stopped.is_set() or self.canceled.is_set() or self.runtime.monotonic() >= min(self.deadline, self.api_lease_deadline):
            self.canceled.set()
            raise ProcessCanceled()
        current = self.runtime.store.load(self.identity["jobId"])
        if self.stopped.is_set() or current.get("status") != "processing":
            self.canceled.set()
            raise ProcessCanceled()
        if not self.execution.owned() or self.stopped.is_set():
            self.canceled.set()
            raise ProcessCanceled()
        if not self.global_execution.owned() or self.stopped.is_set():
            self.canceled.set()
            raise ProcessCanceled()

    def control(self, action, payload, response_schema, *, heartbeat_thread=False):
        for attempt in range(3):
            self.check()
            try:
                result = _control(self.identity, action, payload, runtime=self.runtime,
                    run_root=self.root, deadline=min(self.deadline, self.api_lease_deadline),
                    canceled=self.heartbeat_canceled if heartbeat_thread else self.canceled,
                    response_schema=response_schema)
                with self.lock:
                    self.failures = 0
                return result
            except _ControlRejected as error:
                self.reason = error.code
                self.canceled.set()
                raise ProcessCanceled() from None
            except (TransferError, _ControlRetry):
                with self.lock:
                    self.failures += 1
                    failed = self.failures >= self.identity["policy"]["maxConsecutiveControlFailures"]
                if failed or attempt == 2:
                    self.canceled.set()
                    raise ProcessCanceled() from None
                if self.stopped.wait(0.1):
                    raise ProcessCanceled()

    def heartbeat(self, *, heartbeat_thread=False):
        # Serialize actual-phase reports, so an old in-flight encoding heartbeat
        # cannot arrive after the owner's newer uploading report.
        with self.heartbeat_lock:
            self._heartbeat(heartbeat_thread=heartbeat_thread)

    def _heartbeat(self, *, heartbeat_thread=False):
        payload = validate_model("RenditionHeartbeatRequest", {
            "action": "heartbeat", "identity": self.identity, "phase": self.phase})
        result = self.control("heartbeat", payload, "RenditionHeartbeatResponse", heartbeat_thread=heartbeat_thread)
        if result["jobId"] != self.identity["jobId"] or result["deadlineAt"] != self.identity["deadlineAt"]:
            self.canceled.set()
            raise ProcessCanceled()
        remaining = timestamp_ms(result["leaseExpiresAt"]) - _now(self.runtime)
        observed_span = timestamp_ms(result["leaseExpiresAt"]) - timestamp_ms(result["observedAt"])
        if remaining <= 0 or timestamp_ms(result["observedAt"]) > _now(self.runtime) + 300000:
            self.canceled.set()
            raise ProcessCanceled()
        with self.lock:
            self.api_lease_deadline = min(self.deadline, self.runtime.monotonic() + min(remaining, observed_span) / 1000)
        if self.stopped.is_set():
            raise ProcessCanceled()
        self.execution.extend(60, replace_ttl=True)

    def refresh(self):
        result = self.control("refresh", {"action": "refresh", "identity": self.identity}, "RenditionRefreshResponse")
        if result["identity"] != self.identity:
            self.canceled.set()
            raise ProcessCanceled()
        for kind in ("source", "output"):
            grant = result["grants"][kind]
            if not is_media_url_allowed(grant["url"]) or timestamp_ms(grant["expiresAt"]) <= _now(self.runtime):
                self.canceled.set()
                raise ProcessCanceled()
        return result["grants"]

    def start(self):
        self.heartbeat()

        def watch_deadline():
            while not self.stopped.wait(0.05):
                if self.runtime.monotonic() >= min(self.deadline, self.api_lease_deadline):
                    self.canceled.set()
                    return

        def heartbeat_loop():
            next_heartbeat = self.runtime.monotonic() + self.identity["policy"]["heartbeatIntervalMs"] / 1000
            try:
                while not self.stopped.wait(0.25):
                    self.check()
                    if self.runtime.monotonic() >= next_heartbeat:
                        self.heartbeat(heartbeat_thread=True)
                        next_heartbeat = self.runtime.monotonic() + self.identity["policy"]["heartbeatIntervalMs"] / 1000
            except Exception:
                self.canceled.set()

        self.threads = [threading.Thread(target=watch_deadline, daemon=True), threading.Thread(target=heartbeat_loop, daemon=True)]
        for thread in self.threads:
            thread.start()

    def close(self):
        self.stopped.set()
        self.heartbeat_canceled.set()
        for thread in self.threads:
            thread.join(timeout=12)
            if thread.is_alive():
                raise RuntimeError("Rendition control did not stop")


def _timestamp(runtime):
    return datetime.fromtimestamp(runtime.clock(), timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _persist_terminal(identity, payload, runtime, lease):
    payload = validate_model("RenditionCallback", payload)
    raw = wire_bytes(payload)
    finish = timestamp_ms(payload["finishedAt"])
    delivery_end = min(timestamp_ms(identity["deliveryDeadlineAt"]), finish + identity["policy"]["callbackPostFinishHorizonMs"])
    _transition(runtime, identity, lease, statuses={"processing"}, changes={
        "status": "callback_pending", "terminalBodyBase64": base64.b64encode(raw).decode("ascii"),
        "terminalSha256": hashlib.sha256(raw).hexdigest(), "deliveryDeadlineMs": delivery_end,
        "nextCallbackAtMs": _now(runtime),
    })
    runtime.schedule_callback(identity["jobId"], 0)


def execute_rendition(job_id: str, *, runtime: RenditionRuntime):
    current = runtime.store.load(job_id)
    if not current:
        return
    identity = _stored_identity(current, job_id)
    if current.get("status") == "callback_pending":
        if current.get("nextCallbackAtMs", 0) <= _now(runtime):
            runtime.schedule_callback(job_id, 0)
        return
    if current.get("status") not in ("queued", "processing"):
        return
    remaining = (timestamp_ms(identity["deadlineAt"]) - _now(runtime)) / 1000
    immutable_monotonic_deadline = runtime.monotonic() + max(0, remaining)
    if remaining <= 0:
        runtime.store.transition(job_id, expected_statuses={"queued", "processing"}, changes={"status": "canceled", "stopCode": "RENDITION_EXPIRED"}, retention_seconds=_retention(identity, runtime))
        return
    global_lease = _lease(runtime, "global", "execution", remaining + 10)
    if not global_lease.acquire(blocking=False):
        # Bounded 10s recovery sweep requeues queued work; never spin a task loop.
        return
    lease = _lease(runtime, job_id, "execution", identity["policy"]["leaseMs"] / 1000)
    acquired = False
    active = None
    try:
        acquired = lease.acquire(blocking=False)
        if not acquired:
            return
        current = runtime.store.load(job_id)
        if current.get("status") not in ("queued", "processing"):
            return
        already_attempted = int(current.get("executionAttempts", 0)) >= identity["policy"]["maxExecutionAttempts"]
        _transition(runtime, identity, lease, statuses={current["status"]}, changes={
            "status": "processing", "executionAttempts": int(current.get("executionAttempts", 0)) + (0 if already_attempted else 1)})
        started = runtime.monotonic()
        deadline = min(immutable_monotonic_deadline,
            started + max(0, (timestamp_ms(identity["deadlineAt"]) - _now(runtime)) / 1000),
            started + identity["policy"]["executionBudgetMs"] / 1000)
        payload = None
        with _directory(runtime) as root:
            with _directory(runtime, parent=root, prefix="media-") as media:
                active = _ActiveExecution(identity, runtime, root, lease, global_lease, deadline)
                phase = "downloading"
                try:
                    active.start()
                    if already_attempted:
                        raise RuntimeError("Rendition execution was interrupted")
                    grants = active.refresh()
                    source = media / "source.wav"
                    result = runtime.download(grants["source"]["url"], source,
                        context=TransferContext(media, min(deadline, runtime.monotonic() + (timestamp_ms(grants["source"]["expiresAt"]) - _now(runtime)) / 1000), active.canceled,
                            max_process_rss_bytes=identity["policy"]["sampledProcessGroupRssStopBytes"] // 2),
                        max_bytes=identity["source"]["sizeBytes"])
                    if result.size_bytes != identity["source"]["sizeBytes"] or result.checksum_sha256 != identity["source"]["checksumSha256"]:
                        raise _ControlRejected("RENDITION_OUTPUT_CHANGED")
                    active.check()
                    phase = active.phase = "encoding"
                    active.heartbeat()
                    destination = media / "output.flac"
                    inspection = runtime.encode(identity, source, destination, deadline=min(deadline, runtime.monotonic() + identity["policy"]["encodeBudgetMs"] / 1000), canceled=active.canceled)
                    active.check()
                    phase = active.phase = "uploading"
                    active.heartbeat()
                    grants = active.refresh()
                    active.check()
                    output = validate_model("RenditionOutputResult", {
                        "allocationId": identity["output"]["id"], "fileKey": identity["output"]["fileKey"],
                        "checksumSha256": inspection.checksum_sha256, "pcmChecksumSha256": inspection.pcm_checksum_sha256,
                        "metadata": inspection.metadata})
                    # Full callback validation before PUT binds source PCM/timeline,
                    # allocation, size, deadline and exact selected profile.
                    preview = {"action": "callback", "identity": identity, "status": "completed", "output": output,
                        "processingTimeMs": int((runtime.monotonic() - started) * 1000 + 0.5), "finishedAt": _timestamp(runtime)}
                    validate_model("RenditionCallback", preview)
                    uploaded = runtime.upload(grants["output"]["url"], destination, content_type="audio/flac",
                        context=TransferContext(media, min(deadline, runtime.monotonic() + (timestamp_ms(grants["output"]["expiresAt"]) - _now(runtime)) / 1000), active.canceled,
                            max_process_rss_bytes=identity["policy"]["sampledProcessGroupRssStopBytes"] // 2),
                        max_bytes=identity["output"]["maxSizeBytes"], expected_checksum_sha256=inspection.checksum_sha256)
                    if uploaded.size_bytes != inspection.size_bytes or uploaded.checksum_sha256 != inspection.checksum_sha256:
                        raise RenditionProtocolError()
                    active.check()
                    payload = {**preview, "processingTimeMs": int((runtime.monotonic() - started) * 1000 + 0.5), "finishedAt": _timestamp(runtime)}
                except (ProcessCanceled, TransferCanceled):
                    try:
                        _transition(runtime, identity, lease, statuses={"processing"}, changes={"status": "canceled", "stopCode": active.reason})
                    except state.StateConflict:
                        pass
                except Exception as error:
                    if isinstance(error, _ControlRejected):
                        code = "source_integrity_failed"
                    elif isinstance(error, (ProcessLimit, ProcessDeadline, TransferDeadline)):
                        code = "processing_limit"
                    elif isinstance(error, TransferError):
                        code = "source_unavailable" if phase == "downloading" else "output_unavailable"
                    elif isinstance(error, RenditionProtocolError):
                        code = "output_invalid"
                    else:
                        try:
                            code = validate_model("RenditionFailureCode", getattr(error, "code", "processing_failed"))
                        except RenditionProtocolError:
                            code = "processing_failed"
                    payload = {"action": "callback", "identity": identity, "status": "failed", "failureCode": code,
                        "processingTimeMs": max(0, int((runtime.monotonic() - started) * 1000 + 0.5)), "finishedAt": _timestamp(runtime)}
                finally:
                    active.close()
        # All media/control children and exact-owned temporary files are retired
        # before callback publication or global execution lease release.
        if payload is not None:
            try:
                _persist_terminal(identity, payload, runtime, lease)
            except state.StateConflict:
                pass  # A concurrent signed cancel/lease loss remains terminal.
    finally:
        if acquired:
            _release(lease)
        _release(global_lease)


def deliver_rendition(job_id: str, *, runtime: RenditionRuntime):
    current = runtime.store.load(job_id)
    if not current or current.get("status") != "callback_pending":
        return
    identity = _stored_identity(current, job_id)
    if current.get("nextCallbackAtMs", 0) > _now(runtime):
        return
    lease = _lease(runtime, job_id, "callback", 30)
    if not lease.acquire(blocking=False):
        return
    try:
        current = runtime.store.load(job_id)
        if current.get("status") != "callback_pending":
            return
        attempts = int(current.get("callbackAttempts", 0))
        if attempts >= identity["policy"]["maxCallbackAttempts"] or _now(runtime) >= current["deliveryDeadlineMs"]:
            _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"status": "delivery_exhausted"})
            return
        raw = base64.b64decode(current["terminalBodyBase64"], validate=True)
        if hashlib.sha256(raw).hexdigest() != current["terminalSha256"]:
            raise RenditionProtocolError()
        payload = parse_model("RenditionCallback", raw)
        if payload["identity"] != identity:
            raise RenditionProtocolError()
        attempts += 1
        # A crash after consuming an attempt still leaves a conservative bounded
        # next-delivery time; it cannot bypass the frozen backoff policy.
        backoff = identity["policy"]["callbackBackoffMs"]
        crash_next = min(current["deliveryDeadlineMs"], _now(runtime) + identity["policy"]["controlBudgetMs"] + (backoff[attempts - 1] if attempts <= len(backoff) else 0))
        _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"callbackAttempts": attempts, "nextCallbackAtMs": crash_next})
        retry = False
        try:
            deadline = runtime.monotonic() + max(0, (current["deliveryDeadlineMs"] - _now(runtime)) / 1000)
            with _directory(runtime, prefix="delivery-") as directory:
                response = runtime.post(identity["callbackUrl"], raw, context=TransferContext(directory, deadline, threading.Event(),
                    max_process_rss_bytes=identity["policy"]["sampledProcessGroupRssStopBytes"] // 2),
                    destination_allowed=lambda candidate: runtime.control_allowed(candidate, job_id, "callback"))
            receipt = _response(response, "RenditionCallbackReceipt")
            if receipt["jobId"] != job_id or receipt["payloadSha256"] != current["terminalSha256"]:
                raise _ControlRetry()
            _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"status": payload["status"], "delivered": True})
        except _ControlRejected as error:
            _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"status": "delivery_rejected", "stopCode": error.code})
        except TransferError as error:
            if error.code in ("destination_denied", "redirect_denied", "invalid_allocation", "body_limit"):
                _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"status": "delivery_rejected", "stopCode": "RENDITION_PAYLOAD_INVALID"})
                return
            retry = True
        except _ControlRetry:
            retry = True
        if retry:
            backoff = identity["policy"]["callbackBackoffMs"]
            if attempts >= identity["policy"]["maxCallbackAttempts"]:
                _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"status": "delivery_exhausted"})
                return
            delay = backoff[attempts - 1] / 1000
            if _now(runtime) + int(delay * 1000) >= current["deliveryDeadlineMs"]:
                _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"status": "delivery_exhausted"})
                return
            _transition(runtime, identity, lease, statuses={"callback_pending"}, purpose="callback", changes={"nextCallbackAtMs": _now(runtime) + int(delay * 1000)})
            runtime.schedule_callback(job_id, delay)
    except state.StateConflict:
        return
    finally:
        _release(lease)


def recover_renditions(*, runtime: RenditionRuntime, limit=25):
    if type(limit) is not int or not 1 <= limit <= 25:
        raise ValueError("Invalid rendition recovery limit")
    db = state.client()
    cursor_key = runtime.store.prefix + "recovery-cursor"
    cursor = int(db.get(cursor_key) or 0)
    # Redis COUNT is a hint; retain overflow across sweeps instead of dropping
    # keys or rescanning the first terminal page indefinitely.
    pending_key = runtime.store.prefix + "recovery-pending"
    pending_raw = db.get(pending_key) or "[]"
    if len(pending_raw) > 65536:
        raise ValueError("Rendition recovery cursor exceeds its limit")
    pending = json.loads(pending_raw)
    if not isinstance(pending, list) or len(pending) > 256:
        raise ValueError("Invalid rendition recovery cursor")
    if not pending:
        cursor, keys = db.scan(cursor=cursor, match=runtime.store.prefix + "job:*", count=limit)
        if len(keys) > 256:
            raise ValueError("Rendition recovery scan exceeds its limit")
        pending.extend(keys)
    selected, pending = pending[:limit], pending[limit:]
    db.set(cursor_key, str(cursor), ex=86400)
    pending_raw = json.dumps(pending)
    if len(pending_raw) > 65536:
        raise ValueError("Rendition recovery cursor exceeds its limit")
    db.set(pending_key, pending_raw, ex=86400)
    scheduled = 0
    for key in selected:
        prefix = runtime.store.prefix + "job:"
        if not isinstance(key, str) or not key.startswith(prefix):
            raise ValueError("Invalid rendition recovery namespace")
        raw = db.get(key)
        if not raw:
            continue
        current = json.loads(raw)
        identity = _stored_identity(current, key[len(prefix):])
        job_id = identity["jobId"]
        status = current.get("status")
        if status in ("queued", "processing") and _now(runtime) >= timestamp_ms(identity["deadlineAt"]):
            try:
                runtime.store.transition(job_id, expected_statuses={status}, changes={"status": "canceled", "stopCode": "RENDITION_EXPIRED"}, retention_seconds=_retention(identity, runtime))
            except state.StateConflict:
                pass
            continue
        if status == "callback_pending" and _now(runtime) >= current["deliveryDeadlineMs"]:
            try:
                runtime.store.transition(job_id, expected_statuses={status}, changes={"status": "delivery_exhausted"}, retention_seconds=_retention(identity, runtime))
            except state.StateConflict:
                pass
            continue
        if status == "callback_pending" and current.get("nextCallbackAtMs", 0) <= _now(runtime):
            runtime.schedule_callback(job_id, 0)
            scheduled += 1
        elif status in ("queued", "processing") and not db.exists(runtime.store.lease_key(job_id, "execution")):
            runtime.schedule_execution(job_id)
            scheduled += 1
    return {"visited": len(selected), "scheduled": scheduled, "cursor": cursor}
