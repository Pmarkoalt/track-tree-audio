"""Canonical lifecycle orchestration with controlled state/transport/inspection.

No Redis, codec, HTTP listener or private provider is started by this suite.
Actual shared-helper/codec/state evidence is recorded separately by their owners.
"""

import base64
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from src import rendition_jobs as jobs
from src import state
from src.audio_transfer import TransferError, TransferResult
from src.env import settings
from src.managed_process import ProcessCanceled
from src.rendition_models import RenditionProtocolError, identity_hash, timestamp_ms, validate_identity, wire_bytes
from src.security import create_hmac_signature

FIXTURE = Path(__file__).resolve().parents[1] / "track-tree-contracts/fixtures/rendition-protocol.json"


class Lock:
    def __init__(self, db, key):
        self.db, self.key = db, key
        self.local = SimpleNamespace(token=uuid4().hex)

    def acquire(self, **_):
        if self.key in self.db.leases:
            return False
        self.db.leases[self.key] = self.local.token
        return True

    def owned(self):
        return self.db.leases.get(self.key) == self.local.token

    def extend(self, *_args, **_):
        if not self.owned():
            raise state.StateLeaseLost()

    def release(self):
        if self.owned():
            del self.db.leases[self.key]


class Store:
    prefix = "controlled:rendition:"
    kind = "rendition"

    def __init__(self, db):
        self.db = db
        self.rows = {}

    def lease_key(self, job_id, purpose):
        return self.prefix + purpose + ":" + job_id

    def load(self, job_id):
        return deepcopy(self.rows.get(job_id, {}))

    def save_new(self, job_id, value, *, identity_hash, retention_seconds):
        if job_id in self.rows:
            if self.rows[job_id]["identityHash"] != identity_hash:
                raise state.StateConflict()
            return False, self.load(job_id)
        self.rows[job_id] = {**deepcopy(value), "identityHash": identity_hash}
        return True, self.load(job_id)

    def transition(self, job_id, *, expected_statuses, changes, lease_key=None, lease_token=None, retention_seconds):
        current = self.rows[job_id]
        if current["status"] not in expected_statuses:
            raise state.StateConflict()
        if lease_key is not None and self.db.leases.get(lease_key) != lease_token:
            raise state.StateLeaseLost()
        for key in ("identityHash", "terminalBodyBase64", "terminalSha256"):
            if key in current and key in changes and current[key] != changes[key]:
                raise state.StateConflict()
        current.update(deepcopy(changes))
        return self.load(job_id)


class DB:
    def __init__(self):
        self.leases = {}
        self.values = {}
        self.store = Store(self)
        self.scan_calls = 0
        self.scan_batches = None

    def lock(self, key, **_):
        return Lock(self, key)

    def get(self, key):
        prefix = self.store.prefix + "job:"
        if key.startswith(prefix):
            value = self.store.load(key[len(prefix):])
            return json.dumps(value) if value else None
        return self.values.get(key)

    def set(self, key, value, **_):
        self.values[key] = value

    def exists(self, key):
        return key in self.leases

    def scan(self, *, cursor, match, count):
        self.scan_calls += 1
        if self.scan_batches is not None:
            return self.scan_batches[cursor]
        keys = [self.store.prefix + "job:" + key for key in self.store.rows]
        end = min(cursor + count, len(keys))
        return (0 if end == len(keys) else end), keys[cursor:end]


def iso(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@pytest.fixture
def harness(tmp_path, monkeypatch):
    fixture = json.loads(FIXTURE.read_bytes())
    enqueue = deepcopy(fixture["cases"][0]["payload"])
    identity = enqueue["identity"]
    source = bytes(identity["source"]["sizeBytes"])
    identity["source"]["checksumSha256"] = hashlib.sha256(source).hexdigest()
    clock = [timestamp_ms(identity["createdAt"]) / 1000 + 1]
    db = DB()
    monkeypatch.setattr(state, "client", lambda: db)
    monkeypatch.setattr(settings, "demucssvc_token", fixture["secret"])
    monkeypatch.setattr(settings, "rendition_api_base_url", "https://api.example.test")
    monkeypatch.setattr(settings, "media_url_origins", ["https://media.example.test"])
    os.chmod(tmp_path, 0o700)
    counts = {"encode": 0, "upload": 0, "download": 0}
    queued, callbacks, posts = [], [], []
    fail_callback = []

    def post(url, raw, *, context, destination_allowed):
        assert destination_allowed(url)
        assert context.max_process_rss_bytes == 256 * 1024 * 1024
        assert context.work_directory.stat().st_mode & 0o777 == 0o700
        payload = json.loads(raw)
        posts.append((url, raw))
        if payload["action"] == "heartbeat":
            now = int(clock[0] * 1000)
            value = {"kind": "rendition", "action": "heartbeat", "jobId": identity["jobId"], "status": "active",
                     "observedAt": iso(now), "deadlineAt": identity["deadlineAt"],
                     "leaseExpiresAt": iso(min(now + 60000, timestamp_ms(identity["deadlineAt"])))}
        elif payload["action"] == "refresh":
            now = int(clock[0] * 1000)
            grants = deepcopy(enqueue["grants"])
            grants["issuedAt"] = iso(now)
            for kind in ("source", "output"):
                grants[kind]["expiresAt"] = iso(min(now + 120000, timestamp_ms(identity["deadlineAt"])))
            value = {"action": "refresh", "identity": payload["identity"], "grants": grants}
        else:
            if fail_callback:
                outcome = fail_callback.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                if outcome is not None:
                    return outcome
            value = {"received": True, "kind": "rendition", "jobId": identity["jobId"], "payloadSha256": hashlib.sha256(raw).hexdigest()}
        return TransferResult(200, wire_bytes(value))

    def download(url, path, *, context, **_):
        assert context.max_process_rss_bytes == 256 * 1024 * 1024
        counts["download"] += 1
        path.write_bytes(source)
        return TransferResult(200, size_bytes=len(source), checksum_sha256=hashlib.sha256(source).hexdigest())

    def encode(selected_identity, source_path, destination, *, deadline, canceled):
        counts["encode"] += 1
        destination.write_bytes(b"controlled-encoded-output")
        metadata = {"mimeType": "audio/flac", "codec": "flac", "sampleRateHz": identity["source"]["sampleRateHz"],
                    "channels": 2, "bitDepth": 16, "sampleCount": identity["source"]["sampleCount"],
                    "durationMs": identity["source"]["durationMs"], "sizeBytes": destination.stat().st_size}
        return SimpleNamespace(checksum_sha256=hashlib.sha256(destination.read_bytes()).hexdigest(),
                               pcm_checksum_sha256=identity["source"]["pcmChecksumSha256"], metadata=metadata, size_bytes=metadata["sizeBytes"])

    def upload(url, path, *, expected_checksum_sha256, context, **_):
        assert context.max_process_rss_bytes == 256 * 1024 * 1024
        counts["upload"] += 1
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert digest == expected_checksum_sha256
        return TransferResult(201, size_bytes=path.stat().st_size, checksum_sha256=digest)

    runtime = jobs.RenditionRuntime(tmp_path, encode, queued.append, lambda job_id, delay: callbacks.append((job_id, delay)),
        store=db.store, clock=lambda: clock[0], post=post, download=download, upload=upload)
    return SimpleNamespace(runtime=runtime, enqueue=enqueue, identity=identity, db=db, clock=clock, source=source,
        counts=counts, queued=queued, callbacks=callbacks, posts=posts, fail_callback=fail_callback, secret=fixture["secret"])


def signed(harness, payload):
    raw = wire_bytes(payload)
    timestamp = str(int(harness.clock[0]))
    return raw, "sha256=" + create_hmac_signature(raw, timestamp, harness.secret), timestamp


def accept(harness):
    return jobs.accept_rendition(*signed(harness, harness.enqueue), runtime=harness.runtime, enabled=True)


def terminal(harness):
    return json.loads(base64.b64decode(harness.db.store.rows[harness.identity["jobId"]]["terminalBodyBase64"]))


def test_authentication_before_json_and_disabled_enqueue_has_no_state(harness):
    with pytest.raises(jobs.RenditionJobError) as bad:
        jobs.accept_rendition(b"not-json", "sha256=bad", str(int(harness.clock[0])), runtime=harness.runtime, enabled=True)
    assert bad.value.code == "RENDITION_SIGNATURE_INVALID"
    with pytest.raises(jobs.RenditionJobError) as disabled:
        jobs.accept_rendition(*signed(harness, harness.enqueue), runtime=harness.runtime)
    assert disabled.value.code == "RENDITION_SERVICE_UNAVAILABLE"
    assert not harness.db.store.rows and not harness.queued


def test_cancel_before_enqueue_persists_tombstone_and_identity_conflict(harness):
    cancel = {"action": "cancel", "identity": harness.identity}
    for _ in range(2):
        response = jobs.cancel_rendition(*signed(harness, cancel), runtime=harness.runtime, path_job_id=harness.identity["jobId"].upper())
        assert response["status"] == "canceled"
    with pytest.raises(jobs.RenditionJobError) as late:
        accept(harness)
    assert late.value.code == "RENDITION_CANCELED"
    changed = deepcopy(harness.enqueue)
    changed["identity"]["output"]["fileKey"] += ".changed"
    with pytest.raises(jobs.RenditionJobError) as conflict:
        jobs.accept_rendition(*signed(harness, changed), runtime=harness.runtime, enabled=True)
    assert conflict.value.code == "RENDITION_IDENTITY_CONFLICT"
    assert not harness.queued


@pytest.mark.parametrize("status", ["completed", "failed", "delivery_exhausted", "delivery_rejected"])
def test_cancel_preserves_terminal_outcome_and_bytes(harness, status):
    accept(harness)
    row = harness.db.store.rows[harness.identity["jobId"]]
    row.update(status=status, terminalBodyBase64="preserved-private-bytes", terminalSha256="a" * 64)
    before = deepcopy(row)
    with pytest.raises(jobs.RenditionJobError) as failure:
        jobs.cancel_rendition(*signed(harness, {"action": "cancel", "identity": harness.identity}), runtime=harness.runtime, path_job_id=harness.identity["jobId"])
    assert failure.value.code == "RENDITION_TERMINAL"
    assert row == before


def test_broker_failure_after_durable_state_replay_converges(harness):
    original = harness.runtime.schedule_execution
    harness.runtime.schedule_execution = lambda _: (_ for _ in ()).throw(RuntimeError("private broker diagnostic"))
    with pytest.raises(jobs.RenditionJobError) as failure:
        accept(harness)
    assert failure.value.code == "RENDITION_SERVICE_UNAVAILABLE"
    assert len(harness.db.store.rows) == 1
    harness.runtime.schedule_execution = original
    accept(harness)
    assert len(harness.db.store.rows) == 1 and len(harness.queued) == 1


def test_execution_persists_exact_terminal_before_callback_and_never_reencodes(harness):
    accept(harness)
    job_id = harness.identity["jobId"]
    jobs.execute_rendition(job_id, runtime=harness.runtime)
    row = harness.db.store.rows[job_id]
    assert row["status"] == "callback_pending"
    raw = base64.b64decode(row["terminalBodyBase64"])
    assert hashlib.sha256(raw).hexdigest() == row["terminalSha256"]
    assert terminal(harness)["status"] == "completed"
    assert harness.counts == {"encode": 1, "download": 1, "upload": 1}
    jobs.execute_rendition(job_id, runtime=harness.runtime)
    assert harness.counts["encode"] == 1
    assert list(harness.runtime.work_root.iterdir()) == []
    assert not harness.db.leases


def test_callback_outage_reuses_bytes_and_does_not_execute(harness):
    accept(harness)
    job_id = harness.identity["jobId"]
    jobs.execute_rendition(job_id, runtime=harness.runtime)
    original = harness.db.store.rows[job_id]["terminalBodyBase64"]
    harness.fail_callback.append(TransferError("transfer_failed"))
    jobs.deliver_rendition(job_id, runtime=harness.runtime)
    assert harness.db.store.rows[job_id]["status"] == "callback_pending"
    harness.clock[0] += 1
    jobs.deliver_rendition(job_id, runtime=harness.runtime)
    row = harness.db.store.rows[job_id]
    assert row["status"] == "completed" and row["callbackAttempts"] == 2
    assert row["terminalBodyBase64"] == original
    callbacks = [raw for _, raw in harness.posts if json.loads(raw)["action"] == "callback"]
    assert len(callbacks) == 2 and callbacks[0] == callbacks[1]
    assert harness.counts["encode"] == 1


def test_wrong_rich_receipt_stays_pending_then_exhausts_finitely(harness):
    accept(harness)
    job_id = harness.identity["jobId"]
    jobs.execute_rendition(job_id, runtime=harness.runtime)
    bad = {"received": True, "kind": "rendition", "jobId": job_id, "payloadSha256": "f" * 64}
    for index in range(6):
        harness.fail_callback.append(TransferResult(200, wire_bytes(bad)))
        jobs.deliver_rendition(job_id, runtime=harness.runtime)
        row = harness.db.store.rows[job_id]
        if index < 5:
            assert row["status"] == "callback_pending"
            harness.clock[0] = row["nextCallbackAtMs"] / 1000
    assert row["status"] == "delivery_exhausted" and row["callbackAttempts"] == 6
    assert harness.counts["encode"] == 1


def test_interrupted_execution_is_not_restarted(harness):
    accept(harness)
    row = harness.db.store.rows[harness.identity["jobId"]]
    row.update(status="processing", executionAttempts=1)
    jobs.execute_rendition(harness.identity["jobId"], runtime=harness.runtime)
    assert harness.counts == {"encode": 0, "download": 0, "upload": 0}
    assert terminal(harness)["failureCode"] == "processing_failed"
    assert row["executionAttempts"] == 1


def test_cancel_during_encode_blocks_upload_and_callback(harness):
    accept(harness)
    original = harness.runtime.encode

    def cancel(*args, **kwargs):
        value = original(*args, **kwargs)
        jobs.cancel_rendition(*signed(harness, {"action": "cancel", "identity": harness.identity}), runtime=harness.runtime, path_job_id=harness.identity["jobId"])
        return value

    harness.runtime.encode = cancel
    jobs.execute_rendition(harness.identity["jobId"], runtime=harness.runtime)
    assert harness.db.store.rows[harness.identity["jobId"]]["status"] == "canceled"
    assert harness.counts["upload"] == 0 and not harness.callbacks


def test_independent_api_lease_watchdog_cancels_blocked_encode(harness):
    accept(harness)
    original_post = harness.runtime.post

    def short_lease(url, raw, **options):
        response = original_post(url, raw, **options)
        if json.loads(raw)["action"] == "heartbeat":
            value = json.loads(response.body)
            value["leaseExpiresAt"] = iso(int(harness.clock[0] * 1000) + 60)
            return TransferResult(200, wire_bytes(value))
        return response

    def blocked(identity, source, destination, *, deadline, canceled):
        assert canceled.wait(0.5), "lease watchdog must cancel before third heartbeat"
        raise ProcessCanceled()

    harness.runtime.post = short_lease
    harness.runtime.encode = blocked
    started = time.monotonic()
    jobs.execute_rendition(harness.identity["jobId"], runtime=harness.runtime)
    assert time.monotonic() - started < 0.4
    assert harness.db.store.rows[harness.identity["jobId"]]["status"] == "canceled"
    assert harness.counts["upload"] == 0


def test_global_execution_contention_never_consumes_attempt(harness):
    accept(harness)
    global_key = harness.db.store.lease_key("global", "execution")
    harness.db.leases[global_key] = "another-owner"
    jobs.execute_rendition(harness.identity["jobId"], runtime=harness.runtime)
    row = harness.db.store.rows[harness.identity["jobId"]]
    assert row["status"] == "queued" and row["executionAttempts"] == 0
    assert harness.counts["encode"] == 0
    assert harness.db.leases[global_key] == "another-owner"


def test_recovery_cursor_reaches_work_after_terminal_page(harness):
    for index in range(26):
        identity = deepcopy(harness.identity)
        identity["jobId"] = str(uuid4())
        harness.db.store.rows[identity["jobId"]] = {"identity": identity, "identityHash": identity_hash(identity),
            "status": "completed" if index < 25 else "queued", "executionAttempts": 0}
    first = jobs.recover_renditions(runtime=harness.runtime)
    second = jobs.recover_renditions(runtime=harness.runtime)
    assert first["visited"] == 25 and first["scheduled"] == 0
    assert second["visited"] == 1 and second["scheduled"] == 1
    assert harness.queued == [identity["jobId"]]


def test_recovery_drains_overflow_before_scanning_again(harness):
    accept(harness)
    key = harness.db.store.prefix + "job:" + harness.identity["jobId"]
    harness.db.scan_batches = {0: (13, [key] * 27), 13: (0, [])}
    first = jobs.recover_renditions(runtime=harness.runtime)
    second = jobs.recover_renditions(runtime=harness.runtime)
    assert first["visited"] == 25 and second["visited"] == 2
    assert harness.db.scan_calls == 1


def test_execution_deadline_and_delivery_horizon_stop_recovery(harness):
    accept(harness)
    job_id = harness.identity["jobId"]
    row = harness.db.store.rows[job_id]
    harness.clock[0] = timestamp_ms(harness.identity["deadlineAt"]) / 1000
    harness.queued.clear()
    jobs.recover_renditions(runtime=harness.runtime)
    assert row["status"] == "canceled" and not harness.queued
    row.update(status="callback_pending", deliveryDeadlineMs=int(harness.clock[0] * 1000) - 1)
    jobs.recover_renditions(runtime=harness.runtime)
    assert row["status"] == "delivery_exhausted" and not harness.callbacks


def test_corrupted_persisted_identity_fails_before_compute(harness):
    accept(harness)
    row = harness.db.store.rows[harness.identity["jobId"]]
    row["identity"]["output"]["fileKey"] += ".substituted"
    with pytest.raises(RenditionProtocolError):
        jobs.execute_rendition(harness.identity["jobId"], runtime=harness.runtime)
    assert harness.counts == {"encode": 0, "download": 0, "upload": 0}
    assert not harness.posts and not harness.db.leases


def test_wrong_job_key_cannot_execute_valid_other_identity(harness):
    accept(harness)
    wrong_id = str(uuid4())
    harness.db.store.rows[wrong_id] = deepcopy(harness.db.store.rows[harness.identity["jobId"]])
    with pytest.raises(RenditionProtocolError):
        jobs.execute_rendition(wrong_id, runtime=harness.runtime)
    assert harness.counts == {"encode": 0, "download": 0, "upload": 0}
    assert not harness.posts and not harness.db.leases


def test_corrupt_delivery_identity_never_posts(harness):
    accept(harness)
    job_id = harness.identity["jobId"]
    jobs.execute_rendition(job_id, runtime=harness.runtime)
    harness.posts.clear()
    harness.db.store.rows[job_id]["identityHash"] = "0" * 64
    with pytest.raises(RenditionProtocolError):
        jobs.deliver_rendition(job_id, runtime=harness.runtime)
    assert not harness.posts and not harness.db.leases
