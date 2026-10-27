"""Redis worker state is separate from API/Postgres, including durable callback data."""

import json
import hashlib
from typing import Literal
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
    return worker_store("split").job_key(job_id)


def load(job_id: str) -> dict:
    return worker_store("split").load(job_id)


def save(job_id: str, value: dict):
    worker_store("split").save(job_id, value)


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


class StateConflict(Exception):
    """A competing transition, phase or immutable identity prevents this write."""


class StateLeaseLost(StateConflict):
    """The execution/delivery lease no longer belongs to this worker."""


# Compare the exact saved JSON, never reserialize terminal bytes through Lua.
# The current lease token is checked in the same atomic operation as the write.
_TRANSITION = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[4] == '1' and redis.call('GET', KEYS[2]) ~= ARGV[3] then
  return -1
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[5])
return 1
"""
_MAX_STATE_BYTES = 256 * 1024
_IMMUTABLE = ("identityHash", "terminalBodyBase64", "terminalSha256")


def _encode(value: dict) -> str:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if len(raw.encode("utf-8")) > _MAX_STATE_BYTES:
        raise ValueError("Worker state exceeds its byte budget")
    return raw


def _retention(seconds: int) -> int:
    if type(seconds) is not int or seconds < 1 or seconds > 8 * 86400:
        raise ValueError("Invalid worker state retention")
    return seconds


class WorkerStateStore:
    """Trusted job-kind namespace with atomic local execution/delivery fencing.

    Postgres remains the application authority. This store cannot authorize a
    source, grant, output or terminal publication. Kind is selected by trusted
    registration code, never an HTTP field. Legacy split key bytes are retained.
    """

    def __init__(self, kind: Literal["split", "rendition"]):
        if kind not in ("split", "rendition"):
            raise ValueError("Unknown worker state kind")
        self.kind = kind

    @property
    def prefix(self) -> str:
        return settings.audio_redis_prefix + (
            "" if self.kind == "split" else "rendition:"
        )

    def job_key(self, job_id: str) -> str:
        if not isinstance(job_id, str) or not job_id or len(job_id) > 200:
            raise ValueError("Invalid worker state identifier")
        return self.prefix + "job:" + job_id

    def lease_key(
        self, job_id: str, purpose: Literal["execution", "callback"]
    ) -> str:
        # Validate the same bounded opaque ID, while keeping purposes separate.
        self.job_key(job_id)
        if purpose not in ("execution", "callback"):
            raise ValueError("Unknown worker lease purpose")
        return self.prefix + purpose + "-lease:" + job_id

    def load(self, job_id: str) -> dict:
        value = client().get(self.job_key(job_id))
        return json.loads(value) if value else {}

    def save(self, job_id: str, value: dict):
        # Kept for compatibility with legacy split state.patch and save callers.
        # New rendition writers use save_new/transition rather than blind saves.
        if self.kind != "split":
            raise StateConflict("Rendition state requires an atomic transition")
        client().set(
            self.job_key(job_id),
            _encode(value),
            ex=settings.audio_job_retention_seconds,
        )

    def save_new(
        self,
        job_id: str,
        value: dict,
        *,
        identity_hash: str,
        retention_seconds: int,
    ) -> tuple[bool, dict]:
        if (
            not isinstance(identity_hash, str)
            or len(identity_hash) != 64
            or any(char not in "0123456789abcdef" for char in identity_hash)
        ):
            raise ValueError("Invalid worker identity fingerprint")
        if "identityHash" in value and value["identityHash"] != identity_hash:
            raise StateConflict("Worker immutable identity conflict")
        saved = {**value, "identityHash": identity_hash}
        db = client()
        if db.set(
            self.job_key(job_id),
            _encode(saved),
            nx=True,
            ex=_retention(retention_seconds),
        ):
            return True, saved
        existing = self.load(job_id)
        if existing.get("identityHash") != identity_hash:
            raise StateConflict("Worker immutable identity conflict")
        return False, existing

    def transition(
        self,
        job_id: str,
        *,
        expected_statuses: set[str],
        changes: dict,
        lease_key: str | None = None,
        lease_token: str | bytes | None = None,
        retention_seconds: int,
    ) -> dict:
        if (lease_key is None) != (lease_token is None):
            raise ValueError("Worker lease key and token must be supplied together")
        if lease_key is not None and lease_key not in (
            self.lease_key(job_id, "execution"),
            self.lease_key(job_id, "callback"),
        ):
            raise ValueError("Worker lease belongs to another job or kind")
        db = client()
        key = self.job_key(job_id)
        raw = db.get(key)
        if not raw:
            raise StateConflict("Worker state is missing")
        current = json.loads(raw)
        if current.get("status") not in expected_statuses:
            raise StateConflict("Worker phase conflict")
        for name in _IMMUTABLE:
            if name in current and name in changes and current[name] != changes[name]:
                raise StateConflict("Worker immutable state conflict")
        desired = {**current, **changes}
        # Persist exact UTF-8 terminal bytes, encoded once by the job layer. A
        # matching digest is checked here even before any callback is scheduled.
        terminal = desired.get("terminalBodyBase64")
        terminal_hash = desired.get("terminalSha256")
        if (terminal is None) != (terminal_hash is None):
            raise ValueError("Terminal worker bytes and digest must be paired")
        if terminal is not None:
            import base64
            import binascii

            try:
                body = base64.b64decode(terminal, validate=True)
                body.decode("utf-8")
            except (ValueError, TypeError, UnicodeError, binascii.Error):
                raise ValueError("Invalid terminal worker bytes") from None
            if len(body) > 65536 or hashlib.sha256(body).hexdigest() != terminal_hash:
                raise ValueError("Terminal worker bytes do not match digest")
        result = db.eval(
            _TRANSITION,
            2,
            key,
            lease_key if lease_key is not None else key,
            raw,
            _encode(desired),
            lease_token if lease_token is not None else "",
            "1" if lease_key is not None else "0",
            _retention(retention_seconds),
        )
        if result == -1:
            raise StateLeaseLost("Worker lease ownership changed")
        if result != 1:
            raise StateConflict("Worker state changed concurrently")
        return desired


def worker_store(kind: Literal["split", "rendition"]) -> WorkerStateStore:
    return WorkerStateStore(kind)
