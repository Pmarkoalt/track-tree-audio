"""Strict Python binding to the exact frozen RND-1-v1 generated artifact.

The artifact owns shapes/literals. Explicit refinements below mirror canonical
Zod cross-field rules that JSONschema cannot express. No protocol is inferred
from a local DTO; timestamps retain their exact validated wire spelling.
"""

import calendar
from datetime import date
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit

RELEASE_ID = "RND-1-v1"
SCHEMA_SHA256 = "e5f8fe8784572caefda8c83deb2262004f0fc172d21f011260a2ad061561e408"
SCHEMA_PATH = Path(__file__).with_name("contracts") / "rendition-protocol.schema.json"
MAX_SIGNED_BYTES = 65536
_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?Z$", re.ASCII)


class RenditionProtocolError(ValueError):
    def __init__(self):
        super().__init__("Invalid rendition contract")


def _require(condition):
    if not condition:
        raise RenditionProtocolError()


def timestamp_ms(value: str) -> int:
    match = _DATE.fullmatch(value) if isinstance(value, str) else None
    _require(match is not None)
    year, month, day, hour, minute, second = (int(part or 0) for part in match.groups()[:6])
    _require(1 <= month <= 12 and 1 <= day <= calendar.monthrange(year, month)[1])
    _require(hour <= 23 and minute <= 59 and second <= 59)
    if year:
        days = (date(year, month, day) - date(1970, 1, 1)).days
    else:
        days = (date(1, 1, 1) - date(1970, 1, 1)).days - 366
        days += sum(calendar.monthrange(0, prior)[1] for prior in range(1, month)) + day - 1
    # Date.parse truncates submilliseconds, while preserving the original string.
    fraction = (match.group(7) or "")[:3].ljust(3, "0")
    return ((days * 24 + hour) * 3600 + minute * 60 + second) * 1000 + int(fraction)


@lru_cache(maxsize=1)
def _definitions():
    try:
        _require(not SCHEMA_PATH.is_symlink() and SCHEMA_PATH.stat().st_size <= 1024 * 1024)
        raw = SCHEMA_PATH.read_bytes()
        _require(hashlib.sha256(raw).hexdigest() == SCHEMA_SHA256)
        return json.loads(raw)["$defs"]
    except Exception:
        raise RenditionProtocolError() from None


def _type(value, kind):
    return {
        "null": value is None,
        "boolean": type(value) is bool,
        "object": type(value) is dict,
        "array": type(value) is list,
        "string": isinstance(value, str),
        "number": type(value) in (int, float) and math.isfinite(value),
        "integer": type(value) in (int, float) and math.isfinite(value) and int(value) == value,
    }.get(kind, False)


def _http_url(value: str, *, control=False):
    try:
        parsed = urlsplit(value)
        _require(parsed.scheme in ("http", "https") and bool(parsed.hostname))
        _require(not parsed.username and not parsed.password and not parsed.fragment)
        _require(not control or not parsed.query)
        parsed.port  # Reject malformed/out-of-range ports.
    except (ValueError, TypeError):
        raise RenditionProtocolError() from None


def _schema(value, schema, depth=0):
    _require(depth <= 48)
    if "$ref" in schema:
        return _schema(value, _definitions()[schema["$ref"].removeprefix("#/$defs/")], depth + 1)
    for union in ("oneOf", "anyOf"):
        if union in schema:
            accepted = []
            for option in schema[union]:
                try:
                    accepted.append(_schema(value, option, depth + 1))
                except RenditionProtocolError:
                    pass
            _require(len(accepted) == 1 if union == "oneOf" else bool(accepted))
            return accepted[0]
    kinds = schema.get("type", [])
    kinds = [kinds] if isinstance(kinds, str) else kinds
    _require(not kinds or any(_type(value, kind) for kind in kinds))
    if value is None:
        return None
    if type(value) in (int, float):
        _require(math.isfinite(value))
        if value == int(value):
            value = int(value)
        for bound, comparison in (("minimum", lambda a, b: a >= b), ("maximum", lambda a, b: a <= b),
                                  ("exclusiveMinimum", lambda a, b: a > b), ("exclusiveMaximum", lambda a, b: a < b)):
            _require(bound not in schema or comparison(value, schema[bound]))
    if "enum" in schema:
        _require(any(type(value) is type(choice) and value == choice for choice in schema["enum"]) or (
            type(value) in (int, float) and any(type(choice) in (int, float) and value == choice for choice in schema["enum"])
        ))
    if isinstance(value, str):
        length = len(value.encode("utf-16-le", errors="surrogatepass")) // 2
        _require(length >= schema.get("minLength", 0) and length <= schema.get("maxLength", math.inf))
        _require("pattern" not in schema or re.search(schema["pattern"], value) is not None)
        if schema.get("format") == "uuid":
            _require(_UUID.fullmatch(value) is not None)
            value = value.lower()
        elif schema.get("format") == "date-time":
            timestamp_ms(value)
        elif schema.get("format") == "uri":
            _http_url(value)
    elif isinstance(value, list):
        _require(len(value) >= schema.get("minItems", 0) and len(value) <= schema.get("maxItems", math.inf))
        if "prefixItems" in schema:
            # Canonical z.tuple is exact length; its generated JSONschema omits
            # min/maxItems, so preserve this Zod constraint explicitly.
            _require(len(value) == len(schema["prefixItems"]))
            value = [_schema(item, child, depth + 1) for item, child in zip(value, schema["prefixItems"])]
        elif "items" in schema:
            value = [_schema(item, schema["items"], depth + 1) for item in value]
    elif isinstance(value, dict):
        properties = schema.get("properties", {})
        _require(all(key in value for key in schema.get("required", [])))
        _require(schema.get("additionalProperties") is not False or not (set(value) - set(properties)))
        value = {key: _schema(item, properties[key], depth + 1) if key in properties else item for key, item in value.items()}
    return value


def _timeline(value, *, count="sampleCount", duration="durationMs", rate="sampleRateHz"):
    frames, hz = value[count], value[rate]
    _require(6000 * hz <= frames * 1000 <= 380000 * hz)
    _require(rounded_duration_ms(frames, hz) == value[duration])


def rounded_duration_ms(frames: int, rate: int) -> int:
    """Canonical JS Math.round(sampleCount / sampleRateHz * 1000).

    Preserve double operation order: 384120 / 48000 * 1000 is just below
    8002.5 in both JS Number and Python float, so the canonical answer is 8002.
    Integer rational half-up would incorrectly return 8003 for that input.
    """
    value = float(frames) / float(rate) * 1000.0
    integer = math.floor(value)
    return integer + (1 if value - integer >= 0.5 else 0)


def _invariants(value):
    if isinstance(value, list):
        for item in value:
            _invariants(item)
        return
    if not isinstance(value, dict):
        return
    for key, item in value.items():
        if key == "fileKey":
            _require(not item.startswith("/") and not any(part in (".", "..") for part in item.split("/")))
            _require(re.search(r"[\\\x00-\x1f\x7f]", item) is None)
        if key == "callbackUrl":
            _http_url(item, control=True)
        _invariants(item)
    if "sampleCount" in value and "durationMs" in value and "sampleRateHz" in value:
        _timeline(value)
        if value.get("codec") == "pcm_s16le":
            _require(value["sizeBytes"] >= value["sampleCount"] * 4 + 44)
    if value.get("mode") == "identity" and "sourceSampleCount" in value:
        _timeline(value, count="sourceSampleCount", duration="sourceDurationMs")
    if value.get("kind") == "rendition" and "policy" in value:
        created, deadline = timestamp_ms(value["createdAt"]), timestamp_ms(value["deadlineAt"])
        _require(deadline - created == value["policy"]["executionBudgetMs"])
        _require(timestamp_ms(value["deliveryDeadlineAt"]) - created == value["policy"]["callbackCreationHorizonMs"])
        _require(timestamp_ms(value["retainUntil"]) - deadline == 86400000)
    if "identity" in value and "grants" in value:
        identity, grants = value["identity"], value["grants"]
        issued, start, end = timestamp_ms(grants["issuedAt"]), timestamp_ms(identity["createdAt"]), timestamp_ms(identity["deadlineAt"])
        _require(start <= issued < end)
        for item in (grants["source"], grants["output"]):
            expiry = timestamp_ms(item["expiresAt"])
            _require(0 < expiry - issued <= identity["policy"]["maxGrantLifetimeMs"] and expiry <= end)
    if value.get("action") == "heartbeat" and value.get("status") == "active":
        observed, lease, deadline = (timestamp_ms(value[key]) for key in ("observedAt", "leaseExpiresAt", "deadlineAt"))
        _require(0 < lease - observed <= 60000 and lease <= deadline)
    if value.get("action") == "callback":
        identity = value["identity"]
        _require(timestamp_ms(value["finishedAt"]) >= timestamp_ms(identity["createdAt"]))
        if value["status"] == "completed":
            source, output = identity["source"], value["output"]
            _require(value["processingTimeMs"] <= identity["policy"]["executionBudgetMs"])
            _require(timestamp_ms(value["finishedAt"]) <= timestamp_ms(identity["deadlineAt"]))
            _require(output["allocationId"] == identity["output"]["id"] and output["fileKey"] == identity["output"]["fileKey"])
            _require(output["metadata"]["sizeBytes"] <= identity["output"]["maxSizeBytes"])
            _require(output["pcmChecksumSha256"] == source["pcmChecksumSha256"])
            _require(all(output["metadata"][key] == source[key] for key in ("sampleRateHz", "sampleCount", "durationMs")))
    if all(key in value for key in ("canCancel", "failureCode", "renditionId", "status")):
        _require(not value["canCancel"] or value["status"] in ("queued", "processing"))
        _require((value["status"] == "completed") == (value["renditionId"] is not None))
        _require((value["status"] == "failed") == (value["failureCode"] is not None))
    if "metadata" in value and "timeline" in value:
        metadata, timeline = value["metadata"], value["timeline"]
        _require(metadata["sampleRateHz"] == timeline["sampleRateHz"])
        _require(metadata["sampleCount"] == timeline["sourceSampleCount"])
        _require(metadata["durationMs"] == timeline["sourceDurationMs"])
    if "profiles" in value and "latestJob" in value:
        _require(len(value["catalog"]) == 1 and len(value["profiles"]) == 1)
        _require(value["originalStatus"] == "ready" or not any(item["status"] in ("ready", "queued", "processing") for item in value["profiles"]))
        latest = value["latestJob"]
        _require(latest is None or latest["versionId"] == value["versionId"])
        expected = "no_terminal_job" if latest is None else "job_active" if latest["status"] in ("queued", "processing") else "job_completed" if latest["status"] == "completed" else None
        if expected is not None:
            _require(value["retry"] == {"status": "blocked", "reason": expected})
        elif value["retry"]["status"] == "blocked":
            _require(value["retry"]["reason"] not in ("no_terminal_job", "job_active", "job_completed"))
        for item in value["profiles"]:
            if item["status"] in ("queued", "processing"):
                _require(latest is not None and item["jobId"] == latest["id"] and item["status"] == latest["status"])


def validate_model(name: str, value: Any) -> Any:
    try:
        _require(isinstance(name, str) and name.startswith("Rendition") and name in _definitions())
        value = _schema(value, _definitions()[name])
        _invariants(value)
        if name == "RenditionControlUrl":
            _http_url(value, control=True)
        if name == "RenditionObjectKey":
            _invariants({"fileKey": value})
        return value
    except Exception:
        raise RenditionProtocolError() from None


def parse_model(name: str, raw_body: bytes) -> dict:
    try:
        _require(isinstance(raw_body, bytes) and len(raw_body) <= MAX_SIGNED_BYTES)
        value = json.loads(raw_body.decode("utf-8"), parse_constant=lambda _: (_ for _ in ()).throw(RenditionProtocolError()))
        return validate_model(name, value)
    except Exception:
        raise RenditionProtocolError() from None


def validate_identity(value: dict) -> dict:
    return validate_model("RenditionIdentity", value)


def wire_bytes(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def identity_hash(value: dict) -> str:
    value = validate_identity(value)
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
