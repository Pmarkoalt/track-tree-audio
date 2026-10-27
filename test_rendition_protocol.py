"""Independent exact-byte and strict canonical RND-1-v1 worker conformance."""

import copy
import hashlib
import hmac
import json
from pathlib import Path

import pytest

from src.security import create_hmac_signature, verify_hmac_signature


ROOT = Path(__file__).resolve().parent.parent
FIXTURE_PATH = ROOT / "track-tree-contracts/fixtures/rendition-protocol.json"
MANIFEST_PATH = ROOT / "docs/parallel/contracts/RND-1-v1.manifest.json"
FIXTURE = json.loads(FIXTURE_PATH.read_bytes())


def case(name):
    return copy.deepcopy(next(item for item in FIXTURE["cases"] if item["name"] == name))


def test_fixture_is_the_exact_frozen_release():
    assert hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest() == "dacf8ba13826ea50a08e2837bbccf69936730fddca48d90212f5e6a2cd08d773"
    assert hashlib.sha256(FIXTURE_PATH.read_bytes()).hexdigest() == "ece2265ce12b9df1745243b18ee1ef547777a65efdbe350f347fa7a2d8e1846c"
    assert len(FIXTURE["cases"]) == 24
    assert len(FIXTURE["negativePayloads"]) == 8
    assert len(FIXTURE["authenticationCases"]) == 6


@pytest.mark.parametrize("vector", FIXTURE["cases"], ids=lambda value: value["name"])
def test_actual_security_matches_all_canonical_utf8_vectors(vector):
    raw = vector["rawBody"].encode("utf-8")
    timestamp = FIXTURE["timestamp"]
    signed = timestamp.encode("ascii") + b"." + raw
    independent = "sha256=" + hmac.new(FIXTURE["secret"].encode(), signed, hashlib.sha256).hexdigest()
    assert raw.hex() == vector["bodyUtf8Hex"]
    assert signed.hex() == vector["signedUtf8Hex"]
    assert json.loads(raw) == vector["payload"]
    assert independent == vector["signature"]
    assert "sha256=" + create_hmac_signature(raw, timestamp, FIXTURE["secret"]) == independent
    assert verify_hmac_signature(raw, independent, timestamp, secret=FIXTURE["secret"], now=int(timestamp))


@pytest.mark.parametrize("vector", FIXTURE["authenticationCases"], ids=lambda value: value["name"])
def test_actual_authentication_respects_exact_canonical_negative_vectors(vector):
    assert verify_hmac_signature(
        vector["rawBody"].encode("utf-8"), vector["signature"], vector["timestamp"],
        secret=FIXTURE["secret"], now=int(FIXTURE["timestamp"]),
    ) is (vector["expected"] == "valid")


@pytest.mark.parametrize("difference,expected", [(-301, False), (-300, True), (300, True), (301, False)])
def test_timestamp_freshness_boundary_is_inclusive_at_300_seconds(difference, expected):
    vector = case("enqueue")
    assert verify_hmac_signature(
        vector["rawBody"].encode("utf-8"), vector["signature"], FIXTURE["timestamp"],
        secret=FIXTURE["secret"], now=int(FIXTURE["timestamp"]) + difference,
    ) is expected


def test_receipt_fixture_hashes_terminal_raw_bytes_including_newline_and_unicode():
    raw = case("completed")["rawBody"].encode("utf-8")
    assert hashlib.sha256(raw).hexdigest() == FIXTURE["receipt"]["payloadSha256"]
    assert hashlib.sha256(raw + b" ").hexdigest() != FIXTURE["receipt"]["payloadSha256"]


@pytest.mark.parametrize("vector", FIXTURE["cases"], ids=lambda value: value["name"])
def test_all_signed_payloads_validate_against_actual_canonical_python_binding(vector):
    from src.rendition_models import parse_model, validate_model

    assert validate_model(vector["schema"], vector["payload"]) == vector["payload"]
    assert parse_model(vector["schema"], vector["rawBody"].encode("utf-8")) == vector["payload"]


@pytest.mark.parametrize("vector", FIXTURE["negativePayloads"], ids=lambda value: value["name"])
def test_all_canonical_negative_payloads_fail_actual_binding(vector):
    from src.rendition_models import validate_model

    with pytest.raises(ValueError):
        validate_model(vector["schema"], vector["payload"])


@pytest.mark.parametrize("path", [
    (), ("identity",), ("identity", "source"), ("identity", "profile"),
    ("identity", "policy"), ("identity", "output"), ("grants",),
    ("grants", "source"), ("grants", "output"), ("grants", "output", "headers"),
])
def test_nested_extra_fields_are_rejected_at_every_worker_boundary(path):
    from src.rendition_models import validate_model

    payload = case("enqueue")["payload"]
    selected = payload
    for component in path:
        selected = selected[component]
    selected["unreleasedExecutablePath"] = "/synthetic/private/executable"
    with pytest.raises(ValueError):
        validate_model("RenditionEnqueue", payload)


@pytest.mark.parametrize("bad", [True, "1", 1.1, None])
def test_numeric_literal_revision_does_not_coerce_bool_string_or_fraction(bad):
    from src.rendition_models import validate_identity

    identity = case("enqueue")["payload"]["identity"]
    identity["profile"]["revision"] = bad
    with pytest.raises(ValueError):
        validate_identity(identity)


def test_integral_json_float_is_accepted_and_normalized_like_zod_number():
    from src.rendition_models import validate_identity

    identity = case("enqueue")["payload"]["identity"]
    identity["source"]["sampleCount"] = 264600.0
    identity["profile"]["revision"] = 1.0
    parsed = validate_identity(identity)
    assert parsed["source"]["sampleCount"] == 264600
    assert type(parsed["source"]["sampleCount"]) is int
    assert type(parsed["profile"]["revision"]) is int


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -float("inf"), "100", True, 1.5, 9007199254740992])
def test_callback_milliseconds_are_strict_finite_safe_integers(number):
    from src.rendition_models import validate_model

    payload = case("failed")["payload"]
    payload["processingTimeMs"] = number
    with pytest.raises(ValueError):
        validate_model("RenditionCallback", payload)


def test_source_half_millisecond_rounds_up_like_js_math_round():
    from src.rendition_models import validate_identity

    identity = case("enqueue48")["payload"]["identity"]
    identity["source"].update(sampleCount=288024, durationMs=6001, sizeBytes=288024 * 4 + 44)
    assert validate_identity(identity)["source"]["durationMs"] == 6001
    identity["source"]["durationMs"] = 6000
    with pytest.raises(ValueError):
        validate_identity(identity)


@pytest.mark.parametrize("frames,js_duration", [(384120, 8002), (384456, 8009), (384600, 8012)])
def test_source_duration_matches_frozen_js_ieee754_operation_order(frames, js_duration):
    from src.rendition_models import validate_model

    # Verified against actual frozen Node/Zod source, not ideal rational math:
    #384120/48000*1000 is8002.499999999999, so Math.round returns8002.
    source = case("enqueue48")["payload"]["identity"]["source"]
    source.update(sampleCount=frames, durationMs=js_duration, sizeBytes=frames * 4 + 44)
    assert validate_model("RenditionSource", source)["durationMs"] == js_duration
    source["durationMs"] = js_duration + 1
    with pytest.raises(ValueError):
        validate_model("RenditionSource", source)


@pytest.mark.parametrize("frames,duration", [(264599, 6000), (16758001, 380000)])
def test_exact_source_frames_must_fit_bounds_even_when_rounded_duration_fits(frames, duration):
    from src.rendition_models import validate_identity

    identity = case("enqueue")["payload"]["identity"]
    identity["source"].update(sampleCount=frames, durationMs=duration, sizeBytes=frames * 4 + 44)
    with pytest.raises(ValueError):
        validate_identity(identity)


@pytest.mark.parametrize("bad", ["2026-02-30T16:00:00.000Z", "2026-10-20T16:00:00+00:00", "2026-10-20 16:00:00Z", "2026-10-20T24:00:00Z"])
def test_wire_datetime_rejects_invalid_calendar_offset_and_non_zod_shapes(bad):
    from src.rendition_models import validate_identity

    identity = case("enqueue")["payload"]["identity"]
    identity["createdAt"] = bad
    with pytest.raises(ValueError):
        validate_identity(identity)


def test_timestamp_refinements_use_js_date_parse_millisecond_truncation():
    from src.rendition_models import timestamp_ms

    assert timestamp_ms("2026-10-20T16:00:00.123999Z") == timestamp_ms("2026-10-20T16:00:00.123Z")


def test_validated_timestamp_text_is_preserved_without_implicit_reformatting():
    from src.rendition_models import validate_identity

    identity = case("enqueue")["payload"]["identity"]
    identity["createdAt"] = "2026-10-20T16:00:00Z"
    assert validate_identity(identity)["createdAt"] == "2026-10-20T16:00:00Z"


def test_raw_parser_caps_actual_utf8_bytes_and_accepts_exact_limit():
    from src.rendition_models import parse_model

    raw = case("enqueue")["rawBody"].encode("utf-8")
    padded = b" " * (65536 - len(raw)) + raw
    assert parse_model("RenditionEnqueue", padded) == case("enqueue")["payload"]
    with pytest.raises(ValueError):
        parse_model("RenditionEnqueue", b" " + padded)
    payload = case("enqueue")["payload"]
    payload["correlationId"] = "é" * 33000
    encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    assert len(encoded) > 65536
    with pytest.raises(ValueError):
        parse_model("RenditionEnqueue", encoded)


def test_uuid_normalization_converges_immutable_identity_hash():
    from src.rendition_models import identity_hash, validate_identity

    original = case("enqueue")["payload"]["identity"]
    original["jobId"] = "a1111111-1111-4111-8111-111111111111"
    upper = copy.deepcopy(original)
    upper["jobId"] = upper["jobId"].upper()
    assert validate_identity(upper)["jobId"] == original["jobId"]
    assert identity_hash(validate_identity(upper)) == identity_hash(validate_identity(original))


@pytest.mark.parametrize("vector", [item for item in FIXTURE["cases"] if item["schema"] == "RenditionControlProblem"], ids=lambda value: value["name"])
def test_every_finite_problem_code_rejects_contradictory_http_status(vector):
    from src.rendition_models import validate_model

    payload = copy.deepcopy(vector["payload"])
    payload["status"] = 409 if payload["status"] == 401 else 401
    with pytest.raises(ValueError):
        validate_model("RenditionControlProblem", payload)


@pytest.mark.parametrize("edit", ["issued_before_creation", "expired_at_issue", "lifetime_120001", "issued_at_deadline"])
def test_grants_require_positive_bounded_issued_lifetime_and_immutable_deadline(edit):
    from src.rendition_models import validate_model

    payload = case("enqueue")["payload"]
    if edit == "issued_before_creation":
        payload["grants"]["issuedAt"] = "2026-10-20T15:59:59.999Z"
    elif edit == "expired_at_issue":
        payload["grants"]["source"]["expiresAt"] = payload["grants"]["issuedAt"]
    elif edit == "lifetime_120001":
        payload["grants"]["source"]["expiresAt"] = "2026-10-20T16:02:00.001Z"
    else:
        payload["grants"]["issuedAt"] = payload["identity"]["deadlineAt"]
    with pytest.raises(ValueError):
        validate_model("RenditionEnqueue", payload)


@pytest.mark.parametrize("lease", ["2026-10-20T16:00:00.000Z", "2026-10-20T16:01:00.001Z", "2026-10-20T16:03:00.001Z"])
def test_heartbeat_lease_is_positive_at_most_sixty_seconds_and_not_past_deadline(lease):
    from src.rendition_models import validate_model

    payload = case("active")["payload"]
    payload["leaseExpiresAt"] = lease
    with pytest.raises(ValueError):
        validate_model("RenditionHeartbeatResponse", payload)


@pytest.mark.parametrize("key", ["../foreign.flac", "/absolute.flac", "x/../foreign.flac", "x\\foreign.flac", "x/\x00foreign.flac"])
def test_output_allocation_rejects_unsafe_canonical_object_keys(key):
    from src.rendition_models import validate_identity

    identity = case("enqueue")["payload"]["identity"]
    identity["output"]["fileKey"] = key
    with pytest.raises(ValueError):
        validate_identity(identity)


@pytest.mark.parametrize("url", ["file:///etc/passwd", "https://user:secret@api.example.test/path", "https://api.example.test/path#fragment", "https://api.example.test/path?changed=1"])
def test_callback_canonical_url_rejects_credentials_fragment_or_query(url):
    from src.rendition_models import validate_identity

    identity = case("enqueue")["payload"]["identity"]
    identity["callbackUrl"] = url
    with pytest.raises(ValueError):
        validate_identity(identity)
