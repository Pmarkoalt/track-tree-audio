"""Independent canonical runner faults; codec subprocesses are controlled here.

Original PCM/file hashes are computed independently from actual synthetic WAV
bytes. Fake FLAC/probe/decode outputs prove orchestration, not codec execution.
"""

import copy
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import struct
import sys
import threading
import time
from unittest.mock import Mock
from types import SimpleNamespace
import wave

import pytest

from src.env import settings
from src.managed_process import ProcessCanceled, ProcessDeadline, ProcessResult
from src.rendition_runner import RenditionFailure, encode_rendition
from src.audio_transfer import TransferError, TransferResult
from src.security import create_hmac_signature


FIXTURE = json.loads((Path(__file__).resolve().parent.parent / "track-tree-contracts/fixtures/rendition-protocol.json").read_bytes())


def _fresh_identity():
    identity = copy.deepcopy(next(item["payload"]["identity"] for item in FIXTURE["cases"] if item["name"] == "enqueue"))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for name, offset in (("createdAt", 0), ("deadlineAt", 180), ("deliveryDeadlineAt", 600), ("retainUntil", 180 + 86400)):
        identity[name] = (now + timedelta(seconds=offset)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return identity


def _stream_pcm_checksum(source):
    digest = hashlib.sha256()
    frames = 0
    with wave.open(str(source), "rb") as audio:
        assert audio.getsampwidth() == 2 and audio.getnchannels() == 2
        while chunk := audio.readframes(4096):
            digest.update(chunk)
            frames += len(chunk) // 4
    return digest.hexdigest(), frames


def _fixture(directory, *, frames=264600, rate=44100, junk=False, extensible=False):
    source = directory / "source.wav"
    pcm = struct.pack("<hh", 1234, -2345) * frames
    with wave.open(str(source), "wb") as audio:
        audio.setnchannels(2)
        audio.setsampwidth(2)
        audio.setframerate(rate)
        audio.writeframes(pcm)
    if extensible:
        guid = bytes.fromhex("0100000000001000800000aa00389b71")
        fmt = struct.pack("<HHIIHHHHI", 0xFFFE, 2, rate, rate * 4, 4, 16, 22, 16, 3) + guid
        body = b"WAVEfmt " + struct.pack("<I", len(fmt)) + fmt + b"data" + struct.pack("<I", len(pcm)) + pcm
        source.write_bytes(b"RIFF" + struct.pack("<I", len(body)) + body)
    source.chmod(0o600)
    if junk:
        body = bytearray(source.read_bytes())
        body.extend(b"JUNK" + struct.pack("<I", 16) + b"metadata-padding")
        struct.pack_into("<I", body, 4, len(body) - 8)
        source.write_bytes(body)
    identity = _fresh_identity()
    if extensible:
        pcm_digest = hashlib.sha256()
        with source.open("rb") as stream:
            stream.seek(68)
            remaining = len(pcm)
            while remaining:
                block = stream.read(min(65536, remaining))
                assert block
                pcm_digest.update(block)
                remaining -= len(block)
        expected_pcm_hash = pcm_digest.hexdigest()
    else:
        expected_pcm_hash = _stream_pcm_checksum(source)[0]
    identity["source"].update(
        checksumSha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        pcmChecksumSha256=expected_pcm_hash,
        sizeBytes=source.stat().st_size,
    )
    return source, directory / "output.flac", identity, pcm


def _controlled_codec(monkeypatch, source, destination, pcm, *, fault=None, canceled=None, source_rate=44100):
    monkeypatch.setattr(settings, "ffmpeg_path", sys.executable)
    monkeypatch.setattr(settings, "ffprobe_path", sys.executable)
    encoded = b"fLaC" + b"controlled-codec-output" * 6
    commands = []

    def execute(command, **options):
        commands.append(command)
        assert options["directory"] == source.parent.resolve()
        assert options["limits"].max_rss_bytes == 256 * 1024 * 1024
        if "-show_entries" in command:
            inspecting_source = command[-1] == str(source)
            if fault == "late_encoded_mutation":
                if not inspecting_source:
                    destination.write_bytes(destination.read_bytes() + b"changed")
            stream = {
                "codec_name": "pcm_s16le" if inspecting_source else "flac",
                "sample_fmt": "s16", "sample_rate": str(source_rate) if inspecting_source else "44100",
                "channels": 2, "bits_per_sample": 16 if inspecting_source else 0,
                "bits_per_raw_sample": "0" if inspecting_source else "16",
            }
            if fault == "wrong_rate" and not inspecting_source:
                stream["sample_rate"] = "48000"
            if fault == "wrong_depth" and not inspecting_source:
                stream.update(sample_fmt="s32", bits_per_raw_sample="24")
            stdout = json.dumps({"streams": [stream]}).encode()
        elif command[-1] == str(destination):
            destination.write_bytes(b"bad!" if fault == "bad_magic" else encoded)
            if fault == "late_source_mutation":
                original = bytearray(source.read_bytes())
                original[44] ^= 1
                source.write_bytes(original)
            stdout = b""
        else:
            decoded = pcm
            decoding_output = str(destination) in command
            if fault == "wrong_pcm" and decoding_output:
                decoded = bytes([pcm[0] ^ 1]) + pcm[1:]
            if fault == "wrong_frames" and decoding_output:
                decoded = pcm[:-4]
            Path(command[-1]).write_bytes(decoded)
            if fault == "late_cancel" and decoding_output:
                canceled.set()
            stdout = b""
        return ProcessResult(stdout, 0, 1024 * 1024, 0.001)

    monkeypatch.setattr("src.rendition_runner.run_managed", execute)
    return commands, encoded


@pytest.mark.parametrize("junk", [False, True])
def test_lossless_pcm_hash_is_streamed_samples_not_container_or_encoded_hash(tmp_path, monkeypatch, junk):
    source, output, identity, pcm = _fixture(tmp_path, junk=junk)
    file_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    pcm_hash, frames = _stream_pcm_checksum(source)
    assert pcm_hash != file_hash and frames == 264600
    commands, encoded = _controlled_codec(monkeypatch, source, output, pcm)
    result = encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert len(commands) == 5
    assert result.pcm_checksum_sha256 == pcm_hash
    assert result.checksum_sha256 == hashlib.sha256(encoded).hexdigest()
    assert result.checksum_sha256 != pcm_hash
    assert result.metadata["sampleCount"] == 264600
    assert hashlib.sha256(source.read_bytes()).hexdigest() == file_hash
    assert output.exists() and not (tmp_path / ".rendition-decoded.pcm").exists()


@pytest.mark.parametrize("field", ["checksumSha256", "pcmChecksumSha256"])
def test_wrong_immutable_file_or_pcm_digest_fails_before_encoding(tmp_path, monkeypatch, field):
    source, output, identity, pcm = _fixture(tmp_path)
    identity["source"][field] = "0" * 64
    commands, encoded = _controlled_codec(monkeypatch, source, output, pcm)
    with pytest.raises(RenditionFailure) as error:
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert error.value.code == "source_integrity_failed"
    assert len(commands) == (0 if field == "checksumSha256" else 2)
    assert source.exists() and not output.exists()


def test_container_digest_cannot_be_substituted_for_source_pcm_digest(tmp_path, monkeypatch):
    source, output, identity, pcm = _fixture(tmp_path)
    identity["source"]["pcmChecksumSha256"] = identity["source"]["checksumSha256"]
    commands, encoded = _controlled_codec(monkeypatch, source, output, pcm)
    with pytest.raises(RenditionFailure) as error:
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert error.value.code == "source_integrity_failed"
    assert len(commands) == 2


@pytest.mark.parametrize("actual", ["frames", "rate", "pcm"])
def test_actual_source_pcm_frame_rate_and_samples_must_match_immutable_identity(tmp_path, monkeypatch, actual):
    source, output, identity, pcm = _fixture(
        tmp_path, frames=264599 if actual == "frames" else 264600,
        rate=48000 if actual == "rate" else 44100, junk=True,
    )
    if actual == "pcm":
        identity["source"]["pcmChecksumSha256"] = hashlib.sha256(b"different-reference-samples").hexdigest()
    commands, encoded = _controlled_codec(monkeypatch, source, output, pcm, source_rate=48000 if actual == "rate" else 44100)
    with pytest.raises(RenditionFailure) as error:
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert error.value.code == "source_integrity_failed"
    assert len(commands) <= 2
    assert not any(command[-1] == str(output) and "-show_entries" not in command for command in commands)
    assert not output.exists()


def test_extensible_pcm16_wav_is_not_excluded_by_python_wave_limit(tmp_path, monkeypatch):
    source, output, identity, pcm = _fixture(tmp_path, extensible=True)
    commands, encoded = _controlled_codec(monkeypatch, source, output, pcm)
    result = encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert len(commands) == 5
    assert result.pcm_checksum_sha256 == hashlib.sha256(pcm).hexdigest()
    assert result.pcm_checksum_sha256 != identity["source"]["checksumSha256"]
    assert source.exists() and output.exists()


@pytest.mark.parametrize("fault", ["wrong_pcm", "wrong_frames", "wrong_rate", "wrong_depth", "bad_magic", "late_encoded_mutation"])
def test_controlled_invalid_output_cannot_be_reported_complete(tmp_path, monkeypatch, fault):
    source, output, identity, pcm = _fixture(tmp_path)
    original = source.read_bytes()
    _controlled_codec(monkeypatch, source, output, pcm, fault=fault)
    with pytest.raises(RenditionFailure) as error:
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert error.value.code == "output_invalid"
    assert source.read_bytes() == original
    assert not output.exists() and not (tmp_path / ".rendition-decoded.pcm").exists()


def test_output_expansion_beyond_server_allocation_is_optional_limit_failure(tmp_path, monkeypatch):
    source, output, identity, pcm = _fixture(tmp_path)
    identity["output"]["maxSizeBytes"] = 64
    _controlled_codec(monkeypatch, source, output, pcm)
    with pytest.raises(RenditionFailure) as error:
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert error.value.code == "processing_limit"
    assert source.exists() and not output.exists()


def test_late_source_change_is_rejected_without_deleting_original(tmp_path, monkeypatch):
    source, output, identity, pcm = _fixture(tmp_path)
    _controlled_codec(monkeypatch, source, output, pcm, fault="late_source_mutation")
    with pytest.raises(RenditionFailure) as error:
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    assert error.value.code == "source_integrity_failed"
    assert source.exists() and not output.exists()


def test_late_cancellation_retires_decoded_and_output_media(tmp_path, monkeypatch):
    source, output, identity, pcm = _fixture(tmp_path)
    canceled = threading.Event()
    _controlled_codec(monkeypatch, source, output, pcm, fault="late_cancel", canceled=canceled)
    with pytest.raises(ProcessCanceled):
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=canceled)
    assert source.exists() and not output.exists() and not (tmp_path / ".rendition-decoded.pcm").exists()


@pytest.mark.parametrize("preexisting", ["output", "decoded"])
def test_existing_private_allocation_is_preserved_and_never_reused(tmp_path, monkeypatch, preexisting):
    source, output, identity, pcm = _fixture(tmp_path)
    protected = output if preexisting == "output" else tmp_path / ".rendition-decoded.pcm"
    protected.write_bytes(b"preexisting-private-original")
    execute = Mock()
    monkeypatch.setattr("src.rendition_runner.run_managed", execute)
    with pytest.raises(RenditionFailure):
        encode_rendition(identity, source, output, deadline=time.monotonic() + 3, canceled=threading.Event())
    execute.assert_not_called()
    assert protected.read_bytes() == b"preexisting-private-original" and source.exists()


def test_expired_immutable_deadline_cannot_be_extended_by_monotonic_caller(tmp_path, monkeypatch):
    source, output, identity, pcm = _fixture(tmp_path)
    for field in ("createdAt", "deadlineAt", "deliveryDeadlineAt", "retainUntil"):
        value = datetime.fromisoformat(identity[field].replace("Z", "+00:00")) - timedelta(seconds=400)
        identity[field] = value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    execute = Mock()
    monkeypatch.setattr("src.rendition_runner.run_managed", execute)
    with pytest.raises(ProcessDeadline):
        encode_rendition(identity, source, output, deadline=time.monotonic() + 60, canceled=threading.Event())
    execute.assert_not_called()
    assert source.exists() and not output.exists()


@pytest.fixture
def job_runtime(tmp_path, monkeypatch):
    """Controlled state seam; proves job call/byte logic, not Redis atomicity."""
    from src import rendition_jobs as jobs
    from src.state import WorkerStateStore
    from src.rendition_models import identity_hash, timestamp_ms

    completed = copy.deepcopy(next(item for item in FIXTURE["cases"] if item["name"] == "completed"))
    identity = completed["payload"]["identity"]
    clock = [timestamp_ms(identity["createdAt"]) / 1000 + 0.2]
    current = {
        "identity": identity, "identityHash": identity_hash(identity), "status": "callback_pending",
        "executionAttempts": 1, "callbackAttempts": 0,
        "terminalBodyBase64": base64.b64encode(completed["rawBody"].encode("utf-8")).decode("ascii"),
        "terminalSha256": hashlib.sha256(completed["rawBody"].encode("utf-8")).hexdigest(),
        "deliveryDeadlineMs": timestamp_ms(completed["payload"]["finishedAt"]) + 120000,
        "nextCallbackAtMs": int(clock[0] * 1000),
    }
    store = Mock(spec=WorkerStateStore)
    real_namespace = WorkerStateStore("rendition")
    store.lease_key.side_effect = real_namespace.lease_key
    store.load.side_effect = lambda job_id: copy.deepcopy(current)

    def transition(job_id, *, changes, **options):
        current.update(copy.deepcopy(changes))
        return copy.deepcopy(current)

    store.transition.side_effect = transition
    store.save_new.return_value = (False, copy.deepcopy(current))
    runtime = jobs.RenditionRuntime(
        work_root=tmp_path, encode=Mock(), schedule_execution=Mock(), schedule_callback=Mock(),
        store=store, control_url=lambda job, action: identity["callbackUrl"],
        control_allowed=lambda *arguments: True, clock=lambda: clock[0],
        post=Mock(), download=Mock(), upload=Mock(),
    )
    lease = SimpleNamespace(
        local=SimpleNamespace(token=b"independent-controlled-lease"),
        acquire=Mock(return_value=True), release=Mock(), owned=Mock(return_value=True),
        extend=Mock(return_value=True),
    )
    monkeypatch.setattr(jobs, "_lease", lambda *arguments: lease)
    monkeypatch.setattr(settings, "demucssvc_token", FIXTURE["secret"])
    monkeypatch.setattr(settings, "media_url_origins", ["https://media.example.test"])
    return SimpleNamespace(jobs=jobs, runtime=runtime, store=store, current=current, identity=identity, clock=clock, lease=lease)


def _signed(context, payload):
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    timestamp = str(int(context.clock[0]))
    signature = "sha256=" + create_hmac_signature(raw, timestamp, FIXTURE["secret"])
    return raw, signature, timestamp


def _receipt(context):
    return {
        "received": True, "kind": "rendition", "jobId": context.identity["jobId"],
        "payloadSha256": context.current["terminalSha256"],
    }


def _http(status, body):
    raw = body if isinstance(body, bytes) else json.dumps(body, separators=(",", ":")).encode()
    return TransferResult(status, raw, len(raw))


@pytest.mark.parametrize("failure", ["signature", "malformed", "disabled", "destination", "overflow"])
def test_enqueue_invalid_auth_payload_destination_or_disabled_never_mutates_state(job_runtime, failure):
    context = job_runtime
    payload = copy.deepcopy(next(item["payload"] for item in FIXTURE["cases"] if item["name"] == "enqueue"))
    if failure == "destination":
        payload["identity"]["callbackUrl"] = "https://foreign.example.test/webhooks/renditions"
    raw, signature, timestamp = _signed(context, payload)
    if failure == "signature":
        raw = b"invalid-json-synthetic-capability"
        signature = "sha256=" + "0" * 64
    elif failure == "malformed":
        raw = b"{not-json"
        signature = "sha256=" + create_hmac_signature(raw, timestamp, FIXTURE["secret"])
    elif failure == "overflow":
        raw = b"x" * 65537
    expected = {"signature": (401, "RENDITION_SIGNATURE_INVALID"), "malformed": (400, "RENDITION_PAYLOAD_INVALID"),
                "disabled": (503, "RENDITION_SERVICE_UNAVAILABLE"), "destination": (400, "RENDITION_PAYLOAD_INVALID"),
                "overflow": (413, "RENDITION_BODY_TOO_LARGE")}
    with pytest.raises(context.jobs.RenditionJobError) as error:
        context.jobs.accept_rendition(raw, signature, timestamp, runtime=context.runtime, enabled=failure != "disabled")
    assert (error.value.status_code, error.value.code) == expected[failure]
    assert context.store.mock_calls == []
    context.runtime.encode.assert_not_called()
    context.runtime.schedule_execution.assert_not_called()


def test_cancel_before_enqueue_persists_full_tombstone_and_blocks_late_enqueue(job_runtime):
    context = job_runtime
    from src.rendition_models import identity_hash

    canceled = {"identity": context.identity, "identityHash": identity_hash(context.identity), "status": "canceled", "executionAttempts": 0, "callbackAttempts": 0}
    context.store.save_new.side_effect = [(True, copy.deepcopy(canceled)), (False, copy.deepcopy(canceled))]
    raw, signature, timestamp = _signed(context, {"action": "cancel", "identity": context.identity})
    assert context.jobs.cancel_rendition(raw, signature, timestamp, path_job_id=context.identity["jobId"], runtime=context.runtime)["status"] == "canceled"
    first = context.store.save_new.call_args
    assert first.args[1]["identity"] == context.identity and first.args[1]["status"] == "canceled"
    assert first.kwargs["retention_seconds"] >= 86400
    enqueue = copy.deepcopy(next(item["payload"] for item in FIXTURE["cases"] if item["name"] == "enqueue"))
    raw, signature, timestamp = _signed(context, enqueue)
    with pytest.raises(context.jobs.RenditionJobError) as error:
        context.jobs.accept_rendition(raw, signature, timestamp, runtime=context.runtime, enabled=True)
    assert (error.value.status_code, error.value.code) == (409, "RENDITION_CANCELED")
    context.runtime.schedule_execution.assert_not_called()
    context.store.transition.assert_not_called()


@pytest.mark.parametrize("status", ["completed", "failed", "delivery_exhausted", "delivery_rejected"])
def test_signed_cancel_cannot_overwrite_already_terminal_work(job_runtime, status):
    context = job_runtime
    context.store.save_new.return_value = (False, {"identity": context.identity, "status": status})
    raw, signature, timestamp = _signed(context, {"action": "cancel", "identity": context.identity})
    with pytest.raises(context.jobs.RenditionJobError) as error:
        context.jobs.cancel_rendition(raw, signature, timestamp, path_job_id=context.identity["jobId"], runtime=context.runtime)
    assert (error.value.status_code, error.value.code) == (409, "RENDITION_TERMINAL")
    context.store.transition.assert_not_called()


def test_callback_retry_preserves_exact_terminal_bytes_and_never_reencodes(job_runtime):
    context = job_runtime
    original = base64.b64decode(context.current["terminalBodyBase64"])
    context.runtime.post.side_effect = [TransferError("transfer_failed"), _http(200, _receipt(context))]
    context.jobs.deliver_rendition(context.identity["jobId"], runtime=context.runtime)
    assert context.current["status"] == "callback_pending" and context.current["callbackAttempts"] == 1
    context.runtime.schedule_callback.assert_called_once_with(context.identity["jobId"], 1.0)
    context.clock[0] += 1.001
    context.jobs.deliver_rendition(context.identity["jobId"], runtime=context.runtime)
    assert context.current["status"] == "completed" and context.current["delivered"] is True
    assert context.current["callbackAttempts"] == 2
    assert [call.args[1] for call in context.runtime.post.call_args_list] == [original, original]
    assert all(call.kwargs["context"].max_process_rss_bytes == 256 * 1024 * 1024 for call in context.runtime.post.call_args_list)
    assert base64.b64decode(context.current["terminalBodyBase64"]) == original
    context.runtime.encode.assert_not_called()
    context.runtime.download.assert_not_called()
    context.runtime.upload.assert_not_called()


@pytest.mark.parametrize("fault", ["job", "digest", "extra", "generic", "false", "html", "202"])
def test_mismatched_or_nonexact_receipt_never_acknowledges_delivery(job_runtime, fault):
    context = job_runtime
    receipt = _receipt(context)
    if fault == "job":
        receipt["jobId"] = "99999999-9999-4999-8999-999999999999"
    elif fault == "digest":
        receipt["payloadSha256"] = "0" * 64
    elif fault == "extra":
        receipt["extra"] = True
    elif fault == "generic":
        receipt = {"received": True}
    elif fault == "false":
        receipt["received"] = False
    elif fault == "html":
        receipt = b"<html>unexpected-success</html>"
    context.runtime.post.return_value = _http(202 if fault == "202" else 200, receipt)
    context.jobs.deliver_rendition(context.identity["jobId"], runtime=context.runtime)
    assert context.current["status"] == "callback_pending"
    assert context.current.get("delivered") is not True
    context.runtime.schedule_callback.assert_called_once_with(context.identity["jobId"], 1.0)
    context.runtime.encode.assert_not_called()


@pytest.mark.parametrize("vector", [item for item in FIXTURE["cases"] if item["schema"] == "RenditionControlProblem"], ids=lambda value: value["name"])
def test_callback_obeys_every_frozen_status_code_pair(job_runtime, vector):
    context = job_runtime
    status = vector["payload"]["status"]
    context.runtime.post.return_value = _http(status, vector["rawBody"].encode("utf-8"))
    context.jobs.deliver_rendition(context.identity["jobId"], runtime=context.runtime)
    if status == 503:
        assert context.current["status"] == "callback_pending"
        context.runtime.schedule_callback.assert_called_once()
    else:
        assert context.current["status"] == "delivery_rejected"
        assert context.current["stopCode"] == vector["payload"]["code"]
        context.runtime.schedule_callback.assert_not_called()
    context.runtime.encode.assert_not_called()


def test_callback_status_code_mismatch_is_a_hard_rejection(job_runtime):
    context = job_runtime
    problem = copy.deepcopy(next(item["payload"] for item in FIXTURE["cases"] if item["name"] == "problem-RENDITION_SIGNATURE_INVALID"))
    context.runtime.post.return_value = _http(409, problem)
    context.jobs.deliver_rendition(context.identity["jobId"], runtime=context.runtime)
    assert context.current["status"] == "delivery_rejected"
    assert context.current["stopCode"] != "RENDITION_SIGNATURE_INVALID"
    context.runtime.schedule_callback.assert_not_called()


def test_lease_loss_before_attempt_fence_prevents_callback_network(job_runtime):
    from src.state import StateLeaseLost

    context = job_runtime
    context.store.transition.side_effect = StateLeaseLost("controlled-fence-loss")
    context.jobs.deliver_rendition(context.identity["jobId"], runtime=context.runtime)
    context.runtime.post.assert_not_called()
    assert context.current["callbackAttempts"] == 0
    attempt = context.store.transition.call_args
    assert attempt.kwargs["lease_token"] == context.lease.local.token
    assert attempt.kwargs["lease_key"] == context.store.lease_key(context.identity["jobId"], "callback")
    context.runtime.encode.assert_not_called()


def test_delivery_attempt_exhaustion_never_reencodes_or_changes_terminal_bytes(job_runtime):
    context = job_runtime
    original = context.current["terminalBodyBase64"]
    context.current["callbackAttempts"] = 6
    context.jobs.deliver_rendition(context.identity["jobId"], runtime=context.runtime)
    assert context.current["status"] == "delivery_exhausted"
    assert context.current["terminalBodyBase64"] == original
    context.runtime.post.assert_not_called()
    context.runtime.encode.assert_not_called()


def test_execution_partitions_each_operation_and_observes_at_most_two_with_heartbeat(tmp_path, monkeypatch, job_runtime):
    """Controlled operations with actual heartbeat threads; no codec/network/RSS claim."""
    from src.rendition_models import identity_hash, timestamp_ms
    from src.rendition_runner import RenditionInspection

    context = job_runtime
    reference, unused, fresh, pcm = _fixture(tmp_path)
    reference_bytes = reference.read_bytes()
    context.identity.clear()
    context.identity.update(fresh)
    context.clock[0] = timestamp_ms(fresh["createdAt"]) / 1000 + 0.2
    context.current.clear()
    context.current.update(identity=context.identity, identityHash=identity_hash(fresh), status="queued", executionAttempts=0, callbackAttempts=0)
    context.runtime.work_root = tmp_path / "job-root"
    mono = [1000.0]
    owner = threading.current_thread()
    background_clock_started = threading.Event()

    def monotonic():
        value = mono[0]
        if threading.current_thread() is not owner:
            background_clock_started.set()
        return value

    context.runtime.monotonic = monotonic
    lock = threading.Lock()
    active = [0]
    peak = [0]
    contexts = []
    encoding = threading.Event()
    overlap = threading.Event()
    release_heartbeat = threading.Event()
    heartbeat_finished = threading.Event()

    def enter():
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])

    def leave():
        with lock:
            active[0] -= 1

    def iso(seconds):
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def control(url, raw, **options):
        contexts.append(options["context"])
        enter()
        blocked = False
        try:
            request = json.loads(raw)
            if request["action"] == "heartbeat":
                if encoding.is_set():
                    blocked = True
                    overlap.set()
                    assert release_heartbeat.wait(2)
                body = {
                    "kind": "rendition", "action": "heartbeat", "status": "active",
                    "jobId": fresh["jobId"], "observedAt": iso(context.clock[0]),
                    "deadlineAt": fresh["deadlineAt"], "leaseExpiresAt": iso(context.clock[0] + 60),
                }
            else:
                assert request["action"] == "refresh"
                expires = iso(context.clock[0] + 120)
                body = {"action": "refresh", "identity": fresh, "grants": {
                    "issuedAt": iso(context.clock[0]),
                    "source": {"url": "https://media.example.test/source", "expiresAt": expires, "method": "GET"},
                    "output": {"url": "https://media.example.test/output", "expiresAt": expires, "method": "PUT", "headers": {"Content-Type": "audio/flac"}},
                }}
            return _http(200, body)
        finally:
            leave()
            if blocked:
                heartbeat_finished.set()

    def download(url, destination, **options):
        contexts.append(options["context"])
        enter()
        try:
            destination.write_bytes(reference_bytes)
            return TransferResult(200, size_bytes=len(reference_bytes), checksum_sha256=hashlib.sha256(reference_bytes).hexdigest())
        finally:
            leave()

    encoded = b"fLaCcontrolled-partition-output"

    def encode(identity, source, destination, **options):
        enter()
        encoding.set()
        try:
            assert background_clock_started.wait(1)
            mono[0] += 21
            context.clock[0] += 21
            assert overlap.wait(2)
            assert peak[0] == 2 and active[0] == 2
            release_heartbeat.set()
            assert heartbeat_finished.wait(1)
            destination.write_bytes(encoded)
            return RenditionInspection(
                hashlib.sha256(encoded).hexdigest(), fresh["source"]["pcmChecksumSha256"], len(encoded),
                44100, 264600, 6000,
            )
        finally:
            release_heartbeat.set()
            encoding.clear()
            leave()

    def upload(url, source, **options):
        contexts.append(options["context"])
        enter()
        try:
            assert source.read_bytes() == encoded
            return TransferResult(200, size_bytes=len(encoded), checksum_sha256=hashlib.sha256(encoded).hexdigest())
        finally:
            leave()

    context.runtime.post = control
    context.runtime.download = download
    context.runtime.encode = encode
    context.runtime.upload = upload
    context.jobs.execute_rendition(fresh["jobId"], runtime=context.runtime)
    assert context.current["status"] == "callback_pending"
    terminal = json.loads(base64.b64decode(context.current["terminalBodyBase64"]))
    assert terminal["status"] == "completed"
    assert peak[0] == 2 and active[0] == 0
    assert len(contexts) >= 7
    assert all(item.max_process_rss_bytes == 256 * 1024 * 1024 for item in contexts)
    assert list(context.runtime.work_root.iterdir()) == []
