"""Independent transfer faults. Offline doubles prove only local invariants."""

import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import threading
import time
from unittest.mock import patch
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit

import pytest

from src.audio_transfer import (
    MAX_CONTROL_BYTES,
    MAX_MEDIA_BYTES,
    TransferCanceled,
    TransferContext,
    TransferDeadline,
    TransferError,
    TransferResult,
    download_media,
    post_control,
    upload_media,
)
from src.env import settings
from src.transfer_process import TransferFault, execute_transfer


MEDIA_URL = "https://media.example.test/object?capability=synthetic-private-marker"
CONTROL_URL = "https://api.example.test/internal/audio-control"


class Response:
    def __init__(self, status=200, chunks=(b"ok",)):
        self.status_code = status
        self.chunks = chunks
        self.headers = {"Content-Length": "1"}

    def __enter__(self):
        return self

    def __exit__(self, *arguments):
        return False

    def iter_content(self, chunk_size):
        yield from self.chunks


class Session:
    trust_env = True

    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **options):
        self.calls.append((url, options))
        return self.response

    def put(self, url, *, data, **options):
        self.calls.append((url, options))
        while data.read(3):
            pass
        return self.response

    def post(self, url, *, data, **options):
        self.calls.append((url, options))
        return self.response


def context(tmp_path, seconds=3, *, canceled=False):
    event = threading.Event()
    if canceled:
        event.set()
    return TransferContext(tmp_path, time.monotonic() + seconds, event)


def allocation(path):
    metadata = path.stat()
    return {"device": metadata.st_dev, "inode": metadata.st_ino}


def spec(path, operation="download", maximum=8):
    return {
        "operation": operation,
        "url": MEDIA_URL,
        "path": str(path),
        "fileIdentity": allocation(path),
        "headers": {},
        "maxBytes": maximum,
        "deadline": time.monotonic() + 3,
    }


def allow_media(monkeypatch):
    monkeypatch.setattr(settings, "media_url_origins", ["https://media.example.test"])


@pytest.mark.parametrize("maximum", [0, -1, True, 1.5, MAX_MEDIA_BYTES + 1])
def test_media_limit_cannot_be_relaxed_by_invalid_values(tmp_path, maximum):
    with patch("src.audio_transfer._run_child") as run:
        with pytest.raises(TransferError, match="^invalid_limit$"):
            download_media(MEDIA_URL, tmp_path / "output", context=context(tmp_path), max_bytes=maximum)
    run.assert_not_called()


@pytest.mark.parametrize("expired", [False, True])
def test_pre_canceled_or_expired_transfer_never_spawns(tmp_path, expired):
    selected = context(tmp_path, -1 if expired else 3, canceled=not expired)
    with patch("src.audio_transfer._run_child") as run:
        with pytest.raises(TransferDeadline if expired else TransferCanceled):
            download_media(MEDIA_URL, tmp_path / "output", context=selected)
    run.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_oversized_control_body_is_refused_before_parse_spawn_or_signing(tmp_path):
    with patch("src.audio_transfer._run_child") as run:
        with pytest.raises(TransferError, match="^body_limit$"):
            post_control(CONTROL_URL, b"x" * (MAX_CONTROL_BYTES + 1), context=context(tmp_path))
    run.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_symlink_or_external_media_allocation_is_rejected(tmp_path, monkeypatch):
    allow_media(monkeypatch)
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.write_bytes(b"original")
    linked = tmp_path / "linked"
    linked.symlink_to(outside)
    try:
        with patch("src.audio_transfer._run_child") as run:
            for destination in (outside, linked):
                with pytest.raises(TransferError, match="^invalid_allocation$"):
                    download_media(MEDIA_URL, destination, context=context(tmp_path))
        run.assert_not_called()
        assert outside.read_bytes() == b"original"
    finally:
        outside.unlink()


def test_upload_hardlink_is_not_a_private_owned_source(tmp_path, monkeypatch):
    allow_media(monkeypatch)
    source = tmp_path / "source"
    source.write_bytes(b"original")
    os.link(source, tmp_path / "shared-hardlink")
    with patch("src.audio_transfer._run_child") as run:
        with pytest.raises(TransferError, match="^invalid_allocation$"):
            upload_media(MEDIA_URL, source, content_type="audio/flac", context=context(tmp_path))
    run.assert_not_called()
    assert source.read_bytes() == b"original"


def test_actual_streamed_bytes_overflow_ignores_false_length(tmp_path):
    target = tmp_path / "download"
    target.touch(mode=0o600)
    session = Session(Response(chunks=(b"abc", b"def")))
    with pytest.raises(TransferFault) as error:
        execute_transfer(spec(target, maximum=5), session)
    assert error.value.code == "input_limit"
    assert target.stat().st_size == 3
    assert not session.trust_env
    assert session.calls[0][1]["allow_redirects"] is False


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_redirect_is_a_failure_before_private_output_is_opened(tmp_path, status):
    target = tmp_path / "download"
    target.touch(mode=0o600)
    session = Session(Response(status=status))
    with pytest.raises(TransferFault) as error:
        execute_transfer(spec(target), session)
    assert error.value.code == "redirect_denied"
    assert error.value.status_code == status
    assert target.read_bytes() == b""
    assert len(session.calls) == 1
    assert session.calls[0][1]["allow_redirects"] is False


def test_control_response_actual_bytes_are_capped_before_return(tmp_path):
    request = {
        "operation": "control", "url": CONTROL_URL,
        "headers": {}, "rawBodyHex": b"{}".hex(),
        "maxBytes": MAX_CONTROL_BYTES, "deadline": time.monotonic() + 3,
    }
    session = Session(Response(chunks=(b"x" * MAX_CONTROL_BYTES, b"y")))
    with pytest.raises(TransferFault) as error:
        execute_transfer(request, session)
    assert error.value.code == "response_limit"


def test_partial_or_swapped_upload_source_cannot_report_success(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"original")
    request = spec(source, "upload", maximum=20)

    class MutatingSession(Session):
        def put(self, url, *, data, **options):
            data.read(1)
            source.write_bytes(b"changed-source")
            return self.response

    with pytest.raises(TransferFault) as error:
        execute_transfer(request, MutatingSession(Response(chunks=())))
    assert error.value.code == "source_changed"


def test_private_descriptor_permissions_and_failed_download_cleanup(tmp_path, monkeypatch, capfd):
    allow_media(monkeypatch)
    original = tmp_path / "original"
    original.write_bytes(b"preserved")
    destination = tmp_path / "download"
    observed = []

    def controlled_failure(specification, *, directory, context, deadline):
        observed.append(directory)
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        assert stat.S_IMODE(specification.stat().st_mode) == 0o600
        request = json.loads(specification.read_text())
        assert request["url"] == MEDIA_URL
        Path(request["path"]).write_bytes(b"partial")
        raise RuntimeError("synthetic-private-marker provider credential")

    monkeypatch.setattr("src.audio_transfer._run_child", controlled_failure)
    with pytest.raises(TransferError, match="^transfer_failed$") as error:
        download_media(MEDIA_URL, destination, context=context(tmp_path))
    assert error.value.__cause__ is None
    assert "synthetic-private-marker" not in repr(error.value)
    assert "synthetic-private-marker" not in repr(capfd.readouterr())
    assert not destination.exists()
    assert all(not path.exists() for path in observed)
    assert original.read_bytes() == b"preserved"


def test_cleanup_never_deletes_replacement_foreign_inode(tmp_path, monkeypatch):
    allow_media(monkeypatch)
    destination = tmp_path / "download"
    moved = tmp_path / "owned-partial"

    def replace(specification, *, directory, context, deadline):
        destination.rename(moved)
        destination.write_bytes(b"foreign-replacement")
        raise RuntimeError("controlled-failure")

    monkeypatch.setattr("src.audio_transfer._run_child", replace)
    with pytest.raises(TransferError):
        download_media(MEDIA_URL, destination, context=context(tmp_path))
    assert destination.read_bytes() == b"foreign-replacement"
    assert moved.read_bytes() == b""


def test_transfer_result_does_not_print_private_control_response():
    result = TransferResult(200, b"synthetic-private-control-body", 30)
    assert "synthetic-private-control-body" not in repr(result)


def test_managed_transfer_argv_and_environment_do_not_carry_capability(tmp_path, monkeypatch):
    allow_media(monkeypatch)
    target = tmp_path / "download"
    observed = []

    def managed(command, **options):
        observed.append(command)
        assert "synthetic-private-marker" not in repr(command)
        assert "synthetic-private-marker" not in repr(options["environment"])
        assert options["directory"] == tmp_path.resolve()
        descriptor = Path(command[-1])
        assert stat.S_IMODE(descriptor.stat().st_mode) == 0o600
        request = json.loads(descriptor.read_bytes())
        assert request["url"] == MEDIA_URL
        Path(request["path"]).write_bytes(b"abc")
        (descriptor.parent / "response.bin").write_bytes(b"")
        (descriptor.parent / "result.json").write_text(json.dumps({
            "ok": True, "statusCode": 200, "sizeBytes": 3,
            "checksumSha256": hashlib.sha256(b"abc").hexdigest(),
        }))

    monkeypatch.setattr("src.managed_process.run_managed", managed)
    result = download_media(MEDIA_URL, target, context=context(tmp_path))
    assert result.size_bytes == 3 and target.read_bytes() == b"abc"
    assert len(observed) == 1
    assert not list(tmp_path.glob("track-tree-transfer-*"))


@contextmanager
def _loopback_receiver(records):
    """One owned socket/thread, activated only by the explicit manual entrypoint."""
    stopping = threading.Event()
    entered = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *arguments):
            pass

        def do_GET(self):
            route = urlsplit(self.path).path
            record = {"method": "GET", "route": route, "responseBytes": 0}
            records.append(record)
            entered.set()
            if route == "/stall-headers":
                stopping.wait(5)
                return
            if route == "/redirect":
                self.send_response(307)
                self.send_header("Location", "/sink")
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            if route == "/stall-body":
                stopping.wait(5)
                return
            try:
                if route == "/slow":
                    while not stopping.wait(0.025):
                        self.wfile.write(b"a")
                        self.wfile.flush()
                        record["responseBytes"] += 1
                else:
                    body = b"x" * 128 if route == "/overflow" else b"actual-media"
                    self.wfile.write(body)
                    self.wfile.flush()
                    record["responseBytes"] += len(body)
            except OSError:
                pass

        def do_PUT(self):
            records.append({"method": "PUT", "route": urlsplit(self.path).path, "responseBytes": 0})
            entered.set()
            # Deliberately stall the exchange without consuming the upload body.
            # This is not a claim of measured TCP send-buffer backpressure.
            stopping.wait(5)

        def do_POST(self):
            route = urlsplit(self.path).path
            record = {"method": "POST", "route": route, "responseBytes": 0}
            records.append(record)
            entered.set()
            self.rfile.read(min(65536, int(self.headers.get("Content-Length", "0"))))
            self.send_response(200)
            self.end_headers()
            try:
                body = b"x" * 65537 if route == "/control-overflow" else b'{"transport":true}'
                self.wfile.write(body)
                self.wfile.flush()
                record["responseBytes"] += len(body)
            except OSError:
                pass

    class QuietServer(HTTPServer):
        def handle_error(self, *arguments):
            pass

    server = QuietServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.025})
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", entered
    finally:
        stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)
        assert not thread.is_alive()


def run_loopback_faults():
    """Explicit integration entrypoint: missing lease/configuration fails closed.

    Default pytest collection never creates a listener. The audio parent records
    this recipe/hash and authorizes the finite loopback lease before invocation.
    """
    lease = os.environ.get("TRACK_TREE_AUDIO_FAULT_LOOPBACK_LEASE")
    declaration = os.environ.get("TRACK_TREE_AUDIO_FAULT_RUNTIME_DIR")
    if not lease or not declaration:
        raise RuntimeError("Explicit loopback lease and private runtime directory are required")
    root = Path(declaration)
    if not root.is_absolute() or not root.is_dir() or stat.S_IMODE(root.stat().st_mode) != 0o700:
        raise RuntimeError("Loopback runtime must be an existing private directory")
    previous_umask = os.umask(0o077)
    report = {
        "scope": "actual managed loopback transfers; no API/provider/wire receipt proof",
        "lease": lease, "cases": [], "requests": [], "failures": [],
        "retiredMedia": [],
        "recipeSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    started = time.monotonic()
    scenarios = (
        "download", "redirect", "overflow", "stall-headers", "stall-body", "slow",
        "cancel", "upload-stall", "control-overflow", "control",
    )
    try:
        for scenario in scenarios:
            if time.monotonic() - started > 30 or len(report["requests"]) >= 12:
                raise AssertionError("Loopback aggregate budget exceeded")
            directory = root / scenario
            directory.mkdir(mode=0o700)
            before_requests = len(report["requests"])
            with _loopback_receiver(report["requests"]) as (origin, entered):
                with pytest.MonkeyPatch.context() as selected:
                    selected.setattr(settings, "media_url_origins", [origin])
                    selected.setattr(settings, "api_webhook_url_allowlist", [origin + "/" + scenario])
                    selected.setattr(settings, "demucssvc_token", "synthetic-loopback-only")
                    case_started = time.monotonic()
                    current = context(directory, 2 if scenario == "cancel" else 0.9)
                    destination = directory / "download"
                    observed = "completed"
                    try:
                        if scenario.startswith("control"):
                            result = post_control(origin + "/" + scenario, b"{}", context=current)
                            assert scenario == "control" and result.body == b'{"transport":true}'
                        elif scenario == "upload-stall":
                            source = directory / "source"
                            source.write_bytes(b"z" * 262144)
                            digest = hashlib.sha256(source.read_bytes()).hexdigest()
                            try:
                                upload_media(
                                    origin + "/upload-stall", source,
                                    content_type="audio/flac", context=current,
                                    expected_checksum_sha256=digest,
                                )
                            finally:
                                assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
                        elif scenario == "cancel":
                            def cancel_after_request():
                                if entered.wait(2):
                                    current.canceled.set()

                            canceler = threading.Thread(target=cancel_after_request)
                            canceler.start()
                            try:
                                download_media(origin + "/stall-body", destination, context=current)
                            finally:
                                canceler.join(timeout=3)
                                assert not canceler.is_alive()
                        else:
                            result = download_media(origin + "/" + scenario, destination, context=current, max_bytes=64)
                            assert scenario == "download"
                            assert destination.read_bytes() == b"actual-media"
                            assert result.checksum_sha256 == hashlib.sha256(b"actual-media").hexdigest()
                    except TransferError as error:
                        observed = error.code
                        expected = {
                            "redirect": "redirect_denied", "overflow": "input_limit",
                            "stall-headers": "deadline", "stall-body": "deadline", "slow": "deadline",
                            "cancel": "canceled", "upload-stall": "deadline", "control-overflow": "response_limit",
                        }
                        assert error.code == expected[scenario]
                        assert not destination.exists()
                    else:
                        assert scenario in ("download", "control")
                    assert entered.is_set(), "The actual receiver must observe the request"
                    assert len(report["requests"]) == before_requests + 1
                    if scenario == "slow":
                        assert report["requests"][-1]["responseBytes"] >= 2
                    assert not any(item["route"] == "/sink" for item in report["requests"])
                    assert not list(directory.glob("track-tree-transfer-*"))
                    report["cases"].append({
                        "case": scenario, "outcome": observed,
                        "elapsedSeconds": round(time.monotonic() - case_started, 3),
                        "port": int(origin.rsplit(":", 1)[1]),
                    })
    except Exception as error:
        report["failures"].append(type(error).__name__)
        raise
    finally:
        for scenario in scenarios:
            for name in ("download", "source"):
                media = root / scenario / name
                if media.is_file() and not media.is_symlink():
                    report["retiredMedia"].append({
                        "relativePath": str(media.relative_to(root)), "bytes": media.stat().st_size,
                        "sha256": hashlib.sha256(media.read_bytes()).hexdigest(),
                    })
                    media.unlink()
        report["elapsedSeconds"] = round(time.monotonic() - started, 3)
        report["responseBytesSent"] = sum(record["responseBytes"] for record in report["requests"])
        report["withinResponseBudget"] = report["responseBytesSent"] <= 4 * 1024 * 1024
        if not report["withinResponseBudget"]:
            report["failures"].append("ResponseBudgetExceeded")
        report["remainingTransferDirectories"] = len(list(root.glob("*/track-tree-transfer-*")))
        destination = root / "loopback-results.json"
        destination.write_text(json.dumps(report, indent=2) + "\n")
        destination.chmod(0o600)
        os.umask(previous_umask)
        print(json.dumps({
            "cases": len(report["cases"]), "requests": len(report["requests"]),
            "failures": report["failures"], "elapsedSeconds": report["elapsedSeconds"],
            "remainingTransferDirectories": report["remainingTransferDirectories"],
        }))
        assert report["withinResponseBudget"]


if __name__ == "__main__":
    if sys.argv[1:] != ["--loopback"]:
        raise SystemExit("Use --loopback with an explicitly granted loopback lease")
    run_loopback_faults()

