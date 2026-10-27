"""Offline unit evidence for private transfer boundaries; no listener or service."""

import hashlib
import json
import os
from pathlib import Path
import threading
import time
from unittest.mock import Mock

import pytest

from src import audio_transfer as transfer
from src import transfer_process as child
from src.env import settings
from src.security import verify_hmac_signature


class Response:
    def __init__(self, status=200, chunks=()):
        self.status_code = status
        self.chunks = chunks

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def iter_content(self, _):
        yield from self.chunks


class Session:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.trust_env = True
        self.uploaded = bytearray()

    def get(self, url, **options):
        self.calls.append(("GET", url, options))
        return self.response

    def post(self, url, **options):
        self.calls.append(("POST", url, options))
        return self.response

    def put(self, url, *, data, **options):
        self.calls.append(("PUT", url, options))
        while block := data.read(4096):
            self.uploaded.extend(block)
        return self.response


@pytest.fixture
def context(tmp_path):
    os.chmod(tmp_path, 0o700)
    return transfer.TransferContext(tmp_path, time.monotonic() + 120, threading.Event())


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setattr(settings, "media_url_origins", ["https://media.example.test"])
    monkeypatch.setattr(settings, "api_webhook_url_allowlist", ["https://api.example.test/callback"])
    monkeypatch.setattr(settings, "demucssvc_token", "synthetic-test-key")


def simulated_child(monkeypatch, response):
    observed = []

    def run(specification, *, directory, context, deadline):
        assert specification.stat().st_mode & 0o777 == 0o600
        assert directory.stat().st_mode & 0o777 == 0o700
        spec = json.loads(specification.read_bytes())
        assert spec["deadline"] == deadline
        assert deadline <= context.deadline_monotonic
        session = Session(response)
        observed.append((spec, session))
        try:
            metadata, body = child.execute_transfer(spec, session)
        except child.TransferFault as error:
            metadata, body = {"ok": False, "code": error.code, "statusCode": error.status_code}, b""
        (directory / "result.json").write_text(json.dumps(metadata))
        (directory / "response.bin").write_bytes(body)

    monkeypatch.setattr(transfer, "_run_child", run)
    return observed


def test_download_streams_exact_bytes_and_removes_only_private_descriptor(context, configured, monkeypatch):
    observed = simulated_child(monkeypatch, Response(chunks=(b"first", b"second")))
    target = context.work_directory / "input.audio"
    result = transfer.download_media("https://media.example.test/source?cap=synthetic", target, context=context)
    assert target.read_bytes() == b"firstsecond"
    assert result.size_bytes == 11
    assert result.checksum_sha256 == hashlib.sha256(b"firstsecond").hexdigest()
    assert target.stat().st_mode & 0o777 == 0o600
    assert list(context.work_directory.iterdir()) == [target]
    _, session = observed[0]
    assert not session.trust_env
    assert session.calls[0][2]["allow_redirects"] is False


def test_download_actual_chunks_cap_and_exact_partial_cleanup(context, configured, monkeypatch):
    simulated_child(monkeypatch, Response(chunks=(b"12345", b"6789")))
    sentinel = context.work_directory / "unrelated.txt"
    sentinel.write_bytes(b"keep")
    target = context.work_directory / "input.audio"
    with pytest.raises(transfer.TransferError, match="^input_limit$"):
        transfer.download_media("https://media.example.test/source", target, context=context, max_bytes=8)
    assert not target.exists()
    assert sentinel.read_bytes() == b"keep"
    assert list(context.work_directory.iterdir()) == [sentinel]


def test_control_signs_exact_bytes_and_retains_safe_result_repr(context, configured, monkeypatch):
    observed = simulated_child(monkeypatch, Response(chunks=(b'{"received":true}',)))
    body = '{"synthetic":"\u2603"} '.encode()
    result = transfer.post_control("https://api.example.test/callback", body, context=context)
    spec, session = observed[0]
    headers = spec["headers"]
    assert verify_hmac_signature(body, headers["X-Signature"], headers["X-Timestamp"])
    assert session.calls[0][2]["data"] == body
    assert result.body == b'{"received":true}'
    assert "received" not in repr(result)
    assert list(context.work_directory.iterdir()) == []


@pytest.mark.parametrize("status", [300, 301, 302, 303, 304, 307, 308, 399])
def test_all_redirects_rejected_without_body_or_followup(context, configured, monkeypatch, status):
    observed = simulated_child(monkeypatch, Response(status, chunks=(b"provider-secret",)))
    with pytest.raises(transfer.TransferError, match="^redirect_denied$") as failure:
        transfer.post_control("https://api.example.test/callback", b"{}", context=context)
    assert failure.value.status_code == status
    assert "provider-secret" not in repr(failure.value)
    assert len(observed[0][1].calls) == 1


def test_control_cap_counts_streamed_bytes_not_declared_headers(context, configured, monkeypatch):
    simulated_child(monkeypatch, Response(chunks=(b"a" * 65536, b"b")))
    with pytest.raises(transfer.TransferError, match="^response_limit$"):
        transfer.post_control("https://api.example.test/callback", b"{}", context=context)


def test_upload_streams_source_and_bounds_response(context, configured, monkeypatch):
    observed = simulated_child(monkeypatch, Response(201, chunks=(b"ok",)))
    source = context.work_directory / "output.flac"
    source.write_bytes(b"fLaC" + b"\x01" * 50000)
    result = transfer.upload_media("https://media.example.test/target", source, content_type="audio/flac", context=context)
    assert observed[0][1].uploaded == source.read_bytes()
    assert result.size_bytes == source.stat().st_size
    assert result.checksum_sha256 == hashlib.sha256(source.read_bytes()).hexdigest()
    assert source.exists()


@pytest.mark.parametrize("limit", [True, False, -1, 0, 67108865, float("nan"), float("inf"), "12"])
def test_invalid_media_limit_never_launches(context, configured, monkeypatch, limit):
    run = Mock(side_effect=AssertionError("must not launch"))
    monkeypatch.setattr(transfer, "_run_child", run)
    with pytest.raises(transfer.TransferError, match="^invalid_limit$"):
        transfer.download_media("https://media.example.test/source", context.work_directory / "new", context=context, max_bytes=limit)
    run.assert_not_called()


@pytest.mark.parametrize("condition", ["canceled", "expired", "nan", "bool"])
def test_inactive_context_never_launches(context, configured, monkeypatch, condition):
    run = Mock(side_effect=AssertionError("must not launch"))
    monkeypatch.setattr(transfer, "_run_child", run)
    if condition == "canceled":
        context.canceled.set()
    else:
        deadline = {"expired": time.monotonic() - 1, "nan": float("nan"), "bool": True}[condition]
        context = transfer.TransferContext(context.work_directory, deadline, context.canceled)
    with pytest.raises(transfer.TransferError):
        transfer.post_control("https://api.example.test/callback", b"{}", context=context)
    run.assert_not_called()


def test_oversized_control_body_and_empty_signing_key_fail_before_launch(context, configured, monkeypatch):
    run = Mock()
    monkeypatch.setattr(transfer, "_run_child", run)
    with pytest.raises(transfer.TransferError, match="^body_limit$"):
        transfer.post_control("https://api.example.test/callback", b"x" * 65537, context=context)
    monkeypatch.setattr(settings, "demucssvc_token", "")
    with pytest.raises(transfer.TransferError, match="^signing_unavailable$"):
        transfer.post_control("https://api.example.test/callback", b"{}", context=context)
    run.assert_not_called()


def test_trusted_control_adapter_is_explicit_and_default_stays_exact(context, configured, monkeypatch):
    observed = simulated_child(monkeypatch, Response(chunks=(b"{}",)))
    url = "https://api.example.test/internal/known-job"
    with pytest.raises(transfer.TransferError, match="^destination_denied$"):
        transfer.post_control(url, b"{}", context=context)
    transfer.post_control(url, b"{}", context=context, destination_allowed=lambda candidate: candidate == url)
    assert len(observed) == 1


def test_external_existing_symlink_and_hardlink_allocations_are_preserved(context, configured, monkeypatch, tmp_path_factory):
    run = Mock()
    monkeypatch.setattr(transfer, "_run_child", run)
    outside = tmp_path_factory.mktemp("transfer-outside") / "original"
    outside.write_bytes(b"keep")
    existing = context.work_directory / "existing"
    existing.write_bytes(b"original")
    link = context.work_directory / "link"
    link.symlink_to(outside)
    for path in (outside, existing, link, context.work_directory / ".." / "escape"):
        with pytest.raises(transfer.TransferError, match="^invalid_allocation$"):
            transfer.download_media("https://media.example.test/source", path, context=context)
    hardlink = context.work_directory / "hardlink"
    os.link(existing, hardlink)
    with pytest.raises(transfer.TransferError, match="^invalid_allocation$"):
        transfer.upload_media("https://media.example.test/target", hardlink, content_type="audio/flac", context=context)
    assert outside.read_bytes() == b"keep" and existing.read_bytes() == b"original"
    run.assert_not_called()


def test_provider_exception_never_escapes_or_retains_partial(context, configured, monkeypatch):
    def fail(specification, **_):
        spec = json.loads(specification.read_bytes())
        Path(spec["path"]).write_bytes(b"partial")
        raise RuntimeError("https://secret.invalid?token=marker Authorization: synthetic-header")

    monkeypatch.setattr(transfer, "_run_child", fail)
    target = context.work_directory / "input"
    with pytest.raises(transfer.TransferError) as failure:
        transfer.download_media("https://media.example.test/source", target, context=context)
    assert str(failure.value) == "transfer_failed"
    assert "marker" not in repr(failure.value)
    assert not target.exists()


def test_cancellation_after_partial_child_cleans_owned_file(context, configured, monkeypatch):
    def cancel(specification, **_):
        spec = json.loads(specification.read_bytes())
        Path(spec["path"]).write_bytes(b"partial")
        context.canceled.set()
        raise RuntimeError("private child diagnostic")

    monkeypatch.setattr(transfer, "_run_child", cancel)
    target = context.work_directory / "input"
    with pytest.raises(transfer.TransferCanceled):
        transfer.download_media("https://media.example.test/source", target, context=context)
    assert not target.exists()


def test_cleanup_never_unlinks_replaced_foreign_inode(context, configured, monkeypatch):
    replacement = context.work_directory / "replacement"
    replacement.write_bytes(b"do-not-remove")

    def replace(specification, **_):
        spec = json.loads(specification.read_bytes())
        os.replace(replacement, spec["path"])
        raise RuntimeError("controlled replacement")

    monkeypatch.setattr(transfer, "_run_child", replace)
    target = context.work_directory / "input"
    with pytest.raises(transfer.TransferError):
        transfer.download_media("https://media.example.test/source", target, context=context)
    assert target.read_bytes() == b"do-not-remove"


@pytest.mark.parametrize("changed", [{"deadline": float("nan")}, {"maxBytes": True}, {"maxBytes": 65537}, {"rawBodyHex": "zz"}])
def test_private_child_rejects_invalid_descriptor_limits(changed):
    spec = {"operation": "control", "url": "https://api.example.test/callback", "headers": {},
            "rawBodyHex": "7b7d", "maxBytes": 65536, "deadline": time.monotonic() + 1}
    with pytest.raises(child.TransferFault):
        child._validate_spec({**spec, **changed})


def test_actual_managed_child_reports_pre_network_failure_and_reaps(context, monkeypatch):
    from src import managed_process

    original = managed_process.run_managed
    observed = []

    def run(command, **options):
        assert "synthetic-capability" not in repr(command)
        assert "TRACK_TREE_SYNTHETIC_SECRET" not in options["environment"]
        return original(command, **options, on_started=observed.append)

    monkeypatch.setenv("TRACK_TREE_SYNTHETIC_SECRET", "do-not-inherit")
    monkeypatch.setattr(managed_process, "run_managed", run)
    deadline = time.monotonic() + 5
    # Invalid local descriptor is rejected before a Session or socket operation.
    spec = {"operation": "control", "url": "https://invalid.test/synthetic-capability",
            "headers": {}, "rawBodyHex": "7b7d", "maxBytes": True, "deadline": deadline}
    with pytest.raises(transfer.TransferError, match="^invalid_limit$"):
        transfer._execute(spec, root=context.work_directory, context=context, deadline=deadline)
    assert len(observed) == 1
    for pid in (observed[0].process_pid, observed[0].guardian_pid):
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        with pytest.raises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
    assert list(context.work_directory.iterdir()) == []


def test_arbitrary_owner_abort_propagates_and_cleans_partial(context, configured, monkeypatch):
    class OwnerAbort(BaseException):
        pass

    def abort(specification, **_):
        spec = json.loads(specification.read_bytes())
        Path(spec["path"]).write_bytes(b"partial")
        raise OwnerAbort()

    monkeypatch.setattr(transfer, "_run_child", abort)
    target = context.work_directory / "partial"
    with pytest.raises(OwnerAbort):
        transfer.download_media("https://media.example.test/source", target, context=context)
    assert not target.exists()
    assert list(context.work_directory.iterdir()) == []


def test_upload_expected_inspected_digest_is_enforced(context, configured, monkeypatch):
    simulated_child(monkeypatch, Response(201))
    source = context.work_directory / "output.flac"
    source.write_bytes(b"actual-encoded-output")
    with pytest.raises(transfer.TransferError, match="^source_changed$"):
        transfer.upload_media("https://media.example.test/target", source, content_type="audio/flac", context=context,
                              expected_checksum_sha256=hashlib.sha256(b"inspected-different-output").hexdigest())
    result = transfer.upload_media("https://media.example.test/target", source, content_type="audio/flac", context=context,
                                   expected_checksum_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    assert result.size_bytes == source.stat().st_size


def test_child_detects_same_size_in_place_upload_mutation(context):
    source = context.work_directory / "source.flac"
    source.write_bytes(b"original")
    info = source.stat()
    spec = {"operation": "upload", "url": "https://media.example.test/output", "headers": {},
            "path": str(source), "fileIdentity": {"device": info.st_dev, "inode": info.st_ino},
            "maxBytes": 67108864, "deadline": time.monotonic() + 1}

    class MutatingSession(Session):
        def put(self, url, **options):
            response = super().put(url, **options)
            source.write_bytes(b"modified")
            os.utime(source, ns=(info.st_atime_ns, info.st_mtime_ns + 1))
            return response

    with pytest.raises(child.TransferFault) as failure:
        child.execute_transfer(spec, MutatingSession(Response(201)))
    assert failure.value.code == "source_changed"


@pytest.mark.parametrize("limit", [True, 0, -1, 536870913, float("nan"), "268435456"])
def test_invalid_trusted_context_rss_budget_never_launches(context, configured, monkeypatch, limit):
    run = Mock()
    monkeypatch.setattr(transfer, "_run_child", run)
    invalid = transfer.TransferContext(context.work_directory, context.deadline_monotonic, context.canceled, max_process_rss_bytes=limit)
    with pytest.raises(transfer.TransferError, match="^invalid_limit$"):
        transfer.post_control("https://api.example.test/callback", b"{}", context=invalid)
    run.assert_not_called()


def test_trusted_rss_partition_reaches_managed_limits(context, monkeypatch):
    from src import managed_process

    observed = []
    monkeypatch.setattr(managed_process, "run_managed", lambda command, **options: observed.append(options))
    partitioned = transfer.TransferContext(context.work_directory, context.deadline_monotonic, context.canceled, max_process_rss_bytes=256 * 1024 * 1024)
    transfer._run_child(context.work_directory / "unused-private-descriptor", directory=context.work_directory,
                        context=partitioned, deadline=time.monotonic() + 1)
    assert observed[0]["limits"].max_rss_bytes == 256 * 1024 * 1024
