"""Authored/unrun receipt and managed-control tests; no real child process.

These exercise real producer control flow with native/spawn/signal edges mocked.
Future root authorization is required before running even these checks.
"""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from src import managed_process as mp
from src import process_guard as pg


def controls(tmp_path, monkeypatch):
    directory = tmp_path / "control"
    directory.mkdir(mode=0o700)
    registry = directory / "ownership.jsonl"
    registry.write_bytes(b"")
    registry.chmod(0o600)
    acks = directory / "acks"
    acks.mkdir(mode=0o700)
    monkeypatch.setattr(pg.time, "monotonic", lambda: 100.0)
    return {"TT_SUPERVISOR_REGISTRY": str(registry.resolve()),
            "TT_SUPERVISOR_ACKS": str(acks.resolve()),
            "TT_SUPERVISOR_WORK_END": "110", "TT_SUPERVISOR_TOTAL_END": "120",
            "TT_SUPERVISOR_ROLE": "execution-worker"}


def ack(values, identity="media", **changes):
    value = {"pid": 123, "pgid": 123, "sec": 1, "usec": 2, **changes}
    path = Path(values["TT_SUPERVISOR_ACKS"]) / (identity + ".json")
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    return path


def test_absent_controls_are_inactive_and_partial_controls_fail_closed():
    assert pg.SupervisorContext.from_environment({}) is None
    with pytest.raises(pg.SupervisorReceiptError, match="Partial"):
        pg.SupervisorContext.from_environment({"TT_SUPERVISOR_ROLE": "worker"})


@pytest.mark.parametrize("key,value", [
    ("TT_SUPERVISOR_WORK_END", "nan"), ("TT_SUPERVISOR_TOTAL_END", "inf"),
    ("TT_SUPERVISOR_TOTAL_END", "105"), ("TT_SUPERVISOR_ROLE", "../wrong"),
    ("TT_SUPERVISOR_REGISTRY", "relative"),
])
def test_invalid_control_values_rejected_before_spawn(tmp_path, monkeypatch, key, value):
    values = controls(tmp_path, monkeypatch)
    values[key] = value
    with pytest.raises(pg.SupervisorReceiptError):
        pg.SupervisorContext.from_environment(values)


def test_private_control_symlink_and_loose_modes_rejected(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    registry = Path(values["TT_SUPERVISOR_REGISTRY"])
    registry.chmod(0o644)
    with pytest.raises(pg.SupervisorReceiptError):
        pg.SupervisorContext.from_environment(values)
    registry.chmod(0o600)
    alias = registry.parent / "alias"
    alias.symlink_to(registry)
    with pytest.raises(pg.SupervisorReceiptError):
        pg.SupervisorContext.from_environment({**values, "TT_SUPERVISOR_REGISTRY": str(alias)})


def test_exact_ack_admits_only_known_child_and_projected_controls(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    ack(values)
    context.register(SimpleNamespace(pid=123), "media", "audio-media", 109, threading.Event())
    record = json.loads(Path(values["TT_SUPERVISOR_REGISTRY"]).read_text())
    assert record == {"kind": "register", "id": "media", "owner": "execution-worker", "role": "audio-media", "pid": 123}
    child = context.child_environment({"PATH": "/usr/bin"}, "media")
    assert child == {"PATH": "/usr/bin", **values, "TT_SUPERVISOR_ROLE": "media"}


@pytest.mark.parametrize("changes", [{"pid": 124}, {"pgid": 124}, {"sec": True}, {"usec": 1_000_000}])
def test_wrong_native_ack_never_releases_gate(tmp_path, monkeypatch, changes):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    ack(values, **changes)
    with pytest.raises(pg.SupervisorReceiptError, match="ACK"):
        context.register(SimpleNamespace(pid=123), "media", "audio-media", 109, threading.Event())
    assert context.closed == set()


def test_missing_ack_uses_original_work_end_without_timeout_extension(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    clock = [109.99]
    monkeypatch.setattr(pg.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(pg.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(pg.SupervisorReceiptError, match="deadline"):
        context.register(SimpleNamespace(pid=123), "media", "audio-media", 500, threading.Event())
    assert clock[0] <= 110.001


def test_cancel_before_register_does_not_append(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    event = threading.Event()
    event.set()
    with pytest.raises(pg.SupervisorReceiptError, match="canceled"):
        context.register(SimpleNamespace(pid=123), "media", "audio-media", 109, event)
    assert Path(values["TT_SUPERVISOR_REGISTRY"]).read_bytes() == b""


def test_registry_inode_replacement_and_ack_symlink_fail_closed(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    registry = Path(values["TT_SUPERVISOR_REGISTRY"])
    old = registry.with_suffix(".old")
    registry.rename(old)
    registry.write_bytes(b"")
    registry.chmod(0o600)
    with pytest.raises(pg.SupervisorReceiptError, match="changed"):
        context.register(SimpleNamespace(pid=123), "media", "audio-media", 109, threading.Event())
    registry.unlink()
    old.rename(registry)
    original = ack(values, identity="other")
    (Path(values["TT_SUPERVISOR_ACKS"]) / "media.json").symlink_to(original)
    with pytest.raises((OSError, pg.SupervisorReceiptError)):
        context.register(SimpleNamespace(pid=123), "media", "audio-media", 109, threading.Event())


def test_settlement_requires_actual_returncode_closed_handles_and_is_once_only(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    context.registered.add("media")
    stream = io.BytesIO()
    child = SimpleNamespace(returncode=None, stdout=stream)
    with pytest.raises(pg.SupervisorReceiptError):
        context.settled(child, "media")
    child.returncode = 0
    with pytest.raises(pg.SupervisorReceiptError):
        context.settled(child, "media")
    stream.close()
    context.settled(child, "media")
    with pytest.raises(pg.SupervisorReceiptError):
        context.settled(child, "media")
    records = Path(values["TT_SUPERVISOR_REGISTRY"]).read_text().splitlines()
    assert len(records) == 1 and json.loads(records[0])["waited"] is True


class FakeContext:
    work = 110.0
    stop = 120.0

    def __init__(self, events, refuse=None):
        self.events, self.refuse = events, refuse

    def work_deadline(self, deadline):
        return min(deadline, self.work)

    def timeout(self, deadline, maximum):
        remaining = min(deadline, self.stop) - 100
        if remaining <= 0:
            raise pg.SupervisorReceiptError("Supervisor deadline exceeded")
        return min(maximum, remaining)

    def new_id(self, role):
        return role

    def child_environment(self, environment, identity):
        return {**environment, "TT_SUPERVISOR_ROLE": identity}

    def register(self, process, identity, role, deadline, canceled):
        self.events.append(("register", role))
        if role == self.refuse:
            raise pg.SupervisorReceiptError("Refused ACK")

    def settled(self, process, identity):
        assert process.waits > 0
        assert process.stdout is None or process.stdout.closed
        self.events.append(("settled", identity))


class FakeProcess:
    def __init__(self, pid, events, media=False, fail_wait=False):
        self.pid, self.events, self.media = pid, events, media
        self.fail_wait = fail_wait
        self.stdout = None
        self.returncode = None
        self.waits = 0

    def poll(self):
        if self.media:
            self.returncode = 0
        return self.returncode

    def wait(self, timeout):
        self.events.append(("wait", self.pid, timeout))
        if self.fail_wait:
            raise subprocess.TimeoutExpired("controlled", timeout)
        self.waits += 1
        self.returncode = 0
        return 0


def fake_managed(monkeypatch, *, refuse=None, fail_media_wait=False):
    events = []
    context = FakeContext(events, refuse)
    monkeypatch.setattr(mp.SupervisorContext, "from_environment", classmethod(lambda cls: context))
    monkeypatch.setattr(mp.time, "monotonic", lambda: 100.0)
    children = [FakeProcess(501, events), FakeProcess(502, events, media=True, fail_wait=fail_media_wait)]
    pending = iter(children)
    monkeypatch.setattr(mp.subprocess, "Popen", lambda *args, **kwargs: next(pending))
    descriptors = iter([(11, 12), (13, 14), (15, 16)])
    monkeypatch.setattr(mp.os, "pipe", lambda: next(descriptors))
    monkeypatch.setattr(mp.os, "close", lambda fd: events.append(("close", fd)))
    monkeypatch.setattr(mp.os, "set_blocking", lambda *args: None)
    monkeypatch.setattr(mp.os, "read", lambda *args: b"ARMED\n")

    def write(fd, value):
        events.append(("payload" if value.startswith(b"{") else "control", fd))
        return len(value)

    monkeypatch.setattr(mp.os, "write", write)
    monkeypatch.setattr(mp.select, "select", lambda r, w, e, timeout: (r, w, []))
    monkeypatch.setattr(mp, "_kill_group", lambda child, *args: events.append(("kill", child.pid)))
    return context, children, events


def test_real_managed_flow_registers_both_children_before_payload_and_keeps_old_hook_after(monkeypatch):
    _, children, events = fake_managed(monkeypatch)
    mp.run_managed([sys.executable, "-c", "pass"], environment={}, deadline=200,
                   canceled=threading.Event(), on_started=lambda pids: events.append(("old-hook", pids.process_pid)))
    payload = next(i for i, event in enumerate(events) if event[0] == "payload")
    assert events.index(("register", "audio-guardian")) < payload
    assert events.index(("register", "audio-media")) < payload
    assert events.index(("old-hook", 502)) > payload
    for child, identity in zip(children, ("audio-guardian", "audio-media")):
        assert child.waits > 0 and events.count(("settled", identity)) == 1
    assert events.index(("close", 16)) < events.index(("settled", "audio-media"))


@pytest.mark.parametrize("refuse", ["audio-guardian", "audio-media"])
def test_registration_failure_never_writes_payload_and_awaits_spawned_children(monkeypatch, refuse):
    _, children, events = fake_managed(monkeypatch, refuse=refuse)
    with pytest.raises(mp.ProcessFailed):
        mp.run_managed([sys.executable, "-c", "pass"], environment={}, deadline=200, canceled=threading.Event())
    assert not any(event[0] == "payload" for event in events)
    assert children[0].waits > 0
    if refuse == "audio-media":
        assert children[1].waits > 0


def test_media_wait_failure_does_not_skip_guardian_wait_or_claim_media_settlement(monkeypatch):
    _, children, events = fake_managed(monkeypatch, fail_media_wait=True)
    with pytest.raises(mp.ProcessFailed):
        mp.run_managed([sys.executable, "-c", "pass"], environment={}, deadline=200, canceled=threading.Event())
    assert ("settled", "audio-media") not in events
    assert children[0].waits > 0 and ("settled", "audio-guardian") in events


def test_expired_outer_work_budget_prevents_any_spawn(monkeypatch):
    context, _, events = fake_managed(monkeypatch)
    context.work = 99.0
    with pytest.raises(mp.ProcessDeadline):
        mp.run_managed([sys.executable, "-c", "pass"], environment={}, deadline=200, canceled=threading.Event())
    assert events == []


def test_root_stop_has_no_positive_timeout_padding(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    monkeypatch.setattr(pg.time, "monotonic", lambda: 120.0)
    with pytest.raises(pg.SupervisorReceiptError, match="deadline"):
        context.timeout(500, 2)
    # Expired controls remain parseable for a final immediate owned-group kill.
    assert pg.SupervisorContext.from_environment(values).stop == 120


class FakePipe:
    closed = False

    def fileno(self):
        return 77

    def close(self):
        self.closed = True


def test_supervised_sampler_is_same_group_and_actually_waits_closes_on_failure(monkeypatch):
    events = []
    context = FakeContext(events)
    monkeypatch.setattr(mp.SupervisorContext, "from_environment", classmethod(lambda cls: context))
    monkeypatch.setattr(mp.time, "monotonic", lambda: 100.0)
    child = FakeProcess(777, events)
    child.stdout = FakePipe()

    def spawn(*args, **kwargs):
        assert kwargs["start_new_session"] is False
        return child

    def kill():
        events.append(("direct-kill", child.pid))
        child.returncode = -9

    child.kill = kill
    monkeypatch.setattr(mp.subprocess, "Popen", spawn)
    monkeypatch.setattr(mp.os, "set_blocking", lambda *args: None)
    monkeypatch.setattr(mp.select, "select", lambda *args: ([], [], []))
    monkeypatch.setattr(mp, "_kill_group", lambda *args: pytest.fail("sampler must not kill its shared group"))
    with pytest.raises(mp.ProcessLimit):
        mp._sample_owned_rss(501, 502, 110)
    assert ("direct-kill", 777) in events
    assert child.waits == 1 and child.stdout.closed


def test_eperm_group_probe_is_same_group_and_awaited_without_detached_spawn(monkeypatch):
    events = []
    context = FakeContext(events)
    monkeypatch.setattr(pg.SupervisorContext, "from_environment", classmethod(lambda cls: context))
    monkeypatch.setattr(pg.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(pg.os, "killpg", lambda *args: (_ for _ in ()).throw(PermissionError()))
    child = FakeProcess(888, events)
    child.stdout = FakePipe()
    chunks = iter([b"501 Z\n", b""])
    monkeypatch.setattr(pg.os, "set_blocking", lambda *args: None)
    monkeypatch.setattr(pg.os, "read", lambda *args: next(chunks))
    monkeypatch.setattr(pg.select, "select", lambda r, w, e, timeout: (r, [], []))

    def spawn(*args, **kwargs):
        assert kwargs.get("start_new_session", False) is False
        return child

    monkeypatch.setattr(pg.subprocess, "Popen", spawn)
    pg.kill_owned_group(501, cleanup_deadline=120)
    assert child.waits == 2 and child.stdout.closed


def test_ack_empty_publication_window_waits_for_root_write(tmp_path, monkeypatch):
    values = controls(tmp_path, monkeypatch)
    context = pg.SupervisorContext.from_environment(values)
    path = Path(values["TT_SUPERVISOR_ACKS"]) / "media.json"
    path.write_bytes(b"")
    path.chmod(0o600)
    slept = []

    def publish(seconds):
        slept.append(seconds)
        ack(values)

    monkeypatch.setattr(pg.time, "sleep", publish)
    context.register(SimpleNamespace(pid=123), "media", "audio-media", 109, threading.Event())
    assert len(slept) == 1


def test_unsupervised_managed_flow_keeps_legacy_hook_and_emits_no_receipts(monkeypatch):
    _, children, events = fake_managed(monkeypatch)
    monkeypatch.setattr(mp.SupervisorContext, "from_environment", classmethod(lambda cls: None))
    mp.run_managed([sys.executable, "-c", "pass"], environment={}, deadline=200,
                   canceled=threading.Event(), on_started=lambda pids: events.append(("old-hook", pids.process_pid)))
    assert not any(event[0] in ("register", "settled") for event in events)
    payload = next(i for i, event in enumerate(events) if event[0] == "payload")
    assert events.index(("old-hook", 502)) > payload
    assert all(child.waits for child in children)


def test_unsettled_media_never_triggers_private_scratch_deletion(tmp_path, monkeypatch):
    tmp_path.chmod(0o700)
    _, _, events = fake_managed(monkeypatch, fail_media_wait=True)
    monkeypatch.setattr(mp, "_tree", lambda *args: (set(), 0))
    monkeypatch.setattr(mp, "_sample_owned_rss", lambda *args: 1)
    monkeypatch.setattr(mp, "_remove_created", lambda *args: pytest.fail("unsettled media must retain scratch"))
    with pytest.raises(mp.ProcessFailed):
        mp.run_managed([sys.executable, "-c", "pass"], environment={}, deadline=200,
                       directory=tmp_path, limits=mp.ProcessLimits(), canceled=threading.Event())
    assert ("settled", "audio-guardian") in events



def diagnostic_stream(monkeypatch):
    """Mock native child/clock edges; retain the real bounded receive loop."""
    events = []
    clock = [100.0]
    stop = 101.0
    monkeypatch.setattr(pg.time, "monotonic", lambda: clock[0])

    def timeout(deadline, maximum):
        remaining = min(deadline, stop) - clock[0]
        if remaining <= 0:
            raise pg.SupervisorReceiptError("Supervisor deadline exceeded")
        return min(maximum, remaining)

    context = SimpleNamespace(stop=stop, timeout=timeout)
    child = FakeProcess(888, events)
    child.stdout = FakePipe()

    def kill():
        events.append(("direct-kill", child.pid))
        child.returncode = -9

    child.kill = kill

    def spawn(*args, **kwargs):
        assert kwargs.get("start_new_session", False) is False
        return child

    monkeypatch.setattr(pg.subprocess, "Popen", spawn)
    monkeypatch.setattr(pg.os, "set_blocking", lambda *args: None)
    monkeypatch.setattr(pg.os, "killpg", lambda *args: pytest.fail("diagnostic must never kill its owner's PGID"))
    monkeypatch.setattr(pg.select, "select", lambda r, w, e, timeout: (r, [], []))
    return context, child, events, clock


def test_diagnostic_oversize_refuses_on_first_sentinel_then_direct_wait_close(monkeypatch):
    context, child, events, _ = diagnostic_stream(monkeypatch)
    reads = []

    def read(fd, maximum):
        reads.append(maximum)
        return b"x" * maximum  # Never EOF: an unbounded collector would not finish.

    monkeypatch.setattr(pg.os, "read", read)
    with pytest.raises(PermissionError):
        pg._bounded_group_probe(context, context.stop)
    assert len(reads) == 65 and max(reads) == 4096
    assert reads[-1] == 1 and sum(reads) == 262145
    assert ("direct-kill", 888) in events
    assert child.waits == 1 and child.stdout.closed
    assert events.index(("direct-kill", 888)) < next(i for i, event in enumerate(events) if event[0] == "wait")


def test_diagnostic_trickle_does_not_refresh_receive_deadline_per_chunk(monkeypatch):
    context, child, events, clock = diagnostic_stream(monkeypatch)
    reads = []
    select_budgets = []

    def select_ready(r, w, e, timeout):
        select_budgets.append(timeout)
        return r, [], []

    def read(fd, maximum):
        reads.append(maximum)
        clock[0] += 0.025
        return b"x"  # Always ready, but never EOF before the original0.1s ends.

    monkeypatch.setattr(pg.select, "select", select_ready)
    monkeypatch.setattr(pg.os, "read", read)
    with pytest.raises(PermissionError):
        pg._bounded_group_probe(context, context.stop)
    assert 1 <= len(reads) <= 5 and clock[0] < context.stop
    assert all(later < earlier for earlier, later in zip(select_budgets, select_budgets[1:]))
    assert ("direct-kill", 888) in events and child.waits == 1 and child.stdout.closed
    assert all(event[2] <= context.stop - clock[0] for event in events if event[0] == "wait")


def test_diagnostic_read_error_still_directly_kills_actually_waits_and_closes(monkeypatch):
    context, child, events, _ = diagnostic_stream(monkeypatch)

    def fail_read(*args):
        raise OSError("controlled diagnostic failure")

    monkeypatch.setattr(pg.os, "read", fail_read)
    with pytest.raises(OSError):
        pg._bounded_group_probe(context, context.stop)
    assert ("direct-kill", 888) in events and child.waits == 1 and child.stdout.closed
