"""Independent real tiny-process faults for the approved managed-runtime API.

These tests measure owned local processes. They establish neither media quality
nor an instantaneous physical memory cap, sandbox, network or GPU qualification.
"""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import patch

import pytest

from src.managed_process import (
    ProcessCanceled,
    ProcessDeadline,
    ProcessFailed,
    ProcessLimit,
    ProcessLimits,
    run_managed,
)


def _live(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    status = subprocess.run(
        ["/bin/ps", "-o", "stat=", "-p", str(pid)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=1,
        check=False,
    ).stdout.strip()
    return bool(status) and not status.startswith(b"Z")


def _until(predicate, seconds=3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    assert predicate(), "Owned process did not reach the expected bounded state"


def _call(tmp_path, script, **options):
    return run_managed(
        [sys.executable, "-c", script],
        directory=tmp_path,
        environment=options.pop("environment", {}),
        deadline=options.pop("deadline", time.monotonic() + 4),
        canceled=options.pop("canceled", threading.Event()),
        limits=options.pop("limits", ProcessLimits(poll_interval_seconds=0.025)),
        **options,
    )


def _kill_exact_group(pid):
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        # Darwin may report EPERM for an already-retired zombie-only group.
        # Cleanup must still fail for a group with any live owned member.
        states = subprocess.run(
            ["/bin/ps", "-axo", "pgid=,stat="],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=1,
            check=True,
        ).stdout.splitlines()
        if any(
            len(fields := line.split()) == 2
            and int(fields[0]) == pid
            and not fields[1].startswith(b"Z")
            for line in states
        ):
            raise


def _owner(tmp_path):
    ready = tmp_path / "owner-ready.json"
    descendant = tmp_path / "descendant.pid"
    child_code = (
        "import subprocess,sys,time; from pathlib import Path; "
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); "
        f"Path({str(descendant)!r}).write_text(str(child.pid)); time.sleep(30)"
    )
    script = (
        "import json,sys,threading,time; from pathlib import Path; "
        "from src.managed_process import run_managed,ProcessLimits; "
        "run_managed([sys.executable,'-c',"
        + repr(child_code)
        + "],directory=Path("
        + repr(str(tmp_path))
        + "),environment={},deadline=time.monotonic()+10,canceled=threading.Event(),"
        "limits=ProcessLimits(poll_interval_seconds=0.025),"
        "on_started=lambda p:Path("
        + repr(str(ready))
        + ").write_text(json.dumps({'process':p.process_pid,'guardian':p.guardian_pid})))"
    )
    owner = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=Path(__file__).parent,
        env={"PYTHONPATH": str(Path(__file__).parent), "PATH": os.defpath},
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        _until(lambda: ready.exists() and descendant.exists())
        pids = json.loads(ready.read_text())
        pids["descendant"] = int(descendant.read_text())
        assert all(_live(pid) for pid in pids.values())
        return owner, pids
    except BaseException:
        owner.kill()
        owner.wait(timeout=3)
        if ready.exists():
            pids = json.loads(ready.read_text())
            _kill_exact_group(pids["process"])
            try:
                os.kill(pids["guardian"], signal.SIGKILL)
            except ProcessLookupError:
                pass
        raise


def _retire(owner, pids):
    if owner.poll() is None:
        owner.kill()
    owner.wait(timeout=3)
    _kill_exact_group(pids["process"])
    try:
        os.kill(pids["guardian"], signal.SIGKILL)
    except ProcessLookupError:
        pass


def test_owner_death_stops_actual_media_group_and_descendant(tmp_path):
    owner, pids = _owner(tmp_path)
    try:
        owner.kill()
        owner.wait(timeout=3)
        _until(lambda: all(not _live(pid) for pid in pids.values()))
    finally:
        _retire(owner, pids)


def test_guard_death_is_detected_and_stops_media_group(tmp_path):
    owner, pids = _owner(tmp_path)
    try:
        os.kill(pids["guardian"], signal.SIGKILL)
        assert owner.wait(timeout=3) != 0
        _until(lambda: all(not _live(pid) for pid in pids.values()))
    finally:
        _retire(owner, pids)


def test_successful_leader_exit_cannot_leave_running_descendant(tmp_path):
    descendant_file = tmp_path / "descendant.pid"
    observed = []
    try:
        result = _call(
            tmp_path,
            "import subprocess,sys;from pathlib import Path;"
            "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
            f"Path({str(descendant_file)!r}).write_text(str(p.pid))",
            on_started=observed.append,
        )
        assert result.returncode == 0
        assert descendant_file.exists()
        descendant = int(descendant_file.read_text())
        _until(lambda: not _live(descendant))
    finally:
        if observed:
            _kill_exact_group(observed[0].process_pid)


def test_deadline_stops_real_media_child_and_guard(tmp_path):
    observed = []
    with pytest.raises(ProcessDeadline):
        _call(
            tmp_path,
            "import time;time.sleep(30)",
            deadline=time.monotonic() + 0.2,
            on_started=observed.append,
        )
    assert len(observed) == 1
    assert not _live(observed[0].process_pid)
    assert not _live(observed[0].guardian_pid)


def test_cancel_stops_actual_group_and_reaps_direct_children(tmp_path):
    canceled = threading.Event()
    observed = []
    timer = threading.Timer(0.2, canceled.set)
    timer.start()
    try:
        with pytest.raises(ProcessCanceled):
            _call(tmp_path, "import time;time.sleep(30)", canceled=canceled, on_started=observed.append)
    finally:
        timer.cancel()
        timer.join(timeout=1)
    assert len(observed) == 1
    for pid in (observed[0].process_pid, observed[0].guardian_pid):
        assert not _live(pid)
        with pytest.raises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)


def test_finite_capture_limit_fails_instead_of_retaining_child_output(tmp_path):
    with pytest.raises(ProcessLimit):
        _call(
            tmp_path,
            "import os;os.write(1,b'x'*8192)",
            limits=ProcessLimits(max_capture_bytes=1024, poll_interval_seconds=0.025),
            capture_stdout=True,
        )


def test_per_file_kernel_limit_preserves_original_and_bounds_output(tmp_path):
    original = tmp_path / "original"
    original.write_bytes(b"unchanged-original")
    output = tmp_path / "partial"
    with pytest.raises(ProcessLimit):
        _call(
            tmp_path,
            "import os,signal;signal.signal(signal.SIGXFSZ,signal.SIG_DFL); "
            f"f=open({str(output)!r},'wb',buffering=0); "
            "[(f.write(b'x'*4096)) for _ in range(20)]",
            limits=ProcessLimits(max_file_bytes=4096, poll_interval_seconds=0.025),
        )
    assert original.read_bytes() == b"unchanged-original"
    assert not output.exists()


def test_sampled_scratch_limit_removes_only_new_outputs(tmp_path):
    original = tmp_path / "original"
    original.write_bytes(b"preserved")
    with pytest.raises(ProcessLimit):
        _call(
            tmp_path,
            "from pathlib import Path;import time;"
            "Path('first').write_bytes(b'a'*3000);"
            "Path('second').write_bytes(b'b'*3000);time.sleep(30)",
            limits=ProcessLimits(
                max_file_bytes=4096, max_scratch_bytes=4096, poll_interval_seconds=0.025
            ),
        )
    assert original.read_bytes() == b"preserved"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["original"]


def test_sampled_group_rss_threshold_fails_closed(tmp_path):
    with pytest.raises(ProcessLimit):
        _call(
            tmp_path,
            "import time;data=bytearray(8*1024*1024);time.sleep(30)",
            limits=ProcessLimits(max_rss_bytes=1024 * 1024, poll_interval_seconds=0.025),
        )


def test_managed_child_cannot_read_inherited_parent_fd_or_secret(tmp_path, monkeypatch):
    descriptor = os.open(tmp_path / "private-parent-fd", os.O_CREAT | os.O_RDWR, 0o600)
    os.set_inheritable(descriptor, True)
    identity = os.fstat(descriptor)
    monkeypatch.setenv("SYNTHETIC_PARENT_CAPABILITY", "private-marker")
    try:
        _call(
            tmp_path,
            "import os\nassert 'SYNTHETIC_PARENT_CAPABILITY' not in os.environ\n"
            f"try:\n value=os.fstat({descriptor})\n"
            "except OSError:\n pass\n"
            f"else:\n assert (value.st_dev,value.st_ino)!=({identity.st_dev},{identity.st_ino})\n",
        )
    finally:
        os.close(descriptor)


def test_started_hook_failure_cleans_both_direct_children(tmp_path):
    observed = []

    def reject(pids):
        observed.append(pids)
        raise RuntimeError("controlled-state-failure")

    with pytest.raises(RuntimeError, match="^controlled-state-failure$"):
        _call(tmp_path, "import time;time.sleep(30)", on_started=reject)
    assert len(observed) == 1
    assert not _live(observed[0].process_pid)
    assert not _live(observed[0].guardian_pid)


@pytest.mark.parametrize("pre_canceled", [False, True])
def test_expired_or_pre_canceled_work_never_spawns(pre_canceled, tmp_path):
    canceled = threading.Event()
    if pre_canceled:
        canceled.set()
    with patch("subprocess.Popen") as spawn:
        with pytest.raises(ProcessCanceled if pre_canceled else ProcessDeadline):
            _call(
                tmp_path,
                "raise AssertionError('must not execute')",
                canceled=canceled,
                deadline=time.monotonic() + (5 if pre_canceled else -1),
            )
    spawn.assert_not_called()
