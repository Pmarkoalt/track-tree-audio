"""Contract-independent tiny real-process checks; no codec/runtime lease used."""

from dataclasses import replace
import os
from pathlib import Path
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


def run(script, directory, *, limits=ProcessLimits(), **options):
    return run_managed(
        [sys.executable, "-c", script], directory=directory,
        environment=options.pop("environment", {}),
        deadline=options.pop("deadline", time.monotonic() + 3),
        canceled=options.pop("canceled", threading.Event()), limits=limits,
        **options,
    )


def assert_reaped(pids):
    for pid in (pids.process_pid, pids.guardian_pid):
        with pytest.raises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_literal_argv_and_explicit_environment_do_not_use_a_shell_or_ambient_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("SYNTHETIC_AMBIENT_CAPABILITY", "private-marker")
    pids = []
    result = run_managed(
        [sys.executable, "-c", "import os,sys; print(sys.argv[1]); print(os.getenv('SYNTHETIC_AMBIENT_CAPABILITY')); print(os.getenv('OMP_NUM_THREADS'))", "$(touch escaped); `touch escaped2`"],
        directory=tmp_path, environment={}, deadline=time.monotonic() + 3,
        canceled=threading.Event(), limits=ProcessLimits(), capture_stdout=True, on_started=pids.append,
    )
    assert result.stdout.splitlines() == [b"$(touch escaped); `touch escaped2`", b"None", b"2"]
    assert not (tmp_path / "escaped").exists()
    assert not (tmp_path / "escaped2").exists()
    assert result.peak_group_rss_bytes > 0
    assert_reaped(pids[0])


def test_capture_overflow_fails_without_retaining_private_output_or_partial_files(tmp_path, capfd):
    marker = tmp_path / "source"
    marker.write_bytes(b"original")
    pids = []
    with pytest.raises(ProcessLimit, match="captured output"):
        run("from pathlib import Path; import os; Path('partial').write_bytes(b'x'); os.write(1,b'PRIVATE'*1024)",
            tmp_path, capture_stdout=True, limits=replace(ProcessLimits(), max_capture_bytes=64), on_started=pids.append)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["source"]
    assert marker.read_bytes() == b"original"
    assert "PRIVATE" not in repr(capfd.readouterr())
    assert_reaped(pids[0])


def test_real_per_file_limit_removes_only_new_output(tmp_path):
    marker = tmp_path / "source"
    marker.write_bytes(b"original")
    with pytest.raises(ProcessLimit):
        run("from pathlib import Path; Path('oversized').write_bytes(b'x'*16384)", tmp_path,
            limits=replace(ProcessLimits(), max_file_bytes=1024))
    assert marker.read_bytes() == b"original"
    assert not (tmp_path / "oversized").exists()


def test_deadline_and_cancel_have_distinct_types_and_reap_both_controlled_children(tmp_path):
    pids = []
    with pytest.raises(ProcessDeadline):
        run("import time; time.sleep(30)", tmp_path, deadline=time.monotonic() + 0.2, on_started=pids.append)
    if pids:
        assert_reaped(pids[-1])
    event = threading.Event()
    timer = threading.Timer(0.2, event.set)
    timer.start()
    try:
        with pytest.raises(ProcessCanceled):
            run("import time; time.sleep(30)", tmp_path, canceled=event, on_started=pids.append)
    finally:
        timer.cancel()
        timer.join(timeout=1)
    if pids:
        assert_reaped(pids[-1])


def test_sampler_unavailability_fails_closed_and_cleans_new_output(tmp_path):
    with patch("src.managed_process._sample_owned_rss", side_effect=ProcessLimit("Managed memory sampling is unavailable")):
        with pytest.raises(ProcessLimit, match="sampling is unavailable"):
            run("import time; time.sleep(30)", tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_hook_failure_reaps_and_never_erases_existing_source(tmp_path):
    (tmp_path / "source").write_bytes(b"original")
    pids = []

    def fail(value):
        pids.append(value)
        raise RuntimeError("controlled state-write failure")

    with pytest.raises(RuntimeError, match="controlled state-write failure"):
        run("import time; time.sleep(30)", tmp_path, on_started=fail)
    assert (tmp_path / "source").read_bytes() == b"original"
    assert_reaped(pids[0])


def test_preflight_symlink_and_precancel_do_not_spawn(tmp_path):
    (tmp_path / "link").symlink_to(tmp_path.parent)
    with patch("src.managed_process.subprocess.Popen") as spawn:
        with pytest.raises(ProcessLimit, match="allocation"):
            run("pass", tmp_path)
        spawn.assert_not_called()
    event = threading.Event()
    event.set()
    with patch("src.managed_process.subprocess.Popen") as spawn:
        with pytest.raises(ProcessCanceled):
            run("pass", tmp_path, canceled=event)
        spawn.assert_not_called()


def test_compatibility_mode_preserves_caller_threads_and_existing_cwd(tmp_path):
    # Split inference has its own qualified limits; no rendition policy leaks.
    (tmp_path / "source").write_bytes(b"original")
    result = run("import os; print(os.environ['OMP_NUM_THREADS'])", tmp_path, limits=None,
                 environment={"OMP_NUM_THREADS": "7"}, capture_stdout=True)
    assert result.stdout == b"7\n"
    assert result.peak_group_rss_bytes == 0
    assert (tmp_path / "source").read_bytes() == b"original"


def test_child_failure_is_safe_and_success_output_remains(tmp_path, capfd):
    with pytest.raises(ProcessFailed, match="^Managed process failed$"):
        run("import sys; print('private-provider-response',file=sys.stderr); sys.exit(17)", tmp_path)
    assert "private-provider-response" not in repr(capfd.readouterr())
    run("from pathlib import Path; Path('output').write_bytes(b'valid')", tmp_path)
    assert (tmp_path / "output").read_bytes() == b"valid"
