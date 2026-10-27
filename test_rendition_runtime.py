"""Real tiny-child supervision checks; these do not qualify a rendition codec."""

import os
import sys
import threading
import time
from unittest.mock import patch

import pytest

from src.process_runtime import ProcessCanceled, run_supervised


def child(script, **options):
    return run_supervised(
        [sys.executable, "-c", script],
        environment={},
        canceled=options.pop("canceled", threading.Event()),
        timeout_seconds=options.pop("timeout_seconds", 3),
        **options,
    )


def assert_reaped(pid):
    with pytest.raises(ChildProcessError):
        os.waitpid(pid, os.WNOHANG)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_successful_child_is_reaped():
    pids = []
    assert child("pass", on_started=pids.append) is None
    assert len(pids) == 1
    assert_reaped(pids[0])


def test_failed_child_is_reaped_without_disclosing_its_output(capfd):
    pids = []
    with pytest.raises(RuntimeError, match="^Audio process failed$"):
        child(
            "import sys; print('synthetic-private-output'); sys.exit(17)",
            on_started=pids.append,
        )
    assert_reaped(pids[0])
    assert "synthetic-private-output" not in repr(capfd.readouterr())


def test_real_timeout_terminates_and_reaps_child():
    pids = []
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="^Audio process exceeded time budget$"):
        child("import time; time.sleep(60)", timeout_seconds=0.1, on_started=pids.append)
    assert time.monotonic() - started < 3
    assert_reaped(pids[0])


def test_real_midflight_cancellation_terminates_and_reaps_child():
    pids = []
    canceled = threading.Event()
    timer = threading.Timer(0.1, canceled.set)
    timer.start()
    try:
        with pytest.raises(ProcessCanceled):
            child(
                "import time; time.sleep(60)",
                canceled=canceled,
                on_started=pids.append,
            )
    finally:
        timer.cancel()
        timer.join(timeout=1)
    assert_reaped(pids[0])


def test_state_record_failure_after_spawn_still_reaps_child():
    pids = []

    def record(pid):
        pids.append(pid)
        raise RuntimeError("controlled state failure")

    with pytest.raises(RuntimeError, match="^controlled state failure$"):
        child("import time; time.sleep(60)", on_started=record)
    assert_reaped(pids[0])


def test_parent_interruption_during_started_hook_still_reaps_child():
    pids = []

    def interrupt(pid):
        pids.append(pid)
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        child("import time; time.sleep(60)", on_started=interrupt)
    assert_reaped(pids[0])


def test_pre_canceled_work_never_spawns_child():
    canceled = threading.Event()
    canceled.set()
    with patch("src.managed_process.subprocess.Popen") as spawn:
        with pytest.raises(ProcessCanceled):
            child("raise AssertionError('must not execute')", canceled=canceled)
    spawn.assert_not_called()


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("nan")])
def test_invalid_deadline_never_spawns_child(budget):
    with patch("src.managed_process.subprocess.Popen") as spawn:
        with pytest.raises(ValueError, match="Invalid audio process time budget"):
            child("pass", timeout_seconds=budget)
    spawn.assert_not_called()


def test_child_output_is_discarded_instead_of_captured(capfd):
    # Four MiB over two output streams must not be retained by the supervisor.
    child(
        "import os; data=b'x'*8192; "
        "[(os.write(1,data),os.write(2,data)) for _ in range(256)]"
    )
    captured = capfd.readouterr()
    assert captured.out == captured.err == ""

