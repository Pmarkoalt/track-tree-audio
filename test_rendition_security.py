"""Contract-independent shared-supervisor security checks, not wire proof."""

import sys
import threading
from unittest.mock import patch

import pytest

from src.process_runtime import run_supervised


def test_literal_argument_cannot_invoke_shell_substitution(tmp_path):
    marker = tmp_path / "shell-must-not-run"
    hostile_argument = f"$(touch {marker}); touch {marker}"
    run_supervised(
        [
            sys.executable,
            "-c",
            "import sys; assert sys.argv[1].startswith('$(touch ')",
            hostile_argument,
        ],
        environment={},
        canceled=threading.Event(),
        timeout_seconds=3,
    )
    assert not marker.exists()


def test_child_receives_only_explicit_environment_and_closed_stdin(monkeypatch):
    monkeypatch.setenv("SYNTHETIC_PARENT_SECRET", "must-not-leak")
    run_supervised(
        [
            sys.executable,
            "-c",
            "import os,sys; assert os.environ['SAFE_TEST_VALUE']=='allowed'; "
            "assert 'SYNTHETIC_PARENT_SECRET' not in os.environ; "
            "assert sys.stdin.read()==''",
        ],
        environment={"SAFE_TEST_VALUE": "allowed"},
        canceled=threading.Event(),
        timeout_seconds=3,
    )


@pytest.mark.parametrize("command", [[], ["invalid\0command"], [sys.executable, 3]])
def test_invalid_argv_is_rejected_before_spawn(command):
    with patch("src.managed_process.subprocess.Popen") as spawn:
        with pytest.raises(ValueError, match="^Invalid audio process command$"):
            run_supervised(
                command,
                environment={},
                canceled=threading.Event(),
                timeout_seconds=3,
            )
    spawn.assert_not_called()


def test_spawn_failure_does_not_disclose_paths_or_environment(tmp_path, capfd):
    private_path = tmp_path / "synthetic-private-executable"
    with pytest.raises(RuntimeError, match="^Audio process failed$") as captured:
        run_supervised(
            [str(private_path)],
            environment={"SYNTHETIC_CAPABILITY": "private-marker"},
            canceled=threading.Event(),
            timeout_seconds=3,
        )
    assert captured.value.__cause__ is None
    assert str(private_path) not in str(captured.value)
    assert "private-marker" not in repr(capfd.readouterr())
