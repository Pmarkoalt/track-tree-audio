"""Legacy split compatibility API backed by the shared managed supervisor.

Rendition limits remain opt-in. Existing split callers keep their resource and
error policy, while cancellation/guardian/group cleanup has one implementation.
"""

from collections.abc import Callable, Mapping, Sequence
import math
import threading
import time

from .managed_process import (
    ProcessCanceled,
    ProcessDeadline,
    ProcessFailed,
    ProcessLimit,
    run_managed,
)


def run_supervised(
    command: Sequence[str],
    *,
    environment: Mapping[str, str],
    canceled: threading.Event,
    timeout_seconds: float,
    on_started: Callable[[int], None] | None = None,
    failed_message: str = "Audio process failed",
    timeout_message: str = "Audio process exceeded time budget",
) -> None:
    """Preserve the split API without applying rendition RSS/file/thread limits."""
    if (
        isinstance(command, (str, bytes)) or not command
        or any(not isinstance(arg, str) or "\0" in arg for arg in command)
    ):
        raise ValueError("Invalid audio process command")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("Invalid audio process time budget")
    if canceled.is_set():
        raise ProcessCanceled()
    try:
        run_managed(
            command,
            environment=environment,
            canceled=canceled,
            deadline=time.monotonic() + timeout_seconds,
            limits=None,
            on_started=(lambda pids: on_started(pids.process_pid))
            if on_started is not None else None,
        )
    except ProcessDeadline:
        raise TimeoutError(timeout_message) from None
    except (ProcessFailed, ProcessLimit, ValueError):
        raise RuntimeError(failed_message) from None
