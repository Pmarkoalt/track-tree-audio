"""Single contract-independent process foundation for trusted audio commands.

Rendition limits are opt-in; the parent can adapt legacy split supervision with
limits=None without imposing rendition memory/file/thread policy on Demucs.
This controls cooperative trusted commands, not arbitrary code in a sandbox.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import select
import signal
import stat
import subprocess
import sys
import threading
import time

from .process_guard import SupervisorContext, SupervisorReceiptError, _CONTROL_NAMES, kill_owned_group


class ProcessCanceled(Exception):
    pass


class ProcessDeadline(TimeoutError):
    pass


class ProcessLimit(RuntimeError):
    pass


class ProcessFailed(RuntimeError):
    pass


@dataclass(frozen=True)
class ProcessLimits:
    max_file_bytes: int = 64 * 1024 * 1024
    max_scratch_bytes: int = 256 * 1024 * 1024
    max_rss_bytes: int = 512 * 1024 * 1024
    max_capture_bytes: int = 65536
    max_threads: int = 2
    poll_interval_seconds: float = 0.1


@dataclass(frozen=True)
class ManagedPids:
    process_pid: int
    guardian_pid: int


@dataclass(frozen=True)
class ProcessResult:
    stdout: bytes
    returncode: int
    peak_group_rss_bytes: int
    elapsed_seconds: float


_GUARD = Path(__file__).with_name("process_guard.py")
_MAX_ENTRIES = 256
_MAX_GATE_BYTES = 65536


def _validate_limits(limits: ProcessLimits) -> None:
    for value, ceiling in (
        (limits.max_file_bytes, 64 * 1024 * 1024),
        (limits.max_scratch_bytes, 256 * 1024 * 1024),
        (limits.max_rss_bytes, 512 * 1024 * 1024),
        (limits.max_capture_bytes, 65536),
        (limits.max_threads, 2),
    ):
        if type(value) is not int or not 1 <= value <= ceiling:
            raise ValueError("Invalid managed process limit")
    if not math.isfinite(limits.poll_interval_seconds) or not 0 < limits.poll_interval_seconds <= 0.1:
        raise ValueError("Invalid managed process sampling interval")


def _tree(directory: Path, limits: ProcessLimits) -> tuple[set[Path], int]:
    """Bound traversal count/bytes; never follow symlinks or external hardlinks."""
    seen = set()
    maximum = 0
    total = 0
    pending = [directory]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                path = Path(entry.path)
                seen.add(path)
                if len(seen) > _MAX_ENTRIES:
                    raise ProcessLimit("Managed scratch entry limit exceeded")
                info = entry.stat(follow_symlinks=False)
                if info.st_uid != os.getuid():
                    raise ProcessLimit("Managed scratch allocation is invalid")
                if stat.S_ISDIR(info.st_mode):
                    pending.append(path)
                elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                    total += info.st_size
                    maximum = max(maximum, info.st_size)
                    if info.st_size > limits.max_file_bytes or total > limits.max_scratch_bytes:
                        raise ProcessLimit("Managed scratch byte limit exceeded")
                else:
                    raise ProcessLimit("Managed scratch allocation is invalid")
    return seen, maximum


def _remove_created(directory: Path, original: set[Path]) -> None:
    # Called only after the owned media group is stopped. Do not traverse links.
    paths = []
    for root, directories, files in os.walk(directory, followlinks=False):
        paths.extend(Path(root) / name for name in directories + files)
    for path in sorted(paths, key=lambda p: len(p.parts), reverse=True):
        if path in original:
            continue
        try:
            if path.is_symlink() or not path.is_dir():
                path.unlink(missing_ok=True)
            else:
                path.rmdir()
        except FileNotFoundError:
            pass


def _kill_group(process: subprocess.Popen | None, context: SupervisorContext | None = None) -> None:
    if process is not None:
        try:
            if context:
                kill_owned_group(process.pid, cleanup_deadline=context.stop)
            else:
                kill_owned_group(process.pid)
        except OSError:
            raise ProcessFailed("Managed process group could not be terminated") from None


def _sample_owned_rss(group: int, guardian: int, deadline: float) -> int:
    """Sample media group + guardian + this transient sampler, in bytes.

    This is a fail-stop observation, not an instantaneous physical memory cap.
    Only PID/group/RSS are queried; no unrelated process arguments are read.
    """
    if sys.platform not in ("darwin", "linux"):
        raise ProcessLimit("Managed memory sampling is unavailable")
    context = SupervisorContext.from_environment()
    if context is not None:
        deadline = context.work_deadline(deadline)
    process = None
    data = bytearray()
    stop = min(deadline, time.monotonic() + 0.1)
    try:
        if context:
            context.timeout(stop, 0.1)
        process = subprocess.Popen(
            ["/bin/ps", "-axo", "pid=,pgid=,rss="],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={"PATH": os.defpath, "LC_ALL": "C"}, close_fds=True, start_new_session=context is None,
        )
        assert process.stdout is not None
        os.set_blocking(process.stdout.fileno(), False)
        while True:
            if time.monotonic() >= stop:
                raise ProcessLimit("Managed memory sampling is unavailable")
            ready, _, _ = select.select([process.stdout], [], [], max(0, stop - time.monotonic()))
            if not ready:
                raise ProcessLimit("Managed memory sampling is unavailable")
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > 262144:
                raise ProcessLimit("Managed memory sampling is unavailable")
        process.wait(timeout=context.timeout(stop, 0.1) if context else max(0.001, stop - time.monotonic()))
        if process.returncode:
            raise ProcessLimit("Managed memory sampling is unavailable")
        owned = 0
        for line in data.splitlines():
            pid, pgid, rss = map(int, line.split())
            if pid in (guardian, process.pid) or pgid == group:
                owned += rss * 1024
        if owned <= 0:
            raise ProcessLimit("Managed memory sampling is unavailable")
        return owned
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise ProcessLimit("Managed memory sampling is unavailable") from None
    finally:
        if process is not None:
            try:
                if process.poll() is None:
                    if context:
                        process.kill()  # Same-PGID sampler: never kill its owner's group.
                    else:
                        _kill_group(process)
                process.wait(timeout=context.timeout(context.stop, 2) if context else 2)
            finally:
                if process.stdout is not None:
                    process.stdout.close()


def run_managed(
    command: Sequence[str],
    *,
    directory: Path | None = None,
    environment: Mapping[str, str],
    deadline: float,
    canceled: threading.Event,
    limits: ProcessLimits | None = None,
    capture_stdout: bool = False,
    on_started: Callable[[ManagedPids], None] | None = None,
) -> ProcessResult:
    """Supervise one trusted executor with an independently armed guardian.

    Owner death closes a private pipe and the guardian kills its exact media
    group. Guardian death makes the living owner kill/reap that group. Exec is
    gated until the guardian knows the group, closing the orphan spawn race.
    Successful outputs remain. Failure removes newly-created private scratch
    paths only; pre-existing inputs remain. No sandbox/escaped-group claim.
    """
    started = time.monotonic()
    context = SupervisorContext.from_environment()
    if context is not None:
        deadline = context.work_deadline(deadline)
    if context is None and any(key in environment for key in _CONTROL_NAMES):
        raise SupervisorReceiptError("Supervisor controls must come from the owning process")
    if isinstance(command, (str, bytes)) or not command or any(not isinstance(arg, str) or "\0" in arg for arg in command):
        raise ValueError("Invalid managed command")
    if not os.path.isabs(command[0]) or not os.path.isfile(command[0]) or not os.access(command[0], os.X_OK):
        raise ValueError("Invalid managed executable")
    if not math.isfinite(deadline):
        raise ValueError("Invalid managed deadline")
    if canceled.is_set():
        raise ProcessCanceled()
    if deadline <= started:
        raise ProcessDeadline("Managed process deadline exceeded")
    if any(not isinstance(k, str) or not isinstance(v, str) or not k or "\0" in k + v or "=" in k for k, v in environment.items()):
        raise ValueError("Invalid managed environment")
    environment = dict(environment)
    original = set()
    if limits is not None:
        _validate_limits(limits)
        if directory is None:
            raise ValueError("Private managed directory is required")
        directory = Path(directory)
        try:
            info = directory.lstat()
        except OSError:
            raise ValueError("Invalid private managed directory") from None
        if not directory.is_absolute() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("Invalid private managed directory")
        directory = directory.resolve(strict=True)
        original, _ = _tree(directory, limits)
        for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
            environment[name] = str(limits.max_threads)
    guardian_id = context.new_id("audio-guardian") if context else None
    media_id = context.new_id("audio-media") if context else None
    if context:
        environment = context.child_environment(environment, media_id)
    payload = json.dumps({"command": list(command), "environment": environment,
                          "maxFileBytes": limits.max_file_bytes if limits else None}).encode()
    if len(payload) > _MAX_GATE_BYTES:
        raise ValueError("Managed command descriptor is too large")
    helper_environment = {"PATH": os.defpath, "PYTHONNOUSERSITE": "1"}
    descriptors = set()
    media = guardian = None
    failed = True
    output = bytearray()
    peak = 0
    capture_limit = limits.max_capture_bytes if limits else 65536
    guardian_environment = context.child_environment(helper_environment, guardian_id) if context else helper_environment
    media_environment = context.child_environment(helper_environment, media_id) if context else helper_environment
    interval = limits.poll_interval_seconds if limits else 0.1

    def pipe():
        pair = os.pipe()
        descriptors.update(pair)
        return pair

    def close(fd):
        if fd in descriptors:
            os.close(fd)
            descriptors.remove(fd)

    def check():
        if canceled.is_set():
            raise ProcessCanceled()
        if time.monotonic() >= deadline:
            raise ProcessDeadline("Managed process deadline exceeded")
        if guardian is not None and guardian.poll() is not None:
            raise ProcessFailed("Managed process guardian stopped")

    def drain():
        if media is None or media.stdout is None:
            return
        while True:
            try:
                data = os.read(media.stdout.fileno(), min(4096, capture_limit + 1 - len(output)))
            except BlockingIOError:
                return
            if not data:
                return
            output.extend(data)
            if len(output) > capture_limit:
                raise ProcessLimit("Managed captured output limit exceeded")

    try:
        owner_read, owner_write = pipe()
        status_read, status_write = pipe()
        gate_read, gate_write = pipe()
        check()
        guardian = subprocess.Popen(
            [sys.executable, str(_GUARD), "guard", str(owner_read), str(status_write), str(deadline), str(os.getpid())],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=guardian_environment, close_fds=True, pass_fds=(owner_read, status_write), start_new_session=True,
        )
        close(owner_read)
        close(status_write)
        if context:
            context.register(guardian, guardian_id, "audio-guardian", deadline, canceled)
        media = subprocess.Popen(
            [sys.executable, str(_GUARD), "exec", str(gate_read)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE if capture_stdout else subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, env=media_environment, cwd=directory,
            close_fds=True, pass_fds=(gate_read,), start_new_session=True,
        )
        close(gate_read)
        if context:
            context.register(media, media_id, "audio-media", deadline, canceled)
        check()
        os.write(owner_write, f"ARM {media.pid}\n".encode("ascii"))
        os.set_blocking(status_read, False)
        acknowledgement = bytearray()
        handshake_deadline = min(deadline, time.monotonic() + 5)
        while acknowledgement != b"ARMED\n":
            check()
            if time.monotonic() >= handshake_deadline:
                raise ProcessFailed("Managed guardian handshake failed")
            ready, _, _ = select.select([status_read], [], [], min(interval, handshake_deadline - time.monotonic()))
            if ready:
                part = os.read(status_read, 64)
                if not part:
                    raise ProcessFailed("Managed guardian handshake failed")
                acknowledgement.extend(part)
                if not b"ARMED\n".startswith(acknowledgement):
                    raise ProcessFailed("Managed guardian handshake failed")
        os.set_blocking(gate_write, False)
        position = 0
        while position < len(payload):
            check()
            _, ready, _ = select.select([], [gate_write], [], min(interval, max(0, deadline - time.monotonic())))
            if ready:
                position += os.write(gate_write, payload[position:])
        close(gate_write)
        if media.stdout is not None:
            os.set_blocking(media.stdout.fileno(), False)
        if on_started is not None:
            on_started(ManagedPids(media.pid, guardian.pid))
        next_sample = 0.0
        while True:
            check()
            drain()
            current = time.monotonic()
            if limits is not None and current >= next_sample:
                _tree(directory, limits)
                observed = _sample_owned_rss(media.pid, guardian.pid, deadline)
                peak = max(peak, observed)
                if observed > limits.max_rss_bytes:
                    raise ProcessLimit("Managed memory limit exceeded")
                next_sample = current + interval
            check()
            code = media.poll()
            if code is not None:
                _kill_group(media, context)  # Retire descendants even if the leader exits0.
                drain()
                if limits is not None:
                    _, maximum = _tree(directory, limits)
                    if code == -signal.SIGXFSZ or (code != 0 and maximum >= limits.max_file_bytes):
                        raise ProcessLimit("Managed output file limit exceeded")
                if code:
                    raise ProcessFailed("Managed process failed")
                break
            wait = min(interval, max(0, deadline - time.monotonic()))
            select.select([media.stdout] if media.stdout is not None else [], [], [], wait)
        media.wait(timeout=context.timeout(deadline, 2) if context else 2)
        os.write(owner_write, b"DONE\n")
        close(owner_write)
        guardian.wait(timeout=context.timeout(deadline, 2) if context else 2)
        if guardian.returncode:
            raise ProcessFailed("Managed process guardian stopped")
        check_cancel = canceled.is_set() or time.monotonic() >= deadline
        if check_cancel:
            if canceled.is_set():
                raise ProcessCanceled()
            raise ProcessDeadline("Managed process deadline exceeded")
        failed = False
        return ProcessResult(bytes(output), 0, peak, time.monotonic() - started)
    except SupervisorReceiptError:
        if canceled.is_set():
            raise ProcessCanceled() from None
        if time.monotonic() >= deadline:
            raise ProcessDeadline("Managed process deadline exceeded") from None
        raise ProcessFailed("Managed supervisor receipt failed") from None
    except ProcessDeadline:
        raise
    except ProcessLimit:
        if canceled.is_set():
            raise ProcessCanceled() from None
        if time.monotonic() >= deadline:
            raise ProcessDeadline("Managed process deadline exceeded") from None
        raise
    except (OSError, subprocess.SubprocessError):
        if time.monotonic() >= deadline:
            raise ProcessDeadline("Managed process deadline exceeded") from None
        raise ProcessFailed("Managed process failed") from None
    finally:
        if context is None:
            # Closing owner-liveness is redundant protection if local cleanup fails.
            for fd in list(descriptors):
                close(fd)
            _kill_group(media)
            if media is not None:
                try:
                    media.wait(timeout=2)
                finally:
                    if media.stdout is not None:
                        media.stdout.close()
            if guardian is not None:
                if guardian.poll() is None:
                    try:
                        guardian.wait(timeout=0.5)
                    except subprocess.TimeoutExpired:
                        _kill_group(guardian)
                guardian.wait(timeout=2)
            if failed and limits is not None and directory is not None:
                try:
                    _remove_created(directory, original)
                except OSError:
                    raise ProcessFailed("Managed scratch cleanup failed") from None
        else:
            # Every cleanup path attempts both actual waits, even if one child fails.
            # Root end is a producer-stop cutoff reserving outer reap/report time.
            cleanup_failed = False
            for fd in list(descriptors):
                try:
                    close(fd)
                except OSError:
                    cleanup_failed = True
            for child, identity in ((media, media_id), (guardian, guardian_id)):
                if child is None:
                    continue
                waited = False
                try:
                    if child is guardian:
                        if child.poll() is None:
                            try:
                                child.wait(timeout=context.timeout(context.stop, 0.5) if context else 0.5)
                            except subprocess.TimeoutExpired:
                                _kill_group(child, context)
                    else:
                        _kill_group(child, context)
                    child.wait(timeout=context.timeout(context.stop, 2) if context else 2)
                    waited = True
                except (OSError, subprocess.SubprocessError, ProcessFailed, SupervisorReceiptError):
                    cleanup_failed = True
                finally:
                    if child.stdout is not None:
                        try:
                            child.stdout.close()
                        except OSError:
                            cleanup_failed = True
                            waited = False
                if waited and context:
                    try:
                        context.settled(child, identity)
                    except (OSError, SupervisorReceiptError):
                        cleanup_failed = True
            if failed and limits is not None and directory is not None and not cleanup_failed:
                try:
                    if context:
                        context.timeout(context.stop, 0.1)
                    _remove_created(directory, original)
                except (OSError, SupervisorReceiptError):
                    cleanup_failed = True
            if cleanup_failed:
                raise ProcessFailed("Managed process cleanup or receipts did not settle") from None
