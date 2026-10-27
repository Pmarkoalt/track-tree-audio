"""Private POSIX guardian and gated exec; never a public job protocol.

The owner arms the guardian with an exact process group before releasing exec.
Only pipe descriptors and a monotonic deadline appear in helper argv. Caller
commands and environment travel through a private, close-on-exec gate instead.
"""

import json
import fcntl
from pathlib import Path
import re
import stat
import uuid
import math
import os
import resource
import select
import signal
import subprocess
import sys
import time


MAX_GATE_BYTES = 65536


# Private outer-run instrumentation, never a signed audio/job protocol.
_CONTROL_NAMES = (
    "TT_SUPERVISOR_REGISTRY", "TT_SUPERVISOR_ACKS", "TT_SUPERVISOR_WORK_END",
    "TT_SUPERVISOR_TOTAL_END", "TT_SUPERVISOR_ROLE",
)
_LABEL = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")


class SupervisorReceiptError(RuntimeError):
    pass


class SupervisorContext:
    """Trusted optional receipt channel; no daemon, observer or guardian."""

    def __init__(self, values):
        self.values = dict(values)
        self.registry = Path(values[_CONTROL_NAMES[0]])
        self.acks = Path(values[_CONTROL_NAMES[1]])
        self.owner = values[_CONTROL_NAMES[4]]
        self.work = float(values[_CONTROL_NAMES[2]])
        self.stop = float(values[_CONTROL_NAMES[3]])
        self.registered = set()
        self.closed = set()
        if (not _LABEL.fullmatch(self.owner) or not math.isfinite(self.work)
                or not math.isfinite(self.stop) or not 0 < self.work < self.stop):
            raise SupervisorReceiptError("Invalid supervisor controls")
        for path in (self.registry, self.acks):
            if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
                raise SupervisorReceiptError("Invalid supervisor controls")
            for parent in (path.parent, *path.parents[1:]):
                if not stat.S_ISDIR(parent.lstat().st_mode):
                    raise SupervisorReceiptError("Invalid supervisor controls")
        if self.registry.parent != self.acks.parent:
            raise SupervisorReceiptError("Invalid supervisor controls")
        self._private(self.registry.parent.lstat(), directory=True)
        self._private(self.registry.lstat(), directory=False)
        self._private(self.acks.lstat(), directory=True)
        self.registry_identity = self.registry.stat().st_dev, self.registry.stat().st_ino
        self.acks_identity = self.acks.stat().st_dev, self.acks.stat().st_ino

    @staticmethod
    def _private(info, *, directory):
        valid = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
        expected = 0o700 if directory else 0o600
        if (not valid or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != expected
                or (not directory and info.st_nlink != 1)):
            raise SupervisorReceiptError("Invalid supervisor controls")

    @classmethod
    def from_environment(cls, environment=None):
        environment = os.environ if environment is None else environment
        present = [key for key in _CONTROL_NAMES if key in environment]
        if not present:
            return None
        if len(present) != len(_CONTROL_NAMES):
            raise SupervisorReceiptError("Partial supervisor controls")
        try:
            return cls({key: environment[key] for key in _CONTROL_NAMES})
        except (OSError, TypeError, ValueError, OverflowError):
            raise SupervisorReceiptError("Invalid supervisor controls") from None

    def work_deadline(self, deadline):
        return min(deadline, self.work)

    def timeout(self, deadline, maximum):
        remaining = min(deadline, self.stop) - time.monotonic()
        if remaining <= 0:
            raise SupervisorReceiptError("Supervisor deadline exceeded")
        return min(maximum, remaining)

    def new_id(self, role):
        return role + "-" + uuid.uuid4().hex

    def child_environment(self, environment, identity):
        return {**environment, **self.values, "TT_SUPERVISOR_ROLE": identity}

    def _append(self, event, deadline):
        value = json.dumps(event, separators=(",", ":"), ensure_ascii=True).encode("ascii") + b"\n"
        if len(value) > 4096:
            raise SupervisorReceiptError("Supervisor receipt is oversized")
        fd = os.open(self.registry, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            self._private(info, directory=False)
            if (info.st_dev, info.st_ino) != self.registry_identity:
                raise SupervisorReceiptError("Supervisor registry changed")
            while True:
                self.timeout(deadline, 0.01)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(self.timeout(deadline, 0.01))
            self.timeout(deadline, 0.01)
            named = self.registry.lstat()
            if (named.st_dev, named.st_ino) != self.registry_identity:
                raise SupervisorReceiptError("Supervisor registry changed")
            # One bounded O_APPEND write; all Python producer threads/processes
            # coordinate through the same registry inode's advisory lock.
            if os.write(fd, value) != len(value):
                raise SupervisorReceiptError("Supervisor receipt append failed")
        finally:
            os.close(fd)

    def register(self, process, identity, role, deadline, canceled):
        if not _LABEL.fullmatch(identity) or not _LABEL.fullmatch(role):
            raise SupervisorReceiptError("Invalid supervisor process label")
        end = min(deadline, self.work)
        if canceled.is_set():
            raise SupervisorReceiptError("Supervisor registration canceled")
        self.timeout(end, 0.01)
        self._append({"kind": "register", "id": identity, "owner": self.owner,
                      "role": role, "pid": process.pid}, end)
        self.registered.add(identity)
        ack = self.acks / (identity + ".json")
        while True:
            if canceled.is_set():
                raise SupervisorReceiptError("Supervisor registration canceled")
            self.timeout(end, 0.01)
            parent = self.acks.lstat()
            self._private(parent, directory=True)
            if (parent.st_dev, parent.st_ino) != self.acks_identity:
                raise SupervisorReceiptError("Supervisor ACK directory changed")
            try:
                fd = os.open(ack, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            except FileNotFoundError:
                time.sleep(self.timeout(end, 0.01))
                continue
            try:
                info = os.fstat(fd)
                self._private(info, directory=False)
                if info.st_size == 0:
                    # Root opens O_EXCL before its one bounded write; existence
                    # alone is not publication. Keep the original work budget.
                    time.sleep(self.timeout(end, 0.01))
                    continue
                if not 0 < info.st_size <= 1024:
                    raise SupervisorReceiptError("Invalid supervisor ACK")
                value = json.loads(os.read(fd, 1025))
                named = ack.lstat()
                if ((named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
                        or set(value) != {"pid", "pgid", "sec", "usec"}
                        or any(type(value[key]) is not int for key in value)
                        or value["pid"] != process.pid or value["pgid"] != process.pid
                        or value["sec"] <= 0 or not 0 <= value["usec"] < 1_000_000):
                    raise SupervisorReceiptError("Invalid supervisor ACK")
                self.timeout(end, 0.01)
                return
            except (OSError, TypeError, ValueError, KeyError):
                raise SupervisorReceiptError("Invalid supervisor ACK") from None
            finally:
                os.close(fd)

    def settled(self, process, identity):
        if identity not in self.registered:
            return
        if identity in self.closed or type(process.returncode) is not int:
            raise SupervisorReceiptError("Invalid supervisor settlement")
        if process.stdout is not None and not process.stdout.closed:
            raise SupervisorReceiptError("Supervisor child handles remain open")
        self._append({"kind": "settled", "id": identity, "owner": self.owner,
                      "waited": True, "closed": True, "returncode": process.returncode}, self.stop)
        self.closed.add(identity)


def _bounded_group_probe(context, deadline):
    """Same-PGID diagnostic child: real bounded wait/close, no new session."""
    child = None
    receive_end = min(deadline, context.stop, time.monotonic() + 0.1)
    try:
        context.timeout(receive_end, 0.1)
        child = subprocess.Popen(
            ["/bin/ps", "-axo", "pgid=,stat="], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={"PATH": os.defpath, "LC_ALL": "C"}, close_fds=True,
        )
        assert child.stdout is not None
        os.set_blocking(child.stdout.fileno(), False)
        # One original receive deadline, never a fresh allowance per chunk.
        value = bytearray()
        while True:
            if time.monotonic() >= receive_end:
                raise PermissionError("Managed process group could not be terminated")
            ready, _, _ = select.select([child.stdout], [], [], context.timeout(receive_end, 0.1))
            if not ready:
                raise PermissionError("Managed process group could not be terminated")
            try:
                # At the ceiling, read only one sentinel byte to distinguish EOF.
                chunk = os.read(child.stdout.fileno(), min(4096, 262145 - len(value)))
            except BlockingIOError:
                continue
            if not chunk:
                break
            if len(value) + len(chunk) > 262144:
                raise PermissionError("Managed process group could not be terminated")
            value.extend(chunk)
        child.wait(timeout=context.timeout(receive_end, 0.1))
        if child.returncode:
            raise PermissionError("Managed process group could not be terminated")
        return bytes(value)
    finally:
        if child is not None:
            try:
                if child.poll() is None:
                    child.kill()  # Actual direct child, never this shared PGID.
                child.wait(timeout=context.timeout(deadline, 0.1))
            finally:
                if child.stdout is not None:
                    child.stdout.close()


def kill_owned_group(pgid: int | None, *, cleanup_deadline: float | None = None) -> None:
    if pgid is not None:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            # Darwin can report EPERM for a group whose only remaining member
            # is already a zombie. Ignore only after an independent live-state
            # check, never when a live member still exists or sampling fails.
            try:
                context = SupervisorContext.from_environment()
                if context is None:
                    result = subprocess.run(
                        ["/bin/ps", "-axo", "pgid=,stat="], stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                        env={"PATH": os.defpath, "LC_ALL": "C"}, timeout=0.1, check=True,
                    )
                    output = result.stdout
                else:
                    output = _bounded_group_probe(context, min(context.stop, cleanup_deadline or context.stop))
                if len(output) > 262144:
                    raise ValueError()
                rows = [line.split() for line in output.splitlines()]
                live = any(int(group) == pgid and not status.startswith(b"Z") for group, status in rows)
                if live:
                    raise ValueError()
            except (OSError, ValueError, subprocess.SubprocessError):
                raise PermissionError("Managed process group could not be terminated") from None


def _guard(owner_fd: int, status_fd: int, deadline: float, owner_pid: int) -> int:
    group = None
    pending = bytearray()
    try:
        while True:
            if os.getppid() != owner_pid:
                return 0  # A leaked writer in an unrelated fork cannot mask loss.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return 124
            ready, _, _ = select.select([owner_fd], [], [], min(0.1, remaining))
            if not ready:
                continue
            value = os.read(owner_fd, 256)
            if not value:
                return 0
            pending.extend(value)
            if len(pending) > 128:
                return 126
            while b"\n" in pending:
                line, _, rest = pending.partition(b"\n")
                pending = bytearray(rest)
                if group is None and line.startswith(b"ARM "):
                    value = line[4:]
                    if not value.isdigit() or int(value) <= 1:
                        return 126
                    group = int(value)
                    if group == os.getpgrp():
                        return 126
                    os.write(status_fd, b"ARMED\n")
                elif group is not None and line == b"DONE":
                    return 0
                else:
                    return 126
    finally:
        try:
            context = SupervisorContext.from_environment()
            kill_owned_group(group, cleanup_deadline=context.stop if context else None)
        finally:
            os.close(owner_fd)
            os.close(status_fd)


def _exec(gate_fd: int) -> int:
    payload = bytearray()
    try:
        while True:
            chunk = os.read(gate_fd, min(4096, MAX_GATE_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
            if len(payload) > MAX_GATE_BYTES:
                return 126
    finally:
        os.close(gate_fd)
    if not payload:
        return 126  # Owner died before it armed/released the media process.
    instruction = json.loads(payload)
    if set(instruction) != {"command", "environment", "maxFileBytes"}:
        return 126
    command = instruction["command"]
    environment = instruction["environment"]
    maximum = instruction["maxFileBytes"]
    if (
        not isinstance(command, list)
        or not command
        or any(not isinstance(arg, str) or "\0" in arg for arg in command)
        or not os.path.isabs(command[0])
        or not isinstance(environment, dict)
        or any(not isinstance(k, str) or not isinstance(v, str) or "\0" in k + v or "=" in k for k, v in environment.items())
    ):
        return 126
    if maximum is not None:
        if type(maximum) is not int or maximum < 1 or maximum > 64 * 1024 * 1024:
            return 126
        _, current_hard = resource.getrlimit(resource.RLIMIT_FSIZE)
        effective = maximum if current_hard == resource.RLIM_INFINITY else min(maximum, current_hard)
        resource.setrlimit(resource.RLIMIT_FSIZE, (effective, effective))
        os.umask(0o077)
    # No owner-liveness/status descriptor is inherited by this exec process.
    os.execve(command[0], command, environment)
    return 126


def main() -> int:
    try:
        SupervisorContext.from_environment()  # Partial/invalid controls fail even in gated bootstrap.
        if len(sys.argv) == 6 and sys.argv[1] == "guard":
            deadline = float(sys.argv[4])
            if not math.isfinite(deadline):
                return 126
            return _guard(int(sys.argv[2]), int(sys.argv[3]), deadline, int(sys.argv[5]))
        if len(sys.argv) == 3 and sys.argv[1] == "exec":
            return _exec(int(sys.argv[2]))
    except BaseException:
        # Never include private commands, paths, environment or provider text.
        return 126
    return 126


if __name__ == "__main__":
    raise SystemExit(main())
