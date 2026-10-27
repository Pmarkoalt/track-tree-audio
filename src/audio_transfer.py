"""Finite private HTTP transfers; destination and signing policy stays shared.

Capabilities travel through an exact-owned private file, never subprocess argv.
The managed child bounds blocking requests I/O, including stalled writes/reads.
Only trusted Python callers may supply a control destination validator.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
import json
import math
import os
from pathlib import Path
import stat
import sys
import tempfile
import threading
import time

from .security import (
    get_webhook_headers,
    is_media_url_allowed,
    is_webhook_url_allowed,
)
from .env import settings

MAX_MEDIA_BYTES = 64 * 1024 * 1024
MAX_CONTROL_BYTES = 65536
MEDIA_SECONDS = 30.0
CONTROL_SECONDS = 10.0


class TransferError(Exception):
    """Safe finite failure code; no provider text, URL, header or path."""

    def __init__(self, code: str = "transfer_failed", *, status_code: int | None = None):
        if code not in _ERROR_CODES:
            code = "transfer_failed"
        super().__init__(code)
        self.code = code
        self.status_code = status_code


class TransferCanceled(TransferError):
    def __init__(self):
        super().__init__("canceled")


class TransferDeadline(TransferError, TimeoutError):
    def __init__(self):
        super().__init__("deadline")


_ERROR_CODES = frozenset({
    "transfer_failed", "canceled", "deadline", "invalid_allocation",
    "invalid_limit", "destination_denied", "body_limit", "input_limit",
    "response_limit", "redirect_denied", "http_rejected", "source_changed",
    "invalid_result", "signing_unavailable",
})


@dataclass(frozen=True)
class TransferContext:
    work_directory: Path
    deadline_monotonic: float
    canceled: threading.Event = field(repr=False)
    max_process_rss_bytes: int = 512 * 1024 * 1024


@dataclass(frozen=True)
class TransferResult:
    status_code: int
    body: bytes = field(default=b"", repr=False)
    size_bytes: int = 0
    checksum_sha256: str | None = None


def _limit(value: int, ceiling: int) -> int:
    if type(value) is not int or not 0 < value <= ceiling:
        raise TransferError("invalid_limit")
    return value


def _context(context: TransferContext, seconds: float) -> tuple[Path, float]:
    _limit(context.max_process_rss_bytes, 512 * 1024 * 1024)
    if (
        type(context.deadline_monotonic) not in (float, int)
        or not math.isfinite(context.deadline_monotonic)
    ):
        raise TransferError("invalid_limit")
    if context.canceled.is_set():
        raise TransferCanceled()
    deadline = min(context.deadline_monotonic, time.monotonic() + seconds)
    if deadline <= time.monotonic():
        raise TransferDeadline()
    try:
        root = Path(context.work_directory).resolve(strict=True)
        metadata = root.stat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise ValueError()
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ValueError()
    except (OSError, ValueError, TypeError):
        raise TransferError("invalid_allocation") from None
    return root, deadline


def _private_path(path: Path, root: Path, *, existing: bool, root_alias: Path | None = None) -> Path:
    try:
        # Resolve root aliases (/var on macOS), but reject symlink components below it.
        candidate = Path(path)
        if not candidate.is_absolute():
            raise ValueError()
        try:
            relative = candidate.relative_to(root)
        except ValueError:
            if root_alias is None:
                raise
            relative = candidate.relative_to(Path(root_alias).absolute())
        if not relative.parts or any(part in (".", "..") for part in relative.parts):
            raise ValueError()
        current = root
        for index, part in enumerate(relative.parts):
            current = current / part
            if current.is_symlink():
                raise ValueError()
            if index != len(relative.parts) - 1 and not current.is_dir():
                raise ValueError()
        if existing:
            metadata = current.stat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ValueError()
            if metadata.st_uid != os.getuid():
                raise ValueError()
        elif current.exists():
            raise ValueError()
        return current
    except (OSError, ValueError, TypeError):
        raise TransferError("invalid_allocation") from None


def _identity(path: Path) -> dict[str, int]:
    value = path.stat(follow_symlinks=False)
    return {"device": value.st_dev, "inode": value.st_ino}


def _remove_owned(path: Path, identity: dict[str, int]):
    try:
        if not path.is_symlink() and _identity(path) == identity:
            path.unlink()
    except FileNotFoundError:
        pass


def _allowed(url: str, validator: Callable[[str], bool]):
    try:
        valid = isinstance(url, str) and len(url) <= 16384 and validator(url)
    except Exception:
        valid = False
    if not valid:
        raise TransferError("destination_denied") from None


def _safe_errors(function):
    @wraps(function)
    def safe(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except TransferError:
            raise
        except Exception:
            raise TransferError("transfer_failed") from None
    return safe


def _run_child(spec_path: Path, *, directory: Path, context: TransferContext, deadline: float):
    # Imported lazily: lightweight policy/unit inspection does not start a process.
    from .managed_process import ProcessLimits, run_managed

    run_managed(
        [sys.executable, "-m", "src.transfer_process", str(spec_path)],
        # Account for all job scratch, including the media allocation outside
        # the small control-descriptor subdirectory.
        directory=Path(context.work_directory).resolve(strict=True),
        environment={
            "PATH": os.defpath,
            "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            "PYTHONNOUSERSITE": "1",
        },
        deadline=deadline,
        canceled=context.canceled,
        limits=ProcessLimits(max_rss_bytes=context.max_process_rss_bytes),
    )


def _execute(spec: dict, *, root: Path, context: TransferContext, deadline: float) -> TransferResult:
    with tempfile.TemporaryDirectory(prefix="track-tree-transfer-", dir=root) as value:
        directory = Path(value)
        os.chmod(directory, 0o700)
        specification = directory / "request.json"
        with specification.open("x", encoding="utf-8") as output:
            os.chmod(specification, 0o600)
            json.dump(spec, output, ensure_ascii=False, separators=(",", ":"))
        failure = None
        try:
            _run_child(specification, directory=directory, context=context, deadline=deadline)
        except Exception as error:
            # Always classify using safe fixed text; no child/provider diagnostics leak.
            failure = error
        if context.canceled.is_set():
            raise TransferCanceled() from None
        if isinstance(failure, TimeoutError) or time.monotonic() >= deadline:
            raise TransferDeadline() from None
        try:
            result_path = directory / "result.json"
            if result_path.is_symlink() or result_path.stat().st_size > 4096:
                raise ValueError()
            result = json.loads(result_path.read_bytes())
            if not isinstance(result, dict) or type(result.get("ok")) is not bool:
                raise ValueError()
            if not result["ok"]:
                code = result.get("code")
                status_code = result.get("statusCode")
                if code not in _ERROR_CODES:
                    raise ValueError()
                if status_code is not None and (type(status_code) is not int or not 100 <= status_code <= 599):
                    raise ValueError()
                raise TransferError(code, status_code=status_code)
            if failure is not None:
                raise ValueError()
            status_code = result["statusCode"]
            size_bytes = result["sizeBytes"]
            digest = result.get("checksumSha256")
            if type(status_code) is not int or not 100 <= status_code <= 599:
                raise ValueError()
            if type(size_bytes) is not int or not 0 <= size_bytes <= spec["maxBytes"]:
                raise ValueError()
            if digest is not None and (
                not isinstance(digest, str) or len(digest) != 64
                or any(char not in "0123456789abcdef" for char in digest)
            ):
                raise ValueError()
            body_path = directory / "response.bin"
            if body_path.is_symlink() or body_path.stat().st_size > MAX_CONTROL_BYTES:
                raise ValueError()
            body = body_path.read_bytes()
            if spec["operation"] == "control":
                if size_bytes != len(body) or digest is not None or 300 <= status_code <= 399:
                    raise ValueError()
            elif not size_bytes or digest is None:
                raise ValueError()
            return TransferResult(status_code, body, size_bytes, digest)
        except TransferError:
            raise
        except Exception:
            raise TransferError("transfer_failed" if failure is not None else "invalid_result") from None


@_safe_errors
def download_media(url: str, destination: Path, *, context: TransferContext, max_bytes: int = MAX_MEDIA_BYTES) -> TransferResult:
    maximum = _limit(max_bytes, MAX_MEDIA_BYTES)
    root, deadline = _context(context, MEDIA_SECONDS)
    _allowed(url, is_media_url_allowed)
    target = _private_path(destination, root, existing=False, root_alias=context.work_directory)
    identity = None
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(descriptor)
        identity = _identity(target)
        result = _execute({"operation": "download", "url": url, "path": str(target),
                           "fileIdentity": identity, "headers": {"Accept-Encoding": "identity"},
                           "maxBytes": maximum, "deadline": deadline},
                          root=root, context=context, deadline=deadline)
        if _identity(target) != identity or target.stat().st_size != result.size_bytes or not result.size_bytes:
            raise TransferError("invalid_result")
        return result
    except TransferError:
        raise
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        raise TransferError("transfer_failed") from None
    finally:
        if identity is not None and sys.exc_info()[0] is not None:
            _remove_owned(target, identity)


@_safe_errors
def upload_media(url: str, source: Path, *, content_type: str, context: TransferContext, max_bytes: int = MAX_MEDIA_BYTES, expected_checksum_sha256: str | None = None) -> TransferResult:
    maximum = _limit(max_bytes, MAX_MEDIA_BYTES)
    root, deadline = _context(context, MEDIA_SECONDS)
    _allowed(url, is_media_url_allowed)
    path = _private_path(source, root, existing=True, root_alias=context.work_directory)
    if not isinstance(content_type, str) or not 1 <= len(content_type) <= 128 or any(ord(char) < 33 or ord(char) > 126 for char in content_type):
        raise TransferError("invalid_allocation")
    if not 0 < path.stat().st_size <= maximum:
        raise TransferError("input_limit")
    if expected_checksum_sha256 is not None and (
        not isinstance(expected_checksum_sha256, str) or len(expected_checksum_sha256) != 64
        or any(char not in "0123456789abcdef" for char in expected_checksum_sha256)
    ):
        raise TransferError("invalid_allocation")
    spec = {"operation": "upload", "url": url, "path": str(path),
            "fileIdentity": _identity(path), "headers": {"Content-Type": content_type, "Accept-Encoding": "identity"},
            "maxBytes": maximum, "deadline": deadline}
    if expected_checksum_sha256 is not None:
        spec["expectedChecksumSha256"] = expected_checksum_sha256
    result = _execute(spec, root=root, context=context, deadline=deadline)
    if expected_checksum_sha256 is not None and result.checksum_sha256 != expected_checksum_sha256:
        raise TransferError("source_changed")
    return result


@_safe_errors
def post_control(url: str, raw_body: bytes, *, context: TransferContext, destination_allowed: Callable[[str], bool] | None = None) -> TransferResult:
    if not isinstance(raw_body, bytes) or len(raw_body) > MAX_CONTROL_BYTES:
        raise TransferError("body_limit")
    root, deadline = _context(context, CONTROL_SECONDS)
    _allowed(url, destination_allowed if destination_allowed is not None else is_webhook_url_allowed)
    if not settings.demucssvc_token:
        raise TransferError("signing_unavailable")
    headers = get_webhook_headers(raw_body)
    headers["Accept-Encoding"] = "identity"
    return _execute({"operation": "control", "url": url, "rawBodyHex": raw_body.hex(),
                     "headers": headers, "maxBytes": MAX_CONTROL_BYTES, "deadline": deadline},
                    root=root, context=context, deadline=deadline)
