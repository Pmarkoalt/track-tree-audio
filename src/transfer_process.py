"""One managed HTTP operation. Private descriptor in, bounded safe result out.

Only audio_transfer creates descriptors after shared destination/signature policy.
This process receives no service signing secret or inherited provider credentials.
It runs under the managed-process guardian; blocking I/O cannot outlive its budget.
"""

import hashlib
import json
import math
import os
from pathlib import Path
import stat
import sys
import time

import requests

MAX_RESPONSE_BYTES = 65536
MAX_MEDIA_BYTES = 64 * 1024 * 1024


class TransferFault(Exception):
    def __init__(self, code: str, status_code: int | None = None):
        self.code = code
        self.status_code = status_code


def _active(spec):
    if time.monotonic() >= spec["deadline"]:
        raise TransferFault("deadline")


def _read_response(response, maximum: int, spec) -> bytes:
    result = bytearray()
    for chunk in response.iter_content(65536):
        _active(spec)
        if len(result) + len(chunk) > maximum:
            raise TransferFault("response_limit")
        result.extend(chunk)
    return bytes(result)


def _response_status(response):
    if 300 <= response.status_code <= 399:
        raise TransferFault("redirect_denied", response.status_code)


def _open_allocation(spec, *, writing: bool):
    flags = (os.O_WRONLY if writing else os.O_RDONLY) | os.O_NOFOLLOW
    descriptor = os.open(spec["path"], flags)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
        or metadata.st_uid != os.getuid()
        or {"device": metadata.st_dev, "inode": metadata.st_ino} != spec["fileIdentity"]
        or (writing and metadata.st_size != 0)
        or (not writing and not 0 < metadata.st_size <= spec["maxBytes"])
    ):
        os.close(descriptor)
        raise TransferFault("invalid_allocation")
    return os.fdopen(descriptor, "wb" if writing else "rb")


class _UploadReader:
    def __init__(self, stream, spec):
        self.stream = stream
        self.spec = spec
        metadata = os.fstat(stream.fileno())
        self.length = metadata.st_size
        self.snapshot = (metadata.st_dev, metadata.st_ino, metadata.st_size,
                         metadata.st_mtime_ns, metadata.st_ctime_ns)
        self.count = 0
        self.digest = hashlib.sha256()

    def __len__(self):
        return self.length

    def read(self, amount=-1):
        _active(self.spec)
        block = self.stream.read(65536 if amount < 0 else min(65536, amount))
        if self.count + len(block) > self.spec["maxBytes"]:
            raise TransferFault("input_limit")
        self.count += len(block)
        self.digest.update(block)
        return block


def execute_transfer(spec, session):
    """Execute with an explicit session; fake sessions provide offline unit proof."""
    _active(spec)
    session.trust_env = False
    options = {"headers": spec["headers"], "allow_redirects": False,
               "stream": True, "timeout": (5, 5)}
    operation = spec["operation"]
    if operation == "download":
        with session.get(spec["url"], **options) as response:
            _response_status(response)
            if response.status_code != 200:
                raise TransferFault("http_rejected", response.status_code)
            count = 0
            digest = hashlib.sha256()
            with _open_allocation(spec, writing=True) as output:
                for block in response.iter_content(65536):
                    _active(spec)
                    if count + len(block) > spec["maxBytes"]:
                        raise TransferFault("input_limit")
                    output.write(block)
                    count += len(block)
                    digest.update(block)
            if not count:
                raise TransferFault("input_limit")
            return {"ok": True, "statusCode": 200, "sizeBytes": count,
                    "checksumSha256": digest.hexdigest()}, b""
    if operation == "upload":
        with _open_allocation(spec, writing=False) as source:
            reader = _UploadReader(source, spec)
            with session.put(spec["url"], data=reader, **options) as response:
                _response_status(response)
                if response.status_code not in (200, 201, 204):
                    raise TransferFault("http_rejected", response.status_code)
                body = _read_response(response, MAX_RESPONSE_BYTES, spec)
            metadata = os.fstat(source.fileno())
            snapshot = (metadata.st_dev, metadata.st_ino, metadata.st_size,
                        metadata.st_mtime_ns, metadata.st_ctime_ns)
            if reader.count != reader.length or snapshot != reader.snapshot:
                raise TransferFault("source_changed")
            if spec.get("expectedChecksumSha256", reader.digest.hexdigest()) != reader.digest.hexdigest():
                raise TransferFault("source_changed")
            return {"ok": True, "statusCode": response.status_code, "sizeBytes": reader.count,
                    "checksumSha256": reader.digest.hexdigest()}, body
    if operation == "control":
        body = bytes.fromhex(spec["rawBodyHex"])
        if len(body) > MAX_RESPONSE_BYTES:
            raise TransferFault("body_limit")
        with session.post(spec["url"], data=body, **options) as response:
            _response_status(response)
            body = _read_response(response, MAX_RESPONSE_BYTES, spec)
            return {"ok": True, "statusCode": response.status_code, "sizeBytes": len(body)}, body
    raise TransferFault("invalid_allocation")


def _validate_spec(spec):
    if not isinstance(spec, dict) or spec.get("operation") not in ("download", "upload", "control"):
        raise TransferFault("invalid_allocation")
    control = spec["operation"] == "control"
    required = {"operation", "url", "headers", "maxBytes", "deadline"}
    required |= {"rawBodyHex"} if control else {"path", "fileIdentity"}
    if spec["operation"] == "upload" and "expectedChecksumSha256" in spec:
        required.add("expectedChecksumSha256")
        digest = spec["expectedChecksumSha256"]
        if not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise TransferFault("invalid_allocation")
    if set(spec) != required:
        raise TransferFault("invalid_allocation")
    ceiling = MAX_RESPONSE_BYTES if control else MAX_MEDIA_BYTES
    if type(spec["maxBytes"]) is not int or not 0 < spec["maxBytes"] <= ceiling:
        raise TransferFault("invalid_limit")
    if type(spec["deadline"]) not in (float, int) or not math.isfinite(spec["deadline"]):
        raise TransferFault("invalid_limit")
    if spec["deadline"] > time.monotonic() + (10 if control else 30):
        raise TransferFault("invalid_limit")
    if not isinstance(spec["url"], str) or len(spec["url"]) > 16384:
        raise TransferFault("invalid_allocation")
    if not isinstance(spec["headers"], dict) or len(spec["headers"]) > 8:
        raise TransferFault("invalid_allocation")
    for key, value in spec["headers"].items():
        if (
            not isinstance(key, str) or not isinstance(value, str)
            or len(key) > 100 or len(value) > 16384
            or "\r" in key + value or "\n" in key + value
        ):
            raise TransferFault("invalid_allocation")
    if control:
        if not isinstance(spec["rawBodyHex"], str) or len(spec["rawBodyHex"]) > MAX_RESPONSE_BYTES * 2:
            raise TransferFault("body_limit")
        try:
            if len(bytes.fromhex(spec["rawBodyHex"])) > MAX_RESPONSE_BYTES:
                raise ValueError()
        except ValueError:
            raise TransferFault("body_limit") from None


def main():
    os.umask(0o077)
    specification = Path(sys.argv[1])
    directory = specification.parent
    result = {"ok": False, "code": "transfer_failed"}
    response_body = b""
    try:
        metadata = specification.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > 262144
        ):
            raise TransferFault("invalid_allocation")
        spec = json.loads(specification.read_bytes())
        # The request descriptor is a private trusted capability, not a wire DTO.
        _validate_spec(spec)
        with requests.Session() as session:
            result, response_body = execute_transfer(spec, session)
    except TransferFault as error:
        result = {"ok": False, "code": error.code, "statusCode": error.status_code}
    except BaseException:
        # Never serialize raw requests/provider errors, header values or URLs.
        result = {"ok": False, "code": "transfer_failed"}
    for name, content in (("response.bin", response_body), ("result.json", json.dumps(result, separators=(",", ":")).encode())):
        with (directory / name).open("xb") as stream:
            stream.write(content)
    # Completing the bounded transport report is a successful child execution.
    # HTTP/protocol failure is carried by the fixed safe result. A nonzero exit
    # would make the supervisor remove those newly-created report files before
    # the parent could classify an HTTP rejection (for example an expired grant).
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
