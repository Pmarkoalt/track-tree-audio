"""Frozen RND-1 source-rate FLAC execution and independent PCM verification.

The trusted job layer allocates local files; uploaded data never supplies paths,
executables, switches or a codec catalog. Canonical bindings own wire validation.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import threading
import time

from .env import settings
from .managed_process import ProcessCanceled, ProcessDeadline, ProcessFailed, ProcessLimit, ProcessLimits, run_managed
from .rendition_models import validate_identity, validate_model


class RenditionFailure(RuntimeError):
    def __init__(self, code: str):
        self.code = validate_model("RenditionFailureCode", code)
        super().__init__(self.code)


@dataclass(frozen=True)
class RenditionInspection:
    checksum_sha256: str
    pcm_checksum_sha256: str
    size_bytes: int
    sample_rate_hz: int
    sample_count: int
    duration_ms: int
    channels: int = 2
    bit_depth: int = 16
    mime_type: str = "audio/flac"
    codec: str = "flac"
    max_sampled_owned_rss_bytes: int = 0
    elapsed_ms: int = 0

    @property
    def metadata(self) -> dict:
        return validate_model("RenditionMetadata", {
            "mimeType": self.mime_type, "codec": self.codec,
            "sampleRateHz": self.sample_rate_hz, "channels": self.channels,
            "bitDepth": self.bit_depth, "sampleCount": self.sample_count,
            "durationMs": self.duration_ms, "sizeBytes": self.size_bytes,
        })


def _check(canceled: threading.Event, deadline: float) -> None:
    if canceled.is_set():
        raise ProcessCanceled()
    if time.monotonic() >= deadline:
        raise ProcessDeadline("Rendition processing deadline exceeded")


@contextmanager
def _readonly(path: Path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise OSError("Invalid private media allocation")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            yield source, info
    finally:
        os.close(descriptor)


def _checksum(path: Path, maximum: int, canceled: threading.Event, deadline: float) -> tuple[str, int]:
    checksum = hashlib.sha256()
    total = 0
    with _readonly(path) as (source, info):
        if not 0 < info.st_size <= maximum:
            raise ProcessLimit("Rendition file byte limit exceeded")
        while True:
            _check(canceled, deadline)
            block = source.read(65536)
            if not block:
                break
            total += len(block)
            if total > maximum:
                raise ProcessLimit("Rendition file byte limit exceeded")
            checksum.update(block)
    return checksum.hexdigest(), total


def _allocations(source: Path, destination: Path) -> tuple[Path, Path]:
    if not source.is_absolute() or not destination.is_absolute() or ".." in source.parts or ".." in destination.parts:
        raise RenditionFailure("processing_failed")
    directory = source.parent
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise RenditionFailure("processing_failed")
    directory = directory.resolve(strict=True)
    if destination.parent.resolve(strict=True) != directory:
        raise RenditionFailure("processing_failed")
    decoded = directory / ".rendition-decoded.pcm"
    if source.resolve(strict=True) == destination or source.name == decoded.name:
        raise RenditionFailure("processing_failed")
    if os.path.lexists(destination) or os.path.lexists(decoded):
        raise RenditionFailure("processing_failed")
    with _readonly(source):
        pass
    # Fixed source/output/raw decode allocation bounds the production footprint.
    # Extra existing media is not silently admitted as another codec workspace.
    pending = [directory]
    entries = 0
    while pending:
        current = pending.pop()
        for path in current.iterdir():
            entries += 1
            info = path.lstat()
            if entries > 256 or info.st_uid != os.getuid():
                raise RenditionFailure("processing_limit")
            if stat.S_ISDIR(info.st_mode):
                pending.append(path)
            elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                if path != directory / source.name and info.st_size > 65536:
                    raise RenditionFailure("processing_limit")
            else:
                raise RenditionFailure("processing_failed")
    return directory, decoded


def _runtime() -> tuple[str, str, dict]:
    paths = [Path(settings.ffmpeg_path), Path(settings.ffprobe_path)]
    if any(not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK) for path in paths):
        raise RenditionFailure("processing_failed")
    environment = {"PATH": os.pathsep.join([str(p.parent) for p in paths] + [os.defpath]), "LC_ALL": "C"}
    return str(paths[0]), str(paths[1]), environment


def encode_rendition(
    identity: dict, source: Path, destination: Path, *, deadline: float, canceled: threading.Event,
) -> RenditionInspection:
    """Produce one exact frozen FLAC result, retaining output only on success.

    All paths come from the trusted job allocator, within one private directory.
    Source bytes/decoded PCM/rate/channel/frame identity are verified before
    encoding and source bytes again afterward. A new full raw decode verifies
    the output independently; no frame trimming, gain or resampling is applied.
    """
    started = time.monotonic()
    owned = []
    success = False
    source, destination = Path(source), Path(destination)
    try:
        try:
            identity = validate_identity(identity)
        except (ValueError, TypeError, KeyError):
            raise RenditionFailure("source_integrity_failed") from None
        if not math.isfinite(deadline):
            raise RenditionFailure("processing_failed")
        policy = identity["policy"]
        remaining = datetime.fromisoformat(identity["deadlineAt"].replace("Z", "+00:00")).timestamp() - time.time()
        deadline = min(deadline, started + policy["encodeBudgetMs"] / 1000, time.monotonic() + remaining)
        _check(canceled, deadline)
        directory, decoded = _allocations(source, destination)
        owned = [destination, decoded]
        limits = ProcessLimits(
            max_file_bytes=policy["maxOutputFileBytes"],
            max_scratch_bytes=policy["maxTemporaryBytes"],
            # The job permits one main operation and one heartbeat control
            # operation concurrently. Each owns half the signed job threshold.
            max_rss_bytes=policy["sampledProcessGroupRssStopBytes"] // 2,
            max_capture_bytes=policy["maxSignedResponseBytes"],
            max_threads=policy["maxThreads"], poll_interval_seconds=policy["rssSampleIntervalMs"] / 1000,
        )
        expected = identity["source"]
        original_checksum, size = _checksum(source, policy["maxInputFileBytes"], canceled, deadline)
        if original_checksum != expected["checksumSha256"] or size != expected["sizeBytes"]:
            raise RenditionFailure("source_integrity_failed")
        ffmpeg, ffprobe, environment = _runtime()
        peaks = []

        def execute(command: list[str], *, capture=False):
            _check(canceled, deadline)
            result = run_managed(command, directory=directory, environment=environment,
                                 deadline=deadline, canceled=canceled, limits=limits, capture_stdout=capture)
            peaks.append(result.peak_group_rss_bytes)
            return result.stdout

        def probe_audio(path: Path, *, container: str, codec: str, failure: str):
            try:
                probe = json.loads(execute([
                    ffprobe, "-v", "error", "-threads", "2", "-protocol_whitelist", "file,pipe", "-f", container,
                    "-select_streams", "a:0", "-show_entries",
                    "stream=codec_name,sample_fmt,sample_rate,channels,bits_per_sample,bits_per_raw_sample",
                    "-of", "json", str(path),
                ], capture=True))
                streams = probe["streams"]
                stream = streams[0]
                depth = stream["bits_per_sample"] if codec == "pcm_s16le" else stream["bits_per_raw_sample"]
                if (len(streams) != 1 or stream["codec_name"] != codec or stream["sample_fmt"] != "s16"
                        or int(stream["sample_rate"]) != expected["sampleRateHz"]
                        or stream["channels"] != 2 or int(depth) != 16):
                    raise RenditionFailure(failure)
            except (ValueError, KeyError, TypeError, IndexError, ProcessFailed):
                raise RenditionFailure(failure) from None

        def decode_pcm(path: Path, container: str, failure: str) -> tuple[str, int]:
            try:
                execute([
                    ffmpeg, "-nostdin", "-v", "error", "-n", "-threads", "2", "-filter_threads", "2",
                    "-protocol_whitelist", "file,pipe", "-f", container, "-i", str(path), "-map", "0:a:0",
                    "-c:a", "pcm_s16le", "-threads", "2", "-f", "s16le", str(decoded),
                ])
                checksum, decoded_size = _checksum(decoded, policy["maxDecodedFileBytes"], canceled, deadline)
                if decoded_size != expected["sampleCount"] * 4 or checksum != expected["pcmChecksumSha256"]:
                    raise RenditionFailure(failure)
                return checksum, decoded_size
            except ProcessFailed:
                raise RenditionFailure(failure) from None

        # The same bounded decoder verifies canonical WAV including PCM16
        # WAVE_FORMAT_EXTENSIBLE; Python3.11 wave is not a competing parser.
        probe_audio(source, container="wav", codec="pcm_s16le", failure="source_integrity_failed")
        pcm_checksum, _ = decode_pcm(source, "wav", "source_integrity_failed")
        decoded.unlink()

        execute([
            ffmpeg, "-nostdin", "-v", "error", "-n", "-threads", "2", "-filter_threads", "2",
            "-protocol_whitelist", "file,pipe", "-f", "wav", "-i", str(source), "-map", "0:a:0",
            "-map_metadata", "-1", "-vn", "-c:a", "flac", "-sample_fmt", "s16", "-compression_level", "5",
            "-threads", "2", "-fs", str(identity["output"]["maxSizeBytes"]), "-f", "flac", str(destination),
        ])
        encoded_checksum, output_size = _checksum(destination, policy["maxOutputFileBytes"], canceled, deadline)
        if output_size > identity["output"]["maxSizeBytes"]:
            raise RenditionFailure("processing_limit")
        with _readonly(destination) as (result, _):
            if result.read(4) != b"fLaC":
                raise RenditionFailure("output_invalid")
        probe_audio(destination, container="flac", codec="flac", failure="output_invalid")
        decoded_checksum, _ = decode_pcm(destination, "flac", "output_invalid")
        # The original and encoded object cannot change between validation and
        # handoff. Callback/API independently verifies its immutable object too.
        if _checksum(source, policy["maxInputFileBytes"], canceled, deadline) != (original_checksum, size):
            raise RenditionFailure("source_integrity_failed")
        if _checksum(destination, policy["maxOutputFileBytes"], canceled, deadline) != (encoded_checksum, output_size):
            raise RenditionFailure("output_invalid")
        _check(canceled, deadline)
        inspection = RenditionInspection(
            checksum_sha256=encoded_checksum, pcm_checksum_sha256=decoded_checksum, size_bytes=output_size,
            sample_rate_hz=expected["sampleRateHz"], sample_count=expected["sampleCount"], duration_ms=expected["durationMs"],
            max_sampled_owned_rss_bytes=max(peaks), elapsed_ms=int((time.monotonic() - started) * 1000),
        )
        inspection.metadata  # Enforce canonical output/refinement before success.
        success = True
        return inspection
    except (ProcessCanceled, ProcessDeadline, RenditionFailure):
        raise
    except ProcessLimit:
        raise RenditionFailure("processing_limit") from None
    except (OSError, ValueError, ProcessFailed):
        raise RenditionFailure("processing_failed") from None
    finally:
        for path in owned:
            if path == destination and success:
                continue
            try:
                path.unlink(missing_ok=True)
            except OSError:
                raise RenditionFailure("processing_failed") from None
