"""Independent, contract-free PCM fixture/measurement tools for RND-1.

Importing these tools does not advertise profiles, encode media or start a
runtime. The explicit benchmark CLI requires a coordinator-granted finite
runtime lease before any encoding or large-corpus use.
"""

from dataclasses import asdict, dataclass
import hashlib
import math
from pathlib import Path
import json
import os
import platform
import shutil
import struct
import subprocess
import threading
import time
import wave

import pytest


PCM_HEADER_BYTES = 44
PCM_FRAME_BYTES = 4
MAX_SOURCE_BYTES = 64 * 1024 * 1024
CHUNK_FRAMES = 4096


@dataclass(frozen=True)
class PcmMeasurement:
    frames: int
    sample_rate: int
    channels: int
    size_bytes: int
    checksum_sha256: str
    pcm_sha256: str
    sample_peak: tuple[float, float]
    rms: tuple[float, float]
    clipped_samples: tuple[int, int]
    peak_frame: tuple[int, int]

    def report(self) -> dict:
        """JSON-ready observation, with no inferred LUFS/true-peak promises."""
        return asdict(self)


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def finite_corpus() -> tuple[tuple[str, int, int], ...]:
    """Return finite benchmark name/rate/frame declarations; create no files."""
    return (
        ("short44", 44100, 22050),
        ("timing44", 44100, 264600),
        ("timing48", 48000, 288000),
        ("long44", 44100, 3969000),
        ("limit44", 44100, (MAX_SOURCE_BYTES - PCM_HEADER_BYTES) // PCM_FRAME_BYTES),
    )


def write_original_fixture(path: Path, *, sample_rate: int, frames: int) -> None:
    """Stream deterministic original stereo PCM16, retaining no whole waveform.

    Existing files and symlinks are refused.  This helper accepts a trusted
    harness allocation, not an uploaded path.  Two isolated channel markers are
    deliberately the maximum absolute samples for independent lag detection.
    """
    if sample_rate not in (44100, 48000) or not isinstance(frames, int) or frames < 1:
        raise ValueError("Invalid finite fixture declaration")
    if PCM_HEADER_BYTES + frames * PCM_FRAME_BYTES > MAX_SOURCE_BYTES:
        raise ValueError("Fixture exceeds source budget")
    left_marker = sample_rate // 10
    right_marker = sample_rate * 3 // 10
    # Exclusive open prevents accidental source replacement, including symlinks.
    with path.open("xb") as raw:
        path.chmod(0o600)
        with wave.open(raw, "wb") as target:
            target.setnchannels(2)
            target.setsampwidth(2)
            target.setframerate(sample_rate)
            target.setnframes(frames)
            for offset in range(0, frames, CHUNK_FRAMES):
                block = bytearray()
                for frame in range(offset, min(frames, offset + CHUNK_FRAMES)):
                    t = frame / sample_rate
                    envelope = 0.15 + 0.05 * math.sin(2 * math.pi * 1.7 * t)
                    left = round(32767 * envelope * math.sin(2 * math.pi * 440 * t))
                    right = round(32767 * envelope * math.sin(2 * math.pi * 997 * t))
                    if frame == left_marker:
                        left = 24576
                    if frame == right_marker:
                        right = -24576
                    block.extend(struct.pack("<hh", left, right))
                target.writeframesraw(block)


def inspect_pcm16(path: Path, *, max_bytes: int = MAX_SOURCE_BYTES, deadline: float | None = None) -> PcmMeasurement:
    """Independently inspect a complete decoded stereo PCM16 WAV in chunks.

    FFmpeg's metadata claims alone cannot establish these sample observations.
    Only actual finite integer samples establish sample peak/RMS; they do not
    establish integrated loudness, intersample true peak or perceptual quality.
    """
    size = path.stat().st_size
    if size < PCM_HEADER_BYTES or size > max_bytes:
        raise ValueError("Decoded PCM exceeds inspection budget or is empty")
    peaks = [0, 0]
    sums = [0, 0]
    clipped = [0, 0]
    peak_frame = [0, 0]
    observed = 0
    pcm_digest = hashlib.sha256()
    try:
        with wave.open(str(path), "rb") as source:
            if (
                source.getnchannels() != 2
                or source.getsampwidth() != 2
                or source.getcomptype() != "NONE"
                or source.getframerate() not in (44100, 48000)
            ):
                raise ValueError("Unsupported independent PCM inspection format")
            expected = source.getnframes()
            sample_rate = source.getframerate()
            if expected < 1 or expected * PCM_FRAME_BYTES > max_bytes:
                raise ValueError("Decoded PCM frame declaration exceeds budget")
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError("Independent inspection exceeded time budget")
                block = source.readframes(CHUNK_FRAMES)
                if not block:
                    break
                if len(block) % PCM_FRAME_BYTES:
                    raise ValueError("Truncated decoded PCM frame")
                pcm_digest.update(block)
                for left, right in struct.iter_unpack("<hh", block):
                    for channel, sample in enumerate((left, right)):
                        amplitude = abs(sample)
                        if amplitude > peaks[channel]:
                            peaks[channel] = amplitude
                            peak_frame[channel] = observed
                        sums[channel] += sample * sample
                        clipped[channel] += int(sample in (-32768, 32767))
                    observed += 1
                    if observed > expected:
                        raise ValueError("Decoded PCM exceeds declared length")
            if observed != expected:
                raise ValueError("Truncated decoded PCM body")
    except (wave.Error, EOFError, struct.error) as error:
        raise ValueError("Invalid decoded PCM") from error
    return PcmMeasurement(
        frames=observed,
        sample_rate=sample_rate,
        channels=2,
        size_bytes=size,
        checksum_sha256=checksum(path),
        pcm_sha256=pcm_digest.hexdigest(),
        sample_peak=(peaks[0] / 32768, peaks[1] / 32768),
        rms=(math.sqrt(sums[0] / observed) / 32768, math.sqrt(sums[1] / observed) / 32768),
        clipped_samples=(clipped[0], clipped[1]),
        peak_frame=(peak_frame[0], peak_frame[1]),
    )


def run_candidate_benchmark(directory: Path) -> None:
    """Leased experimental measurements only; no production catalog selection.

    Invoke manually after the parent records exact source/recipe/private path.
    The report retains failed/unexecuted cases rather than converting them into
    passes. Generic parent-loss and independent runtime qualification stay with
    AUDIO-21-03; this harness is not production supervision proof.
    """
    from src.process_runtime import run_supervised

    ffmpeg = "/opt/homebrew/opt/ffmpeg@8/bin/ffmpeg"
    ffprobe = "/opt/homebrew/opt/ffmpeg@8/bin/ffprobe"
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    os.umask(0o077)
    started = time.monotonic()
    deadline = started + 450  # Reserve >=150 seconds of the shared ten-minute lease.
    report = {"kind": "experimental-rendition-candidates", "profilesSelected": False,
              "platform": platform.platform(), "python": platform.python_version(),
              "harnessSha256": checksum(Path(__file__)), "cases": [], "failures": []}
    environment = {"PATH": str(Path(ffmpeg).parent) + ":/usr/bin:/bin", "OMP_NUM_THREADS": "2"}

    def capture(command: list[str], seconds: float = 10) -> str:
        # Trusted generated inputs and fixed -show_entries keep output finite.
        result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, env=environment,
                                timeout=min(seconds, max(0.01, deadline - time.monotonic())), check=True)
        if len(result.stdout) > 32768:
            raise ValueError("Inspection report exceeded byte budget")
        return result.stdout.decode("utf-8", errors="strict")

    def save():
        report["elapsedSeconds"] = round(time.monotonic() - started, 3)
        (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    try:
        report["ffmpegVersion"] = capture([ffmpeg, "-version"]).splitlines()[0]
        report["ffprobeVersion"] = capture([ffprobe, "-version"]).splitlines()[0]
        # Experimental fixed commands are only benchmark candidates, never an
        # allowlist derived from uploaded strings or a new advertised profile.
        candidates = (
            ("lossless-reference", ".flac", ["-c:a", "flac", "-compression_level", "5"]),
            ("aac-low", ".m4a", ["-c:a", "aac", "-profile:a", "aac_low", "-b:a", "160k", "-movflags", "+faststart"]),
            ("aac-high", ".m4a", ["-c:a", "aac", "-profile:a", "aac_low", "-b:a", "256k", "-movflags", "+faststart"]),
        )
        for fixture, rate, frames in finite_corpus():
            if time.monotonic() >= deadline or shutil.disk_usage(directory).free < 1024 ** 3:
                raise RuntimeError("Aggregate time/free-space stop condition reached")
            source = directory / (fixture + ".wav")
            write_original_fixture(source, sample_rate=rate, frames=frames)
            original = inspect_pcm16(source, deadline=deadline)
            for name, suffix, options in candidates:
                output = directory / (fixture + "-" + name + suffix)
                decoded = directory / (fixture + "-" + name + "-decoded.wav")
                case = {"fixture": fixture, "candidate": name, "source": original.report()}
                case_started = time.monotonic()
                try:
                    command = [ffmpeg, "-nostdin", "-v", "error", "-n", "-threads", "2", "-filter_threads", "2",
                               "-i", str(source), "-map", "0:a:0", "-map_metadata", "-1", "-vn", *options,
                               "-threads", "2", "-fs", str(MAX_SOURCE_BYTES + 4096), str(output)]
                    case["encodeArgvSha256"] = hashlib.sha256(json.dumps(command).encode()).hexdigest()
                    budget = min(120 if fixture == "limit44" else 60, deadline - time.monotonic())
                    run_supervised(command, environment=environment, canceled=threading.Event(), timeout_seconds=budget)
                    case["encodeSeconds"] = round(time.monotonic() - case_started, 3)
                    if output.stat().st_size > MAX_SOURCE_BYTES + 4096:
                        raise ValueError("Encoded output exceeds allocation budget")
                    case["outputBytes"] = output.stat().st_size
                    case["outputSha256"] = checksum(output)
                    case["probe"] = json.loads(capture([ffprobe, "-v", "error", "-select_streams", "a:0",
                        "-show_entries", "stream=codec_name,sample_rate,channels,duration,start_time:format=format_name,duration,size", "-of", "json", str(output)]))
                    decode_command = [ffmpeg, "-nostdin", "-v", "error", "-n", "-threads", "2", "-filter_threads", "2",
                                      "-i", str(output), "-map", "0:a:0", "-c:a", "pcm_s16le", "-threads", "2",
                                      "-fs", str(MAX_SOURCE_BYTES + 4096), str(decoded)]
                    run_supervised(decode_command, environment=environment, canceled=threading.Event(),
                                   timeout_seconds=min(120, deadline - time.monotonic()))
                    measured = inspect_pcm16(decoded, max_bytes=MAX_SOURCE_BYTES + 4096, deadline=deadline)
                    case["decoded"] = measured.report()
                    case["frameDelta"] = measured.frames - original.frames
                    case["markerLagFrames"] = [measured.peak_frame[i] - original.peak_frame[i] for i in range(2)]
                    case["sourceUnchanged"] = checksum(source) == original.checksum_sha256
                    if not case["sourceUnchanged"]:
                        raise ValueError("Original source checksum changed")
                    if name == "lossless-reference" and (measured.pcm_sha256 != original.pcm_sha256 or measured.frames != frames or measured.sample_peak != original.sample_peak
                            or measured.rms != original.rms or measured.peak_frame != original.peak_frame):
                        raise ValueError("Lossless decoded observations differ")
                    case["state"] = "measured"
                except Exception as error:
                    case["state"] = "failed"
                    case["errorType"] = type(error).__name__
                    report["failures"].append({"fixture": fixture, "candidate": name, "errorType": type(error).__name__})
                case["elapsedSeconds"] = round(time.monotonic() - case_started, 3)
                report["cases"].append(case)
                save()
                # Keep short timing outputs for the independent analyzer; never
                # retain the long/near-limit corpus after measuring its bytes.
                if fixture in ("long44", "limit44"):
                    output.unlink(missing_ok=True)
                    decoded.unlink(missing_ok=True)
            if fixture in ("long44", "limit44"):
                source.unlink(missing_ok=True)
    except Exception as error:
        report["failures"].append({"stage": "benchmark", "errorType": type(error).__name__})
        raise
    finally:
        save()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Explicitly leased RND candidate measurements")
    parser.add_argument("--benchmark-dir", type=Path, required=True)
    run_candidate_benchmark(parser.parse_args().benchmark_dir)


def test_reference_fixture_has_distinct_channel_timing_and_preserved_identity(tmp_path):
    path = tmp_path / "reference.wav"
    write_original_fixture(path, sample_rate=44100, frames=22050)
    original = checksum(path)
    result = inspect_pcm16(path)
    assert result.frames == 22050
    assert result.sample_rate == 44100
    assert result.channels == 2
    assert result.size_bytes == PCM_HEADER_BYTES + 22050 * PCM_FRAME_BYTES
    assert result.peak_frame == (4410, 13230)
    assert result.sample_peak == (0.75, 0.75)
    assert result.clipped_samples == (0, 0)
    assert all(0.05 < level < 0.2 for level in result.rms)
    assert result.checksum_sha256 == original == checksum(path)


def test_pcm_observations_detect_asymmetric_full_scale_samples(tmp_path):
    path = tmp_path / "clipping.wav"
    with wave.open(str(path), "wb") as target:
        target.setparams((2, 2, 48000, 0, "NONE", "not compressed"))
        target.writeframes(struct.pack("<hhhhhhhh", 0, 0, 32767, 4, -32768, 8, 0, -32768))
    result = inspect_pcm16(path)
    assert result.frames == 4
    assert result.clipped_samples == (2, 1)
    assert result.peak_frame == (2, 3)
    assert result.sample_peak == (1.0, 1.0)
    assert result.rms[0] > result.rms[1]


def test_reference_inspector_rejects_truncated_body_and_nonstereo(tmp_path):
    path = tmp_path / "truncated.wav"
    write_original_fixture(path, sample_rate=48000, frames=16)
    path.write_bytes(path.read_bytes()[:-PCM_FRAME_BYTES])
    with pytest.raises(ValueError, match="Truncated"):
        inspect_pcm16(path)
    mono = tmp_path / "mono.wav"
    with wave.open(str(mono), "wb") as target:
        target.setparams((1, 2, 48000, 0, "NONE", "not compressed"))
        target.writeframes(struct.pack("<h", 0))
    with pytest.raises(ValueError, match="Unsupported"):
        inspect_pcm16(mono)


def test_fixture_allocations_are_exclusive_and_finite(tmp_path):
    path = tmp_path / "original.wav"
    write_original_fixture(path, sample_rate=44100, frames=1)
    original = checksum(path)
    with pytest.raises(FileExistsError):
        write_original_fixture(path, sample_rate=44100, frames=2)
    with pytest.raises(ValueError, match="budget"):
        write_original_fixture(tmp_path / "oversized.wav", sample_rate=44100, frames=MAX_SOURCE_BYTES)
    assert checksum(path) == original
    assert not (tmp_path / "oversized.wav").exists()
    assert all(PCM_HEADER_BYTES + frames * PCM_FRAME_BYTES <= MAX_SOURCE_BYTES for _, _, frames in finite_corpus())


def _frozen_runner_identity(source=None):
    from copy import deepcopy
    from datetime import datetime, timedelta, timezone
    from src.rendition_models import rounded_duration_ms
    fixture = json.loads((Path(__file__).parent.parent / "track-tree-contracts/fixtures/rendition-protocol.json").read_text())
    identity = deepcopy(next(case["payload"]["identity"] for case in fixture["cases"] if case["name"] == "enqueue"))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for key, seconds in (("createdAt", 0), ("deadlineAt", 180), ("deliveryDeadlineAt", 600), ("retainUntil", 180 + 86400)):
        identity[key] = (now + timedelta(seconds=seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if source is not None:
        actual = inspect_pcm16(source)
        identity["source"].update({
            "checksumSha256": actual.checksum_sha256, "pcmChecksumSha256": actual.pcm_sha256,
            "sizeBytes": actual.size_bytes, "sampleRateHz": actual.sample_rate,
            "sampleCount": actual.frames, "durationMs": rounded_duration_ms(actual.frames, actual.sample_rate),
        })
    return identity


def test_runner_expired_signed_deadline_cannot_be_extended_by_caller(tmp_path):
    from datetime import datetime, timedelta, timezone
    from unittest.mock import patch
    from src.managed_process import ProcessDeadline
    from src.rendition_runner import encode_rendition
    identity = _frozen_runner_identity()
    now = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=1000)
    for key, seconds in (("createdAt", 0), ("deadlineAt", 180), ("deliveryDeadlineAt", 600), ("retainUntil", 180 + 86400)):
        identity[key] = (now + timedelta(seconds=seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with patch("src.rendition_runner.run_managed") as launch:
        with pytest.raises(ProcessDeadline):
            encode_rendition(identity, tmp_path / "missing.wav", tmp_path / "output.flac",
                             deadline=time.monotonic() + 180, canceled=threading.Event())
        launch.assert_not_called()


def test_runner_changed_source_fails_before_native_processing(tmp_path):
    from unittest.mock import patch
    from src.rendition_runner import RenditionFailure, encode_rendition
    source = tmp_path / "source.wav"
    write_original_fixture(source, sample_rate=44100, frames=264600)
    identity = _frozen_runner_identity(source)
    data = bytearray(source.read_bytes())
    data[-1] ^= 1
    source.write_bytes(data)
    changed = checksum(source)
    with patch("src.rendition_runner.run_managed") as launch:
        with pytest.raises(RenditionFailure, match="^source_integrity_failed$"):
            encode_rendition(identity, source, tmp_path / "output.flac",
                             deadline=time.monotonic() + 3, canceled=threading.Event())
        launch.assert_not_called()
    assert checksum(source) == changed
    assert sorted(path.name for path in tmp_path.iterdir()) == ["source.wav"]


def test_runner_never_retires_preexisting_output_or_inspection_allocations(tmp_path):
    from unittest.mock import patch
    from src.rendition_runner import RenditionFailure, encode_rendition
    source = tmp_path / "source.wav"
    write_original_fixture(source, sample_rate=48000, frames=288000)
    identity = _frozen_runner_identity(source)
    destination = tmp_path / "output.flac"
    original = b"preexisting-owned-file"
    for reserved in (destination, tmp_path / ".rendition-decoded.pcm"):
        reserved.write_bytes(original)
        with patch("src.rendition_runner.run_managed") as launch:
            with pytest.raises(RenditionFailure, match="^processing_failed$"):
                encode_rendition(identity, source, destination,
                                 deadline=time.monotonic() + 3, canceled=threading.Event())
            launch.assert_not_called()
        assert reserved.read_bytes() == original
        reserved.unlink()


def test_runner_reserves_half_the_signed_job_memory_for_concurrent_control(tmp_path):
    from unittest.mock import patch
    from src.managed_process import ProcessFailed
    from src.rendition_runner import RenditionFailure, encode_rendition
    source = tmp_path / "source.wav"
    write_original_fixture(source, sample_rate=44100, frames=264600)
    identity = _frozen_runner_identity(source)
    original = checksum(source)
    with patch("src.rendition_runner._runtime", return_value=("/trusted/ffmpeg", "/trusted/ffprobe", {})), \
            patch("src.rendition_runner.run_managed", side_effect=ProcessFailed("probe failed")) as launch:
        with pytest.raises(RenditionFailure, match="^source_integrity_failed$"):
            encode_rendition(identity, source, tmp_path / "output.flac",
                             deadline=time.monotonic() + 3, canceled=threading.Event())
    launch.assert_called_once()
    assert identity["policy"]["sampledProcessGroupRssStopBytes"] == 512 * 1024 * 1024
    assert launch.call_args.kwargs["limits"].max_rss_bytes == 256 * 1024 * 1024
    assert checksum(source) == original
    assert sorted(path.name for path in tmp_path.iterdir()) == ["source.wav"]
