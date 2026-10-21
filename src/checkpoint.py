"""Only the reviewed official checkpoint may reach Demucs deserialization."""

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import shutil
import tempfile
import urllib.request

CHECKPOINT_NAME = "955717e8-8726e21a.th"
CHECKPOINT_SHA256 = "8726e21a993978c7ba086d3872e7608d7d5bfca646ca4aca459ffda844faa8b4"
CHECKPOINT_URL = (
    "https://dl.fbaipublicfiles.com/demucs/hybrid_transformer/" + CHECKPOINT_NAME
)
MAX_CHECKPOINT_BYTES = 128 * 1024 * 1024


def copy_verified(source, destination: Path):
    digest = hashlib.sha256()
    size = 0
    with destination.open("xb") as target:
        os.chmod(destination, 0o600)
        while chunk := source.read(1024 * 1024):
            size += len(chunk)
            if size > MAX_CHECKPOINT_BYTES:
                raise ValueError("Checkpoint exceeds its trusted size limit")
            digest.update(chunk)
            target.write(chunk)
    if digest.hexdigest() != CHECKPOINT_SHA256:
        destination.unlink(missing_ok=True)
        raise ValueError("Checkpoint does not match the reviewed SHA256")


@contextmanager
def verified_model_repo(legacy_checkpoint: Path | None = None):
    cache = Path.home() / ".cache" / "track-tree-audio" / "models"
    if cache.is_symlink():
        raise ValueError("Model cache must be a private directory")
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    if cache.stat().st_uid != os.getuid():
        raise ValueError("Model cache must belong to the service user")
    os.chmod(cache, 0o700)
    cached = cache / CHECKPOINT_NAME
    with tempfile.TemporaryDirectory(prefix="verified-", dir=cache) as directory:
        repo = Path(directory)
        checkpoint = repo / CHECKPOINT_NAME
        source_path = cached if cached.exists() else legacy_checkpoint
        if source_path is not None and source_path.exists():
            # Rehash every cached load into a private snapshot; fail closed on corruption.
            with source_path.open("rb") as source:
                copy_verified(source, checkpoint)
        else:
            with urllib.request.urlopen(CHECKPOINT_URL, timeout=60) as response:
                if response.status != 200 or response.url != CHECKPOINT_URL:
                    raise ValueError("Checkpoint download destination changed")
                copy_verified(response, checkpoint)
        if not cached.exists():
            staged = repo / "cache.tmp"
            shutil.copyfile(checkpoint, staged)
            os.chmod(staged, 0o600)
            os.replace(staged, cached)
        # A private snapshot closes the shared-cache verify/load replacement window.
        (repo / "htdemucs.yaml").write_text("models: ['955717e8']\n")
        yield repo
