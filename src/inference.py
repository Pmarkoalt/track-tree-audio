"""Killable Demucs CLI with a parent-loss guardian; prevents orphan compute."""

import os
from pathlib import Path
import signal
import sys
import threading
import time


def main():
    parent_pid = int(sys.argv[1])
    sys.argv = ["demucs"] + sys.argv[2:]

    def guard():
        while True:
            time.sleep(0.5)
            if os.getppid() != parent_pid:
                os.killpg(os.getpid(), signal.SIGKILL)

    threading.Thread(target=guard, daemon=True).start()
    import torch
    from .checkpoint import CHECKPOINT_NAME, verified_model_repo
    from demucs.separate import main as demucs_main

    legacy_checkpoint = Path(torch.hub.get_dir()) / "checkpoints" / CHECKPOINT_NAME
    with verified_model_repo(legacy_checkpoint) as repo:
        # Demucs' reviewed checkpoint contains its model class, not only tensors.
        # LocalRepo needs the same trusted-object loading used by RemoteRepo.
        # This process can load only the full-SHA-verified private repository above.
        os.environ["TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD"] = "1"
        sys.argv.extend(["--repo", str(repo)])
        demucs_main()


if __name__ == "__main__":
    main()
