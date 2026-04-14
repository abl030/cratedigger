"""Library filesystem permissions — centralised helpers.

Issue #84: beets creates directories via `os.mkdir()` with no explicit mode,
which means the resulting mode is `0o777 & ~umask`. Even though the systemd
service runs with `UMask=0000`, something in the subprocess chain
(import_one.py → run_beets_harness.sh → .beet-wrapped → beets) intermittently
resets the umask, producing `0o755` artist/album directories that block any
other user (abl030) from running beets commands against the library.

Rather than chase the elusive umask-reset through every layer, we:

1. Explicitly `os.umask(0)` at every pipeline entry point (belt-and-suspenders
   over systemd's `UMask=0000`).
2. After a successful import, recursively chmod the imported album dir and
   its parent (artist dir) to guarantee the final on-disk mode regardless of
   which layer dropped the umask.

The final modes (`0o777` for dirs, `0o666` for files) are the values the
service would have produced under `umask=0`, so this is not looser than the
intended policy — it just enforces it unconditionally.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

LIBRARY_DIR_MODE = 0o777
LIBRARY_FILE_MODE = 0o666


def reset_umask() -> None:
    """Set the process umask to 0, matching the systemd unit's UMask=0000.

    Call this once at every pipeline entry point (soularr.py, import_one.py,
    beets_harness.py) so subprocesses further down the chain inherit it.
    """
    os.umask(0)


def fix_library_modes(path: str) -> None:
    """Recursively chmod a library path to enforce LIBRARY_DIR_MODE / LIBRARY_FILE_MODE.

    If `path` is a directory:
      - the directory itself and its immediate parent (typically the artist
        dir above an album dir) are chmod'd to `LIBRARY_DIR_MODE` (0o777)
      - every descendant directory → `LIBRARY_DIR_MODE`
      - every descendant file → `LIBRARY_FILE_MODE` (0o666)

    If `path` is a regular file, only the file is chmod'd.

    Non-existent paths are a no-op. Per-entry chmod failures are logged and
    swallowed so one stubborn child cannot block the whole pass.
    """
    if not os.path.exists(path):
        return

    if os.path.isfile(path):
        _safe_chmod(path, LIBRARY_FILE_MODE)
        return

    # Target dir itself
    _safe_chmod(path, LIBRARY_DIR_MODE)

    # Parent (artist dir) — the specific case from #84 where the artist
    # dir was created with 0755 while the album dir underneath was 0777.
    parent = os.path.dirname(path)
    if parent and parent != path:
        _safe_chmod(parent, LIBRARY_DIR_MODE)

    for root, dirs, files in os.walk(path):
        for d in dirs:
            _safe_chmod(os.path.join(root, d), LIBRARY_DIR_MODE)
        for f in files:
            _safe_chmod(os.path.join(root, f), LIBRARY_FILE_MODE)


def _safe_chmod(path: str, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except OSError as exc:
        logger.debug("chmod %s %o failed: %s", path, mode, exc)
