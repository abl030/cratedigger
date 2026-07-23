"""Post-commit preservation of corrupt staged audio."""

from __future__ import annotations

import logging
import os

from lib.dispatch.types import PostCommitQuarantineAudit
from lib.import_manifest import audio_relative_paths, move_failed_import_curated

logger = logging.getLogger("cratedigger")

_ERROR_LIMIT = 1024


def _bounded_error(exc: BaseException) -> str:
    text = " ".join(str(exc).split())
    return f"{type(exc).__name__}: {text}"[:_ERROR_LIMIT]


def quarantine_corrupt_audio_source(
    *,
    source_path: str,
    quarantine_root: str,
) -> PostCommitQuarantineAudit:
    """Move a complete corrupt candidate to the standard protected bucket.

    Failures are values: the caller persists the returned audit against the
    already-committed terminal download row. The source is never deleted as a
    fallback.
    """
    source_path = os.path.abspath(source_path)
    quarantine_root = os.path.abspath(quarantine_root) if quarantine_root else ""
    if not os.path.isdir(source_path):
        return PostCommitQuarantineAudit(
            source_path=source_path,
            path_missing=True,
            error="source path is missing or not a directory",
        )
    if not quarantine_root or not os.path.isdir(quarantine_root):
        return PostCommitQuarantineAudit(
            source_path=source_path,
            error="configured slskd download root is missing or not a directory",
        )
    try:
        quarantine_path = move_failed_import_curated(
            source_path,
            allowed_audio=audio_relative_paths(source_path),
            scenario="audio_corrupt",
            quarantine_root=quarantine_root,
        )
    except Exception as exc:
        error = _bounded_error(exc)
        logger.warning(
            "AUDIO QUARANTINE FAILED: source=%s error=%s",
            source_path,
            error,
        )
        return PostCommitQuarantineAudit(
            source_path=source_path,
            error=error,
        )
    if quarantine_path is None:
        return PostCommitQuarantineAudit(
            source_path=source_path,
            path_missing=True,
            error="source path vanished before quarantine",
        )
    return PostCommitQuarantineAudit(
        source_path=source_path,
        quarantine_path=quarantine_path,
        moved=True,
    )
