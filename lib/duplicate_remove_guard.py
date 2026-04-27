"""Duplicate-remove guard failure handling."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass

from lib.processing_paths import (
    duplicate_remove_guard_path,
    duplicate_remove_guard_root,
    path_is_within_root,
)


@dataclass(frozen=True)
class DuplicateRemoveGuardQuarantineResult:
    source_path: str
    quarantine_path: str | None = None
    moved: bool = False
    already_quarantined: bool = False
    path_missing: bool = False
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None and (self.moved or self.already_quarantined)


def _unique_destination(path: str) -> str:
    if not os.path.exists(path):
        return path
    base = path
    idx = 2
    while True:
        candidate = f"{base}-{idx}"
        if not os.path.exists(candidate):
            return candidate
        idx += 1


def quarantine_duplicate_remove_guard_source(
    *,
    source_path: str,
    staging_dir: str,
    request_id: int | None = None,
    attempt_id: int | None = None,
) -> DuplicateRemoveGuardQuarantineResult:
    """Move a risky duplicate candidate out of active staging.

    The source is preserved for operator forensics, but moved under a separate
    Incoming quarantine so the same staged directory is not retried.
    """
    quarantine_root = duplicate_remove_guard_root(staging_dir=staging_dir)
    if path_is_within_root(source_path, quarantine_root):
        return DuplicateRemoveGuardQuarantineResult(
            source_path=source_path,
            quarantine_path=source_path,
            already_quarantined=True,
        )

    if not os.path.exists(source_path):
        return DuplicateRemoveGuardQuarantineResult(
            source_path=source_path,
            path_missing=True,
            error="source path missing",
        )

    destination = _unique_destination(duplicate_remove_guard_path(
        staging_dir=staging_dir,
        source_path=source_path,
        request_id=request_id,
        attempt_id=attempt_id,
    ))
    try:
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        moved_to = shutil.move(source_path, destination)
    except OSError as exc:
        return DuplicateRemoveGuardQuarantineResult(
            source_path=source_path,
            quarantine_path=destination,
            error=str(exc),
        )

    return DuplicateRemoveGuardQuarantineResult(
        source_path=source_path,
        quarantine_path=str(moved_to),
        moved=True,
    )
