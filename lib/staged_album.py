"""Typed ownership of a staged album's current filesystem location."""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    checkpoint,
)
from lib.processing_paths import bounded_staged_filename

if TYPE_CHECKING:
    from lib.grab_list import DownloadFile, GrabListEntry


logger = logging.getLogger("cratedigger")


def staged_filename(file: DownloadFile) -> str:
    """Return the local filename used once a track is under album staging.

    The remote basename is peer-controlled and may exceed the local
    filesystem's 255-byte name cap, so the assembled name is bounded last —
    after the disc prefix, which is part of what has to fit.
    """
    filename = file.filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    if file.disk_no is not None and file.disk_count is not None and file.disk_count > 1:
        filename = f"Disk {file.disk_no} - {filename}"
    return bounded_staged_filename(filename)


@dataclass
class StagedAlbum:
    """Album directory whose current location is owned explicitly."""

    current_path: str
    request_id: int | None = None

    @classmethod
    def from_entry(
        cls,
        entry: GrabListEntry,
        *,
        default_path: str,
    ) -> StagedAlbum:
        return cls(
            current_path=entry.import_folder or default_path,
            request_id=entry.db_request_id,
        )

    def import_path_for(self, file: DownloadFile) -> str:
        return os.path.join(self.current_path, staged_filename(file))

    def bind_import_paths(self, files: list[DownloadFile]) -> None:
        for file in files:
            file.import_path = self.import_path_for(file)

    def move_to(
        self,
        dest: str,
        *,
        cancellation_token: CancellationToken | None = None,
    ) -> str:
        """Move album contents without inferring lifecycle ownership from path."""
        source = os.path.abspath(self.current_path)
        target = os.path.abspath(dest)
        target_preexisted = os.path.isdir(target)

        if source == target:
            self.current_path = target
            return self.current_path

        moved_entries: list[tuple[str, str]] = []
        try:
            checkpoint(cancellation_token)
            os.makedirs(target, exist_ok=True)
            for entry in os.listdir(source):
                source_entry = os.path.join(source, entry)
                target_entry = os.path.join(target, entry)
                checkpoint(cancellation_token)
                shutil.move(source_entry, target_entry)
                moved_entries.append((source_entry, target_entry))
            self.current_path = target
            checkpoint(cancellation_token)
            shutil.rmtree(source, ignore_errors=True)
            return self.current_path
        except ExecutionCancelled:
            # Fail-stop ownership deliberately leaves any completed atomic
            # moves in place. Recovery reconciles them from durable evidence;
            # rollback would itself be a forbidden post-cancellation mutation.
            raise
        except Exception as exc:
            rollback_failures: list[tuple[str, str]] = []
            if moved_entries:
                checkpoint(cancellation_token)
                os.makedirs(source, exist_ok=True)
                for source_entry, target_entry in reversed(moved_entries):
                    if os.path.exists(target_entry):
                        try:
                            checkpoint(cancellation_token)
                            shutil.move(target_entry, source_entry)
                        except ExecutionCancelled:
                            raise
                        except Exception:
                            rollback_failures.append((source_entry, target_entry))
                            logger.exception(
                                "Failed to roll back staged move %s -> %s",
                                target_entry,
                                source_entry,
                            )
            elif (
                not target_preexisted
                and os.path.isdir(target)
                and not os.listdir(target)
            ):
                checkpoint(cancellation_token)
                shutil.rmtree(target, ignore_errors=True)
            self.current_path = source
            if rollback_failures:
                first_source, first_target = rollback_failures[0]
                raise RuntimeError(
                    "Failed to roll back staged move cleanly after a staging "
                    f"error: {first_target} -> {first_source}"
                ) from exc
            raise
