"""Typed ownership of a staged album's current filesystem location."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import shutil
from typing import Protocol, TYPE_CHECKING

from lib.util import sanitize_folder_name

if TYPE_CHECKING:
    from lib.grab_list import DownloadFile, GrabListEntry


logger = logging.getLogger("cratedigger")

AUTO_IMPORT_STAGING_SUBDIR = "auto-import"
POST_VALIDATION_STAGING_SUBDIR = "post-validation"


class SupportsCurrentPathUpdate(Protocol):
    """Minimal DB seam for persisting ``active_download_state.current_path``."""

    def update_download_state_current_path(
        self,
        request_id: int,
        current_path: str | None,
    ) -> None:
        ...


def staged_filename(file: "DownloadFile") -> str:
    """Return the local filename used once a track is under album staging."""
    filename = file.filename.split("\\")[-1]
    if file.disk_no is not None and file.disk_count is not None and file.disk_count > 1:
        return f"Disk {file.disk_no} - {filename}"
    return filename


def stage_to_ai_root(
    *,
    staging_dir: str,
    auto_import: bool | None = None,
) -> str:
    """Return the root staging directory for a given validation branch."""
    if auto_import is None:
        return staging_dir
    subdir = (
        AUTO_IMPORT_STAGING_SUBDIR
        if auto_import
        else POST_VALIDATION_STAGING_SUBDIR
    )
    return os.path.join(staging_dir, subdir)


def stage_to_ai_path(
    *,
    artist: str,
    title: str,
    staging_dir: str,
    request_id: int | None = None,
    auto_import: bool | None = None,
) -> str:
    """Return the beets staging destination for an album."""
    artist_dir = sanitize_folder_name(artist)
    album_dir = sanitize_folder_name(title)
    if request_id is not None:
        album_dir = f"{album_dir} [request-{request_id}]"
    return os.path.join(
        stage_to_ai_root(staging_dir=staging_dir, auto_import=auto_import),
        artist_dir,
        album_dir,
    )


@dataclass
class StagedAlbum:
    """Album directory whose current location is owned explicitly."""

    current_path: str
    request_id: int | None = None

    @classmethod
    def from_entry(
        cls,
        entry: "GrabListEntry",
        *,
        default_path: str,
    ) -> "StagedAlbum":
        return cls(
            current_path=entry.import_folder or default_path,
            request_id=entry.db_request_id,
        )

    def import_path_for(self, file: "DownloadFile") -> str:
        return os.path.join(self.current_path, staged_filename(file))

    def bind_import_paths(self, files: list["DownloadFile"]) -> None:
        for file in files:
            file.import_path = self.import_path_for(file)

    def persist_current_path(
        self,
        db: SupportsCurrentPathUpdate | None,
    ) -> None:
        if self.request_id is None or db is None:
            return
        db.update_download_state_current_path(self.request_id, self.current_path)

    def move_to(
        self,
        dest: str,
        db: SupportsCurrentPathUpdate | None = None,
    ) -> str:
        """Move album contents into ``dest`` and persist the new location."""
        source = os.path.abspath(self.current_path)
        target = os.path.abspath(dest)
        target_preexisted = os.path.isdir(target)

        if source == target:
            self.current_path = target
            self.persist_current_path(db)
            return self.current_path

        moved_entries: list[tuple[str, str]] = []
        try:
            os.makedirs(target, exist_ok=True)
            for entry in os.listdir(source):
                source_entry = os.path.join(source, entry)
                target_entry = os.path.join(target, entry)
                shutil.move(source_entry, target_entry)
                moved_entries.append((source_entry, target_entry))
            shutil.rmtree(source, ignore_errors=True)
            self.current_path = target
            self.persist_current_path(db)
            return self.current_path
        except Exception:
            if moved_entries:
                os.makedirs(source, exist_ok=True)
                for source_entry, target_entry in reversed(moved_entries):
                    if os.path.exists(target_entry):
                        try:
                            shutil.move(target_entry, source_entry)
                        except Exception:
                            logger.exception(
                                "Failed to roll back staged move %s -> %s",
                                target_entry,
                                source_entry,
                            )
            elif not target_preexisted and os.path.isdir(target) and not os.listdir(target):
                shutil.rmtree(target, ignore_errors=True)
            self.current_path = source
            raise
