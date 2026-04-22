"""Typed recovery seam for active download processing paths."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Callable, Literal

ProcessingPathKind = Literal[
    "canonical",
    "request_scoped_auto_import_staged",
    "request_scoped_post_validation_staged",
    "legacy_shared_staged",
    "external",
]

BlockedRecoveryReason = Literal[
    "multiple_populated_paths",
    "legacy_shared_only",
]


@dataclass(frozen=True)
class ProcessingPathLocation:
    """Classified local processing path for one active download."""

    path: str
    kind: ProcessingPathKind

    @property
    def display_name(self) -> str:
        if self.kind == "canonical":
            return "canonical processing path"
        if self.kind == "request_scoped_auto_import_staged":
            return "request-scoped auto-import staged path"
        if self.kind == "request_scoped_post_validation_staged":
            return "request-scoped post-validation staged path"
        if self.kind == "legacy_shared_staged":
            return "legacy shared staged path"
        return "processing path"

    @property
    def short_label(self) -> str:
        if self.kind == "canonical":
            return "canonical"
        if self.kind == "request_scoped_auto_import_staged":
            return "auto-import"
        if self.kind == "request_scoped_post_validation_staged":
            return "post-validation"
        if self.kind == "legacy_shared_staged":
            return "legacy-shared"
        return "external"

    @property
    def blocks_post_move_retry(self) -> bool:
        return self.kind == "request_scoped_auto_import_staged"

    @property
    def blocks_auto_import_dispatch(self) -> bool:
        return self.kind in (
            "request_scoped_auto_import_staged",
            "legacy_shared_staged",
        )


@dataclass(frozen=True)
class ResumeRecoveryDecision:
    """Result of probing local recovery candidates for a missing current_path."""

    canonical_path: str
    legacy_shared_path: str
    populated_locations: tuple[ProcessingPathLocation, ...]
    selected_location: ProcessingPathLocation | None = None
    blocked_reason: BlockedRecoveryReason | None = None


def canonical_processing_path(
    *,
    artist: str,
    title: str,
    year: str,
    slskd_download_dir: str,
) -> str:
    """Return the canonical local processing directory for a completed album."""
    from lib.util import sanitize_folder_name

    import_folder_name = sanitize_folder_name(f"{artist} - {title} ({year})")
    return os.path.join(slskd_download_dir, import_folder_name)


def classify_processing_path(
    *,
    current_path: str,
    artist: str,
    title: str,
    year: str,
    request_id: int,
    staging_dir: str,
    slskd_download_dir: str,
) -> ProcessingPathLocation:
    """Classify a persisted current_path against the active download seam."""
    canonical_path = canonical_processing_path(
        artist=artist,
        title=title,
        year=year,
        slskd_download_dir=slskd_download_dir,
    )
    if os.path.realpath(current_path) == os.path.realpath(canonical_path):
        return ProcessingPathLocation(path=current_path, kind="canonical")

    request_scoped_auto_import = _stage_to_ai_path(
        artist=artist,
        title=title,
        staging_dir=staging_dir,
        request_id=request_id,
        auto_import=True,
    )
    if _path_is_within(current_path, request_scoped_auto_import):
        return ProcessingPathLocation(
            path=current_path,
            kind="request_scoped_auto_import_staged",
        )

    request_scoped_post_validation = _stage_to_ai_path(
        artist=artist,
        title=title,
        staging_dir=staging_dir,
        request_id=request_id,
        auto_import=False,
    )
    if _path_is_within(current_path, request_scoped_post_validation):
        return ProcessingPathLocation(
            path=current_path,
            kind="request_scoped_post_validation_staged",
        )

    legacy_shared_path = _stage_to_ai_path(
        artist=artist,
        title=title,
        staging_dir=staging_dir,
    )
    if _path_is_within(current_path, legacy_shared_path):
        return ProcessingPathLocation(
            path=current_path,
            kind="legacy_shared_staged",
        )

    return ProcessingPathLocation(path=current_path, kind="external")


def resolve_missing_current_path(
    *,
    artist: str,
    title: str,
    year: str,
    request_id: int,
    staging_dir: str,
    slskd_download_dir: str,
    has_entries: Callable[[str], bool],
) -> ResumeRecoveryDecision:
    """Resolve a missing current_path by probing the known recovery locations."""
    canonical_path = canonical_processing_path(
        artist=artist,
        title=title,
        year=year,
        slskd_download_dir=slskd_download_dir,
    )
    candidates = (
        ProcessingPathLocation(path=canonical_path, kind="canonical"),
        ProcessingPathLocation(
            path=_stage_to_ai_path(
                artist=artist,
                title=title,
                staging_dir=staging_dir,
                request_id=request_id,
                auto_import=True,
            ),
            kind="request_scoped_auto_import_staged",
        ),
        ProcessingPathLocation(
            path=_stage_to_ai_path(
                artist=artist,
                title=title,
                staging_dir=staging_dir,
                request_id=request_id,
                auto_import=False,
            ),
            kind="request_scoped_post_validation_staged",
        ),
        ProcessingPathLocation(
            path=_stage_to_ai_path(
                artist=artist,
                title=title,
                staging_dir=staging_dir,
            ),
            kind="legacy_shared_staged",
        ),
    )
    populated_locations = tuple(
        candidate
        for candidate in candidates
        if has_entries(candidate.path)
    )
    if len(populated_locations) > 1:
        return ResumeRecoveryDecision(
            canonical_path=canonical_path,
            legacy_shared_path=candidates[-1].path,
            populated_locations=populated_locations,
            blocked_reason="multiple_populated_paths",
        )
    if len(populated_locations) == 1:
        selected_location = populated_locations[0]
        if selected_location.kind == "legacy_shared_staged":
            return ResumeRecoveryDecision(
                canonical_path=canonical_path,
                legacy_shared_path=selected_location.path,
                populated_locations=populated_locations,
                blocked_reason="legacy_shared_only",
            )
        return ResumeRecoveryDecision(
            canonical_path=canonical_path,
            legacy_shared_path=candidates[-1].path,
            populated_locations=populated_locations,
            selected_location=selected_location,
        )
    return ResumeRecoveryDecision(
        canonical_path=canonical_path,
        legacy_shared_path=candidates[-1].path,
        populated_locations=(),
        selected_location=candidates[0],
    )


def _path_is_within(path: str, root: str) -> bool:
    """Return True when ``path`` is located under ``root``."""
    if not root:
        return False
    abs_path = os.path.realpath(path)
    abs_root = os.path.realpath(root)
    try:
        return os.path.commonpath([abs_path, abs_root]) == abs_root
    except ValueError:
        return False


def _stage_to_ai_path(
    *,
    artist: str,
    title: str,
    staging_dir: str,
    request_id: int | None = None,
    auto_import: bool | None = None,
) -> str:
    from lib.staged_album import stage_to_ai_path

    return stage_to_ai_path(
        artist=artist,
        title=title,
        staging_dir=staging_dir,
        request_id=request_id,
        auto_import=auto_import,
    )
