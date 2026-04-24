"""Typed recovery seam for active download processing paths."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Callable, Literal

from lib.processing_paths import (
    canonical_processing_path,
    normalize_processing_path,
    stage_to_ai_path,
)

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
        # ``legacy_shared_staged`` should be unreachable in the normal
        # recovery flow, but keep the dispatch guard as defense-in-depth
        # for manually edited rows or future bypass paths.
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


@dataclass(frozen=True)
class BlockedRecoveryIssue:
    """Blocked local recovery state for a row with no persisted current_path."""

    request_id: int
    detail: str


def _recovery_candidates(
    *,
    artist: str,
    title: str,
    year: str,
    request_id: int,
    staging_dir: str,
    slskd_download_dir: str,
) -> tuple[ProcessingPathLocation, ...]:
    canonical_path = canonical_processing_path(
        artist=artist,
        title=title,
        year=year,
        slskd_download_dir=slskd_download_dir,
    )
    return (
        ProcessingPathLocation(path=canonical_path, kind="canonical"),
        ProcessingPathLocation(
            path=stage_to_ai_path(
                artist=artist,
                title=title,
                staging_dir=staging_dir,
                request_id=request_id,
                auto_import=True,
            ),
            kind="request_scoped_auto_import_staged",
        ),
        ProcessingPathLocation(
            path=stage_to_ai_path(
                artist=artist,
                title=title,
                staging_dir=staging_dir,
                request_id=request_id,
                auto_import=False,
            ),
            kind="request_scoped_post_validation_staged",
        ),
        ProcessingPathLocation(
            path=stage_to_ai_path(
                artist=artist,
                title=title,
                staging_dir=staging_dir,
            ),
            kind="legacy_shared_staged",
        ),
    )


def _resolve_recovery_candidates(
    candidates: tuple[ProcessingPathLocation, ...],
    *,
    has_entries: Callable[[str], bool],
) -> ResumeRecoveryDecision:
    populated_locations = tuple(
        candidate
        for candidate in candidates
        if has_entries(candidate.path)
    )
    canonical_path = candidates[0].path
    legacy_shared_path = candidates[-1].path
    if len(populated_locations) > 1:
        return ResumeRecoveryDecision(
            canonical_path=canonical_path,
            legacy_shared_path=legacy_shared_path,
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
            legacy_shared_path=legacy_shared_path,
            populated_locations=populated_locations,
            selected_location=selected_location,
        )
    return ResumeRecoveryDecision(
        canonical_path=canonical_path,
        legacy_shared_path=legacy_shared_path,
        populated_locations=(),
        selected_location=candidates[0],
    )


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
    if normalize_processing_path(current_path) == normalize_processing_path(
        canonical_path,
    ):
        return ProcessingPathLocation(path=current_path, kind="canonical")

    request_scoped_auto_import = stage_to_ai_path(
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

    request_scoped_post_validation = stage_to_ai_path(
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

    legacy_shared_path = stage_to_ai_path(
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
    candidates = _recovery_candidates(
        artist=artist,
        title=title,
        year=year,
        request_id=request_id,
        staging_dir=staging_dir,
        slskd_download_dir=slskd_download_dir,
    )
    return _resolve_recovery_candidates(
        candidates,
        has_entries=has_entries,
    )


def reconcile_processing_current_path(
    *,
    current_path: str | None,
    artist: str,
    title: str,
    year: str,
    request_id: int,
    staging_dir: str,
    slskd_download_dir: str,
    has_entries: Callable[[str], bool],
) -> ResumeRecoveryDecision:
    """Resolve the best local path for a row already in local processing.

    If the persisted ``current_path`` is missing, this is the ordinary
    mid-process recovery path. If it still points at the canonical
    processing dir but that dir no longer holds the album, probe the
    staged candidates before the poll loop re-materializes a stale
    canonical path and strands the moved files.
    """
    candidates = _recovery_candidates(
        artist=artist,
        title=title,
        year=year,
        request_id=request_id,
        staging_dir=staging_dir,
        slskd_download_dir=slskd_download_dir,
    )
    if current_path is None:
        return _resolve_recovery_candidates(
            candidates,
            has_entries=has_entries,
        )

    current_location = classify_processing_path(
        current_path=current_path,
        artist=artist,
        title=title,
        year=year,
        request_id=request_id,
        staging_dir=staging_dir,
        slskd_download_dir=slskd_download_dir,
    )
    if current_location.kind != "canonical" or has_entries(current_location.path):
        return ResumeRecoveryDecision(
            canonical_path=candidates[0].path,
            legacy_shared_path=candidates[-1].path,
            populated_locations=(),
            selected_location=current_location,
        )
    return _resolve_recovery_candidates(
        candidates,
        has_entries=has_entries,
    )


def find_blocked_recovery_issues(
    db_rows: list[dict[str, object]],
    active_transfers: set[tuple[str, str]],
    *,
    staging_dir: str,
    slskd_download_dir: str,
    has_entries: Callable[[str], bool],
) -> list[BlockedRecoveryIssue]:
    """Find rows whose mid-processing recovery is blocked by ambiguity."""
    issues: list[BlockedRecoveryIssue] = []
    for row in db_rows:
        if row.get("status") != "downloading":
            continue
        state = row.get("active_download_state")
        if not isinstance(state, dict):
            continue
        if state.get("processing_started_at") is None:
            continue
        if state.get("current_path") is not None:
            continue
        files = state.get("files")
        if not isinstance(files, list) or not files:
            continue
        has_active = any(
            (
                str(file_state.get("username") or ""),
                str(file_state.get("filename") or ""),
            ) in active_transfers
            for file_state in files
            if isinstance(file_state, dict)
        )
        if has_active:
            continue

        request_id = row.get("id")
        if not isinstance(request_id, int) or isinstance(request_id, bool):
            continue
        recovery_decision = resolve_missing_current_path(
            artist=str(row.get("artist_name") or ""),
            title=str(row.get("album_title") or ""),
            year=str(row.get("year") or ""),
            request_id=request_id,
            staging_dir=staging_dir,
            slskd_download_dir=slskd_download_dir,
            has_entries=has_entries,
        )
        if recovery_decision.blocked_reason == "multiple_populated_paths":
            rendered_candidates = ", ".join(
                f"{location.short_label}={location.path}"
                for location in recovery_decision.populated_locations
            )
            issues.append(BlockedRecoveryIssue(
                request_id=request_id,
                detail=(
                    "multiple populated local recovery paths block automatic "
                    f"resume: {rendered_candidates}"
                ),
            ))
            continue
        if recovery_decision.blocked_reason == "legacy_shared_only":
            issues.append(BlockedRecoveryIssue(
                request_id=request_id,
                detail=(
                    "ambiguous legacy shared staged path blocks automatic "
                    f"resume: {recovery_decision.legacy_shared_path}"
                ),
            ))
            continue
    return issues


def find_blocked_processing_path_issues(
    db_rows: list[dict[str, object]],
    active_transfers: set[tuple[str, str]],
    *,
    staging_dir: str,
    slskd_download_dir: str,
    has_entries: Callable[[str], bool],
) -> list[BlockedRecoveryIssue]:
    """Find persisted processing paths that block automatic resume."""
    issues: list[BlockedRecoveryIssue] = []
    for row in db_rows:
        if row.get("status") != "downloading":
            continue
        state = row.get("active_download_state")
        if not isinstance(state, dict):
            continue
        if state.get("processing_started_at") is None:
            continue
        current_path = state.get("current_path")
        if not isinstance(current_path, str) or not current_path:
            continue
        files = state.get("files")
        if not isinstance(files, list) or not files:
            continue
        has_active = any(
            (
                str(file_state.get("username") or ""),
                str(file_state.get("filename") or ""),
            ) in active_transfers
            for file_state in files
            if isinstance(file_state, dict)
        )
        if has_active:
            continue

        request_id = row.get("id")
        if not isinstance(request_id, int) or isinstance(request_id, bool):
            continue
        recovery_decision = reconcile_processing_current_path(
            current_path=current_path,
            artist=str(row.get("artist_name") or ""),
            title=str(row.get("album_title") or ""),
            year=str(row.get("year") or ""),
            request_id=request_id,
            staging_dir=staging_dir,
            slskd_download_dir=slskd_download_dir,
            has_entries=has_entries,
        )
        if recovery_decision.blocked_reason == "multiple_populated_paths":
            rendered_candidates = ", ".join(
                f"{location.short_label}={location.path}"
                for location in recovery_decision.populated_locations
            )
            issues.append(BlockedRecoveryIssue(
                request_id=request_id,
                detail=(
                    "multiple populated local recovery paths block automatic "
                    f"resume: {rendered_candidates}"
                ),
            ))
            continue
        if recovery_decision.blocked_reason == "legacy_shared_only":
            issues.append(BlockedRecoveryIssue(
                request_id=request_id,
                detail=(
                    "ambiguous legacy shared staged path blocks automatic "
                    f"resume: {recovery_decision.legacy_shared_path}"
                ),
            ))
            continue

        assert recovery_decision.selected_location is not None
        location = recovery_decision.selected_location
        if location.path != current_path:
            continue
        if not has_entries(current_path):
            issues.append(BlockedRecoveryIssue(
                request_id=request_id,
                detail=(
                    "persisted processing path missing after local "
                    f"processing: {current_path}"
                ),
            ))
            continue
        if not location.blocks_auto_import_dispatch:
            continue
        issues.append(BlockedRecoveryIssue(
            request_id=request_id,
            detail=(
                f"persisted {location.display_name} blocks automatic resume: "
                f"{location.path}"
            ),
        ))
    return issues


def _path_is_within(path: str, root: str) -> bool:
    """Return True when ``path`` is located under ``root``."""
    if not root:
        return False
    abs_path = normalize_processing_path(path)
    abs_root = normalize_processing_path(root)
    try:
        return os.path.commonpath([abs_path, abs_root]) == abs_root
    except ValueError:
        return False
