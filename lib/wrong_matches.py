"""Shared cleanup helpers for Wrong Matches entries."""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

from lib.util import FAILED_IMPORT_SEARCH_DIRS, resolve_failed_path
from lib.validation_envelope import decode_validation_envelope
from lib.wrong_match_policy import (
    WRONG_MATCH_QUARANTINE_DIR,
    rejection_scenario_is_wrong_match_candidate,
)

if TYPE_CHECKING:
    from lib.pipeline_db.rows import DownloadLogWithEvidenceRow, WrongMatchCandidateRow

_WRONG_MATCH_SOURCE_DIR_NAMES = ("failed_imports", WRONG_MATCH_QUARANTINE_DIR)


def wrong_match_row_is_visible(
    row: Mapping[str, object],
    *,
    include_replaced: bool = False,
) -> bool:
    """Return whether a projected row belongs in the operator worklist.

    Replaced requests are frozen audit history, not live/actionable Wrong
    Matches. Explicit history views can opt back in; every default consumer
    shares this predicate so card visibility and lifecycle references agree.
    """
    if not include_replaced and row.get("request_status") == "replaced":
        return False
    scenario = decode_validation_envelope(row.get("validation_result")).scenario
    return rejection_scenario_is_wrong_match_candidate(scenario)


@runtime_checkable
class WrongMatchSourceDB(Protocol):
    """The PipelineDB surface the wrong-match source helpers use (#409).

    ``WrongMatchCleanupDB`` and ``WrongMatchDeleteDB`` extend this protocol
    because their services forward the handle into these helpers. Parity
    tests live in ``tests/test_wrong_matches_cleanup.py``.
    """

    def get_wrong_matches(self) -> "list[WrongMatchCandidateRow]": ...

    def get_download_log_entry(
        self, log_id: int,
    ) -> "DownloadLogWithEvidenceRow | None": ...

    def clear_wrong_match_path(self, log_id: int) -> bool: ...

    def clear_wrong_match_paths(
        self,
        request_id: int,
        failed_paths: list[str] | tuple[str, ...] | set[str],
    ) -> int: ...


@dataclass(frozen=True)
class WrongMatchCleanupResult:
    download_log_id: int
    entry_found: bool
    request_id: int | None = None
    raw_failed_path: str | None = None
    failed_path_hint: str | None = None
    resolved_path: str | None = None
    deleted_path: str | None = None
    path_missing: bool = False
    cleared_rows: int = 0
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.entry_found and self.error is None

    def to_dict(self) -> dict[str, object]:
        return {
            "download_log_id": self.download_log_id,
            "entry_found": self.entry_found,
            "request_id": self.request_id,
            "raw_failed_path": self.raw_failed_path,
            "failed_path_hint": self.failed_path_hint,
            "resolved_path": self.resolved_path,
            "deleted_path": self.deleted_path,
            "path_missing": self.path_missing,
            "cleared_rows": self.cleared_rows,
            "error": self.error,
            "success": self.success,
        }


@dataclass(frozen=True)
class WrongMatchDismissResult:
    download_log_id: int
    entry_found: bool
    request_id: int | None = None
    raw_failed_path: str | None = None
    failed_path_hint: str | None = None
    resolved_path: str | None = None
    cleared_rows: int = 0
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.entry_found and self.error is None

    def to_dict(self) -> dict[str, object]:
        return {
            "download_log_id": self.download_log_id,
            "entry_found": self.entry_found,
            "request_id": self.request_id,
            "raw_failed_path": self.raw_failed_path,
            "failed_path_hint": self.failed_path_hint,
            "resolved_path": self.resolved_path,
            "cleared_rows": self.cleared_rows,
            "error": self.error,
            "success": self.success,
        }


def validation_failed_path(raw: Any) -> str | None:
    return decode_validation_envelope(raw).failed_path or None


def _path_candidates(*paths: str | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for path in paths:
        if not path:
            continue
        normalized = str(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _wrong_match_entry_parts(
    db: WrongMatchSourceDB,
    download_log_id: int,
) -> tuple[Mapping[str, Any] | None, int | None, str | None]:
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return None, None, None
    # ``request_id`` is a required, non-nullable ``download_log`` column
    # (DownloadLogRow), so the row type already proves this is an ``int`` —
    # no runtime narrowing needed once the row comes through the typed
    # projection.
    request_id = entry["request_id"]
    raw_path = validation_failed_path(entry.get("validation_result"))
    return entry, request_id, raw_path


def _resolved_candidates(candidates: list[str]) -> tuple[str | None, list[str]]:
    resolved_path: str | None = None
    for path in candidates:
        resolved_path = resolve_failed_path(path)
        if resolved_path is not None:
            resolved_path = os.path.abspath(resolved_path)
            candidates = _path_candidates(*candidates, resolved_path)
            break
    return resolved_path, candidates


def unsafe_failed_import_path_reason(path: str) -> str | None:
    """Return a reason when ``path`` is outside a Wrong Match source root.

    ``failed_imports`` remains authorized for historical rows; new match
    failures live under the dedicated sibling ``wrong_matches`` root.
    """
    real_path = os.path.realpath(path)
    for root in _wrong_match_source_roots():
        if _is_child_path(real_path, root):
            return None
    if _has_wrong_match_source_ancestor(real_path):
        return None
    return f"unsafe_failed_import_path: {path}"


def _wrong_match_source_roots() -> tuple[str, ...]:
    return tuple(
        os.path.realpath(os.path.join(base, root_name))
        for base in FAILED_IMPORT_SEARCH_DIRS
        for root_name in _WRONG_MATCH_SOURCE_DIR_NAMES
    )


def _is_child_path(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([path, root]) == root and path != root
    except ValueError:
        return False


def _has_wrong_match_source_ancestor(path: str) -> bool:
    parts = path.split(os.sep)
    for index, part in enumerate(parts):
        if (
            part in _WRONG_MATCH_SOURCE_DIR_NAMES
            and index < len(parts) - 1
        ):
            return True
    return False


def _equivalent_failed_path_aliases(
    db: WrongMatchSourceDB,
    request_id: int | None,
    resolved_path: str | None,
) -> list[str]:
    if request_id is None or resolved_path is None:
        return []
    target_path = os.path.realpath(resolved_path)
    aliases: list[str] = []
    for row in db.get_wrong_matches():
        if row.get("request_id") != request_id:
            continue
        raw_path = validation_failed_path(row.get("validation_result"))
        if not raw_path:
            continue
        row_resolved = resolve_failed_path(raw_path)
        if row_resolved and os.path.realpath(row_resolved) == target_path:
            aliases.append(raw_path)
    return aliases


def dismiss_wrong_match_source(
    db: WrongMatchSourceDB,
    download_log_id: int,
    *,
    failed_path_hint: str | None = None,
) -> WrongMatchDismissResult:
    """Clear one wrong-match source from review without deleting its files.

    Converge queues the selected folder for import. The importer still needs
    the source path from the job payload, so this helper only removes the DB
    pointers that make the folder appear actionable in Wrong Matches.
    """
    _entry, request_id, raw_path = _wrong_match_entry_parts(db, download_log_id)
    if _entry is None:
        return WrongMatchDismissResult(
            download_log_id=download_log_id,
            entry_found=False,
            failed_path_hint=failed_path_hint,
            error=f"Download log entry {download_log_id} not found",
        )

    candidates = _path_candidates(failed_path_hint, raw_path)
    resolved_path, candidates = _resolved_candidates(candidates)
    candidates = _path_candidates(
        *candidates,
        *_equivalent_failed_path_aliases(db, request_id, resolved_path),
    )

    cleared_rows = 0
    if request_id is not None:
        cleared_rows = int(db.clear_wrong_match_paths(request_id, candidates))
    elif raw_path:
        cleared_rows = 1 if db.clear_wrong_match_path(download_log_id) else 0

    return WrongMatchDismissResult(
        download_log_id=download_log_id,
        entry_found=True,
        request_id=request_id,
        raw_failed_path=raw_path,
        failed_path_hint=failed_path_hint,
        resolved_path=resolved_path,
        cleared_rows=cleared_rows,
    )


def cleanup_wrong_match_source(
    db: WrongMatchSourceDB,
    download_log_id: int,
    *,
    failed_path_hint: str | None = None,
    clear_missing: bool = True,
) -> WrongMatchCleanupResult:
    """Delete one wrong-match source and clear its actionable DB pointers.

    The import dispatcher intentionally preserves force-import source folders
    on rejection. This helper is for queue-owned force-import failures where the
    operator already chose a specific Wrong Matches candidate and a terminal
    rejection means that candidate should leave the review queue.
    """
    _entry, request_id, raw_path = _wrong_match_entry_parts(db, download_log_id)
    if _entry is None:
        return WrongMatchCleanupResult(
            download_log_id=download_log_id,
            entry_found=False,
            failed_path_hint=failed_path_hint,
            error=f"Download log entry {download_log_id} not found",
        )

    candidates = _path_candidates(failed_path_hint, raw_path)
    resolved_path, candidates = _resolved_candidates(candidates)
    candidates = _path_candidates(
        *candidates,
        *_equivalent_failed_path_aliases(db, request_id, resolved_path),
    )

    if resolved_path is not None:
        unsafe_reason = unsafe_failed_import_path_reason(resolved_path)
        if unsafe_reason:
            return WrongMatchCleanupResult(
                download_log_id=download_log_id,
                entry_found=True,
                request_id=request_id,
                raw_failed_path=raw_path,
                failed_path_hint=failed_path_hint,
                resolved_path=resolved_path,
                error=unsafe_reason,
            )
        try:
            shutil.rmtree(resolved_path)
        except FileNotFoundError:
            deleted_path = None
            path_missing = True
        except Exception as exc:
            return WrongMatchCleanupResult(
                download_log_id=download_log_id,
                entry_found=True,
                request_id=request_id,
                raw_failed_path=raw_path,
                failed_path_hint=failed_path_hint,
                resolved_path=resolved_path,
                error=f"{type(exc).__name__}: {exc}",
            )
        else:
            candidates = _path_candidates(*candidates, resolved_path)
            deleted_path = resolved_path
            path_missing = False
    else:
        deleted_path = None
        path_missing = True

    cleared_rows = 0
    if path_missing and not clear_missing:
        cleared_rows = 0
    elif request_id is not None:
        cleared_rows = int(db.clear_wrong_match_paths(request_id, candidates))
    elif raw_path:
        cleared_rows = 1 if db.clear_wrong_match_path(download_log_id) else 0

    return WrongMatchCleanupResult(
        download_log_id=download_log_id,
        entry_found=True,
        request_id=request_id,
        raw_failed_path=raw_path,
        failed_path_hint=failed_path_hint,
        resolved_path=resolved_path,
        deleted_path=deleted_path,
        path_missing=path_missing,
        cleared_rows=cleared_rows,
    )
