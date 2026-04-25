"""Shared cleanup helpers for Wrong Matches entries."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from typing import Any

from lib.util import resolve_failed_path


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


def _validation_result_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


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


def cleanup_wrong_match_source(
    db: Any,
    download_log_id: int,
    *,
    failed_path_hint: str | None = None,
) -> WrongMatchCleanupResult:
    """Delete one wrong-match source and clear its actionable DB pointers.

    The import dispatcher intentionally preserves force/manual source folders
    on rejection. This helper is for queue-owned force-import failures where the
    operator already chose a specific Wrong Matches candidate and a terminal
    rejection means that candidate should leave the review queue.
    """
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return WrongMatchCleanupResult(
            download_log_id=download_log_id,
            entry_found=False,
            failed_path_hint=failed_path_hint,
            error=f"Download log entry {download_log_id} not found",
        )

    request_id_raw = entry.get("request_id")
    request_id = request_id_raw if isinstance(request_id_raw, int) else None
    vr = _validation_result_dict(entry.get("validation_result"))
    raw_failed_path = vr.get("failed_path")
    raw_path = raw_failed_path if isinstance(raw_failed_path, str) else None

    candidates = _path_candidates(failed_path_hint, raw_path)
    resolved_path: str | None = None
    for path in candidates:
        resolved_path = resolve_failed_path(path)
        if resolved_path is not None:
            break

    if resolved_path is not None:
        resolved_path = os.path.abspath(resolved_path)
        try:
            shutil.rmtree(resolved_path)
        except FileNotFoundError:
            candidates = _path_candidates(*candidates, resolved_path)
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
    if request_id is not None:
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
