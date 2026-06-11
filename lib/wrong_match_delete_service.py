"""Manual Wrong Matches source deletion service."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Iterable, Protocol, cast, runtime_checkable

import msgspec

from lib.import_queue import ImportJob
from lib.wrong_matches import (
    cleanup_wrong_match_source,
    unsafe_failed_import_path_reason,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
    wrong_match_cleanup_lock_key,
)
from lib.processing_paths import normalize_source_dirs
from lib.util import resolve_failed_path
from lib.validation_envelope import (
    ValidationResultEnvelope,
    decode_validation_envelope,
)


@runtime_checkable
class WrongMatchDeleteDB(Protocol):
    """The PipelineDB surface this service uses directly (#409).

    Satisfied structurally by ``PipelineDB`` and ``FakePipelineDB``; parity
    tests live in ``tests/test_wrong_matches_cleanup.py``. The handle is
    also forwarded to ``cleanup_wrong_match_source`` (lib/wrong_matches.py),
    which gets its own protocol in its own #409 increment.
    """

    def get_wrong_matches(self) -> list[dict[str, object]]: ...

    def get_download_log_entry(self, log_id: int) -> dict[str, Any] | None: ...

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def list_active_import_jobs_for_wrong_match(
        self,
        *,
        download_log_id: int,
        request_id: int | None,
        failed_paths: Iterable[str],
        source_dirs: Iterable[str],
        ignore_import_job_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]: ...


OUTCOME_DELETED = "deleted"
OUTCOME_DELETE_FAILED = "delete_failed"
OUTCOME_SKIPPED_ACTIVE_JOB = "skipped_active_job"
OUTCOME_SKIPPED_INVALID_ROW = "skipped_invalid_row"
OUTCOME_SKIPPED_NOT_VISIBLE = "skipped_not_visible"
OUTCOME_SKIPPED_LOCKED = "skipped_locked"
OUTCOME_SKIPPED_UNSAFE_PATH = "skipped_unsafe_path"

GROUP_OUTCOME_DELETED = "deleted"
GROUP_OUTCOME_EMPTY = "empty"
GROUP_OUTCOME_PARTIAL = "partial"
GROUP_OUTCOME_FAILED = "failed"


class WrongMatchDeleteResult(msgspec.Struct, frozen=True):
    download_log_id: int
    outcome: str
    success: bool = False
    request_id: int | None = None
    entry_found: bool = False
    visible: bool = False
    raw_failed_path: str | None = None
    failed_path_hint: str | None = None
    resolved_path: str | None = None
    deleted_path: str | None = None
    path_missing: bool = False
    cleared_rows: int = 0
    skipped: bool = False
    reason: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], msgspec.to_builtins(self))


class WrongMatchDeleteSummary(msgspec.Struct, frozen=True):
    request_id: int
    outcome: str
    success: bool
    processed: int
    deleted: int
    deleted_paths: int
    cleared: int
    skipped: int
    errors: int
    remaining: int
    group_empty: bool
    results: tuple[WrongMatchDeleteResult, ...]

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], msgspec.to_builtins(self))


def delete_wrong_match(
    db: WrongMatchDeleteDB,
    download_log_id: int,
    *,
    failed_path_hint: str | None = None,
    source_dirs_hint: Iterable[str] = (),
    ignore_import_job_id: int | None = None,
    require_visible: bool = True,
) -> WrongMatchDeleteResult:
    """Delete and clear one Wrong Matches source without deciding importability."""
    try:
        return _delete_wrong_match(
            db,
            download_log_id,
            failed_path_hint=failed_path_hint,
            source_dirs_hint=source_dirs_hint,
            ignore_import_job_id=ignore_import_job_id,
            require_visible=require_visible,
        )
    except Exception as exc:  # noqa: BLE001
        return WrongMatchDeleteResult(
            download_log_id=download_log_id,
            outcome=OUTCOME_DELETE_FAILED,
            error=f"{type(exc).__name__}: {exc}",
            reason="operational_failure",
        )


def delete_wrong_match_group(
    db: WrongMatchDeleteDB,
    request_id: int,
) -> WrongMatchDeleteSummary:
    results: list[WrongMatchDeleteResult] = []
    for row in list(db.get_wrong_matches()):
        if row.get("request_id") != request_id:
            continue
        log_id = row.get("download_log_id")
        if not isinstance(log_id, int) or isinstance(log_id, bool):
            results.append(WrongMatchDeleteResult(
                download_log_id=0,
                request_id=request_id,
                outcome=OUTCOME_SKIPPED_INVALID_ROW,
                skipped=True,
                reason="invalid_download_log_id",
            ))
            continue
        if _visible_wrong_match_row(db, log_id) is None:
            continue
        results.append(delete_wrong_match(db, log_id, require_visible=True))

    remaining = _remaining_visible_count(db, request_id)
    deleted = sum(1 for result in results if result.success and result.cleared_rows)
    deleted_paths = sum(1 for result in results if result.deleted_path)
    cleared = sum(result.cleared_rows for result in results)
    skipped = sum(1 for result in results if result.skipped)
    errors = sum(
        1
        for result in results
        if result.error or result.outcome == OUTCOME_DELETE_FAILED
    )
    success = (
        (not results and remaining == 0)
        or (errors == 0 and skipped == 0 and remaining == 0)
    )
    outcome = _group_outcome(
        processed=len(results),
        success=success,
        errors=errors,
        skipped=skipped,
        remaining=remaining,
    )
    return WrongMatchDeleteSummary(
        request_id=request_id,
        outcome=outcome,
        success=success,
        processed=len(results),
        deleted=deleted,
        deleted_paths=deleted_paths,
        cleared=cleared,
        skipped=skipped,
        errors=errors,
        remaining=remaining,
        group_empty=remaining == 0,
        results=tuple(results),
    )


def _delete_wrong_match(
    db: WrongMatchDeleteDB,
    download_log_id: int,
    *,
    failed_path_hint: str | None,
    source_dirs_hint: Iterable[str],
    ignore_import_job_id: int | None,
    require_visible: bool,
) -> WrongMatchDeleteResult:
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_INVALID_ROW,
            reason="download_log_missing",
        )
    request_id_raw = entry.get("request_id")
    request_id = request_id_raw if type(request_id_raw) is int else None
    validation_result = decode_validation_envelope(entry.get("validation_result"))
    raw_failed_path = validation_result.failed_path or None
    if not raw_failed_path:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_INVALID_ROW,
            request_id=request_id,
            entry_found=True,
            reason="failed_path_missing",
        )

    if require_visible and _visible_wrong_match_row(db, download_log_id) is None:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_NOT_VISIBLE,
            request_id=request_id,
            entry_found=True,
            raw_failed_path=raw_failed_path,
            failed_path_hint=failed_path_hint,
            skipped=True,
            reason="wrong_match_not_visible",
        )

    candidates = _path_candidates(failed_path_hint, raw_failed_path)
    resolved_path = _resolve_first_existing(candidates)
    if resolved_path:
        candidates = _path_candidates(*candidates, resolved_path)
        unsafe_reason = unsafe_failed_import_path_reason(resolved_path)
        if unsafe_reason:
            return _result(
                download_log_id,
                OUTCOME_SKIPPED_UNSAFE_PATH,
                request_id=request_id,
                entry_found=True,
                visible=True,
                raw_failed_path=raw_failed_path,
                failed_path_hint=failed_path_hint,
                resolved_path=resolved_path,
                skipped=True,
                reason=unsafe_reason,
                error=unsafe_reason,
            )
    source_dirs = _source_dirs(validation_result, source_dirs_hint)

    active_jobs = _active_jobs(
        db,
        download_log_id=download_log_id,
        request_id=request_id,
        failed_paths=candidates,
        source_dirs=source_dirs,
        ignore_import_job_id=ignore_import_job_id,
    )
    if active_jobs:
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_ACTIVE_JOB,
            request_id=request_id,
            entry_found=True,
            visible=True,
            raw_failed_path=raw_failed_path,
            failed_path_hint=failed_path_hint,
            resolved_path=resolved_path,
            skipped=True,
            reason="active_import_job",
        )

    lock_key = wrong_match_cleanup_lock_key(
        request_id,
        download_log_id,
        resolved_path or failed_path_hint or raw_failed_path,
    )
    with db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
        lock_key,
    ) as acquired:
        if not acquired:
            return _result(
                download_log_id,
                OUTCOME_SKIPPED_LOCKED,
                request_id=request_id,
                entry_found=True,
                visible=True,
                raw_failed_path=raw_failed_path,
                failed_path_hint=failed_path_hint,
                resolved_path=resolved_path,
                skipped=True,
                reason="cleanup_lock_unavailable",
            )
        if require_visible and _visible_wrong_match_row(db, download_log_id) is None:
            return _result(
                download_log_id,
                OUTCOME_SKIPPED_NOT_VISIBLE,
                request_id=request_id,
                entry_found=True,
                raw_failed_path=raw_failed_path,
                failed_path_hint=failed_path_hint,
                resolved_path=resolved_path,
                skipped=True,
                reason="wrong_match_not_visible",
            )
        active_jobs = _active_jobs(
            db,
            download_log_id=download_log_id,
            request_id=request_id,
            failed_paths=candidates,
            source_dirs=source_dirs,
            ignore_import_job_id=ignore_import_job_id,
        )
        if active_jobs:
            return _result(
                download_log_id,
                OUTCOME_SKIPPED_ACTIVE_JOB,
                request_id=request_id,
                entry_found=True,
                visible=True,
                raw_failed_path=raw_failed_path,
                failed_path_hint=failed_path_hint,
                resolved_path=resolved_path,
                skipped=True,
                reason="active_import_job",
            )
        cleanup = cleanup_wrong_match_source(
            db,
            download_log_id,
            failed_path_hint=resolved_path or failed_path_hint,
        )

    if not cleanup.success or cleanup.error:
        return _result(
            download_log_id,
            OUTCOME_DELETE_FAILED,
            request_id=cleanup.request_id,
            entry_found=cleanup.entry_found,
            visible=True,
            raw_failed_path=cleanup.raw_failed_path,
            failed_path_hint=cleanup.failed_path_hint,
            resolved_path=cleanup.resolved_path,
            path_missing=cleanup.path_missing,
            cleared_rows=cleanup.cleared_rows,
            reason=cleanup.error or "delete_failed",
            error=cleanup.error or "delete_failed",
        )
    return _result(
        download_log_id,
        OUTCOME_DELETED,
        success=True,
        request_id=cleanup.request_id,
        entry_found=cleanup.entry_found,
        visible=True,
        raw_failed_path=cleanup.raw_failed_path,
        failed_path_hint=cleanup.failed_path_hint,
        resolved_path=cleanup.resolved_path,
        deleted_path=cleanup.deleted_path,
        path_missing=cleanup.path_missing,
        cleared_rows=cleanup.cleared_rows,
    )


def _result(
    download_log_id: int,
    outcome: str,
    *,
    success: bool = False,
    request_id: int | None = None,
    entry_found: bool = False,
    visible: bool = False,
    raw_failed_path: str | None = None,
    failed_path_hint: str | None = None,
    resolved_path: str | None = None,
    deleted_path: str | None = None,
    path_missing: bool = False,
    cleared_rows: int = 0,
    skipped: bool = False,
    reason: str | None = None,
    error: str | None = None,
) -> WrongMatchDeleteResult:
    return WrongMatchDeleteResult(
        download_log_id=download_log_id,
        outcome=outcome,
        success=success,
        request_id=request_id,
        entry_found=entry_found,
        visible=visible,
        raw_failed_path=raw_failed_path,
        failed_path_hint=failed_path_hint,
        resolved_path=resolved_path,
        deleted_path=deleted_path,
        path_missing=path_missing,
        cleared_rows=cleared_rows,
        skipped=skipped,
        reason=reason,
        error=error,
    )


def _group_outcome(
    *,
    processed: int,
    success: bool,
    errors: int,
    skipped: int,
    remaining: int,
) -> str:
    if success:
        return GROUP_OUTCOME_DELETED if processed else GROUP_OUTCOME_EMPTY
    if errors:
        return GROUP_OUTCOME_FAILED
    if skipped or remaining:
        return GROUP_OUTCOME_PARTIAL
    return GROUP_OUTCOME_PARTIAL


def _visible_wrong_match_row(db: WrongMatchDeleteDB, download_log_id: int) -> dict[str, Any] | None:
    for row in db.get_wrong_matches():
        if row.get("download_log_id") == download_log_id:
            return row
    return None


def _remaining_visible_count(db: WrongMatchDeleteDB, request_id: int) -> int:
    return sum(1 for row in db.get_wrong_matches() if row.get("request_id") == request_id)


def _source_dirs(
    validation_result: ValidationResultEnvelope,
    source_dirs_hint: Iterable[str],
) -> tuple[str, ...]:
    dirs = [*validation_result.source_dirs]
    dirs.extend(str(path) for path in source_dirs_hint if path)
    return tuple(normalize_source_dirs(dirs))


def _path_candidates(*paths: str | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for path in paths:
        if not path:
            continue
        value = str(path)
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _resolve_first_existing(paths: Iterable[str]) -> str | None:
    for path in paths:
        resolved = resolve_failed_path(path)
        if resolved is not None:
            return resolved
    return None


def _active_jobs(
    db: WrongMatchDeleteDB,
    *,
    download_log_id: int,
    request_id: int | None,
    failed_paths: Iterable[str],
    source_dirs: Iterable[str],
    ignore_import_job_id: int | None,
) -> list[ImportJob]:
    return db.list_active_import_jobs_for_wrong_match(
        download_log_id=download_log_id,
        request_id=request_id,
        failed_paths=failed_paths,
        source_dirs=source_dirs,
        ignore_import_job_id=ignore_import_job_id,
    )
