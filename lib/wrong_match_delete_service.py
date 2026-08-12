"""Manual Wrong Matches source deletion service."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any, Protocol, runtime_checkable

import msgspec

from lib.fs_authority import DirectoryObservation
from lib.import_queue import ImportJob
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
    wrong_match_cleanup_lock_key,
)
from lib.processing_paths import normalize_source_dirs
from lib.util import observe_failed_path
from lib.validation_envelope import (
    ValidationResultEnvelope,
    decode_validation_envelope,
)
from lib.wrong_matches import (
    WrongMatchSourceDB,
    cleanup_wrong_match_source,
    unsafe_failed_import_path_reason,
)


@runtime_checkable
class WrongMatchDeleteDB(WrongMatchSourceDB, Protocol):
    """The PipelineDB surface this service uses (#409).

    Extends ``WrongMatchSourceDB`` because the handle is forwarded into
    ``cleanup_wrong_match_source``. Satisfied structurally by ``PipelineDB``
    and ``FakePipelineDB``; parity tests live in
    ``tests/test_wrong_matches_cleanup.py``.
    """

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
OUTCOME_PATH_MISSING = "path_missing"
"""The folder was PROVEN absent, so its stale pointer was cleared.

Successful and clearing, exactly as before, but no longer reported as
``deleted``: invariant 1 of issue #1063 says ``deleted`` means the exact
authorized source folder was actually removed, and ``[39527] deleted``
over an intact 445MB folder is the headline the incident quoted.
"""
OUTCOME_DELETE_FAILED = "delete_failed"
OUTCOME_SKIPPED_ACTIVE_JOB = "skipped_active_job"
OUTCOME_SKIPPED_INVALID_ROW = "skipped_invalid_row"
OUTCOME_SKIPPED_NOT_VISIBLE = "skipped_not_visible"
OUTCOME_SKIPPED_LOCKED = "skipped_locked"
OUTCOME_SKIPPED_UNSAFE_PATH = "skipped_unsafe_path"
OUTCOME_SKIPPED_PATH_UNAVAILABLE = "skipped_path_unavailable"
"""The source could not be OBSERVED — never evidence that it is gone.

Carries neither ``deleted_path`` nor ``path_missing``, keeps the DB
pointer, and is non-success (issue #1063).
"""

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
        return msgspec.to_builtins(self)


class WrongMatchDeleteSummary(msgspec.Struct, frozen=True):
    request_id: int
    outcome: str
    success: bool
    processed: int
    deleted: int
    """Rows whose FOLDER was actually removed.

    Pointer-only clears over a proven-absent folder are counted in
    ``cleared_missing`` instead. ``deleted`` headlines the operator's
    toast, and calling a folder we never touched "deleted" is exactly the
    overclaim issue #1063 removed from the single-delete path. Gated on
    ``success`` and ``deleted_path`` alone — NOT on ``cleared_rows`` (issue
    #1086 review): a folder can really be gone while its own pointer-clear
    affected zero rows (a concurrent alias sweep already cleared it), and
    that candidate's folder fact is still "deleted", not "counted nowhere".
    ``cleared`` below reports the pointer-row count separately.
    """
    cleared_missing: int
    deleted_paths: int
    cleared: int
    unavailable: int
    """Candidates refused with :data:`OUTCOME_SKIPPED_PATH_UNAVAILABLE`.

    Split out of both ``skipped`` and ``errors`` (issue #1086 item 3): that
    outcome sets a ``WrongMatchDeleteResult`` with ``skipped=True`` AND a
    non-``None`` ``error`` (the unavailable reason doubles as the error
    text), so counting ``skipped`` and ``errors`` independently landed one
    candidate in both totals — the toast read ``deleted 1 · skipped 1 ·
    errors 1`` for two real outcomes, not three. "Unavailable" is precisely
    the fact #1084 exists to keep distinct from both "skipped" (an
    operator/policy decision) and "failed" (a genuine delete error): the
    server never learned whether the folder is even there, so lumping it
    into either bucket loses that distinction. ``skipped_unsafe_path`` sets
    the identical ``skipped=True`` + non-``None`` ``error`` shape and is a
    DIFFERENT double-count of the same pre-existing convention, but it does
    NOT join this bucket — the path there WAS positively observed and
    refused on containment grounds, which is nothing like "could not be
    observed". It is fixed by no longer also counting toward ``errors``
    (see ``errors`` below), keeping it in ``skipped`` alone.
    """
    skipped: int
    """Refused for a reason OTHER than path-unavailable.

    ``result.skipped`` is ``True`` and ``result.outcome`` is not
    ``OUTCOME_SKIPPED_PATH_UNAVAILABLE`` — active-job holds, lock
    contention, an invalid/vanished row, and (issue #1086) the unsafe-path
    refusal, which used to double-count into ``errors`` too.
    """
    errors: int
    """A genuine delete failure: never also ``skipped`` (issue #1086).

    ``skipped`` and ``errors`` are disjoint by construction — a refused
    candidate (unavailable or otherwise skipped) is not also a delete
    failure, and every candidate in ``results`` lands in exactly one of
    ``deleted`` / ``cleared_missing`` / ``unavailable`` / ``skipped`` /
    ``errors``.
    """
    remaining: int
    group_empty: bool
    results: tuple[WrongMatchDeleteResult, ...]

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


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
    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
        # ``download_log_id`` is a required, non-nullable ``download_log.id``
        # column (WrongMatchCandidateRow), so the row type already proves
        # this is an ``int`` — only the bool-subtype guard still needs a
        # runtime check.
        log_id = row["download_log_id"]
        if isinstance(log_id, bool):
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
    # The folder fact is `deleted_path`/`path_missing` alone — `cleared_rows`
    # is a SEPARATE pointer-row count (already reported via `cleared` below)
    # and gating on it here dropped a candidate whose folder really went
    # (or was proven gone) but whose own pointer-clear affected zero rows
    # (e.g. a concurrent alias sweep already cleared it): success=True,
    # cleared_rows=0, into no bucket at all. The toast then computed
    # deleted=0 and reported "Deleted nothing" over a folder that was
    # actually gone (issue #1086 review blocker 1).
    deleted = sum(
        1 for result in results
        if result.success and result.deleted_path
    )
    cleared_missing = sum(
        1 for result in results
        if result.success and result.path_missing
    )
    deleted_paths = sum(1 for result in results if result.deleted_path)
    cleared = sum(result.cleared_rows for result in results)
    # Three disjoint buckets, in that order, so every candidate lands in
    # exactly one (issue #1086 item 3): unavailable is carved out of both
    # skipped and errors FIRST, then skipped excludes it, then errors
    # excludes anything skipped (including skipped_unsafe_path, which used
    # to double-count into errors the same way path_unavailable did).
    unavailable = sum(
        1 for result in results
        if result.outcome == OUTCOME_SKIPPED_PATH_UNAVAILABLE
    )
    skipped = sum(
        1 for result in results
        if result.skipped and result.outcome != OUTCOME_SKIPPED_PATH_UNAVAILABLE
    )
    errors = sum(
        1
        for result in results
        if not result.skipped
        and (result.error or result.outcome == OUTCOME_DELETE_FAILED)
    )
    success = (
        (not results and remaining == 0)
        or (errors == 0 and skipped == 0 and unavailable == 0 and remaining == 0)
    )
    outcome = _group_outcome(
        processed=len(results),
        success=success,
        errors=errors,
    )
    return WrongMatchDeleteSummary(
        request_id=request_id,
        outcome=outcome,
        success=success,
        processed=len(results),
        deleted=deleted,
        cleared_missing=cleared_missing,
        deleted_paths=deleted_paths,
        cleared=cleared,
        unavailable=unavailable,
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
            skipped=True,
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
            skipped=True,
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
    observation = _observe_first_existing(candidates)
    if observation.indeterminate:
        # Refuse before the lock and before any pointer write: an
        # unreadable source is not a deletable one, and it is certainly
        # not an absent one (issue #1063).
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_PATH_UNAVAILABLE,
            request_id=request_id,
            entry_found=True,
            visible=True,
            raw_failed_path=raw_failed_path,
            failed_path_hint=failed_path_hint,
            skipped=True,
            reason=observation.unavailable_reason(),
            error=observation.unavailable_reason(),
        )
    resolved_path = observation.path
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

    if cleanup.path_unavailable:
        # The observation flipped under us (a mount went away, a mode
        # changed) between the preflight above and the locked cleanup.
        # Same rule at both sites: unreadable is not deleted and not gone.
        return _result(
            download_log_id,
            OUTCOME_SKIPPED_PATH_UNAVAILABLE,
            request_id=cleanup.request_id,
            entry_found=cleanup.entry_found,
            visible=True,
            raw_failed_path=cleanup.raw_failed_path,
            failed_path_hint=cleanup.failed_path_hint,
            resolved_path=cleanup.resolved_path,
            cleared_rows=cleanup.cleared_rows,
            skipped=True,
            reason=cleanup.error or "path_unavailable",
            error=cleanup.error or "path_unavailable",
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
        OUTCOME_PATH_MISSING if cleanup.path_missing else OUTCOME_DELETED,
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
) -> str:
    """Not-``success`` always means SOMETHING outstanding by construction.

    ``success`` is only ``False`` when either ``errors``, ``skipped``,
    ``unavailable``, or ``remaining`` is nonzero (see the formula above this
    function's one caller). Once ``success`` and ``errors`` are both ruled
    out, the remaining three facts are irrelevant to the return value —
    whichever of them is nonzero, the answer is the same PARTIAL outcome —
    so this function no longer takes them as parameters (issue #1086
    review: the prior two-branch shape returned the identical value on
    both paths, which is dead code, not a real distinction).
    """
    if success:
        return GROUP_OUTCOME_DELETED if processed else GROUP_OUTCOME_EMPTY
    if errors:
        return GROUP_OUTCOME_FAILED
    return GROUP_OUTCOME_PARTIAL


def _visible_wrong_match_row(
    db: WrongMatchDeleteDB, download_log_id: int,
) -> Mapping[str, Any] | None:
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


def _observe_first_existing(paths: Iterable[str]) -> DirectoryObservation:
    """First present candidate wins; otherwise a refused probe outranks absence."""
    refused: DirectoryObservation | None = None
    last = DirectoryObservation(presence="absent", code="missing")
    for path in paths:
        last = observe_failed_path(path)
        if last.present:
            return last
        if last.indeterminate and refused is None:
            refused = last
    return refused if refused is not None else last


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
