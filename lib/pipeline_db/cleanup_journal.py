"""Exact-owner processor-cleanup journal persistence.

This module owns only the durable intent/checkpoint/receipt boundary.  It does
not inspect the filesystem, derive paths, execute cleanup, or apply a terminal
request outcome.  Callers must journal every exact path and manifest before
their first filesystem mutation, then checkpoint deterministic idempotent
steps through the revision CAS below.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, TypedDict

import msgspec
import psycopg2.extras

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import _msgspec_json_dumps

CleanupResultStatus = Literal["wanted", "imported"]
CleanupReceiptOutcome = Literal["completed", "no_op"]
CleanupJournalConflictKind = Literal[
    "request_missing",
    "owner_mismatch",
    "job_mismatch",
    "journal_missing",
    "intent_conflict",
    "progress_conflict",
    "revision_conflict",
    "receipt_conflict",
    "already_completed",
    "retarget_conflict",
]

# Manifests and progress are deliberately JSON-shaped rather than prescribing a
# filesystem model here.  The persistence layer validates and round-trips every
# key/value without interpreting it; the executor owns the action-specific
# typed entries and step vocabulary.
CleanupManifest = tuple[dict[str, object], ...]
CleanupStepProgress = dict[str, object]


class CleanupJournalIntent(msgspec.Struct, frozen=True, kw_only=True):
    """Immutable cleanup plan persisted before the first filesystem effect."""

    action: str
    source_path: str
    source_manifest: CleanupManifest
    source_manifest_hash: str
    destination_path: str | None = None
    destination_manifest: CleanupManifest | None = None
    destination_manifest_hash: str | None = None
    selected_destination_path: str | None = None
    declared_result_status: CleanupResultStatus | None = None
    declared_reason: str | None = None
    evidence_revision: str | None = None


class CleanupJournalReceipt(msgspec.Struct, frozen=True, kw_only=True):
    """Typed proof that the exact journaled cleanup plan completed."""

    outcome: CleanupReceiptOutcome
    action: str
    source_path: str
    source_manifest_hash: str
    step_progress: CleanupStepProgress
    destination_path: str | None = None
    destination_manifest_hash: str | None = None
    selected_destination_path: str | None = None
    details: dict[str, object] = msgspec.field(
        default_factory=dict[str, object],
    )


class ProcessingCleanupJournalRow(TypedDict):
    """One exact ``processing_cleanup_journal`` row."""

    job_id: int
    request_id: int
    revision: int
    action: str
    source_path: str
    source_manifest: list[dict[str, object]]
    source_manifest_hash: str
    destination_path: str | None
    destination_manifest: list[dict[str, object]] | None
    destination_manifest_hash: str | None
    selected_destination_path: str | None
    step_progress: CleanupStepProgress
    declared_result_status: CleanupResultStatus | None
    declared_reason: str | None
    evidence_revision: str | None
    completed_receipt: CleanupJournalReceipt | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None


class CleanupJournalConflict(RuntimeError):
    """A journal command no longer matches its exact owner or revision."""

    def __init__(self, kind: CleanupJournalConflictKind, message: str) -> None:
        self.kind = kind
        super().__init__(message)


@dataclass(frozen=True)
class _LockedCleanupScope:
    request_status: str | None
    active_job_id: int | None
    jobs: tuple[Mapping[str, object], ...]
    journals: tuple[Mapping[str, object], ...]


def _json_clone(value: object) -> object:
    """Validate JSON compatibility and detach caller-owned mutable values."""
    return msgspec.json.decode(msgspec.json.encode(value))


def _manifest_builtins(
    manifest: CleanupManifest | list[dict[str, object]],
) -> list[dict[str, object]]:
    converted = msgspec.convert(
        _json_clone(manifest),
        type=list[dict[str, object]],
    )
    if any(not entry for entry in converted):
        raise ValueError("cleanup manifest entries must be non-empty objects")
    return converted


def _progress_builtins(
    progress: Mapping[str, object],
) -> CleanupStepProgress:
    return msgspec.convert(
        _json_clone(dict(progress)),
        type=dict[str, object],
    )


def _receipt_builtins(
    receipt: CleanupJournalReceipt,
) -> dict[str, object]:
    return msgspec.convert(
        _json_clone(msgspec.to_builtins(receipt)),
        type=dict[str, object],
    )


def _json_param(value: object) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value, dumps=_msgspec_json_dumps)


def _require_nonblank(value: str, field: str) -> None:
    if not value.strip():
        raise ValueError(f"{field} must be non-blank")


def _row_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"cleanup journal {field} is not an integer")
    return value


def _validate_intent(intent: CleanupJournalIntent) -> None:
    _require_nonblank(intent.action, "action")
    _require_nonblank(intent.source_path, "source_path")
    _require_nonblank(intent.source_manifest_hash, "source_manifest_hash")
    _manifest_builtins(intent.source_manifest)

    declaration = (
        intent.declared_result_status,
        intent.declared_reason,
        intent.evidence_revision,
    )
    if not all(value is None for value in declaration):
        if any(value is None for value in declaration):
            raise ValueError(
                "cleanup recovery declaration must be provided together"
            )
        assert intent.declared_reason is not None
        assert intent.evidence_revision is not None
        _require_nonblank(intent.declared_reason, "declared_reason")
        _require_nonblank(intent.evidence_revision, "evidence_revision")

    destination = (
        intent.destination_path,
        intent.destination_manifest,
        intent.destination_manifest_hash,
        intent.selected_destination_path,
    )
    if all(value is None for value in destination):
        return
    if any(value is None for value in destination):
        raise ValueError(
            "destination path, manifest, hash, and selected path must be "
            "provided together"
        )
    assert intent.destination_path is not None
    assert intent.destination_manifest is not None
    assert intent.destination_manifest_hash is not None
    assert intent.selected_destination_path is not None
    _require_nonblank(intent.destination_path, "destination_path")
    _manifest_builtins(intent.destination_manifest)
    _require_nonblank(
        intent.destination_manifest_hash,
        "destination_manifest_hash",
    )
    _require_nonblank(
        intent.selected_destination_path,
        "selected_destination_path",
    )


def cleanup_journal_row(
    raw: Mapping[str, object],
) -> ProcessingCleanupJournalRow:
    """Decode one cursor row at the PostgreSQL boundary."""
    row = dict(raw)
    row["source_manifest"] = _manifest_builtins(
        msgspec.convert(
            row["source_manifest"],
            type=list[dict[str, object]],
        )
    )
    destination_manifest = row["destination_manifest"]
    if destination_manifest is not None:
        row["destination_manifest"] = _manifest_builtins(
            msgspec.convert(
                destination_manifest,
                type=list[dict[str, object]],
            )
        )
    row["step_progress"] = _progress_builtins(
        msgspec.convert(
            row["step_progress"],
            type=dict[str, object],
        )
    )
    completed_receipt = row["completed_receipt"]
    if completed_receipt is not None:
        row["completed_receipt"] = msgspec.convert(
            completed_receipt,
            type=CleanupJournalReceipt,
        )
    return msgspec.convert(row, type=ProcessingCleanupJournalRow)


def _intent_matches_row(
    intent: CleanupJournalIntent,
    row: ProcessingCleanupJournalRow,
) -> bool:
    return (
        row["action"] == intent.action
        and row["source_path"] == intent.source_path
        and row["source_manifest"]
        == _manifest_builtins(intent.source_manifest)
        and row["source_manifest_hash"] == intent.source_manifest_hash
        and row["destination_path"] == intent.destination_path
        and row["destination_manifest"]
        == (
            None
            if intent.destination_manifest is None
            else _manifest_builtins(intent.destination_manifest)
        )
        and row["destination_manifest_hash"]
        == intent.destination_manifest_hash
        and row["selected_destination_path"]
        == intent.selected_destination_path
        and row["declared_result_status"]
        == intent.declared_result_status
        and row["declared_reason"] == intent.declared_reason
        and row["evidence_revision"] == intent.evidence_revision
    )


def _receipt_matches_row(
    receipt: CleanupJournalReceipt,
    row: ProcessingCleanupJournalRow,
) -> bool:
    return (
        receipt.action == row["action"]
        and receipt.source_path == row["source_path"]
        and receipt.source_manifest_hash == row["source_manifest_hash"]
        and receipt.destination_path == row["destination_path"]
        and receipt.destination_manifest_hash
        == row["destination_manifest_hash"]
        and receipt.selected_destination_path
        == row["selected_destination_path"]
        and _progress_builtins(receipt.step_progress)
        == row["step_progress"]
    )


class _CleanupJournalMixin(_PipelineDBBase):
    """Durable exact-owner cleanup intent, checkpoints, and receipts."""

    def _lock_processing_cleanup_scope(
        self,
        cur: Any,
        *,
        request_id: int,
    ) -> _LockedCleanupScope:
        """Lock request -> every request job -> every journal, in that order."""
        cur.execute(
            """
            SELECT status, active_automation_import_job_id
            FROM album_requests
            WHERE id = %s
            FOR UPDATE
            """,
            (request_id,),
        )
        request = cur.fetchone()
        cur.execute(
            """
            SELECT id, request_id, job_type, status
            FROM import_jobs
            WHERE request_id = %s
            ORDER BY id
            FOR UPDATE
            """,
            (request_id,),
        )
        jobs = tuple(cur.fetchall())
        cur.execute(
            """
            SELECT *
            FROM processing_cleanup_journal
            WHERE request_id = %s
            ORDER BY job_id
            FOR UPDATE
            """,
            (request_id,),
        )
        journals = tuple(cur.fetchall())
        return _LockedCleanupScope(
            request_status=(
                None if request is None else str(request["status"])
            ),
            active_job_id=(
                None
                if request is None
                or request["active_automation_import_job_id"] is None
                else int(request["active_automation_import_job_id"])
            ),
            jobs=jobs,
            journals=journals,
        )

    @staticmethod
    def _require_exact_processing_owner(
        scope: _LockedCleanupScope,
        *,
        request_id: int,
        job_id: int,
    ) -> None:
        if scope.request_status is None:
            raise CleanupJournalConflict(
                "request_missing",
                f"cleanup request {request_id} does not exist",
            )
        if (
            scope.request_status != "processing"
            or scope.active_job_id != job_id
        ):
            raise CleanupJournalConflict(
                "owner_mismatch",
                f"job {job_id} is not request {request_id}'s exact "
                "processing owner",
            )
        job = next(
            (
                row
                for row in scope.jobs
                if _row_int(row["id"], "job id") == job_id
            ),
            None,
        )
        if (
            job is None
            or _row_int(job["request_id"], "job request id") != request_id
            or job["job_type"] != "automation_import"
            or job["status"]
            not in ("queued", "running", "recovery_required")
        ):
            raise CleanupJournalConflict(
                "job_mismatch",
                f"job {job_id} is not an active automation job for "
                f"request {request_id}",
            )

    @staticmethod
    def _journal_from_scope(
        scope: _LockedCleanupScope,
        *,
        request_id: int,
        job_id: int,
    ) -> ProcessingCleanupJournalRow | None:
        raw = next(
            (
                row
                for row in scope.journals
                if _row_int(row["job_id"], "job id") == job_id
                and _row_int(row["request_id"], "request id") == request_id
            ),
            None,
        )
        return None if raw is None else cleanup_journal_row(raw)

    def _get_processing_cleanup_journal_locked(
        self,
        *,
        request_id: int,
        job_id: int,
        scope: _LockedCleanupScope,
    ) -> ProcessingCleanupJournalRow | None:
        """Read one exact key from a caller's ordered locked snapshot."""
        return self._journal_from_scope(
            scope,
            request_id=request_id,
            job_id=job_id,
        )

    def _create_processing_cleanup_journal_locked(
        self,
        cur: Any,
        *,
        request_id: int,
        job_id: int,
        intent: CleanupJournalIntent,
        scope: _LockedCleanupScope,
    ) -> ProcessingCleanupJournalRow:
        """Create under caller-held ordered locks; never commits."""
        _validate_intent(intent)
        self._require_exact_processing_owner(
            scope,
            request_id=request_id,
            job_id=job_id,
        )
        existing = self._get_processing_cleanup_journal_locked(
            request_id=request_id,
            job_id=job_id,
            scope=scope,
        )
        if existing is not None:
            if _intent_matches_row(intent, existing):
                return existing
            raise CleanupJournalConflict(
                "intent_conflict",
                "cleanup journal already contains a different exact intent",
            )

        cur.execute(
            """
            INSERT INTO processing_cleanup_journal (
                job_id,
                request_id,
                action,
                source_path,
                source_manifest,
                source_manifest_hash,
                destination_path,
                destination_manifest,
                destination_manifest_hash,
                selected_destination_path,
                declared_result_status,
                declared_reason,
                evidence_revision
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *
            """,
            (
                job_id,
                request_id,
                intent.action,
                intent.source_path,
                _json_param(_manifest_builtins(intent.source_manifest)),
                intent.source_manifest_hash,
                intent.destination_path,
                (
                    None
                    if intent.destination_manifest is None
                    else _json_param(
                        _manifest_builtins(intent.destination_manifest)
                    )
                ),
                intent.destination_manifest_hash,
                intent.selected_destination_path,
                intent.declared_result_status,
                intent.declared_reason,
                intent.evidence_revision,
            ),
        )
        raw = cur.fetchone()
        if raw is None:
            raise RuntimeError("cleanup journal insert returned no row")
        return cleanup_journal_row(raw)

    def create_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        intent: CleanupJournalIntent,
    ) -> ProcessingCleanupJournalRow:
        """Persist exact cleanup intent for the current processing owner."""
        with self._atomic(), self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            scope = self._lock_processing_cleanup_scope(
                cur,
                request_id=request_id,
            )
            row = self._create_processing_cleanup_journal_locked(
                cur,
                request_id=request_id,
                job_id=job_id,
                intent=intent,
                scope=scope,
            )
            self.conn.commit()
            return row

    def get_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
    ) -> ProcessingCleanupJournalRow | None:
        """Read exactly one journal key; never infer an owner from a path."""
        cur = self._execute(
            """
            SELECT *
            FROM processing_cleanup_journal
            WHERE job_id = %s AND request_id = %s
            """,
            (job_id, request_id),
        )
        raw = cur.fetchone()
        return None if raw is None else cleanup_journal_row(raw)

    def _checkpoint_processing_cleanup_journal_locked(
        self,
        cur: Any,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        step_progress: Mapping[str, object],
        scope: _LockedCleanupScope,
    ) -> ProcessingCleanupJournalRow:
        """CAS one deterministic progress snapshot; never commits."""
        if expected_revision <= 0:
            raise ValueError("expected_revision must be positive")
        self._require_exact_processing_owner(
            scope,
            request_id=request_id,
            job_id=job_id,
        )
        existing = self._get_processing_cleanup_journal_locked(
            request_id=request_id,
            job_id=job_id,
            scope=scope,
        )
        if existing is None:
            raise CleanupJournalConflict(
                "journal_missing",
                "cleanup journal does not exist for the exact owner",
            )
        progress = _progress_builtins(step_progress)
        if progress == existing["step_progress"]:
            return existing
        if existing["completed_receipt"] is not None:
            raise CleanupJournalConflict(
                "already_completed",
                "completed cleanup progress cannot change",
            )
        if existing["revision"] != expected_revision:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup checkpoint revision changed",
            )
        if any(
            key not in progress or progress[key] != value
            for key, value in existing["step_progress"].items()
        ):
            raise CleanupJournalConflict(
                "progress_conflict",
                "cleanup checkpoint cannot remove or rewrite prior progress",
            )

        cur.execute(
            """
            UPDATE processing_cleanup_journal
            SET step_progress = %s,
                revision = revision + 1,
                updated_at = NOW()
            WHERE job_id = %s
              AND request_id = %s
              AND revision = %s
              AND completed_receipt IS NULL
            RETURNING *
            """,
            (
                _json_param(progress),
                job_id,
                request_id,
                expected_revision,
            ),
        )
        raw = cur.fetchone()
        if raw is None:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup checkpoint lost its revision CAS",
            )
        return cleanup_journal_row(raw)

    def checkpoint_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        step_progress: Mapping[str, object],
    ) -> ProcessingCleanupJournalRow:
        """Checkpoint one monotonic progress snapshot under exact ownership."""
        with self._atomic(), self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            scope = self._lock_processing_cleanup_scope(
                cur,
                request_id=request_id,
            )
            row = self._checkpoint_processing_cleanup_journal_locked(
                cur,
                request_id=request_id,
                job_id=job_id,
                expected_revision=expected_revision,
                step_progress=step_progress,
                scope=scope,
            )
            self.conn.commit()
            return row

    def _complete_processing_cleanup_journal_locked(
        self,
        cur: Any,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        receipt: CleanupJournalReceipt,
        scope: _LockedCleanupScope,
    ) -> ProcessingCleanupJournalRow:
        """CAS the typed completed receipt under caller-held locks."""
        if expected_revision <= 0:
            raise ValueError("expected_revision must be positive")
        self._require_exact_processing_owner(
            scope,
            request_id=request_id,
            job_id=job_id,
        )
        existing = self._get_processing_cleanup_journal_locked(
            request_id=request_id,
            job_id=job_id,
            scope=scope,
        )
        if existing is None:
            raise CleanupJournalConflict(
                "journal_missing",
                "cleanup journal does not exist for the exact owner",
            )
        if not _receipt_matches_row(receipt, existing):
            raise CleanupJournalConflict(
                "receipt_conflict",
                "cleanup receipt does not describe the exact journaled plan",
            )
        completed = existing["completed_receipt"]
        if completed is not None:
            if completed == receipt:
                return existing
            raise CleanupJournalConflict(
                "receipt_conflict",
                "cleanup journal already has a different completed receipt",
            )
        if existing["revision"] != expected_revision:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup completion revision changed",
            )

        cur.execute(
            """
            UPDATE processing_cleanup_journal
            SET completed_receipt = %s,
                completed_at = NOW(),
                revision = revision + 1,
                updated_at = NOW()
            WHERE job_id = %s
              AND request_id = %s
              AND revision = %s
              AND completed_receipt IS NULL
            RETURNING *
            """,
            (
                _json_param(_receipt_builtins(receipt)),
                job_id,
                request_id,
                expected_revision,
            ),
        )
        raw = cur.fetchone()
        if raw is None:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup completion lost its revision CAS",
            )
        return cleanup_journal_row(raw)

    def complete_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        receipt: CleanupJournalReceipt,
    ) -> ProcessingCleanupJournalRow:
        """Persist a typed receipt; this does not claim cleanup was executed."""
        with self._atomic(), self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            scope = self._lock_processing_cleanup_scope(
                cur,
                request_id=request_id,
            )
            row = self._complete_processing_cleanup_journal_locked(
                cur,
                request_id=request_id,
                job_id=job_id,
                expected_revision=expected_revision,
                receipt=receipt,
                scope=scope,
            )
            self.conn.commit()
            return row

    def _retarget_processing_cleanup_journal_locked(
        self,
        cur: Any,
        *,
        request_id: int,
        old_job_id: int,
        new_job_id: int,
        expected_revision: int,
        scope: _LockedCleanupScope,
    ) -> ProcessingCleanupJournalRow | None:
        """Retarget during recovery's caller-owned atomic owner swap.

        The caller must already hold the ordered scope locks and must retarget
        the request owner in the same transaction.  This helper deliberately
        does not commit.  Only ``job_id`` and ``revision`` change; every
        journaled path, manifest, progress/receipt byte, and timestamp remains
        untouched.
        """
        if expected_revision <= 0:
            raise ValueError("expected_revision must be positive")
        existing = self._get_processing_cleanup_journal_locked(
            request_id=request_id,
            job_id=old_job_id,
            scope=scope,
        )
        if existing is None:
            return None
        if existing["revision"] != expected_revision:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup retarget revision changed",
            )
        if any(
            _row_int(row["job_id"], "job id") == new_job_id
            for row in scope.journals
        ):
            raise CleanupJournalConflict(
                "retarget_conflict",
                "replacement owner already has a cleanup journal",
            )
        replacement = next(
            (
                row
                for row in scope.jobs
                if _row_int(row["id"], "job id") == new_job_id
            ),
            None,
        )
        # Recovery may insert the replacement after taking the ordered locks.
        # Re-read that exact new key without deriving it from any path.
        if replacement is None:
            cur.execute(
                """
                SELECT id, request_id, job_type, status
                FROM import_jobs
                WHERE id = %s AND request_id = %s
                """,
                (new_job_id, request_id),
            )
            replacement = cur.fetchone()
        if (
            replacement is None
            or replacement["job_type"] != "automation_import"
            or replacement["status"]
            not in ("queued", "running", "recovery_required")
        ):
            raise CleanupJournalConflict(
                "retarget_conflict",
                "replacement owner is not an active automation job",
            )

        cur.execute(
            """
            UPDATE processing_cleanup_journal
            SET job_id = %s,
                revision = revision + 1
            WHERE job_id = %s
              AND request_id = %s
              AND revision = %s
            RETURNING *
            """,
            (
                new_job_id,
                old_job_id,
                request_id,
                expected_revision,
            ),
        )
        raw = cur.fetchone()
        if raw is None:
            raise CleanupJournalConflict(
                "revision_conflict",
                "cleanup retarget lost its revision CAS",
            )
        return cleanup_journal_row(raw)
