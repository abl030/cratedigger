"""Import-queue + preview-queue lifecycle."""
import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

import msgspec
import psycopg2
import psycopg2.extras

from lib import transitions
from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_PREVIEW_WAITING,
    AutomationHandoffResult,
    ForceImportPayload,
    ImportJob,
    YoutubeImportPayload,
    automation_import_dedupe_key,
    automation_import_payload,
    validate_job_type,
    validate_payload,
    validate_preview_failure_status,
    validate_status,
)
from lib.json_narrow import is_dict_like, is_str_object_dict
from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import ADVISORY_LOCK_NAMESPACE_IMPORT
from lib.pipeline_db.cleanup_journal import (
    CleanupJournalConflict,
    ProcessingCleanupJournalRow,
    _CleanupJournalMixin,
)
from lib.pipeline_db.rows import AlbumRequestRow, album_request_row
from lib.pipeline_db.terminal_outcomes import (
    ImportJobTerminalConflict,
    _TerminalOutcomesMixin,
)
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    CleanupJournalRefusalDisposition,
    ImportJobTerminal,
    cleanup_journal_refusal_disposition,
)

if TYPE_CHECKING:
    from lib.import_job_recovery_service import AutomationCompletionReceipt

logger = logging.getLogger("cratedigger")

# Every automation world failure surfaces under this exact label, so one
# Recents read tells the operator the request self-healed rather than stopped.
# Owned here because both the in-flight importer self-heal and the recovery
# re-probe below must be indistinguishable to the operator reading the log.
AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX = (
    "Automation world failure; request returned to the search pool"
)

# The attached-owner stages a proven-dead recovery can drive. ``queued`` and
# ``running`` are the two live lanes; ``recovery_required`` is a close that
# stopped mid-frame, never a resting state (CLAUDE.md invariant 11).
_RECOVERABLE_OWNER_STAGES: frozenset[tuple[str, str | None]] = frozenset({
    ("queued", "running"),
    ("running", "evidence_ready"),
})


def _recovery_stage_is_recoverable(job: ImportJob) -> bool:
    if job.job_type != IMPORT_JOB_AUTOMATION or job.completed_at is not None:
        return False
    if job.status == "recovery_required":
        return True
    return (job.status, job.preview_status) in _RECOVERABLE_OWNER_STAGES


def _job_terminal_stage(
    status: str,
) -> Literal["queued", "running", "recovery_required"]:
    if status not in ("queued", "running", "recovery_required"):
        raise ValueError(f"job status {status!r} is not a terminal-write stage")
    return status


def automation_completion_receipt(
    job: ImportJob,
) -> "AutomationCompletionReceipt | None":
    """Decode one job's persisted Beets completion receipt, if it captured one.

    A launched job with no receipt is the genuine ambiguity: nothing observed
    whether Beets finished. That distinction is the whole reason the owner
    authority treats the two cases differently, so it is decoded from the
    persisted result rather than inferred from any path or timestamp.
    """
    from lib.import_job_recovery_service import (
        AUTOMATION_COMPLETION_RESULT_KEY,
        AutomationCompletionReceipt,
        automation_completion_result_patch,
    )

    raw = (
        None
        if job.result is None
        else job.result.get(AUTOMATION_COMPLETION_RESULT_KEY)
    )
    if raw is None:
        return None
    receipt = msgspec.convert(
        raw,
        type=AutomationCompletionReceipt,
        strict=True,
    )
    automation_completion_result_patch(receipt)
    return receipt


class AutomationRecoveryCAS(msgspec.Struct, frozen=True):
    """Exact persisted snapshot compared by an automation recovery write."""

    request_id: int
    job_id: int
    job_status: str
    preview_status: str | None
    canonical_path: str
    beets_launch_authorized_at: datetime | None
    beets_launch_release_id: str | None
    beets_launch_source_path: str | None
    beets_launch_request_status: str | None
    beets_launch_snapshot_fingerprint: str | None
    execution_invocation_id: str | None
    execution_host_boot_id: str | None
    execution_systemd_unit: str | None
    execution_worker_pid: int | None
    execution_worker_start_ticks: int | None
    execution_beets_pid: int | None
    execution_beets_start_ticks: int | None
    cleanup_job_id: int | None = None
    cleanup_request_id: int | None = None
    cleanup_revision: int | None = None
    cleanup_progress: dict[str, object] | None = None

    def __post_init__(self) -> None:
        if self.request_id <= 0 or self.job_id <= 0:
            raise ValueError("automation recovery IDs must be positive")
        if not self.canonical_path.strip():
            raise ValueError(
                "automation recovery canonical path must be nonblank"
            )
        cleanup = (
            self.cleanup_job_id,
            self.cleanup_request_id,
            self.cleanup_revision,
            self.cleanup_progress,
        )
        if any(value is not None for value in cleanup) \
                and any(value is None for value in cleanup):
            raise ValueError(
                "automation recovery cleanup CAS must be complete or absent"
            )


@dataclass(frozen=True)
class AutomationRecoveryRetryApplied:
    original: ImportJob
    retry: ImportJob
    journal: ProcessingCleanupJournalRow | None


class AutomationRecoveryEvidenceChanged(RuntimeError):
    """The exact owner facts used by a recovery observation changed."""


def _lease_values(
    lease: ExecutionLeaseSnapshot | None,
) -> tuple[object, ...]:
    if lease is None:
        return (None, None, None, None, None)
    return (
        lease.invocation_id,
        lease.host_boot_id,
        lease.systemd_unit,
        lease.worker.pid,
        lease.worker.start_ticks,
    )


def _child_lease_values(
    lease: ExecutionLeaseSnapshot | None,
) -> tuple[object, object]:
    if lease is None or lease.beets is None:
        return (None, None)
    return (lease.beets.pid, lease.beets.start_ticks)


def _decision_proves_exact_lease_dead(
    decision: ExecutionLivenessDecision,
    expected: ExecutionLeaseSnapshot | None,
) -> bool:
    return (
        decision.status == "dead"
        and decision.evidence.lease == expected
    )


def _json_mapping(value: object) -> dict[str, object]:
    built = msgspec.to_builtins(value)
    return built if is_str_object_dict(built) else {}


def _recovery_cleanup_matches(
    expected: AutomationRecoveryCAS,
    journal: ProcessingCleanupJournalRow | None,
) -> bool:
    if expected.cleanup_job_id is None:
        return journal is None
    assert expected.cleanup_request_id is not None
    assert expected.cleanup_revision is not None
    assert expected.cleanup_progress is not None
    return (
        journal is not None
        and journal["job_id"] == expected.cleanup_job_id
        and journal["request_id"] == expected.cleanup_request_id
        and journal["revision"] == expected.cleanup_revision
        and _json_mapping(journal["step_progress"])
        == _json_mapping(expected.cleanup_progress)
    )


def _recovery_owner_matches(
    expected: AutomationRecoveryCAS,
    *,
    request: Mapping[str, object] | None,
    job: Mapping[str, object] | None,
) -> bool:
    if request is None or job is None:
        return False
    raw_state = request.get("active_download_state")
    state = (
        _json_mapping(raw_state)
        if is_dict_like(raw_state)
        else {}
    )
    return (
        request.get("status") == "processing"
        and request.get("active_automation_import_job_id") == expected.job_id
        and state.get("current_path") == expected.canonical_path
        and job.get("id") == expected.job_id
        and job.get("request_id") == expected.request_id
        and job.get("job_type") == IMPORT_JOB_AUTOMATION
        and job.get("status") == expected.job_status
        and job.get("preview_status") == expected.preview_status
        and job.get("completed_at") is None
        and job.get("beets_launch_authorized_at")
        == expected.beets_launch_authorized_at
        and job.get("beets_launch_release_id")
        == expected.beets_launch_release_id
        and job.get("beets_launch_source_path")
        == expected.beets_launch_source_path
        and job.get("beets_launch_request_status")
        == expected.beets_launch_request_status
        and job.get("beets_launch_snapshot_fingerprint")
        == expected.beets_launch_snapshot_fingerprint
        and job.get("execution_invocation_id")
        == expected.execution_invocation_id
        and job.get("execution_host_boot_id")
        == expected.execution_host_boot_id
        and job.get("execution_systemd_unit")
        == expected.execution_systemd_unit
        and job.get("execution_worker_pid")
        == expected.execution_worker_pid
        and job.get("execution_worker_start_ticks")
        == expected.execution_worker_start_ticks
        and job.get("execution_beets_pid")
        == expected.execution_beets_pid
        and job.get("execution_beets_start_ticks")
        == expected.execution_beets_start_ticks
    )


class _ImportJobsMixin(
    _CleanupJournalMixin,
    _TerminalOutcomesMixin,
    _PipelineDBBase,
):
    """Import-queue + preview-queue lifecycle.

    The cleanup-journal and terminal-outcome clusters are real bases rather
    than ``_core`` stubs: recovering an abandoned owner has to resume that
    owner's journaled cleanup and then commit its terminal bundle, so the
    dependency is type-checked here instead of asserted in a stub list. The
    composed ``PipelineDB`` MRO is unchanged — it already lists both after
    this mixin.
    """


    # --- import_jobs queue ---

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob:
        """Create an import job or return the active job with the same key."""
        validate_job_type(job_type)
        if job_type == IMPORT_JOB_AUTOMATION:
            raise ValueError(
                "automation_import jobs may only be created by "
                "handoff_automation_import"
            )
        payload = validate_payload(job_type, payload or {})
        cur = self._execute("""
            WITH inserted AS (
                INSERT INTO import_jobs (
                    job_type, request_id, dedupe_key, payload, message,
                    preview_status, preview_message, preview_completed_at,
                    importable_at, expected_request_status
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, NULL, NULL, NULL,
                    (SELECT status FROM album_requests WHERE id = %s)
                )
                ON CONFLICT (dedupe_key)
                    WHERE dedupe_key IS NOT NULL
                      AND status IN (
                          'queued', 'running', 'recovery_required'
                      )
                DO NOTHING
                RETURNING *
            )
            SELECT inserted.*, false AS deduped
            FROM inserted
            UNION ALL
            SELECT import_jobs.*, true AS deduped
            FROM import_jobs
            WHERE %s IS NOT NULL
              AND dedupe_key = %s
              AND status IN ('queued', 'running', 'recovery_required')
              AND NOT EXISTS (SELECT 1 FROM inserted)
            ORDER BY deduped
            LIMIT 1
        """, (
            job_type,
            request_id,
            dedupe_key,
            psycopg2.extras.Json(payload),
            message,
            IMPORT_JOB_PREVIEW_WAITING,
            request_id,
            dedupe_key,
            dedupe_key,
        ))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("import job enqueue returned no row")
        return ImportJob.from_row(dict(row), deduped=bool(row["deduped"]))


    def _automation_handoff_write_boundary(
        self,
        index: int,
        label: str,
    ) -> None:
        """Post-write fault-injection seam; production deliberately does nothing."""

    def _automation_handoff_enforce_witness(self) -> bool:
        """Production witness guard; tests mutate this seam to qualify it."""
        return True

    def _automation_handoff_before_request_lock(self) -> None:
        """Concurrency-test seam immediately before ``SELECT ... FOR UPDATE``."""


    def handoff_automation_import(
        self,
        *,
        request_id: int,
        expected_enqueued_at: str,
        canonical_path: str,
        message: str,
    ) -> AutomationHandoffResult:
        """Atomically transfer one exact download incarnation to one job.

        The per-request IMPORT advisory lock is held around the row lock and
        commit. No filesystem operation belongs in this command.
        """
        if not expected_enqueued_at:
            raise ValueError("expected_enqueued_at must be non-empty")
        if not canonical_path:
            raise ValueError("canonical_path must be non-empty")

        with self.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            request_id,
        ) as acquired:
            if not acquired:
                return AutomationHandoffResult("lock_unavailable")

            enforce_witness = self._automation_handoff_enforce_witness()
            with self._atomic():
                with self.conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor,
                ) as cur:
                    self._automation_handoff_before_request_lock()
                    cur.execute(
                        """
                        SELECT
                            id,
                            status,
                            active_automation_import_job_id,
                            active_download_state,
                            jsonb_typeof(active_download_state)
                                AS active_download_state_type,
                            active_download_state ->> 'enqueued_at'
                                AS stored_enqueued_at
                        FROM album_requests
                        WHERE id = %s
                        FOR UPDATE
                        """,
                        (request_id,),
                    )
                    request = cur.fetchone()
                    if request is None:
                        self.conn.rollback()
                        return AutomationHandoffResult("request_missing")
                    if str(request["status"]) != "downloading":
                        self.conn.rollback()
                        return AutomationHandoffResult("not_downloading")
                    if (
                        request["active_download_state"] is None
                        or request["active_download_state_type"] != "object"
                    ):
                        self.conn.rollback()
                        return AutomationHandoffResult("missing_state")
                    if (
                        enforce_witness
                        and request["stored_enqueued_at"]
                        != expected_enqueued_at
                    ):
                        self.conn.rollback()
                        return AutomationHandoffResult("witness_mismatch")
                    if request["active_automation_import_job_id"] is not None:
                        self.conn.rollback()
                        return AutomationHandoffResult("owner_conflict")

                    cur.execute(
                        """
                        SELECT id
                        FROM import_jobs
                        WHERE request_id = %s
                          AND job_type = 'automation_import'
                          AND status IN (
                              'queued', 'running', 'recovery_required'
                          )
                        LIMIT 1
                        """,
                        (request_id,),
                    )
                    if cur.fetchone() is not None:
                        self.conn.rollback()
                        return AutomationHandoffResult("owner_conflict")

                    started_at = datetime.now(UTC).isoformat()
                    cur.execute(
                        """
                        INSERT INTO import_jobs (
                            job_type,
                            request_id,
                            dedupe_key,
                            payload,
                            message,
                            preview_status,
                            expected_request_status
                        )
                        VALUES (
                            'automation_import',
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            'processing'
                        )
                        RETURNING *
                        """,
                        (
                            request_id,
                            automation_import_dedupe_key(request_id),
                            psycopg2.extras.Json(
                                automation_import_payload(),
                            ),
                            message,
                            IMPORT_JOB_PREVIEW_WAITING,
                        ),
                    )
                    job_row = cur.fetchone()
                    if job_row is None:
                        raise RuntimeError(
                            "automation handoff job insert returned no row"
                        )
                    job = ImportJob.from_row(dict(job_row))
                    self._automation_handoff_write_boundary(
                        1,
                        "import_job.inserted",
                    )

                    cur.execute(
                        """
                        UPDATE album_requests
                        SET status = 'processing',
                            active_automation_import_job_id = %s,
                            active_download_state = jsonb_set(
                                jsonb_set(
                                    active_download_state,
                                    '{current_path}',
                                    to_jsonb(%s::text),
                                    true
                                ),
                                '{processing_started_at}',
                                to_jsonb(%s::text),
                                true
                            ),
                            updated_at = NOW()
                        WHERE id = %s
                          AND status = 'downloading'
                          AND active_automation_import_job_id IS NULL
                          AND active_download_state IS NOT NULL
                          AND (
                              %s = false
                              OR active_download_state ->> 'enqueued_at' = %s
                          )
                        """,
                        (
                            job.id,
                            canonical_path,
                            started_at,
                            request_id,
                            enforce_witness,
                            expected_enqueued_at,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError(
                            "automation handoff request update lost its "
                            "locked download incarnation"
                        )
                    self._automation_handoff_write_boundary(
                        2,
                        "request.processing_owner",
                    )
                self.conn.commit()
                return AutomationHandoffResult("committed", job)


    def get_import_job(self, job_id: int) -> ImportJob | None:
        cur = self._execute(
            "SELECT * FROM import_jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def list_import_jobs(
        self,
        *,
        status: str | None = None,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        params: list[Any] = []
        clauses: list[str] = []
        if status is not None:
            validate_status(status)
            clauses.append("status = %s")
            params.append(status)
        if request_id is not None:
            clauses.append("request_id = %s")
            params.append(request_id)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(limit)
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            {where}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
        """, tuple(params))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def list_active_import_jobs(
        self,
        *,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        params: list[Any] = []
        request_filter = ""
        if request_id is not None:
            request_filter = "AND request_id = %s"
            params.append(request_id)
        params.append(limit)
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            WHERE status IN ('queued', 'running', 'recovery_required')
            {request_filter}
            ORDER BY created_at ASC, id ASC
            LIMIT %s
        """, tuple(params))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]


    def list_active_import_jobs_for_wrong_match(
        self,
        *,
        download_log_id: int,
        request_id: int | None,
        failed_paths: Iterable[str],
        source_dirs: Iterable[str],
        ignore_import_job_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        """Return active import jobs that could be using this source."""
        paths = [str(path) for path in dict.fromkeys(failed_paths) if path]
        dirs = [str(path) for path in dict.fromkeys(source_dirs) if path]
        match_clauses: list[str] = ["payload->>'download_log_id' = %s::text"]
        match_params: list[Any] = [str(int(download_log_id))]
        if paths:
            match_clauses.append("payload->>'failed_path' = ANY(%s::text[])")
            match_params.append(paths)
        if dirs:
            match_clauses.append("(payload->'source_dirs') ?| %s::text[]")
            match_params.append(dirs)

        ignore_clause = ""
        ignore_params: list[Any] = []
        if ignore_import_job_id is not None:
            ignore_clause = "AND id <> %s"
            ignore_params.append(int(ignore_import_job_id))
        params = [*ignore_params, *match_params, limit]
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            WHERE status IN ('queued', 'running', 'recovery_required')
              {ignore_clause}
              AND ({" OR ".join(match_clauses)})
            ORDER BY created_at ASC, id ASC
            LIMIT %s
        """, tuple(params))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]


    def count_import_jobs_by_status(self) -> dict[str, int]:
        cur = self._execute("""
            SELECT status, COUNT(*) AS count
            FROM import_jobs
            GROUP BY status
        """)
        return {str(row["status"]): int(row["count"]) for row in cur.fetchall()}


    def list_import_job_timeline(self, *, limit: int = 50) -> list[ImportJob]:
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE status IN ('queued', 'running', 'recovery_required')
            ORDER BY
              CASE
                WHEN status = 'queued' AND preview_status = 'evidence_ready' THEN 0
                WHEN status = 'recovery_required' THEN 0
                WHEN status = 'running' THEN 1
                WHEN status = 'queued' AND preview_status = 'running' THEN 2
                WHEN status = 'queued' AND preview_status = 'waiting' THEN 3
                ELSE 4
              END,
              CASE
                WHEN status = 'queued' THEN importable_at
              END ASC NULLS LAST,
              created_at ASC,
              id ASC
            LIMIT %s
        """, (limit,))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]


    def list_automation_import_jobs_for_startup_recovery(
        self,
    ) -> list[ImportJob]:
        """Return every exact owner whose persisted execution may need recovery.

        Recovery must not infer authority from a recent-job timeline. The
        request's explicit owner pointer selects the automation job before any
        ordering, and the query is intentionally unbounded so unrelated queue
        activity cannot hide a crashed owner.

        ``recovery_required`` is included because it is no longer a resting
        state (CLAUDE.md invariant 11): it is only ever the momentary authority
        stage a close transits, so an owner still ATTACHED there is a world
        that stopped mid-close and must be re-driven. Omitting it is how a
        crash inside that one frame became a permanent, unalerted park.
        """
        cur = self._execute("""
            SELECT job.*
            FROM album_requests AS request
            JOIN import_jobs AS job
              ON job.id = request.active_automation_import_job_id
             AND job.request_id = request.id
            WHERE request.status = 'processing'
              AND job.job_type = 'automation_import'
              AND (
                  (
                      job.status = 'queued'
                      AND job.preview_status = 'running'
                  )
                  OR (
                      job.status = 'running'
                      AND job.preview_status = 'evidence_ready'
                  )
                  OR job.status = 'recovery_required'
              )
            ORDER BY job.created_at ASC, job.id ASC
        """)
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]


    def peek_import_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]:
        """Select a bounded ordered import set without creating state."""
        if limit <= 0:
            raise ValueError("import candidate limit must be positive")
        if offset < 0:
            raise ValueError("import candidate offset cannot be negative")
        if execution_lease is not None and execution_lease.beets is not None:
            return []
        lease = _lease_values(execution_lease)
        candidate_cur = self._execute("""
            SELECT job.*
            FROM import_jobs AS job
            LEFT JOIN album_requests AS request
              ON request.id = job.request_id
            WHERE job.status = 'queued'
              AND job.preview_status = 'evidence_ready'
              AND (
                  job.job_type NOT IN (
                      'automation_import',
                      'force_import'
                  )
                  OR (
                      job.job_type = 'automation_import'
                      AND %s IS NOT NULL
                      AND request.status = 'processing'
                      AND request.active_automation_import_job_id = job.id
                  )
                  OR (
                      job.job_type = 'force_import'
                      AND request.status = job.expected_request_status
                      AND request.status NOT IN ('processing', 'replaced')
                      AND request.active_automation_import_job_id IS NULL
                  )
              )
            ORDER BY
                job.importable_at ASC NULLS LAST,
                job.created_at ASC,
                job.id ASC
            LIMIT %s
            OFFSET %s
        """, (lease[0], limit, offset))
        return [
            ImportJob.from_row(dict(candidate))
            for candidate in candidate_cur.fetchall()
        ]


    def claim_import_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        """Claim one exact non-request-scoped candidate from a bounded scan."""
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'running',
                attempts = attempts + 1,
                worker_id = %s,
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND job_type NOT IN ('automation_import', 'force_import')
              AND status = 'queued'
              AND preview_status = 'evidence_ready'
            RETURNING *
        """, (worker_id, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def claim_force_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None:
        """Claim the exact force job while the caller retains pinned IMPORT."""
        with self._atomic():
            request_cur = self._execute("""
                SELECT status
                FROM album_requests
                WHERE id = %s
                  AND status NOT IN ('processing', 'replaced')
                  AND active_automation_import_job_id IS NULL
                FOR UPDATE
            """, (request_id,))
            request = request_cur.fetchone()
            if request is None:
                self.conn.rollback()
                return None
            job_cur = self._execute("""
                SELECT id
                FROM import_jobs
                WHERE id = %s
                  AND request_id = %s
                  AND job_type = 'force_import'
                  AND status = 'queued'
                  AND preview_status = 'evidence_ready'
                  AND expected_request_status = %s
                FOR UPDATE
            """, (job_id, request_id, request["status"]))
            if job_cur.fetchone() is None:
                self.conn.rollback()
                return None
            claimed_cur = self._execute("""
                UPDATE import_jobs
                SET status = 'running',
                    attempts = attempts + 1,
                    worker_id = %s,
                    started_at = COALESCE(started_at, NOW()),
                    heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
            """, (worker_id, job_id))
            row = claimed_cur.fetchone()
            if row is None:
                self.conn.rollback()
                return None
            self.conn.commit()
            return ImportJob.from_row(dict(row))


    def claim_automation_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None:
        """Claim the exact owner while the caller retains pinned IMPORT."""
        lease = _lease_values(execution_lease)
        with self._atomic():
            request_cur = self._execute("""
                SELECT id
                FROM album_requests
                WHERE id = %s
                  AND status = 'processing'
                  AND active_automation_import_job_id = %s
                FOR UPDATE
            """, (request_id, job_id))
            if request_cur.fetchone() is None:
                self.conn.rollback()
                return None
            job_cur = self._execute("""
                SELECT id
                FROM import_jobs
                WHERE id = %s
                  AND request_id = %s
                  AND job_type = 'automation_import'
                  AND status = 'queued'
                  AND preview_status = 'evidence_ready'
                FOR UPDATE
            """, (job_id, request_id))
            if job_cur.fetchone() is None:
                self.conn.rollback()
                return None
            claimed_cur = self._execute("""
                UPDATE import_jobs
                SET status = 'running',
                    attempts = attempts + 1,
                    worker_id = %s,
                    started_at = COALESCE(started_at, NOW()),
                    heartbeat_at = NOW(),
                    execution_invocation_id = %s,
                    execution_host_boot_id = %s,
                    execution_systemd_unit = %s,
                    execution_worker_pid = %s,
                    execution_worker_start_ticks = %s,
                    execution_beets_pid = NULL,
                    execution_beets_start_ticks = NULL,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
            """, (worker_id, *lease, job_id))
            row = claimed_cur.fetchone()
            if row is None:
                self.conn.rollback()
                return None
            self.conn.commit()
            return ImportJob.from_row(dict(row))


    def heartbeat_import_job(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool:
        """Heartbeat a running importer without letting lease evidence drift."""
        lease = _lease_values(expected_execution_lease)
        child = _child_lease_values(expected_execution_lease)
        cur = self._execute("""
            UPDATE import_jobs AS job
            SET heartbeat_at = NOW(), updated_at = NOW()
            WHERE job.id = %s
              AND job.status = 'running'
              AND job.preview_status = 'evidence_ready'
              AND (
                  job.job_type <> 'automation_import'
                  OR (
                      %s IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM album_requests AS request
                          WHERE request.id = job.request_id
                            AND request.status = 'processing'
                            AND request.active_automation_import_job_id = job.id
                      )
                      AND job.execution_invocation_id = %s
                      AND job.execution_host_boot_id = %s
                      AND job.execution_systemd_unit = %s
                      AND job.execution_worker_pid = %s
                      AND job.execution_worker_start_ticks = %s
                      AND job.execution_beets_pid IS NOT DISTINCT FROM %s
                      AND job.execution_beets_start_ticks
                          IS NOT DISTINCT FROM %s
                  )
              )
            RETURNING job.id
        """, (job_id, lease[0], *lease, *child))
        return cur.fetchone() is not None


    def mark_import_job_completed(
        self,
        job_id: int,
        *,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'completed',
                result = %s,
                message = %s,
                error = NULL,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND job_type <> 'automation_import'
              AND status IN ('queued', 'running')
            RETURNING *
        """, (psycopg2.extras.Json(result or {}), message, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def authorize_import_job_launch(
        self,
        job_id: int,
        *,
        request_id: int,
        release_id: str,
        source_path: str,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None:
        """Atomically bind one running job to the exact Beets launch.

        This is the final authorization immediately before ``import_one.py``.
        It runs while the caller holds the release advisory lock.  The linked
        candidate-evidence row binds the release and content fingerprint. Its
        ``source_path`` is immutable capture provenance, never launch
        authority. The active path belongs exclusively to the job-type-specific
        predicate below; the caller and harness verify those current files
        against the evidence snapshot before Beets may mutate the library.
        """
        if (
            expected_execution_lease is not None
            and expected_execution_lease.beets is not None
        ):
            return None
        lease = _lease_values(expected_execution_lease)
        cur = self._execute("""
            UPDATE import_jobs AS job
            SET beets_launch_authorized_at = NOW(),
                beets_launch_release_id = request.mb_release_id,
                beets_launch_source_path = %s,
                beets_launch_request_status = request.status,
                beets_launch_snapshot_fingerprint = evidence.snapshot_fingerprint,
                updated_at = NOW()
            FROM album_requests AS request,
                 album_quality_evidence AS evidence
            WHERE job.id = %s
              AND job.status = 'running'
              AND job.beets_launch_authorized_at IS NULL
              AND job.request_id = %s
              AND request.id = job.request_id
              AND request.id = %s
              AND job.expected_request_status IS NOT NULL
              AND request.status = job.expected_request_status
              AND request.status != 'replaced'
              AND request.mb_release_id = %s
              AND evidence.id = job.candidate_evidence_id
              AND evidence.mb_release_id = %s
              AND evidence.snapshot_fingerprint IS NOT NULL
              AND evidence.snapshot_fingerprint != ''
              AND (
                    (
                        job.job_type = 'automation_import'
                        AND %s IS NOT NULL
                        AND request.status = 'processing'
                        AND request.active_automation_import_job_id = job.id
                        AND request.id = job.request_id
                        AND job.preview_status = 'evidence_ready'
                        AND job.execution_invocation_id = %s
                        AND job.execution_host_boot_id = %s
                        AND job.execution_systemd_unit = %s
                        AND job.execution_worker_pid = %s
                        AND job.execution_worker_start_ticks = %s
                        AND job.execution_beets_pid IS NULL
                        AND job.execution_beets_start_ticks IS NULL
                        AND request.active_download_state IS NOT NULL
                        AND request.active_download_state->>'current_path' = %s
                    )
                    OR (
                        job.job_type = 'force_import'
                        AND request.status != 'processing'
                        AND request.active_automation_import_job_id IS NULL
                        AND job.payload->>'failed_path' = %s
                    )
                    OR (
                        job.job_type = 'youtube_import'
                        AND request.status IN ('wanted', 'unsearchable')
                        AND job.payload->>'staged_path' = %s
                    )
              )
            RETURNING job.*
        """, (
            source_path,
            job_id,
            request_id,
            request_id,
            release_id,
            release_id,
            lease[0],
            *lease,
            source_path,
            source_path,
            source_path,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def record_import_job_beets_child(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
        beets_pid: int,
        beets_start_ticks: int,
    ) -> ImportJob | None:
        """Persist the exact launched child as evidence, never authority."""
        if expected_execution_lease.beets is not None:
            return None
        if beets_pid <= 0:
            raise ValueError("beets_pid must be positive")
        if beets_start_ticks < 0:
            raise ValueError("beets_start_ticks must be non-negative")
        lease = _lease_values(expected_execution_lease)
        cur = self._execute("""
            UPDATE import_jobs AS job
            SET execution_beets_pid = %s,
                execution_beets_start_ticks = %s,
                updated_at = NOW()
            FROM album_requests AS request
            WHERE job.id = %s
              AND job.job_type = 'automation_import'
              AND job.status = 'running'
              AND job.preview_status = 'evidence_ready'
              AND job.beets_launch_authorized_at IS NOT NULL
              AND job.execution_beets_pid IS NULL
              AND job.execution_beets_start_ticks IS NULL
              AND request.id = job.request_id
              AND request.status = 'processing'
              AND request.active_automation_import_job_id = job.id
              AND job.execution_invocation_id = %s
              AND job.execution_host_boot_id = %s
              AND job.execution_systemd_unit = %s
              AND job.execution_worker_pid = %s
              AND job.execution_worker_start_ticks = %s
            RETURNING job.*
        """, (
            beets_pid,
            beets_start_ticks,
            job_id,
            *lease,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def mark_import_job_recovery_required(
        self,
        job_id: int,
        *,
        reason: str,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None:
        """Stop a launched-but-unacknowledged job for operator recovery."""
        lease = _lease_values(expected_execution_lease)
        child = _child_lease_values(expected_execution_lease)
        cur = self._execute("""
            UPDATE import_jobs AS job
            SET status = 'recovery_required',
                message = %s,
                error = %s,
                worker_id = NULL,
                heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'running'
              AND beets_launch_authorized_at IS NOT NULL
              AND (
                  job.job_type <> 'automation_import'
                  OR (
                      %s IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM album_requests AS request
                          WHERE request.id = job.request_id
                            AND request.status = 'processing'
                            AND request.active_automation_import_job_id = job.id
                      )
                      AND job.preview_status = 'evidence_ready'
                      AND job.execution_invocation_id = %s
                      AND job.execution_host_boot_id = %s
                      AND job.execution_systemd_unit = %s
                      AND job.execution_worker_pid = %s
                      AND job.execution_worker_start_ticks = %s
                      AND job.execution_beets_pid IS NOT DISTINCT FROM %s
                      AND job.execution_beets_start_ticks
                          IS NOT DISTINCT FROM %s
                  )
              )
            RETURNING *
        """, (
            f"Recovery required: {reason}",
            "Automatic replay refused because Beets may have mutated the library",
            job_id,
            lease[0],
            *lease,
            *child,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def _automation_recovery_write_boundary(
        self,
        index: int,
        label: str,
    ) -> None:
        """Post-write fault-injection seam; production deliberately does nothing."""
        del index, label


    def require_automation_recovery_owner(
        self,
        expected: AutomationRecoveryCAS,
    ) -> None:
        """Fail unless the current owner still matches the probed CAS.

        Cleanup calls this before and after every filesystem boundary.  Journal
        progress is intentionally excluded because the cleanup executor itself
        advances it; the owner, stage, canonical path, launch fence, and every
        persisted execution identity remain immutable throughout that work.
        """
        cur = self._execute(
            """
            SELECT *
            FROM album_requests
            WHERE id = %s
            """,
            (expected.request_id,),
        )
        request_raw = cur.fetchone()
        cur = self._execute(
            """
            SELECT *
            FROM import_jobs
            WHERE id = %s AND request_id = %s
            """,
            (expected.job_id, expected.request_id),
        )
        job_raw = cur.fetchone()
        if not _recovery_owner_matches(
            expected,
            request=(
                None if request_raw is None else dict(request_raw)
            ),
            job=None if job_raw is None else dict(job_raw),
        ):
            raise AutomationRecoveryEvidenceChanged(
                "automation recovery owner evidence changed"
            )


    def require_automation_recovery_cas(
        self,
        expected: AutomationRecoveryCAS,
    ) -> None:
        """Lock and compare the complete owner plus cleanup observation."""
        with self._atomic(), self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            scope = self._lock_processing_cleanup_scope(
                cur,
                request_id=expected.request_id,
            )
            cur.execute(
                "SELECT * FROM album_requests WHERE id = %s",
                (expected.request_id,),
            )
            request_raw = cur.fetchone()
            cur.execute(
                """
                SELECT *
                FROM import_jobs
                WHERE id = %s AND request_id = %s
                """,
                (expected.job_id, expected.request_id),
            )
            job_raw = cur.fetchone()
            journal = self._get_processing_cleanup_journal_locked(
                request_id=expected.request_id,
                job_id=expected.job_id,
                scope=scope,
            )
            if (
                not _recovery_owner_matches(
                    expected,
                    request=(
                        None if request_raw is None else dict(request_raw)
                    ),
                    job=None if job_raw is None else dict(job_raw),
                )
                or not _recovery_cleanup_matches(expected, journal)
            ):
                self.conn.rollback()
                raise AutomationRecoveryEvidenceChanged(
                    "automation recovery evidence changed"
                )
            self.conn.commit()


    def retry_automation_import_recovery(
        self,
        expected: AutomationRecoveryCAS,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None,
        liveness: ExecutionLivenessDecision,
        reason: str,
        evidence_revision: str,
    ) -> AutomationRecoveryRetryApplied | None:
        """Atomically replace one exact dead recovery owner with a fresh job."""
        reason = reason.strip()
        if not reason:
            raise ValueError("automation recovery retry requires a reason")
        if not evidence_revision.strip():
            raise ValueError(
                "automation recovery retry requires an evidence revision"
            )
        if expected.job_status != "recovery_required":
            return None
        if not _decision_proves_exact_lease_dead(
            liveness,
            expected_execution_lease,
        ):
            return None
        if (
            _lease_values(expected_execution_lease)
            != (
                expected.execution_invocation_id,
                expected.execution_host_boot_id,
                expected.execution_systemd_unit,
                expected.execution_worker_pid,
                expected.execution_worker_start_ticks,
            )
            or _child_lease_values(expected_execution_lease)
            != (
                expected.execution_beets_pid,
                expected.execution_beets_start_ticks,
            )
        ):
            return None

        with self.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            expected.request_id,
        ) as acquired:
            if not acquired:
                return None
            with self._atomic(), self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                scope = self._lock_processing_cleanup_scope(
                    cur,
                    request_id=expected.request_id,
                )
                cur.execute(
                    """
                    SELECT *
                    FROM album_requests
                    WHERE id = %s
                    """,
                    (expected.request_id,),
                )
                request_raw = cur.fetchone()
                cur.execute(
                    """
                    SELECT *
                    FROM import_jobs
                    WHERE id = %s AND request_id = %s
                    """,
                    (expected.job_id, expected.request_id),
                )
                job_raw = cur.fetchone()
                journal = self._get_processing_cleanup_journal_locked(
                    request_id=expected.request_id,
                    job_id=expected.job_id,
                    scope=scope,
                )
                if (
                    not _recovery_owner_matches(
                        expected,
                        request=(
                            None
                            if request_raw is None
                            else dict(request_raw)
                        ),
                        job=None if job_raw is None else dict(job_raw),
                    )
                    or not _recovery_cleanup_matches(expected, journal)
                ):
                    self.conn.rollback()
                    return None
                assert job_raw is not None
                original = ImportJob.from_row(dict(job_raw))

                recovery_result = {
                    "recovery_resolution": {
                        "resolution": "retry",
                        "reason": reason,
                        "evidence_revision": evidence_revision,
                    },
                }
                cur.execute(
                    """
                    UPDATE import_jobs
                    SET status = 'failed',
                        result = COALESCE(result, '{}'::jsonb) || %s::jsonb,
                        message = %s,
                        error = %s,
                        worker_id = NULL,
                        heartbeat_at = NULL,
                        completed_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                      AND request_id = %s
                      AND status = 'recovery_required'
                    RETURNING *
                    """,
                    (
                        psycopg2.extras.Json(recovery_result),
                        f"Operator authorized a fresh retry: {reason}",
                        "Ambiguous Beets operation closed before fresh retry",
                        expected.job_id,
                        expected.request_id,
                    ),
                )
                resolved_raw = cur.fetchone()
                if resolved_raw is None:
                    self.conn.rollback()
                    return None
                self._automation_recovery_write_boundary(1, "old_job.failed")
                resolved = ImportJob.from_row(dict(resolved_raw))

                retry_status = (
                    "recovery_required"
                    if journal is not None
                    else "queued"
                )
                retry_message = (
                    "Recovery retry retargeted an unresolved processing "
                    f"cleanup journal from job {original.id}; reconcile "
                    "cleanup before replay"
                    if journal is not None
                    else (
                        "Operator-authorized retry of recovery job "
                        f"{original.id}: {reason}"
                    )
                )
                cur.execute(
                    """
                    INSERT INTO import_jobs (
                        job_type,
                        status,
                        request_id,
                        dedupe_key,
                        payload,
                        message,
                        preview_status,
                        preview_result,
                        preview_message,
                        preview_error,
                        preview_attempts,
                        preview_completed_at,
                        importable_at,
                        candidate_evidence_id,
                        expected_request_status
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    RETURNING *
                    """,
                    (
                        original.job_type,
                        retry_status,
                        original.request_id,
                        original.dedupe_key,
                        psycopg2.extras.Json(
                            msgspec.to_builtins(original.payload)
                        ),
                        retry_message,
                        original.preview_status,
                        (
                            None
                            if original.preview_result is None
                            else psycopg2.extras.Json(
                                original.preview_result
                            )
                        ),
                        original.preview_message,
                        original.preview_error,
                        original.preview_attempts,
                        original.preview_completed_at,
                        original.importable_at,
                        original.candidate_evidence_id,
                        original.expected_request_status,
                    ),
                )
                retry_raw = cur.fetchone()
                if retry_raw is None:
                    raise RuntimeError(
                        "automation recovery retry insert returned no row"
                    )
                retry = ImportJob.from_row(dict(retry_raw))
                self._automation_recovery_write_boundary(2, "retry_job.inserted")

                retargeted: ProcessingCleanupJournalRow | None = None
                if journal is not None:
                    retargeted = (
                        self._retarget_processing_cleanup_journal_locked(
                            cur,
                            request_id=expected.request_id,
                            old_job_id=expected.job_id,
                            new_job_id=retry.id,
                            expected_revision=journal["revision"],
                            scope=scope,
                        )
                    )
                    if retargeted is None:
                        raise CleanupJournalConflict(
                            "retarget_conflict",
                            "cleanup journal disappeared during retry",
                        )
                    self._automation_recovery_write_boundary(
                        3,
                        "cleanup_journal.retargeted",
                    )

                cur.execute(
                    """
                    UPDATE album_requests
                    SET active_automation_import_job_id = %s,
                        updated_at = NOW()
                    WHERE id = %s
                      AND status = 'processing'
                      AND active_automation_import_job_id = %s
                    RETURNING id
                    """,
                    (
                        retry.id,
                        expected.request_id,
                        expected.job_id,
                    ),
                )
                if cur.fetchone() is None:
                    raise AutomationRecoveryEvidenceChanged(
                        "processing owner changed during recovery retry"
                    )
                self._automation_recovery_write_boundary(
                    4,
                    "request.owner_retargeted",
                )
                self.conn.commit()
                return AutomationRecoveryRetryApplied(
                    original=resolved,
                    retry=retry,
                    journal=retargeted,
                )


    def resolve_import_job_recovery(
        self,
        job_id: int,
        *,
        resolution: str,
        reason: str,
    ) -> tuple[ImportJob, ImportJob | None] | None:
        """Resolve one recovery row, optionally creating a new operation.

        ``retry`` closes the ambiguous operation and inserts a fresh job ID;
        it never reuses the operation that may already have reached Beets.
        ``close`` records that the operator reconciled the external state and
        intentionally schedules no replay.
        """
        if resolution not in ("retry", "close"):
            raise ValueError(f"Invalid import recovery resolution: {resolution}")
        reason = reason.strip()
        if not reason:
            raise ValueError("Import recovery resolution requires a reason")

        candidate_cur = self._execute(
            "SELECT job.request_id FROM import_jobs AS job "
            "WHERE job.id = %s "
            "AND job.job_type <> 'automation_import' "
            "AND NOT EXISTS ("
            "    SELECT 1 FROM album_requests AS request "
            "    WHERE request.active_automation_import_job_id = job.id"
            ")",
            (job_id,),
        )
        candidate = candidate_cur.fetchone()
        if candidate is None:
            return None
        candidate_request_id = candidate["request_id"]
        request_id = (
            int(candidate_request_id)
            if candidate_request_id is not None
            else None
        )
        if request_id is None:
            return self._resolve_import_job_recovery_under_lock(
                job_id,
                request_id=None,
                resolution=resolution,
                reason=reason,
            )
        with self.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            request_id,
        ) as acquired:
            if not acquired:
                return None
            return self._resolve_import_job_recovery_under_lock(
                job_id,
                request_id=request_id,
                resolution=resolution,
                reason=reason,
            )


    def _resolve_import_job_recovery_under_lock(
        self,
        job_id: int,
        *,
        request_id: int | None,
        resolution: str,
        reason: str,
    ) -> tuple[ImportJob, ImportJob | None] | None:
        """Resolve after IMPORT, locking request before its recovery job."""
        with self._atomic():
            request_authority = None
            if request_id is not None:
                request_cur = self._execute("""
                    SELECT status, mb_release_id
                    FROM album_requests
                    WHERE id = %s
                    FOR UPDATE
                """, (request_id,))
                request_authority = request_cur.fetchone()
                if request_authority is None:
                    self.conn.rollback()
                    return None
            cur = self._execute(
                "SELECT * FROM import_jobs AS job "
                "WHERE job.id = %s "
                "AND job.request_id IS NOT DISTINCT FROM %s "
                "AND job.job_type <> 'automation_import' "
                "AND NOT EXISTS ("
                "    SELECT 1 FROM album_requests AS request "
                "    WHERE request.active_automation_import_job_id = job.id"
                ") "
                "FOR UPDATE",
                (job_id, request_id),
            )
            raw = cur.fetchone()
            if raw is None or raw["status"] != "recovery_required":
                self.conn.rollback()
                return None
            original = ImportJob.from_row(dict(raw))

            if resolution == "retry":
                evidence_cur = self._execute("""
                    SELECT snapshot_fingerprint
                    FROM album_quality_evidence
                    WHERE id = %s
                """, (original.candidate_evidence_id,))
                evidence = evidence_cur.fetchone()
                if (
                    request_authority is None
                    or evidence is None
                    or request_authority["status"]
                    != original.beets_launch_request_status
                    or request_authority["mb_release_id"]
                    != original.beets_launch_release_id
                    or evidence["snapshot_fingerprint"]
                    != original.beets_launch_snapshot_fingerprint
                ):
                    self.conn.rollback()
                    return None

                expected_source = None
                if original.job_type == "force_import":
                    if not isinstance(original.payload, ForceImportPayload):
                        raise AssertionError("force_import payload type mismatch")
                    expected_source = original.payload.failed_path
                elif original.job_type == "youtube_import":
                    if not isinstance(original.payload, YoutubeImportPayload):
                        raise AssertionError("youtube_import payload type mismatch")
                    expected_source = original.payload.staged_path
                else:
                    self.conn.rollback()
                    return None
                if expected_source != original.beets_launch_source_path:
                    self.conn.rollback()
                    return None

            resolution_result = {
                "recovery_resolution": {
                    "resolution": resolution,
                    "reason": reason,
                },
            }
            resolved_cur = self._execute("""
                UPDATE import_jobs
                SET status = 'failed',
                    result = COALESCE(result, '{}'::jsonb) || %s::jsonb,
                    message = %s,
                    error = %s,
                    worker_id = NULL,
                    heartbeat_at = NULL,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'recovery_required'
                RETURNING *
            """, (
                psycopg2.extras.Json(resolution_result),
                (
                    f"Operator authorized a fresh retry: {reason}"
                    if resolution == "retry"
                    else f"Operator resolved without replay: {reason}"
                ),
                (
                    "Ambiguous Beets operation closed before fresh retry"
                    if resolution == "retry"
                    else "Ambiguous Beets operation closed by operator"
                ),
                job_id,
            ))
            resolved_raw = resolved_cur.fetchone()
            if resolved_raw is None:
                self.conn.rollback()
                return None
            resolved = ImportJob.from_row(dict(resolved_raw))

            retry: ImportJob | None = None
            if resolution == "retry":
                retry_cur = self._execute("""
                    INSERT INTO import_jobs (
                        job_type,
                        request_id,
                        dedupe_key,
                        payload,
                        message,
                        preview_status,
                        preview_result,
                        preview_message,
                        preview_error,
                        preview_attempts,
                        preview_completed_at,
                        importable_at,
                        candidate_evidence_id,
                        expected_request_status
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s
                )
                    RETURNING *
                """, (
                    original.job_type,
                    original.request_id,
                    original.dedupe_key,
                    psycopg2.extras.Json(msgspec.to_builtins(original.payload)),
                    f"Operator-authorized retry of recovery job {original.id}",
                    (
                        IMPORT_JOB_PREVIEW_WAITING
                        if original.job_type == "force_import"
                        else original.preview_status
                    ),
                    (
                        None
                        if original.job_type == "force_import"
                        else (
                            psycopg2.extras.Json(original.preview_result)
                            if original.preview_result is not None
                            else None
                        )
                    ),
                    (
                        None if original.job_type == "force_import"
                        else original.preview_message
                    ),
                    (
                        None if original.job_type == "force_import"
                        else original.preview_error
                    ),
                    0 if original.job_type == "force_import" else original.preview_attempts,
                    (
                        None if original.job_type == "force_import"
                        else original.preview_completed_at
                    ),
                    (
                        None if original.job_type == "force_import"
                        else original.importable_at
                    ),
                    (
                        None if original.job_type == "force_import"
                        else original.candidate_evidence_id
                    ),
                    original.expected_request_status,
                ))
                retry_raw = retry_cur.fetchone()
                if retry_raw is None:
                    raise RuntimeError("import recovery retry insert returned no row")
                retry = ImportJob.from_row(dict(retry_raw))

            self.conn.commit()
        return resolved, retry


    def mark_import_job_failed(
        self,
        job_id: int,
        *,
        error: str,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'failed',
                result = %s,
                message = %s,
                error = %s,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND job_type <> 'automation_import'
              AND status IN ('queued', 'running')
            RETURNING *
        """, (psycopg2.extras.Json(result or {}), message, error, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def merge_import_job_result(
        self,
        job_id: int,
        patch: dict[str, object],
    ) -> ImportJob | None:
        """Enrich a detached terminal audit after post-commit convergence."""
        cur = self._execute("""
            UPDATE import_jobs
            SET result = COALESCE(result, '{}'::jsonb) || %s::jsonb,
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('completed', 'failed')
              AND NOT EXISTS (
                  SELECT 1
                  FROM album_requests AS request
                  WHERE request.active_automation_import_job_id = import_jobs.id
              )
            RETURNING *
        """, (psycopg2.extras.Json(patch), job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def recover_running_import_jobs(
        self,
        *,
        requeue_message: str,
        recovery_message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        """Recover abandoned jobs without replaying possible Beets effects."""
        cur = self._execute("""
            WITH running AS (
                SELECT id, beets_launch_authorized_at
                FROM import_jobs
                WHERE status = 'running'
                  AND job_type <> 'automation_import'
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET status = CASE
                    WHEN running.beets_launch_authorized_at IS NULL
                        THEN 'queued'
                    ELSE 'recovery_required'
                END,
                message = CASE
                    WHEN running.beets_launch_authorized_at IS NULL
                        THEN %s
                    ELSE %s
                END,
                error = CASE
                    WHEN running.beets_launch_authorized_at IS NULL
                        THEN NULL
                    ELSE 'Automatic replay refused because Beets may have mutated the library'
                END,
                worker_id = NULL,
                started_at = CASE
                    WHEN running.beets_launch_authorized_at IS NULL
                        THEN NULL
                    ELSE import_jobs.started_at
                END,
                heartbeat_at = NULL,
                updated_at = NOW()
            FROM running
            WHERE import_jobs.id = running.id
            RETURNING import_jobs.*
        """, (limit, requeue_message, recovery_message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]


    def recover_automation_import_job(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None,
        decision: ExecutionLivenessDecision,
        requeue_message: str,
        recovery_message: str,
    ) -> ImportJob | None:
        """Recover one abandoned owner after exact persisted death proof.

        An owner whose execution is proven dead never rests anywhere an
        operator has to reach it (CLAUDE.md invariant 11). There are exactly
        two outcomes:

        - **Proven replayable** — nothing was authorized at Beets and no
          cleanup was journaled, so the same job is requeued into its own lane
          with its dead lease cleared. The request keeps its owner because that
          owner is runnable again.
        - **Anything else** — the journaled cleanup is RESUMED to completion,
          then the owner is closed terminally: a ``download_log`` row records
          the world failure so it reads in Recents, the job ends ``failed``,
          and the request returns to ``wanted`` with its owner and state
          cleared so the next cycle re-derives everything. "Did Beets already
          mutate the library?" answers itself on that re-derivation — album
          present means upgrade/no-op, absent means re-import.

        LIVENESS AMBIGUITY IS NOT WORLD AMBIGUITY — do not "fix" this by
        recovering an unproven lease. Invariant 11 says an ambiguous WORLD
        (half-finished cleanup, unacknowledged Beets child) must be recorded
        and restarted, and that is what the close above does. An unproven
        EXECUTION is a different thing: it is an unfinished observation of one
        of our own processes, and treating it as dead would steal a live
        worker's request mid-flight, which is the one failure no later cycle
        can repair. Such a row is not parked either — nothing about it needs a
        human, and the periodic re-probe converges it as soon as the lease is
        provable. So the gate below stays exact: status ``dead`` AND evidence
        about this exact lease, or nothing happens at all.

        ``expected_execution_lease`` is ``None`` for an owner no execution ever
        claimed — the leaseless replacement an operator retry mints when it
        retargets an unresolved cleanup journal. ``never_claimed`` is a real
        death proof, not a weakening of one, so such an owner converges on a
        re-probe instead of resting where only an operator can reach it.
        """
        if not _decision_proves_exact_lease_dead(
            decision,
            expected_execution_lease,
        ):
            return None
        request_cur = self._execute(
            "SELECT request_id FROM import_jobs WHERE id = %s",
            (job_id,),
        )
        candidate = request_cur.fetchone()
        if candidate is None or candidate["request_id"] is None:
            return None
        request_id = int(candidate["request_id"])

        with self.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            request_id,
        ) as acquired:
            if not acquired:
                return None
            # This branch read is deliberately outside a transaction: the close
            # below must commit its cleanup checkpoints as it goes, so it
            # cannot run inside one. Safety comes from the exact-owner CAS on
            # both write paths, not from this snapshot — a journal or launch
            # fence appearing after the read makes the requeue's own predicate
            # miss and return None rather than write the wrong outcome.
            job = self.get_import_job(job_id)
            if job is None or not _recovery_stage_is_recoverable(job):
                return None
            journalled = self.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
            ) is not None
            if (
                job.beets_launch_authorized_at is None
                and job.status != "recovery_required"
                and not journalled
            ):
                return self._requeue_proven_unstarted_automation_owner(
                    job_id,
                    request_id=request_id,
                    expected_execution_lease=expected_execution_lease,
                    requeue_message=requeue_message,
                )
            return self._close_abandoned_automation_owner(
                job,
                request_id=request_id,
                expected_execution_lease=expected_execution_lease,
                reason=recovery_message,
            )


    def _requeue_proven_unstarted_automation_owner(
        self,
        job_id: int,
        *,
        request_id: int,
        expected_execution_lease: ExecutionLeaseSnapshot | None,
        requeue_message: str,
    ) -> ImportJob | None:
        """Return one provably unstarted owner to its own lane, owner intact.

        Nothing was authorized at Beets and nothing was journaled, so there is
        no world to surface: the exact same job becomes claimable again with
        its dead lease cleared. The request stays ``processing`` because its
        owner is runnable, not because anything is waiting for a human.
        """
        lease = _lease_values(expected_execution_lease)
        child = _child_lease_values(expected_execution_lease)
        with self._atomic():
            request_lock = self._execute("""
                SELECT id
                FROM album_requests
                WHERE id = %s
                  AND status = 'processing'
                  AND active_automation_import_job_id = %s
                FOR UPDATE
            """, (request_id, job_id))
            if request_lock.fetchone() is None:
                self.conn.rollback()
                return None
            cur = self._execute("""
                UPDATE import_jobs
                SET status = 'queued',
                    preview_status = CASE
                        WHEN status = 'queued' AND preview_status = 'running'
                            THEN 'waiting'
                        ELSE preview_status
                    END,
                    message = %(requeue_message)s,
                    error = NULL,
                    worker_id = NULL,
                    started_at = NULL,
                    heartbeat_at = NULL,
                    preview_worker_id = NULL,
                    preview_started_at = NULL,
                    preview_heartbeat_at = NULL,
                    execution_invocation_id = NULL,
                    execution_host_boot_id = NULL,
                    execution_systemd_unit = NULL,
                    execution_worker_pid = NULL,
                    execution_worker_start_ticks = NULL,
                    execution_beets_pid = NULL,
                    execution_beets_start_ticks = NULL,
                    updated_at = NOW()
                WHERE id = %(job_id)s
                  AND request_id = %(request_id)s
                  AND job_type = 'automation_import'
                  AND beets_launch_authorized_at IS NULL
                  AND (
                      (status = 'queued' AND preview_status = 'running')
                      OR (
                          status = 'running'
                          AND preview_status = 'evidence_ready'
                      )
                  )
                  AND execution_invocation_id = %(invocation_id)s
                  AND execution_host_boot_id = %(host_boot_id)s
                  AND execution_systemd_unit = %(systemd_unit)s
                  AND execution_worker_pid = %(worker_pid)s
                  AND execution_worker_start_ticks = %(worker_start_ticks)s
                  AND execution_beets_pid
                      IS NOT DISTINCT FROM %(beets_pid)s
                  AND execution_beets_start_ticks
                      IS NOT DISTINCT FROM %(beets_start_ticks)s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM processing_cleanup_journal AS journal
                      WHERE journal.job_id = %(job_id)s
                        AND journal.request_id = %(request_id)s
                  )
                RETURNING *
            """, {
                "requeue_message": requeue_message,
                "job_id": job_id,
                "request_id": request_id,
                "invocation_id": lease[0],
                "host_boot_id": lease[1],
                "systemd_unit": lease[2],
                "worker_pid": lease[3],
                "worker_start_ticks": lease[4],
                "beets_pid": child[0],
                "beets_start_ticks": child[1],
            })
            row = cur.fetchone()
            if row is None:
                self.conn.rollback()
                return None
            self.conn.commit()
            return ImportJob.from_row(dict(row))


    def _close_abandoned_automation_owner(
        self,
        job: ImportJob,
        *,
        request_id: int,
        expected_execution_lease: ExecutionLeaseSnapshot | None,
        reason: str,
    ) -> ImportJob | None:
        """Close a dead owner into ``wanted`` without inventing cleanup proof.

        Cleanup runs FIRST and while the dead owner is still attached, because
        the terminal bundle consumes the receipt it produces and the deferred
        066 triggers refuse to release a request that still has a journal. The
        journal is resumed, never re-planned: its pre-checkpoints are the only
        record of which filesystem mutations already happened.

        A typed cleanup refusal is terminal evidence, not a reason to park or
        to mutate the remaining tree by some broader fallback. The owner-atomic
        bundle stores the refusal plus the exact incomplete journal snapshot,
        consumes that matched journal, fails the job, and releases the request
        to ``wanted``. A receipt is never fabricated. Ownership/CAS conflicts
        still leave every database write rolled back for a later re-probe.
        """
        from lib.download import _local_completion_terminal_outcome
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.processing_cleanup import (
            ProcessingCleanupError,
            complete_owner_processing_cleanup,
        )
        from lib.quality.download_state import ActiveDownloadState

        detail = f"{AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX}: {reason}"
        try:
            row = self._recovery_request_row(request_id)
            raw_state = None if row is None else row.get("active_download_state")
            if row is None or raw_state is None:
                return None
            state = ActiveDownloadState.from_raw(raw_state)
            if state.current_path is None:
                return None
            canonical_path = os.path.abspath(state.current_path)
            cas = self._automation_recovery_owner_cas(
                job,
                request_id=request_id,
                canonical_path=canonical_path,
            )

            def checkpoint() -> None:
                self.require_automation_recovery_owner(cas)

            cleanup_refusal: CleanupJournalRefusalDisposition | None = None
            try:
                cleanup_receipt = complete_owner_processing_cleanup(
                    self,
                    request_id=request_id,
                    job_id=job.id,
                    source_path=canonical_path,
                    owner_checkpoint=checkpoint,
                )
            except ProcessingCleanupError as exc:
                journal = self.get_processing_cleanup_journal(
                    request_id=request_id,
                    job_id=job.id,
                )
                cleanup_receipt = None
                cleanup_refusal = cleanup_journal_refusal_disposition(
                    journal,
                    error_code=exc.code,
                    error_message=str(exc),
                )
                detail = (
                    f"{detail}; processor cleanup refused "
                    f"({exc.code}): {exc}"
                )
            completion_receipt = automation_completion_receipt(job)
            expected_job_status: Literal["queued", "running", "recovery_required"]
            expected_job_status = _job_terminal_stage(job.status)
            if (
                job.beets_launch_authorized_at is not None
                and completion_receipt is None
                and expected_job_status != "recovery_required"
            ):
                # The owner authority refuses a ``running`` terminal write for
                # a launched job that never captured its completion receipt --
                # and that missing receipt IS the ambiguity being closed.
                # ``recovery_required`` is the one stage the CAS accepts
                # without it, so the ambiguity transits that stage and is
                # closed in this same frame. It never RESTS there: a crash
                # inside this window leaves the owner ATTACHED at
                # ``recovery_required``, which the recovery query deliberately
                # selects so the next pass finishes the close.
                staged = self.mark_import_job_recovery_required(
                    job.id,
                    reason=reason,
                    expected_execution_lease=expected_execution_lease,
                )
                if staged is None:
                    return None
                expected_job_status = "recovery_required"
            pending = _local_completion_terminal_outcome(
                reconstruct_grab_list_entry(row, state),
                state,
                request_id=request_id,
                import_job_id=job.id,
                transition=transitions.RequestTransition.to_wanted(
                    from_status="processing",
                    attempt_type="validation",
                ),
                outcome="failed",
                detail=detail,
                error_message=detail,
            )
            terminal = self.persist_import_terminal_outcome(
                replace(
                    pending,
                    automation=AutomationTerminalAuthority(
                        expected_job_status=expected_job_status,
                        expected_preview_status=job.preview_status,
                        expected_execution_lease=expected_execution_lease,
                        cleanup_receipt=cleanup_receipt,
                        completion_receipt=completion_receipt,
                        cleanup_refusal=cleanup_refusal,
                    ),
                ).with_job(ImportJobTerminal(
                    status="failed",
                    error=detail,
                    result={"automation_recovery_self_heal": {"reason": reason}},
                    message=detail,
                ))
            )
        except (
            AutomationRecoveryEvidenceChanged,
            CleanupJournalConflict,
            ImportJobTerminalConflict,
        ):
            logger.exception(
                "Abandoned automation owner %s could not be closed yet (%s); "
                "leaving it for the next liveness re-probe",
                job.id,
                reason,
            )
            return None
        logger.warning(
            "Recovery returned request %s to the search pool after abandoned "
            "import job %s: %s",
            request_id,
            job.id,
            reason,
        )
        return terminal.job


    def _recovery_request_row(
        self,
        request_id: int,
    ) -> AlbumRequestRow | None:
        """Read one request row for a recovery bundle, own-cluster only."""
        cur = self._execute(
            "SELECT * FROM album_requests WHERE id = %s",
            (request_id,),
        )
        row = cur.fetchone()
        return album_request_row(row) if row is not None else None


    def _automation_recovery_owner_cas(
        self,
        job: ImportJob,
        *,
        request_id: int,
        canonical_path: str,
    ) -> AutomationRecoveryCAS:
        """Freeze the exact owner facts a cleanup resume must not outlive."""
        return AutomationRecoveryCAS(
            request_id=request_id,
            job_id=job.id,
            job_status=job.status,
            preview_status=job.preview_status,
            canonical_path=canonical_path,
            beets_launch_authorized_at=job.beets_launch_authorized_at,
            beets_launch_release_id=job.beets_launch_release_id,
            beets_launch_source_path=job.beets_launch_source_path,
            beets_launch_request_status=job.beets_launch_request_status,
            beets_launch_snapshot_fingerprint=(
                job.beets_launch_snapshot_fingerprint
            ),
            execution_invocation_id=job.execution_invocation_id,
            execution_host_boot_id=job.execution_host_boot_id,
            execution_systemd_unit=job.execution_systemd_unit,
            execution_worker_pid=job.execution_worker_pid,
            execution_worker_start_ticks=job.execution_worker_start_ticks,
            execution_beets_pid=job.execution_beets_pid,
            execution_beets_start_ticks=job.execution_beets_start_ticks,
        )


    def requeue_import_job_for_preview(
        self,
        job_id: int,
        *,
        reason: str,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None:
        """Flip a running import job back to preview's lane.

        Used by the importer's dispatch path when candidate evidence is
        missing, stale, or incomplete at claim time. Preview will pick up
        the row on its next sweep, measure, persist evidence, and mark it
        importable again.

        Column semantics (modeled on pre-launch running-job recovery):
        - ``status`` → ``queued``
        - ``preview_status`` → ``waiting``
        - ``worker_id`` / ``started_at`` / ``heartbeat_at`` → ``NULL``
        - ``preview_message`` / ``preview_error`` → ``NULL`` so preview's
          claim starts clean
        - ``message`` → ``reason`` (top-level diagnostic)
        - ``attempts`` and ``preview_attempts`` preserved (historical
          counters; the cycle is operator-visible via these)

        Idempotent: only matches rows currently in ``status='running'``.
        Returns ``None`` if the job is not running (already requeued,
        completed, failed, or non-existent).
        """
        lease = _lease_values(expected_execution_lease)
        child = _child_lease_values(expected_execution_lease)
        cur = self._execute("""
            UPDATE import_jobs AS job
            SET status = 'queued',
                preview_status = 'waiting',
                message = %s,
                error = NULL,
                worker_id = NULL,
                started_at = NULL,
                heartbeat_at = NULL,
                preview_message = NULL,
                preview_error = NULL,
                execution_invocation_id = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_invocation_id END,
                execution_host_boot_id = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_host_boot_id END,
                execution_systemd_unit = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_systemd_unit END,
                execution_worker_pid = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_worker_pid END,
                execution_worker_start_ticks = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_worker_start_ticks END,
                execution_beets_pid = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_beets_pid END,
                execution_beets_start_ticks = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_beets_start_ticks END,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'running'
              AND beets_launch_authorized_at IS NULL
              AND (
                  job.job_type <> 'automation_import'
                  OR (
                      %s IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM album_requests AS request
                          WHERE request.id = job.request_id
                            AND request.status = 'processing'
                            AND request.active_automation_import_job_id = job.id
                      )
                      AND job.preview_status = 'evidence_ready'
                      AND job.execution_invocation_id = %s
                      AND job.execution_host_boot_id = %s
                      AND job.execution_systemd_unit = %s
                      AND job.execution_worker_pid = %s
                      AND job.execution_worker_start_ticks = %s
                      AND job.execution_beets_pid IS NOT DISTINCT FROM %s
                      AND job.execution_beets_start_ticks
                          IS NOT DISTINCT FROM %s
                  )
              )
            RETURNING *
        """, (
            reason,
            job_id,
            lease[0],
            *lease,
            *child,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def peek_import_preview_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]:
        """Select a bounded ordered preview set without creating state."""
        if limit <= 0:
            raise ValueError("preview candidate limit must be positive")
        if offset < 0:
            raise ValueError("preview candidate offset cannot be negative")
        if execution_lease is not None and execution_lease.beets is not None:
            return []
        lease = _lease_values(execution_lease)
        candidate_cur = self._execute("""
            SELECT job.*
            FROM import_jobs AS job
            LEFT JOIN album_requests AS request
              ON request.id = job.request_id
            WHERE job.status = 'queued'
              AND job.preview_status = 'waiting'
              AND (
                  job.job_type NOT IN (
                      'automation_import',
                      'force_import'
                  )
                  OR (
                      job.job_type = 'automation_import'
                      AND %s IS NOT NULL
                      AND request.status = 'processing'
                      AND request.active_automation_import_job_id = job.id
                  )
                  OR (
                      job.job_type = 'force_import'
                      AND request.status = job.expected_request_status
                      AND request.status NOT IN ('processing', 'replaced')
                      AND request.active_automation_import_job_id IS NULL
                  )
            )
            ORDER BY job.created_at ASC, job.id ASC
            LIMIT %s
            OFFSET %s
        """, (lease[0], limit, offset))
        return [
            ImportJob.from_row(dict(candidate))
            for candidate in candidate_cur.fetchall()
        ]


    def claim_import_preview_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        """Claim one exact non-request-scoped candidate from a bounded scan."""
        cur = self._execute("""
            UPDATE import_jobs
            SET preview_status = 'running',
                preview_attempts = preview_attempts + 1,
                preview_worker_id = %s,
                preview_started_at = COALESCE(preview_started_at, NOW()),
                preview_heartbeat_at = NOW(),
                preview_message = NULL,
                preview_error = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND job_type NOT IN ('automation_import', 'force_import')
              AND status = 'queued'
              AND preview_status = 'waiting'
            RETURNING *
        """, (worker_id, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def claim_force_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None:
        """Claim the exact force preview while caller retains pinned IMPORT."""
        with self._atomic():
            request_cur = self._execute("""
                SELECT status
                FROM album_requests
                WHERE id = %s
                  AND status NOT IN ('processing', 'replaced')
                  AND active_automation_import_job_id IS NULL
                FOR UPDATE
            """, (request_id,))
            request = request_cur.fetchone()
            if request is None:
                self.conn.rollback()
                return None
            job_cur = self._execute("""
                SELECT id
                FROM import_jobs
                WHERE id = %s
                  AND request_id = %s
                  AND job_type = 'force_import'
                  AND status = 'queued'
                  AND preview_status = 'waiting'
                  AND expected_request_status = %s
                FOR UPDATE
            """, (job_id, request_id, request["status"]))
            if job_cur.fetchone() is None:
                self.conn.rollback()
                return None
            claimed_cur = self._execute("""
                UPDATE import_jobs
                SET preview_status = 'running',
                    preview_attempts = preview_attempts + 1,
                    preview_worker_id = %s,
                    preview_started_at = COALESCE(
                        preview_started_at,
                        NOW()
                    ),
                    preview_heartbeat_at = NOW(),
                    preview_message = NULL,
                    preview_error = NULL,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
            """, (worker_id, job_id))
            row = claimed_cur.fetchone()
            if row is None:
                self.conn.rollback()
                return None
            self.conn.commit()
            return ImportJob.from_row(dict(row))


    def claim_automation_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None:
        """Claim the exact preview while the caller retains pinned IMPORT."""
        lease = _lease_values(execution_lease)
        with self._atomic():
            request_cur = self._execute("""
                SELECT id
                FROM album_requests
                WHERE id = %s
                  AND status = 'processing'
                  AND active_automation_import_job_id = %s
                FOR UPDATE
            """, (request_id, job_id))
            if request_cur.fetchone() is None:
                self.conn.rollback()
                return None
            job_cur = self._execute("""
                SELECT id
                FROM import_jobs
                WHERE id = %s
                  AND request_id = %s
                  AND job_type = 'automation_import'
                  AND status = 'queued'
                  AND preview_status = 'waiting'
                FOR UPDATE
            """, (job_id, request_id))
            if job_cur.fetchone() is None:
                self.conn.rollback()
                return None
            claimed_cur = self._execute("""
                UPDATE import_jobs
                SET preview_status = 'running',
                    preview_attempts = preview_attempts + 1,
                    preview_worker_id = %s,
                    preview_started_at = COALESCE(
                        preview_started_at,
                        NOW()
                    ),
                    preview_heartbeat_at = NOW(),
                    preview_message = NULL,
                    preview_error = NULL,
                    execution_invocation_id = %s,
                    execution_host_boot_id = %s,
                    execution_systemd_unit = %s,
                    execution_worker_pid = %s,
                    execution_worker_start_ticks = %s,
                    execution_beets_pid = NULL,
                    execution_beets_start_ticks = NULL,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
            """, (worker_id, *lease, job_id))
            row = claimed_cur.fetchone()
            if row is None:
                self.conn.rollback()
                return None
            self.conn.commit()
            return ImportJob.from_row(dict(row))


    def heartbeat_import_job_preview(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool:
        if (
            expected_execution_lease is not None
            and expected_execution_lease.beets is not None
        ):
            return False
        lease = _lease_values(expected_execution_lease)
        cur = self._execute("""
            UPDATE import_jobs AS job
            SET preview_heartbeat_at = NOW(), updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status = 'running'
              AND (
                  job.job_type <> 'automation_import'
                  OR (
                      %s IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM album_requests AS request
                          WHERE request.id = job.request_id
                            AND request.status = 'processing'
                            AND request.active_automation_import_job_id = job.id
                      )
                      AND job.execution_invocation_id = %s
                      AND job.execution_host_boot_id = %s
                      AND job.execution_systemd_unit = %s
                      AND job.execution_worker_pid = %s
                      AND job.execution_worker_start_ticks = %s
                      AND job.execution_beets_pid IS NULL
                      AND job.execution_beets_start_ticks IS NULL
                  )
              )
            RETURNING id
        """, (job_id, lease[0], *lease))
        return cur.fetchone() is not None


    def mark_import_job_preview_importable(
        self,
        job_id: int,
        *,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None:
        if (
            expected_execution_lease is not None
            and expected_execution_lease.beets is not None
        ):
            return None
        lease = _lease_values(expected_execution_lease)
        cur = self._execute("""
            UPDATE import_jobs AS job
            SET preview_status = 'evidence_ready',
                preview_result = %s,
                preview_message = %s,
                preview_error = NULL,
                preview_completed_at = NOW(),
                importable_at = COALESCE(importable_at, NOW()),
                preview_worker_id = NULL,
                preview_heartbeat_at = NULL,
                execution_invocation_id = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_invocation_id END,
                execution_host_boot_id = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_host_boot_id END,
                execution_systemd_unit = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_systemd_unit END,
                execution_worker_pid = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_worker_pid END,
                execution_worker_start_ticks = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_worker_start_ticks END,
                execution_beets_pid = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_beets_pid END,
                execution_beets_start_ticks = CASE
                    WHEN job.job_type = 'automation_import' THEN NULL
                    ELSE job.execution_beets_start_ticks END,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND (
                  (
                      job.job_type <> 'automation_import'
                      AND preview_status IN ('waiting', 'running')
                  )
                  OR (
                      job.job_type = 'automation_import'
                      AND preview_status = 'running'
                      AND %s IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM album_requests AS request
                          WHERE request.id = job.request_id
                            AND request.status = 'processing'
                            AND request.active_automation_import_job_id = job.id
                      )
                      AND job.execution_invocation_id = %s
                      AND job.execution_host_boot_id = %s
                      AND job.execution_systemd_unit = %s
                      AND job.execution_worker_pid = %s
                      AND job.execution_worker_start_ticks = %s
                      AND job.execution_beets_pid IS NULL
                      AND job.execution_beets_start_ticks IS NULL
                  )
              )
            RETURNING *
        """, (
            psycopg2.extras.Json(preview_result or {}),
            message,
            job_id,
            lease[0],
            *lease,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def mark_import_job_preview_failed(
        self,
        job_id: int,
        *,
        preview_status: str,
        error: str,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        validate_preview_failure_status(preview_status)
        result = dict(preview_result or {})
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'failed',
                preview_status = %s,
                preview_result = %s,
                preview_message = %s,
                preview_error = %s,
                result = %s,
                message = %s,
                error = %s,
                preview_completed_at = NOW(),
                completed_at = NOW(),
                preview_worker_id = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND job_type <> 'automation_import'
              AND status = 'queued'
              AND preview_status IN ('waiting', 'running')
            RETURNING *
        """, (
            preview_status,
            psycopg2.extras.Json(result),
            message,
            error,
            psycopg2.extras.Json({"preview": result}),
            message,
            error,
            job_id,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def requeue_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(UTC) - older_than
        cur = self._execute("""
            WITH stale AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'running'
                  AND job_type <> 'automation_import'
                  AND COALESCE(preview_heartbeat_at, preview_started_at, updated_at) < %s
                ORDER BY updated_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE import_jobs
            SET preview_status = 'waiting',
                preview_message = %s,
                preview_error = NULL,
                preview_worker_id = NULL,
                preview_started_at = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            FROM stale
            WHERE import_jobs.id = stale.id
              AND import_jobs.status = 'queued'
              AND import_jobs.preview_status = 'running'
            RETURNING import_jobs.*
        """, (cutoff, limit, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]


    def requeue_running_import_preview_jobs(
        self,
        *,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        """Reset every running preview job to ``waiting`` for immediate retry.

        Mirrors import-job startup recovery for the preview lane.
        Called at preview-worker startup: the previous worker process is
        dead by definition (systemd has just spawned this one), so any
        ``preview_status='running'`` row is owned by a ghost worker and
        must be released immediately — no heartbeat-age threshold. The
        periodic ``requeue_stale_import_preview_jobs`` sweep retains the
        15-minute window for jobs that get orphaned while a worker is
        otherwise alive.
        """
        cur = self._execute("""
            WITH running AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'running'
                  AND job_type <> 'automation_import'
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET preview_status = 'waiting',
                preview_message = %s,
                preview_error = NULL,
                preview_worker_id = NULL,
                preview_started_at = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            FROM running
            WHERE import_jobs.id = running.id
            RETURNING import_jobs.*
        """, (limit, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]
