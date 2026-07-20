"""Import-queue + preview-queue lifecycle."""
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
import psycopg2
import psycopg2.extras

from lib.import_queue import (
    IMPORT_JOB_PREVIEW_WAITING,
    ImportJob,
    validate_job_type,
    validate_payload,
    validate_preview_failure_status,
    validate_status,
)

from lib.pipeline_db._core import _PipelineDBBase


class _ImportJobsMixin(_PipelineDBBase):
    """Import-queue + preview-queue lifecycle."""


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
                WHEN status = 'queued' AND preview_status = 'would_import' THEN 0
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


    def claim_next_import_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            WITH next_job AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status IN ('evidence_ready', 'would_import')
                ORDER BY importable_at ASC NULLS LAST, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE import_jobs
            SET status = 'running',
                attempts = attempts + 1,
                worker_id = %s,
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                updated_at = NOW()
            FROM next_job
            WHERE import_jobs.id = next_job.id
            RETURNING import_jobs.*
        """, (worker_id,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


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
    ) -> ImportJob | None:
        """Atomically bind one running job to the exact Beets launch.

        This is the final authorization immediately before ``import_one.py``.
        It runs while the caller holds the release advisory lock.  The linked
        candidate-evidence row is the content-addressed source snapshot; the
        job-type-specific path predicate prevents a stale payload or request
        staging path from reaching Beets.
        """
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
              AND evidence.source_path = %s
              AND evidence.snapshot_fingerprint IS NOT NULL
              AND evidence.snapshot_fingerprint != ''
              AND (
                    (
                        job.job_type = 'automation_import'
                        AND request.status = 'downloading'
                        AND request.active_download_state IS NOT NULL
                        AND request.active_download_state->>'current_path' = %s
                    )
                    OR (
                        job.job_type = 'force_import'
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
            source_path,
            source_path,
            source_path,
            source_path,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def mark_import_job_recovery_required(
        self,
        job_id: int,
        *,
        reason: str,
    ) -> ImportJob | None:
        """Stop a launched-but-unacknowledged job for operator recovery."""
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'recovery_required',
                message = %s,
                error = %s,
                worker_id = NULL,
                heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'running'
              AND beets_launch_authorized_at IS NOT NULL
            RETURNING *
        """, (
            f"Recovery required: {reason}",
            "Automatic replay refused because Beets may have mutated the library",
            job_id,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


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

        with self._atomic():
            cur = self._execute(
                "SELECT * FROM import_jobs WHERE id = %s FOR UPDATE",
                (job_id,),
            )
            raw = cur.fetchone()
            if raw is None or raw["status"] != "recovery_required":
                self.conn.rollback()
                return None
            original = ImportJob.from_row(dict(raw))

            if resolution == "retry":
                authority_cur = self._execute("""
                    SELECT request.status,
                           request.mb_release_id,
                           request.active_download_state,
                           evidence.snapshot_fingerprint
                    FROM album_requests AS request
                    LEFT JOIN album_quality_evidence AS evidence
                      ON evidence.id = %s
                    WHERE request.id = %s
                    FOR UPDATE OF request
                """, (original.candidate_evidence_id, original.request_id))
                authority = authority_cur.fetchone()
                if (
                    authority is None
                    or authority["status"]
                    != original.beets_launch_request_status
                    or authority["mb_release_id"]
                    != original.beets_launch_release_id
                    or authority["snapshot_fingerprint"]
                    != original.beets_launch_snapshot_fingerprint
                ):
                    self.conn.rollback()
                    return None

                expected_source = None
                if original.job_type == "automation_import":
                    state = authority["active_download_state"]
                    expected_source = (
                        state.get("current_path")
                        if isinstance(state, dict)
                        else None
                    )
                elif original.job_type == "force_import":
                    expected_source = original.payload.get("failed_path")
                elif original.job_type == "youtube_import":
                    expected_source = original.payload.get("staged_path")
                else:
                    self.conn.rollback()
                    return None
                if expected_source != original.beets_launch_source_path:
                    self.conn.rollback()
                    return None

            if resolution == "retry" and original.job_type == "automation_import":
                clear = self._execute("""
                    UPDATE album_requests
                    SET active_download_state =
                            active_download_state - 'import_subprocess_started_at',
                        updated_at = NOW()
                    WHERE id = %s
                      AND status = 'downloading'
                      AND mb_release_id = %s
                      AND active_download_state IS NOT NULL
                      AND active_download_state->>'current_path' = %s
                    RETURNING id
                """, (
                    original.request_id,
                    original.beets_launch_release_id,
                    original.beets_launch_source_path,
                ))
                if clear.fetchone() is None:
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
                    psycopg2.extras.Json(original.payload),
                    f"Operator-authorized retry of recovery job {original.id}",
                    original.preview_status,
                    (
                        psycopg2.extras.Json(original.preview_result)
                        if original.preview_result is not None
                        else None
                    ),
                    original.preview_message,
                    original.preview_error,
                    original.preview_attempts,
                    original.preview_completed_at,
                    original.importable_at,
                    original.candidate_evidence_id,
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
        """Append best-effort post-commit convergence details to a job."""
        cur = self._execute("""
            UPDATE import_jobs
            SET result = COALESCE(result, '{}'::jsonb) || %s::jsonb,
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('completed', 'failed')
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


    def requeue_import_job_for_preview(
        self,
        job_id: int,
        *,
        reason: str,
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
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'queued',
                preview_status = 'waiting',
                message = %s,
                error = NULL,
                worker_id = NULL,
                started_at = NULL,
                heartbeat_at = NULL,
                preview_message = NULL,
                preview_error = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'running'
              AND beets_launch_authorized_at IS NULL
            RETURNING *
        """, (reason, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def claim_next_import_preview_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            WITH next_job AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'waiting'
                ORDER BY created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE import_jobs
            SET preview_status = 'running',
                preview_attempts = preview_attempts + 1,
                preview_worker_id = %s,
                preview_started_at = COALESCE(preview_started_at, NOW()),
                preview_heartbeat_at = NOW(),
                preview_message = NULL,
                preview_error = NULL,
                updated_at = NOW()
            FROM next_job
            WHERE import_jobs.id = next_job.id
            RETURNING import_jobs.*
        """, (worker_id,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None


    def heartbeat_import_job_preview(self, job_id: int) -> bool:
        cur = self._execute("""
            UPDATE import_jobs
            SET preview_heartbeat_at = NOW(), updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status = 'running'
            RETURNING id
        """, (job_id,))
        return cur.fetchone() is not None


    def mark_import_job_preview_importable(
        self,
        job_id: int,
        *,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET preview_status = 'evidence_ready',
                preview_result = %s,
                preview_message = %s,
                preview_error = NULL,
                preview_completed_at = NOW(),
                importable_at = COALESCE(importable_at, NOW()),
                preview_worker_id = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status IN ('waiting', 'running')
            RETURNING *
        """, (
            psycopg2.extras.Json(preview_result or {}),
            message,
            job_id,
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
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            WITH stale AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'running'
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


    def get_active_import_job_for_request(
        self,
        request_id: int,
    ) -> ImportJob | None:
        """Return the most recent active import job for this request.

        Used by the ban-source route's importer-race check (E1.3 in the
        plan). All callers consume the queue's concrete ``ImportJob`` shape.
        """
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE request_id = %s
              AND status IN ('queued', 'running', 'recovery_required')
            ORDER BY id DESC
            LIMIT 1
        """, (request_id,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None
