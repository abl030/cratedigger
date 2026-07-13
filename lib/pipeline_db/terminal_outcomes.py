"""DB-owned transactions for terminal import and preview outcomes."""

from __future__ import annotations

from datetime import datetime, timezone

import msgspec

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import BACKOFF_BASE_MINUTES, BACKOFF_MAX_MINUTES
from lib.terminal_outcomes import (
    DenylistWrite,
    DownloadAuditWrite,
    ImportSuccessOutcome,
    ImportJobOutcomeSupplement,
    ImportedRequestWrite,
    ImporterRejectionOutcome,
    PreviewMeasurementFailureOutcome,
    TerminalOutcomeApplied,
    TerminalOutcomeBoundary,
    TerminalOutcomeConflict,
    canonicalize_download_audit,
)
from lib.transitions import validate_transition


class _TerminalOutcomesMixin(_PipelineDBBase):
    """Own the one-transaction persistence boundary for terminal outcomes."""

    def _terminal_outcome_boundary(
        self,
        boundary: TerminalOutcomeBoundary,
    ) -> None:
        """No-op production hook overridden by rollback-qualification tests."""
        del boundary

    def _lock_terminal_request(self, request_id: int) -> str:
        row = self._execute(
            "SELECT status FROM album_requests WHERE id = %s FOR UPDATE",
            (request_id,),
        ).fetchone()
        if row is None:
            raise TerminalOutcomeConflict(f"request {request_id} not found")
        status = str(row["status"])
        if status == "replaced":
            raise TerminalOutcomeConflict(
                f"request {request_id} is replaced and immutable"
            )
        return status

    def _lock_terminal_import_job(
        self,
        *,
        import_job_id: int,
        request_id: int,
        preview: bool,
    ) -> None:
        row = self._execute(
            "SELECT request_id, status, preview_status FROM import_jobs "
            "WHERE id = %s FOR UPDATE",
            (import_job_id,),
        ).fetchone()
        if row is None:
            raise TerminalOutcomeConflict(
                f"import job {import_job_id} not found"
            )
        row_request_id = row["request_id"]
        if row_request_id is None or int(row_request_id) != request_id:
            raise TerminalOutcomeConflict(
                f"import job {import_job_id} does not belong to request {request_id}"
            )
        status = str(row["status"])
        preview_status = str(row["preview_status"] or "")
        if preview:
            if status != "queued" or preview_status not in ("waiting", "running"):
                raise TerminalOutcomeConflict(
                    f"preview job {import_job_id} is no longer active"
                )
        elif status not in ("queued", "running"):
            raise TerminalOutcomeConflict(
                f"import job {import_job_id} is no longer active"
            )

    def _insert_terminal_download_audit(
        self,
        request_id: int,
        audit: DownloadAuditWrite,
    ) -> int:
        audit = canonicalize_download_audit(audit)
        cur = self._execute(
            """
            INSERT INTO download_log (
                request_id, soulseek_username, filetype, download_path,
                beets_distance, beets_scenario, beets_detail, valid,
                outcome, staged_path, error_message,
                bitrate, sample_rate, bit_depth, is_vbr,
                was_converted, original_filetype, slskd_filetype,
                actual_filetype, actual_min_bitrate,
                spectral_grade, spectral_bitrate,
                existing_min_bitrate, existing_spectral_bitrate,
                import_result, validation_result, final_format,
                v0_probe_kind, v0_probe_min_bitrate,
                v0_probe_avg_bitrate, v0_probe_median_bitrate,
                existing_v0_probe_kind, existing_v0_probe_min_bitrate,
                existing_v0_probe_avg_bitrate,
                existing_v0_probe_median_bitrate
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            RETURNING id
            """,
            (
                request_id,
                audit.soulseek_username,
                audit.filetype,
                audit.download_path,
                audit.beets_distance,
                audit.beets_scenario,
                audit.beets_detail,
                audit.valid,
                audit.outcome,
                audit.staged_path,
                audit.error_message,
                audit.bitrate,
                audit.sample_rate,
                audit.bit_depth,
                audit.is_vbr,
                audit.was_converted,
                audit.original_filetype,
                audit.slskd_filetype,
                audit.actual_filetype,
                audit.actual_min_bitrate,
                audit.spectral_grade,
                audit.spectral_bitrate,
                audit.existing_min_bitrate,
                audit.existing_spectral_bitrate,
                audit.import_result_json,
                audit.validation_result_json,
                audit.final_format,
                audit.v0_probe_kind,
                audit.v0_probe_min_bitrate,
                audit.v0_probe_avg_bitrate,
                audit.v0_probe_median_bitrate,
                audit.existing_v0_probe_kind,
                audit.existing_v0_probe_min_bitrate,
                audit.existing_v0_probe_avg_bitrate,
                audit.existing_v0_probe_median_bitrate,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("terminal download audit INSERT returned no row")
        return int(row["id"])

    def _insert_terminal_denylist(
        self,
        request_id: int,
        entries: tuple[DenylistWrite, ...],
    ) -> None:
        if not entries:
            return
        usernames = [entry.username for entry in entries]
        reasons = [entry.reason for entry in entries]
        self._execute(
            """
            INSERT INTO source_denylist (request_id, username, reason)
            SELECT %s, entry.username, entry.reason
            FROM UNNEST(%s::text[], %s::text[]) AS entry(username, reason)
            ON CONFLICT (request_id, username) DO NOTHING
            """,
            (request_id, usernames, reasons),
        )

    def _write_wanted_terminal_request(
        self,
        *,
        request_id: int,
        source_status: str,
        record_validation_attempt: bool,
        write_search_filetype_override: bool,
        search_filetype_override: str | None,
        write_min_bitrate: bool = False,
        min_bitrate: int | None = None,
    ) -> None:
        if not validate_transition(source_status, "wanted"):
            raise TerminalOutcomeConflict(
                f"request {request_id} cannot transition "
                f"from {source_status!r} to 'wanted'"
            )
        now = datetime.now(timezone.utc)
        if source_status == "downloading":
            cur = self._execute(
                """
                UPDATE album_requests
                SET status = 'wanted',
                    active_download_state = NULL,
                    manual_reason = NULL,
                    search_filetype_override = CASE WHEN %s
                        THEN %s ELSE search_filetype_override END,
                    min_bitrate = CASE WHEN %s THEN %s ELSE min_bitrate END,
                    validation_attempts = CASE WHEN %s
                        THEN COALESCE(validation_attempts, 0) + 1
                        ELSE validation_attempts END,
                    last_attempt_at = CASE WHEN %s THEN %s ELSE last_attempt_at END,
                    next_retry_after = CASE WHEN %s THEN %s + (
                        LEAST(
                            %s * POWER(2, COALESCE(validation_attempts, 0)),
                            %s
                        ) * INTERVAL '1 minute'
                    ) ELSE next_retry_after END,
                    updated_at = %s
                WHERE id = %s AND status = %s AND status != 'replaced'
                RETURNING id
                """,
                (
                    write_search_filetype_override,
                    search_filetype_override,
                    write_min_bitrate,
                    min_bitrate,
                    record_validation_attempt,
                    record_validation_attempt,
                    now,
                    record_validation_attempt,
                    now,
                    BACKOFF_BASE_MINUTES,
                    BACKOFF_MAX_MINUTES,
                    now,
                    request_id,
                    source_status,
                ),
            )
        else:
            cur = self._execute(
                """
                UPDATE album_requests
                SET status = 'wanted',
                    active_download_state = NULL,
                    manual_reason = NULL,
                    search_attempts = 0,
                    download_attempts = 0,
                    validation_attempts = CASE WHEN %s THEN 1 ELSE 0 END,
                    last_attempt_at = CASE WHEN %s THEN %s ELSE NULL END,
                    next_retry_after = CASE WHEN %s
                        THEN %s + (%s * INTERVAL '1 minute') ELSE NULL END,
                    search_filetype_override = CASE WHEN %s
                        THEN %s ELSE search_filetype_override END,
                    min_bitrate = CASE WHEN %s THEN %s ELSE min_bitrate END,
                    updated_at = %s
                WHERE id = %s AND status = %s AND status != 'replaced'
                RETURNING id
                """,
                (
                    record_validation_attempt,
                    record_validation_attempt,
                    now,
                    record_validation_attempt,
                    now,
                    BACKOFF_BASE_MINUTES,
                    write_search_filetype_override,
                    search_filetype_override,
                    write_min_bitrate,
                    min_bitrate,
                    now,
                    request_id,
                    source_status,
                ),
            )
        if cur.fetchone() is None:
            raise TerminalOutcomeConflict(
                f"request {request_id} changed before terminal wanted write"
            )

    def _write_imported_terminal_request(
        self,
        *,
        request_id: int,
        source_status: str,
        request: ImportedRequestWrite,
    ) -> None:
        """CAS one locked request to imported with its complete metadata."""
        now = datetime.now(timezone.utc)
        cur = self._execute(
            """
            UPDATE album_requests AS ar
            SET status = 'imported',
                active_download_state = NULL,
                updated_at = %s,
                rescued_at = CASE
                    WHEN ar.unfindable_category IS NOT NULL
                     AND ar.rescued_at IS NULL THEN %s
                    ELSE ar.rescued_at END,
                prior_unfindable_category = CASE
                    WHEN ar.unfindable_category IS NOT NULL
                     AND ar.rescued_at IS NULL THEN ar.unfindable_category
                    ELSE ar.prior_unfindable_category END,
                unfindable_categorised_at = CASE
                    WHEN ar.unfindable_category IS NOT NULL THEN %s
                    ELSE ar.unfindable_categorised_at END,
                unfindable_category = NULL,
                beets_distance = %s,
                beets_scenario = %s,
                imported_path = %s,
                verified_lossless = %s,
                final_format = %s,
                last_download_spectral_grade = CASE WHEN %s THEN %s
                    ELSE ar.last_download_spectral_grade END,
                last_download_spectral_bitrate = CASE WHEN %s THEN %s
                    ELSE ar.last_download_spectral_bitrate END,
                current_spectral_grade = CASE WHEN %s THEN %s
                    ELSE ar.current_spectral_grade END,
                current_spectral_bitrate = CASE WHEN %s THEN %s
                    ELSE ar.current_spectral_bitrate END,
                current_lossless_source_v0_probe_min_bitrate = CASE WHEN %s
                    THEN %s ELSE ar.current_lossless_source_v0_probe_min_bitrate END,
                current_lossless_source_v0_probe_avg_bitrate = CASE WHEN %s
                    THEN %s ELSE ar.current_lossless_source_v0_probe_avg_bitrate END,
                current_lossless_source_v0_probe_median_bitrate = CASE WHEN %s
                    THEN %s ELSE ar.current_lossless_source_v0_probe_median_bitrate END,
                prev_min_bitrate = CASE WHEN %s THEN %s ELSE ar.prev_min_bitrate END,
                min_bitrate = CASE WHEN %s THEN %s ELSE ar.min_bitrate END
            WHERE ar.id = %s AND ar.status = %s AND ar.status != 'replaced'
            RETURNING ar.id
            """,
            (
                now,
                now,
                now,
                request.beets_distance,
                request.beets_scenario,
                request.imported_path,
                request.verified_lossless,
                request.final_format,
                request.write_spectral,
                request.last_download_spectral_grade,
                request.write_spectral,
                request.last_download_spectral_bitrate,
                request.write_spectral,
                request.current_spectral_grade,
                request.write_spectral,
                request.current_spectral_bitrate,
                request.write_v0_probe,
                request.current_lossless_source_v0_probe_min_bitrate,
                request.write_v0_probe,
                request.current_lossless_source_v0_probe_avg_bitrate,
                request.write_v0_probe,
                request.current_lossless_source_v0_probe_median_bitrate,
                request.write_quality_delta,
                request.prev_min_bitrate,
                request.write_quality_delta,
                request.min_bitrate,
                request_id,
                source_status,
            ),
        )
        if cur.fetchone() is None:
            raise TerminalOutcomeConflict(
                f"request {request_id} changed before import write"
            )

    def persist_import_success(
        self,
        outcome: ImportSuccessOutcome,
    ) -> TerminalOutcomeApplied:
        """Commit request import, audit, denylist and job completion together."""
        with self._atomic():
            source_status = self._lock_terminal_request(outcome.request_id)
            if not validate_transition(source_status, "imported"):
                raise TerminalOutcomeConflict(
                    f"request {outcome.request_id} cannot transition "
                    f"from {source_status!r} to 'imported'"
                )
            if outcome.import_job_id is not None:
                self._lock_terminal_import_job(
                    import_job_id=outcome.import_job_id,
                    request_id=outcome.request_id,
                    preview=False,
                )
            self._write_imported_terminal_request(
                request_id=outcome.request_id,
                source_status=source_status,
                request=outcome.request,
            )
            self._terminal_outcome_boundary(TerminalOutcomeBoundary.request)

            download_log_id = self._insert_terminal_download_audit(
                outcome.request_id,
                outcome.audit,
            )
            self._terminal_outcome_boundary(TerminalOutcomeBoundary.audit)

            if outcome.denylist:
                self._insert_terminal_denylist(outcome.request_id, outcome.denylist)
                self._terminal_outcome_boundary(TerminalOutcomeBoundary.denylist)

            if outcome.requeue_after_import:
                self._write_wanted_terminal_request(
                    request_id=outcome.request_id,
                    source_status="imported",
                    record_validation_attempt=False,
                    write_search_filetype_override=True,
                    search_filetype_override=(
                        outcome.requeue_search_filetype_override
                    ),
                    write_min_bitrate=outcome.requeue_min_bitrate is not None,
                    min_bitrate=outcome.requeue_min_bitrate,
                )
                self._terminal_outcome_boundary(
                    TerminalOutcomeBoundary.final_request
                )

            if outcome.import_job_id is not None:
                job_json = msgspec.json.encode(outcome.job_result).decode()
                cur = self._execute(
                    """
                    UPDATE import_jobs
                    SET status = 'completed',
                        result = %s::jsonb,
                        message = %s,
                        error = NULL,
                        completed_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s AND request_id = %s
                      AND status IN ('queued', 'running')
                    RETURNING id
                    """,
                    (
                        job_json,
                        outcome.job_message,
                        outcome.import_job_id,
                        outcome.request_id,
                    ),
                )
                if cur.fetchone() is None:
                    raise TerminalOutcomeConflict(
                        f"import job {outcome.import_job_id} changed before completion"
                    )
                self._terminal_outcome_boundary(TerminalOutcomeBoundary.job)

            self.conn.commit()
            return TerminalOutcomeApplied(
                request_id=outcome.request_id,
                download_log_id=download_log_id,
                import_job_id=outcome.import_job_id,
            )

    def persist_importer_rejection(
        self,
        outcome: ImporterRejectionOutcome,
    ) -> TerminalOutcomeApplied:
        """Commit request self-heal, attempt, audit, denylist and job failure."""
        with self._atomic():
            source_status = self._lock_terminal_request(outcome.request_id)
            if outcome.import_job_id is not None:
                self._lock_terminal_import_job(
                    import_job_id=outcome.import_job_id,
                    request_id=outcome.request_id,
                    preview=False,
                )
            if outcome.requeue_to_wanted:
                self._write_wanted_terminal_request(
                    request_id=outcome.request_id,
                    source_status=source_status,
                    record_validation_attempt=outcome.record_validation_attempt,
                    write_search_filetype_override=(
                        outcome.write_search_filetype_override
                    ),
                    search_filetype_override=outcome.search_filetype_override,
                )
                self._terminal_outcome_boundary(TerminalOutcomeBoundary.request)

            download_log_id = self._insert_terminal_download_audit(
                outcome.request_id,
                outcome.audit,
            )
            self._terminal_outcome_boundary(TerminalOutcomeBoundary.audit)

            if outcome.denylist:
                self._insert_terminal_denylist(outcome.request_id, outcome.denylist)
                self._terminal_outcome_boundary(TerminalOutcomeBoundary.denylist)

            if outcome.import_job_id is not None:
                job_json = msgspec.json.encode(outcome.job_result).decode()
                cur = self._execute(
                    """
                    UPDATE import_jobs
                    SET status = 'failed',
                        result = %s::jsonb,
                        message = %s,
                        error = %s,
                        completed_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s AND request_id = %s
                      AND status IN ('queued', 'running')
                    RETURNING id
                    """,
                    (
                        job_json,
                        outcome.job_message,
                        outcome.job_error,
                        outcome.import_job_id,
                        outcome.request_id,
                    ),
                )
                if cur.fetchone() is None:
                    raise TerminalOutcomeConflict(
                        f"import job {outcome.import_job_id} changed before failure"
                    )
                self._terminal_outcome_boundary(TerminalOutcomeBoundary.job)

            self.conn.commit()
            return TerminalOutcomeApplied(
                request_id=outcome.request_id,
                download_log_id=download_log_id,
                import_job_id=outcome.import_job_id,
            )

    def persist_preview_measurement_failure(
        self,
        outcome: PreviewMeasurementFailureOutcome,
    ) -> TerminalOutcomeApplied:
        """Commit preview failure, parent self-heal, audit and denylist together."""
        with self._atomic():
            source_status = self._lock_terminal_request(outcome.request_id)
            self._lock_terminal_import_job(
                import_job_id=outcome.import_job_id,
                request_id=outcome.request_id,
                preview=True,
            )
            self._write_wanted_terminal_request(
                request_id=outcome.request_id,
                source_status=source_status,
                record_validation_attempt=False,
                write_search_filetype_override=False,
                search_filetype_override=None,
            )
            self._terminal_outcome_boundary(TerminalOutcomeBoundary.request)

            download_log_id = self._insert_terminal_download_audit(
                outcome.request_id,
                DownloadAuditWrite(
                    outcome="measurement_failed",
                    beets_scenario="measurement_failed",
                    beets_detail=outcome.detail,
                    staged_path=outcome.staged_path,
                    import_result_json=outcome.import_result_json,
                    validation_result_json=outcome.validation_result_json,
                ),
            )
            self._terminal_outcome_boundary(TerminalOutcomeBoundary.audit)

            if outcome.denylist:
                self._insert_terminal_denylist(outcome.request_id, outcome.denylist)
                self._terminal_outcome_boundary(TerminalOutcomeBoundary.denylist)

            cur = self._execute(
                """
                UPDATE import_jobs
                SET status = 'failed',
                    preview_status = %s,
                    preview_result = %s::jsonb,
                    preview_message = %s,
                    preview_error = %s,
                    result = jsonb_build_object('preview', %s::jsonb),
                    message = %s,
                    error = %s,
                    preview_completed_at = NOW(),
                    completed_at = NOW(),
                    preview_worker_id = NULL,
                    preview_heartbeat_at = NULL,
                    updated_at = NOW()
                WHERE id = %s AND request_id = %s
                  AND status = 'queued'
                  AND preview_status IN ('waiting', 'running')
                RETURNING id
                """,
                (
                    outcome.preview_status,
                    outcome.preview_result_json,
                    outcome.preview_message,
                    outcome.preview_error,
                    outcome.preview_result_json,
                    outcome.preview_message,
                    outcome.preview_error,
                    outcome.import_job_id,
                    outcome.request_id,
                ),
            )
            if cur.fetchone() is None:
                raise TerminalOutcomeConflict(
                    f"preview job {outcome.import_job_id} changed before failure"
                )
            self._terminal_outcome_boundary(TerminalOutcomeBoundary.job)

            self.conn.commit()
            return TerminalOutcomeApplied(
                request_id=outcome.request_id,
                download_log_id=download_log_id,
                import_job_id=outcome.import_job_id,
            )

    def supplement_terminal_import_job_result(
        self,
        supplement: ImportJobOutcomeSupplement,
    ) -> None:
        """Append post-terminal filesystem audit without re-finalizing a job."""
        with self._atomic():
            cur = self._execute(
                """
                UPDATE import_jobs
                SET result = COALESCE(result, '{}'::jsonb)
                    || jsonb_build_object(%s, %s::jsonb),
                    updated_at = NOW()
                WHERE id = %s AND status IN ('completed', 'failed')
                RETURNING id
                """,
                (
                    supplement.key.value,
                    supplement.payload_json,
                    supplement.import_job_id,
                ),
            )
            if cur.fetchone() is None:
                raise TerminalOutcomeConflict(
                    f"import job {supplement.import_job_id} is not terminal"
                )
            self.conn.commit()
