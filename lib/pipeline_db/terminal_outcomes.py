"""Atomic terminal import/preview domain-outcome persistence."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import msgspec
import psycopg2.extras

from lib import transitions
from lib.import_queue import ImportJob, validate_preview_failure_status
from lib.terminal_outcomes import (
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    TerminalCooldown,
    TerminalDenylist,
    TerminalDownloadAudit,
    TerminalOutcomeResult,
    operator_search_stop_is_current,
)
from lib.validation_envelope import derive_validation_log_columns

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import (
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    validate_request_metadata_fields,
)


class ImportJobTerminalConflict(RuntimeError):
    """The owned import job was no longer active at terminal commit time."""


class _TransactionalTransitionsDB:
    """Existing transition engine backed by uncommitted cursor-level SQL."""

    def __init__(
        self,
        db: _PipelineDBBase,
        boundary: Callable[[str], None],
    ) -> None:
        self._db = db
        self._boundary = boundary

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        cur = self._db._execute(
            "SELECT * FROM album_requests WHERE id = %s",
            (request_id,),
        )
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool:
        del request_id, state_json, expected_status
        raise ValueError("terminal outcomes cannot transition to downloading")

    def compare_request_status(
        self,
        request_id: int,
        *,
        expected_status: str,
    ) -> bool:
        row = self.get_request(request_id)
        return bool(row is not None and row["status"] == expected_status)

    def _update_metadata(
        self,
        request_id: int,
        fields: dict[str, Any],
        *,
        expected_status: str,
        now: datetime,
    ) -> bool:
        validate_request_metadata_fields(fields)
        if not fields:
            return True
        assignments = ", ".join(
            f"{key} = populated.{key}" for key in sorted(fields)
        )
        cur = self._db._execute(
            f"UPDATE album_requests AS ar "
            f"SET updated_at = %s, {assignments} "
            "FROM jsonb_populate_record("
            "NULL::album_requests, %s::jsonb) AS populated "
            "WHERE ar.id = %s AND ar.status != 'replaced' "
            "AND ar.status = %s",
            (
                now,
                psycopg2.extras.Json(
                    fields,
                    dumps=lambda value: msgspec.json.encode(value).decode(),
                ),
                request_id,
                expected_status,
            ),
        )
        self._boundary("request.metadata")
        return cur.rowcount > 0

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool:
        unknown = sorted(
            set(fields)
            - {"search_filetype_override", "min_bitrate", "prev_min_bitrate"}
        )
        if unknown:
            raise ValueError(
                "reset_to_wanted does not accept fields: " + ", ".join(unknown)
            )
        if expected_status is None or expected_status == "replaced":
            return False
        now = datetime.now(timezone.utc)
        override_present = "search_filetype_override" in fields
        min_bitrate_present = "min_bitrate" in fields
        prev_min_bitrate_present = "prev_min_bitrate" in fields
        cur = self._db._execute(
            "UPDATE album_requests "
            "SET status = 'wanted', active_download_state = NULL, "
            "updated_at = %s, "
            "search_attempts = CASE WHEN %s THEN 0 ELSE search_attempts END, "
            "download_attempts = CASE WHEN %s THEN 0 ELSE download_attempts END, "
            "validation_attempts = CASE WHEN %s THEN 0 ELSE validation_attempts END, "
            "next_retry_after = CASE WHEN %s THEN NULL ELSE next_retry_after END, "
            "last_attempt_at = CASE WHEN %s THEN NULL ELSE last_attempt_at END, "
            "prev_min_bitrate = CASE WHEN %s THEN %s "
            "WHEN %s THEN COALESCE(min_bitrate, prev_min_bitrate) "
            "ELSE prev_min_bitrate END, "
            "min_bitrate = CASE WHEN %s THEN %s ELSE min_bitrate END, "
            "search_filetype_override = CASE WHEN %s THEN %s "
            "ELSE search_filetype_override END "
            "WHERE id = %s AND status = %s AND status != 'replaced'",
            (
                now,
                clear_retry_counters,
                clear_retry_counters,
                clear_retry_counters,
                clear_retry_counters,
                clear_retry_counters,
                prev_min_bitrate_present,
                fields.get("prev_min_bitrate"),
                min_bitrate_present,
                min_bitrate_present,
                fields.get("min_bitrate"),
                override_present,
                fields.get("search_filetype_override"),
                request_id,
                expected_status,
            ),
        )
        self._boundary("request.wanted")
        return cur.rowcount > 0

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool:
        unknown = sorted(
            set(fields)
            - {"search_filetype_override", "min_bitrate", "prev_min_bitrate"}
        )
        if unknown:
            raise ValueError(
                "reset_downloading_to_wanted does not accept fields: "
                + ", ".join(unknown)
            )
        if expected_status != "downloading":
            return False
        now = datetime.now(timezone.utc)
        override_present = "search_filetype_override" in fields
        min_bitrate_present = "min_bitrate" in fields
        prev_min_bitrate_present = "prev_min_bitrate" in fields
        cur = self._db._execute(
            "UPDATE album_requests "
            "SET status = 'wanted', active_download_state = NULL, "
            "updated_at = %s, "
            "prev_min_bitrate = CASE WHEN %s THEN %s "
            "WHEN %s THEN COALESCE(min_bitrate, prev_min_bitrate) "
            "ELSE prev_min_bitrate END, "
            "min_bitrate = CASE WHEN %s THEN %s ELSE min_bitrate END, "
            "search_filetype_override = CASE WHEN %s THEN %s "
            "ELSE search_filetype_override END "
            "WHERE id = %s AND status = %s AND status != 'replaced'",
            (
                now,
                prev_min_bitrate_present,
                fields.get("prev_min_bitrate"),
                min_bitrate_present,
                min_bitrate_present,
                fields.get("min_bitrate"),
                override_present,
                fields.get("search_filetype_override"),
                request_id,
                expected_status,
            ),
        )
        self._boundary("request.wanted")
        return cur.rowcount > 0

    def apply_wanted_policy_without_requeue(
        self,
        request_id: int,
        *,
        expected_status: str,
        fields: dict[str, object],
        attempt_type: str | None,
    ) -> bool:
        """Apply wanted-policy facts while retaining the locked lifecycle.

        This is the terminal operator-stop path. It deliberately preserves
        status and retry counters while retaining the
        ordinary wanted transition's field and attempt/backoff effects.
        """
        unknown = sorted(
            set(fields)
            - {"search_filetype_override", "min_bitrate", "prev_min_bitrate"}
        )
        if unknown:
            raise ValueError(
                "wanted policy does not accept fields: " + ", ".join(unknown)
            )
        if fields:
            now = datetime.now(timezone.utc)
            override_present = "search_filetype_override" in fields
            min_bitrate_present = "min_bitrate" in fields
            prev_min_bitrate_present = "prev_min_bitrate" in fields
            cur = self._db._execute(
                "UPDATE album_requests "
                "SET updated_at = %s, "
                "prev_min_bitrate = CASE WHEN %s THEN %s "
                "WHEN %s THEN COALESCE(min_bitrate, prev_min_bitrate) "
                "ELSE prev_min_bitrate END, "
                "min_bitrate = CASE WHEN %s THEN %s ELSE min_bitrate END, "
                "search_filetype_override = CASE WHEN %s THEN %s "
                "ELSE search_filetype_override END "
                "WHERE id = %s AND status = %s AND status != 'replaced'",
                (
                    now,
                    prev_min_bitrate_present,
                    fields.get("prev_min_bitrate"),
                    min_bitrate_present,
                    min_bitrate_present,
                    fields.get("min_bitrate"),
                    override_present,
                    fields.get("search_filetype_override"),
                    request_id,
                    expected_status,
                ),
            )
            self._boundary("request.wanted_policy")
            if cur.rowcount <= 0:
                return False
        if attempt_type is not None:
            return self.record_attempt(
                request_id,
                attempt_type,
                expected_status=expected_status,
            )
        return True

    def apply_terminal_metadata_without_transition(
        self,
        request_id: int,
        *,
        expected_status: str,
        fields: dict[str, object],
    ) -> bool:
        """Persist terminal facts while retaining operator lifecycle state."""
        return self._update_metadata(
            request_id,
            dict(fields),
            expected_status=expected_status,
            now=datetime.now(timezone.utc),
        )

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool:
        if attempt_type not in {"search", "download", "validation"}:
            raise ValueError(f"Unknown attempt type: {attempt_type!r}")
        column = f"{attempt_type}_attempts"
        now = datetime.now(timezone.utc)
        cur = self._db._execute(
            f"UPDATE album_requests "
            f"SET {column} = COALESCE({column}, 0) + 1, "
            "last_attempt_at = %s, "
            "next_retry_after = %s + ("
            "LEAST(%s * POWER(2, COALESCE("
            f"{column}, 0)), %s) * INTERVAL '1 minute'), "
            "updated_at = %s "
            "WHERE id = %s AND status = %s AND status != 'replaced' "
            f"RETURNING {column}",
            (
                now,
                now,
                BACKOFF_BASE_MINUTES,
                BACKOFF_MAX_MINUTES,
                now,
                request_id,
                expected_status,
            ),
        )
        self._boundary(f"request.attempt.{attempt_type}")
        return cur.fetchone() is not None

    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        rescue_owned = {"unfindable_category", "unfindable_categorised_at"}
        bad_rescue_fields = sorted(set(extra) & rescue_owned)
        if bad_rescue_fields:
            raise ValueError(
                "mark_imported_with_rescue cannot accept rescue-owned fields: "
                + ", ".join(bad_rescue_fields)
            )
        validate_request_metadata_fields(dict(extra))
        if expected_status is None or expected_status == "replaced":
            return False
        now = datetime.now(timezone.utc)
        cur = self._db._execute(
            "UPDATE album_requests AS ar "
            "SET status = 'imported', active_download_state = NULL, "
            "updated_at = %s, "
            "rescued_at = CASE WHEN ar.unfindable_category IS NOT NULL "
            "AND ar.rescued_at IS NULL THEN %s ELSE ar.rescued_at END, "
            "prior_unfindable_category = CASE "
            "WHEN ar.unfindable_category IS NOT NULL AND ar.rescued_at IS NULL "
            "THEN ar.unfindable_category ELSE ar.prior_unfindable_category END, "
            "unfindable_categorised_at = CASE "
            "WHEN ar.unfindable_category IS NOT NULL THEN %s "
            "ELSE ar.unfindable_categorised_at END, "
            "unfindable_category = NULL "
            "WHERE ar.id = %s AND ar.status = %s AND ar.status != 'replaced'",
            (now, now, now, request_id, expected_status),
        )
        self._boundary("request.imported")
        if cur.rowcount <= 0:
            return False
        return self._update_metadata(
            request_id,
            dict(extra),
            expected_status="imported",
            now=now,
        )

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        if status == "replaced":
            raise ValueError("status='replaced' is owned by supersede_request_mbid")
        validate_request_metadata_fields(dict(extra))
        if expected_status is None or expected_status == "replaced":
            return False
        now = datetime.now(timezone.utc)
        cur = self._db._execute(
            "UPDATE album_requests "
            "SET status = %s, active_download_state = NULL, updated_at = %s "
            "WHERE id = %s AND status = %s AND status != 'replaced'",
            (status, now, request_id, expected_status),
        )
        self._boundary("request.status")
        if cur.rowcount <= 0:
            return False
        return self._update_metadata(
            request_id,
            dict(extra),
            expected_status=status,
            now=now,
        )


class _TerminalOutcomesMixin(_PipelineDBBase):
    """Persist terminal domain outcomes with one explicit transaction."""

    def _terminal_outcome_write_boundary(self, index: int, label: str) -> None:
        """Post-write fault-injection seam; production deliberately does nothing."""
        del index, label

    def _boundary_emitter(self) -> Callable[[str], None]:
        index = 0

        def emit(label: str) -> None:
            nonlocal index
            index += 1
            self._terminal_outcome_write_boundary(index, label)

        return emit

    def _lock_terminal_request_status(self, request_id: int) -> str | None:
        """Lock and return lifecycle state used by a terminal policy bundle."""
        cur = self._execute(
            "SELECT status FROM album_requests WHERE id = %s FOR UPDATE",
            (request_id,),
        )
        row = cur.fetchone()
        return str(row["status"]) if row is not None else None

    def _apply_terminal_request_transition(
        self,
        transition_db: _TransactionalTransitionsDB,
        request_id: int,
        transition: transitions.RequestTransition,
        *,
        operator_stop_was_current: bool,
        successful_terminal_acceptance: bool,
    ) -> tuple[transitions.TransitionApplied, ...]:
        """Apply one transition without letting automation clear a stop.

        Every terminal request command passes here. When the row carried the
        operator stop at lock time, a non-accepting outcome retains that state
        while applying wanted-policy accounting or imported metadata in place.
        """
        preserve_stop = (
            operator_stop_was_current
            and not successful_terminal_acceptance
        )
        if not preserve_stop:
            return (transitions.require_transition_applied(
                transitions.finalize_request(
                    transition_db,
                    request_id,
                    transition,
                )
            ),)

        row = transition_db.get_request(request_id)
        if row is None:
            return (transitions.require_transition_applied(
                transitions.finalize_request(
                    transition_db,
                    request_id,
                    transition,
                )
            ),)
        current_status = str(row["status"])
        applied: list[transitions.TransitionApplied] = []
        if transition.target_status == "wanted":
            has_policy_effect = bool(transition.fields) or (
                transition.attempt_type is not None
            )
            if not has_policy_effect:
                return ()
            if not transition_db.apply_wanted_policy_without_requeue(
                request_id,
                expected_status=current_status,
                fields=dict(transition.fields),
                attempt_type=transition.attempt_type,
            ):
                raise RuntimeError(
                    "locked operator-stop row changed during terminal policy"
                )
            applied.append(transitions.TransitionApplied(
                request_id=request_id,
                from_status=current_status,
                target_status=current_status,
            ))
            return tuple(applied)
        if transition.attempt_type is not None:
            raise ValueError(
                f"{transition.target_status} transition cannot record an attempt"
            )
        if transition.target_status in {"imported", "unsearchable"}:
            if not transition.fields:
                return ()
            if not transition_db.apply_terminal_metadata_without_transition(
                request_id,
                expected_status=current_status,
                fields=dict(transition.fields),
            ):
                raise RuntimeError(
                    "locked operator-stop row changed during terminal metadata"
                )
            return (transitions.TransitionApplied(
                request_id=request_id,
                from_status=current_status,
                target_status=current_status,
            ),)
        return (transitions.require_transition_applied(
            transitions.finalize_request(
                transition_db,
                request_id,
                transition,
            )
        ),)

    def _insert_terminal_download_audit(
        self,
        request_id: int,
        audit: TerminalDownloadAudit,
        boundary: Callable[[str], None],
    ) -> int:
        beets_distance, beets_scenario = derive_validation_log_columns(
            audit.validation_result,
            beets_distance=audit.beets_distance,
            beets_scenario=audit.beets_scenario,
        )
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
                existing_v0_probe_avg_bitrate, existing_v0_probe_median_bitrate,
                source_download_log_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id
            """,
            (
                request_id,
                audit.soulseek_username,
                audit.filetype,
                audit.download_path,
                beets_distance,
                beets_scenario,
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
                audit.import_result,
                audit.validation_result,
                audit.final_format,
                audit.v0_probe_kind,
                audit.v0_probe_min_bitrate,
                audit.v0_probe_avg_bitrate,
                audit.v0_probe_median_bitrate,
                audit.existing_v0_probe_kind,
                audit.existing_v0_probe_min_bitrate,
                audit.existing_v0_probe_avg_bitrate,
                audit.existing_v0_probe_median_bitrate,
                audit.source_download_log_id,
            ),
        )
        row = cur.fetchone()
        assert row is not None
        boundary("download_log")
        return int(row["id"])

    def _persist_terminal_denylist(
        self,
        request_id: int,
        entry: TerminalDenylist,
        boundary: Callable[[str], None],
    ) -> bool:
        cur = self._execute(
            """
            INSERT INTO source_denylist (request_id, username, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (request_id, username) DO NOTHING
            RETURNING request_id
            """,
            (request_id, entry.username, entry.reason),
        )
        inserted = cur.fetchone() is not None
        if inserted:
            boundary("denylist")
        if not entry.apply_cooldown:
            return False
        return self._persist_terminal_cooldown(
            TerminalCooldown(entry.username),
            boundary,
        )

    def _persist_terminal_cooldown(
        self,
        entry: TerminalCooldown,
        boundary: Callable[[str], None],
    ) -> bool:
        """Evaluate one username's global outcome streak without denylisting.

        Shares the ONE streak evaluator with ``check_and_apply_cooldown``
        (decision 20 follow-up) but keeps its write inside the enclosing
        terminal-outcome transaction — delegating the write too would
        commit mid-transaction and break the all-or-none contract.
        """

        verdict = self._cooldown_streak_verdict(entry.username)
        if verdict is None:
            return False
        self._execute(
            """
            INSERT INTO user_cooldowns (username, cooldown_until, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (username) DO UPDATE
                SET cooldown_until = EXCLUDED.cooldown_until,
                    reason = EXCLUDED.reason
            """,
            (entry.username, verdict[0], verdict[1]),
        )
        boundary("cooldown")
        return True

    def _persist_terminal_import_job(
        self,
        command: ImportTerminalOutcome,
        boundary: Callable[[str], None],
    ) -> ImportJob:
        job = command.job
        completed = job.status == "completed"
        cur = self._execute(
            """
            UPDATE import_jobs
            SET status = %s,
                result = %s,
                message = %s,
                error = %s,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND request_id = %s
              AND status IN ('queued', 'running')
            RETURNING *
            """,
            (
                job.status,
                psycopg2.extras.Json(job.result),
                job.message,
                None if completed else job.error,
                command.import_job_id,
                command.request_id,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise ImportJobTerminalConflict(
                f"import job {command.import_job_id} is no longer active for "
                f"request {command.request_id}"
            )
        boundary(f"import_job.{job.status}")
        return ImportJob.from_row(dict(row))

    def persist_import_terminal_outcome(
        self,
        command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult:
        boundary = self._boundary_emitter()
        applied: list[transitions.TransitionApplied] = []
        cooled: set[str] = set()
        with self._atomic():
            transition_db = _TransactionalTransitionsDB(self, boundary)
            locked_status = self._lock_terminal_request_status(
                command.request_id
            )
            operator_stop_was_current = operator_search_stop_is_current(
                locked_status
            )
            if command.initial_transition is not None:
                if (
                    operator_stop_was_current
                    and not command.successful_terminal_acceptance
                    and command.initial_transition.from_status is not None
                    and command.initial_transition.from_status != locked_status
                ):
                    transitions.require_transition_applied(
                        transitions.finalize_request(
                            transition_db,
                            command.request_id,
                            command.initial_transition,
                        )
                    )
                applied.extend(self._apply_terminal_request_transition(
                    transition_db,
                    command.request_id,
                    command.initial_transition,
                    operator_stop_was_current=operator_stop_was_current,
                    successful_terminal_acceptance=(
                        command.successful_terminal_acceptance
                    ),
                ))
            download_log_id = self._insert_terminal_download_audit(
                command.request_id,
                command.audit,
                boundary,
            )
            for transition in command.post_audit_transitions:
                applied.extend(self._apply_terminal_request_transition(
                    transition_db,
                    command.request_id,
                    transition,
                    operator_stop_was_current=operator_stop_was_current,
                    successful_terminal_acceptance=(
                        command.successful_terminal_acceptance
                    ),
                ))
            # Authority: "A successful exact-release terminal import
            # acceptance supersedes an operator-owned `unsearchable` search
            # stop and records the request as `imported`." —
            # https://github.com/abl030/cratedigger/issues/737#issuecomment-5013436918
            for entry in command.denylists:
                if self._persist_terminal_denylist(
                    command.request_id,
                    entry,
                    boundary,
                ):
                    cooled.add(entry.username)
            for entry in command.cooldowns:
                if self._persist_terminal_cooldown(entry, boundary):
                    cooled.add(entry.username)
            job = self._persist_terminal_import_job(command, boundary)
            self.conn.commit()
        return TerminalOutcomeResult(
            download_log_id=download_log_id,
            job=job,
            transitions=tuple(applied),
            cooled_down_users=frozenset(cooled),
        )

    def persist_preview_terminal_outcome(
        self,
        command: PreviewTerminalOutcome,
    ) -> TerminalOutcomeResult:
        validate_preview_failure_status(command.preview_status)
        boundary = self._boundary_emitter()
        cooled: set[str] = set()
        with self._atomic():
            transition_db = _TransactionalTransitionsDB(self, boundary)
            applied = []
            current_status = self._lock_terminal_request_status(
                command.request_id
            )
            preserve_current = (
                command.request_transition is not None
                and command.request_transition.target_status == "wanted"
                and operator_search_stop_is_current(current_status)
            )
            if command.request_transition is not None and not preserve_current:
                applied.append(transitions.require_transition_applied(
                    transitions.finalize_request(
                        transition_db,
                        command.request_id,
                        command.request_transition,
                    )
                ))
            download_log_id = self._insert_terminal_download_audit(
                command.request_id,
                command.audit,
                boundary,
            )
            for entry in command.denylists:
                if self._persist_terminal_denylist(
                    command.request_id,
                    entry,
                    boundary,
                ):
                    cooled.add(entry.username)
            cur = self._execute(
                """
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
                  AND request_id = %s
                  AND status = 'queued'
                  AND preview_status IN ('waiting', 'running')
                RETURNING *
                """,
                (
                    command.preview_status,
                    psycopg2.extras.Json(command.preview_result),
                    command.message,
                    command.error,
                    psycopg2.extras.Json({"preview": command.preview_result}),
                    command.message,
                    command.error,
                    command.import_job_id,
                    command.request_id,
                ),
            )
            row = cur.fetchone()
            if row is None:
                raise ImportJobTerminalConflict(
                    f"preview job {command.import_job_id} is no longer active for "
                    f"request {command.request_id}"
                )
            boundary("import_job.preview_failed")
            job = ImportJob.from_row(dict(row))
            self.conn.commit()
        return TerminalOutcomeResult(
            download_log_id=download_log_id,
            job=job,
            transitions=tuple(applied),
            cooled_down_users=frozenset(cooled),
        )
