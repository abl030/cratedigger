"""Atomic terminal import/preview domain-outcome persistence."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import msgspec
import psycopg2.extras

from lib import transitions
from lib.convergence_service import normalize_contributor_usernames
from lib.import_execution import ExecutionLeaseSnapshot
from lib.import_queue import ImportJob, validate_preview_failure_status
from lib.json_narrow import is_str_object_dict, json_dict
from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import (
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    REQUEST_PRESENTATION_FROM,
    REQUEST_PRESENTATION_SELECT,
    _msgspec_json_dumps,
    request_presentation_row,
    validate_request_metadata_fields,
)
from lib.pipeline_db.cleanup_journal import (
    CleanupJournalConflict,
    _CleanupCursor,
)
from lib.pipeline_db.decisions import (
    SEARCH_BACKOFF_MAX_EXPONENT,
    search_backoff_minutes,
)
from lib.pipeline_db.rows import AlbumRequestRow
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    RequestRejectionOutcome,
    RequestRejectionResult,
    TerminalCooldown,
    TerminalDenylist,
    TerminalDownloadAudit,
    TerminalOutcomeResult,
    automation_world_failure_self_heal,
    cleanup_journal_refusal_matches,
    operator_search_stop_is_current,
    validate_automation_terminal_authority,
)
from lib.validation_envelope import derive_validation_log_columns


class ImportJobTerminalConflict(RuntimeError):
    """The owned import job was no longer active at terminal commit time."""


AUTOMATION_COMPLETION_RESULT_KEY = "automation_completion"
PROCESSING_CLEANUP_AUDIT_KEY = "processing_cleanup"
PROCESSING_CLEANUP_RESULT_KEY = "processing_cleanup"

# The owner handoff enters ``processing`` from ``downloading``, so that is the
# ordinary edge the private terminal edge stands in for.
PROCESSING_ENTRY_STATUS = "downloading"


def _terminal_edge_side_effects(
    from_status: str,
    target_status: str,
) -> transitions.TransitionSideEffects:
    """Answer the private processing edge from the ONE canonical table.

    ``processing`` is deliberately absent from ``VALID_TRANSITIONS``: only
    owner-aware bundles may cross that edge. That privacy governs WHO may
    write the edge, not WHAT the edge does, so retry-counter policy is read
    from ``transitions.VALID_TRANSITIONS`` instead of being restated here. A
    ``processing`` source therefore resolves to the automatic ``downloading``
    edge it stands in for — which retains retry counters exactly like
    ``reset_downloading_to_wanted``, so automatic backoff keeps growing.
    """
    source = (
        PROCESSING_ENTRY_STATUS
        if from_status == "processing"
        else from_status
    )
    side_effects = transitions.VALID_TRANSITIONS.get((source, target_status))
    if side_effects is None:
        raise ValueError(
            f"automation terminal edge {from_status!r} -> {target_status!r} "
            "has no canonical side-effect policy"
        )
    return side_effects


def _lease_values(
    lease: ExecutionLeaseSnapshot | None,
) -> tuple[object, ...]:
    if lease is None:
        return (None, None, None, None, None, None, None)
    return (
        lease.invocation_id,
        lease.host_boot_id,
        lease.systemd_unit,
        lease.worker.pid,
        lease.worker.start_ticks,
        None if lease.beets is None else lease.beets.pid,
        None if lease.beets is None else lease.beets.start_ticks,
    )


def _receipt_builtins(receipt: object) -> dict[str, object]:
    builtins: dict[str, object] = msgspec.to_builtins(receipt)
    return builtins


class _TransactionalTransitionsDB:
    """Existing transition engine backed by uncommitted cursor-level SQL."""

    def __init__(
        self,
        db: _PipelineDBBase,
        boundary: Callable[[str], None],
    ) -> None:
        self._db = db
        self._boundary = boundary

    def get_request(self, request_id: int) -> AlbumRequestRow | None:
        # Must project ``processing_owner`` exactly like the real
        # ``PipelineDB.get_request`` (issue #1355 item 3): a plain
        # ``SELECT *`` leaves ``processing_locked_conflict`` unable to prove
        # a currently-``processing`` row's exact owner, which it treats as a
        # malformed row and raises ``TypeError`` instead of the intended
        # conflict. This class is shared by all three terminal-outcome
        # bundles — the two pre-existing job-backed ones
        # (``persist_import_terminal_outcome``,
        # ``persist_preview_terminal_outcome``) and the new job-less one
        # (``persist_request_rejection_outcome``) added here — so the fix
        # hardens all three; it was latent in the two pre-existing callers
        # and would have been a fresh regression in the new one, since a
        # job-less rejection is the first caller ordinarily positioned to
        # meet a row a DIFFERENT worker still owns.
        cur = self._db._execute(
            f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE request_row.id = %s
            """,
            (request_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return request_presentation_row(row)

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
                    dumps=_msgspec_json_dumps,
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
        now = datetime.now(UTC)
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
        now = datetime.now(UTC)
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
            now = datetime.now(UTC)
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
            now=datetime.now(UTC),
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
        now = datetime.now(UTC)
        cur = self._db._execute(
            f"UPDATE album_requests "
            f"SET {column} = COALESCE({column}, 0) + 1, "
            "last_attempt_at = %s, "
            "next_retry_after = %s + ("
            "LEAST(%s * POWER(2, LEAST(COALESCE("
            f"{column}, 0), %s)), %s) * INTERVAL '1 minute'), "
            "updated_at = %s "
            "WHERE id = %s AND status = %s AND status != 'replaced' "
            f"RETURNING {column}",
            (
                now,
                now,
                BACKOFF_BASE_MINUTES,
                SEARCH_BACKOFF_MAX_EXPONENT,
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
        now = datetime.now(UTC)
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
        now = datetime.now(UTC)
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

    def capture_automation_import_completion(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
        receipt: object,
    ) -> ImportJob | None:
        """Persist exact child completion before any post-Beets effect.

        The receipt is evidence, never authority.  The UPDATE therefore
        compares the complete current processing owner, importer stage,
        launch fence, canonical path/release, and worker/child lease.  An
        identical replay is idempotent; a different receipt cannot overwrite
        the first captured completion.
        """
        if expected_execution_lease.beets is None:
            return None
        from lib.import_job_recovery_service import (
            AutomationCompletionReceipt,
            automation_completion_result_patch,
        )

        try:
            typed_receipt = msgspec.convert(
                msgspec.to_builtins(receipt),
                type=AutomationCompletionReceipt,
                strict=True,
            )
            patch = automation_completion_result_patch(typed_receipt)
        except (TypeError, ValueError, msgspec.ValidationError):
            return None
        if typed_receipt.job_id != job_id:
            return None
        lease = _lease_values(expected_execution_lease)
        receipt_json = patch[AUTOMATION_COMPLETION_RESULT_KEY]
        cur = self._execute(
            """
            UPDATE import_jobs AS job
            SET result = COALESCE(job.result, '{}'::jsonb) || %s::jsonb,
                updated_at = NOW()
            FROM album_requests AS request
            WHERE job.id = %s
              AND job.request_id = %s
              AND job.job_type = 'automation_import'
              AND job.status = 'running'
              AND job.preview_status = 'evidence_ready'
              AND job.completed_at IS NULL
              AND job.beets_launch_authorized_at IS NOT NULL
              AND job.beets_launch_release_id = %s
              AND job.beets_launch_source_path = %s
              AND request.id = job.request_id
              AND request.status = 'processing'
              AND request.active_automation_import_job_id = job.id
              AND request.active_download_state ->> 'current_path' = %s
              AND job.execution_invocation_id = %s
              AND job.execution_host_boot_id = %s
              AND job.execution_systemd_unit = %s
              AND job.execution_worker_pid = %s
              AND job.execution_worker_start_ticks = %s
              AND job.execution_beets_pid = %s
              AND job.execution_beets_start_ticks = %s
              AND (
                  NOT (COALESCE(job.result, '{}'::jsonb) ? %s)
                  OR job.result -> %s = %s::jsonb
              )
            RETURNING job.*
            """,
            (
                psycopg2.extras.Json(
                    patch,
                    dumps=_msgspec_json_dumps,
                ),
                job_id,
                typed_receipt.request_id,
                typed_receipt.release_id,
                typed_receipt.canonical_path,
                typed_receipt.canonical_path,
                *lease,
                AUTOMATION_COMPLETION_RESULT_KEY,
                AUTOMATION_COMPLETION_RESULT_KEY,
                psycopg2.extras.Json(
                    receipt_json,
                    dumps=_msgspec_json_dumps,
                ),
            ),
        )
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row is not None else None

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

    def _insert_nonjob_download_audit(
        self,
        request_id: int,
        audit: TerminalDownloadAudit,
    ) -> int:
        """The job-less ``download_log`` INSERT (issue #1355 item 3).

        Mirrors ``log_download``'s own INSERT: no ``import_jobs`` join, no
        candidate-evidence linkage, ``source`` left to the schema's
        ``'slskd'`` default — none of that exists for a rejection with no
        owning import job. The job-backed twin,
        ``_insert_terminal_download_audit`` below, owns that shape.
        Spelled directly against ``TerminalDownloadAudit``'s own typed
        fields (not a dynamic dict spread — the same reasoning that
        retired the Struct's now-deleted dict-spread bridge entirely) so
        this stays inside ``persist_request_rejection_outcome``'s
        transaction without a cross-mixin call into
        ``lib/pipeline_db/download_log.py``.
        """
        beets_distance, beets_scenario = derive_validation_log_columns(
            audit.validation_result,
            beets_distance=audit.beets_distance,
            beets_scenario=audit.beets_scenario,
        )
        contributor_usernames = list(normalize_contributor_usernames(
            audit.contributor_usernames,
        )) or None
        cur = self._execute(
            """
            INSERT INTO download_log (
                request_id, soulseek_username, candidate_contributor_usernames,
                filetype, download_path,
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            RETURNING id
            """,
            (
                request_id, audit.soulseek_username, contributor_usernames,
                audit.filetype, audit.download_path,
                beets_distance, beets_scenario, audit.beets_detail, audit.valid,
                audit.outcome, audit.staged_path, audit.error_message,
                audit.bitrate, audit.sample_rate, audit.bit_depth, audit.is_vbr,
                audit.was_converted, audit.original_filetype, audit.slskd_filetype,
                audit.actual_filetype, audit.actual_min_bitrate,
                audit.spectral_grade, audit.spectral_bitrate,
                audit.existing_min_bitrate, audit.existing_spectral_bitrate,
                audit.import_result, audit.validation_result, audit.final_format,
                audit.v0_probe_kind, audit.v0_probe_min_bitrate,
                audit.v0_probe_avg_bitrate, audit.v0_probe_median_bitrate,
                audit.existing_v0_probe_kind, audit.existing_v0_probe_min_bitrate,
                audit.existing_v0_probe_avg_bitrate,
                audit.existing_v0_probe_median_bitrate,
                audit.source_download_log_id,
            ),
        )
        row = cur.fetchone()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])

    def _insert_terminal_download_audit(
        self,
        request_id: int,
        import_job_id: int,
        audit: TerminalDownloadAudit,
        boundary: Callable[[str], None],
    ) -> int:
        beets_distance, beets_scenario = derive_validation_log_columns(
            audit.validation_result,
            beets_distance=audit.beets_distance,
            beets_scenario=audit.beets_scenario,
        )
        contributor_usernames = list(normalize_contributor_usernames(
            audit.contributor_usernames,
        )) or None
        cur = self._execute(
            """
            WITH origin AS (
                SELECT source, candidate_contributor_usernames
                FROM download_log
                WHERE id = %s
                  AND request_id = %s
            ), contributor_identity AS MATERIALIZED (
                SELECT COALESCE(
                    %s::TEXT[],
                    (SELECT candidate_contributor_usernames FROM origin)
                ) AS usernames
            )
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
                source, source_download_log_id, candidate_evidence_id,
                candidate_contributor_usernames, candidate_evidence_direct
            ) SELECT
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                COALESCE(
                    (SELECT source FROM origin),
                    (SELECT CASE WHEN job_type = 'youtube_import' THEN 'youtube'
                                 WHEN job_type = 'local_import' THEN 'local'
                                 ELSE 'slskd' END
                     FROM import_jobs
                     WHERE id = %s AND request_id = %s),
                    'slskd'
                ),
                CASE WHEN EXISTS (SELECT 1 FROM origin) THEN %s::bigint END,
                (SELECT candidate_evidence_id FROM import_jobs
                 WHERE id = %s AND request_id = %s),
                (SELECT usernames FROM contributor_identity),
                EXISTS (
                    SELECT 1 FROM import_jobs
                    WHERE id = %s
                      AND request_id = %s
                      AND candidate_evidence_id IS NOT NULL
                ) AND COALESCE(CARDINALITY(
                    (SELECT usernames FROM contributor_identity)
                ), 0) > 0
            RETURNING id, (SELECT EXISTS (SELECT 1 FROM origin)) AS origin_exists
            """,
            (
                audit.source_download_log_id,
                request_id,
                contributor_usernames,
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
                import_job_id,
                request_id,
                audit.source_download_log_id,
                import_job_id,
                request_id,
                import_job_id,
                request_id,
            ),
        )
        row = cur.fetchone()
        assert row is not None, "INSERT RETURNING should always return a row"
        if (
            audit.source_download_log_id is not None
            and not bool(row["origin_exists"])
        ):
            from lib.failure_presentation import unlinked_source_provenance_message

            self._execute(
                "UPDATE download_log SET error_message = %s WHERE id = %s",
                (
                    unlinked_source_provenance_message(audit.error_message),
                    int(row["id"]),
                ),
            )
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

    def _require_automation_terminal_scope(
        self,
        cur: _CleanupCursor,
        *,
        request_id: int,
        job_id: int,
        authority: AutomationTerminalAuthority,
        allow_missing_completion_receipt: bool = False,
    ) -> tuple[dict[str, object], dict[str, object]]:
        scope = self._lock_processing_cleanup_scope(
            cur,
            request_id=request_id,
        )
        try:
            self._require_exact_processing_owner(
                scope,
                request_id=request_id,
                job_id=job_id,
            )
        except CleanupJournalConflict as exc:
            raise ImportJobTerminalConflict(str(exc)) from exc
        cur.execute(
            """
            SELECT *
            FROM album_requests
            WHERE id = %s
            """,
            (request_id,),
        )
        request_raw = cur.fetchone()
        cur.execute(
            """
            SELECT *
            FROM import_jobs
            WHERE id = %s AND request_id = %s
            """,
            (job_id, request_id),
        )
        job_raw = cur.fetchone()
        if request_raw is None or job_raw is None:
            raise ImportJobTerminalConflict(
                "automation terminal owner disappeared under lock"
            )
        request = dict(request_raw)
        job = dict(job_raw)
        if (
            job["job_type"] != "automation_import"
            or job["status"] != authority.expected_job_status
            or job["preview_status"] != authority.expected_preview_status
            or job["completed_at"] is not None
        ):
            raise ImportJobTerminalConflict(
                f"automation job {job_id} no longer has the exact terminal stage"
            )
        actual_lease = (
            job["execution_invocation_id"],
            job["execution_host_boot_id"],
            job["execution_systemd_unit"],
            job["execution_worker_pid"],
            job["execution_worker_start_ticks"],
            job["execution_beets_pid"],
            job["execution_beets_start_ticks"],
        )
        if actual_lease != _lease_values(authority.expected_execution_lease):
            raise ImportJobTerminalConflict(
                f"automation job {job_id} execution lease changed"
            )

        journal = self._get_processing_cleanup_journal_locked(
            request_id=request_id,
            job_id=job_id,
            scope=scope,
        )
        receipt = authority.cleanup_receipt
        refusal = authority.cleanup_refusal
        receipt_matches = (
            receipt is not None
            and journal is not None
            and journal["completed_at"] is not None
            and journal["completed_receipt"] is not None
            and _receipt_builtins(journal["completed_receipt"])
            == _receipt_builtins(receipt)
        )
        refusal_matches = (
            refusal is not None
            and cleanup_journal_refusal_matches(refusal, journal)
        )
        if not receipt_matches and not refusal_matches:
            raise ImportJobTerminalConflict(
                f"automation job {job_id} cleanup disposition is not exact"
            )

        raw_result = job["result"]
        result = json_dict(raw_result)
        completion = authority.completion_receipt
        if completion is not None:
            completion_builtins = _receipt_builtins(completion)
            if (
                result.get(AUTOMATION_COMPLETION_RESULT_KEY)
                != completion_builtins
            ):
                raise ImportJobTerminalConflict(
                    f"automation job {job_id} completion receipt changed"
                )
        elif (
            authority.expected_job_status == "running"
            and authority.expected_preview_status == "evidence_ready"
            and job["beets_launch_authorized_at"] is not None
            and not allow_missing_completion_receipt
        ):
            raise ImportJobTerminalConflict(
                f"automation job {job_id} lacks its completion receipt"
            )
        return request, job

    @staticmethod
    def _automation_audit(
        audit: TerminalDownloadAudit,
        authority: AutomationTerminalAuthority,
    ) -> TerminalDownloadAudit:
        raw = audit.validation_result
        if raw is None or raw == "":
            payload: dict[str, object] = {}
        else:
            decoded = json.loads(raw)
            if not is_str_object_dict(decoded):
                raise ValueError(
                    "automation validation audit must be a JSON object"
                )
            payload = decoded
        cleanup = _receipt_builtins(authority.cleanup_disposition)
        existing = payload.get(PROCESSING_CLEANUP_AUDIT_KEY)
        if existing is not None and existing != cleanup:
            raise ValueError(
                "validation audit contains another cleanup disposition"
            )
        payload[PROCESSING_CLEANUP_AUDIT_KEY] = cleanup
        return replace(
            audit,
            validation_result=_msgspec_json_dumps(payload),
        )

    def _automation_job_result(
        self,
        *,
        existing: object,
        terminal: dict[str, object],
        authority: AutomationTerminalAuthority,
    ) -> dict[str, object]:
        merged = dict(json_dict(existing))
        merged.update(terminal)
        merged[PROCESSING_CLEANUP_RESULT_KEY] = _receipt_builtins(
            authority.cleanup_disposition
        )
        if authority.completion_receipt is not None:
            completion = _receipt_builtins(authority.completion_receipt)
            existing_completion = merged.get(AUTOMATION_COMPLETION_RESULT_KEY)
            if (
                existing_completion is not None
                and existing_completion != completion
            ):
                raise ImportJobTerminalConflict(
                    "terminal result conflicts with completion receipt"
                )
            merged[AUTOMATION_COMPLETION_RESULT_KEY] = completion
        return merged

    def _finish_processing_request_last(
        self,
        *,
        request: dict[str, object],
        request_id: int,
        job_id: int,
        request_transitions: tuple[transitions.RequestTransition, ...],
        boundary: Callable[[str], None],
    ) -> tuple[transitions.TransitionApplied, ...]:
        if not request_transitions:
            raise ImportJobTerminalConflict(
                "automation terminal outcome has no private request edge"
            )
        virtual_status = "processing"
        applied: list[transitions.TransitionApplied] = []
        fields: dict[str, object] = {}

        def counter_value(name: str) -> int:
            value = request.get(name)
            if value is None:
                return 0
            if not isinstance(value, int) or isinstance(value, bool):
                raise ImportJobTerminalConflict(
                    f"automation request counter {name} is invalid"
                )
            return value

        counters = {
            name: counter_value(name)
            for name in (
                "search_attempts",
                "download_attempts",
                "validation_attempts",
            )
        }
        retry_state_changed = False
        attempt_backoff_minutes: int | None = None
        imported_seen = False
        for index, transition in enumerate(request_transitions):
            if transition.target_status not in {"wanted", "imported"}:
                raise ValueError(
                    "automation terminal edge must end wanted or imported"
                )
            if transition.from_status is not None:
                allowed_sources = {virtual_status}
                if index == 0:
                    allowed_sources.add("downloading")
                if transition.from_status not in allowed_sources:
                    raise ImportJobTerminalConflict(
                        "automation terminal transition source changed"
                    )
            previous = virtual_status
            if _terminal_edge_side_effects(
                previous,
                transition.target_status,
            ).clear_retry_counters:
                counters = dict.fromkeys(counters, 0)
                retry_state_changed = True
                attempt_backoff_minutes = None
            if transition.attempt_type is not None:
                if transition.attempt_type not in {
                    "search",
                    "download",
                    "validation",
                }:
                    raise ValueError(
                        f"Unknown attempt type: {transition.attempt_type!r}"
                    )
                counter = f"{transition.attempt_type}_attempts"
                prior_attempts = counters[counter]
                counters[counter] += 1
                retry_state_changed = True
                attempt_backoff_minutes = search_backoff_minutes(
                    prior_attempts
                )
            transition_fields = dict(transition.fields)
            validate_request_metadata_fields(transition_fields)
            if (
                "min_bitrate" in transition_fields
                and "prev_min_bitrate" not in transition_fields
            ):
                fields["prev_min_bitrate"] = fields.get(
                    "min_bitrate",
                    request.get("min_bitrate"),
                )
            fields.update(transition_fields)
            imported_seen = (
                imported_seen or transition.target_status == "imported"
            )
            virtual_status = transition.target_status
            applied.append(transitions.TransitionApplied(
                request_id=request_id,
                from_status=previous,
                target_status=virtual_status,
            ))

        now = datetime.now(UTC)
        next_retry_after = (
            now + timedelta(minutes=attempt_backoff_minutes)
            if attempt_backoff_minutes is not None
            else None
        )
        if fields:
            assignments = ", ".join(
                f"{key} = populated.{key}" for key in sorted(fields)
            )
            cur = self._execute(
                f"""
                UPDATE album_requests AS request
                SET {assignments}
                FROM jsonb_populate_record(
                    NULL::album_requests, %s::jsonb
                ) AS populated
                WHERE request.id = %s
                  AND request.status = 'processing'
                  AND request.active_automation_import_job_id = %s
                RETURNING request.id
                """,
                (
                    psycopg2.extras.Json(
                        fields,
                        dumps=_msgspec_json_dumps,
                    ),
                    request_id,
                    job_id,
                ),
            )
            if cur.fetchone() is None:
                raise ImportJobTerminalConflict(
                    "automation processing owner changed before metadata write"
                )
            boundary("request.processing_metadata")

        cur = self._execute(
            """
            UPDATE album_requests AS request
            SET status = %s,
                active_automation_import_job_id = NULL,
                active_download_state = NULL,
                search_attempts = %s,
                download_attempts = %s,
                validation_attempts = %s,
                next_retry_after = CASE
                    WHEN %s THEN %s
                    ELSE request.next_retry_after
                END,
                last_attempt_at = CASE
                    WHEN %s THEN %s
                    ELSE request.last_attempt_at
                END,
                rescued_at = CASE
                    WHEN %s
                     AND request.unfindable_category IS NOT NULL
                     AND request.rescued_at IS NULL
                    THEN %s
                    ELSE request.rescued_at
                END,
                prior_unfindable_category = CASE
                    WHEN %s
                     AND request.unfindable_category IS NOT NULL
                     AND request.rescued_at IS NULL
                    THEN request.unfindable_category
                    ELSE request.prior_unfindable_category
                END,
                unfindable_categorised_at = CASE
                    WHEN %s
                     AND request.unfindable_category IS NOT NULL
                    THEN %s
                    ELSE request.unfindable_categorised_at
                END,
                unfindable_category = CASE
                    WHEN %s THEN NULL
                    ELSE request.unfindable_category
                END,
                updated_at = %s
            WHERE request.id = %s
              AND request.status = 'processing'
              AND request.active_automation_import_job_id = %s
            RETURNING request.id
            """,
            (
                virtual_status,
                counters["search_attempts"],
                counters["download_attempts"],
                counters["validation_attempts"],
                retry_state_changed,
                next_retry_after,
                retry_state_changed,
                now if attempt_backoff_minutes is not None else None,
                imported_seen,
                now,
                imported_seen,
                imported_seen,
                now,
                imported_seen,
                now,
                request_id,
                job_id,
            ),
        )
        if cur.fetchone() is None:
            raise ImportJobTerminalConflict(
                "automation processing owner changed before final request write"
            )
        boundary(f"request.processing_to_{virtual_status}")
        return tuple(applied)

    def _consume_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        authority: AutomationTerminalAuthority,
        boundary: Callable[[str], None],
    ) -> None:
        refusal = authority.cleanup_refusal
        if refusal is not None and refusal.journal is None:
            return
        if refusal is None:
            cur = self._execute(
                """
                DELETE FROM processing_cleanup_journal
                WHERE request_id = %s
                  AND job_id = %s
                  AND completed_receipt IS NOT NULL
                  AND completed_at IS NOT NULL
                RETURNING job_id
                """,
                (request_id, job_id),
            )
        else:
            assert refusal.journal is not None
            cur = self._execute(
                """
                DELETE FROM processing_cleanup_journal
                WHERE request_id = %s
                  AND job_id = %s
                  AND revision = %s
                  AND completed_receipt IS NULL
                  AND completed_at IS NULL
                RETURNING job_id
                """,
                (request_id, job_id, refusal.journal.revision),
            )
        if cur.fetchone() is None:
            raise ImportJobTerminalConflict(
                "automation cleanup journal was not consumable"
            )
        boundary("processing_cleanup.consumed")

    def _persist_terminal_import_job(
        self,
        command: ImportTerminalOutcome,
        boundary: Callable[[str], None],
        *,
        existing_result: object = None,
    ) -> ImportJob:
        job = command.job
        completed = job.status == "completed"
        result = (
            job.result
            if command.automation is None
            else self._automation_job_result(
                existing=existing_result,
                terminal=job.result,
                authority=command.automation,
            )
        )
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
              AND (
                  (
                      %s::text IS NULL
                      AND status IN ('queued', 'running')
                  )
                  OR (
                      %s::text IS NOT NULL
                      AND status = %s
                      AND preview_status IS NOT DISTINCT FROM %s
                  )
              )
            RETURNING *
            """,
            (
                job.status,
                psycopg2.extras.Json(
                    result,
                    dumps=_msgspec_json_dumps,
                ),
                job.message,
                None if completed else job.error,
                command.import_job_id,
                command.request_id,
                (
                    None
                    if command.automation is None
                    else command.automation.expected_job_status
                ),
                (
                    None
                    if command.automation is None
                    else command.automation.expected_job_status
                ),
                (
                    None
                    if command.automation is None
                    else command.automation.expected_job_status
                ),
                (
                    None
                    if command.automation is None
                    else command.automation.expected_preview_status
                ),
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
        if command.automation is not None:
            return self._persist_automation_import_terminal_outcome(command)
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
                command.import_job_id,
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

    def persist_request_rejection_outcome(
        self,
        command: RequestRejectionOutcome,
    ) -> RequestRejectionResult:
        """Atomically commit a job-less rejection (issue #1355 item 3).

        The job-less counterpart of ``persist_import_terminal_outcome``: no
        import job owns this rejection, so there is no job-status write and
        no automation authority to validate — only the request transition
        (if any), the mandatory ``download_log`` audit row, and any
        denylist/cooldown entries, committed together as one PostgreSQL
        transaction. A row lock is taken first so a concurrent terminal
        writer for the same request cannot interleave with this bundle.

        On any exception the whole bundle rolls back — the request is left
        exactly where it was, for the existing processing-recovery machinery
        to re-derive, never partially transitioned with no audit trail.

        Closes the atomicity gap only. Unlike the job-backed bundles, this
        applies ``command.transition`` via a direct
        ``transitions.finalize_request`` call rather than
        ``_apply_terminal_request_transition``, so it does NOT preserve an
        operator-owned ``unsearchable`` stop on a non-accepting outcome — a
        job-less rejection against an ``unsearchable`` request still
        transitions to ``wanted`` unconditionally, exactly as it did before
        issue #1355 item 3.
        """
        boundary = self._boundary_emitter()
        with self._atomic():
            transition_db = _TransactionalTransitionsDB(self, boundary)
            self._lock_terminal_request_status(command.request_id)
            applied: transitions.TransitionApplied | None = None
            if command.transition is not None:
                applied = transitions.require_transition_applied(
                    transitions.finalize_request(
                        transition_db,
                        command.request_id,
                        command.transition,
                    )
                )
            download_log_id = self._insert_nonjob_download_audit(
                command.request_id,
                command.audit,
            )
            boundary("download_log")
            cooled: set[str] = set()
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
            self.conn.commit()
        return RequestRejectionResult(
            download_log_id=download_log_id,
            transition=applied,
            cooled_down_users=frozenset(cooled),
        )

    def _persist_automation_import_terminal_outcome(
        self,
        command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult:
        authority = command.automation
        assert authority is not None
        validate_automation_terminal_authority(command)
        boundary = self._boundary_emitter()
        cooled: set[str] = set()
        request_transitions = tuple(
            transition
            for transition in (
                command.initial_transition,
                *command.post_audit_transitions,
            )
            if transition is not None
        )
        with self._atomic():
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                request, job_row = self._require_automation_terminal_scope(
                    cur,
                    request_id=command.request_id,
                    job_id=command.import_job_id,
                    authority=authority,
                    allow_missing_completion_receipt=(
                        automation_world_failure_self_heal(command)
                        and authority.completion_receipt is None
                    ),
                )
            audit = self._automation_audit(command.audit, authority)
            download_log_id = self._insert_terminal_download_audit(
                command.request_id,
                command.import_job_id,
                audit,
                boundary,
            )
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
            job = self._persist_terminal_import_job(
                command,
                boundary,
                existing_result=job_row["result"],
            )
            self._consume_processing_cleanup_journal(
                request_id=command.request_id,
                job_id=command.import_job_id,
                authority=authority,
                boundary=boundary,
            )
            applied = self._finish_processing_request_last(
                request=request,
                request_id=command.request_id,
                job_id=command.import_job_id,
                request_transitions=request_transitions,
                boundary=boundary,
            )
            self.conn.commit()
        return TerminalOutcomeResult(
            download_log_id=download_log_id,
            job=job,
            transitions=applied,
            cooled_down_users=frozenset(cooled),
        )

    def persist_preview_terminal_outcome(
        self,
        command: PreviewTerminalOutcome,
    ) -> TerminalOutcomeResult:
        if command.automation is not None:
            return self._persist_automation_preview_terminal_outcome(command)
        validate_preview_failure_status(command.preview_status)
        boundary = self._boundary_emitter()
        cooled: set[str] = set()
        with self._atomic():
            transition_db = _TransactionalTransitionsDB(self, boundary)
            applied: list[transitions.TransitionApplied] = []
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
                command.import_job_id,
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

    def _persist_automation_preview_terminal_outcome(
        self,
        command: PreviewTerminalOutcome,
    ) -> TerminalOutcomeResult:
        validate_preview_failure_status(command.preview_status)
        authority = command.automation
        assert authority is not None
        boundary = self._boundary_emitter()
        cooled: set[str] = set()
        request_transitions = (
            ()
            if command.request_transition is None
            else (command.request_transition,)
        )
        with self._atomic():
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                request, job_row = self._require_automation_terminal_scope(
                    cur,
                    request_id=command.request_id,
                    job_id=command.import_job_id,
                    authority=authority,
                )
            audit = self._automation_audit(command.audit, authority)
            download_log_id = self._insert_terminal_download_audit(
                command.request_id,
                command.import_job_id,
                audit,
                boundary,
            )
            for entry in command.denylists:
                if self._persist_terminal_denylist(
                    command.request_id,
                    entry,
                    boundary,
                ):
                    cooled.add(entry.username)
            result = self._automation_job_result(
                existing=job_row["result"],
                terminal={"preview": command.preview_result},
                authority=authority,
            )
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
                  AND status = %s
                  AND preview_status IS NOT DISTINCT FROM %s
                RETURNING *
                """,
                (
                    command.preview_status,
                    psycopg2.extras.Json(
                        command.preview_result,
                        dumps=_msgspec_json_dumps,
                    ),
                    command.message,
                    command.error,
                    psycopg2.extras.Json(
                        result,
                        dumps=_msgspec_json_dumps,
                    ),
                    command.message,
                    command.error,
                    command.import_job_id,
                    command.request_id,
                    authority.expected_job_status,
                    authority.expected_preview_status,
                ),
            )
            row = cur.fetchone()
            if row is None:
                raise ImportJobTerminalConflict(
                    f"preview job {command.import_job_id} changed after lock"
                )
            boundary("import_job.preview_failed")
            job = ImportJob.from_row(dict(row))
            self._consume_processing_cleanup_journal(
                request_id=command.request_id,
                job_id=command.import_job_id,
                authority=authority,
                boundary=boundary,
            )
            applied = self._finish_processing_request_last(
                request=request,
                request_id=command.request_id,
                job_id=command.import_job_id,
                request_transitions=request_transitions,
                boundary=boundary,
            )
            self.conn.commit()
        return TerminalOutcomeResult(
            download_log_id=download_log_id,
            job=job,
            transitions=applied,
            cooled_down_users=frozenset(cooled),
        )
