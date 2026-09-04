"""FakePipelineDB terminal_outcomes cluster — mirrors ``lib/pipeline_db/terminal_outcomes.py``.

Terminal import/preview outcome persistence.
"""
from __future__ import annotations

import copy
import json
from collections.abc import (
    Callable,
)
from dataclasses import fields, replace
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db import (
        AlbumRequestRow,
    )
from lib import transitions
from lib.import_execution import (
    ExecutionLeaseSnapshot,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
    validate_preview_failure_status,
)
from lib.pipeline_db import (
    CleanupJournalConflict,
)
from lib.pipeline_db.decisions import (
    search_backoff_minutes,
)
from lib.pipeline_db.terminal_outcomes import (
    ImportJobTerminalConflict,
    _terminal_edge_side_effects,
)
from lib.quality import (
    CooldownConfig,
)
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    RequestPolicyOutcome,
    RequestPolicyResult,
    RequestRejectionOutcome,
    RequestRejectionResult,
    RequestSuccessOutcome,
    RequestSuccessResult,
    TerminalOutcomeResult,
    automation_world_failure_self_heal,
    cleanup_journal_refusal_matches,
    operator_search_stop_is_current,
    validate_automation_terminal_authority,
)
from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakeTerminalTransitionsDB:
    """Emit production-shaped write boundaries while mutating the fake."""

    def __init__(
        self,
        db: _FakePipelineDBBase,
        boundary: Callable[[str], None],
    ) -> None:
        self._db = db
        self._boundary = boundary

    def get_request(self, request_id: int) -> AlbumRequestRow | None:
        return self._db.get_request(request_id)

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

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool:
        applied = self._db.reset_to_wanted(
            request_id,
            expected_status=expected_status,
            clear_retry_counters=clear_retry_counters,
            **fields,
        )
        if applied:
            self._boundary("request.wanted")
        return applied

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool:
        applied = self._db.reset_downloading_to_wanted(
            request_id,
            expected_status=expected_status,
            **fields,
        )
        if applied:
            self._boundary("request.wanted")
        return applied

    def apply_wanted_policy_without_requeue(
        self,
        request_id: int,
        *,
        expected_status: str,
        fields: dict[str, object],
        attempt_type: str | None,
    ) -> bool:
        row = self._db._requests.get(request_id)
        if row is None or row.get("status") != expected_status:
            return False
        updates = dict(fields)
        if "min_bitrate" in updates and "prev_min_bitrate" not in updates:
            current_min_bitrate = row.get("min_bitrate")
            updates["prev_min_bitrate"] = (
                current_min_bitrate
                if current_min_bitrate is not None
                else row.get("prev_min_bitrate")
            )
        if updates:
            applied = self._db.update_request_fields(
                request_id,
                expected_status=expected_status,
                **updates,
            )
            if not applied:
                return False
            self._boundary("request.wanted_policy")
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
        row = self._db._requests.get(request_id)
        if row is None or row.get("status") != expected_status:
            return False
        if not fields:
            return True
        applied = self._db.update_request_fields(
            request_id,
            expected_status=expected_status,
            **fields,
        )
        if applied:
            self._boundary("request.metadata")
        return applied

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool:
        applied = self._db.record_attempt(
            request_id,
            attempt_type,
            expected_status=expected_status,
        )
        if applied:
            self._boundary(f"request.attempt.{attempt_type}")
        return applied

    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        applied = self._db.mark_imported_with_rescue(
            request_id,
            expected_status=expected_status,
            **extra,
        )
        if applied:
            self._boundary("request.imported")
            if extra:
                self._boundary("request.metadata")
        return applied

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        applied = self._db.update_status(
            request_id,
            status,
            expected_status=expected_status,
            **extra,
        )
        if applied:
            self._boundary("request.status")
            if extra:
                self._boundary("request.metadata")
        return applied


class _FakeTerminalOutcomesMixin(_FakePipelineDBBase):
    """Terminal import/preview outcome persistence."""

    def capture_automation_import_completion(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
        receipt: object,
    ) -> ImportJob | None:
        from lib.import_job_recovery_service import (
            AutomationCompletionReceipt,
            automation_completion_result_patch,
        )

        if expected_execution_lease.beets is None:
            return None
        try:
            typed = msgspec.convert(
                msgspec.to_builtins(receipt),
                type=AutomationCompletionReceipt,
                strict=True,
            )
            patch = automation_completion_result_patch(typed)
        except (TypeError, ValueError, msgspec.ValidationError):
            return None
        for row in self._import_jobs:
            request = self._requests.get(int(row.get("request_id") or 0))
            state = (
                request.get("active_download_state")
                if request is not None
                else None
            )
            existing = dict(row.get("result") or {})
            if (
                row.get("id") != job_id
                or typed.job_id != job_id
                or row.get("request_id") != typed.request_id
                or row.get("job_type") != IMPORT_JOB_AUTOMATION
                or row.get("status") != "running"
                or row.get("preview_status") != "evidence_ready"
                or row.get("completed_at") is not None
                or row.get("beets_launch_authorized_at") is None
                or row.get("beets_launch_release_id") != typed.release_id
                or row.get("beets_launch_source_path")
                != typed.canonical_path
                or request is None
                or request.get("status") != "processing"
                or request.get("active_automation_import_job_id") != job_id
                or not isinstance(state, dict)
                or state.get("current_path") != typed.canonical_path
                or not self._execution_lease_matches(
                    row,
                    expected_execution_lease,
                    include_child=True,
                )
                or (
                    "automation_completion" in existing
                    and existing["automation_completion"]
                    != patch["automation_completion"]
                )
            ):
                continue
            existing.update(patch)
            row["result"] = existing
            row["updated_at"] = _utcnow()
            return ImportJob.from_row(copy.deepcopy(row))
        return None

    def _terminal_state_snapshot(self) -> tuple[object, ...]:
        return copy.deepcopy((
            self._requests,
            self._import_jobs,
            self.download_logs,
            self.denylist,
            self.user_cooldowns,
            self.status_history,
            self.recorded_attempts,
            self.cooldowns_applied,
            self._processing_cleanup_journals,
        ))

    def _restore_terminal_state(self, snapshot: tuple[object, ...]) -> None:
        (
            self._requests,
            self._import_jobs,
            self.download_logs,
            self.denylist,
            self.user_cooldowns,
            self.status_history,
            self.recorded_attempts,
            self.cooldowns_applied,
            self._processing_cleanup_journals,
        ) = cast(Any, snapshot)

    def _terminal_outcome_write_boundary(self, index: int, label: str) -> None:
        del index, label

    def _apply_terminal_request_transition(
        self,
        transition_db: _FakeTerminalTransitionsDB,
        request_id: int,
        transition: transitions.RequestTransition,
        *,
        operator_stop_was_current: bool,
        successful_terminal_acceptance: bool,
    ) -> tuple[transitions.TransitionApplied, ...]:
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

    def persist_import_terminal_outcome(
        self,
        command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult:
        if command.automation is not None:
            return self._persist_automation_import_terminal_outcome(command)
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            transition_db = _FakeTerminalTransitionsDB(self, boundary)
            applied = []
            locked_status = (
                str(self._requests[command.request_id]["status"])
                if command.request_id in self._requests
                else None
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
            download_log_id = self._log_terminal_audit(
                command.request_id,
                command.import_job_id,
                command.audit,
            )
            self.set_download_log_candidate_evidence(
                download_log_id,
                self.get_import_job_candidate_evidence_id(command.import_job_id),
                direct_attribution=True,
            )
            boundary("download_log")
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
            cooled: set[str] = set()
            for entry in command.denylists:
                denied_before = len(self.denylist)
                self.add_denylist(
                    command.request_id,
                    entry.username,
                    entry.reason,
                )
                if len(self.denylist) > denied_before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            for entry in command.cooldowns:
                if self.check_and_apply_cooldown(entry.username):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            if command.job.status == "completed":
                job = self.mark_import_job_completed(
                    command.import_job_id,
                    result=command.job.result,
                    message=command.job.message,
                )
            else:
                assert command.job.error is not None
                job = self.mark_import_job_failed(
                    command.import_job_id,
                    error=command.job.error,
                    result=command.job.result,
                    message=command.job.message,
                )
            if job is None or job.request_id != command.request_id:
                raise ImportJobTerminalConflict(
                    f"import job {command.import_job_id} is no longer active "
                    f"for request {command.request_id}"
                )
            boundary(f"import_job.{command.job.status}")
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_import_terminal_outcome_calls.append(command)
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
        """Mirrors ``PipelineDB.persist_request_rejection_outcome`` (#1355
        item 3, operator-stop arbitration added by item A4): the job-less
        sibling of ``persist_import_terminal_outcome`` — snapshot/restore
        proves the same all-or-nothing commit the real transaction gives,
        with no job-status write to fence.
        """
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            transition_db = _FakeTerminalTransitionsDB(self, boundary)
            locked_status = (
                str(self._requests[command.request_id]["status"])
                if command.request_id in self._requests
                else None
            )
            operator_stop_was_current = operator_search_stop_is_current(
                locked_status
            )
            applied: tuple[transitions.TransitionApplied, ...] = ()
            if command.transition is not None:
                applied = self._apply_terminal_request_transition(
                    transition_db,
                    command.request_id,
                    command.transition,
                    operator_stop_was_current=operator_stop_was_current,
                    successful_terminal_acceptance=False,
                )
            audit = command.audit
            download_log_id = self.log_download(
                request_id=command.request_id,
                soulseek_username=audit.soulseek_username,
                contributor_usernames=audit.contributor_usernames,
                filetype=audit.filetype,
                download_path=audit.download_path,
                beets_distance=audit.beets_distance,
                beets_scenario=audit.beets_scenario,
                beets_detail=audit.beets_detail,
                valid=audit.valid,
                outcome=audit.outcome,
                staged_path=audit.staged_path,
                error_message=audit.error_message,
                bitrate=audit.bitrate,
                sample_rate=audit.sample_rate,
                bit_depth=audit.bit_depth,
                is_vbr=audit.is_vbr,
                was_converted=audit.was_converted,
                original_filetype=audit.original_filetype,
                slskd_filetype=audit.slskd_filetype,
                actual_filetype=audit.actual_filetype,
                actual_min_bitrate=audit.actual_min_bitrate,
                spectral_grade=audit.spectral_grade,
                spectral_bitrate=audit.spectral_bitrate,
                existing_min_bitrate=audit.existing_min_bitrate,
                existing_spectral_bitrate=audit.existing_spectral_bitrate,
                import_result=audit.import_result,
                validation_result=audit.validation_result,
                final_format=audit.final_format,
                v0_probe_kind=audit.v0_probe_kind,
                v0_probe_min_bitrate=audit.v0_probe_min_bitrate,
                v0_probe_avg_bitrate=audit.v0_probe_avg_bitrate,
                v0_probe_median_bitrate=audit.v0_probe_median_bitrate,
                existing_v0_probe_kind=audit.existing_v0_probe_kind,
                existing_v0_probe_min_bitrate=audit.existing_v0_probe_min_bitrate,
                existing_v0_probe_avg_bitrate=audit.existing_v0_probe_avg_bitrate,
                existing_v0_probe_median_bitrate=(
                    audit.existing_v0_probe_median_bitrate
                ),
                source_download_log_id=audit.source_download_log_id,
            )
            boundary("download_log")
            cooled: set[str] = set()
            for entry in command.denylists:
                denied_before = len(self.denylist)
                self.add_denylist(command.request_id, entry.username, entry.reason)
                if len(self.denylist) > denied_before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            for entry in command.cooldowns:
                if self.check_and_apply_cooldown(entry.username):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_request_rejection_outcome_calls.append(command)
        return RequestRejectionResult(
            download_log_id=download_log_id,
            transition=applied[0] if applied else None,
            cooled_down_users=frozenset(cooled),
        )

    def persist_request_success_outcome(
        self,
        command: RequestSuccessOutcome,
    ) -> RequestSuccessResult:
        """Mirrors ``PipelineDB.persist_request_success_outcome`` (issue
        #1355 item A1): the job-less success sibling of
        ``persist_request_rejection_outcome`` — snapshot/restore proves
        the same all-or-nothing commit, with no job-status write and no
        denylist/cooldown to fence.
        """
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            transition_db = _FakeTerminalTransitionsDB(self, boundary)
            locked_status = (
                str(self._requests[command.request_id]["status"])
                if command.request_id in self._requests
                else None
            )
            operator_stop_was_current = operator_search_stop_is_current(
                locked_status
            )
            applied = self._apply_terminal_request_transition(
                transition_db,
                command.request_id,
                command.transition,
                operator_stop_was_current=operator_stop_was_current,
                successful_terminal_acceptance=True,
            )
            audit = command.audit
            download_log_id = self.log_download(
                request_id=command.request_id,
                soulseek_username=audit.soulseek_username,
                contributor_usernames=audit.contributor_usernames,
                filetype=audit.filetype,
                download_path=audit.download_path,
                beets_distance=audit.beets_distance,
                beets_scenario=audit.beets_scenario,
                beets_detail=audit.beets_detail,
                valid=audit.valid,
                outcome=audit.outcome,
                staged_path=audit.staged_path,
                error_message=audit.error_message,
                bitrate=audit.bitrate,
                sample_rate=audit.sample_rate,
                bit_depth=audit.bit_depth,
                is_vbr=audit.is_vbr,
                was_converted=audit.was_converted,
                original_filetype=audit.original_filetype,
                slskd_filetype=audit.slskd_filetype,
                actual_filetype=audit.actual_filetype,
                actual_min_bitrate=audit.actual_min_bitrate,
                spectral_grade=audit.spectral_grade,
                spectral_bitrate=audit.spectral_bitrate,
                existing_min_bitrate=audit.existing_min_bitrate,
                existing_spectral_bitrate=audit.existing_spectral_bitrate,
                import_result=audit.import_result,
                validation_result=audit.validation_result,
                final_format=audit.final_format,
                v0_probe_kind=audit.v0_probe_kind,
                v0_probe_min_bitrate=audit.v0_probe_min_bitrate,
                v0_probe_avg_bitrate=audit.v0_probe_avg_bitrate,
                v0_probe_median_bitrate=audit.v0_probe_median_bitrate,
                existing_v0_probe_kind=audit.existing_v0_probe_kind,
                existing_v0_probe_min_bitrate=audit.existing_v0_probe_min_bitrate,
                existing_v0_probe_avg_bitrate=audit.existing_v0_probe_avg_bitrate,
                existing_v0_probe_median_bitrate=(
                    audit.existing_v0_probe_median_bitrate
                ),
                source_download_log_id=audit.source_download_log_id,
            )
            boundary("download_log")
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_request_success_outcome_calls.append(command)
        return RequestSuccessResult(
            download_log_id=download_log_id,
            transition=applied[0] if applied else None,
        )

    def persist_request_policy_outcome(
        self,
        command: RequestPolicyOutcome,
    ) -> RequestPolicyResult:
        """Mirrors ``PipelineDB.persist_request_policy_outcome`` (issue
        #1355 item A2): a job-less transition-plus-denylist/cooldown
        bundle with no audit row and no job — snapshot/restore proves the
        same all-or-nothing commit.
        """
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            transition_db = _FakeTerminalTransitionsDB(self, boundary)
            locked_status = (
                str(self._requests[command.request_id]["status"])
                if command.request_id in self._requests
                else None
            )
            operator_stop_was_current = operator_search_stop_is_current(
                locked_status
            )
            applied: tuple[transitions.TransitionApplied, ...] = ()
            if command.transition is not None:
                applied = self._apply_terminal_request_transition(
                    transition_db,
                    command.request_id,
                    command.transition,
                    operator_stop_was_current=operator_stop_was_current,
                    successful_terminal_acceptance=(
                        command.successful_terminal_acceptance
                    ),
                )
            cooled: set[str] = set()
            for entry in command.denylists:
                denied_before = len(self.denylist)
                self.add_denylist(command.request_id, entry.username, entry.reason)
                if len(self.denylist) > denied_before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            for entry in command.cooldowns:
                if self.check_and_apply_cooldown(entry.username):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_request_policy_outcome_calls.append(command)
        return RequestPolicyResult(
            transitions=applied,
            cooled_down_users=frozenset(cooled),
        )

    def _require_fake_automation_terminal(
        self,
        *,
        request_id: int,
        job_id: int,
        authority: AutomationTerminalAuthority,
        allow_missing_completion_receipt: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            self._require_fake_exact_processing_owner(
                request_id=request_id,
                job_id=job_id,
            )
        except CleanupJournalConflict as exc:
            raise ImportJobTerminalConflict(str(exc)) from exc
        request = self._requests.get(request_id)
        job = next(
            (
                row
                for row in self._import_jobs
                if row["id"] == job_id
                and row.get("request_id") == request_id
            ),
            None,
        )
        if request is None or job is None:
            raise ImportJobTerminalConflict(
                "automation terminal owner disappeared under lock"
            )
        if (
            job.get("job_type") != IMPORT_JOB_AUTOMATION
            or job.get("status") != authority.expected_job_status
            or job.get("preview_status") != authority.expected_preview_status
            or job.get("completed_at") is not None
        ):
            raise ImportJobTerminalConflict(
                f"automation job {job_id} no longer has the exact terminal "
                "stage"
            )
        lease = authority.expected_execution_lease
        lease_matches = (
            all(
                job.get(field) is None
                for field in (
                    "execution_invocation_id",
                    "execution_host_boot_id",
                    "execution_systemd_unit",
                    "execution_worker_pid",
                    "execution_worker_start_ticks",
                    "execution_beets_pid",
                    "execution_beets_start_ticks",
                )
            )
            if lease is None
            else self._execution_lease_matches(
                job,
                lease,
                include_child=True,
            )
        )
        if not lease_matches:
            raise ImportJobTerminalConflict(
                f"automation job {job_id} execution lease changed"
            )
        journal = self._processing_cleanup_journals.get(
            (job_id, request_id)
        )
        receipt = authority.cleanup_receipt
        refusal = authority.cleanup_refusal
        receipt_matches = (
            receipt is not None
            and journal is not None
            and journal["completed_receipt"] == receipt
            and journal["completed_at"] is not None
        )
        refusal_matches = (
            refusal is not None
            and cleanup_journal_refusal_matches(refusal, journal)
        )
        if not receipt_matches and not refusal_matches:
            raise ImportJobTerminalConflict(
                f"automation job {job_id} cleanup disposition is not exact"
            )
        if authority.completion_receipt is not None:
            result = dict(job.get("result") or {})
            expected = msgspec.to_builtins(authority.completion_receipt)
            if result.get("automation_completion") != expected:
                raise ImportJobTerminalConflict(
                    f"automation job {job_id} completion receipt changed"
                )
        elif (
            authority.expected_job_status == "running"
            and authority.expected_preview_status == "evidence_ready"
            and job.get("beets_launch_authorized_at") is not None
            and not allow_missing_completion_receipt
        ):
            raise ImportJobTerminalConflict(
                f"automation job {job_id} lacks its completion receipt"
            )
        return request, job

    @staticmethod
    def _fake_automation_audit(
        audit,
        authority: AutomationTerminalAuthority,
    ):
        payload = (
            {}
            if not audit.validation_result
            else json.loads(audit.validation_result)
        )
        if not isinstance(payload, dict):
            raise TypeError("automation validation audit must be an object")
        cleanup = msgspec.to_builtins(authority.cleanup_disposition)
        existing = payload.get("processing_cleanup")
        if existing is not None and existing != cleanup:
            raise ValueError("automation cleanup audit conflicts")
        payload["processing_cleanup"] = cleanup
        return replace(
            audit,
            validation_result=json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    def _fake_finish_processing_request(
        self,
        request,
        command: ImportTerminalOutcome,
        boundary: Callable[[str], None],
    ) -> tuple[transitions.TransitionApplied, ...]:
        sequence = tuple(
            transition
            for transition in (
                command.initial_transition,
                *command.post_audit_transitions,
            )
            if transition is not None
        )
        if not sequence:
            raise ImportJobTerminalConflict(
                "automation terminal outcome has no private request edge"
            )
        applied: list[transitions.TransitionApplied] = []
        virtual = "processing"
        imported_seen = False
        metadata_changed = False
        counters = {
            name: int(request.get(name) or 0)
            for name in (
                "search_attempts",
                "download_attempts",
                "validation_attempts",
            )
        }
        retry_state_changed = False
        attempt_backoff_minutes: int | None = None
        for transition in sequence:
            if transition.target_status not in {"wanted", "imported"}:
                raise ValueError(
                    "automation terminal edge must end wanted or imported"
                )
            previous = virtual
            virtual = transition.target_status
            # Retry-counter policy is read from the ONE canonical
            # ``transitions.VALID_TRANSITIONS`` table, exactly as production's
            # ``_finish_processing_request_last`` resolves it.  Restating a
            # reset here would make the fake more permissive than the database
            # it stands in for.
            if _terminal_edge_side_effects(
                previous,
                virtual,
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
                key = f"{transition.attempt_type}_attempts"
                prior_attempts = counters[key]
                counters[key] = prior_attempts + 1
                retry_state_changed = True
                attempt_backoff_minutes = search_backoff_minutes(
                    prior_attempts
                )
            fields = dict(transition.fields)
            metadata_changed = metadata_changed or bool(fields)
            if (
                "min_bitrate" in fields
                and "prev_min_bitrate" not in fields
            ):
                request["prev_min_bitrate"] = request.get("min_bitrate")
            request.update(fields)
            imported_seen = imported_seen or virtual == "imported"
            applied.append(transitions.TransitionApplied(
                request_id=command.request_id,
                from_status=previous,
                target_status=virtual,
            ))
        now = _utcnow()
        if imported_seen and request.get("unfindable_category") is not None:
            if request.get("rescued_at") is None:
                request["rescued_at"] = now
                request["prior_unfindable_category"] = request.get(
                    "unfindable_category"
                )
            request["unfindable_category"] = None
            request["unfindable_categorised_at"] = now
        if metadata_changed:
            boundary("request.processing_metadata")
        request.update(counters)
        if retry_state_changed:
            request["next_retry_after"] = (
                None
                if attempt_backoff_minutes is None
                else now + timedelta(minutes=attempt_backoff_minutes)
            )
            request["last_attempt_at"] = (
                None if attempt_backoff_minutes is None else now
            )
        request["status"] = virtual
        request["active_download_state"] = None
        request["active_automation_import_job_id"] = None
        request["updated_at"] = now
        boundary(f"request.processing_to_{virtual}")
        return tuple(applied)

    def _log_terminal_audit(self, request_id: int, import_job_id: int, audit):
        from lib.failure_presentation import unlinked_source_provenance_message

        source = None
        if audit.source_download_log_id is not None:
            source = next(
                (
                    entry for entry in self.download_logs
                    if entry.id == audit.source_download_log_id
                    and entry.request_id == request_id
                ),
                None,
            )
        job = next(
            (row for row in self._import_jobs if row["id"] == import_job_id),
            None,
        )
        fallback_source = (
            "youtube"
            if job is not None and job.get("job_type") == IMPORT_JOB_YOUTUBE
            else "slskd"
        )
        kwargs = {item.name: getattr(audit, item.name) for item in fields(audit)}
        if not audit.contributor_usernames and source is not None:
            kwargs["contributor_usernames"] = (
                source.candidate_contributor_usernames or ()
            )
        if audit.source_download_log_id is not None and source is None:
            kwargs["source_download_log_id"] = None
            kwargs["error_message"] = unlinked_source_provenance_message(
                audit.error_message,
            )
        download_log_id = self.log_download(
            request_id=request_id,
            **kwargs,
        )
        terminal = next(
            entry for entry in self.download_logs
            if entry.id == download_log_id
        )
        terminal.source = source.source if source is not None else fallback_source
        return download_log_id

    def _persist_automation_import_terminal_outcome(
        self,
        command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult:
        authority = command.automation
        assert authority is not None
        validate_automation_terminal_authority(command)
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            request, job_row = self._require_fake_automation_terminal(
                request_id=command.request_id,
                job_id=command.import_job_id,
                authority=authority,
                allow_missing_completion_receipt=(
                    automation_world_failure_self_heal(command)
                    and authority.completion_receipt is None
                ),
            )
            audit = self._fake_automation_audit(command.audit, authority)
            download_log_id = self._log_terminal_audit(
                command.request_id,
                command.import_job_id,
                audit,
            )
            boundary("download_log")
            cooled: set[str] = set()
            for entry in command.denylists:
                before = len(self.denylist)
                self.add_denylist(
                    command.request_id,
                    entry.username,
                    entry.reason,
                )
                if len(self.denylist) > before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            for entry in command.cooldowns:
                if self.check_and_apply_cooldown(entry.username):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            result = dict(job_row.get("result") or {})
            result.update(command.job.result)
            result["processing_cleanup"] = msgspec.to_builtins(
                authority.cleanup_disposition
            )
            if authority.completion_receipt is not None:
                result["automation_completion"] = msgspec.to_builtins(
                    authority.completion_receipt
                )
            now = _utcnow()
            job_row["status"] = command.job.status
            job_row["result"] = result
            job_row["message"] = command.job.message
            job_row["error"] = command.job.error
            job_row["completed_at"] = now
            job_row["updated_at"] = now
            boundary(f"import_job.{command.job.status}")
            if authority.cleanup_refusal is None \
                    or authority.cleanup_refusal.journal is not None:
                del self._processing_cleanup_journals[
                    (command.import_job_id, command.request_id)
                ]
                boundary("processing_cleanup.consumed")
            applied = self._fake_finish_processing_request(
                request,
                command,
                boundary,
            )
            job = ImportJob.from_row(copy.deepcopy(job_row))
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_import_terminal_outcome_calls.append(command)
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
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            transition_db = _FakeTerminalTransitionsDB(self, boundary)
            applied = []
            current_status = (
                str(self._requests[command.request_id]["status"])
                if command.request_id in self._requests
                else None
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
            download_log_id = self._log_terminal_audit(
                command.request_id,
                command.import_job_id,
                command.audit,
            )
            self.set_download_log_candidate_evidence(
                download_log_id,
                self.get_import_job_candidate_evidence_id(command.import_job_id),
                direct_attribution=True,
            )
            boundary("download_log")
            cooled: set[str] = set()
            for entry in command.denylists:
                denied_before = len(self.denylist)
                self.add_denylist(
                    command.request_id,
                    entry.username,
                    entry.reason,
                )
                if len(self.denylist) > denied_before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            job = self.mark_import_job_preview_failed(
                command.import_job_id,
                preview_status=command.preview_status,
                error=command.error,
                preview_result=command.preview_result,
                message=command.message,
            )
            if job is None or job.request_id != command.request_id:
                raise ImportJobTerminalConflict(
                    f"preview job {command.import_job_id} is no longer active "
                    f"for request {command.request_id}"
                )
            boundary("import_job.preview_failed")
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_preview_terminal_outcome_calls.append(command)
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
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            request, job_row = self._require_fake_automation_terminal(
                request_id=command.request_id,
                job_id=command.import_job_id,
                authority=authority,
            )
            audit = self._fake_automation_audit(command.audit, authority)
            download_log_id = cast(Any, self.log_download)(
                request_id=command.request_id,
                **{item.name: getattr(audit, item.name) for item in fields(audit)},
            )
            boundary("download_log")
            cooled: set[str] = set()
            for entry in command.denylists:
                denied_before = len(self.denylist)
                self.add_denylist(
                    command.request_id,
                    entry.username,
                    entry.reason,
                )
                if len(self.denylist) > denied_before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            result = dict(job_row.get("result") or {})
            result["preview"] = copy.deepcopy(command.preview_result)
            result["processing_cleanup"] = msgspec.to_builtins(
                authority.cleanup_disposition
            )
            now = _utcnow()
            job_row.update({
                "status": "failed",
                "preview_status": command.preview_status,
                "preview_result": copy.deepcopy(command.preview_result),
                "preview_message": command.message,
                "preview_error": command.error,
                "result": result,
                "message": command.message,
                "error": command.error,
                "preview_completed_at": now,
                "completed_at": now,
                "preview_worker_id": None,
                "preview_heartbeat_at": None,
                "updated_at": now,
            })
            boundary("import_job.preview_failed")
            if authority.cleanup_refusal is None \
                    or authority.cleanup_refusal.journal is not None:
                del self._processing_cleanup_journals[
                    (command.import_job_id, command.request_id)
                ]
                boundary("processing_cleanup.consumed")
            transition = command.request_transition
            if transition is None:
                raise ImportJobTerminalConflict(
                    "automation terminal outcome has no private request edge"
                )
            request["status"] = transition.target_status
            request.update(dict(transition.fields))
            request["active_download_state"] = None
            request["active_automation_import_job_id"] = None
            request["updated_at"] = now
            boundary(
                f"request.processing_to_{transition.target_status}"
            )
            applied = (
                transitions.TransitionApplied(
                    request_id=command.request_id,
                    from_status="processing",
                    target_status=transition.target_status,
                ),
            )
            job = ImportJob.from_row(copy.deepcopy(job_row))
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_preview_terminal_outcome_calls.append(command)
        return TerminalOutcomeResult(
            download_log_id=download_log_id,
            job=job,
            transitions=applied,
            cooled_down_users=frozenset(cooled),
        )

