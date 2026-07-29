"""Typed in-process commands for terminal import/preview DB outcomes."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, fields, replace
from typing import TYPE_CHECKING, Literal

from lib.import_execution import ExecutionLeaseSnapshot
from lib.import_queue import ImportJob
from lib.json_narrow import is_str_object_dict
from lib.transitions import RequestTransition, TransitionApplied
from lib.validation_envelope import (
    VALIDATION_PROJECTION_UNSET,
    ValidationProjectionUnset,
)

if TYPE_CHECKING:
    from lib.import_job_recovery_service import AutomationCompletionReceipt
    from lib.pipeline_db.cleanup_journal import CleanupJournalReceipt
    from lib.pipeline_db.download_log import DownloadLogOutcome


OPERATOR_SEARCH_STOP_STATUS = "unsearchable"


def operator_search_stop_is_current(status: str | None) -> bool:
    """Return whether the request currently carries the operator stop."""
    return status == OPERATOR_SEARCH_STOP_STATUS


@dataclass(frozen=True)
class TerminalDownloadAudit:
    """One mandatory ``download_log`` row in a terminal outcome bundle."""

    outcome: DownloadLogOutcome
    soulseek_username: str | None = None
    filetype: str | None = None
    download_path: str | None = None
    beets_distance: float | None | ValidationProjectionUnset = (
        VALIDATION_PROJECTION_UNSET
    )
    beets_scenario: str | None | ValidationProjectionUnset = (
        VALIDATION_PROJECTION_UNSET
    )
    beets_detail: str | None = None
    valid: bool | None = None
    staged_path: str | None = None
    error_message: str | None = None
    bitrate: int | None = None
    sample_rate: int | None = None
    bit_depth: int | None = None
    is_vbr: bool | None = None
    was_converted: bool | None = None
    original_filetype: str | None = None
    slskd_filetype: str | None = None
    actual_filetype: str | None = None
    actual_min_bitrate: int | None = None
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_min_bitrate: int | None = None
    existing_spectral_bitrate: int | None = None
    import_result: str | None = None
    validation_result: str | None = None
    final_format: str | None = None
    v0_probe_kind: str | None = None
    v0_probe_min_bitrate: int | None = None
    v0_probe_avg_bitrate: int | None = None
    v0_probe_median_bitrate: int | None = None
    existing_v0_probe_kind: str | None = None
    existing_v0_probe_min_bitrate: int | None = None
    existing_v0_probe_avg_bitrate: int | None = None
    existing_v0_probe_median_bitrate: int | None = None
    source_download_log_id: int | None = None

    def as_log_kwargs(self) -> dict[str, object]:
        """Return the exact public ``log_download`` keyword projection."""
        return {item.name: getattr(self, item.name) for item in fields(self)}


@dataclass(frozen=True)
class TerminalDenylist:
    """One source denylist write and its existing optional cooldown check."""

    username: str
    reason: str | None = None
    apply_cooldown: bool = False


@dataclass(frozen=True)
class TerminalCooldown:
    """One global source-user cooldown evaluation, without a denylist write."""

    username: str


@dataclass(frozen=True)
class ImportJobTerminal:
    """Terminal import-job fields committed with its domain outcome."""

    status: Literal["completed", "failed"]
    result: dict[str, object]
    message: str | None
    error: str | None = None

    def __post_init__(self) -> None:
        if self.status == "completed" and self.error is not None:
            raise ValueError("completed import job cannot carry an error")
        if self.status == "failed" and self.error is None:
            raise ValueError("failed import job requires an error")


@dataclass(frozen=True)
class AutomationTerminalAuthority:
    """Exact processing-owner proof required by an automation terminal write."""

    expected_job_status: Literal["queued", "running", "recovery_required"]
    expected_preview_status: str | None
    expected_execution_lease: ExecutionLeaseSnapshot | None
    cleanup_receipt: CleanupJournalReceipt
    completion_receipt: AutomationCompletionReceipt | None = None
    declared_result_status: Literal["wanted", "imported"] | None = None
    declared_reason: str | None = None
    evidence_revision: str | None = None


@dataclass(frozen=True)
class ImportTerminalOutcome:
    """Complete PostgreSQL-owned terminal outcome for one import job."""

    request_id: int
    import_job_id: int
    initial_transition: RequestTransition | None
    audit: TerminalDownloadAudit
    job: ImportJobTerminal
    post_audit_transitions: tuple[RequestTransition, ...] = ()
    denylists: tuple[TerminalDenylist, ...] = ()
    cooldowns: tuple[TerminalCooldown, ...] = ()
    successful_terminal_acceptance: bool = False
    automation: AutomationTerminalAuthority | None = None

    def __post_init__(self) -> None:
        if not self.successful_terminal_acceptance:
            return
        final_transition = (
            self.post_audit_transitions[-1]
            if self.post_audit_transitions
            else self.initial_transition
        )
        if (
            self.job.status != "completed"
            or self.audit.outcome not in ("success", "force_import")
            or final_transition is None
            or final_transition.target_status != "imported"
        ):
            raise ValueError(
                "successful terminal acceptance requires a completed import, "
                "a success audit, and a final imported transition"
            )


@dataclass(frozen=True)
class PendingImportTerminalOutcome:
    """Terminal request/audit intent completed by the importer job owner."""

    request_id: int
    import_job_id: int
    initial_transition: RequestTransition | None
    audit: TerminalDownloadAudit
    post_audit_transitions: tuple[RequestTransition, ...] = ()
    denylists: tuple[TerminalDenylist, ...] = ()
    cooldowns: tuple[TerminalCooldown, ...] = ()
    successful_terminal_acceptance: bool = False
    automation: AutomationTerminalAuthority | None = None

    def with_job(self, job: ImportJobTerminal) -> ImportTerminalOutcome:
        return ImportTerminalOutcome(
            request_id=self.request_id,
            import_job_id=self.import_job_id,
            initial_transition=self.initial_transition,
            audit=self.audit,
            job=job,
            post_audit_transitions=self.post_audit_transitions,
            denylists=self.denylists,
            cooldowns=self.cooldowns,
            successful_terminal_acceptance=(
                self.successful_terminal_acceptance
            ),
            automation=self.automation,
        )

    def append_transitions(
        self,
        *transitions: RequestTransition,
    ) -> PendingImportTerminalOutcome:
        return replace(
            self,
            post_audit_transitions=self.post_audit_transitions + transitions,
        )

    def append_denylists(
        self,
        *entries: TerminalDenylist,
    ) -> PendingImportTerminalOutcome:
        return replace(self, denylists=self.denylists + entries)

    def mark_successful_terminal_acceptance(
        self,
    ) -> PendingImportTerminalOutcome:
        """Authorize the successful-import stop-supersession exception."""
        return replace(self, successful_terminal_acceptance=True)

@dataclass(frozen=True)
class PreviewTerminalOutcome:
    """Complete PostgreSQL-owned preview measurement-failure outcome."""

    request_id: int
    import_job_id: int
    request_transition: RequestTransition | None
    audit: TerminalDownloadAudit
    preview_status: str
    preview_result: dict[str, object]
    message: str
    error: str
    denylists: tuple[TerminalDenylist, ...] = ()
    automation: AutomationTerminalAuthority | None = None


@dataclass(frozen=True)
class TerminalOutcomeResult:
    """Rows and side effects produced by a committed terminal bundle."""

    download_log_id: int
    job: ImportJob
    transitions: tuple[TransitionApplied, ...]
    cooled_down_users: frozenset[str] = field(default_factory=lambda: frozenset())


def validate_automation_terminal_declaration(
    command: ImportTerminalOutcome,
) -> None:
    """Reject a recovery close whose command contradicts its declaration."""
    authority = command.automation
    if authority is None:
        return
    declaration = (
        authority.declared_result_status,
        authority.declared_reason,
        authority.evidence_revision,
    )
    if all(value is None for value in declaration):
        return
    if any(value is None for value in declaration):
        raise ValueError(
            "automation recovery declaration must be complete"
        )
    result_status = authority.declared_result_status
    reason = authority.declared_reason
    evidence_revision = authority.evidence_revision
    assert result_status is not None
    assert reason is not None
    assert evidence_revision is not None
    if (
        result_status not in ("wanted", "imported")
        or not reason.strip()
        or not evidence_revision.strip()
    ):
        raise ValueError(
            "automation recovery declaration must be valid and non-blank"
        )

    transitions = tuple(
        transition
        for transition in (
            command.initial_transition,
            *command.post_audit_transitions,
        )
        if transition is not None
    )
    if not transitions or transitions[-1].target_status != result_status:
        raise ValueError(
            "automation recovery transition contradicts declared result"
        )
    if command.job.status != "failed":
        raise ValueError(
            "automation recovery close must fail the ambiguous job"
        )
    if command.successful_terminal_acceptance:
        raise ValueError(
            "automation recovery close is not a successful worker acceptance"
        )

    recovery = {
        "resolution": "close",
        "result_status": result_status,
        "reason": reason,
        "evidence_revision": evidence_revision,
    }
    if (
        command.audit.outcome != "failed"
        or command.audit.error_message != reason
        or command.job.result.get("recovery_resolution") != recovery
    ):
        raise ValueError(
            "automation recovery audit contradicts declared result"
        )
    raw_validation = command.audit.validation_result
    try:
        validation: object = (
            None if raw_validation is None else json.loads(raw_validation)
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            "automation recovery audit must be valid JSON"
        ) from exc
    if (
        not is_str_object_dict(validation)
        or validation.get("automation_recovery") != recovery
    ):
        raise ValueError(
            "automation recovery validation audit omits its declaration"
        )


def automation_recovery_close_outcome(
    *,
    request_id: int,
    import_job_id: int,
    result_status: Literal["wanted", "imported"],
    reason: str,
    evidence_revision: str,
    expected_job_status: Literal["queued", "running", "recovery_required"],
    expected_preview_status: str | None,
    expected_execution_lease: ExecutionLeaseSnapshot | None,
    cleanup_receipt: CleanupJournalReceipt,
    completion_receipt: AutomationCompletionReceipt | None,
) -> ImportTerminalOutcome:
    """Build the one canonical explicit automation recovery close command."""
    reason = reason.strip()
    if not reason:
        raise ValueError("automation recovery close requires a reason")
    if not evidence_revision.strip():
        raise ValueError(
            "automation recovery close requires an evidence revision"
        )
    imported = result_status == "imported"
    recovery_audit = {
        "valid": imported,
        "scenario": "automation_recovery_close",
        "detail": reason,
        "automation_recovery": {
            "resolution": "close",
            "result_status": result_status,
            "reason": reason,
            "evidence_revision": evidence_revision,
        },
    }
    return ImportTerminalOutcome(
        request_id=request_id,
        import_job_id=import_job_id,
        initial_transition=(
            RequestTransition.to_imported(from_status="processing")
            if imported
            else RequestTransition.to_wanted(from_status="processing")
        ),
        audit=TerminalDownloadAudit(
            outcome="failed",
            error_message=reason,
            validation_result=json.dumps(
                recovery_audit,
                sort_keys=True,
                separators=(",", ":"),
            ),
        ),
        job=ImportJobTerminal(
            status="failed",
            result={
                "recovery_resolution": {
                    "resolution": "close",
                    "result_status": result_status,
                    "reason": reason,
                    "evidence_revision": evidence_revision,
                },
            },
            message=(
                f"Operator reconciled automation import as {result_status}: "
                f"{reason}"
            ),
            error=(
                f"Automation recovery closed as {result_status} by operator"
            ),
        ),
        automation=AutomationTerminalAuthority(
            expected_job_status=expected_job_status,
            expected_preview_status=expected_preview_status,
            expected_execution_lease=expected_execution_lease,
            cleanup_receipt=cleanup_receipt,
            completion_receipt=completion_receipt,
            declared_result_status=result_status,
            declared_reason=reason,
            evidence_revision=evidence_revision,
        ),
    )
