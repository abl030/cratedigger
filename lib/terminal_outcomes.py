"""Typed in-process commands for terminal import/preview DB outcomes."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal

import msgspec

from lib.failure_presentation import (
    NON_AUTOMATION_IMPORT_FAILURE_PREFIXES,
    non_automation_import_failure_message,
)
from lib.import_execution import ExecutionLeaseSnapshot
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    ForceImportPayload,
    ImportJob,
    LocalImportPayload,
    YoutubeImportPayload,
)
from lib.transitions import RequestTransition, TransitionApplied
from lib.validation_envelope import (
    VALIDATION_PROJECTION_UNSET,
    ValidationProjectionUnset,
)

if TYPE_CHECKING:
    from lib.import_job_recovery_service import AutomationCompletionReceipt
    from lib.pipeline_db.cleanup_journal import (
        CleanupJournalReceipt,
        ProcessingCleanupJournalRow,
    )
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
    contributor_usernames: tuple[str, ...] = ()
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


class CleanupJournalSnapshot(msgspec.Struct, frozen=True, kw_only=True):
    """Exact incomplete cleanup-journal facts retained by a refusal."""

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
    step_progress: dict[str, object]


class CleanupJournalRefusalDisposition(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
):
    """Truthful terminal disposition for cleanup the executor refused."""

    error_code: str
    error_message: str
    journal: CleanupJournalSnapshot | None
    outcome: Literal["refused"] = "refused"
    disposition: Literal["left_in_place"] = "left_in_place"

    def __post_init__(self) -> None:
        if not self.error_code.strip() or not self.error_message.strip():
            raise ValueError("cleanup refusal error facts must be non-blank")


def cleanup_journal_snapshot(
    journal: ProcessingCleanupJournalRow,
) -> CleanupJournalSnapshot:
    """Detach the exact logical snapshot of one incomplete journal row."""
    if (
        journal["completed_receipt"] is not None
        or journal["completed_at"] is not None
    ):
        raise ValueError("cleanup refusal requires an incomplete journal")
    destination_manifest = journal["destination_manifest"]
    return CleanupJournalSnapshot(
        job_id=journal["job_id"],
        request_id=journal["request_id"],
        revision=journal["revision"],
        action=journal["action"],
        source_path=journal["source_path"],
        source_manifest=[
            deepcopy(entry) for entry in journal["source_manifest"]
        ],
        source_manifest_hash=journal["source_manifest_hash"],
        destination_path=journal["destination_path"],
        destination_manifest=(
            None
            if destination_manifest is None
            else [deepcopy(entry) for entry in destination_manifest]
        ),
        destination_manifest_hash=journal["destination_manifest_hash"],
        selected_destination_path=journal["selected_destination_path"],
        step_progress=deepcopy(journal["step_progress"]),
    )


def cleanup_journal_refusal_disposition(
    journal: ProcessingCleanupJournalRow | None,
    *,
    error_code: str,
    error_message: str,
) -> CleanupJournalRefusalDisposition:
    """Capture a refusal without claiming any filesystem effect completed."""
    return CleanupJournalRefusalDisposition(
        error_code=error_code,
        error_message=error_message,
        journal=(
            None if journal is None else cleanup_journal_snapshot(journal)
        ),
    )


def cleanup_journal_refusal_matches(
    refusal: CleanupJournalRefusalDisposition,
    journal: ProcessingCleanupJournalRow | None,
) -> bool:
    """Compare a terminal refusal with the exact currently locked journal."""
    if refusal.journal is None:
        return journal is None
    if journal is None:
        return False
    try:
        return refusal.journal == cleanup_journal_snapshot(journal)
    except ValueError:
        return False


@dataclass(frozen=True)
class AutomationTerminalAuthority:
    """Exact processing-owner proof required by an automation terminal write."""

    expected_job_status: Literal["queued", "running", "recovery_required"]
    expected_preview_status: str | None
    expected_execution_lease: ExecutionLeaseSnapshot | None
    cleanup_receipt: CleanupJournalReceipt | None
    completion_receipt: AutomationCompletionReceipt | None = None
    cleanup_refusal: CleanupJournalRefusalDisposition | None = None

    def __post_init__(self) -> None:
        if (self.cleanup_receipt is None) == (self.cleanup_refusal is None):
            raise ValueError(
                "automation terminal authority requires exactly one cleanup "
                "receipt or refusal"
            )

    @property
    def cleanup_disposition(
        self,
    ) -> CleanupJournalReceipt | CleanupJournalRefusalDisposition:
        receipt = self.cleanup_receipt
        if receipt is not None:
            return receipt
        refusal = self.cleanup_refusal
        assert refusal is not None
        return refusal


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
            # issue #1176 PR3 F1: local_import was never added here, so a
            # successful strictly-validated local import (a genuine
            # verified-lossless terminal acceptance, exactly like force)
            # raised ValueError AFTER beets had already imported the
            # album — an unhandled crash outside every enclosing try, no
            # download_log row, no imported transition, no cleanup.
            or self.audit.outcome not in ("success", "force_import", "local_import")
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
class RequestRejectionOutcome:
    """The job-less sibling of ``PendingImportTerminalOutcome`` (issue #1355
    item 3).

    A job-backed rejection has no request-lifecycle authority of its own —
    it builds a ``PendingImportTerminalOutcome`` and waits for the owning
    import job to finish it later, atomically, alongside that job's own
    terminal status write. A job-less rejection has no import job to wait
    for, so this command carries everything needed to commit immediately:
    the optional request transition, the mandatory ``download_log`` audit
    row, and any denylist/cooldown entries. ``PipelineDB.
    persist_request_rejection_outcome`` commits all of it in one PostgreSQL
    transaction — a request transitioned back to ``wanted`` with no audit
    row explaining why, or denylisted peers with no matching transition, are
    exactly the partial-write worlds this type exists to make unreachable.

    ``transition`` is ``None`` for a rejection that neither requeues nor
    preserves ``imported`` (e.g. force-import's ``requeue_on_failure=False``
    lane) — the audit row and any denylist/cooldown entries still commit,
    the request's own lifecycle status is simply left untouched.
    """

    request_id: int
    audit: TerminalDownloadAudit
    transition: RequestTransition | None = None
    denylists: tuple[TerminalDenylist, ...] = ()
    cooldowns: tuple[TerminalCooldown, ...] = ()


@dataclass(frozen=True)
class RequestRejectionResult:
    """Rows and side effects produced by a committed job-less rejection."""

    download_log_id: int
    transition: TransitionApplied | None
    cooled_down_users: frozenset[str] = field(default_factory=lambda: frozenset())


def non_automation_failure_terminal_outcome(
    job: ImportJob,
    *,
    error: str,
    message: str,
    result: dict[str, object],
) -> ImportTerminalOutcome:
    """Build the sole terminal-and-visible failure command for force/
    local-import/YouTube.

    Non-automation attempts do not own a request lifecycle transition.  Their
    terminal record is instead one atomic pair: the exact job becomes failed
    and its origin download row gains a linked, operator-readable failure —
    except local-import (issue #1176 PR3, ``LocalImportPayload``), which has
    no origin ``download_log`` row at all: the operator names a request ID
    and a folder already on disk, not a rejected download, so
    ``source_download_log_id`` is ``None`` here on purpose. The PostgreSQL
    write (``lib/pipeline_db/terminal_outcomes.py``) already handles a
    ``None`` source cleanly — no origin CTE match, no spurious "unavailable
    or refused" provenance suffix — and already derives ``source='local'``
    for a ``local_import`` job type independent of any origin row.
    """
    if job.job_type == IMPORT_JOB_AUTOMATION:
        raise ValueError("automation failures require owner-terminal authority")
    if job.request_id is None:
        raise ValueError("non-automation import job requires a request")
    payload = job.payload
    if not isinstance(
        payload, (ForceImportPayload, YoutubeImportPayload, LocalImportPayload),
    ):
        raise TypeError("non-automation import job has no source download log")
    diagnostic = non_automation_import_failure_message(
        job.job_type,
        message,
        error,
    )
    if diagnostic in NON_AUTOMATION_IMPORT_FAILURE_PREFIXES:
        raise ValueError("non-automation failure requires a diagnostic")
    source_download_log_id = (
        payload.download_log_id
        if isinstance(payload, (ForceImportPayload, YoutubeImportPayload))
        else None
    )
    return ImportTerminalOutcome(
        request_id=job.request_id,
        import_job_id=job.id,
        initial_transition=None,
        audit=TerminalDownloadAudit(
            outcome="failed",
            error_message=diagnostic,
            source_download_log_id=source_download_log_id,
        ),
        job=ImportJobTerminal(
            status="failed",
            error=error,
            message=message,
            result=result,
        ),
    )

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


AUTOMATION_WORLD_FAILURE_RESULT_KEY = "automation_recovery_self_heal"


def automation_world_failure_self_heal(
    command: ImportTerminalOutcome,
) -> bool:
    """Return whether this is the exact fail-and-restart terminal shape."""
    authority = command.automation
    if authority is None:
        return False
    transitions = tuple(
        transition
        for transition in (
            command.initial_transition,
            *command.post_audit_transitions,
        )
        if transition is not None
    )
    reason = command.job.result.get(AUTOMATION_WORLD_FAILURE_RESULT_KEY)
    expected_stage = (
        authority.expected_job_status,
        authority.expected_preview_status,
    )
    return (
        (
            expected_stage
            in {
                ("queued", "running"),
                ("running", "evidence_ready"),
            }
            or authority.expected_job_status == "recovery_required"
        )
        and command.job.status == "failed"
        and isinstance(reason, str)
        and bool(reason.strip())
        and bool(transitions)
        and transitions[-1].target_status == "wanted"
        and not command.successful_terminal_acceptance
        and command.audit.outcome == "failed"
    )


def validate_automation_terminal_authority(
    command: ImportTerminalOutcome,
) -> None:
    """Validate the narrow terminal shapes that relax ordinary authority."""
    authority = command.automation
    if authority is None:
        return
    transitions = tuple(
        transition
        for transition in (
            command.initial_transition,
            *command.post_audit_transitions,
        )
        if transition is not None
    )
    if authority.cleanup_refusal is not None and (
        command.job.status != "failed"
        or not transitions
        or transitions[-1].target_status != "wanted"
        or command.successful_terminal_acceptance
        or command.audit.outcome != "failed"
    ):
        raise ValueError(
            "cleanup refusal must fail the job and return the request to wanted"
        )
    if (
        AUTOMATION_WORLD_FAILURE_RESULT_KEY in command.job.result
        and not automation_world_failure_self_heal(command)
    ):
        raise ValueError(
            "automation world-failure self-heal must fail the job, record a "
            "failed audit, and return the request to wanted"
        )
