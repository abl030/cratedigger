"""Typed in-process commands for terminal import/preview DB outcomes."""

from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Literal, TYPE_CHECKING

from lib.import_queue import ImportJob
from lib.transitions import RequestTransition, TransitionApplied
from lib.validation_envelope import (
    VALIDATION_PROJECTION_UNSET,
    ValidationProjectionUnset,
)

if TYPE_CHECKING:
    from lib.pipeline_db.download_log import DownloadLogOutcome


OPERATOR_SEARCH_STOP_STATUS = "manual"


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
        )

    def append_transitions(
        self,
        *transitions: RequestTransition,
    ) -> "PendingImportTerminalOutcome":
        return replace(
            self,
            post_audit_transitions=self.post_audit_transitions + transitions,
        )

    def append_denylists(
        self,
        *entries: TerminalDenylist,
    ) -> "PendingImportTerminalOutcome":
        return replace(self, denylists=self.denylists + entries)

    def mark_successful_terminal_acceptance(
        self,
    ) -> "PendingImportTerminalOutcome":
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


@dataclass(frozen=True)
class TerminalOutcomeResult:
    """Rows and side effects produced by a committed terminal bundle."""

    download_log_id: int
    job: ImportJob
    transitions: tuple[TransitionApplied, ...]
    cooled_down_users: frozenset[str] = field(default_factory=frozenset)
