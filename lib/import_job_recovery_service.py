"""Operator evidence and resolution for ambiguous Beets operations.

The automation recovery detail is deliberately observational.  In particular,
it never infers a completed Beets operation from a path or from current-library
membership.  Automation workers persist a typed ``automation_completion``
receipt in ``import_jobs.result`` when they have captured child completion;
until that writer lands, post-launch completion is ``unavailable`` rather than
the dangerously stronger ``absent``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, cast

import msgspec

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessProbe,
    OwnerSessionIdentity,
    OwnerSessionProbe,
    ProcessIdentity,
    ProcessObservation,
    probe_execution_liveness,
)
from lib.import_queue import ImportJob
from lib.json_narrow import is_str_object_dict
from lib.pipeline_db._core import OwnerSessionLost
from lib.pipeline_db._shared import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    release_id_to_lock_key,
)
from lib.pipeline_db.cleanup_journal import (
    CleanupJournalConflict,
    CleanupJournalIntent,
    CleanupJournalReceipt,
    ProcessingCleanupJournalRow,
)
from lib.pipeline_db.import_jobs import (
    AutomationRecoveryCAS,
    AutomationRecoveryEvidenceChanged,
    AutomationRecoveryRetryApplied,
)
from lib.pipeline_db.rows import AlbumRequestRow
from lib.pipeline_db.terminal_outcomes import ImportJobTerminalConflict
from lib.processing_cleanup import (
    PROCESSING_CLEANUP_NO_OP,
    PROCESSING_CLEANUP_REMOVE_SOURCE,
    ProcessingCleanupError,
    cleanup_manifest_builtins,
    cleanup_manifest_hash,
    execute_processing_cleanup,
    inspect_processing_cleanup_source,
)
from lib.quality.download_state import ActiveDownloadState
from lib.release_identity import ReleaseIdentity
from lib.terminal_outcomes import (
    ImportTerminalOutcome,
    TerminalOutcomeResult,
    automation_recovery_close_outcome,
)

logger = logging.getLogger("cratedigger")


RECOVERY_RESOLUTION_RETRY = "retry"
RECOVERY_RESOLUTION_CLOSE = "close"
AUTOMATION_COMPLETION_RESULT_KEY = "automation_completion"
RECOVERY_RESOLUTIONS = frozenset({
    RECOVERY_RESOLUTION_RETRY,
    RECOVERY_RESOLUTION_CLOSE,
})
_AUTOMATION_ACTIVE_STATUSES = frozenset({
    "queued",
    "running",
    "recovery_required",
})

CompletionObservationStatus = Literal[
    "captured",
    "absent",
    "unavailable",
]
ExactLibraryObservationStatus = Literal[
    "unique",
    "missing",
    "ambiguous",
    "unavailable",
]
CleanupJournalStatus = Literal[
    "missing",
    "incomplete",
    "completed",
    "unavailable",
]
CanonicalPathStatus = Literal["captured", "absent", "unavailable"]


class ImportRecoveryDB(Protocol):
    def get_import_job(self, job_id: int) -> ImportJob | None: ...

    def resolve_import_job_recovery(
        self,
        job_id: int,
        *,
        resolution: str,
        reason: str,
    ) -> tuple[ImportJob, ImportJob | None] | None: ...


class AutomationRecoveryDetailDB(Protocol):
    """Read-only PipelineDB shape for the shared recovery projection."""

    def get_import_job(self, job_id: int) -> ImportJob | None: ...

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def get_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
    ) -> ProcessingCleanupJournalRow | None: ...


class AutomationRecoveryMutationDB(
    AutomationRecoveryDetailDB,
    Protocol,
):
    def advisory_lock(self, namespace: int, key: int) -> Any: ...

    def _pin_owner_session(
        self,
        token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...

    def _probe_owner_session(
        self,
        identity: OwnerSessionIdentity,
    ) -> OwnerSessionProbe: ...

    def retry_automation_import_recovery(
        self,
        expected: AutomationRecoveryCAS,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None,
        liveness: ExecutionLivenessDecision,
        reason: str,
        evidence_revision: str,
    ) -> AutomationRecoveryRetryApplied | None: ...

    def require_automation_recovery_owner(
        self,
        expected: AutomationRecoveryCAS,
    ) -> None: ...

    def require_automation_recovery_cas(
        self,
        expected: AutomationRecoveryCAS,
    ) -> None: ...

    def create_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        intent: CleanupJournalIntent,
    ) -> ProcessingCleanupJournalRow: ...

    def checkpoint_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        step_progress: Mapping[str, object],
    ) -> ProcessingCleanupJournalRow: ...

    def complete_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
        expected_revision: int,
        receipt: CleanupJournalReceipt,
    ) -> ProcessingCleanupJournalRow: ...

    def persist_import_terminal_outcome(
        self,
        command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult: ...


class AutomationRecoveryBeets(Protocol):
    def resolve_current_release(
        self,
        identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...


class ImportRecoveryResolution(msgspec.Struct, frozen=True):
    outcome: str
    job: ImportJob | None = None
    retry_job: ImportJob | None = None
    message: str = ""


class AutomationCompletionReceipt(msgspec.Struct, frozen=True):
    """Durable child-completion writer contract for the terminal slice.

    The receipt identifies the exact operation whose child exit was captured.
    ``returncode`` records completion, not success; terminal policy interprets
    the subprocess result separately.
    """

    job_id: int
    request_id: int
    release_id: str
    canonical_path: str
    returncode: int
    captured_at: str


class AutomationRecoveryRequest(msgspec.Struct, frozen=True):
    request_id: int | None
    status: str | None
    active_owner_job_id: int | None
    artist_name: str | None
    album_title: str | None


class AutomationRecoveryRelease(msgspec.Struct, frozen=True):
    source: Literal["musicbrainz", "discogs"]
    release_id: str


class AutomationOwnerStage(msgspec.Struct, frozen=True):
    job_id: int
    job_type: str
    job_status: str
    preview_status: str | None
    exact_active_owner: bool


class AutomationLaunchFence(msgspec.Struct, frozen=True):
    authorized_at: str | None
    release_id: str | None
    source_path: str | None
    request_status: str | None
    snapshot_fingerprint: str | None


class PersistedExecutionLease(msgspec.Struct, frozen=True):
    invocation_id: str | None
    host_boot_id: str | None
    systemd_unit: str | None
    worker_pid: int | None
    worker_start_ticks: int | None
    beets_pid: int | None
    beets_start_ticks: int | None
    valid: bool
    validation_error: str | None = None


class ProcessLivenessTranscript(msgspec.Struct, frozen=True):
    pid: int
    start_ticks: int
    state: str
    observed_start_ticks: int | None
    cgroup_path: str | None
    reason: str


class InvocationLivenessTranscript(msgspec.Struct, frozen=True):
    state: str
    stored_invocation_id: str
    observed_invocation_id: str | None
    control_group: str | None
    reason: str
    active_state: str | None
    sub_state: str | None


class CgroupLivenessTranscript(msgspec.Struct, frozen=True):
    state: str
    path: str | None
    member_pids: tuple[int, ...]
    reason: str


class ExecutionLivenessTranscript(msgspec.Struct, frozen=True):
    current_host_boot_id: str | None
    boot_error: str | None
    worker: ProcessLivenessTranscript | None
    beets: ProcessLivenessTranscript | None
    invocation: InvocationLivenessTranscript | None
    cgroup: CgroupLivenessTranscript | None
    probe_error: str | None


class AutomationExecutionLiveness(msgspec.Struct, frozen=True):
    status: Literal["live", "dead", "unknown"]
    reason: str
    observed_at: str
    transcript: ExecutionLivenessTranscript


class AutomationCompletionObservation(msgspec.Struct, frozen=True):
    status: CompletionObservationStatus
    observed_at: str
    receipt: AutomationCompletionReceipt | None = None
    reason: str | None = None


class AutomationExactLibraryObservation(msgspec.Struct, frozen=True):
    status: ExactLibraryObservationStatus
    observed_at: str
    album_id: int | None = None
    album_path: str | None = None
    album_ids: tuple[int, ...] = ()
    reason: str | None = None


class AutomationCleanupJournalSnapshot(msgspec.Struct, frozen=True):
    status: CleanupJournalStatus
    observed_at: str
    job_id: int | None = None
    request_id: int | None = None
    revision: int | None = None
    action: str | None = None
    step_progress: dict[str, object] = msgspec.field(
        default_factory=dict[str, object],
    )
    completed_at: str | None = None
    completed_receipt: dict[str, object] | None = None
    declared_result_status: Literal["wanted", "imported"] | None = None
    declared_reason: str | None = None
    declared_evidence_revision: str | None = None
    reason: str | None = None


class AutomationRecoveryDetail(msgspec.Struct, frozen=True):
    request: AutomationRecoveryRequest
    release: AutomationRecoveryRelease | None
    owner_stage: AutomationOwnerStage
    canonical_path: str | None
    canonical_path_status: CanonicalPathStatus
    canonical_path_reason: str | None
    launch_fence: AutomationLaunchFence
    execution_lease: PersistedExecutionLease
    execution_liveness: AutomationExecutionLiveness
    completion: AutomationCompletionObservation
    exact_library: AutomationExactLibraryObservation
    cleanup_journal: AutomationCleanupJournalSnapshot
    evidence_revision: str
    close_eligible: bool
    close_block_reason: str | None


class AutomationRecoveryDetailResult(msgspec.Struct, frozen=True):
    outcome: Literal["ok", "not_found"]
    detail: AutomationRecoveryDetail | None = None
    message: str = ""

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


AutomationRecoveryActionOutcome = Literal[
    "retry_queued",
    "retry_recovery_required",
    "closed",
    "not_found",
    "wrong_state",
    "ineligible",
    "execution_live",
    "execution_unknown",
    "evidence_changed",
    "lock_unavailable",
    "cleanup_uninspectable",
    "cleanup_failed",
]


class AutomationRecoveryActionResult(msgspec.Struct, frozen=True):
    outcome: AutomationRecoveryActionOutcome
    detail: AutomationRecoveryDetail | None = None
    job: ImportJob | None = None
    retry_job: ImportJob | None = None
    message: str = ""

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


@dataclass(frozen=True)
class _AutomationRecoveryObservation:
    result: AutomationRecoveryDetailResult
    job: ImportJob | None
    lease: ExecutionLeaseSnapshot | None
    liveness: ExecutionLivenessDecision | None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _validate_completion_receipt(
    receipt: AutomationCompletionReceipt,
) -> None:
    if receipt.job_id <= 0 or receipt.request_id <= 0:
        raise ValueError("automation completion IDs must be positive")
    if not receipt.release_id.strip():
        raise ValueError("automation completion release_id must be nonblank")
    if not receipt.canonical_path.strip():
        raise ValueError("automation completion canonical_path must be nonblank")
    try:
        captured_at = datetime.fromisoformat(receipt.captured_at)
    except ValueError as exc:
        raise ValueError(
            "automation completion captured_at must be ISO8601"
        ) from exc
    if captured_at.tzinfo is None:
        raise ValueError(
            "automation completion captured_at must include a timezone"
        )


def automation_completion_result_patch(
    receipt: AutomationCompletionReceipt,
) -> dict[str, object]:
    """Return the exact JSONB patch automation terminal writers must persist."""
    _validate_completion_receipt(receipt)
    return {
        AUTOMATION_COMPLETION_RESULT_KEY: msgspec.to_builtins(receipt),
    }


def _persisted_lease(
    job: ImportJob,
) -> tuple[PersistedExecutionLease, ExecutionLeaseSnapshot | None]:
    wire = PersistedExecutionLease(
        invocation_id=job.execution_invocation_id,
        host_boot_id=job.execution_host_boot_id,
        systemd_unit=job.execution_systemd_unit,
        worker_pid=job.execution_worker_pid,
        worker_start_ticks=job.execution_worker_start_ticks,
        beets_pid=job.execution_beets_pid,
        beets_start_ticks=job.execution_beets_start_ticks,
        valid=False,
    )
    required = (
        wire.invocation_id,
        wire.host_boot_id,
        wire.systemd_unit,
        wire.worker_pid,
        wire.worker_start_ticks,
    )
    child = (wire.beets_pid, wire.beets_start_ticks)
    if all(value is None for value in (*required, *child)):
        return wire, None
    if any(value is None for value in required):
        return msgspec.structs.replace(
            wire,
            validation_error="persisted_execution_lease_incomplete",
        ), None
    if (wire.beets_pid is None) != (wire.beets_start_ticks is None):
        return msgspec.structs.replace(
            wire,
            validation_error="persisted_beets_lease_incomplete",
        ), None
    assert wire.invocation_id is not None
    assert wire.host_boot_id is not None
    assert wire.systemd_unit is not None
    assert wire.worker_pid is not None
    assert wire.worker_start_ticks is not None
    try:
        lease = ExecutionLeaseSnapshot(
            host_boot_id=wire.host_boot_id,
            invocation_id=wire.invocation_id,
            systemd_unit=wire.systemd_unit,
            worker=ProcessIdentity(
                wire.worker_pid,
                wire.worker_start_ticks,
            ),
            beets=(
                None
                if wire.beets_pid is None
                else ProcessIdentity(
                    wire.beets_pid,
                    wire.beets_start_ticks or 0,
                )
            ),
        )
    except ValueError:
        return msgspec.structs.replace(
            wire,
            validation_error="persisted_execution_lease_invalid",
        ), None
    return msgspec.structs.replace(wire, valid=True), lease


def _process_transcript(
    observation: ProcessObservation | None,
) -> ProcessLivenessTranscript | None:
    if observation is None:
        return None
    return ProcessLivenessTranscript(
        pid=observation.identity.pid,
        start_ticks=observation.identity.start_ticks,
        state=observation.state,
        observed_start_ticks=observation.observed_start_ticks,
        cgroup_path=observation.cgroup_path,
        reason=observation.reason,
    )


def _liveness_from_decision(
    decision: ExecutionLivenessDecision,
    *,
    observed_at: str,
) -> AutomationExecutionLiveness:
    evidence = decision.evidence
    invocation = evidence.invocation
    cgroup = evidence.cgroup
    return AutomationExecutionLiveness(
        status=decision.status,
        reason=decision.reason,
        observed_at=observed_at,
        transcript=ExecutionLivenessTranscript(
            current_host_boot_id=evidence.current_host_boot_id,
            boot_error=evidence.boot_error,
            worker=_process_transcript(evidence.worker),
            beets=_process_transcript(evidence.beets),
            invocation=(
                None
                if invocation is None
                else InvocationLivenessTranscript(
                    state=invocation.state,
                    stored_invocation_id=invocation.stored_invocation_id,
                    observed_invocation_id=invocation.observed_invocation_id,
                    control_group=invocation.control_group,
                    reason=invocation.reason,
                    active_state=invocation.active_state,
                    sub_state=invocation.sub_state,
                )
            ),
            cgroup=(
                None
                if cgroup is None
                else CgroupLivenessTranscript(
                    state=cgroup.state,
                    path=cgroup.path,
                    member_pids=cgroup.member_pids,
                    reason=cgroup.reason,
                )
            ),
            probe_error=evidence.probe_error,
        ),
    )


def _unknown_liveness(
    reason: str,
    *,
    observed_at: str,
) -> AutomationExecutionLiveness:
    return AutomationExecutionLiveness(
        status="unknown",
        reason=reason,
        observed_at=observed_at,
        transcript=ExecutionLivenessTranscript(
            current_host_boot_id=None,
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
            probe_error=reason,
        ),
    )


def _canonical_path(
    request: AlbumRequestRow | None,
) -> tuple[str | None, CanonicalPathStatus, str | None]:
    if request is None:
        return None, "unavailable", "request_unavailable"
    raw = request.get("active_download_state")
    if raw is None:
        return None, "absent", None
    try:
        state = ActiveDownloadState.from_raw(raw)
    except (TypeError, ValueError, msgspec.ValidationError):
        return None, "unavailable", "active_download_state_invalid"
    if state.current_path is None:
        return None, "absent", None
    if not state.current_path.strip():
        return None, "unavailable", "canonical_path_blank"
    return state.current_path, "captured", None


def _completion_observation(
    job: ImportJob,
    *,
    exact_active_owner: bool,
    canonical_path: str | None,
    observed_at: str,
) -> AutomationCompletionObservation:
    has_receipt = (
        job.result is not None
        and AUTOMATION_COMPLETION_RESULT_KEY in job.result
    )
    if not has_receipt:
        if (
            exact_active_owner
            and job.completed_at is None
            and job.beets_launch_authorized_at is None
        ):
            return AutomationCompletionObservation(
                status="absent",
                observed_at=observed_at,
                reason="beets_launch_not_authorized",
            )
        return AutomationCompletionObservation(
            status="unavailable",
            observed_at=observed_at,
            reason=(
                "active_owner_completed_at_inconsistent"
                if exact_active_owner and job.completed_at is not None
                else "automation_completion_receipt_missing"
            ),
        )
    assert job.result is not None
    raw_receipt = job.result[AUTOMATION_COMPLETION_RESULT_KEY]
    try:
        receipt = msgspec.convert(
            raw_receipt,
            type=AutomationCompletionReceipt,
            strict=True,
        )
        _validate_completion_receipt(receipt)
    except (TypeError, ValueError, msgspec.ValidationError):
        return AutomationCompletionObservation(
            status="unavailable",
            observed_at=observed_at,
            reason="automation_completion_receipt_invalid",
        )
    if not exact_active_owner or job.completed_at is not None:
        return AutomationCompletionObservation(
            status="unavailable",
            observed_at=observed_at,
            reason="completion_receipt_not_for_active_owner",
        )
    if (
        job.request_id is None
        or receipt.job_id != job.id
        or receipt.request_id != job.request_id
        or receipt.release_id != job.beets_launch_release_id
        or receipt.canonical_path != canonical_path
        or receipt.canonical_path != job.beets_launch_source_path
        or job.beets_launch_authorized_at is None
    ):
        return AutomationCompletionObservation(
            status="unavailable",
            observed_at=observed_at,
            reason="automation_completion_receipt_authority_mismatch",
        )
    return AutomationCompletionObservation(
        status="captured",
        observed_at=observed_at,
        receipt=receipt,
    )


def _library_observation(
    beets: AutomationRecoveryBeets | None,
    identity: ReleaseIdentity | None,
    *,
    observed_at: str,
) -> AutomationExactLibraryObservation:
    if beets is None or identity is None:
        return AutomationExactLibraryObservation(
            status="unavailable",
            observed_at=observed_at,
            reason=(
                "beets_db_unavailable"
                if beets is None
                else "release_identity_unavailable"
            ),
        )
    try:
        current = beets.resolve_current_release(identity)
    except Exception as exc:  # noqa: BLE001 - observation boundary
        return AutomationExactLibraryObservation(
            status="unavailable",
            observed_at=observed_at,
            reason=f"library_probe_failed:{type(exc).__name__}",
        )
    match current:
        case CurrentBeetsUnique():
            return AutomationExactLibraryObservation(
                status="unique",
                observed_at=observed_at,
                album_id=current.album_id,
                album_path=current.album_path,
            )
        case CurrentBeetsMissing():
            return AutomationExactLibraryObservation(
                status="missing",
                observed_at=observed_at,
            )
        case CurrentBeetsAmbiguous():
            return AutomationExactLibraryObservation(
                status="ambiguous",
                observed_at=observed_at,
                album_ids=current.album_ids,
                reason=current.reason,
            )
        case _:
            return AutomationExactLibraryObservation(
                status="unavailable",
                observed_at=observed_at,
                reason="library_probe_invalid_result",
            )


def _cleanup_snapshot(
    db: AutomationRecoveryDetailDB,
    *,
    request_id: int | None,
    job_id: int,
    observed_at: str,
) -> AutomationCleanupJournalSnapshot:
    if request_id is None:
        return AutomationCleanupJournalSnapshot(
            status="unavailable",
            observed_at=observed_at,
            reason="request_id_unavailable",
        )
    try:
        row = db.get_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
        )
    except Exception as exc:  # noqa: BLE001 - observation boundary
        return AutomationCleanupJournalSnapshot(
            status="unavailable",
            observed_at=observed_at,
            reason=f"cleanup_journal_probe_failed:{type(exc).__name__}",
        )
    if row is None:
        return AutomationCleanupJournalSnapshot(
            status="missing",
            observed_at=observed_at,
        )
    try:
        receipt = row["completed_receipt"]
        completed_at = row["completed_at"]
        if (receipt is None) != (completed_at is None):
            status: CleanupJournalStatus = "unavailable"
            reason = "cleanup_journal_completion_inconsistent"
        elif receipt is not None:
            status = "completed"
            reason = None
        else:
            status = "incomplete"
            reason = None
        return AutomationCleanupJournalSnapshot(
            status=status,
            observed_at=observed_at,
            job_id=row["job_id"],
            request_id=row["request_id"],
            revision=row["revision"],
            action=row["action"],
            step_progress=msgspec.convert(
                msgspec.to_builtins(row["step_progress"]),
                type=dict[str, object],
            ),
            completed_at=_iso(completed_at),
            completed_receipt=(
                None
                if receipt is None
                else msgspec.convert(
                    msgspec.to_builtins(receipt),
                    type=dict[str, object],
                )
            ),
            declared_result_status=row["declared_result_status"],
            declared_reason=row["declared_reason"],
            declared_evidence_revision=row["evidence_revision"],
            reason=reason,
        )
    except Exception as exc:  # noqa: BLE001 - observation boundary
        return AutomationCleanupJournalSnapshot(
            status="unavailable",
            observed_at=observed_at,
            reason=f"cleanup_journal_decode_failed:{type(exc).__name__}",
        )


def _close_block_reason(
    *,
    job: ImportJob,
    request: AlbumRequestRow | None,
    exact_active_owner: bool,
    release: ReleaseIdentity | None,
    path_status: CanonicalPathStatus,
    liveness: AutomationExecutionLiveness,
    cleanup: AutomationCleanupJournalSnapshot,
) -> str | None:
    if job.job_type != "automation_import":
        return "not_automation_import"
    if request is None:
        return "request_unavailable"
    if not exact_active_owner:
        return "not_exact_processing_owner"
    if job.status not in _AUTOMATION_ACTIVE_STATUSES:
        return "owner_stage_ineligible"
    if job.completed_at is not None:
        return "owner_stage_inconsistent"
    if liveness.status == "live":
        return "execution_live"
    if liveness.status == "unknown":
        return "execution_liveness_unknown"
    if release is None:
        return "release_identity_unavailable"
    if path_status == "absent":
        return "canonical_path_absent"
    if path_status == "unavailable":
        return "canonical_path_unavailable"
    if cleanup.status == "unavailable":
        return "cleanup_journal_unavailable"
    if cleanup.status == "incomplete" and (
        cleanup.declared_result_status is None
        or cleanup.declared_reason is None
        or cleanup.declared_evidence_revision is None
    ):
        return "cleanup_journal_incomplete"
    return None


def _revision_payload(
    detail: AutomationRecoveryDetail,
) -> dict[str, object]:
    payload: dict[str, object] = msgspec.to_builtins(detail)
    payload.pop("evidence_revision")
    payload.pop("close_eligible")
    payload.pop("close_block_reason")
    liveness = payload["execution_liveness"]
    completion = payload["completion"]
    library = payload["exact_library"]
    cleanup = payload["cleanup_journal"]
    assert is_str_object_dict(liveness)
    assert is_str_object_dict(completion)
    assert is_str_object_dict(library)
    assert is_str_object_dict(cleanup)
    liveness.pop("observed_at")
    completion.pop("observed_at")
    library.pop("observed_at")
    cleanup.pop("observed_at")
    return payload


def _evidence_revision(detail: AutomationRecoveryDetail) -> str:
    encoded = json.dumps(
        _revision_payload(detail),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _observe_automation_recovery(
    db: AutomationRecoveryDetailDB,
    beets: AutomationRecoveryBeets | None,
    job_id: int,
    *,
    liveness_probe: ExecutionLivenessProbe | None = None,
    observed_at: datetime | None = None,
) -> _AutomationRecoveryObservation:
    """Observe one job once and retain its exact liveness decision."""
    job = db.get_import_job(int(job_id))
    if job is None:
        return _AutomationRecoveryObservation(
            result=AutomationRecoveryDetailResult(
                outcome="not_found",
                message=f"Import job {job_id} not found",
            ),
            job=None,
            lease=None,
            liveness=None,
        )
    request = (
        None
        if job.request_id is None
        else db.get_request(job.request_id)
    )
    raw_active_owner_id = (
        None
        if request is None
        else request.get("active_automation_import_job_id")
    )
    active_owner_id = (
        int(raw_active_owner_id)
        if isinstance(raw_active_owner_id, int)
        and not isinstance(raw_active_owner_id, bool)
        else None
    )
    exact_active_owner = (
        job.job_type == "automation_import"
        and job.request_id is not None
        and request is not None
        and request.get("status") == "processing"
        and active_owner_id == job.id
        and job.status in _AUTOMATION_ACTIVE_STATUSES
    )
    identity = (
        None
        if request is None
        else ReleaseIdentity.from_strict_fields(
            request.get("mb_release_id"),
            request.get("discogs_release_id"),
        )
    )
    canonical_path, path_status, path_reason = _canonical_path(request)
    observed = (observed_at or datetime.now(UTC)).isoformat()
    cleanup = _cleanup_snapshot(
        db,
        request_id=job.request_id,
        job_id=job.id,
        observed_at=observed,
    )
    persisted_lease, lease = _persisted_lease(job)
    retained_cleanup_never_claimed = (
        job.status == "recovery_required"
        and job.beets_launch_authorized_at is None
        and cleanup.status in {"incomplete", "completed"}
    )
    liveness_decision: ExecutionLivenessDecision | None
    if persisted_lease.validation_error is not None:
        liveness_decision = None
        liveness = _unknown_liveness(
            persisted_lease.validation_error,
            observed_at=observed,
        )
    elif lease is None and (
        (
            job.status != "queued"
            and not retained_cleanup_never_claimed
        )
        or job.beets_launch_authorized_at is not None
    ):
        liveness_decision = None
        liveness = _unknown_liveness(
            "persisted_execution_lease_missing",
            observed_at=observed,
        )
    else:
        liveness_decision = probe_execution_liveness(
            lease,
            probe=liveness_probe,
        )
        liveness = _liveness_from_decision(
            liveness_decision,
            observed_at=observed,
        )
    completion = _completion_observation(
        job,
        exact_active_owner=exact_active_owner,
        canonical_path=canonical_path,
        observed_at=observed,
    )
    library = _library_observation(
        beets,
        identity,
        observed_at=observed,
    )
    block_reason = _close_block_reason(
        job=job,
        request=request,
        exact_active_owner=exact_active_owner,
        release=identity,
        path_status=path_status,
        liveness=liveness,
        cleanup=cleanup,
    )
    detail = AutomationRecoveryDetail(
        request=AutomationRecoveryRequest(
            request_id=job.request_id,
            status=None if request is None else request["status"],
            active_owner_job_id=active_owner_id,
            artist_name=None if request is None else request["artist_name"],
            album_title=None if request is None else request["album_title"],
        ),
        release=(
            None
            if identity is None
            else AutomationRecoveryRelease(
                source=identity.source,
                release_id=identity.release_id,
            )
        ),
        owner_stage=AutomationOwnerStage(
            job_id=job.id,
            job_type=job.job_type,
            job_status=job.status,
            preview_status=job.preview_status,
            exact_active_owner=exact_active_owner,
        ),
        canonical_path=canonical_path,
        canonical_path_status=path_status,
        canonical_path_reason=path_reason,
        launch_fence=AutomationLaunchFence(
            authorized_at=_iso(job.beets_launch_authorized_at),
            release_id=job.beets_launch_release_id,
            source_path=job.beets_launch_source_path,
            request_status=job.beets_launch_request_status,
            snapshot_fingerprint=job.beets_launch_snapshot_fingerprint,
        ),
        execution_lease=persisted_lease,
        execution_liveness=liveness,
        completion=completion,
        exact_library=library,
        cleanup_journal=cleanup,
        evidence_revision="",
        close_eligible=block_reason is None,
        close_block_reason=block_reason,
    )
    detail = msgspec.structs.replace(
        detail,
        evidence_revision=_evidence_revision(detail),
    )
    return _AutomationRecoveryObservation(
        result=AutomationRecoveryDetailResult(
            outcome="ok",
            detail=detail,
            message="Automation recovery evidence observed",
        ),
        job=job,
        lease=lease,
        liveness=liveness_decision,
    )


def get_automation_recovery_detail(
    db: AutomationRecoveryDetailDB,
    beets: AutomationRecoveryBeets | None,
    job_id: int,
    *,
    liveness_probe: ExecutionLivenessProbe | None = None,
    observed_at: datetime | None = None,
) -> AutomationRecoveryDetailResult:
    """Observe one job through the shared fail-closed recovery read model."""
    return _observe_automation_recovery(
        db,
        beets,
        job_id,
        liveness_probe=liveness_probe,
        observed_at=observed_at,
    ).result


def _automation_recovery_cas(
    detail: AutomationRecoveryDetail,
    job: ImportJob,
) -> AutomationRecoveryCAS:
    if detail.request.request_id is None or detail.canonical_path is None:
        raise ValueError("automation recovery owner evidence is incomplete")
    cleanup = detail.cleanup_journal
    cleanup_present = cleanup.status in {"incomplete", "completed"}
    return AutomationRecoveryCAS(
        request_id=detail.request.request_id,
        job_id=job.id,
        job_status=job.status,
        preview_status=job.preview_status,
        canonical_path=detail.canonical_path,
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
        cleanup_job_id=cleanup.job_id if cleanup_present else None,
        cleanup_request_id=(
            cleanup.request_id if cleanup_present else None
        ),
        cleanup_revision=cleanup.revision if cleanup_present else None,
        cleanup_progress=(
            _json_object(cleanup.step_progress)
            if cleanup_present
            else None
        ),
    )


def _json_object(value: object) -> dict[str, object]:
    return msgspec.convert(
        msgspec.to_builtins(value),
        type=dict[str, object],
    )


def _refreshed_action_result(
    db: AutomationRecoveryDetailDB,
    beets: AutomationRecoveryBeets | None,
    job_id: int,
    *,
    outcome: AutomationRecoveryActionOutcome,
    message: str,
    liveness_probe: ExecutionLivenessProbe | None,
) -> AutomationRecoveryActionResult:
    refreshed = get_automation_recovery_detail(
        db,
        beets,
        job_id,
        liveness_probe=liveness_probe,
    )
    return AutomationRecoveryActionResult(
        outcome=outcome,
        detail=refreshed.detail,
        message=message,
    )


def _automation_action_block(
    observation: _AutomationRecoveryObservation,
    *,
    action: Literal["retry", "close"],
) -> tuple[AutomationRecoveryActionOutcome, str] | None:
    detail = observation.result.detail
    assert detail is not None
    if not detail.owner_stage.exact_active_owner:
        return "wrong_state", "Job is not the exact processing owner"
    if observation.liveness is None \
            or detail.execution_liveness.status == "unknown":
        return "execution_unknown", detail.execution_liveness.reason
    if detail.execution_liveness.status == "live":
        return "execution_live", detail.execution_liveness.reason
    if action == "retry" and detail.owner_stage.job_status != "recovery_required":
        return (
            "wrong_state",
            "Automation retry requires a recovery_required owner",
        )
    if detail.release is None:
        return "ineligible", "Release identity is unavailable"
    if detail.canonical_path_status != "captured":
        return (
            "ineligible",
            detail.canonical_path_reason
            or f"Canonical path is {detail.canonical_path_status}",
        )
    if detail.cleanup_journal.status == "unavailable":
        return (
            "ineligible",
            detail.cleanup_journal.reason or "Cleanup journal is unavailable",
        )
    return None


def _checkpoint_automation_recovery_close(
    db: AutomationRecoveryMutationDB,
    *,
    token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
    expected: AutomationRecoveryCAS,
) -> None:
    """Fail-stop unless the same live session still holds the exact owner."""
    token.raise_if_cancelled()
    probe = db._probe_owner_session(owner_session_identity)
    if not probe.live:
        token.cancel(f"owner_session_lost:{probe.reason}")
        token.raise_if_cancelled()
    db.require_automation_recovery_owner(expected)
    token.raise_if_cancelled()


def _apply_import_job_recovery(
    db: AutomationRecoveryMutationDB,
    beets: AutomationRecoveryBeets | None,
    job_id: int,
    *,
    action: Literal["retry", "close"],
    reason: str,
    evidence_revision: str | None = None,
    result_status: Literal["wanted", "imported"] | None = None,
    liveness_probe: ExecutionLivenessProbe | None = None,
) -> AutomationRecoveryActionResult:
    """Apply one legacy or revisioned exact-owner recovery action."""
    reason = reason.strip()
    if not reason:
        raise ValueError("recovery resolution requires a non-empty reason")

    current = db.get_import_job(int(job_id))
    if current is None:
        return AutomationRecoveryActionResult(
            outcome="not_found",
            message=f"Import job {job_id} not found",
        )
    if current.job_type != "automation_import":
        legacy = resolve_import_job_recovery(
            cast(ImportRecoveryDB, db),
            job_id,
            resolution=action,
            reason=reason,
        )
        if legacy.outcome == "not_found":
            outcome: AutomationRecoveryActionOutcome = "not_found"
        elif legacy.outcome in {"wrong_state", "authority_changed"}:
            outcome = "wrong_state"
        elif legacy.outcome == "retry_queued":
            outcome = "retry_queued"
        else:
            outcome = "closed"
        return AutomationRecoveryActionResult(
            outcome=outcome,
            job=legacy.job,
            retry_job=legacy.retry_job,
            message=legacy.message,
        )

    if evidence_revision is None or not evidence_revision.strip():
        raise ValueError(
            "automation recovery requires --evidence-revision"
        )
    if action == "close" and result_status not in {"wanted", "imported"}:
        raise ValueError(
            "automation recovery close requires result_status "
            "'wanted' or 'imported'"
        )
    if action == "retry" and result_status is not None:
        raise ValueError("automation recovery retry does not accept result_status")

    observed = _observe_automation_recovery(
        db,
        beets,
        job_id,
        liveness_probe=liveness_probe,
    )
    detail = observed.result.detail
    if detail is None or observed.job is None:
        return AutomationRecoveryActionResult(
            outcome="not_found",
            message=observed.result.message,
        )
    if evidence_revision != detail.evidence_revision:
        return AutomationRecoveryActionResult(
            outcome="evidence_changed",
            detail=detail,
            message="Automation recovery evidence changed; review and resubmit",
        )
    blocked = _automation_action_block(observed, action=action)
    if blocked is not None:
        return AutomationRecoveryActionResult(
            outcome=blocked[0],
            detail=detail,
            message=blocked[1],
        )
    assert observed.liveness is not None
    expected = _automation_recovery_cas(detail, observed.job)

    if action == "retry":
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            expected.request_id,
        ) as acquired:
            if not acquired:
                return AutomationRecoveryActionResult(
                    outcome="lock_unavailable",
                    detail=detail,
                    message="Automation import owner is locked",
                )
            applied = db.retry_automation_import_recovery(
                expected,
                expected_execution_lease=observed.lease,
                liveness=observed.liveness,
                reason=reason,
                evidence_revision=evidence_revision,
            )
        if applied is None:
            return _refreshed_action_result(
                db,
                beets,
                job_id,
                outcome="evidence_changed",
                message=(
                    "Automation recovery evidence changed; review and resubmit"
                ),
                liveness_probe=liveness_probe,
            )
        retained_cleanup = applied.journal is not None
        return AutomationRecoveryActionResult(
            outcome=(
                "retry_recovery_required"
                if retained_cleanup
                else "retry_queued"
            ),
            job=applied.original,
            retry_job=applied.retry,
            message=(
                (
                    f"Closed ambiguous job {job_id}, retargeted its cleanup "
                    f"journal to automation job {applied.retry.id}, and kept "
                    "the replacement recovery-required"
                )
                if retained_cleanup
                else (
                    f"Closed ambiguous job {job_id} and queued fresh "
                    f"automation job {applied.retry.id}"
                )
            ),
        )

    assert result_status is not None
    assert detail.release is not None
    request_id = expected.request_id
    release_key = release_id_to_lock_key(detail.release.release_id)
    token = CancellationToken()
    with (
        db._pin_owner_session(token) as owner_session_identity,
        db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            request_id,
        ) as import_acquired,
    ):
        if not import_acquired:
            return AutomationRecoveryActionResult(
                outcome="lock_unavailable",
                detail=detail,
                message="Automation import owner is locked",
            )
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_key,
        ) as release_acquired:
            if not release_acquired:
                return AutomationRecoveryActionResult(
                    outcome="lock_unavailable",
                    detail=detail,
                    message="Exact release is locked",
                )
            owner_checkpoint = lambda: _checkpoint_automation_recovery_close(
                db,
                token=token,
                owner_session_identity=owner_session_identity,
                expected=expected,
            )
            owner_checkpoint()
            try:
                db.require_automation_recovery_cas(expected)
            except AutomationRecoveryEvidenceChanged:
                return _refreshed_action_result(
                    db,
                    beets,
                    job_id,
                    outcome="evidence_changed",
                    message=(
                        "Automation recovery evidence changed; "
                        "review and resubmit"
                    ),
                    liveness_probe=liveness_probe,
                )

            journal = db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
            )
            if journal is None:
                inspection = inspect_processing_cleanup_source(
                    expected.canonical_path
                )
                if inspection.status == "uninspectable":
                    return AutomationRecoveryActionResult(
                        outcome="cleanup_uninspectable",
                        detail=detail,
                        message=(
                            inspection.reason
                            or "Canonical processing source is uninspectable"
                        ),
                    )
                if inspection.status == "complete":
                    action_name = PROCESSING_CLEANUP_REMOVE_SOURCE
                    manifest = cleanup_manifest_builtins(
                        inspection.manifest
                    )
                    manifest_hash = inspection.manifest_hash
                    assert manifest_hash is not None
                else:
                    action_name = PROCESSING_CLEANUP_NO_OP
                    manifest = ()
                    manifest_hash = cleanup_manifest_hash(())
                intent = CleanupJournalIntent(
                    action=action_name,
                    source_path=expected.canonical_path,
                    source_manifest=manifest,
                    source_manifest_hash=manifest_hash,
                    declared_result_status=result_status,
                    declared_reason=reason,
                    evidence_revision=evidence_revision,
                )
                owner_checkpoint()
                try:
                    journal = db.create_processing_cleanup_journal(
                        request_id=request_id,
                        job_id=job_id,
                        intent=intent,
                    )
                except CleanupJournalConflict:
                    return _refreshed_action_result(
                        db,
                        beets,
                        job_id,
                        outcome="evidence_changed",
                        message=(
                            "Cleanup authority changed; review and resubmit"
                        ),
                        liveness_probe=liveness_probe,
                    )
            elif (
                journal["declared_result_status"] != result_status
                or journal["declared_reason"] != reason
                or journal["evidence_revision"] is None
            ):
                return AutomationRecoveryActionResult(
                    outcome="ineligible",
                    detail=detail,
                    message=(
                        "Existing cleanup declaration does not match this close"
                    ),
                )

            try:
                completed = execute_processing_cleanup(
                    db,
                    journal,
                    owner_checkpoint=owner_checkpoint,
                )
            except AutomationRecoveryEvidenceChanged:
                return _refreshed_action_result(
                    db,
                    beets,
                    job_id,
                    outcome="evidence_changed",
                    message=(
                        "Automation recovery owner changed during cleanup"
                    ),
                    liveness_probe=liveness_probe,
                )
            except (CleanupJournalConflict, ProcessingCleanupError) as exc:
                return AutomationRecoveryActionResult(
                    outcome="cleanup_failed",
                    detail=detail,
                    message=str(exc),
                )
            receipt = completed["completed_receipt"]
            if receipt is None:
                return AutomationRecoveryActionResult(
                    outcome="cleanup_failed",
                    detail=detail,
                    message="Cleanup completed without a typed receipt",
                )
            terminal_revision = completed["evidence_revision"]
            if terminal_revision is None:
                return AutomationRecoveryActionResult(
                    outcome="cleanup_failed",
                    detail=detail,
                    message="Cleanup declaration lacks an evidence revision",
                )
            completion = (
                detail.completion.receipt
                if detail.completion.status == "captured"
                else None
            )
            command = automation_recovery_close_outcome(
                request_id=request_id,
                import_job_id=job_id,
                result_status=result_status,
                reason=reason,
                evidence_revision=terminal_revision,
                expected_job_status=cast(
                    Literal["queued", "running", "recovery_required"],
                    detail.owner_stage.job_status,
                ),
                expected_preview_status=detail.owner_stage.preview_status,
                expected_execution_lease=observed.lease,
                cleanup_receipt=receipt,
                completion_receipt=completion,
            )
            owner_checkpoint()
            try:
                terminal = db.persist_import_terminal_outcome(command)
            except (
                AutomationRecoveryEvidenceChanged,
                ImportJobTerminalConflict,
                CleanupJournalConflict,
            ):
                return _refreshed_action_result(
                    db,
                    beets,
                    job_id,
                    outcome="evidence_changed",
                    message=(
                        "Automation recovery evidence changed before close"
                    ),
                    liveness_probe=liveness_probe,
                )
            return AutomationRecoveryActionResult(
                outcome="closed",
                job=terminal.job,
                message=(
                    f"Closed automation job {job_id} as {result_status}"
                ),
            )


def apply_import_job_recovery(
    db: AutomationRecoveryMutationDB,
    beets: AutomationRecoveryBeets | None,
    job_id: int,
    *,
    action: Literal["retry", "close"],
    reason: str,
    evidence_revision: str | None = None,
    result_status: Literal["wanted", "imported"] | None = None,
    liveness_probe: ExecutionLivenessProbe | None = None,
) -> AutomationRecoveryActionResult:
    """Apply recovery and convert owner-session loss into fail-stop evidence."""
    try:
        return _apply_import_job_recovery(
            db,
            beets,
            job_id,
            action=action,
            reason=reason,
            evidence_revision=evidence_revision,
            result_status=result_status,
            liveness_probe=liveness_probe,
        )
    except (ExecutionCancelled, OwnerSessionLost) as exc:
        if action != "close":
            raise
        return AutomationRecoveryActionResult(
            outcome="cleanup_failed",
            message=(
                "Automation recovery close lost its pinned owner session: "
                f"{exc}"
            ),
        )


def resolve_import_job_recovery(
    db: ImportRecoveryDB,
    job_id: int,
    *,
    resolution: str,
    reason: str,
) -> ImportRecoveryResolution:
    """Apply one explicit operator decision without inferring Beets state."""
    if resolution not in RECOVERY_RESOLUTIONS:
        raise ValueError(
            "resolution must be 'retry' (operator confirmed not applied) "
            "or 'close' (operator reconciled without replay)"
        )
    reason = reason.strip()
    if not reason:
        raise ValueError("recovery resolution requires a non-empty reason")

    current = db.get_import_job(int(job_id))
    if current is None:
        return ImportRecoveryResolution(
            outcome="not_found",
            message=f"Import job {job_id} not found",
        )
    if current.status != "recovery_required":
        return ImportRecoveryResolution(
            outcome="wrong_state",
            job=current,
            message=(
                f"Import job {job_id} is {current.status!r}, not "
                "'recovery_required'"
            ),
        )

    resolved = db.resolve_import_job_recovery(
        int(job_id),
        resolution=resolution,
        reason=reason,
    )
    if resolved is None:
        latest = db.get_import_job(int(job_id))
        return ImportRecoveryResolution(
            outcome="authority_changed",
            job=latest,
            message=(
                "Recovery authority changed; inspect the request, release, "
                "and source before trying again"
            ),
        )
    job, retry_job = resolved
    if job.job_type == "force_import" and job.preview_result is not None:
        action_path = job.preview_result.get("action_path")
        if isinstance(action_path, str) and action_path:
            try:
                from lib.config import read_runtime_config
                from lib.import_preview import cleanup_force_action_copy_for_job

                cleanup_force_action_copy_for_job(
                    action_path,
                    read_runtime_config(),
                    import_job_id=job.id,
                )
            except Exception:
                # The operator resolution is durable. A private deterministic
                # copy can be reclaimed later; it must not undo that decision.
                logger.exception(
                    "Failed to remove resolved force action copy for job %s", job.id,
                )
    return ImportRecoveryResolution(
        outcome=("retry_queued" if retry_job is not None else "closed"),
        job=job,
        retry_job=retry_job,
        message=(
            f"Queued fresh import job {retry_job.id}"
            if retry_job is not None
            else "Recovery closed without automatic replay"
        ),
    )
