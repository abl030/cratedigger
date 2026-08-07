"""Read-only diagnostic evidence for ambiguous Beets operations.

The automation recovery detail is deliberately observational.  In particular,
it never infers a completed Beets operation from a path or from current-library
membership.  Automation workers persist a typed ``automation_completion``
receipt in ``import_jobs.result`` when they have captured child completion;
until that writer lands, post-launch completion is ``unavailable`` rather than
the dangerously stronger ``absent``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Literal, Protocol

import msgspec

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessProbe,
    ProcessIdentity,
    ProcessObservation,
    probe_execution_liveness,
)
from lib.import_queue import ImportJob
from lib.pipeline_db.cleanup_journal import ProcessingCleanupJournalRow
from lib.pipeline_db.rows import AlbumRequestRow
from lib.quality.download_state import ActiveDownloadState
from lib.release_identity import ReleaseIdentity
from lib.request_identity import resolve_current_for_request

AUTOMATION_COMPLETION_RESULT_KEY = "automation_completion"
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


class AutomationRecoveryBeets(Protocol):
    def resolve_current_release(
        self,
        identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...


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


class AutomationRecoveryDetailResult(msgspec.Struct, frozen=True):
    outcome: Literal["ok", "not_found"]
    detail: AutomationRecoveryDetail | None = None
    message: str = ""

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


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
    row: Mapping[str, object] | None = None,
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
        # A request row is the identity authority.  An omitted union answer
        # therefore cannot be laundered through an acquisition-only probe:
        # doing so may turn unavailable current state into a misleading
        # unique observation that later cleanup trusts.  Untracked jobs have
        # no row and retain the exact-release observation contract.
        current = (
            resolve_current_for_request(beets, row)
            if row is not None
            else beets.resolve_current_release(identity)
        )
        if current is None:
            return AutomationExactLibraryObservation(
                status="unavailable",
                observed_at=observed_at,
                reason="request_union_authority_unavailable",
            )
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
        step_progress: dict[str, object] = msgspec.to_builtins(
            row["step_progress"]
        )
        completed_receipt: dict[str, object] | None = (
            None
            if receipt is None
            else msgspec.to_builtins(receipt)
        )
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
            step_progress=step_progress,
            completed_at=_iso(completed_at),
            completed_receipt=completed_receipt,
            reason=reason,
        )
    except Exception as exc:  # noqa: BLE001 - observation boundary
        return AutomationCleanupJournalSnapshot(
            status="unavailable",
            observed_at=observed_at,
            reason=f"cleanup_journal_decode_failed:{type(exc).__name__}",
        )


def _observe_automation_recovery(
    db: AutomationRecoveryDetailDB,
    beets: AutomationRecoveryBeets | None,
    job_id: int,
    *,
    liveness_probe: ExecutionLivenessProbe | None = None,
    observed_at: datetime | None = None,
) -> AutomationRecoveryDetailResult:
    """Observe one job through the fail-closed read model."""
    job = db.get_import_job(int(job_id))
    if job is None:
        return AutomationRecoveryDetailResult(
            outcome="not_found",
            message=f"Import job {job_id} not found",
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
    historical_retained_cleanup_never_claimed = (
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
            and not historical_retained_cleanup_never_claimed
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
        row=request,
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
    )
    return AutomationRecoveryDetailResult(
        outcome="ok",
        detail=detail,
        message="Automation recovery evidence observed",
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
    )
