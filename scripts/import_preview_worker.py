"""Run async no-mutation previews for queued import jobs."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import threading
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from cratedigger import TrackRecord

import msgspec
import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import (
    CratediggerConfig,
    read_runtime_config,
    resolve_startup_config_paths,
)
from lib.current_library_evidence import (
    CurrentLibraryAuthorityUnavailable,
    CurrentLibraryEvidence,
    HaveEnrichment,
    HavePreparation,
    enrich_incomplete_current_evidence_for_request,
    load_current_evidence_for_preview,
    persist_measured_have_spectral,
    prepare_current_evidence_for_failure,
    resolve_current_library_evidence,
)
from lib.dispatch import _record_preview_measurement_failed
from lib.dispatch.types import DispatchOutcome, PostCommitCleanup
from lib.import_evidence import (
    CANDIDATE_STATUS_REUSED,
    ensure_candidate_evidence_for_action,
)
from lib.import_execution import (
    AutomationOwnerFailStop,
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    ExecutionLivenessProbe,
    OwnerSessionIdentity,
    probe_execution_liveness,
)
from lib.import_preview import (
    PREVIEW_VERDICT_EVIDENCE_READY,
    PREVIEW_VERDICT_MEASUREMENT_FAILED,
    ImportPreviewResult,
    measure_and_persist_candidate_evidence,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_YOUTUBE,
    ForceImportPayload,
    ImportJob,
    LocalImportPayload,
    YoutubeImportPayload,
)
from lib.import_worker_loop import (
    CandidateScanCursor,
    ClaimState,
    capture_worker_execution_lease,
    claim_one_candidate,
    execution_lease_from_job,
    stage_dsn,
)
from lib.measurement import (
    ExistingSpectralAuditLookup,
    ExistingSpectralResolver,
    SpectralDetailAnalyzer,
    analyze_spectral_audit_path,
    collect_release_attempt_spectral_audit,
    existing_spectral_resolver_for_config,
    spectral_detail_from_persisted_source,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    DEFAULT_DSN,
    PipelineDB,
)
from lib.pipeline_db._core import OwnerSessionLost
from lib.preview_snapshot import (
    ACTION_COPY_PREFIX_BY_JOB_TYPE,
    FORCE_ACTION_PREFIX,
    LOCAL_IMPORT_ACTION_PREFIX,
    cleanup_force_action_copy_for_job,
    force_action_copy_path,
    remove_preview_snapshot,
    retain_preview_snapshot_for_force_action,
    snapshot_configured_local_import_directory,
    snapshot_configured_quarantine_directory,
)
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality import (
    ActiveDownloadState,
    AlbumQualityEvidence,
    ImportResult,
    MeasurementFailure,
    SpectralAnalysisDetail,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    QualityEvidenceDB,
    audio_snapshot_matches,
    candidate_evidence_persistence_receipt_semantic_error,
    load_candidate_evidence_for_source,
)
from lib.terminal_outcomes import AutomationTerminalAuthority
from lib.validation_envelope import decode_validation_envelope

logger = logging.getLogger("cratedigger-import-preview-worker")
STALE_PREVIEW_MESSAGE = "Preview worker restarted while job was running; retry queued"
RESTART_PREVIEW_MESSAGE = "Preview worker restarted while job was running; retry queued"
PREVIEW_HEARTBEAT_INTERVAL_SECONDS = 30.0
PREVIEW_STALE_RECOVERY_INTERVAL_SECONDS = 60.0
PREVIEW_STALE_AGE = timedelta(minutes=15)
PREVIEW_SYSTEMD_UNIT = "cratedigger-import-preview-worker.service"
PREVIEW_CANDIDATE_SCAN_LIMIT = 32


FailureHavePrepareFn = Callable[..., HavePreparation]
FailureHaveEnrichFn = Callable[..., HaveEnrichment]


def _noop_header_repair(_path: str) -> None:
    """Keep force-preview measurement read-only."""


class AutomationPreviewTerminalHandoffRequired(RuntimeError):
    """U4 boundary: automation terminal outcomes need one owner bundle."""


@dataclass(frozen=True)
class _AutomationPreviewAuthority:
    request: dict[str, Any]
    state: ActiveDownloadState
    canonical_path: str


class _AutomationPreviewDelegate(Protocol):
    def set_import_job_candidate_evidence(
        self,
        import_job_id: int,
        evidence_id: int | None,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool: ...

    def mark_import_job_preview_importable(
        self,
        job_id: int,
        *,
        preview_result: dict[str, object] | None = None,
        message: str | None = None,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None: ...


class _AutomationPreviewDB:
    """Lease-aware adapter over the exact pinned automation DB session."""

    def __init__(
        self,
        db: _AutomationPreviewDelegate,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> None:
        self._db = db
        self._execution_lease = execution_lease

    def __getattr__(self, name: str) -> object:
        return getattr(self._db, name)

    def set_import_job_candidate_evidence(
        self,
        import_job_id: int,
        evidence_id: int | None,
    ) -> bool:
        return bool(self._db.set_import_job_candidate_evidence(
            import_job_id,
            evidence_id,
            expected_execution_lease=self._execution_lease,
        ))

    def mark_import_job_preview_importable(
        self,
        import_job_id: int,
        *,
        preview_result: dict[str, object] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        return self._db.mark_import_job_preview_importable(
            import_job_id,
            preview_result=preview_result,
            message=message,
            expected_execution_lease=self._execution_lease,
        )

    def mark_import_job_preview_failed(
        self,
        import_job_id: int,
        *,
        preview_status: str,
        error: str,
        preview_result: dict[str, object] | None = None,
        message: str | None = None,
    ) -> None:
        del import_job_id, preview_status, error, preview_result, message
        raise AutomationPreviewTerminalHandoffRequired(
            "automation preview failure requires the U4 owner terminal bundle"
        )


def _resolve_runtime_config(
    runtime_config: CratediggerConfig | None,
) -> CratediggerConfig:
    """Use an injected worker configuration or load the production config now."""
    if runtime_config is not None:
        return runtime_config
    return read_runtime_config()


def _preview_result_dict(result: ImportPreviewResult) -> dict[str, Any]:
    return result.to_dict()


def _preview_reason(result: ImportPreviewResult) -> str:
    return result.reason or result.decision or result.verdict


def _download_log_id_from_job(job: ImportJob) -> int | None:
    if isinstance(job.payload, (ForceImportPayload, YoutubeImportPayload)):
        return job.payload.download_log_id
    return None


def _candidate_evidence_ready_for_job(
    db: Any,
    job: ImportJob,
    result: ImportPreviewResult,
) -> tuple[bool, str]:
    source_path = result.source_path
    if not source_path:
        return False, "preview_source_path_missing"
    # Completion is deliberately stricter than the reuse/action loader: this
    # preview must have written the *job's* FK and a non-empty fingerprint.
    # In particular, do not quietly accept an older download-log FK here.
    evidence_id = db.get_import_job_candidate_evidence_id(job.id)
    if evidence_id is None:
        return False, "preview did not link candidate evidence to import job"
    evidence = db.load_album_quality_evidence_by_id(evidence_id)
    if evidence is None:
        return False, f"candidate evidence id {evidence_id} not found"
    if not evidence.snapshot_fingerprint:
        return False, "candidate evidence has empty snapshot fingerprint"
    action_path = result.action_path or source_path
    receipt = result.candidate_evidence_receipt
    if receipt is not None:
        receipt_error = candidate_evidence_persistence_receipt_semantic_error(
            receipt
        )
        if receipt_error is not None:
            return False, f"candidate receipt semantic invalid: {receipt_error}"
        if receipt.evidence_id != evidence_id:
            return False, "persistence receipt does not match import job FK"
        if receipt.snapshot_fingerprint != evidence.snapshot_fingerprint:
            return False, "persistence receipt does not match evidence snapshot"
        if not audio_snapshot_matches(action_path, evidence.files):
            return False, "candidate source changed after persistence receipt"
        return True, "persisted"

    # Compatibility path for tests and old callers that synthesize a preview
    # result directly. Production measurement results carry the explicit
    # receipt above; cache eligibility is intentionally not their completion
    # criterion.
    candidate = ensure_candidate_evidence_for_action(
        db,
        source_path=action_path,
        import_job_id=job.id,
    )
    if candidate.available and candidate.evidence is not None:
        if candidate.evidence.id == evidence_id:
            return True, "ready"
        return False, "candidate evidence id does not match import job FK"
    return (
        False,
        candidate.provenance.fallback_reason
        or candidate.provenance.candidate_status
        or "candidate_evidence_unavailable",
    )


def _cleanup_terminal_preview_force_action(
    job: ImportJob,
    terminal_job: ImportJob | None,
    *,
    action_path: str | None,
    runtime_config: CratediggerConfig | None,
) -> ImportJob | None:
    """Discard a force/local-import action only after preview reaches a
    known terminal state."""
    prefix = ACTION_COPY_PREFIX_BY_JOB_TYPE.get(job.job_type)
    if (
        prefix is None
        or terminal_job is None
        or terminal_job.status == "recovery_required"
    ):
        return terminal_job
    # A preparation failure can occur before this claim publishes a new action
    # copy.  Reclaim the deterministic path anyway: it can only be the stale
    # private copy left by the prior import attempt, and this preview claim is
    # still its owner until it returns a terminal outcome.
    try:
        resolved_config = _resolve_runtime_config(runtime_config)
        terminal_action_path = (
            action_path
            or force_action_copy_path(resolved_config, job.id, prefix=prefix)
        )
        cleanup_force_action_copy_for_job(
            terminal_action_path,
            resolved_config,
            import_job_id=job.id,
            prefix=prefix,
        )
    except Exception:
        # Preview's DB outcome is already durable. A stale private copy is
        # reclaimable on its deterministic path and must not change it.
        logger.exception("Failed to remove terminal preview force action for job %s", job.id)
    return terminal_job


class _PreviewDBSource:
    """Minimal ``PipelineDBSource`` for preview materialization.

    Only ``_get_db`` is live — the preview worker already holds the DB
    handle. Every other protocol member raises: reaching one from a
    preview materialization is a programming error, same sentinel shape
    as ``lib.enqueue._WorkerPipelineDBSource``.
    """

    def __init__(self, db: object) -> None:
        self._db = db

    def _get_db(self) -> object:
        return self._db

    def get_tracks(self, album_record: object) -> list[TrackRecord]:
        raise AssertionError("preview materialization must not read tracks")

    def get_wanted_searchable(
        self, *args: object, **kwargs: object,
    ) -> list[object]:
        raise AssertionError("preview materialization must not search")

    def mark_done(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("preview materialization must not mark done")

    def reject_and_requeue(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("preview materialization must not requeue")

    def close(self) -> None:
        raise AssertionError("preview materialization does not own the DB")


def _automation_authority_snapshot(
    db: Any,
    job: ImportJob,
    execution_lease: ExecutionLeaseSnapshot,
    *,
    runtime_config: CratediggerConfig | None = None,
) -> _AutomationPreviewAuthority | None:
    """Reread one exact processing owner without touching the filesystem."""
    from lib.download_reconstruction import reconstruct_grab_list_entry

    if job.request_id is None:
        return None
    stored_job = db.get_import_job(job.id)
    try:
        request = db.get_request(job.request_id)
    except RuntimeError:
        # The processing projection rejects a dangling exact-owner join.
        # Treat that as stale authority and stop before filesystem access.
        return None
    if (
        stored_job is None
        or request is None
        or stored_job.job_type != IMPORT_JOB_AUTOMATION
        or stored_job.status != "queued"
        or stored_job.preview_status != "running"
        or request.get("status") != "processing"
        or request.get("active_automation_import_job_id") != job.id
        or stored_job.execution_invocation_id != execution_lease.invocation_id
        or stored_job.execution_host_boot_id != execution_lease.host_boot_id
        or stored_job.execution_systemd_unit != execution_lease.systemd_unit
        or stored_job.execution_worker_pid != execution_lease.worker.pid
        or stored_job.execution_worker_start_ticks
        != execution_lease.worker.start_ticks
        or stored_job.execution_beets_pid is not None
        or stored_job.execution_beets_start_ticks is not None
    ):
        return None
    try:
        state = ActiveDownloadState.from_raw(request.get("active_download_state"))
    except ValueError:
        return None
    if not state.current_path or not state.files:
        return None
    cfg = _resolve_runtime_config(runtime_config)
    entry = reconstruct_grab_list_entry(request, state)
    expected_canonical = canonical_folder_for_row(
        entry,
        processing_albums_dir(cfg.processing_dir),
    )
    if os.path.abspath(state.current_path) != os.path.abspath(expected_canonical):
        return None
    return _AutomationPreviewAuthority(
        request=request,
        state=state,
        canonical_path=expected_canonical,
    )


def _materialize_automation_authority(
    db: Any,
    job: ImportJob,
    authority: _AutomationPreviewAuthority,
    *,
    runtime_config: CratediggerConfig | None,
    cancellation_token: CancellationToken,
    materialize_fn: Callable[..., object] | None = None,
) -> str:
    """Resume only the persisted exact manifest into its persisted path."""
    from lib.context import CratediggerContext, WorkerCollaborators
    from lib.download_materialization import (
        Materialized,
        _materialize_processing_dir,
    )
    from lib.download_reconstruction import reconstruct_grab_list_entry
    from lib.staged_album import StagedAlbum

    cancellation_token.raise_if_cancelled()
    cfg = _resolve_runtime_config(runtime_config)
    entry = reconstruct_grab_list_entry(
        authority.request,
        authority.state,
    )
    staged_album = StagedAlbum.from_entry(
        entry,
        default_path=authority.canonical_path,
    )
    if os.path.abspath(staged_album.current_path) != os.path.abspath(
        authority.canonical_path
    ):
        raise RuntimeError("persisted automation path is not canonical")
    # WorkerCollaborators: the preview worker holds no slskd client, so no
    # destructive slskd path is reachable and the ownership writer that
    # gates one is structurally absent rather than defaulted away (#1313).
    ctx = CratediggerContext(
        collaborators=WorkerCollaborators(
            cfg=cfg,
            pipeline_db_source=_PreviewDBSource(db),
        ),
    )
    materialize = materialize_fn or _materialize_processing_dir
    materialized = materialize(
        entry,
        staged_album,
        ctx,
        cancellation_token=cancellation_token,
    )
    if not isinstance(materialized, Materialized):
        raise RuntimeError(  # noqa: TRY004 - state-machine outcome, not caller type
            f"Album request {job.request_id} could not be materialized for preview"
        )
    cancellation_token.raise_if_cancelled()
    if os.path.abspath(staged_album.current_path) != os.path.abspath(
        authority.canonical_path
    ):
        raise RuntimeError("automation materialization changed canonical path")
    return authority.canonical_path


def _front_gate_source_path(
    db: Any,
    job: ImportJob,
    *,
    automation_authority: _AutomationPreviewAuthority | None = None,
) -> str | None:
    """Cheap source-path derivation for the candidate-evidence front-gate.

    Returns the path the evidence snapshot would have captured, or ``None``
    when the path cannot be derived without invoking measurement-time
    materialization. ``None`` is a graceful skip: the worker falls through
    to the existing measurement codepath.
    """
    if job.job_type == IMPORT_JOB_FORCE:
        # A force payload is audit metadata, not filesystem authority. Its
        # path is resolved only from the download_log row at execution time.
        return None
    if job.job_type == IMPORT_JOB_YOUTUBE:
        # KTD1: YT path NEVER reads ``active_download_state``. The
        # staged path comes from the typed payload decoded at the DB boundary.
        if not isinstance(job.payload, YoutubeImportPayload):
            raise AssertionError("youtube_import payload type mismatch")
        return job.payload.staged_path
    if job.job_type == IMPORT_JOB_AUTOMATION:
        del db
        if automation_authority is None:
            return None
        return automation_authority.canonical_path
    return None


class _DownloadLogEntryReader(Protocol):
    def get_download_log_entry(
        self,
        download_log_id: int,
    ) -> Mapping[str, object] | None: ...


class _ImportPreviewJobClaimer(Protocol):
    def peek_import_preview_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]: ...

    def claim_import_preview_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str,
    ) -> ImportJob | None: ...


class _PreviewHeartbeatDB(Protocol):
    def heartbeat_import_job_preview(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool: ...

    def close(self) -> None: ...


def _force_download_log_failed_path(
    db: _DownloadLogEntryReader,
    job: ImportJob,
) -> tuple[int, str]:
    """Return the sole filesystem name authoritative for a force preview."""
    download_log_id = _download_log_id_from_job(job)
    if download_log_id is None:
        raise ValueError("Force import preview job is missing download_log_id")
    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        raise ValueError(f"Download log {download_log_id} not found")
    raw_path = decode_validation_envelope(entry.get("validation_result")).failed_path
    if not raw_path:
        raise ValueError("Download log has no failed_path")
    return download_log_id, raw_path


def _reused_evidence_preview_payload(
    job: ImportJob,
    evidence: AlbumQualityEvidence,
    source_path: str,
    import_result: ImportResult,
    *,
    action_path: str | None = None,
) -> dict[str, object]:
    """Synthesize a preview_result payload for the reused-evidence branch.

    Mirrors the shape ``ImportPreviewResult.to_dict()`` produces so
    downstream consumers (web UI recents tab, decision-tree viewers) see
    the keys they already render. Adds top-level ``candidate_status``
    provenance so the reused path is distinguishable from the measured
    path.
    """
    del evidence  # measurement is recorded in the evidence row itself
    # ``msgspec.to_builtins`` returns ``Any``; ``msgspec.convert`` recovers
    # the parameterized dict shape (established wire-boundary adapter,
    # CLAUDE.md "Wire-boundary types") instead of an ``isinstance`` assert,
    # which would narrow to ``dict[Unknown, Unknown]``.
    payload = msgspec.convert(
        msgspec.to_builtins(ImportPreviewResult(
            mode="reused",
            verdict="would_import",
            would_import=True,
            decision="candidate_evidence_reused",
            reason="candidate_evidence_reused",
            stage_chain=["preview:candidate_evidence_reused"],
            request_id=job.request_id,
            download_log_id=_download_log_id_from_job(job),
            source_path=source_path,
            import_result=import_result,
        )),
        type=dict[str, object],
    )
    payload["candidate_status"] = CANDIDATE_STATUS_REUSED
    if action_path is not None:
        payload["action_path"] = action_path
    return payload


@dataclass(frozen=True)
class _JobActionAuthority:
    """Bundles what varies between job types sharing
    ``_prepare_force_action_path`` (issue #1176 PR3): which authority
    resolves the operator/configured root, which prefix names the retained
    job-scoped action copy, and whether the operator's raw path is safe to
    surface as measurement-failure audit display text. Bundled as one
    parameter rather than three so the entry point still takes exactly ONE
    new argument for "the authority" — everything about the alternate
    lane travels together.

    ``audit_shows_raw_path`` defaults to force's own historical behavior
    (``True``): force's raw path is a Cratedigger-managed quarantine
    folder, safe to display. Local-import sets it ``False`` — CLAUDE.md
    decision 2 for #1176 requires every audit surface this lane touches to
    name the disposable action copy, never the operator's real folder.
    Two readers consult this flag. ``execute_preview_job`` reads it to
    decide what ``measure_and_persist_candidate_evidence``'s
    ``source_display_path`` (measurement-failure audit text — a different
    surface than dispatch's Wrong Matches ``failed_path``, but the same
    "never the operator's folder" rule) receives.
    ``process_claimed_preview_job`` (issue #1176 PR3 review round, F6)
    reads it separately to build ``front_gate_audit_source`` — the alias
    every ``MeasurementFailure``/``ImportPreviewResult``/reused-evidence
    source field there uses instead of the raw ``front_gate_source`` its
    own ``_front_gate_check`` call resolves. A PR3 review round found 4
    such sites in ``process_claimed_preview_job`` still using
    ``front_gate_source`` directly, unaffected by this flag. A second
    review round corrected the fix itself: the action copy those sites
    need is USUALLY already established by the SAME
    ``_action_copy_front_gate`` call that produced ``front_gate_source``,
    but its own except branch is exactly a "the copy doesn't exist yet"
    case (``_prepare_force_action_path`` can itself fail — a moved/renamed
    operator folder, EACCES, ENOSPC, a symlink inside it), and the first
    fix's alias fell back to the raw ``front_gate_source`` in exactly that
    world. ``front_gate_audit_source`` now fails closed instead: the
    deterministic path this job would have retained had preparation
    succeeded, never the operator's real folder.
    """

    snapshot_fn: Callable[[str, CratediggerConfig], str]
    action_prefix: str
    audit_shows_raw_path: bool = True


#: Default authority: today's force-import quarantine resolution, unchanged.
_FORCE_ACTION_AUTHORITY = _JobActionAuthority(
    snapshot_fn=snapshot_configured_quarantine_directory,
    action_prefix=FORCE_ACTION_PREFIX,
)

#: The local-import lane's authority (issue #1176 PR3): resolves through
#: ``open_configured_local_import_directory`` instead of the quarantine
#: roots, retains its copy under a distinct prefix, and never surfaces the
#: operator's raw path in preview's own audit trail either.
_LOCAL_IMPORT_ACTION_AUTHORITY = _JobActionAuthority(
    snapshot_fn=snapshot_configured_local_import_directory,
    action_prefix=LOCAL_IMPORT_ACTION_PREFIX,
    audit_shows_raw_path=False,
)


def _prepare_force_action_path(
    db: object,
    job: ImportJob,
    cfg: CratediggerConfig,
    *,
    raw_path: str,
    authority: _JobActionAuthority = _FORCE_ACTION_AUTHORITY,
) -> str:
    """Publish one normalized, private action copy for a force/local-import
    job.

    This is the only such copy's lifecycle owner.  Repeating a preview
    replaces the job's deterministic action directory, so retrying cannot
    accumulate random copies of an operator-owned source. ``authority``
    (issue #1176 PR3) defaults to force's own quarantine resolution + prefix,
    so every existing force-import caller is byte-identical; the
    local-import lane passes ``_LOCAL_IMPORT_ACTION_AUTHORITY`` instead.
    """
    del db
    snapshot = authority.snapshot_fn(raw_path, cfg)
    try:
        # The descriptor copy is now private working state.  Normalize before
        # inventorying/persisting evidence, and never touch ``raw_path``.
        from lib.media_readiness import normalize_media_metadata

        # A decode failure must rejoin the ordinary measurement-failure
        # terminal path below, not escape this private-copy helper and be
        # retried as a worker crash.
        normalize_media_metadata(snapshot, fail_closed=False)
        return retain_preview_snapshot_for_force_action(
            snapshot,
            cfg,
            import_job_id=job.id,
            prefix=authority.action_prefix,
        )
    finally:
        if os.path.isdir(snapshot):
            remove_preview_snapshot(snapshot, cfg)


def _local_import_raw_path(job: ImportJob) -> tuple[int | None, str]:
    """A local-import job's raw path IS its payload — no download_log
    lookup needed (issue #1176 PR3): unlike force/YouTube, a local import
    has no originating ``download_log`` row.
    """
    # Programmer-error invariant (the payload is already decoded/validated
    # by job_type at read time), not a caller type error — mirrors every
    # other "<job_type> payload type mismatch" AssertionError in this file.
    if not isinstance(job.payload, LocalImportPayload):
        raise AssertionError("local_import payload type mismatch")  # noqa: TRY004
    return None, job.payload.source_path


def _action_copy_front_gate(
    db: QualityEvidenceDB,
    job: ImportJob,
    *,
    resolve_raw_path: Callable[[], tuple[int | None, str]],
    runtime_config: CratediggerConfig | None,
    authority: _JobActionAuthority,
    candidate_evidence_loader: Callable[..., EvidenceBuildResult] | None,
) -> tuple[EvidenceBuildResult | None, str | None, str | None]:
    """Shared front-gate body for job types that retain a private,
    job-scoped action copy (force-import, local-import — issue #1176 PR3).

    ``resolve_raw_path`` stays inside the ``try`` (rather than being
    resolved by the caller) so a failure deriving it — force's
    download_log lookup can raise — is caught exactly as it always was;
    local-import's own resolver cannot raise (the payload is already
    validated at decode time) but shares the same shape for one reason:
    consistency, not necessity.
    """
    raw_path: str | None = None
    try:
        download_log_id, raw_path = resolve_raw_path()
        cfg = _resolve_runtime_config(runtime_config)
        action_path = _prepare_force_action_path(
            db, job, cfg, raw_path=raw_path, authority=authority,
        )
        load_candidate = (
            candidate_evidence_loader or load_candidate_evidence_for_source
        )
        result = load_candidate(
            db,
            source_path=action_path,
            download_log_id=download_log_id,
            import_job_id=job.id,
        )
        # Reuse is allowed to discover a content-addressed evidence row
        # through the originating audit record, but completion is not:
        # bind the proven exact action snapshot to this job before its
        # strict completion gate runs.
        if (
            result.status == "ready"
            and result.evidence is not None
            and result.evidence.id is not None
        ):
            db.set_import_job_candidate_evidence(job.id, result.evidence.id)
        return result, raw_path, action_path
    except (ExecutionCancelled, OwnerSessionLost):
        raise
    except Exception:
        logger.debug(
            "%s front-gate isolation failed for job %s; falling through",
            job.job_type,
            job.id,
            exc_info=True,
        )
        # The front gate has already recovered the persisted source. Keep it
        # for a later failure audit even when its isolated snapshot was not
        # available; only the evidence-reuse optimization failed.
        return None, raw_path, None


def _front_gate_check(
    db: Any,
    job: ImportJob,
    *,
    runtime_config: CratediggerConfig | None = None,
    candidate_evidence_loader: Callable[..., EvidenceBuildResult] | None = None,
    automation_authority: _AutomationPreviewAuthority | None = None,
    cancellation_token: CancellationToken | None = None,
) -> tuple[EvidenceBuildResult | None, str | None, str | None]:
    """Run the cheap candidate-evidence front-gate for ``job``.

    Returns ``(result, source_path, action_path)``. ``result is None`` means the
    front-gate could not run at all (path-derivation deferred to the
    measurement path) and the caller should fall through. A non-None
    result with ``status == 'ready'`` means measurement can be skipped.
    """
    if job.job_type == IMPORT_JOB_FORCE:
        return _action_copy_front_gate(
            db,
            job,
            resolve_raw_path=lambda: _force_download_log_failed_path(db, job),
            runtime_config=runtime_config,
            authority=_FORCE_ACTION_AUTHORITY,
            candidate_evidence_loader=candidate_evidence_loader,
        )

    if job.job_type == IMPORT_JOB_LOCAL:
        return _action_copy_front_gate(
            db,
            job,
            resolve_raw_path=lambda: _local_import_raw_path(job),
            runtime_config=runtime_config,
            authority=_LOCAL_IMPORT_ACTION_AUTHORITY,
            candidate_evidence_loader=candidate_evidence_loader,
        )

    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()
    source_path = _front_gate_source_path(
        db,
        job,
        automation_authority=automation_authority,
    )
    if not source_path:
        return None, None, None
    try:
        result = load_candidate_evidence_for_source(
            db,
            source_path=source_path,
            download_log_id=_download_log_id_from_job(job),
            import_job_id=job.id,
        )
    except (ExecutionCancelled, OwnerSessionLost):
        raise
    except Exception:
        logger.debug(
            "front-gate evidence load failed for job %s; "
            "falling through to measurement",
            job.id,
            exc_info=True,
        )
        return None, source_path, None
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()
    return result, source_path, None


def _preview_input(
    db: Any,
    job: ImportJob,
    *,
    runtime_config: CratediggerConfig | None = None,
    automation_authority: _AutomationPreviewAuthority | None = None,
    cancellation_token: CancellationToken | None = None,
    automation_materialize_fn: Callable[..., object] | None = None,
) -> dict[str, Any]:
    if job.request_id is None:
        raise ValueError("Import job has no request_id")

    if job.job_type == IMPORT_JOB_FORCE:
        raise ValueError("Force import preview inputs are resolved from download_log")

    if job.job_type == IMPORT_JOB_LOCAL:
        raise ValueError("Local import preview inputs are resolved from the payload")

    if job.job_type == IMPORT_JOB_AUTOMATION:
        if automation_authority is None or cancellation_token is None:
            raise ValueError("automation preview requires exact owner authority")
        path = _materialize_automation_authority(
            db,
            job,
            automation_authority,
            runtime_config=runtime_config,
            cancellation_token=cancellation_token,
            materialize_fn=automation_materialize_fn,
        )
        return {
            "request_id": job.request_id,
            "path": path,
            "force": False,
            "download_log_id": None,
        }

    if job.job_type == IMPORT_JOB_YOUTUBE:
        # KTD1: never read ``active_download_state``. The staged path
        # is the authoritative source — yt-dlp already wrote files
        # there, and we measure them in place.
        if not isinstance(job.payload, YoutubeImportPayload):
            raise AssertionError("youtube_import payload type mismatch")
        return {
            "request_id": job.request_id,
            "path": job.payload.staged_path,
            "force": False,
            "download_log_id": None,
        }

    raise ValueError(f"Unsupported import job type: {job.job_type}")


def execute_preview_job(
    db: Any,
    job: ImportJob,
    *,
    runtime_config: CratediggerConfig | None = None,
    prepared_force_action_path: str | None = None,
    prepared_force_source_path: str | None = None,
    automation_authority: _AutomationPreviewAuthority | None = None,
    cancellation_token: CancellationToken | None = None,
    candidate_measurement_fn: Callable[..., ImportPreviewResult] | None = None,
    automation_materialize_fn: Callable[..., object] | None = None,
) -> ImportPreviewResult:
    measure_candidate = (
        candidate_measurement_fn or measure_and_persist_candidate_evidence
    )
    if job.job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_LOCAL):
        if job.request_id is None:
            raise ValueError("Import job has no request_id")
        authority = (
            _FORCE_ACTION_AUTHORITY
            if job.job_type == IMPORT_JOB_FORCE
            else _LOCAL_IMPORT_ACTION_AUTHORITY
        )
        resolve_raw_path = (
            (lambda: _force_download_log_failed_path(db, job))
            if job.job_type == IMPORT_JOB_FORCE
            else (lambda: _local_import_raw_path(job))
        )
        download_log_id, raw_path = resolve_raw_path()
        if prepared_force_source_path is not None:
            raw_path = prepared_force_source_path
        cfg = _resolve_runtime_config(runtime_config)
        action_path = prepared_force_action_path or _prepare_force_action_path(
            db, job, cfg, raw_path=raw_path, authority=authority,
        )
        result = measure_candidate(
            db,
            request_id=job.request_id,
            path=action_path,
            source_display_path=(
                raw_path if authority.audit_shows_raw_path else None
            ),
            force=True,
            download_log_id=download_log_id,
            import_job_id=job.id,
            runtime_config=cfg,
            repair_fn=_noop_header_repair,
        )
        return msgspec.structs.replace(result, action_path=action_path)
    preview_input = _preview_input(
        db,
        job,
        runtime_config=runtime_config,
        automation_authority=automation_authority,
        cancellation_token=cancellation_token,
        automation_materialize_fn=automation_materialize_fn,
    )
    return measure_candidate(
        db,
        import_job_id=job.id,
        cancellation_token=cancellation_token,
        **preview_input,
    )


def _handle_measurement_failed(
    db: Any,
    job: ImportJob,
    result: ImportPreviewResult,
    *,
    prepare_failure_have_fn: FailureHavePrepareFn | None = None,
    enrich_failure_have_fn: FailureHaveEnrichFn | None = None,
    runtime_config: CratediggerConfig | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
) -> ImportJob | None:
    """Persist a measurement failure through one DB-owned terminal bundle.

    Request-backed jobs atomically commit the preview fields, failed job,
    request lifecycle action, mandatory audit, and any denylist writes. A malformed
    orphan job with no request row has no legal ``download_log`` owner, so it
    remains a job-only precondition failure.

    ``denylist_username`` is currently always None — the per-user 5-strikes
    rule lives in the importer-side reject path (U6). Preview measurement
    failures are infrastructure-class failures (source vanished, snapshot
    stale, crashed); the user isn't responsible for the source going away
    mid-measure, so we do not denylist here.
    """
    payload = result.failure
    if payload is None:
        # Defensive: every measurement_failed result must carry a payload.
        # Synthesize one from the result fields so we never fall through
        # without firing the terminal lifecycle bundle.
        payload = MeasurementFailure(
            reason="measurement_crashed",
            detail=result.detail or result.reason or "measurement_failed",
            source_path=result.source_path or "",
        )
    preview_payload = _preview_result_dict(result)
    request = (
        db.get_request(job.request_id)
        if job.request_id is not None
        else None
    )
    if job.request_id is None or request is None:
        return db.mark_import_job_preview_failed(
            job.id,
            preview_status=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            error=payload.reason,
            preview_result=preview_payload,
            message=f"Preview measurement failed: {payload.reason}",
        )

    mb_release_id = request.get("mb_release_id")
    configured_runtime = runtime_config
    prepared_outcome: HavePreparation | None = None
    if isinstance(mb_release_id, str) and mb_release_id:
        try:
            configured_runtime = _resolve_runtime_config(configured_runtime)
        except Exception:
            logger.warning(
                "Unable to load runtime config while preparing HAVE evidence "
                "for preview failure on request %s",
                job.request_id,
                exc_info=True,
            )
        if configured_runtime is not None:
            prepare_fn = (
                prepare_failure_have_fn
                or prepare_current_evidence_for_failure
            )
            try:
                prepared_outcome = prepare_fn(
                    db,
                    request_id=job.request_id,
                    mb_release_id=mb_release_id,
                    quality_ranks=configured_runtime.quality_ranks,
                    beets_library_root=configured_runtime.beets_directory,
                )
            except Exception:
                logger.warning(
                    "HAVE evidence preparation crashed for preview failure "
                    "on request %s",
                    job.request_id,
                    exc_info=True,
                )

    automation_terminal_authority = None
    if job.job_type == IMPORT_JOB_AUTOMATION:
        if (
            execution_lease is None
            or cancellation_token is None
            or owner_session_identity is None
        ):
            raise AutomationPreviewTerminalHandoffRequired(
                "automation preview terminal outcome lacks exact authority"
            )
        from scripts.importer import (
            _complete_automation_processing_cleanup,
        )

        cleanup_receipt = _complete_automation_processing_cleanup(
            db,
            job,
            DispatchOutcome(
                success=False,
                message=payload.detail,
                post_commit_cleanup=PostCommitCleanup(
                    staged_path=payload.source_path or None,
                ),
            ),
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )
        automation_terminal_authority = AutomationTerminalAuthority(
            expected_job_status="queued",
            expected_preview_status="running",
            expected_execution_lease=execution_lease,
            cleanup_receipt=cleanup_receipt,
        )

    _record_preview_measurement_failed(
        db,
        request_id=job.request_id,
        import_job_id=job.id,
        payload=payload,
        import_result=result.import_result,
        preview_result=preview_payload,
        requeue_to_wanted=job.job_type == IMPORT_JOB_AUTOMATION,
        automation_terminal_authority=automation_terminal_authority,
    )

    if (
        prepared_outcome == HavePreparation.READY
        and configured_runtime is not None
    ):
        enrich_fn = (
            enrich_failure_have_fn
            or enrich_incomplete_current_evidence_for_request
        )
        try:
            enrich_fn(
                db,
                request_id=job.request_id,
                mb_release_id=mb_release_id,
                quality_ranks=configured_runtime.quality_ranks,
                beets_library_root=configured_runtime.beets_directory,
            )
        except Exception:
            logger.warning(
                "HAVE evidence enrichment crashed after preview failure "
                "on request %s",
                job.request_id,
                exc_info=True,
            )

    if hasattr(db, "get_import_job"):
        return db.get_import_job(job.id)
    return None


PreviewFn = Callable[[Any, ImportJob], ImportPreviewResult]


def process_claimed_preview_job(
    db: Any,
    job: ImportJob,
    *,
    spectral_detail_analyzer: SpectralDetailAnalyzer | None = None,
    existing_spectral_resolver: ExistingSpectralResolver | None = None,
    preview_fn: PreviewFn | None = None,
    prepare_failure_have_fn: FailureHavePrepareFn | None = None,
    enrich_failure_have_fn: FailureHaveEnrichFn | None = None,
    current_evidence_loader: Callable[..., EvidenceBuildResult] | None = None,
    runtime_config: CratediggerConfig | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    automation_authority: _AutomationPreviewAuthority | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
    candidate_measurement_fn: Callable[..., ImportPreviewResult] | None = None,
    automation_materialize_fn: Callable[..., object] | None = None,
) -> ImportJob | None:
    if job.job_type == IMPORT_JOB_AUTOMATION:
        if (
            execution_lease is None
            or automation_authority is None
            or cancellation_token is None
        ):
            return None
        cancellation_token.raise_if_cancelled()
        db = _AutomationPreviewDB(db, execution_lease)

    front_gate_source = (
        automation_authority.canonical_path
        if automation_authority is not None
        else None
    )
    front_gate_action: str | None = None

    def handle_measurement_failed(result: ImportPreviewResult) -> ImportJob | None:
        terminal = _handle_measurement_failed(
            db,
            job,
            result,
            prepare_failure_have_fn=prepare_failure_have_fn,
            enrich_failure_have_fn=enrich_failure_have_fn,
            runtime_config=runtime_config,
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )
        return _cleanup_terminal_preview_force_action(
            job,
            terminal,
            action_path=result.action_path,
            runtime_config=runtime_config,
        )

    def handle_current_authority_failed(
        detail: str,
        *,
        source_path: str,
    ) -> ImportJob | None:
        failure = MeasurementFailure(
            reason="measurement_crashed",
            detail=detail,
            source_path=source_path,
        )
        return handle_measurement_failed(ImportPreviewResult(
            mode="path",
            verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            decision="current_evidence_failed",
            reason="measurement_crashed",
            detail=detail,
            source_path=source_path,
            action_path=front_gate_action,
            request_id=job.request_id,
            download_log_id=_download_log_id_from_job(job),
            failure=failure,
        ))

    # Automation must first prove that the persisted canonical directory is
    # the exact downloaded manifest. Candidate evidence intentionally ignores
    # non-audio control debris and therefore cannot stand in for this boundary.
    # The materializer has a safe fast path for an already-complete canonical
    # directory and rejects partial, extra-entry, symlink and special-file
    # destinations.
    if automation_authority is not None:
        assert cancellation_token is not None
        try:
            front_gate_source = _materialize_automation_authority(
                db,
                job,
                automation_authority,
                runtime_config=runtime_config,
                cancellation_token=cancellation_token,
                materialize_fn=automation_materialize_fn,
            )
        except (ExecutionCancelled, OwnerSessionLost):
            raise
        except Exception as exc:
            logger.exception(
                "Automation job %s failed exact canonical materialization",
                job.id,
            )
            return handle_current_authority_failed(
                f"{type(exc).__name__}: {exc}",
                source_path=automation_authority.canonical_path,
            )

    # Front-gate: after materialization authority is proven, matching stored
    # candidate evidence may skip measurement. The post-measurement gate below
    # remains as belt-and-braces for the fall-through path.
    front_gate_result, front_gate_source, front_gate_action = _front_gate_check(
        db,
        job,
        runtime_config=runtime_config,
        automation_authority=automation_authority,
        cancellation_token=cancellation_token,
    )
    # F6 (issue #1176 PR3 review round, corrected in a second round): front_
    # gate_source is the OPERATOR's raw path for a local-import job — never
    # safe to persist into an audit-visible field per decision 2, exactly
    # the boundary _JobActionAuthority.audit_shows_raw_path already draws
    # for measure_and_persist_candidate_evidence's own source_display_path.
    # front_gate_action is the disposable private action copy, USUALLY
    # already established by the SAME _action_copy_front_gate call that
    # produced front_gate_source — but its own except branch (line ~776
    # above) is exactly the case a first review round's comment here denied:
    # _prepare_force_action_path can itself fail (the operator's folder
    # moved/renamed between enqueue and preview claim — realistic with a
    # backed-up preview queue — a symlink/special file inside it, EACCES,
    # ENOSPC on the processing tree), returning action_path=None while
    # raw_path (front_gate_source) is still the real operator path. Every
    # MeasurementFailure / ImportPreviewResult / reused-evidence payload
    # below that names "the source" uses this alias instead of
    # front_gate_source directly, so local-import must fail CLOSED here:
    # when no action copy exists, fall back to the deterministic path this
    # job would have retained had preparation succeeded (never to the raw
    # operator path), and to the documented "not resolved" sentinel (``""``,
    # the same one MeasurementFailure.source_path's own docstring defines)
    # only if resolving even that requires config this job's own claim
    # already proved readable and still somehow fails. Force keeps showing
    # its own real wrong_matches-rooted front_gate_source unchanged: its raw
    # path is already safe to display (audit_shows_raw_path=True) and is the
    # more durable, reviewable location for an operator to inspect — only
    # local's leak closes here.
    front_gate_audit_source = front_gate_source
    if job.job_type == IMPORT_JOB_LOCAL:
        if front_gate_action is not None:
            front_gate_audit_source = front_gate_action
        else:
            try:
                front_gate_audit_source = force_action_copy_path(
                    _resolve_runtime_config(runtime_config),
                    job.id,
                    prefix=LOCAL_IMPORT_ACTION_PREFIX,
                )
            except Exception:
                logger.debug(
                    "Unable to resolve the deterministic local-import "
                    "action path for job %s while building the audit-safe "
                    "source alias; falling back to the not-resolved "
                    "sentinel",
                    job.id,
                    exc_info=True,
                )
                front_gate_audit_source = ""
    if (
        front_gate_result is not None
        and front_gate_result.status == "ready"
        and front_gate_result.evidence is not None
        and front_gate_source is not None
    ):
        # Re-derive with the SAME rule as front_gate_audit_source above,
        # but from a point where pyright has already narrowed
        # front_gate_source to str (the guard just above) — the outer
        # front_gate_audit_source is str | None because it was computed
        # before that narrowing existed.
        narrowed_audit_source: str = (
            front_gate_audit_source
            if front_gate_audit_source is not None
            else front_gate_source
        )
        resolved_have = CurrentLibraryEvidence(
            evidence=None,
            existing_spectral_evidence=SpectralAnalysisDetail(attempted=False),
            reuse_have_evidence=False,
            preserve_have_source=False,
        )
        mb_release_id = ""
        if job.request_id is not None:
            try:
                # ``db`` is the worker's untyped handle, so
                # ``db.get_request(...)`` is ``Any``; declaring ``req``'s own
                # type recovers a known shape for the ``.get`` calls below
                # without touching ``db``'s parameter type.
                req: dict[str, object] = db.get_request(job.request_id) or {}
                mb_release_id = str(req.get("mb_release_id") or "")
                preview_cfg = _resolve_runtime_config(runtime_config)
                # The same HAVE resolution both preview lanes run — the
                # sequence lives in lib/current_library_evidence.py so the
                # reuse path here and lib/import_preview.py's lanes cannot
                # drift (issue #1313; it was copy-pasted before).
                outcome = resolve_current_library_evidence(
                    db,
                    request_id=job.request_id,
                    mb_release_id=mb_release_id,
                    quality_ranks=preview_cfg.quality_ranks,
                    beets_library_root=getattr(
                        preview_cfg, "beets_directory", ""
                    ),
                    loader=(
                        current_evidence_loader
                        or load_current_evidence_for_preview
                    ),
                )
                if isinstance(outcome, CurrentLibraryAuthorityUnavailable):
                    return handle_current_authority_failed(
                        outcome.detail,
                        source_path=narrowed_audit_source,
                    )
                resolved_have = outcome
            except (ExecutionCancelled, OwnerSessionLost):
                raise
            except Exception as exc:
                logger.exception(
                    "Unable to load reused HAVE evidence for request %s",
                    job.request_id,
                )
                return handle_current_authority_failed(
                    f"{type(exc).__name__}: {exc}",
                    source_path=narrowed_audit_source,
                )
        # Explicit annotation gives the fallback lambda below an expected
        # type to infer its parameter from (otherwise its parameter type is
        # unknown under strict mode).
        audit_resolver: ExistingSpectralResolver | None = existing_spectral_resolver
        if audit_resolver is None:
            try:
                audit_cfg = _resolve_runtime_config(runtime_config)
            except Exception as exc:
                logger.exception("Unable to load config for reused HAVE audit")
                failed_lookup = ExistingSpectralAuditLookup(
                    failure=SpectralAnalysisDetail(
                        attempted=True,
                        error=f"{type(exc).__name__}: {exc}",
                    ),
                )
                audit_resolver = lambda _release_id: failed_lookup
            else:
                audit_resolver = existing_spectral_resolver_for_config(audit_cfg)
        if cancellation_token is not None:
            cancellation_token.raise_if_cancelled()
        audit, have_lookup = collect_release_attempt_spectral_audit(
            front_gate_action or front_gate_source,
            mb_release_id,
            existing_spectral_evidence=resolved_have.existing_spectral_evidence,
            preserve_existing_source_spectral=resolved_have.preserve_have_source,
            analyzer=(
                spectral_detail_analyzer or analyze_spectral_audit_path
            ),
            existing_resolver=audit_resolver,
            # The front-gate already proved this exact content snapshot owns
            # complete candidate evidence. Re-project its persisted spectral
            # fact into the attempt audit instead of analyzing the same bytes
            # again. HAVE remains separate: a complete matching usable fact is
            # re-projected; otherwise the exact installed bytes are analyzed.
            candidate_detail=spectral_detail_from_persisted_source(
                front_gate_result.evidence.measurement.spectral_grade,
                front_gate_result.evidence.measurement.spectral_bitrate_kbps,
                cliff_hz=front_gate_result.evidence.measurement.cliff_hz,
                codec_family=front_gate_result.evidence.measurement.codec_family,
                ultrasonic_deficit_db=(
                    front_gate_result.evidence.measurement.ultrasonic_deficit_db
                ),
                spectral_measurement_version=(
                    front_gate_result.evidence.measurement.spectral_measurement_version
                ),
            ),
            existing_detail=(
                resolved_have.existing_spectral_evidence
                if resolved_have.reuse_have_evidence
                else None
            ),
        )
        if cancellation_token is not None:
            cancellation_token.raise_if_cancelled()
        # A newly measured HAVE fact must become durable BEFORE this job is
        # marked importable — an audit-only scan left the decision spectrally
        # blind (download_log 37206). Reused evidence has no lookup path and
        # needs no write; the shared guard declines those cases, and the
        # persist helper's own exact-path/exact-snapshot guards keep fresh
        # failures fail-soft like the audit itself.
        if job.request_id is not None:
            try:
                persist_measured_have_spectral(
                    db,
                    request_id=job.request_id,
                    resolved=resolved_have,
                    measured_existing=audit.existing,
                    measured_existing_path=have_lookup.path,
                )
            except (ExecutionCancelled, OwnerSessionLost):
                raise
            except Exception:
                logger.exception(
                    "Unable to persist reused-path HAVE spectral for "
                    "request %s",
                    job.request_id,
                )
        reused_payload = _reused_evidence_preview_payload(
            job,
            front_gate_result.evidence,
            narrowed_audit_source,
            ImportResult(spectral=audit),
            action_path=front_gate_action,
        )
        logger.info(
            "Reused candidate evidence for import job %s; skipping preview measurement",
            job.id,
        )
        db.set_import_job_candidate_evidence(
            job.id,
            front_gate_result.evidence.id,
        )
        if cancellation_token is not None:
            cancellation_token.raise_if_cancelled()
        return db.mark_import_job_preview_importable(
            job.id,
            preview_result=reused_payload,
            message="Reused stored candidate evidence (snapshot matched)",
        )

    try:
        if preview_fn is not None:
            result = preview_fn(db, job)
        else:
            result = execute_preview_job(
                db,
                job,
                runtime_config=runtime_config,
                prepared_force_action_path=front_gate_action,
                prepared_force_source_path=front_gate_source,
                automation_authority=automation_authority,
                cancellation_token=cancellation_token,
                candidate_measurement_fn=candidate_measurement_fn,
                automation_materialize_fn=automation_materialize_fn,
            )
    except (ExecutionCancelled, OwnerSessionLost):
        raise
    except Exception as exc:
        logger.exception("Import job %s preview crashed", job.id)
        # Worker-mode preview should not raise — but if it does, route the
        # crash through the same lifecycle helper so automation is not
        # stranded and operator state is not overwritten.
        crash_payload = MeasurementFailure(
            reason="measurement_crashed",
            detail=f"{type(exc).__name__}: {exc}",
            source_path=front_gate_audit_source or "",
        )
        crash_result = ImportPreviewResult(
            mode="path",
            verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            uncertain=False,
            decision="measurement_crashed",
            reason="measurement_crashed",
            detail=f"{type(exc).__name__}: {exc}",
            request_id=job.request_id,
            download_log_id=_download_log_id_from_job(job),
            source_path=front_gate_audit_source,
            action_path=front_gate_action,
            failure=crash_payload,
        )
        return handle_measurement_failed(crash_result)

    if result.verdict == PREVIEW_VERDICT_MEASUREMENT_FAILED:
        return handle_measurement_failed(result)

    if result.verdict == PREVIEW_VERDICT_EVIDENCE_READY:
        preview_payload = _preview_result_dict(result)
        # Belt-and-braces: confirm candidate evidence is actually
        # persisted on disk before marking importable. If the
        # persistence stage was skipped or partial, fall back to
        # measurement_failed so caller lifecycle authority still applies.
        evidence_ready, evidence_reason = _candidate_evidence_ready_for_job(
            db,
            job,
            result,
        )
        if evidence_ready:
            if cancellation_token is not None:
                cancellation_token.raise_if_cancelled()
            return db.mark_import_job_preview_importable(
                job.id,
                preview_result=preview_payload,
                message=f"Evidence ready for final check: {_preview_reason(result)}",
            )
        fallback_payload = MeasurementFailure(
            reason="evidence_persist_failed",
            detail=evidence_reason or "candidate evidence unavailable",
            source_path=result.source_path or "",
        )
        fallback_result = ImportPreviewResult(
            mode=result.mode,
            verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            decision="evidence_persist_failed",
            reason="evidence_persist_failed",
            detail=evidence_reason,
            source_path=result.source_path,
            action_path=result.action_path,
            request_id=result.request_id,
            download_log_id=result.download_log_id,
            import_result=result.import_result,
            failure=fallback_payload,
        )
        return handle_measurement_failed(fallback_result)

    # Defensive: anything else (including legacy verdicts in case of bugs)
    # routes through measurement_failed so caller lifecycle authority applies
    # and the job does not get stuck.
    logger.warning(
        "Import job %s preview returned unexpected verdict %r; treating as measurement_failed",
        job.id,
        result.verdict,
    )
    fallback_payload = MeasurementFailure(
        reason="measurement_crashed",
        detail=f"unexpected verdict: {result.verdict}",
        source_path=result.source_path or "",
    )
    fallback_result = ImportPreviewResult(
        mode=result.mode,
        verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
        decision="unexpected_verdict",
        reason=result.verdict,
        detail=f"unexpected verdict: {result.verdict}",
        source_path=result.source_path,
        action_path=result.action_path,
        request_id=result.request_id,
        download_log_id=result.download_log_id,
        import_result=result.import_result,
        failure=fallback_payload,
    )
    return handle_measurement_failed(fallback_result)


def preview_heartbeat_loop(
    *,
    dsn: str,
    job_id: int,
    stop: threading.Event,
    interval: float = PREVIEW_HEARTBEAT_INTERVAL_SECONDS,
    db_factory: Any | None = None,
    expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
) -> None:
    """Heartbeat a running preview from its own DB session."""
    factory = db_factory or PipelineDB
    db = factory(dsn)
    try:
        while not stop.wait(interval):
            if not db.heartbeat_import_job_preview(
                job_id,
                expected_execution_lease=expected_execution_lease,
            ):
                if cancellation_token is not None:
                    cancellation_token.cancel("preview_heartbeat_rejected")
                return
    except Exception:
        if cancellation_token is not None:
            cancellation_token.cancel("preview_heartbeat_failed")
        logger.warning("Preview heartbeat failed for job %s", job_id, exc_info=True)
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()


def process_claimed_preview_job_with_heartbeat(
    db: Any,
    job: ImportJob,
    *,
    heartbeat_interval: float = PREVIEW_HEARTBEAT_INTERVAL_SECONDS,
    runtime_config: CratediggerConfig | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    automation_authority: _AutomationPreviewAuthority | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
    heartbeat_db_factory: Callable[[str], _PreviewHeartbeatDB] | None = None,
    candidate_measurement_fn: Callable[..., ImportPreviewResult] | None = None,
    automation_materialize_fn: Callable[..., object] | None = None,
) -> ImportJob | None:
    dsn = getattr(db, "dsn", None)
    if not dsn:
        return process_claimed_preview_job(
            db,
            job,
            runtime_config=runtime_config,
            execution_lease=execution_lease,
            automation_authority=automation_authority,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
            candidate_measurement_fn=candidate_measurement_fn,
            automation_materialize_fn=automation_materialize_fn,
        )

    stop = threading.Event()
    heartbeat_thread = threading.Thread(
        target=preview_heartbeat_loop,
        kwargs={
            "dsn": str(dsn),
            "job_id": job.id,
            "stop": stop,
            "interval": heartbeat_interval,
            "db_factory": heartbeat_db_factory or PipelineDB,
            "expected_execution_lease": execution_lease,
            "cancellation_token": cancellation_token,
        },
        daemon=True,
        name=f"preview-heartbeat-{job.id}",
    )
    heartbeat_thread.start()
    try:
        return process_claimed_preview_job(
            db,
            job,
            runtime_config=runtime_config,
            execution_lease=execution_lease,
            automation_authority=automation_authority,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
            candidate_measurement_fn=candidate_measurement_fn,
            automation_materialize_fn=automation_materialize_fn,
        )
    finally:
        stop.set()
        heartbeat_thread.join(timeout=5.0)


@runtime_checkable
class _AutomationPreviewStageDB(Protocol):
    def _pin_owner_session(
        self,
        cancellation_token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...

    def advisory_lock(
        self,
        namespace: int,
        key: int,
    ) -> AbstractContextManager[bool]: ...

    def claim_automation_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None: ...

    def close(self) -> None: ...


@runtime_checkable
class _RequestScopedPreviewStageDB(Protocol):
    """Preview stage-DB surface shared by force-import and local-import
    claiming (issue #1176 PR3): both are request-scoped, IMPORT-pinned
    claims that differ only in which method names the job type — declaring
    both methods on one Protocol lets ``_process_force_claim`` serve either
    lane through its ``claim_fn`` seam without a second isinstance check.
    """

    def _pin_owner_session(
        self,
        cancellation_token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...

    def advisory_lock(
        self,
        namespace: int,
        key: int,
    ) -> AbstractContextManager[bool]: ...

    def claim_force_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_local_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def close(self) -> None: ...


def _claim_force_import_preview(
    stage_db: _RequestScopedPreviewStageDB,
    job_id: int,
    *,
    request_id: int,
    worker_id: str | None,
) -> ImportJob | None:
    return stage_db.claim_force_import_preview_job_under_lock(
        job_id, request_id=request_id, worker_id=worker_id,
    )


def _claim_local_import_preview(
    stage_db: _RequestScopedPreviewStageDB,
    job_id: int,
    *,
    request_id: int,
    worker_id: str | None,
) -> ImportJob | None:
    return stage_db.claim_local_import_preview_job_under_lock(
        job_id, request_id=request_id, worker_id=worker_id,
    )


def _process_automation_claim(
    candidate: ImportJob,
    *,
    dsn: str,
    worker_id: str,
    execution_lease: ExecutionLeaseSnapshot,
    heartbeat_interval: float,
    runtime_config: CratediggerConfig | None,
    stage_db_factory: Callable[[str], object],
    heartbeat_db_factory: Callable[[str], _PreviewHeartbeatDB],
    candidate_measurement_fn: Callable[..., ImportPreviewResult] | None = None,
    claim_callback: Callable[[], None] | None = None,
    process_fn: Callable[..., ImportJob | None] = (
        process_claimed_preview_job_with_heartbeat
    ),
) -> ImportJob | None:
    """Claim and run one preview on its exact pinned IMPORT session."""
    if candidate.request_id is None:
        return None
    stage_db = stage_db_factory(dsn)
    if not isinstance(stage_db, _AutomationPreviewStageDB):
        raise TypeError("preview stage DB is missing its owner-session protocol")
    token = CancellationToken()
    try:
        with stage_db._pin_owner_session(
            token,
        ) as owner_session_identity, stage_db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            job = stage_db.claim_automation_import_preview_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
                execution_lease=execution_lease,
            )
            if job is None:
                return None
            if claim_callback is not None:
                claim_callback()
            logger.info(
                "Claimed import preview job %s (%s)",
                job.id,
                job.job_type,
            )
            authority = _automation_authority_snapshot(
                stage_db,
                job,
                execution_lease,
                runtime_config=runtime_config,
            )
            if authority is None:
                raise AutomationOwnerFailStop(
                    f"claimed automation preview job {job.id} lost its exact "
                    "processing authority"
                )
            token.raise_if_cancelled()
            return process_fn(
                stage_db,
                job,
                heartbeat_interval=heartbeat_interval,
                runtime_config=runtime_config,
                execution_lease=execution_lease,
                automation_authority=authority,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
                heartbeat_db_factory=heartbeat_db_factory,
                candidate_measurement_fn=candidate_measurement_fn,
            )
    finally:
        stage_db.close()


def _process_force_claim(
    candidate: ImportJob,
    *,
    dsn: str,
    worker_id: str,
    heartbeat_interval: float,
    runtime_config: CratediggerConfig | None,
    stage_db_factory: Callable[[str], object],
    heartbeat_db_factory: Callable[[str], _PreviewHeartbeatDB],
    candidate_measurement_fn: Callable[..., ImportPreviewResult] | None = None,
    claim_callback: Callable[[], None] | None = None,
    process_fn: Callable[..., ImportJob | None] = (
        process_claimed_preview_job_with_heartbeat
    ),
    claim_fn: Callable[
        ..., ImportJob | None,
    ] = _claim_force_import_preview,
) -> ImportJob | None:
    """Claim and run force/local-import preview effects on one pinned
    IMPORT session.

    ``claim_fn`` (issue #1176 PR3) defaults to force's own claim method so
    every existing caller stays byte-identical; the local-import lane
    passes ``_claim_local_import_preview`` instead. Everything else — pin
    the owner session, take the per-request IMPORT lock, dispatch to
    ``process_fn`` — is shared, because the ONLY thing that differs between
    the two claims is which method names the job type.
    """
    if candidate.request_id is None:
        return None
    stage_db = stage_db_factory(dsn)
    if not isinstance(stage_db, _RequestScopedPreviewStageDB):
        raise TypeError("force preview DB is missing its owner-session protocol")
    token = CancellationToken()
    try:
        with stage_db._pin_owner_session(
            token,
        ) as owner_session_identity, stage_db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            token.raise_if_cancelled()
            if not acquired:
                return None
            job = claim_fn(
                stage_db,
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
            if job is None:
                return None
            if claim_callback is not None:
                claim_callback()
            logger.info(
                "Claimed import preview job %s (%s)",
                job.id,
                job.job_type,
            )
            return process_fn(
                stage_db,
                job,
                heartbeat_interval=heartbeat_interval,
                runtime_config=runtime_config,
                cancellation_token=token,
                owner_session_identity=owner_session_identity,
                heartbeat_db_factory=heartbeat_db_factory,
                candidate_measurement_fn=candidate_measurement_fn,
            )
    finally:
        stage_db.close()


@dataclass(frozen=True)
class _PreviewClaimAttempt:
    """Everything a preview claim route needs for one candidate, one pass."""

    db: _ImportPreviewJobClaimer
    worker_id: str
    execution_lease: ExecutionLeaseSnapshot | None
    heartbeat_interval: float
    runtime_config: CratediggerConfig | None
    stage_db_factory: Callable[[str], object]
    heartbeat_db_factory: Callable[[str], _PreviewHeartbeatDB]
    #: The caller's own value, ``None`` included. Carried beside the
    #: defaulted one for FIDELITY, not for a runtime hazard: the ladder's
    #: pinned-session arms passed ``heartbeat_db_factory or PipelineDB`` and
    #: its ``else`` arm passed the raw value, and ``process_fn`` reads
    #: ``None`` as "use my own default". Since production never overrides
    #: ``process_fn`` the two are equivalent in production; the distinction
    #: is observable only to a test that inspects the forwarded kwarg, which
    #: is exactly how the ladder's own behaviour is pinned.
    raw_heartbeat_db_factory: Callable[[str], _PreviewHeartbeatDB] | None
    candidate_measurement_fn: Callable[..., ImportPreviewResult] | None
    process_fn: Callable[..., ImportJob | None]
    claim_state: ClaimState

    def dsn(self) -> str | None:
        """The pinned-session routes each open a stage connection of their own."""
        return stage_dsn(self.db)


class _PreviewClaimRoute(Protocol):
    """How ``run_once`` claims and previews one candidate of a given kind."""

    def __call__(
        self,
        candidate: ImportJob,
        attempt: _PreviewClaimAttempt,
    ) -> ImportJob | None: ...


def _preview_route_automation(
    candidate: ImportJob,
    attempt: _PreviewClaimAttempt,
) -> ImportJob | None:
    """Claim one automation candidate on its own pinned IMPORT session.

    Both misses below leave the claim unmarked, so the shared scan moves on
    to the next candidate exactly as the old ``continue`` arms did:
    automation stays invisible without a complete invocation lease, and
    every pinned-session route needs a DSN of its own.
    """
    if attempt.execution_lease is None:
        return None
    dsn = attempt.dsn()
    if dsn is None:
        return None
    return _process_automation_claim(
        candidate,
        dsn=dsn,
        worker_id=attempt.worker_id,
        execution_lease=attempt.execution_lease,
        heartbeat_interval=attempt.heartbeat_interval,
        runtime_config=attempt.runtime_config,
        stage_db_factory=attempt.stage_db_factory,
        heartbeat_db_factory=attempt.heartbeat_db_factory,
        candidate_measurement_fn=attempt.candidate_measurement_fn,
        claim_callback=attempt.claim_state.mark,
        process_fn=attempt.process_fn,
    )


def _preview_route_request_scoped(
    candidate: ImportJob,
    attempt: _PreviewClaimAttempt,
    *,
    claim_fn: Callable[..., ImportJob | None],
) -> ImportJob | None:
    """The force/local-import preview claim, differing only in ``claim_fn``."""
    dsn = attempt.dsn()
    if dsn is None:
        return None
    return _process_force_claim(
        candidate,
        dsn=dsn,
        worker_id=attempt.worker_id,
        heartbeat_interval=attempt.heartbeat_interval,
        runtime_config=attempt.runtime_config,
        stage_db_factory=attempt.stage_db_factory,
        heartbeat_db_factory=attempt.heartbeat_db_factory,
        candidate_measurement_fn=attempt.candidate_measurement_fn,
        claim_callback=attempt.claim_state.mark,
        process_fn=attempt.process_fn,
        claim_fn=claim_fn,
    )


def _preview_route_force_import(
    candidate: ImportJob,
    attempt: _PreviewClaimAttempt,
) -> ImportJob | None:
    return _preview_route_request_scoped(
        candidate, attempt, claim_fn=_claim_force_import_preview,
    )


def _preview_route_local_import(
    candidate: ImportJob,
    attempt: _PreviewClaimAttempt,
) -> ImportJob | None:
    return _preview_route_request_scoped(
        candidate, attempt, claim_fn=_claim_local_import_preview,
    )


def _preview_route_plain(
    candidate: ImportJob,
    attempt: _PreviewClaimAttempt,
) -> ImportJob | None:
    """Claim one candidate on the queue connection, with no pinned session."""
    job = attempt.db.claim_import_preview_job_candidate(
        candidate.id,
        worker_id=attempt.worker_id,
    )
    if job is None:
        return None
    attempt.claim_state.mark()
    logger.info(
        "Claimed import preview job %s (%s)",
        job.id,
        job.job_type,
    )
    return attempt.process_fn(
        attempt.db,
        job,
        heartbeat_interval=attempt.heartbeat_interval,
        runtime_config=attempt.runtime_config,
        heartbeat_db_factory=attempt.raw_heartbeat_db_factory,
        candidate_measurement_fn=attempt.candidate_measurement_fn,
    )


#: The one place every preview CLAIM-ROUTE decision in this module is made
#: — the preview lane's twin of ``scripts/importer.py``'s
#: ``_IMPORT_JOB_KINDS``, which replaced the same if/elif ladder on the
#: import side (issue #1278). Scope, deliberately narrow: this module still
#: decides on ``job_type`` elsewhere (the action-copy prefix table, the
#: preview-input ladder, the front-gate arms); only claiming routes here.
#: Positive routing, like the candidate scans' own ``job_type`` table:
#: every type is named.
_PREVIEW_CLAIM_ROUTES: dict[str, _PreviewClaimRoute] = {
    IMPORT_JOB_AUTOMATION: _preview_route_automation,
    IMPORT_JOB_FORCE: _preview_route_force_import,
    IMPORT_JOB_LOCAL: _preview_route_local_import,
    IMPORT_JOB_YOUTUBE: _preview_route_plain,
}

#: NOT a fifth route: the exact shape the pre-registry ``else`` arm gave a
#: ``job_type`` no adapter claims. ``ImportJob.from_row`` runs
#: ``validate_job_type``, so nothing can reach it today — it exists so an
#: unvalidated type still claims through the unguarded route rather than
#: crashing the scan, exactly as the ladder's ``else`` did.
_UNROUTED_PREVIEW_CLAIM_ROUTE: _PreviewClaimRoute = _preview_route_plain


def _preview_claim_route_for(job_type: str) -> _PreviewClaimRoute:
    """Answer "how is this preview candidate claimed" once, for every caller."""
    return _PREVIEW_CLAIM_ROUTES.get(job_type, _UNROUTED_PREVIEW_CLAIM_ROUTE)


def run_once(
    db: _ImportPreviewJobClaimer,
    *,
    worker_id: str,
    heartbeat_interval: float = PREVIEW_HEARTBEAT_INTERVAL_SECONDS,
    runtime_config: CratediggerConfig | None = None,
    stage_db_factory: Callable[[str], object] | None = None,
    heartbeat_db_factory: Callable[[str], _PreviewHeartbeatDB] | None = None,
    execution_lease_factory: Callable[..., ExecutionLeaseSnapshot] | None = None,
    candidate_measurement_fn: Callable[..., ImportPreviewResult] | None = None,
    process_fn: Callable[..., ImportJob | None] = (
        process_claimed_preview_job_with_heartbeat
    ),
    scan_cursor: CandidateScanCursor | None = None,
) -> ImportJob | None:
    cursor = scan_cursor or CandidateScanCursor()
    execution_lease = capture_worker_execution_lease(
        systemd_unit=PREVIEW_SYSTEMD_UNIT,
        factory=execution_lease_factory,
    )

    def _peek(offset: int) -> list[ImportJob]:
        return db.peek_import_preview_job_candidates(
            execution_lease=execution_lease,
            limit=PREVIEW_CANDIDATE_SCAN_LIMIT,
            offset=offset,
        )

    def _claim(
        candidate: ImportJob,
        claim_state: ClaimState,
    ) -> ImportJob | None:
        attempt = _PreviewClaimAttempt(
            db=db,
            worker_id=worker_id,
            execution_lease=execution_lease,
            heartbeat_interval=heartbeat_interval,
            runtime_config=runtime_config,
            stage_db_factory=stage_db_factory or PipelineDB,
            heartbeat_db_factory=heartbeat_db_factory or PipelineDB,
            raw_heartbeat_db_factory=heartbeat_db_factory,
            candidate_measurement_fn=candidate_measurement_fn,
            process_fn=process_fn,
            claim_state=claim_state,
        )
        return _preview_claim_route_for(candidate.job_type)(candidate, attempt)

    return claim_one_candidate(
        scan_cursor=cursor, peek=_peek, claim=_claim,
    )


def recover_abandoned_preview_jobs(
    db: PipelineDB,
    *,
    older_than: timedelta = PREVIEW_STALE_AGE,
) -> list[ImportJob]:
    return db.requeue_stale_import_preview_jobs(
        older_than=older_than,
        message=STALE_PREVIEW_MESSAGE,
    )


def recover_running_preview_jobs(
    db: PipelineDB,
    *,
    liveness_probe: ExecutionLivenessProbe | None = None,
) -> list[ImportJob]:
    """Requeue every preview job left running by a previous worker process.

    Called once at startup. Legacy Force/local-import/YouTube jobs retain immediate
    same-process recovery. Automation is requeued only when the shared
    execution probe proves the exact persisted lease dead; live, unknown,
    incomplete, or mismatched evidence leaves the row untouched. The periodic
    stale sweep remains legacy-only.
    """
    recovered = db.requeue_running_import_preview_jobs(
        message=RESTART_PREVIEW_MESSAGE,
    )
    for job in db.list_automation_import_jobs_for_startup_recovery():
        if (
            job.status != "queued"
            or job.preview_status != "running"
        ):
            continue
        lease = execution_lease_from_job(job)
        if lease is None:
            continue
        decision = probe_execution_liveness(
            lease,
            probe=liveness_probe,
        )
        recovered_job = db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=decision,
            requeue_message=RESTART_PREVIEW_MESSAGE,
            recovery_message=(
                "Preview execution ended after launch; restarting request "
                "acquisition"
            ),
        )
        if recovered_job is not None:
            recovered.append(recovered_job)
    return recovered


def preview_recovery_loop(
    *,
    dsn: str,
    stop: threading.Event,
    interval: float = PREVIEW_STALE_RECOVERY_INTERVAL_SECONDS,
    db_factory: Any | None = None,
) -> None:
    factory = db_factory or PipelineDB
    db = factory(dsn)
    try:
        while not stop.wait(interval):
            recovered = recover_abandoned_preview_jobs(db)
            if recovered:
                logger.warning(
                    "Requeued %s abandoned import preview job(s)",
                    len(recovered),
                )
    except Exception:
        logger.exception("Import preview recovery loop crashed")
        raise
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()


def run_threaded_workers(
    *,
    dsn: str,
    worker_id: str,
    worker_count: int,
    poll_interval: float,
    runtime_config: CratediggerConfig | None = None,
) -> int:
    stop = threading.Event()
    errors: list[BaseException] = []
    error_lock = threading.Lock()

    def record_error(exc: BaseException) -> None:
        with error_lock:
            errors.append(exc)
        stop.set()

    def worker_loop(index: int) -> None:
        thread_db = PipelineDB(dsn)
        thread_worker_id = f"{worker_id}:preview-{index}"
        scan_cursor = CandidateScanCursor()
        try:
            while not stop.is_set():
                try:
                    job = run_once(
                        thread_db,
                        worker_id=thread_worker_id,
                        runtime_config=runtime_config,
                        scan_cursor=scan_cursor,
                    )
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    # Transient DB connection loss — the live failure mode
                    # is PostgreSQL dropping the worker's idle connection
                    # between jobs. ``PipelineDB._execute`` reconnects on
                    # subsequent calls, so we just need to back off and
                    # keep polling rather than tearing the whole process
                    # down. A persistent failure will surface as repeated
                    # warnings and either Postgres recovery or systemd
                    # restart resolves it.
                    logger.warning(
                        "Import preview worker thread %s lost DB connection; "
                        "backing off and retrying: %s",
                        index, exc,
                    )
                    stop.wait(poll_interval)
                    continue
                if job is None:
                    stop.wait(poll_interval)
        except BaseException as exc:
            record_error(exc)
            logger.exception("Import preview worker thread %s crashed", index)
        finally:
            thread_db.close()

    def recovery_loop() -> None:
        try:
            preview_recovery_loop(dsn=dsn, stop=stop, db_factory=PipelineDB)
        except BaseException as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            record_error(exc)

    threads = [
        threading.Thread(target=worker_loop, args=(i,), daemon=False)
        for i in range(worker_count)
    ]
    recovery_thread = threading.Thread(
        target=recovery_loop,
        daemon=False,
        name="preview-recovery",
    )
    recovery_thread.start()
    for thread in threads:
        thread.start()
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        stop.set()
        for thread in threads:
            thread.join()
        recovery_thread.join()
        return 0

    stop.set()
    recovery_thread.join()

    if errors:
        logger.error(
            "Import preview worker exiting after %s worker thread crash(es)",
            len(errors),
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run async previews for Cratedigger import jobs",
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument(
        "--config",
        default=None,
        help="Immutable runtime config (default: env or cwd/config.ini)",
    )
    parser.add_argument(
        "--runtime-dir",
        default=None,
        help="Mutable runtime directory (default: cwd)",
    )
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be >= 1")
    config_path, runtime_dir = resolve_startup_config_paths(
        config_path=args.config,
        runtime_dir=args.runtime_dir,
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    try:
        admitted_config = enforce_beets_startup(
            role="preview",
            config_path=config_path,
            runtime_dir=runtime_dir,
            logger=logger,
        )
    except BeetsStartupError:
        return 1

    # Startup write-probe (issue #1085): fail loudly, before any queue
    # recovery, claim, DB mutation, or filesystem mutation, if a required
    # path cannot be used the way this unit is about to use it.
    from lib.startup_write_probe import (
        StartupProbeError,
        preview_worker_required_paths,
        probe_startup_paths,
    )
    required_paths = preview_worker_required_paths(admitted_config)
    try:
        probe_startup_paths(
            unit="cratedigger-import-preview-worker",
            logger=logger,
            required=required_paths,
        )
    except StartupProbeError:
        return 1

    worker_id = args.worker_id or f"{socket.gethostname()}:{os.getpid()}"
    db = PipelineDB(args.dsn)
    try:
        recovered = recover_running_preview_jobs(db)
        if recovered:
            logger.warning(
                "Requeued %s abandoned import preview job(s)",
                len(recovered),
            )

        if not args.once:
            return run_threaded_workers(
                dsn=args.dsn,
                worker_id=worker_id,
                worker_count=args.workers,
                poll_interval=args.poll_interval,
                runtime_config=admitted_config,
            )

        run_once(
            db,
            worker_id=worker_id,
            runtime_config=admitted_config,
            scan_cursor=CandidateScanCursor(),
        )
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
