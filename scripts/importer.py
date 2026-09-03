"""Drain the shared import queue through one beets-mutating lane."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol, assert_never, runtime_checkable

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db import DownloadLogOutcome

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib import transitions
from lib.automation_recovery_debris import (
    RecoveryDebrisRemovalFn,
    remove_recovery_debris,
)
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import (
    CratediggerConfig,
    resolve_startup_config_paths,
)
from lib.dispatch import (
    DISPATCH_CODE_REQUEUE_EXHAUSTED,
    DISPATCH_CODE_REQUEUE_FAILED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    DispatchOutcome,
    _requeue_import_job_to_preview,
)
from lib.dispatch.types import (
    PostCommitCleanup,
)
from lib.download_processing import (
    Completed,
    CompletionDeferred,
    CompletionDispatched,
    CompletionFailed,
    CompletionResult,
    ProcessAlbumFn,
)
from lib.import_execution import (
    AutomationOwnerCheckpointDB,
    AutomationOwnerFailStop,
    CancellationToken,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessProbe,
    ExecutionOwnerProof,
    OwnerSessionIdentity,
    checkpoint,
    checkpoint_automation_owner,
    probe_execution_liveness,
)
from lib.import_manifest import audio_relative_paths
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_RECOVERY_REQUIRED,
    IMPORT_JOB_YOUTUBE,
    ForceImportPayload,
    ImportJob,
    ImportJobPayload,
    LocalImportPayload,
    YoutubeImportPayload,
)
from lib.import_worker_loop import (
    CandidateScanCursor,
    ClaimState,
    GracefulShutdown,
    capture_worker_execution_lease,
    claim_one_candidate,
    execution_lease_from_job,
    stage_dsn,
)
from lib.mb_canonical import configure_canonical_release_lookup
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_IMPORTER,
    DEFAULT_DSN,
    CleanupJournalIntent,
    CleanupJournalReceipt,
    PipelineDB,
)
from lib.pipeline_db._core import OwnerSessionLost
from lib.pipeline_db.import_jobs import (
    AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX,
    _default_force_action_copy_path,
    automation_completion_receipt,
)
from lib.pipeline_db.rows import AlbumRequestRow
from lib.processing_cleanup import (
    PROCESSING_CLEANUP_NO_OP,
    PROCESSING_CLEANUP_QUARANTINE_SOURCE,
    OwnerProcessingCleanupDB,
    canonical_source_cleanup_intent,
    complete_owner_processing_cleanup,
)
from lib.quality import ActiveDownloadFileState, ActiveDownloadState
from lib.terminal_outcomes import (
    AUTOMATION_WORLD_FAILURE_RESULT_KEY,
    AutomationTerminalAuthority,
    ImportJobTerminal,
    PendingImportTerminalOutcome,
    non_automation_failure_terminal_outcome,
)
from lib.wrong_matches import WrongMatchSourceDB
from lib.youtube_ingest_service import (
    YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES,
)

logger = logging.getLogger("cratedigger-importer")
RESTART_REQUEUE_MESSAGE = "Importer restarted while job was running; retry queued"
RESTART_RECOVERY_MESSAGE = (
    "Importer restarted after Beets launch authorization"
)
# One label for every automation world failure, whichever path surfaces it, so
# a single Recents read tells the operator the request self-healed rather than
# stopped. Owned by the recovery cluster; aliased here for this module's users.
_WORLD_FAILURE_AUDIT_PREFIX = AUTOMATION_WORLD_FAILURE_AUDIT_PREFIX
IMPORT_CANDIDATE_SCAN_LIMIT = 32
IMPORTER_SYSTEMD_UNIT = "cratedigger-importer.service"
# A dead owner must not wait for the next process restart to be noticed. The
# startup sweep cannot see a transient procfs failure coming, so the same exact
# death proof is re-run on this cadence for as long as the worker lives.
AUTOMATION_RECOVERY_REPROBE_INTERVAL_SECONDS = 300.0


def _job_result(outcome: DispatchOutcome) -> dict[str, Any]:
    return {
        "success": outcome.success,
        "message": outcome.message,
        "deferred": outcome.deferred,
        "code": outcome.code,
        # Durably records the decision ``_cleanup_failed_force_import`` (the
        # failure lane) needs, so a startup replay can rebuild an equivalent
        # ``DispatchOutcome`` from persisted state alone after a crash
        # between the terminal commit and the live wrong-match cleanup call
        # (issue #1122) — never from a live outcome the process may not
        # still have.
        "post_commit_wrong_match_scenario": outcome.post_commit_wrong_match_scenario,
    }


# --- Per-kind import job adapters ------------------------------------------
#
# One registry answers "which import job kind is this" for every
# job-type-DISPATCH decision in this module: which executor runs the job,
# which claim route ``run_once`` takes, which execution authority
# ``process_claimed_job`` demands, whether the kind retains a job-scoped
# private action copy, and which of the two Wrong Matches roles it plays.
# Every such consumer below reads the kind instead of re-deriving the answer
# from ``job.job_type`` at its own point of use.
#
# One job-type decision deliberately stays OUTSIDE the registry:
# ``_automation_claim_is_current``'s ``current.job_type ==
# IMPORT_JOB_AUTOMATION`` is one clause of an exact-identity conjunction
# over the re-read owner job AND its request row — the job's id,
# request_id, job_type, status, preview_status, launch fence and execution
# lease, plus the request's status, owner pointer and active download
# state — not a routing question. Restating it as an authority-class
# comparison would weaken it, since two kinds may share an authority
# class. The remaining ``job.job_type`` reads below are log arguments and
# message interpolation, which are not decisions at all.
#
# The four lanes stay four: this is convergence of dispatch, never a fifth
# Beets-mutation lane (CLAUDE.md § Decision architecture).

#: ``process_claimed_job`` demands the exact automation execution lease AND a
#: pinned owner session, and forwards all three to the executor.
_AUTHORITY_AUTOMATION = "automation"
#: A request-scoped pinned-IMPORT claim (force / local-import, issue #1176
#: PR3): the cancellation token and pinned session are optional, must be
#: PAIRED, and are forwarded to the executor whenever present.
_AUTHORITY_PINNED_PAIR = "pinned_pair"
#: No pinned session at all (youtube): the executor is called plainly.
_AUTHORITY_PLAIN = "plain"


@dataclass(frozen=True)
class _ActionCopySource:
    """The originating-source facts one action-copy lane hands dispatch."""

    source_reference_path: str | None
    source_username: str | None
    source_dirs: list[str] | None
    download_log_id: int | None


def _force_action_copy_source(payload: ImportJobPayload) -> _ActionCopySource:
    """Force-import exposes its originating Wrong Matches folder and peer."""
    # Programmer-error invariant (the payload is already decoded/validated
    # by job_type at read time), not a caller type error — mirrors every
    # other "<job_type> payload type mismatch" AssertionError in this file
    # and its sibling in scripts/import_preview_worker.py.
    if not isinstance(payload, ForceImportPayload):
        raise AssertionError("force_import payload type mismatch")  # noqa: TRY004
    return _ActionCopySource(
        source_reference_path=payload.failed_path,
        source_username=payload.source_username,
        source_dirs=(
            [source_dir for source_dir in payload.source_dirs if source_dir]
            or None
        ),
        download_log_id=payload.download_log_id,
    )


def _local_action_copy_source(payload: ImportJobPayload) -> _ActionCopySource:
    """Local-import exposes no originating source to dispatch at all.

    LOCAL delegates through the same ``dispatch_import_from_db`` entry point
    as FORCE (issue #1176 PR3), differing in the 3 documented ways: no
    operator-path audit exposure (``source_reference_path=None`` — CLAUDE.md
    decision 2), the strict automation distance threshold instead of
    ``FORCE_IMPORT_DISTANCE_THRESHOLD`` (decision 3), and its own attempt
    scenario (decision 4). There is no Soulseek peer behind a local import,
    so ``source_username``/``source_dirs`` stay unset — mirrors
    ``youtube_import``, which never populates them either.
    """
    # Programmer-error invariant, exactly as in the force sibling above.
    if not isinstance(payload, LocalImportPayload):
        raise AssertionError("local_import payload type mismatch")  # noqa: TRY004
    return _ActionCopySource(
        source_reference_path=None,
        source_username=None,
        source_dirs=None,
        download_log_id=None,
    )


def _force_import_distance_threshold(cfg: CratediggerConfig) -> float | None:
    """Force import overrides the Beets distance instead of resolving one."""
    del cfg
    return None


def _configured_distance_threshold(cfg: CratediggerConfig) -> float | None:
    """The ordinary automation validation threshold."""
    return cfg.beets_distance_threshold


@dataclass(frozen=True)
class _ActionCopyLane:
    """Everything the shared action-copy dispatch body differs on per lane."""

    job_type: str
    lane_label: str
    scenario: DownloadLogOutcome
    distance_threshold_fn: Callable[[CratediggerConfig], float | None]
    source_fn: Callable[[ImportJobPayload], _ActionCopySource]

    def action_copy_prefix(self) -> str:
        """Read this lane's prefix from the one table that owns it.

        ``ACTION_COPY_PREFIX_BY_JOB_TYPE`` stays the single source for the
        job-type-to-prefix mapping (issue #1176 PR3 F5): a wrong prefix
        compares the retained copy against another job type's deterministic
        name and raises ``FilesystemAuthorityError`` before ever touching
        the filesystem.
        """
        from lib.preview_snapshot import ACTION_COPY_PREFIX_BY_JOB_TYPE

        return ACTION_COPY_PREFIX_BY_JOB_TYPE[self.job_type]


_FORCE_ACTION_COPY_LANE = _ActionCopyLane(
    job_type=IMPORT_JOB_FORCE,
    lane_label="force",
    scenario="force_import",
    distance_threshold_fn=_force_import_distance_threshold,
    source_fn=_force_action_copy_source,
)

_LOCAL_IMPORT_ACTION_COPY_LANE = _ActionCopyLane(
    job_type=IMPORT_JOB_LOCAL,
    lane_label="local-import",
    scenario="local_import",
    distance_threshold_fn=_configured_distance_threshold,
    source_fn=_local_action_copy_source,
)


class _JobKindExecutor(Protocol):
    """One kind's entry into its own execution lane.

    Every kind takes the same complete argument set and ignores what its own
    lane does not use, so ``execute_import_job`` hands the same call to all
    of them rather than shaping one per type.
    """

    def __call__(
        self,
        db: PipelineDB,
        job: ImportJob,
        *,
        request_id: int,
        ctx: object,
        execution_lease: ExecutionLeaseSnapshot | None,
        cancellation_token: CancellationToken | None,
        owner_session_identity: OwnerSessionIdentity | None,
        force_dispatch_fn: Callable[..., DispatchOutcome] | None,
        force_runtime_config: CratediggerConfig | None,
    ) -> DispatchOutcome: ...


@dataclass(frozen=True)
class _ClaimAttempt:
    """Everything a claim route needs for one candidate on one poll pass."""

    db: PipelineDB
    worker_id: str
    ctx: object
    execution_lease: ExecutionLeaseSnapshot | None
    stage_db_factory: Callable[[str], object]
    execute_fn: Callable[..., DispatchOutcome]
    claim_state: ClaimState

    def dsn(self) -> str | None:
        """The pinned-session routes each open a stage connection of their own."""
        return stage_dsn(self.db)


class _JobKindClaimRoute(Protocol):
    """How ``run_once`` claims and runs one candidate of a given kind."""

    def __call__(
        self,
        candidate: ImportJob,
        attempt: _ClaimAttempt,
    ) -> ImportJob | None: ...


@dataclass(frozen=True)
class _ImportJobKind:
    """One import job type's complete adapter."""

    authority: str
    #: Named ``execute_fn``, not ``execute``: ``tests/test_replaced_write_
    #: audit.py``'s deliberately bounded grammar treats every ``.execute(...)``
    #: call as a DB cursor executing SQL it must resolve statically, and a
    #: registry-dispatched call cannot be. The name matches this module's own
    #: ``execute_fn`` kwarg convention anyway. Note the module now spells
    #: ``execute_fn`` for two different things: THIS is the fixed per-kind
    #: adapter, chosen by job type and never injected, whereas
    #: ``run_once``/``process_claimed_job``/``_ClaimAttempt``'s ``execute_fn``
    #: is the injectable executor seam (default ``execute_import_job``) that a
    #: caller may replace. The registry selects; the seam substitutes.
    execute_fn: _JobKindExecutor
    claim_route: _JobKindClaimRoute
    #: The job-scoped private action copy this lane retains, if any.
    action_copy: _ActionCopyLane | None
    #: Whether a terminal rejection this job produced converges a NEW Wrong
    #: Matches row (``_cleanup_committed_wrong_match_rejection``). Force is
    #: the exception: it never reaches that reducer at all — see
    #: ``docs/rejection-routing.md``.
    runs_committed_wrong_match_cleanup: bool
    #: Whether this job's payload names an EXISTING Wrong Matches row that
    #: its terminal outcome consumes (``_dismiss_successful_force_import`` /
    #: ``_cleanup_failed_force_import``).
    owns_wrong_match_source: bool


class _AutomationCleanupDB(
    AutomationOwnerCheckpointDB,
    OwnerProcessingCleanupDB,
    Protocol,
):
    """Exact persistence surface for owned processor cleanup."""

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...


class _AutomationRecoveryDB(Protocol):
    """Persistence surface the re-runnable automation recovery sweep needs."""

    def list_automation_import_jobs_for_startup_recovery(
        self,
    ) -> list[ImportJob]: ...

    def recover_automation_import_job(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None,
        decision: ExecutionLivenessDecision,
        requeue_message: str,
        recovery_message: str,
        debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
    ) -> ImportJob | None: ...


class _ForceActionCleanupDB(Protocol):
    """Persistence surface for durable force-action cleanup receipts."""

    def list_terminal_force_action_cleanup_jobs(self) -> list[ImportJob]: ...

    def merge_import_job_result(
        self,
        job_id: int,
        patch: dict[str, object],
    ) -> ImportJob | None: ...


class _ForceWrongMatchCleanupDB(_ForceActionCleanupDB, WrongMatchSourceDB, Protocol):
    """Persistence surface for durable force wrong-match source receipts.

    Sibling of ``_ForceActionCleanupDB`` for the OTHER terminal force
    receipt (issue #1122): the original Wrong Matches source's row
    consumption and folder deletion/preservation. Extends
    ``WrongMatchSourceDB`` because the replay calls the exact same
    ``_dismiss_successful_force_import`` / ``_cleanup_failed_force_import``
    helpers the live path calls.
    """

    def list_terminal_force_wrong_match_cleanup_jobs(self) -> list[ImportJob]: ...


class _StartupRecoveryDB(
    _AutomationRecoveryDB,
    _ForceWrongMatchCleanupDB,
    Protocol,
):
    """Complete persistence surface used by the one-shot startup sweep."""

    def recover_running_import_jobs(
        self,
        *,
        requeue_message: str,
        recovery_message: str,
        limit: int = 50,
        debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
        force_action_copy_path_fn: Callable[
            [int], str,
        ] = _default_force_action_copy_path,
    ) -> list[ImportJob]: ...


def _run_post_commit_cleanup(
    outcome: DispatchOutcome,
) -> dict[str, object] | None:
    """Run narrow convergence only after terminal acknowledgement."""
    plan = outcome.post_commit_cleanup
    if plan is None:
        return None

    details: dict[str, object] = {}
    if plan.duplicate_guard_source_path is not None:
        try:
            from lib.duplicate_remove_guard import (
                quarantine_duplicate_remove_guard_source,
            )

            quarantine = quarantine_duplicate_remove_guard_source(
                source_path=plan.duplicate_guard_source_path,
                staging_dir=plan.duplicate_guard_staging_dir or "",
                request_id=plan.duplicate_guard_request_id,
            )
            details["duplicate_guard_quarantine"] = {
                "source_path": quarantine.source_path,
                "quarantine_path": quarantine.quarantine_path,
                "moved": quarantine.moved,
                "already_quarantined": quarantine.already_quarantined,
                "path_missing": quarantine.path_missing,
                "error": quarantine.error,
            }
        except Exception as exc:
            logger.exception("Post-commit duplicate-guard quarantine failed")
            details["duplicate_guard_quarantine"] = {
                "source_path": plan.duplicate_guard_source_path,
                "error": f"{type(exc).__name__}: {exc}",
            }

    if plan.staged_path is not None:
        try:
            from lib.dispatch.helpers import _cleanup_staged_dir

            # Issue #1077, R3-3 (widened issue #1122, review round 2):
            # this is a real caller of ``_cleanup_staged_dir`` furthest
            # from ``cfg`` — the guard has to travel through the
            # ``PostCommitCleanup`` plan itself
            # (``staged_path_protected_parents``) since this function only
            # ever receives ``outcome: DispatchOutcome``.
            _cleanup_staged_dir(
                plan.staged_path,
                protected_parents=plan.staged_path_protected_parents,
            )
            details["staged_path"] = {
                "path": plan.staged_path,
                "success": True,
            }
        except Exception as exc:
            logger.exception("Post-commit staged-path cleanup failed")
            details["staged_path"] = {
                "path": plan.staged_path,
                "success": False,
                "error": f"{type(exc).__name__}: {exc}",
            }

    return details or None


def _select_missing_destination(path: str, *, separator: str) -> str:
    candidate = os.path.abspath(path)
    index = 1 if separator == "_" else 2
    while os.path.lexists(candidate):
        candidate = f"{os.path.abspath(path)}{separator}{index}"
        index += 1
    return candidate


def _automation_cleanup_intent(
    *,
    source_path: str,
    plan: PostCommitCleanup | None,
) -> CleanupJournalIntent:
    # One inspection, shared with every proven owner: the plan-free canonical
    # intent is the base, and a plan may only retarget WHERE the already
    # measured manifest goes, never re-measure it.
    canonical = canonical_source_cleanup_intent(source_path)
    if canonical.action == PROCESSING_CLEANUP_NO_OP:
        return canonical
    manifest = canonical.source_manifest
    manifest_hash = canonical.source_manifest_hash

    # No audio-corrupt quarantine branch here (issue #1077, D3): a bad rip's
    # owned canonical folder is deleted outright, so it falls straight
    # through to the plan-free ``canonical`` intent below (REMOVE_SOURCE),
    # exactly like every other reject that carries no retargeting plan.

    if (
        plan is not None
        and plan.duplicate_guard_source_path is not None
    ):
        if os.path.abspath(plan.duplicate_guard_source_path) != source_path:
            raise RuntimeError(
                "duplicate guard plan does not name the canonical owner path"
            )
        from lib.processing_paths import duplicate_remove_guard_path

        base = duplicate_remove_guard_path(
            staging_dir=plan.duplicate_guard_staging_dir or "",
            source_path=source_path,
            request_id=plan.duplicate_guard_request_id,
        )
        destination = _select_missing_destination(base, separator="-")
        return CleanupJournalIntent(
            action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
            source_path=source_path,
            source_manifest=manifest,
            source_manifest_hash=manifest_hash,
            destination_path=destination,
            destination_manifest=manifest,
            destination_manifest_hash=manifest_hash,
            selected_destination_path=destination,
        )

    if (
        plan is not None
        and plan.staged_path is not None
        and os.path.abspath(plan.staged_path) != source_path
    ):
        raise RuntimeError(
            "staged cleanup plan does not name the canonical owner path"
        )
    return canonical


def _complete_automation_processing_cleanup(
    db: _AutomationCleanupDB,
    job: ImportJob,
    outcome: DispatchOutcome,
    *,
    execution_lease: ExecutionLeaseSnapshot,
    cancellation_token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
) -> CleanupJournalReceipt:
    if job.request_id is None:
        raise RuntimeError("automation cleanup job has no request")
    request = db.get_request(job.request_id)
    if request is None or request.get("status") != "processing":
        raise RuntimeError("automation cleanup request is no longer processing")
    raw_state = request.get("active_download_state")
    if raw_state is None:
        raise RuntimeError("automation cleanup request lost active state")
    state = ActiveDownloadState.from_raw(raw_state)
    if state.current_path is None:
        raise RuntimeError("automation cleanup request has no canonical path")
    source_path = os.path.abspath(state.current_path)

    def checkpoint() -> None:
        checkpoint_automation_owner(
            db,
            import_job_id=job.id,
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )

    def intent_factory(path: str) -> CleanupJournalIntent:
        return _automation_cleanup_intent(
            source_path=path,
            plan=outcome.post_commit_cleanup,
        )

    return complete_owner_processing_cleanup(
        db,
        request_id=job.request_id,
        job_id=job.id,
        source_path=source_path,
        owner_checkpoint=checkpoint,
        intent_factory=intent_factory,
    )




def _force_job_wrong_match_payload(job: ImportJob) -> tuple[int, str | None] | None:
    if not _kind_for(job.job_type).owns_wrong_match_source:
        return None
    if not isinstance(job.payload, ForceImportPayload):
        raise TypeError("force_import payload type mismatch")
    return job.payload.download_log_id, job.payload.failed_path


def _force_action_path(job: ImportJob) -> str | None:
    """Return the retained private copy selected by this preview, if any."""
    preview = job.preview_result
    value = preview.get("action_path") if preview is not None else None
    return value if isinstance(value, str) and value else None


def _cleanup_terminal_force_action(job: ImportJob) -> dict[str, object] | None:
    """Best-effort cleanup after a terminal force outcome.

    The action copy must outlive Beets, but it is disposable once no launch is
    pending.  Cleanup is deliberately reported after the real outcome rather
    than allowed to replace it.
    """
    action_path = _force_action_path(job)
    if action_path is None:
        return None
    try:
        from lib.config import read_runtime_config
        from lib.preview_snapshot import cleanup_force_action_copy_for_job

        cfg = read_runtime_config()
        # issue #1176 PR3 F5: this call used to hardcode force's own
        # "force-action-" prefix, so it compared every local-import action
        # path against the WRONG job type's deterministic name and raised
        # before ever touching the filesystem — the copy leaked forever and
        # re-raised on every subsequent importer startup recovery sweep. The
        # kind's own action-copy lane resolves the prefix from
        # ``ACTION_COPY_PREFIX_BY_JOB_TYPE``, the same table
        # scripts/import_preview_worker.py's sibling cleanup already uses.
        # issue #1246 item 2: an unrecognized ``job.job_type`` fails CLOSED
        # (skips cleanup, returns ``None``) instead of silently defaulting
        # to ``FORCE_ACTION_PREFIX`` — matching the sibling
        # ``scripts/import_preview_worker.py::_cleanup_terminal_preview_
        # force_action``'s own ``prefix is None`` early return. The
        # fail-open default was left in place by issue #1211 PR1 as an
        # explicit open question; this closes it now that the prefix table
        # is single-sourced. Since issue #1278 the same fail-closed answer
        # comes from the kind registry: a lane-less kind (automation,
        # youtube) and an unregistered type alike carry
        # ``action_copy is None``. Both the old default and the early
        # return are dead today two independent ways:
        # ``ImportJob.from_row`` runs ``validate_job_type``, so
        # ``job.job_type`` can only ever be one of the four
        # ``IMPORT_JOB_TYPES``; and of the two types with no lane
        # (``automation_import``, ``youtube_import``), the small minority of
        # live doc2 rows of either type that carry an ``action_path`` key AT
        # ALL carry it as JSON ``null`` (not a string) -- most rows of both
        # types have no such key, and none observed to date has a non-null
        # one -- so ``_force_action_path`` above already returns ``None``
        # and this function never reaches here for either. A live doc2 count
        # is deliberately not reproduced here: the table grows continuously
        # (rows accrued between successive measurements already differed),
        # so a frozen number inside a permanent comment would only go stale.
        # Re-query ``import_jobs`` for the current counts if this needs
        # re-verifying.
        lane = _kind_for(job.job_type).action_copy
        if lane is None:
            return None
        cleanup_force_action_copy_for_job(
            action_path,
            cfg,
            import_job_id=job.id,
            prefix=lane.action_copy_prefix(),
        )
        return {"action_path": action_path, "removed": True}
    except Exception as exc:
        logger.exception("Failed to remove retained force action copy %s", action_path)
        return {
            "action_path": action_path,
            "removed": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _record_terminal_force_action_cleanup(
    db: _ForceActionCleanupDB,
    job: ImportJob,
    terminal_job: ImportJob | None,
) -> ImportJob | None:
    cleanup = _cleanup_terminal_force_action(job)
    if cleanup is None:
        return terminal_job
    try:
        merged = db.merge_import_job_result(
            job.id, {"force_action_cleanup": cleanup},
        )
    except Exception:  # terminal acknowledgement must remain authoritative
        logger.exception(
            "Failed to record retained force action cleanup for job %s", job.id,
        )
        return terminal_job
    return merged or terminal_job


def _cleanup_failed_force_import(
    db: WrongMatchSourceDB,
    job: ImportJob,
    outcome: DispatchOutcome,
) -> dict[str, object] | None:
    if outcome.deferred:
        return None
    force_payload = _force_job_wrong_match_payload(job)
    if force_payload is None:
        return None
    download_log_id, failed_path_hint = force_payload
    if outcome.post_commit_wrong_match_scenario == "audio_corrupt":
        # Bad rips are ban + delete, never preserved (issue #1077, D3): the
        # peer is already denylisted by ``dispatch_action("audio_corrupt")``.
        # ``staged_path`` in the dispatch-level post-commit plan names the
        # disposable force action copy, not this ORIGINAL Wrong Matches
        # source — delete it here, reusing the same source-deletion helper
        # the successful-force-import (D7) and cleanup-reducer paths use
        # (no new teardown machinery, CLAUDE.md invariant 7).
        # ``clear_missing=True`` (issue #1077, R3-2 — corrects round-2's
        # false premise). ``clear_missing`` is NEVER consulted for an
        # unobservable path: ``cleanup_wrong_match_source`` short-circuits
        # at the indeterminate-observation check
        # (``lib/wrong_matches.py``) and returns with nothing cleared,
        # BEFORE ``clear_missing`` is ever read — an unreadable folder
        # already can't clear its pointer here, regardless of this flag.
        # ``clear_missing`` only governs a DETERMINATE absence (the path
        # was positively proven gone, or vanished between resolve and
        # delete). This force-failure IS an explicit operator action
        # completing (the operator chose this exact candidate to force-
        # import; the terminal audio_corrupt failure is the answer), so a
        # proven-gone source must clear its pointer here rather than
        # leaving a phantom row the worklist can never act on again
        # (``wrong_match_row_is_visible`` never consults the filesystem).
        # Only the autonomous reducer (``lib.wrong_match_cleanup_service``)
        # keeps ``False`` — a quality decision second-guessing its own
        # read, not an operator's completed action.
        from lib.wrong_matches import cleanup_wrong_match_source

        result = cleanup_wrong_match_source(
            db,
            download_log_id,
            failed_path_hint=failed_path_hint,
            clear_missing=True,
        )
        payload = result.to_dict()
        payload["outcome"] = "deleted_operator_force_source"
        payload["dispatch_code"] = outcome.code
        payload["dispatch_message"] = outcome.message
        return payload
    # The original force/quarantine directory is operator authority and audit
    # evidence. Dispatch consumes only the private action copy; cleanup of the
    # raw source requires a distinct operator action, never a quality result.
    return {
        "success": True,
        "download_log_id": download_log_id,
        "failed_path_hint": failed_path_hint,
        "outcome": "preserved_operator_force_source",
        "skipped": True,
        "dispatch_code": outcome.code,
        "dispatch_message": outcome.message,
    }


def _dismiss_successful_force_import(
    db: WrongMatchSourceDB,
    job: ImportJob,
) -> dict[str, object] | None:
    """Consume a successfully imported source's Wrong Matches folder.

    This runs only after the terminal acknowledgement. Force-import success
    completes the operator's own explicit action (issue #1077, D7): the
    quarantine folder is deleted, not merely dismissed from the actionable
    list, reusing the same source-deletion helper the cleanup reducer uses
    (``lib.wrong_match_cleanup_service.cleanup_wrong_match``). Failure keeps
    ``preserved_operator_force_source`` (``_cleanup_failed_force_import``)
    exactly as-is.
    """
    force_payload = _force_job_wrong_match_payload(job)
    if force_payload is None:
        return None
    download_log_id, failed_path_hint = force_payload
    try:
        from lib.wrong_matches import cleanup_wrong_match_source

        # ``clear_missing=True`` (the library default, stated explicitly
        # here for clarity) — matches the D3 force-failure path above,
        # both now the OPPOSITE of the autonomous reducer's own override
        # (issue #1077, R3-2, correcting round-2's false premise).
        # ``clear_missing`` is never even reached for an unobservable path
        # — ``cleanup_wrong_match_source`` short-circuits at the
        # indeterminate-observation check and returns with nothing
        # cleared regardless of this flag (see
        # ``lib/wrong_matches.py::cleanup_wrong_match_source``). It only
        # governs a DETERMINATE absence: force-import already succeeded,
        # so a proven-gone source folder must clear its pointer here — a
        # dead pointer left visible would be the stranded-phantom state,
        # an already-imported row that looks stuck in the queue forever,
        # not a safety net (``wrong_match_row_is_visible`` never consults
        # the filesystem). Only the autonomous reducer
        # (``lib.wrong_match_cleanup_service``) keeps ``False`` — a
        # quality decision second-guessing its own read, not an
        # operator's completed action.
        return cleanup_wrong_match_source(
            db,
            download_log_id,
            failed_path_hint=failed_path_hint,
            clear_missing=True,
        ).to_dict()
    except Exception as exc:
        logger.exception(
            "Failed to dismiss successful force import source for job %s",
            job.id,
        )
        return {
            "success": False,
            "download_log_id": download_log_id,
            "failed_path_hint": failed_path_hint,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _replay_force_wrong_match_outcome(job: ImportJob) -> DispatchOutcome:
    """Rebuild a minimal outcome from durable state, never a live one.

    Startup replay (issue #1122) has no ``DispatchOutcome`` — the process
    that produced it may be long gone. ``_cleanup_failed_force_import``
    reads ``deferred``, the wrong-match ``post_commit_wrong_match_scenario``
    decision, and ``code``/``message`` for audit text — every one of those
    was durably persisted into ``result`` by ``_job_result`` at the SAME
    terminal commit that wrote the job's ``failed`` status, so they are
    read back here instead of invented.

    ``deferred`` matters even at a terminal ``failed`` status: a force job
    can reach ``failed`` with ``deferred=True`` (e.g. release-lock
    contention — ``lib/dispatch/core.py``'s ``"Another import is already
    in progress"`` outcome) — the live call still skips the wrong-match
    decision via ``_cleanup_failed_force_import``'s own first line, and
    replay must reach that identical no-op rather than invent a delete/
    preserve verdict the live path never made. The startup query's own
    predicate already excludes these rows from ever reaching this
    function; reconstructing ``deferred`` here too is defense in depth,
    not the only guard.
    """
    stored = job.result or {}
    message = stored.get("message")
    code = stored.get("code")
    scenario = stored.get("post_commit_wrong_match_scenario")
    return DispatchOutcome(
        success=False,
        message=message if isinstance(message, str) else "",
        deferred=stored.get("deferred") is True,
        code=code if isinstance(code, str) else None,
        post_commit_wrong_match_scenario=(
            scenario if isinstance(scenario, str) else None
        ),
    )


def _record_terminal_force_wrong_match_cleanup(
    db: _ForceWrongMatchCleanupDB,
    job: ImportJob,
) -> ImportJob | None:
    """Replay the durable wrong-match source receipt for one terminal force job.

    Sibling of ``_record_terminal_force_action_cleanup`` (issue #1122): a
    crash between the terminal commit and the live
    ``_dismiss_successful_force_import`` (success) /
    ``_cleanup_failed_force_import`` (failure) call leaves that receipt
    missing. This calls the SAME helper the live path calls — so the
    delete/preserve policy is identical either way — over durably
    persisted state alone, and is idempotent: retried every startup until
    the receipt lands (CLAUDE.md invariant 11).
    """
    if job.status == "completed":
        dismissal = _dismiss_successful_force_import(db, job)
        key = "wrong_match_dismissal"
    elif job.status == "failed":
        dismissal = _cleanup_failed_force_import(
            db, job, _replay_force_wrong_match_outcome(job),
        )
        key = "cleanup"
    else:
        return None
    if dismissal is None:
        return None
    try:
        return db.merge_import_job_result(job.id, {key: dismissal})
    except Exception:
        logger.exception(
            "Failed to record wrong-match cleanup receipt for job %s", job.id,
        )
        return None


def _execute_action_copy_dispatch(
    db: PipelineDB,
    job: ImportJob,
    *,
    lane: _ActionCopyLane,
    request_id: int,
    ctx: object,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
    force_dispatch_fn: Callable[..., DispatchOutcome] | None,
    force_runtime_config: CratediggerConfig | None,
) -> DispatchOutcome:
    """Shared job-scoped-action-copy dispatch body for force/local-import
    (issue #1176 PR3).

    Delegates straight to ``dispatch_import_from_db``, which already
    returns a terminal ``DispatchOutcome`` from its own decision tree — no
    ``CompletionResult`` in the middle, so nothing here is parallel to
    ``_dispatch_outcome_from_completion`` below. See that function's
    docstring (issue #510) for why this isn't unified further.

    Everything that differs between force-import and local-import lives on
    ``lane`` (issue #1278); everything else (resolve config, verify the
    retained action copy is the job's own deterministic one, the
    cancellation/session pairing, the two-shaped dispatch call) is
    identical and lives here once.
    """
    source = lane.source_fn(job.payload)
    action_prefix = lane.action_copy_prefix()

    from lib.dispatch import dispatch_import_from_db
    force_dispatch = force_dispatch_fn or dispatch_import_from_db

    from lib.config import read_runtime_config
    from lib.preview_snapshot import force_action_copy_path

    # F9 (issue #1176 PR3 review round): this helper serves BOTH lanes, but
    # its two operator-facing strings below were hardcoded to "force" even
    # for a local-import job — one raises a ValueError whose text surfaces
    # verbatim in the Recents crash audit
    # ("Executor crashed: ValueError: ...") via
    # _terminalize_non_automation_failure, the other is a requeue reason.
    lane_label = lane.lane_label

    if (cancellation_token is None) != (owner_session_identity is None):
        raise ValueError(
            f"{lane_label} job cancellation and pinned session must be paired"
        )
    checkpoint(cancellation_token)
    runtime_config = (
        force_runtime_config
        or getattr(ctx, "cfg", None)
        or read_runtime_config()
    )
    action_path = _force_action_path(job)
    expected_action_path = force_action_copy_path(
        runtime_config, job.id, prefix=action_prefix,
    )
    if (
        action_path is None
        or action_path != expected_action_path
        or not os.path.isdir(action_path)
    ):
        return _requeue_import_job_to_preview(
            db,
            import_job_id=job.id,
            reason=f"{lane_label} action copy unavailable; preview must rebuild it",
        )
    resolved_distance_threshold = lane.distance_threshold_fn(runtime_config)
    if cancellation_token is None:
        return force_dispatch(
            db,
            request_id=request_id,
            failed_path=action_path,
            source_reference_path=source.source_reference_path,
            source_username=source.source_username,
            source_dirs=source.source_dirs,
            import_job_id=job.id,
            download_log_id=source.download_log_id,
            cfg=runtime_config,
            distance_threshold=resolved_distance_threshold,
            scenario=lane.scenario,
        )
    assert owner_session_identity is not None
    return force_dispatch(
        db,
        request_id=request_id,
        failed_path=action_path,
        source_reference_path=source.source_reference_path,
        source_username=source.source_username,
        source_dirs=source.source_dirs,
        import_job_id=job.id,
        download_log_id=source.download_log_id,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
        cfg=runtime_config,
        distance_threshold=resolved_distance_threshold,
        scenario=lane.scenario,
    )


def _execute_force_kind(
    db: PipelineDB,
    job: ImportJob,
    *,
    request_id: int,
    ctx: object,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
    force_dispatch_fn: Callable[..., DispatchOutcome] | None,
    force_runtime_config: CratediggerConfig | None,
) -> DispatchOutcome:
    """Run one force-import job through the shared action-copy lane."""
    del execution_lease
    return _execute_action_copy_dispatch(
        db,
        job,
        lane=_FORCE_ACTION_COPY_LANE,
        request_id=request_id,
        ctx=ctx,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
        force_dispatch_fn=force_dispatch_fn,
        force_runtime_config=force_runtime_config,
    )


def _execute_local_kind(
    db: PipelineDB,
    job: ImportJob,
    *,
    request_id: int,
    ctx: object,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
    force_dispatch_fn: Callable[..., DispatchOutcome] | None,
    force_runtime_config: CratediggerConfig | None,
) -> DispatchOutcome:
    """Run one local-import job through the shared action-copy lane."""
    del execution_lease
    return _execute_action_copy_dispatch(
        db,
        job,
        lane=_LOCAL_IMPORT_ACTION_COPY_LANE,
        request_id=request_id,
        ctx=ctx,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
        force_dispatch_fn=force_dispatch_fn,
        force_runtime_config=force_runtime_config,
    )


def _execute_automation_kind(
    db: PipelineDB,
    job: ImportJob,
    *,
    request_id: int,
    ctx: object,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
    force_dispatch_fn: Callable[..., DispatchOutcome] | None,
    force_runtime_config: CratediggerConfig | None,
) -> DispatchOutcome:
    """Run one automation job under its own exact execution authority."""
    del request_id, force_dispatch_fn, force_runtime_config
    return execute_automation_import_job(
        db,
        job,
        ctx=ctx,
        execution_lease=execution_lease,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
    )


def _execute_youtube_kind(
    db: PipelineDB,
    job: ImportJob,
    *,
    request_id: int,
    ctx: object,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
    force_dispatch_fn: Callable[..., DispatchOutcome] | None,
    force_runtime_config: CratediggerConfig | None,
) -> DispatchOutcome:
    """Run one YouTube-rescue job; this lane pins no session at all."""
    del request_id, execution_lease, cancellation_token
    del owner_session_identity, force_dispatch_fn, force_runtime_config
    return execute_youtube_import_job(db, job, ctx=ctx)


def _execute_unsupported_kind(
    db: PipelineDB,
    job: ImportJob,
    *,
    request_id: int,
    ctx: object,
    execution_lease: ExecutionLeaseSnapshot | None,
    cancellation_token: CancellationToken | None,
    owner_session_identity: OwnerSessionIdentity | None,
    force_dispatch_fn: Callable[..., DispatchOutcome] | None,
    force_runtime_config: CratediggerConfig | None,
) -> DispatchOutcome:
    """Refuse a job type no adapter claims, exactly as the old fall-through did."""
    del db, request_id, ctx, execution_lease, cancellation_token
    del owner_session_identity, force_dispatch_fn, force_runtime_config
    return DispatchOutcome(
        success=False,
        message=f"Unsupported import job type: {job.job_type}",
    )


def execute_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
    force_dispatch_fn: Callable[..., DispatchOutcome] | None = None,
    force_runtime_config: CratediggerConfig | None = None,
) -> DispatchOutcome:
    """Execute one claimed import job without mutating job status."""
    if job.request_id is None:
        return DispatchOutcome(
            success=False,
            message="Import job has no request_id",
        )

    return _kind_for(job.job_type).execute_fn(
        db,
        job,
        request_id=job.request_id,
        ctx=ctx,
        execution_lease=execution_lease,
        cancellation_token=cancellation_token,
        owner_session_identity=owner_session_identity,
        force_dispatch_fn=force_dispatch_fn,
        force_runtime_config=force_runtime_config,
    )


def _build_runtime_context(
    db: PipelineDB,
    *,
    borrow_session: bool = False,
):
    """Build the minimal CratediggerContext needed by download processing.

    ``WorkerCollaborators``, not ``CycleCollaborators``: the serial
    importer holds no slskd client, so no destructive slskd path is
    reachable from this context and the ownership writer that gates one
    is structurally absent rather than defaulted away (#1313). Giving
    this worker an slskd client means swapping the collaborator type,
    which cannot be constructed without naming an ownership writer.
    """
    from album_source import DatabaseSource
    from lib.config import read_runtime_config
    from lib.context import CratediggerContext, WorkerCollaborators
    from web.api_bases import mb_ws2_base

    cfg = read_runtime_config()
    source = DatabaseSource(
        db.dsn,
        musicbrainz_ws2_base=mb_ws2_base(cfg.musicbrainz_api_base),
        discogs_api_base=cfg.discogs_api_base,
        borrowed_db=db if borrow_session else None,
    )
    return CratediggerContext(
        collaborators=WorkerCollaborators(cfg=cfg, pipeline_db_source=source),
    )


def _dispatch_outcome_from_completion(
    result: CompletionResult,
    *,
    deferred_message: str,
    completed_message: str,
    failed_message: str,
    fallback_terminal_outcome: PendingImportTerminalOutcome | None = None,
) -> DispatchOutcome:
    """Map the completion-processing tag to the queue's DispatchOutcome.

    Both ``execute_automation_import_job`` and ``execute_youtube_import_job``
    drive the same completion-processing protocol (issue #474) and need to
    report the same four outcomes back to the importer queue; this is the
    single conversion so the two callers don't duplicate the match.

    FORCE import jobs deliberately do NOT route through this mapper (issue
    #510 considered and rejected folding all three job types in here): they
    never produce a ``CompletionResult`` at all. ``execute_import_job`` sends
    them straight to
    ``dispatch_import_from_db`` -> ``dispatch_import_core`` — a
    structurally different decision tree (manifest guard, evidence gate,
    quality gate) that already returns ``DispatchOutcome`` directly from
    many branches. Routing them through here would mean wrapping that
    already-terminal ``DispatchOutcome`` in a synthetic completion tag
    just to unwrap it again a line later — ceremony, not dedup. The
    mapper that DOES unify all three job types is one layer up:
    ``process_claimed_job`` (+ ``_job_result``) converts any
    ``DispatchOutcome`` — regardless of which job-type executor produced
    it — into the ``ImportJob``'s terminal queue status.

    Policy (issue #859): a deferred attempt REMAINS a terminal failed job
    carrying the honest ``result.detail`` (e.g. "incomplete_or_unsafe_canonical")
    appended to ``deferred_message`` — never a generic message that hides
    the diagnostic. Retry ownership stays with the poll cycle, which
    re-enqueues on the next cycle; requeueing inside the serial importer
    drain here would risk a hot loop.
    """
    if isinstance(result, CompletionDeferred):
        message = deferred_message
        if result.detail:
            message = f"{deferred_message}: {result.detail}"
        return DispatchOutcome(
            success=False,
            message=message,
            deferred=True,
            terminal_outcome=fallback_terminal_outcome,
        )
    if isinstance(result, CompletionDispatched):
        return result.outcome
    if isinstance(result, Completed):
        return DispatchOutcome(
            success=True,
            message=completed_message,
            terminal_outcome=(
                result.terminal_outcome or fallback_terminal_outcome
            ),
        )
    match result:
        case CompletionFailed():
            return DispatchOutcome(
                success=False,
                message=(
                    f"{failed_message}: {result.reason}"
                    if result.reason else failed_message
                ),
                terminal_outcome=(
                    result.terminal_outcome or fallback_terminal_outcome
                ),
            )
    assert_never(result)


def execute_automation_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    process_album_fn: ProcessAlbumFn | None = None,
    completed_processing_fn: Callable[..., CompletionResult] | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
) -> DispatchOutcome:
    """Run completed-download processing from an automation queue job."""
    from lib.download import (
        _local_completion_terminal_outcome,
        _run_completed_processing,
    )
    from lib.download_reconstruction import reconstruct_grab_list_entry

    request_id = job.request_id
    if request_id is None:
        return DispatchOutcome(False, "Automation import job has no request_id")

    if (
        execution_lease is None
        or cancellation_token is None
        or owner_session_identity is None
    ):
        raise ValueError(
            "automation execution requires lease, cancellation token, "
            "and pinned owner session"
        )
    cancellation_token.raise_if_cancelled()
    row = db.get_request(request_id)
    if not row:
        return DispatchOutcome(False, f"Album request {request_id} not found")

    raw_state = row.get("active_download_state")
    if not raw_state:
        return DispatchOutcome(
            False,
            f"Album request {request_id} has no active_download_state",
        )
    state = ActiveDownloadState.from_raw(raw_state)
    entry = reconstruct_grab_list_entry(row, state)
    created_ctx = ctx is None
    runtime_ctx = ctx or _build_runtime_context(db, borrow_session=True)
    try:
        process_completed = completed_processing_fn or _run_completed_processing
        result = process_completed(
            entry,
            state,
            runtime_ctx,
            import_job_id=job.id,
            process_album_fn=process_album_fn,
            cancellation_token=cancellation_token,
            owner_proof=ExecutionOwnerProof(
                execution_lease=execution_lease,
                owner_session_identity=owner_session_identity,
            ),
        )
    finally:
        if created_ctx:
            runtime_ctx.pipeline_db_source.close()
    fallback_terminal: PendingImportTerminalOutcome | None = None
    if isinstance(result, Completed) and result.terminal_outcome is None:
        fallback_terminal = _local_completion_terminal_outcome(
            entry,
            state,
            request_id=request_id,
            import_job_id=job.id,
            transition=transitions.RequestTransition.to_imported(
                from_status="processing",
            ),
            outcome="success",
            detail="Local automation import processing completed",
        )
    elif isinstance(result, (CompletionDeferred, CompletionFailed)):
        detail = (
            result.detail
            if isinstance(result, CompletionDeferred)
            else result.reason
        )
        fallback_terminal = _local_completion_terminal_outcome(
            entry,
            state,
            request_id=request_id,
            import_job_id=job.id,
            transition=transitions.RequestTransition.to_wanted(
                from_status="processing",
                attempt_type="download",
            ),
            outcome="failed",
            detail=detail or "local automation processing failed",
            error_message=detail or "local automation processing failed",
        )
    elif (
        isinstance(result, CompletionDispatched)
        and result.outcome.terminal_outcome is None
        and result.outcome.code != DISPATCH_CODE_REQUEUED_FOR_PREVIEW
    ):
        fallback_terminal = _local_completion_terminal_outcome(
            entry,
            state,
            request_id=request_id,
            import_job_id=job.id,
            transition=(
                transitions.RequestTransition.to_imported(
                    from_status="processing",
                )
                if result.outcome.success
                else transitions.RequestTransition.to_wanted(
                    from_status="processing",
                    attempt_type="validation",
                )
            ),
            outcome="success" if result.outcome.success else "failed",
            detail=result.outcome.message,
            error_message=(
                None if result.outcome.success else result.outcome.message
            ),
        )
        result = CompletionDispatched(outcome=replace(
            result.outcome,
            terminal_outcome=fallback_terminal,
        ))
    return _dispatch_outcome_from_completion(
        result,
        deferred_message=(
            "Automation import was deferred or requires manual recovery"
        ),
        completed_message="Automation import processing completed",
        failed_message="Automation import processing failed",
        fallback_terminal_outcome=fallback_terminal,
    )


def execute_youtube_import_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    process_album_fn: ProcessAlbumFn | None = None,
) -> DispatchOutcome:
    """Run completed-staging processing for a YouTube-rescue import job.

    Mirrors ``execute_automation_import_job`` structurally but sources the
    staged path from the typed payload decoded at ``ImportJob.from_row`` rather
    than from ``album_requests.active_download_state``.

    KTD1: this path never reads from nor writes to ``active_download_state``.
    The YT staged dir lives under ``/Incoming/auto-import`` already (the
    U6 worker stages it there directly), so the downstream pipeline
    observes a ready local staging path with no slskd-resume state
    attached.

    R17: terminal status flips run through
    ``transitions.finalize_request → mark_imported_with_rescue`` (the
    single source-agnostic write site), so YT rescues populate
    ``rescued_at`` + ``prior_unfindable_category`` atomically when the
    request had a prior ``unfindable_category``.

    No cooldown side effects: the slskd cooldown machinery is keyed on
    peer usernames; YT has no peers. We never call ``denylist_user`` /
    ``update_user_failure_count`` / ``check_and_apply_cooldown``. The
    synthetic ``ActiveDownloadState`` we build uses blank usernames for
    the staged audio manifest, so the rejection paths inside
    ``_handle_rejected_result`` find no peers to denylist.
    """
    from lib.config import read_runtime_config
    from lib.download_processing import process_completed_album
    from lib.download_reconstruction import reconstruct_grab_list_entry
    from lib.processing_paths import protected_staging_roots

    request_id = job.request_id
    if request_id is None:
        return DispatchOutcome(False, "YouTube import job has no request_id")

    if not isinstance(job.payload, YoutubeImportPayload):
        raise TypeError("youtube_import payload type mismatch")
    payload = job.payload

    row = db.get_request(request_id)
    if not row:
        return DispatchOutcome(False, f"Album request {request_id} not found")
    status = str(row.get("status") or "")
    if status not in YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES:
        # Issue #1122 item 4 (widened, review round 2): payload.staged_path
        # is a direct child of the shared, externally provisioned
        # auto-import staging root (the U6 worker's build_service stages
        # every YT rescue there — see
        # lib.youtube_ingest_service.YoutubeIngestService.staging_root and
        # scripts/youtube_ingest_worker.py). Without this guard,
        # _run_post_commit_cleanup's empty-parent prune
        # (lib.dispatch.helpers._cleanup_staged_dir) could rmdir that
        # shared root the moment this album's staged folder was its only
        # child. Uses the same protected_staging_roots(...) derivation as
        # every other _cleanup_staged_dir producer (lib/dispatch/core.py,
        # lib/dispatch/outcome_actions.py, lib/download_rejection.py) and
        # its harness/import_one.py twin — this producer only needs the
        # auto-import root, but passing the shared set is the one obvious
        # way rather than a narrower hand-built form. ctx is always None
        # from the production caller at this point (the runtime context
        # below is only built after this early return), so the config is
        # resolved directly — mirroring the getattr(ctx, "cfg", None) or
        # read_runtime_config() pattern used by the FORCE branch above.
        # Ownership-drift residual (root deleted -> recreated
        # cratedigger-owned by the next rescue): see
        # lib.processing_paths.protected_staging_roots's docstring.
        cfg = getattr(ctx, "cfg", None) or read_runtime_config()
        return DispatchOutcome(
            False,
            (
                f"Album request {request_id} is status {status!r}; "
                "YouTube import requires wanted/unsearchable"
            ),
            post_commit_cleanup=PostCommitCleanup(
                staged_path=payload.staged_path,
                staged_path_protected_parents=protected_staging_roots(
                    processing_dir=cfg.processing_dir,
                    beets_staging_dir=cfg.beets_staging_dir,
                ),
            ),
        )

    staged_files = _youtube_active_download_files(payload.staged_path)

    # Synthetic ActiveDownloadState — used ONLY to feed
    # reconstruct_grab_list_entry. Files are a manifest bridge for the
    # already-staged yt-dlp audio; current_path = the payload's staged
    # path. This struct is never persisted: KTD1 keeps
    # active_download_state untouched on the row.
    state = ActiveDownloadState(
        filetype=row.get("target_format") or "opus",
        enqueued_at="",
        last_progress_at="",
        files=staged_files,
        current_path=payload.staged_path,
    )
    entry = reconstruct_grab_list_entry(row, state)
    entry.import_folder = payload.staged_path

    created_ctx = ctx is None
    runtime_ctx = ctx or _build_runtime_context(db)
    try:
        process_completed = process_album_fn or process_completed_album
        result = process_completed(
            entry,
            runtime_ctx,
            import_job_id=job.id,
        )
    finally:
        if created_ctx:
            runtime_ctx.pipeline_db_source.close()

    return _dispatch_outcome_from_completion(
        result,
        deferred_message=(
            "YouTube import was deferred or requires manual recovery"
        ),
        completed_message="YouTube import processing completed",
        failed_message="YouTube import processing failed",
    )


def _youtube_active_download_files(staged_path: str) -> list[ActiveDownloadFileState]:
    """Build the manifest bridge for a YT-staged album directory."""
    out: list[ActiveDownloadFileState] = []
    for rel_path in audio_relative_paths(staged_path):
        full_path = os.path.join(staged_path, rel_path)
        try:
            size = os.path.getsize(full_path)
        except OSError:
            size = 0
        out.append(ActiveDownloadFileState(
            username="",
            filename=rel_path,
            file_dir=os.path.dirname(rel_path),
            size=size,
        ))
    return out


def _cleanup_committed_wrong_match_rejection(
    db: PipelineDB,
    job: ImportJob,
    download_log_id: int,
    outcome: DispatchOutcome,
    *,
    cleanup_wrong_match_fn: Callable[..., object] | None = None,
) -> None:
    """Run Wrong Matches convergence only after the terminal bundle commits."""
    from lib.wrong_match_policy import (
        rejection_scenario_is_delete_eligible,
        rejection_scenario_is_wrong_match_candidate,
    )

    scenario = outcome.post_commit_wrong_match_scenario
    if not rejection_scenario_is_wrong_match_candidate(scenario):
        # Bad rips and every other folder/audio-integrity or quality-only
        # reject were never quarantined (issue #1077, D3): there is no
        # worklist row to link evidence to or hand to the reducer.
        return
    try:
        evidence_id = db.get_import_job_candidate_evidence_id(job.id)
        if evidence_id is not None:
            db.set_download_log_candidate_evidence(
                download_log_id,
                evidence_id,
                direct_attribution=True,
            )
        if not rejection_scenario_is_delete_eligible(scenario):
            # World failures with a reviewable folder, and every unknown or
            # novel scenario string, are kept + banned + visible (issue
            # #1077, D1/D4/D6): the evaluate-and-possibly-delete reducer
            # never even looks at them.
            return
        if cleanup_wrong_match_fn is None:
            from lib.wrong_match_cleanup_service import cleanup_wrong_match

            cleanup_wrong_match_fn = cleanup_wrong_match

        cleanup_wrong_match_fn(
            db,
            download_log_id,
            ignore_import_job_id=job.id,
        )
    except Exception:
        logger.exception(
            "WRONG-MATCH CLEANUP FAILED after terminal commit: download_log_id=%s",
            download_log_id,
        )


@runtime_checkable
class _AutomationOwnerDB(Protocol):
    def get_import_job(self, job_id: int) -> ImportJob | None: ...

    def get_request(
        self,
        request_id: int,
    ) -> Mapping[str, object] | None: ...


@runtime_checkable
class _AutomationStageDB(_AutomationOwnerDB, Protocol):
    def _pin_owner_session(
        self,
        cancellation_token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...

    def advisory_lock(
        self,
        namespace: int,
        key: int,
    ) -> AbstractContextManager[bool]: ...

    def claim_automation_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None: ...

    def close(self) -> None: ...


@runtime_checkable
class _ForceStageDB(Protocol):
    """Claim-stage DB surface shared by force-import and local-import
    (issue #1176 PR3): both are request-scoped, IMPORT-pinned claims that
    differ only in which method names the job type."""

    def _pin_owner_session(
        self,
        cancellation_token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...

    def advisory_lock(
        self,
        namespace: int,
        key: int,
    ) -> AbstractContextManager[bool]: ...

    def claim_force_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_local_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def close(self) -> None: ...


def _claim_force_import(
    stage_db: _ForceStageDB,
    job_id: int,
    *,
    request_id: int,
    worker_id: str | None,
) -> ImportJob | None:
    return stage_db.claim_force_import_job_under_lock(
        job_id, request_id=request_id, worker_id=worker_id,
    )


def _claim_local_import(
    stage_db: _ForceStageDB,
    job_id: int,
    *,
    request_id: int,
    worker_id: str | None,
) -> ImportJob | None:
    return stage_db.claim_local_import_job_under_lock(
        job_id, request_id=request_id, worker_id=worker_id,
    )


def _self_heal_automation_world_failure(
    db: PipelineDB,
    job: ImportJob,
    *,
    execution_lease: ExecutionLeaseSnapshot,
    cancellation_token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
    reason: str,
    result: dict[str, object],
    debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
) -> ImportJob | None:
    """Surface one world failure as audit evidence and re-open the search.

    The automation importer never parks a request for human adjudication. A
    fault that leaves the world in an unexpected state is surfaced as a
    ``download_log`` row — so it reads in Recents — and the request goes
    straight back to ``wanted``: the request is the source of truth and the
    next cycle rebuilds everything derived from it, including the
    "did Beets already mutate the library?" ambiguity (album present ->
    upgrade/no-op, absent -> re-import). Parking instead left
    ``status='processing'`` behind an inactive job, which ``get_wanted()``
    never selects again — the album silently stopped being acquired, forever,
    with nothing telling the operator the world needed fixing.

    The ``processing -> wanted`` edge retains retry counters and records an
    attempt, so a permanently broken world backs off instead of hot-looping.

    ``job`` must be the freshly re-read owner row: its launch fence, preview
    stage and captured completion receipt are the exact authority the terminal
    compare-and-set compares against.

    Issue #1089 (B1): this is THE composition the RCA's own incident lived
    in — a launched Beets child dying while THIS process stays alive (a
    crash, an ambiguous acknowledgement, a missing completion receipt) is
    exactly as exposed to a committed-but-unmoved catalog row as the
    restart-based abandoned-owner sweep already covered
    (``lib.automation_recovery_debris``). Before cleanup — while an explicit
    checkpoint still proves this exact owner session live — the same
    ``debris_removal_fn`` checks Beets for an album provably this job's own
    crash debris and removes only its catalog row when proven; either
    precondition unmet is surfaced, never removed, exactly like the
    restart-side check.

    Raises :class:`AutomationOwnerFailStop` when this execution cannot author
    an owner-atomic terminal write. The row stays ``running`` under its
    persisted lease, but the exception ends this daemon so systemd restart and
    lease-proven recovery can converge it automatically. Never return to the
    daemon loop with that same lease still live, and never fabricate a write
    the owner authority refuses.
    """
    from lib.download import _local_completion_terminal_outcome
    from lib.download_reconstruction import reconstruct_grab_list_entry

    request_id = job.request_id
    if request_id is None:
        raise AutomationOwnerFailStop(
            f"automation job {job.id} lost its request before self-heal"
        )
    detail = f"{_WORLD_FAILURE_AUDIT_PREFIX}: {reason}"
    try:
        row = db.get_request(request_id)
        raw_state = None if row is None else row.get("active_download_state")
        if row is None or raw_state is None:
            raise AutomationOwnerFailStop(
                f"automation job {job.id} has no owned download state "
                f"to self-heal ({reason})"
            )
        state = ActiveDownloadState.from_raw(raw_state)
        # Re-verify this exact owner session immediately before the
        # irreversible Beets mutation below (issue #1089 review n8) — the
        # same live-session proof ``_complete_automation_processing_cleanup``
        # itself re-checks via its own internal checkpoint, just pulled
        # forward so the debris removal is never unfenced.
        checkpoint_automation_owner(
            db,
            import_job_id=job.id,
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )
        debris_report = debris_removal_fn(
            launch_release_id=job.beets_launch_release_id,
            launch_source_path=job.beets_launch_source_path,
        )
        if debris_report.outcome != "no_launch":
            detail = f"{detail}; recovery debris {debris_report.outcome}"
            if debris_report.album_id is not None:
                detail = f"{detail} (beets album {debris_report.album_id})"
            if debris_report.detail:
                detail = f"{detail}: {debris_report.detail}"
        # Issue #1089 review MINOR-3: built ONCE, after the debris-outcome
        # augmentation above, mirroring the restart-sweep sibling
        # (``_fail_abandoned_automation_owner``) exactly — both
        # ``TerminalDownloadAudit.beets_detail`` (via ``detail=``) and
        # ``.error_message`` (via ``error_message=``) get the SAME fully
        # composed string. Building this before the debris check and then
        # patching only ``error_message`` afterward (the prior shape) left
        # ``beets_detail`` permanently stale on any world that adds to
        # ``detail`` post-construction.
        pending = _local_completion_terminal_outcome(
            reconstruct_grab_list_entry(row, state),
            state,
            request_id=request_id,
            import_job_id=job.id,
            transition=transitions.RequestTransition.to_wanted(
                from_status="processing",
                attempt_type="validation",
            ),
            outcome="failed",
            detail=detail,
            error_message=detail,
        )
        # Cleanup runs first and while the job is still the running owner: its
        # checkpoint heartbeats a 'running' row, and the terminal bundle
        # consumes the receipt it produces.
        cleanup_receipt = _complete_automation_processing_cleanup(
            db,
            job,
            DispatchOutcome(success=False, message=detail),
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )
        completion_receipt = automation_completion_receipt(job)
        terminal_result = dict(result)
        terminal_result[AUTOMATION_WORLD_FAILURE_RESULT_KEY] = reason
        terminal_result["recovery_debris_removal"] = msgspec.to_builtins(
            debris_report,
        )
        terminal = db.persist_import_terminal_outcome(
            replace(
                pending,
                automation=AutomationTerminalAuthority(
                    expected_job_status="running",
                    expected_preview_status=job.preview_status,
                    expected_execution_lease=execution_lease,
                    cleanup_receipt=cleanup_receipt,
                    completion_receipt=completion_receipt,
                ),
            ).with_job(ImportJobTerminal(
                status="failed",
                error=detail,
                result=terminal_result,
                message=detail,
            ))
        )
    except (ExecutionCancelled, OwnerSessionLost):
        # Fail-stop: this execution's authority is gone, so no further write on
        # this session is trustworthy — including this self-heal, which would
        # raise again. Startup recovery owns the row once its lease is proven
        # dead.
        raise
    except AutomationOwnerFailStop:
        raise
    except Exception as exc:
        logger.exception(
            "Self-heal terminal write failed for import job %s (%s); "
            "fail-stopping this worker for lease-proven recovery",
            job.id,
            reason,
        )
        raise AutomationOwnerFailStop(
            f"automation job {job.id} self-heal failed: {reason}"
        ) from exc
    logger.warning(
        "Import job %s returned request %s to the search pool after a world "
        "failure: %s",
        job.id,
        request_id,
        reason,
    )
    return terminal.job


def _automation_claim_is_current(
    db: _AutomationOwnerDB,
    job: ImportJob,
    execution_lease: ExecutionLeaseSnapshot,
) -> bool:
    if job.request_id is None:
        return False
    current = db.get_import_job(job.id)
    try:
        request = db.get_request(job.request_id)
    except RuntimeError:
        # The joined processing-owner projection deliberately rejects a
        # dangling/mismatched owner pointer. At this boundary that is stale
        # authority, not permission to continue or an importer crash.
        return False
    return bool(
        current is not None
        and request is not None
        and current.id == job.id
        and current.request_id == job.request_id
        and current.job_type == IMPORT_JOB_AUTOMATION
        and current.status == "running"
        and current.preview_status == "evidence_ready"
        and current.beets_launch_authorized_at is None
        and execution_lease_from_job(current) == execution_lease
        and request.get("status") == "processing"
        and request.get("active_automation_import_job_id") == job.id
        and request.get("active_download_state") is not None
    )


def _process_automation_claim(
    candidate: ImportJob,
    *,
    dsn: str,
    worker_id: str,
    execution_lease: ExecutionLeaseSnapshot,
    ctx: object | None,
    stage_db_factory: Callable[[str], object],
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
    claim_callback: Callable[[], None] | None = None,
) -> ImportJob | None:
    """Claim and run one automation job on the pinned IMPORT session."""
    if candidate.request_id is None:
        return None
    stage_db = stage_db_factory(dsn)
    if not isinstance(stage_db, _AutomationStageDB):
        raise TypeError("automation stage DB is missing its owner-session protocol")
    token = CancellationToken()
    try:
        # Pin first. Acquiring IMPORT before pinning could reconnect between
        # scopes and leave the pinned backend without the authority lock.
        with stage_db._pin_owner_session(token) as owner_session_identity:
            token.raise_if_cancelled()
            with stage_db.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                candidate.request_id,
            ) as acquired:
                token.raise_if_cancelled()
                if not acquired:
                    # The queue connection only selected this candidate.
                    # Preserve queued claimability for the next poll.
                    return None
                job = stage_db.claim_automation_import_job_under_lock(
                    candidate.id,
                    request_id=candidate.request_id,
                    worker_id=worker_id,
                    execution_lease=execution_lease,
                )
                if job is None:
                    return None
                logger.info(
                    "Claimed import job %s (%s)",
                    job.id,
                    job.job_type,
                )
                if claim_callback is not None:
                    claim_callback()
                if not _automation_claim_is_current(
                    stage_db,
                    job,
                    execution_lease,
                ):
                    return None
                return process_claimed_job(
                    stage_db,  # pyright: ignore[reportArgumentType]
                    job,
                    ctx=ctx,
                    execute_fn=execute_fn,
                    execution_lease=execution_lease,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                )
    finally:
        stage_db.close()


def _process_force_claim(
    candidate: ImportJob,
    *,
    dsn: str,
    worker_id: str,
    ctx: object | None,
    stage_db_factory: Callable[[str], object],
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
    claim_callback: Callable[[], None] | None = None,
    claim_fn: Callable[..., ImportJob | None],
) -> ImportJob | None:
    """Claim and run force/local-import effects on one pinned IMPORT session.

    ``claim_fn`` (issue #1176 PR3) is the ONE difference between the two
    claims: ``_claim_force_import`` or ``_claim_local_import``. Each lane's
    claim route names its own (issue #1278), so there is no default to fall
    back to — a new lane must state which claim method owns its job type.
    """
    if candidate.request_id is None:
        return None
    stage_db = stage_db_factory(dsn)
    if not isinstance(stage_db, _ForceStageDB):
        raise TypeError("force stage DB is missing its owner-session protocol")
    token = CancellationToken()
    try:
        with stage_db._pin_owner_session(token) as owner_session_identity:
            token.raise_if_cancelled()
            with stage_db.advisory_lock(
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
                logger.info("Claimed import job %s (%s)", job.id, job.job_type)
                return process_claimed_job(
                    stage_db,  # pyright: ignore[reportArgumentType]
                    job,
                    ctx=ctx,
                    execute_fn=execute_fn,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                )
    finally:
        stage_db.close()


def _terminalize_non_automation_failure(
    db: PipelineDB,
    job: ImportJob,
    *,
    error: str,
    message: str,
    result: dict[str, object],
) -> ImportJob | None:
    """Persist one failed force/local-import/YouTube attempt and its Recents audit atomically."""
    terminal = db.persist_import_terminal_outcome(
        non_automation_failure_terminal_outcome(
            job,
            error=error,
            message=message,
            result=result,
        )
    )
    return terminal.job


def process_claimed_job(
    db: PipelineDB,
    job: ImportJob,
    *,
    ctx: Any = None,
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_session_identity: OwnerSessionIdentity | None = None,
    debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
) -> ImportJob | None:
    """Execute a claimed job and persist its terminal queue status.

    This is the single queue-outcome mapper all four job types (automation,
    force, local-import, youtube) route through: whichever job-type executor
    produced ``outcome``, the success/requeue/failure -> terminal
    ``ImportJob`` status conversion below is one shared path (see
    ``_dispatch_outcome_from_completion``'s docstring for why the
    completion-result -> DispatchOutcome conversion is instead scoped to
    just automation + youtube, issue #510).

    ``is_force`` (issue #1176 PR3: widened to also cover ``local_import``)
    gates pinned-session/cancellation handling for every job type claimed
    via ``_process_force_claim`` — which now includes local-import, since
    it claims through the SAME pinned-IMPORT-session helper as force (see
    ``_claim_route_local_import``). YouTube stays outside this flag: it
    claims through the plain ``db.claim_import_job_candidate`` path with no
    pinned session at all, exactly as before. Narrowing this check back to
    force-only would silently drop local-import's pinned
    ``cancellation_token`` / ``owner_session_identity`` on every real
    dispatch — the job would still run, just without graceful-shutdown
    cancellation support. Both flags now read one answer from the job's own
    kind adapter (issue #1278) rather than re-deriving it from
    ``job.job_type`` here.
    """
    kind = _kind_for(job.job_type)
    is_automation = kind.authority == _AUTHORITY_AUTOMATION
    is_force = kind.authority == _AUTHORITY_PINNED_PAIR
    if is_automation and execution_lease is None:
        raise ValueError(
            "automation job processing requires exact execution authority"
        )
    if is_automation and (
        cancellation_token is None
        or owner_session_identity is None
    ):
        raise ValueError(
            f"{job.job_type} processing requires pinned session authority"
        )
    if is_force and (
        (cancellation_token is None) != (owner_session_identity is None)
    ):
        raise ValueError(
            "force job cancellation and pinned session must be paired"
        )
    try:
        if is_force and cancellation_token is not None:
            cancellation_token.raise_if_cancelled()
        if is_automation:
            outcome = execute_fn(
                db,
                job,
                ctx=ctx,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
        elif is_force and cancellation_token is not None:
            outcome = execute_fn(
                db,
                job,
                ctx=ctx,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
        else:
            outcome = execute_fn(db, job, ctx=ctx)
    except ExecutionCancelled:
        raise
    except Exception as exc:
        logger.exception("Import job %s crashed", job.id)
        if is_automation:
            current = db.get_import_job(job.id)
            current_lease = (
                execution_lease_from_job(current)
                if current is not None else None
            )
            if current_lease is None:
                return None
            assert current is not None
            assert cancellation_token is not None
            assert owner_session_identity is not None
            crash = f"{type(exc).__name__}: {exc}"
            if current.beets_launch_authorized_at is not None:
                return _self_heal_automation_world_failure(
                    db,
                    current,
                    execution_lease=current_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                    reason=crash,
                    result={"success": False, "message": crash},
                    debris_removal_fn=debris_removal_fn,
                )
            return db.requeue_import_job_for_preview(
                job.id,
                reason=crash,
                expected_execution_lease=current_lease,
            )
        # Force, local-import, and YouTube jobs do not own their request's
        # ``processing`` status. Their shared terminal command records the
        # failed attempt in Recents while leaving that request exactly as
        # the operator left it.
        failed = _terminalize_non_automation_failure(
            db,
            job,
            error=f"{type(exc).__name__}: {exc}",
            message=(
                f"Executor crashed: {type(exc).__name__}: {exc}"
            ),
            result={"success": False},
        )
        return _record_terminal_force_action_cleanup(db, job, failed)

    result = _job_result(outcome)
    if is_force and cancellation_token is not None:
        cancellation_token.raise_if_cancelled()
    if is_automation:
        assert execution_lease is not None
        assert cancellation_token is not None
        assert owner_session_identity is not None
        cancellation_token.raise_if_cancelled()
        if outcome.code == DISPATCH_CODE_REQUEUED_FOR_PREVIEW:
            return None
        current = db.get_import_job(job.id)
        current_lease = (
            execution_lease_from_job(current)
            if current is not None else None
        )
        if current is None or current_lease is None:
            return None
        if (
            current.beets_launch_authorized_at is None
            and outcome.terminal_outcome is None
            and (
                outcome.deferred
                or outcome.code in (
                    DISPATCH_CODE_REQUEUE_FAILED,
                    DISPATCH_CODE_REQUEUE_EXHAUSTED,
                )
            )
        ):
            if outcome.code in (
                DISPATCH_CODE_REQUEUE_FAILED,
                DISPATCH_CODE_REQUEUE_EXHAUSTED,
            ):
                return _self_heal_automation_world_failure(
                    db,
                    current,
                    execution_lease=current_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                    reason=outcome.message,
                    result=result,
                    debris_removal_fn=debris_removal_fn,
                )
            db.requeue_import_job_for_preview(
                job.id,
                reason=outcome.message,
                expected_execution_lease=current_lease,
            )
            return None
        if (
            current.beets_launch_authorized_at is not None
            and (
                outcome.code == "beets_acknowledgement_ambiguous"
                or outcome.terminal_outcome is None
            )
        ):
            # Beets was launched and did not acknowledge. Whether it mutated
            # the library is unknowable from here and needs no adjudication:
            # the next cycle re-reads the library from the request.
            return _self_heal_automation_world_failure(
                db,
                current,
                execution_lease=current_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
                reason=outcome.message,
                result=result,
                debris_removal_fn=debris_removal_fn,
            )
        if outcome.terminal_outcome is None:
            return _self_heal_automation_world_failure(
                db,
                current,
                execution_lease=current_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
                reason=(
                    "Automation processor returned no owner-atomic terminal "
                    "outcome"
                ),
                result=result,
                debris_removal_fn=debris_removal_fn,
            )
        # The owned-processor cleanup and the terminal persist are the last
        # stage of a Beets-mutating execution, and both raise: seven owner
        # RuntimeErrors plus ProcessingCleanupError/CleanupJournalConflict
        # from the cleanup, ImportJobTerminalConflict from the terminal CAS.
        # An escape here kills the whole importer process — the shared,
        # serial lane for every request — where the identical class of crash
        # escaping ``execute_fn`` self-heals the request. Route it the same way.
        try:
            cleanup_receipt = _complete_automation_processing_cleanup(
                db,
                current,
                outcome,
                execution_lease=current_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
            completion_receipt = automation_completion_receipt(current)
            if (
                current.beets_launch_authorized_at is not None
                and completion_receipt is None
            ):
                return _self_heal_automation_world_failure(
                    db,
                    current,
                    execution_lease=current_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                    reason=(
                        "Automation completion receipt is missing or invalid"
                    ),
                    result=result,
                    debris_removal_fn=debris_removal_fn,
                )
            pending = replace(
                outcome.terminal_outcome,
                automation=AutomationTerminalAuthority(
                    expected_job_status="running",
                    expected_preview_status=current.preview_status,
                    expected_execution_lease=current_lease,
                    cleanup_receipt=cleanup_receipt,
                    completion_receipt=completion_receipt,
                ),
            )
            terminal = db.persist_import_terminal_outcome(
                pending.with_job(ImportJobTerminal(
                    status="completed" if outcome.success else "failed",
                    error=None if outcome.success else outcome.message,
                    result=result,
                    message=outcome.message,
                ))
            )
        except (ExecutionCancelled, OwnerSessionLost):
            # Fail-stop: this execution's authority is gone, so no further
            # write on this session is trustworthy — including the self-heal
            # write below, which would raise again. Startup recovery owns
            # the row once its lease is proven dead.
            raise
        except Exception as exc:
            logger.exception(
                "Automation terminal stage failed for import job %s", job.id,
            )
            return _self_heal_automation_world_failure(
                db,
                current,
                execution_lease=current_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
                reason=f"{type(exc).__name__}: {exc}",
                result=result,
                debris_removal_fn=debris_removal_fn,
            )
        _cleanup_committed_wrong_match_rejection(
            db,
            job,
            terminal.download_log_id,
            outcome,
        )
        return terminal.job
    if outcome.success:
        if outcome.terminal_outcome is not None:
            terminal = db.persist_import_terminal_outcome(
                outcome.terminal_outcome.with_job(ImportJobTerminal(
                    status="completed",
                    result=result,
                    message=outcome.message,
                ))
            )
            terminal_job = terminal.job
            post_commit_cleanup = _run_post_commit_cleanup(outcome)
            if post_commit_cleanup is not None:
                merged = db.merge_import_job_result(
                    job.id,
                    {"post_commit_cleanup": post_commit_cleanup},
                )
                if merged is not None:
                    terminal_job = merged
            if kind.runs_committed_wrong_match_cleanup:
                _cleanup_committed_wrong_match_rejection(
                    db,
                    job,
                    terminal.download_log_id,
                    outcome,
                )
            dismissal = _dismiss_successful_force_import(db, job)
            if dismissal is not None:
                merged = db.merge_import_job_result(
                    job.id,
                    {"wrong_match_dismissal": dismissal},
                )
                if merged is not None:
                    terminal_job = merged
            return _record_terminal_force_action_cleanup(db, job, terminal_job)
        # A force/local-import/YouTube success without a bundle is recorded, not parked: the
        # job never owned the request's ``processing`` status, so the terminal
        # completed status is the whole surface this outcome needs.
        completed = db.mark_import_job_completed(
            job.id,
            result=result,
            message=outcome.message,
        )
        if completed is None:
            return None
        dismissal = _dismiss_successful_force_import(db, job)
        if dismissal is not None:
            completed = db.merge_import_job_result(
                job.id,
                {"wrong_match_dismissal": dismissal},
            ) or completed
        return _record_terminal_force_action_cleanup(db, job, completed)
    # U2: dispatch flipped this row back to the preview lane (or tried to).
    # We do NOT write a terminal failed status, do NOT bump retry counters,
    # and do NOT run the wrong-match cleanup decision. The dispatch-side
    # state change is already persisted; we just log and yield.
    if outcome.code == DISPATCH_CODE_REQUEUED_FOR_PREVIEW:
        logger.info(
            "Import job %s (request %s) requeued for preview: %s",
            job.id,
            job.request_id,
            outcome.message,
        )
        # The preview claim now owns this deterministic action path.  It may
        # publish its fresh copy before this importer frame returns, so an old
        # importer must never reclaim it after the durable requeue.
        return None
    if outcome.code == DISPATCH_CODE_REQUEUE_FAILED:
        # The requeue UPDATE itself failed (DB transient). Mark the job
        # terminally failed so it surfaces to ops rather than leaving it in
        # 'running' for startup recovery, which would just re-claim and hit
        # the same condition (REL-001). The operator can re-trigger the
        # import once the underlying DB issue is resolved.
        logger.error(
            "Import job %s (request %s) requeue to preview failed; "
            "marking job failed (operator must investigate): %s",
            job.id,
            job.request_id,
            outcome.message,
        )
        failed = _terminalize_non_automation_failure(
            db,
            job,
            error=outcome.message,
            message=outcome.message,
            result=result,
        )
        return _record_terminal_force_action_cleanup(db, job, failed)
    if outcome.code == DISPATCH_CODE_REQUEUE_EXHAUSTED:
        # A retry-budget bail does not adjudicate candidate quality, so keep
        # it out of wrong-match cleanup while recording the terminal audit.
        failed = _terminalize_non_automation_failure(
            db,
            job,
            error=outcome.message,
            message=outcome.message,
            result=result,
        )
        return _record_terminal_force_action_cleanup(db, job, failed)
    if outcome.terminal_outcome is not None:
        terminal = db.persist_import_terminal_outcome(
            outcome.terminal_outcome.with_job(ImportJobTerminal(
                status="failed",
                error=outcome.message,
                result=result,
                message=outcome.message,
            ))
        )
        terminal_job = terminal.job
        post_commit_cleanup = _run_post_commit_cleanup(outcome)
        if post_commit_cleanup is not None:
            merged = db.merge_import_job_result(
                job.id,
                {"post_commit_cleanup": post_commit_cleanup},
            )
            if merged is not None:
                terminal_job = merged
        cleanup = _cleanup_failed_force_import(db, job, outcome)
        if cleanup is not None:
            merged = db.merge_import_job_result(job.id, {"cleanup": cleanup})
            if merged is not None:
                terminal_job = merged
        if kind.runs_committed_wrong_match_cleanup:
            _cleanup_committed_wrong_match_rejection(
                db,
                job,
                terminal.download_log_id,
                outcome,
            )
        return _record_terminal_force_action_cleanup(db, job, terminal_job)
    # A bundle-less force/local-import/YouTube failure is recorded terminally rather than
    # parked. No request is stopped by it, and the producer diagnostic stays
    # intact for the Recents audit.
    failed = _terminalize_non_automation_failure(
        db,
        job,
        error=outcome.message,
        message=outcome.message,
        result=result,
    )
    if failed is None:
        return None
    terminal_job = failed
    post_commit_cleanup = _run_post_commit_cleanup(outcome)
    if post_commit_cleanup is not None:
        merged = db.merge_import_job_result(
            job.id,
            {"post_commit_cleanup": post_commit_cleanup},
        )
        if merged is not None:
            terminal_job = merged
    cleanup = _cleanup_failed_force_import(db, job, outcome)
    if cleanup is not None:
        terminal_job = db.merge_import_job_result(
            job.id,
            {"cleanup": cleanup},
        ) or terminal_job
    return _record_terminal_force_action_cleanup(db, job, terminal_job)


def _claim_route_automation(
    candidate: ImportJob,
    attempt: _ClaimAttempt,
) -> ImportJob | None:
    """Claim one automation candidate on its own pinned IMPORT session.

    Both misses below leave the claim unmarked, so ``run_once`` moves on to
    the next candidate exactly as its old ``continue`` arms did: automation
    stays invisible without a complete invocation lease, and every
    pinned-session route needs a DSN of its own to open a stage connection.
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
        ctx=attempt.ctx,
        stage_db_factory=attempt.stage_db_factory,
        execute_fn=attempt.execute_fn,
        claim_callback=attempt.claim_state.mark,
    )


def _claim_request_scoped_pinned(
    candidate: ImportJob,
    attempt: _ClaimAttempt,
    *,
    claim_fn: Callable[..., ImportJob | None],
) -> ImportJob | None:
    """The force/local-import claim, differing only in ``claim_fn``."""
    dsn = attempt.dsn()
    if dsn is None:
        return None
    return _process_force_claim(
        candidate,
        dsn=dsn,
        worker_id=attempt.worker_id,
        ctx=attempt.ctx,
        stage_db_factory=attempt.stage_db_factory,
        execute_fn=attempt.execute_fn,
        claim_callback=attempt.claim_state.mark,
        claim_fn=claim_fn,
    )


def _claim_route_force_import(
    candidate: ImportJob,
    attempt: _ClaimAttempt,
) -> ImportJob | None:
    return _claim_request_scoped_pinned(
        candidate, attempt, claim_fn=_claim_force_import,
    )


def _claim_route_local_import(
    candidate: ImportJob,
    attempt: _ClaimAttempt,
) -> ImportJob | None:
    return _claim_request_scoped_pinned(
        candidate, attempt, claim_fn=_claim_local_import,
    )


def _claim_route_plain(
    candidate: ImportJob,
    attempt: _ClaimAttempt,
) -> ImportJob | None:
    """Claim one candidate on the queue connection, with no pinned session."""
    job = attempt.db.claim_import_job_candidate(
        candidate.id,
        worker_id=attempt.worker_id,
    )
    if job is None:
        return None
    attempt.claim_state.mark()
    logger.info("Claimed import job %s (%s)", job.id, job.job_type)
    return process_claimed_job(
        attempt.db,
        job,
        ctx=attempt.ctx,
        execute_fn=attempt.execute_fn,
    )


#: The one place every job-type-DISPATCH decision in this module is made.
#: ``_automation_claim_is_current``'s identity conjunction is the one
#: job-type decision that stays outside it, deliberately — see the section
#: comment above ``_AUTHORITY_AUTOMATION``.
_IMPORT_JOB_KINDS: dict[str, _ImportJobKind] = {
    IMPORT_JOB_FORCE: _ImportJobKind(
        authority=_AUTHORITY_PINNED_PAIR,
        execute_fn=_execute_force_kind,
        claim_route=_claim_route_force_import,
        action_copy=_FORCE_ACTION_COPY_LANE,
        # Force jobs never reach ``_cleanup_committed_wrong_match_rejection``
        # at all: the row this job came FROM is the one it consumes below,
        # and a force rejection preserves the operator's own quarantine
        # folder (issue #1077 D7; docs/rejection-routing.md).
        runs_committed_wrong_match_cleanup=False,
        owns_wrong_match_source=True,
    ),
    IMPORT_JOB_LOCAL: _ImportJobKind(
        authority=_AUTHORITY_PINNED_PAIR,
        execute_fn=_execute_local_kind,
        claim_route=_claim_route_local_import,
        action_copy=_LOCAL_IMPORT_ACTION_COPY_LANE,
        runs_committed_wrong_match_cleanup=True,
        owns_wrong_match_source=False,
    ),
    IMPORT_JOB_AUTOMATION: _ImportJobKind(
        authority=_AUTHORITY_AUTOMATION,
        execute_fn=_execute_automation_kind,
        claim_route=_claim_route_automation,
        action_copy=None,
        runs_committed_wrong_match_cleanup=True,
        owns_wrong_match_source=False,
    ),
    IMPORT_JOB_YOUTUBE: _ImportJobKind(
        authority=_AUTHORITY_PLAIN,
        execute_fn=_execute_youtube_kind,
        claim_route=_claim_route_plain,
        action_copy=None,
        runs_committed_wrong_match_cleanup=True,
        owns_wrong_match_source=False,
    ),
}

#: NOT a fifth lane: the exact shape the pre-registry ``else``/fall-through
#: arms gave a ``job_type`` no adapter claims. ``ImportJob.from_row`` runs
#: ``validate_job_type``, so nothing can reach it today (issue #1246) — it
#: exists so an unvalidated type still fails closed at every consumer
#: instead of each one inventing its own default.
_UNROUTED_JOB_KIND = _ImportJobKind(
    authority=_AUTHORITY_PLAIN,
    execute_fn=_execute_unsupported_kind,
    claim_route=_claim_route_plain,
    action_copy=None,
    runs_committed_wrong_match_cleanup=True,
    owns_wrong_match_source=False,
)


def _kind_for(job_type: str) -> _ImportJobKind:
    """Answer "which import job kind is this" once, for every consumer."""
    return _IMPORT_JOB_KINDS.get(job_type, _UNROUTED_JOB_KIND)


def run_once(
    db: PipelineDB,
    *,
    worker_id: str,
    ctx: Any = None,
    stage_db_factory: Callable[[str], object] | None = None,
    execution_lease_factory: Callable[..., ExecutionLeaseSnapshot] | None = None,
    execute_fn: Callable[..., DispatchOutcome] = execute_import_job,
    scan_cursor: CandidateScanCursor | None = None,
) -> ImportJob | None:
    cursor = scan_cursor or CandidateScanCursor()
    execution_lease = capture_worker_execution_lease(
        systemd_unit=IMPORTER_SYSTEMD_UNIT,
        factory=execution_lease_factory,
    )

    def _peek(offset: int) -> list[ImportJob]:
        return db.peek_import_job_candidates(
            execution_lease=execution_lease,
            limit=IMPORT_CANDIDATE_SCAN_LIMIT,
            offset=offset,
        )

    def _claim(
        candidate: ImportJob,
        claim_state: ClaimState,
    ) -> ImportJob | None:
        attempt = _ClaimAttempt(
            db=db,
            worker_id=worker_id,
            ctx=ctx,
            execution_lease=execution_lease,
            stage_db_factory=stage_db_factory or PipelineDB,
            execute_fn=execute_fn,
            claim_state=claim_state,
        )
        return _kind_for(candidate.job_type).claim_route(candidate, attempt)

    return claim_one_candidate(
        scan_cursor=cursor, peek=_peek, claim=_claim,
    )


def recover_abandoned_automation_owners(
    db: _AutomationRecoveryDB,
    *,
    liveness_probe: ExecutionLivenessProbe | None = None,
    debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
) -> list[ImportJob]:
    """Re-probe every attached automation owner and recover the proven dead.

    Safe to run repeatedly against a live fleet, which is the point: a
    transient probe failure must not cost a request its acquisition until the
    next process restart. Every recovery still needs an exact death proof, so a
    live preview or import execution — including this worker's own in-flight
    job — is observed alive and left completely alone. The sweep is deliberately
    lane-agnostic: a dead owner is equally stuck whichever lane abandoned it,
    and the recovery write is owner-atomic under ``IMPORT(request_id)``.

    ``debris_removal_fn`` (issue #1089) is forwarded, unchanged, to
    ``db.recover_automation_import_job`` — production always uses the real
    ``remove_recovery_debris`` default; tests inject a stub.
    """
    recovered: list[ImportJob] = []
    for job in db.list_automation_import_jobs_for_startup_recovery():
        lease = execution_lease_from_job(job)
        if lease is None and job.status != IMPORT_JOB_RECOVERY_REQUIRED:
            # A leaseless owner in a live lane is simply waiting to be claimed
            # — normal, not abandoned. A historical leaseless
            # ``recovery_required`` owner still needs automatic convergence,
            # and ``never_claimed`` is its exact death proof.
            continue
        decision = probe_execution_liveness(
            lease,
            probe=liveness_probe,
        )
        try:
            recovered_job = db.recover_automation_import_job(
                job.id,
                expected_execution_lease=lease,
                decision=decision,
                requeue_message=RESTART_REQUEUE_MESSAGE,
                recovery_message=RESTART_RECOVERY_MESSAGE,
                debris_removal_fn=debris_removal_fn,
            )
        except Exception:
            # One unrecoverable owner must not abort the sweep or the worker's
            # startup — the other stuck requests still need their pass. The row
            # is left exactly as it was and retried on the next re-probe.
            logger.exception(
                "Recovery pass failed for automation owner %s; continuing",
                job.id,
            )
            continue
        if recovered_job is not None:
            recovered.append(recovered_job)
    return recovered


def recover_abandoned_running_jobs(
    db: _StartupRecoveryDB,
    *,
    liveness_probe: ExecutionLivenessProbe | None = None,
    debris_removal_fn: RecoveryDebrisRemovalFn = remove_recovery_debris,
    force_action_copy_path_fn: Callable[
        [int], str,
    ] = _default_force_action_copy_path,
) -> list[ImportJob]:
    """Recover only executions whose exact persisted lease is proven dead.

    Startup only: ``recover_running_import_jobs`` requeues every non-automation
    ``running`` row without a liveness probe, which is sound exactly once,
    before this singleton worker has claimed anything. The automation half is
    the re-runnable sweep and is also called periodically from ``main``.

    ``debris_removal_fn`` (issue #1089) is forwarded, unchanged, to BOTH
    ``db.recover_running_import_jobs`` and (review MINOR-7)
    ``recover_abandoned_automation_owners`` — a caller stubbing this one
    seam stubs both sweeps, the exact trap ``_no_debris_removal`` exists to
    prevent; production always uses the real ``remove_recovery_debris``
    default, tests inject a stub. ``force_action_copy_path_fn`` (issue
    #1089 review MAJOR-1) is forwarded, unchanged, to
    ``db.recover_running_import_jobs`` only — it derives a force job's
    debris-confinement root and has no automation-lane counterpart.
    """
    recovered: list[ImportJob] = []
    batch_size = 50
    while True:
        batch = db.recover_running_import_jobs(
            requeue_message=RESTART_REQUEUE_MESSAGE,
            recovery_message=RESTART_RECOVERY_MESSAGE,
            limit=batch_size,
            debris_removal_fn=debris_removal_fn,
            force_action_copy_path_fn=force_action_copy_path_fn,
        )
        recovered.extend(batch)
        if len(batch) < batch_size:
            break
    # The terminal job is the durable retry edge across a process kill or a
    # transient cleanup failure. Successful exact-path cleanup is recorded in
    # the job result; every missing/failed marker is retried on this startup.
    for job in db.list_terminal_force_action_cleanup_jobs():
        _record_terminal_force_action_cleanup(db, job, job)
    for job in db.list_terminal_force_wrong_match_cleanup_jobs():
        try:
            _record_terminal_force_wrong_match_cleanup(db, job)
        except Exception:
            # One unrecoverable job must not crash the whole importer
            # process (issue #1122 review MEDIUM-4): unlike
            # ``_record_terminal_force_action_cleanup``'s own callee
            # (fully exception-safe internally), ``_cleanup_failed_force_import``'s
            # ``audio_corrupt`` branch calls DB methods with no internal
            # guard, and an escape here would otherwise propagate through
            # this startup sweep into ``main()`` — a full process exit
            # under ``Restart=on-failure`` takes the entire import queue
            # down in a 5s crash loop over one bad row. The row is left
            # exactly as it was and retried on the next startup.
            logger.exception(
                "Recovery pass failed for wrong-match cleanup job %s; "
                "continuing",
                job.id,
            )
    recovered.extend(recover_abandoned_automation_owners(
        db,
        liveness_probe=liveness_probe,
        debris_removal_fn=debris_removal_fn,
    ))
    return recovered


def _drain_import_queue(
    db: PipelineDB,
    *,
    worker_id: str,
    poll_interval: float,
    once: bool,
    shutdown: GracefulShutdown,
) -> None:
    """The importer's steady-state poll loop, extracted for direct testing.

    Checked once per iteration, before the next claim attempt — never
    mid-``run_once()`` (see ``GracefulShutdown``). ``once`` still returns
    after at most one claimed job regardless of ``shutdown``, preserving
    the existing ``--once`` contract untouched.
    """
    scan_cursor = CandidateScanCursor()
    next_reprobe_at = (
        time.monotonic() + AUTOMATION_RECOVERY_REPROBE_INTERVAL_SECONDS
    )
    while True:
        if shutdown.requested:
            logger.info(
                "SIGTERM received; no further import jobs will be claimed",
            )
            return
        if time.monotonic() >= next_reprobe_at:
            next_reprobe_at = (
                time.monotonic()
                + AUTOMATION_RECOVERY_REPROBE_INTERVAL_SECONDS
            )
            reprobed = recover_abandoned_automation_owners(db)
            if reprobed:
                logger.warning(
                    "Liveness re-probe recovered %s abandoned "
                    "automation owner(s) a restart would have stranded",
                    len(reprobed),
                )
        job = run_once(
            db,
            worker_id=worker_id,
            scan_cursor=scan_cursor,
        )
        if once:
            return
        if job is None:
            time.sleep(poll_interval)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Drain the Cratedigger import queue",
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--worker-id", default=None)
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
    config_path, runtime_dir = resolve_startup_config_paths(
        config_path=args.config,
        runtime_dir=args.runtime_dir,
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    try:
        cfg = enforce_beets_startup(
            role="importer",
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
        importer_required_paths,
        probe_startup_paths,
    )
    required_paths = importer_required_paths(cfg)
    try:
        probe_startup_paths(
            unit="cratedigger-importer",
            logger=logger,
            required=required_paths,
        )
    except StartupProbeError:
        return 1

    configure_canonical_release_lookup(cfg)

    worker_id = args.worker_id or f"{socket.gethostname()}:{os.getpid()}"
    db = PipelineDB(args.dsn)
    # Best-effort graceful drain (issue #1089): systemd's ordinary SIGTERM
    # would otherwise terminate this process immediately, mid-job, exactly
    # the world the recovery-side crash-debris removal exists to clean up
    # after. A bounded ``TimeoutStopSec`` on the unit still SIGKILLs past
    # this — that world is unaffected and remains covered by the same fix.
    shutdown = GracefulShutdown()
    signal.signal(signal.SIGTERM, shutdown.request)
    try:
        # Keep the beets-mutating queue to one worker process. See
        # docs/advisory-locks.md for namespace rules.
        with db.advisory_lock(ADVISORY_LOCK_NAMESPACE_IMPORTER, 1) as acquired:
            if not acquired:
                logger.error("Another cratedigger importer is already running")
                return 1

            recovered = recover_abandoned_running_jobs(db)
            if recovered:
                logger.warning(
                    "Recovered %s abandoned running import job(s)",
                    len(recovered),
                )

            _drain_import_queue(
                db,
                worker_id=worker_id,
                poll_interval=args.poll_interval,
                once=args.once,
                shutdown=shutdown,
            )
            return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
