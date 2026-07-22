"""Download polling — the poll state machine and search orchestration.

All functions receive a CratediggerContext instead of reading
module-level globals. Split (issue #146 phase 3): materialization and
recovery live in lib/download_materialization.py; exact-release validation and
dispatch live in lib/download_validation.py; completion orchestration lives in
lib/download_processing.py; slskd transfer helpers in lib/slskd_transfers.py;
event-feed ingestion in
lib/slskd_events.py.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from contextlib import AbstractContextManager
from typing import (Any, Callable, Protocol, TYPE_CHECKING, assert_never,
                    runtime_checkable)

import msgspec

from lib import download_processing
from lib.download_processing import (
    Completed,
    CompletionDeferred,
    CompletionDispatched,
    CompletionFailed,
    CompletionResult,
    ProcessAlbumFn,
)
from lib.download_materialization import (
    Materialized,
    MaterializeFailed,
    MaterializeGuarded,
    MaterializeResult,
    _evaluate_staged_path_readiness,
    _materialize_processing_dir,
)
from lib.download_recovery import (
    classify_processing_path,
    reconcile_processing_current_path,
)
from lib.download_reconstruction import (
    reconstruct_grab_list_entry as _reconstruct_grab_list_entry,
)
from lib.grab_list import GrabListEntry, DownloadFile
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_folder_for_row,
    directory_has_entries,
)
from lib.quality import (ActiveDownloadState, ActiveDownloadFileState,
                         CooldownConfig,
                         FileFailureDetail,
                         PollCycleConfig,
                         PollCycleDecision,
                         PollCycleSnapshot,
                         PollFileSnapshot,
                         reduce_poll_cycle,
                         extract_usernames)
from lib import transitions
from lib.dispatch import _build_download_info
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    ImportJob,
    automation_import_dedupe_key,
    automation_import_payload,
)
from lib.slskd_client import DownloadUser
from lib.slskd_transfers import (
    _get_all_downloads_snapshot,
    cancel_and_delete,
    match_transfer_for_attempt,
    slskd_do_enqueue,
)
from lib.staged_album import StagedAlbum
from lib.terminal_outcomes import (
    PendingImportTerminalOutcome,
    TerminalDownloadAudit,
)

if TYPE_CHECKING:
    from album_source import AlbumRecord
    from lib.context import CratediggerContext
    from lib.pipeline_db import DownloadLogOutcome
    from lib.pipeline_db.rows import AlbumRequestRow

logger = logging.getLogger("cratedigger")


@runtime_checkable
class DownloadDB(transitions.TransitionsDB, Protocol):
    """The PipelineDB surface the download poll/search loop uses (#409).

    Extends ``TransitionsDB`` because the handle is forwarded into
    ``transitions.finalize_request``. ``log_download`` is declared with
    only the kwargs this module passes (the full signature lives on
    ``PipelineDB.log_download``). Parity tests live in
    ``tests/test_download.py``.
    """

    def get_downloading(self) -> list[AlbumRequestRow]: ...

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def check_and_apply_cooldown(
        self, username: str, config: CooldownConfig | None = None,
    ) -> bool: ...

    def update_download_state(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "downloading",
    ) -> bool: ...

    def update_download_state_if_downloading(
        self, request_id: int, state_json: str,
    ) -> bool: ...

    def update_download_state_current_path(
        self, request_id: int, current_path: str | None,
    ) -> bool: ...

    def log_download(
        self,
        request_id: int,
        *,
        soulseek_username: str | None = None,
        filetype: str | None = None,
        outcome: DownloadLogOutcome | None = None,
        error_message: str | None = None,
        transfer_detail: Any = None,
    ) -> int: ...

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob: ...

    def get_active_import_job_for_request(
        self, request_id: int,
    ) -> ImportJob | None: ...

    def get_import_job_candidate_evidence_id(
        self, import_job_id: int,
    ) -> int | None: ...

    def set_download_log_candidate_evidence(
        self, download_log_id: int, evidence_id: int | None,
    ) -> None: ...

    def abandon_auto_import_request(
        self,
        *,
        request_id: int,
        current_path: str,
        soulseek_username: str | None,
        filetype: str | None,
        beets_detail: str,
        outcome: str,
        staged_path: str,
        error_message: str,
        validation_result: str | None,
    ) -> int | None: ...


MAX_FILE_RETRIES = 5
# How long a completed download may keep failing local materialization
# (e.g. an event-stamp that never arrives) before the poller stops
# retrying and self-heals the request back to 'wanted' for re-download.
# Generous relative to the 5-min cycle: the benign completion-vs-event
# race resolves on the very next cycle.
PROCESSING_MATERIALIZE_GRACE_S = 3600


# === ActiveDownloadState building ===

def build_active_download_state(
    entry: GrabListEntry,
    *,
    enqueued_at: str | None = None,
    last_progress_at: str | None = None,
    processing_started_at: str | None = None,
    import_subprocess_started_at: str | None = None,
    current_path: str | None = None,
) -> ActiveDownloadState:
    """Build an ActiveDownloadState from a GrabListEntry.

    Callers can pass the original enqueued_at/processing_started_at when
    persisting updated retry state across polling cycles. The
    ``import_subprocess_started_at`` flag is preserved through state
    rebuilds so cycle-based retry persistence cannot accidentally clear
    the resume guard's witness — only the terminal status transitions
    (which NULL ``active_download_state`` inline) wipe it. See
    ``docs/advisory-locks.md``.
    """
    enqueued_at_value = enqueued_at or datetime.now(timezone.utc).isoformat()
    files = [
        ActiveDownloadFileState(
            username=f.username,
            filename=f.filename,
            file_dir=f.file_dir,
            size=f.size,
            disk_no=f.disk_no,
            disk_count=f.disk_count,
            retry_count=f.retry or 0,
            bytes_transferred=f.bytes_transferred or 0,
            last_state=f.last_state,
            last_exception=f.last_exception,
            local_path=f.local_path,
        )
        for f in entry.files
    ]
    return ActiveDownloadState(
        filetype=entry.filetype,
        enqueued_at=enqueued_at_value,
        last_progress_at=last_progress_at or enqueued_at_value,
        files=files,
        processing_started_at=processing_started_at,
        import_subprocess_started_at=import_subprocess_started_at,
        current_path=(
            current_path
            if current_path is not None
            else entry.import_folder
        ),
    )



# === Async download polling ===

def summarize_file_failures(files: list[DownloadFile]) -> str | None:
    """Compose a deterministic, human-readable summary of per-file
    download failures (issue #564 C5) — the evidence a download-timeout
    message names instead of a generic "vanished"/"errored" verdict.

    Per file, prefers ``last_exception`` (slskd's real per-transfer
    failure reason); falls back to a terminal ``last_state`` (any state
    starting ``"Completed,"`` other than ``"Completed, Succeeded"``).
    Files with no exception and no terminal-error state (still in
    progress, or genuinely never observed) contribute nothing.

    Returns ``None`` when no file carries any evidence at all — callers
    use that to distinguish "genuinely never observed" from "observed
    and failed" (I2).

    Deterministic ordering: most common reason first, ties broken
    alphabetically, so the composed message never varies cycle to cycle
    for the same evidence set.
    """
    counts: dict[str, int] = {}
    for f in files:
        reason = f.last_exception
        if not reason:
            state = f.last_state
            if (state and state.startswith("Completed,")
                    and state != "Completed, Succeeded"):
                reason = state
        if reason:
            counts[reason] = counts.get(reason, 0) + 1
    if not counts:
        return None
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(f"{count}× '{reason}'" for reason, count in ordered)


def _file_failure_details(files: list[DownloadFile]) -> list[FileFailureDetail]:
    """Per-file failure detail behind the composed timeout summary
    (issue #564 C7) — the full audit record (one entry per tracked
    file, not only the ones with evidence) persisted to
    ``download_log.transfer_detail``."""
    return [
        FileFailureDetail(
            username=f.username,
            filename=f.filename,
            last_state=f.last_state,
            last_exception=f.last_exception,
            bytes_transferred=f.bytes_transferred or 0,
            retry_count=f.retry or 0,
        )
        for f in files
    ]


def _vanished_timeout_reason(files: list[DownloadFile]) -> str:
    """Compose the reason for the "transfers vanished from slskd" timeout
    path (issue #564 C5/I2): names the last observed evidence when any
    exists (persisted from a prior cycle's poll or pre-purge harvest),
    and claims nothing was ever observed only when that's actually true.
    """
    summary = summarize_file_failures(files)
    if summary:
        return f"transfers no longer in slskd — last observed: {summary}"
    return (
        "transfers vanished from slskd before any status was observed "
        "(slskd restart?)")


def _enrich_timeout_reason(reason: str, files: list[DownloadFile]) -> str:
    """Append the per-file failure-evidence summary to a timeout reason
    (issue #564 C5), unless it's already embedded — the vanished-timeout
    reason above already names the same evidence inline, so this stays a
    no-op for that caller while still enriching every
    ``decide_download_action``-derived reason (whose strings are
    UNCHANGED — simulator scenarios depend on them; this is where the
    enrichment happens instead).
    """
    summary = summarize_file_failures(files)
    if summary and summary not in reason:
        return f"{reason} — {summary}"
    return reason


def _prepare_have_evidence_before_failure_log(
    request_id: int,
    mb_release_id: str,
    ctx: CratediggerContext,
    *,
    prepare_fn: Callable[..., str] | None = None,
) -> str:
    """Prepare canonical HAVE before the failure row establishes history."""
    if ctx.evidence_enrichment_budget <= 0:
        return "budget_exhausted"
    try:
        if prepare_fn is None:
            from lib.import_preview import prepare_current_evidence_for_failure
            prepare_fn = prepare_current_evidence_for_failure
        db = ctx.pipeline_db_source._get_db()
        outcome = prepare_fn(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=ctx.cfg.quality_ranks,
            beets_library_root=ctx.cfg.beets_directory,
        )
    except Exception:
        outcome = "failed"
        logger.warning(
            "HAVE evidence preparation crashed for request %s",
            request_id,
            exc_info=True,
        )
    if outcome == "failed":
        ctx.evidence_enrichment_budget -= 1
        logger.warning("HAVE evidence preparation failed for request %s", request_id)
    elif outcome not in ("ready", "no_current_evidence"):
        ctx.evidence_enrichment_budget -= 1
        logger.warning(
            "HAVE evidence preparation returned %r for request %s",
            outcome,
            request_id,
        )
        return "failed"
    return outcome


def _enrich_have_evidence_after_failure(
    request_id: int,
    mb_release_id: str,
    ctx: CratediggerContext,
    *,
    prepared_outcome: str,
    enrich_fn: Callable[..., str] | None = None,
) -> None:
    """Fill missing HAVE evidence after failure bookkeeping completes.

    A failed download never reaches preview — the only other place HAVE
    spectral/V0 evidence gets completed — but the request's on-disk copy is
    right there to measure. Budgeted per cycle so failure bursts never
    balloon the loop; a complete row costs nothing and is not budgeted.
    Never lets an enrichment error disturb failure bookkeeping.
    """
    if prepared_outcome != "ready" or ctx.evidence_enrichment_budget <= 0:
        return
    try:
        if enrich_fn is None:
            from lib.import_preview import (
                enrich_incomplete_current_evidence_for_request,
            )
            enrich_fn = enrich_incomplete_current_evidence_for_request
        db = ctx.pipeline_db_source._get_db()
        outcome = enrich_fn(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=ctx.cfg.quality_ranks,
            beets_library_root=ctx.cfg.beets_directory,
        )
    except Exception:
        ctx.evidence_enrichment_budget -= 1
        logger.warning(
            "HAVE evidence enrichment failed for request %s",
            request_id,
            exc_info=True,
        )
        return
    if outcome not in ("complete", "no_current_evidence", "stale"):
        ctx.evidence_enrichment_budget -= 1
        logger.info(
            "HAVE evidence enrichment for request %s: %s",
            request_id,
            outcome,
        )


def _timeout_album(
    entry: GrabListEntry,
    request_id: int,
    reason: str,
    ctx: CratediggerContext,
    *,
    prepare_fn: Callable[..., str] | None = None,
    enrich_fn: Callable[..., str] | None = None,
) -> None:
    """Handle download timeout: cancel, log, reset to wanted."""
    cancel_and_delete(entry.files, ctx)

    total = len(entry.files)
    completed = sum(1 for f in entry.files
                    if f.status and f.status.state == "Completed, Succeeded")

    dl_info = _build_download_info(entry)
    reason = _enrich_timeout_reason(reason, entry.files)
    transfer_detail = msgspec.to_builtins(_file_failure_details(entry.files))

    logger.info(f"DOWNLOAD TIMEOUT: {entry.artist} - {entry.title} "
                f"({completed}/{total} files done, reason={reason})")

    db = ctx.pipeline_db_source._get_db()
    # Capture/backfill HAVE before creating the audit row so Recents can
    # distinguish this unchanged pre-import library snapshot from a later
    # successful mutation. The helper is fail-soft; failure bookkeeping
    # still proceeds unchanged.
    prepared_outcome = _prepare_have_evidence_before_failure_log(
        request_id,
        entry.mb_release_id,
        ctx,
        prepare_fn=prepare_fn,
    )
    db.log_download(
        request_id=request_id,
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        outcome="timeout",
        error_message=reason,
        transfer_detail=transfer_detail,
    )
    for username in extract_usernames(entry.files):
        if db.check_and_apply_cooldown(username):
            ctx.cooled_down_users.add(username)
    transitions.require_transition_applied(transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
        ),
    ))
    _enrich_have_evidence_after_failure(
        request_id,
        entry.mb_release_id,
        ctx,
        prepared_outcome=prepared_outcome,
        enrich_fn=enrich_fn,
    )


def _persist_updated_download_state(
    db: DownloadDB,
    request_id: int,
    entry: GrabListEntry,
    state: ActiveDownloadState,
) -> bool:
    """Persist retry counters or processing markers back to JSONB."""
    return db.update_download_state(
        request_id,
        build_active_download_state(
            entry,
            enqueued_at=state.enqueued_at,
            last_progress_at=state.last_progress_at,
            processing_started_at=state.processing_started_at,
            import_subprocess_started_at=(
                state.import_subprocess_started_at
            ),
            current_path=entry.import_folder,
        ).to_json(),
        expected_status="downloading",
    )


def harvest_terminal_transfer_evidence(ctx: CratediggerContext) -> None:
    """Harvest terminal slskd transfer evidence immediately before the
    end-of-cycle purge (issue #564 root cause #3, C3).

    The end-of-cycle group in
    ``lib/convergence.py::CONVERGENCE_STEPS`` invokes
    ``lib.slskd_transfers.purge_completed_transfers`` every cycle, which
    removes each transfer record it purges from slskd's own history (issue
    #571 PR 5 flipped this from a bulk ``remove_completed_downloads()`` call
    to per-id, ledger-owned removal — narrower blast radius, same
    discard-on-removal effect for the records it DOES take). Any transfer
    that completed and errored within the SAME cycle it was enqueued, before
    the next poll cycle ever observes it, would otherwise lose its
    per-transfer terminal state — including the ``exception`` reason — the
    moment its record is removed. The very next poll then finds no transfer
    at all and reports a generic "vanished from slskd" timeout with zero
    evidence.

    This takes one final bulk snapshot and, for every ``downloading`` row
    that hasn't reached local processing yet, stamps any file whose
    matched transfer is now terminal into ``active_download_state`` —
    the SAME persisted fields ``reduce_poll_cycle`` returns
    (``last_state``, ``last_exception``, ``bytes_transferred``) via the
    real ``ActiveDownloadState`` round trip (decode -> mutate -> encode),
    never a hand-rolled JSON dict. Rows already past
    ``processing_started_at`` are skipped — their files already moved to
    local processing and are no longer purely slskd-side transfers.

    Matching is attempt-scoped (issue #820): the bulk snapshot's
    ``includeRemoved=True`` history can still contain a terminal record
    from a much older attempt at the SAME ``(username, filename)`` queue
    key (slskd never expires removed history). Matching with the plain
    ``match_transfer`` here — no attempt boundary — let a months-old
    ``Completed, Succeeded`` record outrank and get stamped over the
    CURRENT attempt's genuine terminal state (e.g. a real
    ``Completed, Errored``), laundering a real failure into a false
    "download complete" the next poll cycle then trusted. Every match
    here goes through ``match_transfer_for_attempt`` with
    ``not_before=state.enqueued_at`` — the same attempt boundary the poll
    path (``_poll_one_active_download``) already applies — so only
    evidence belonging to THIS attempt is ever stamped.

    Best-effort and silent on the happy path: a snapshot failure skips
    the whole pass, and ANY per-row failure (undecodable
    ``active_download_state``, a matcher error, the state write raising)
    skips only that row — one row's failure must never abort harvesting
    the remaining rows, because the purge runs immediately after and an
    aborted loop would destroy the un-harvested rows' evidence (the I1b
    failure mode). The purge always still runs regardless (the
    pre-existing behavior). The write goes through the status-guarded
    ``update_download_state_if_downloading`` — mirroring the poll path's
    fresh-status guard — so a row a concurrent operator action just
    flipped out of ``downloading`` is never rewritten. MUST be called
    before ``purge_completed_transfers``; that ordering is owned by the
    end-of-cycle registry in ``lib/convergence.py::CONVERGENCE_STEPS``.
    """
    db = ctx.pipeline_db_source._get_db()
    downloading = db.get_downloading()
    if not downloading:
        return

    snapshot = _get_all_downloads_snapshot(
        ctx.slskd, purpose="pre-purge terminal evidence harvest")
    if snapshot is None:
        return

    harvested = 0
    for row in downloading:
        request_id = row["id"]
        try:
            raw_state = row.get("active_download_state")
            if not raw_state:
                continue
            state = ActiveDownloadState.from_raw(raw_state)
            if state.processing_started_at is not None:
                continue

            dirty = False
            for f in state.files:
                if f.last_state and f.last_state.startswith("Completed,"):
                    continue
                transfer = match_transfer_for_attempt(
                    snapshot, f.filename, username=f.username,
                    not_before=state.enqueued_at)
                if (transfer is None
                        or not transfer.state.startswith("Completed,")):
                    continue
                f.last_state = transfer.state
                f.last_exception = transfer.exception or f.last_exception
                f.bytes_transferred = transfer.bytes_transferred
                dirty = True

            if dirty and db.update_download_state_if_downloading(
                    request_id, state.to_json()):
                harvested += 1
        except Exception:
            logger.warning(
                "HARVEST: request %s could not be harvested — skipping "
                "this row this cycle", request_id, exc_info=True)
            continue

    if harvested:
        logger.info(
            "HARVEST: captured pre-purge terminal transfer evidence for "
            "%d downloading row(s)", harvested)

def _run_completed_processing(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: DownloadDB,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    process_album_fn: ProcessAlbumFn | None = None,
    bundle_terminal_outcome: bool = False,
) -> CompletionResult:
    """Run or resume local post-download processing for a completed album.

    ``process_album_fn`` is an opt-in DI seam for tests that exercise the
    outer transition flow without going through the full
    ``process_completed_album`` body. Defaults to the real production
    function so callers in ``scripts/importer.py`` are unchanged.

    ``bundle_terminal_outcome`` is set only by the import-job owner. It
    returns the local fallback request transition plus mandatory audit as
    typed intent for ``process_claimed_job`` to commit with the job. The
    default retains the direct/no-job transition behavior.

    The default is resolved via the ``download_processing`` module
    reference (not a from-import binding) so that patching
    ``lib.download_processing.process_completed_album`` in tests is
    honored here at call time, regardless of import order (#536).
    """
    _process = (
        process_album_fn
        if process_album_fn is not None
        else download_processing.process_completed_album
    )

    if state.processing_started_at is None:
        if entry.import_folder is None:
            entry.import_folder = canonical_folder_for_row(
                entry,
                ctx.cfg.slskd_download_dir,
            )
        state.processing_started_at = datetime.now(timezone.utc).isoformat()
        if not _persist_updated_download_state(db, request_id, entry, state):
            return CompletionDeferred(
                detail="request_state_changed_before_local_processing",
            )

    try:
        result = _process(
            entry,
            ctx,
            import_job_id=import_job_id,
        )
    except Exception:
        logger.exception(f"Error processing completed download {entry.artist} - {entry.title} "
                         f"— will retry local processing next cycle")
        return CompletionDeferred(detail="unhandled_exception_during_local_processing")

    # Ownership return from ``process_completed_album`` (see
    # ``CompletionResult`` in lib/download_processing.py):
    # - Completed           → processing succeeded; flip to 'imported' if
    #   status is still 'downloading'.
    # - CompletionFailed    → a non-deferred failure path returned; reset
    #   to 'wanted' only if the request row is still 'downloading'.
    # - CompletionDispatched → dispatch/finalization already owned request
    #   transitions; return the summary to the queue owner only.
    # - CompletionDeferred  → leave the row untouched. This covers
    #   release-lock contention, guarded post-move staged paths, and
    #   ownership-less request rejects that require manual recovery. Do
    #   NOT touch state here.
    if isinstance(result, CompletionDeferred):
        return result

    if isinstance(result, CompletionDispatched):
        return result

    if isinstance(result, Completed):
        refreshed = db.get_request(request_id)
        transition = None
        if refreshed and refreshed["status"] == "downloading":
            transition = transitions.RequestTransition.to_imported(
                from_status="downloading",
            )
        if bundle_terminal_outcome and transition is not None:
            return Completed(terminal_outcome=_local_completion_terminal_outcome(
                entry,
                state,
                request_id=request_id,
                import_job_id=import_job_id,
                transition=transition,
                outcome="success",
                detail="Local automation import processing completed",
            ))
        if transition is not None:
            logger.info(
                "  process_completed_album succeeded without setting status "
                "— setting imported"
            )
            transitions.require_transition_applied(
                transitions.finalize_request(db, request_id, transition)
            )
        return result

    match result:
        case CompletionFailed():
            refreshed = db.get_request(request_id)
            transition = None
            if refreshed and refreshed["status"] == "downloading":
                transition = transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                    attempt_type="download",
                )
            if bundle_terminal_outcome and transition is not None:
                return CompletionFailed(
                    reason=result.reason,
                    terminal_outcome=_local_completion_terminal_outcome(
                        entry,
                        state,
                        request_id=request_id,
                        import_job_id=import_job_id,
                        transition=transition,
                        outcome="failed",
                        detail=result.reason,
                        error_message=result.reason,
                    ),
                )
            if transition is not None:
                logger.warning(
                    "  process_completed_album failed without setting status "
                    "— resetting to wanted"
                )
                transitions.require_transition_applied(
                    transitions.finalize_request(db, request_id, transition)
                )
            return result
    assert_never(result)


def _local_completion_terminal_outcome(
    entry: GrabListEntry,
    state: ActiveDownloadState,
    *,
    request_id: int,
    import_job_id: int,
    transition: transitions.RequestTransition | None,
    outcome: DownloadLogOutcome,
    detail: str,
    error_message: str | None = None,
) -> PendingImportTerminalOutcome:
    """Build the atomic fallback outcome for one automation import job."""
    dl_info = _build_download_info(entry)
    source_path = entry.import_folder or state.current_path
    return PendingImportTerminalOutcome(
        request_id=request_id,
        import_job_id=import_job_id,
        initial_transition=transition,
        audit=TerminalDownloadAudit(
            soulseek_username=dl_info.username,
            filetype=dl_info.filetype or state.filetype,
            download_path=source_path,
            beets_detail=detail,
            outcome=outcome,
            error_message=error_message,
        ),
    )


def _active_import_job_for_request(
    db: DownloadDB, request_id: int,
) -> ImportJob | None:
    return db.get_active_import_job_for_request(request_id)


def materialize_failure_action(
    materialized: MaterializeResult,
    processing_started_at: str | None,
    now: datetime,
    *,
    grace_seconds: int = PROCESSING_MATERIALIZE_GRACE_S,
) -> str:
    """Decide what the poller does with a non-``Materialized`` outcome.

    - ``"leave"`` — ``MaterializeGuarded`` marks paths needing manual
      recovery; NEVER auto-reset those, regardless of age. (Also the
      no-op answer if ``materialized`` is actually ``Materialized`` —
      callers only invoke this after already excluding that case.)
    - ``"retry"`` — ``MaterializeFailed`` within the grace window retries
      next cycle (covers the benign completion-vs-event-write race,
      which resolves on the next ingest).
    - ``"reset"`` — ``MaterializeFailed`` past the grace window self-heals
      the request back to 'wanted' for re-download.
    """
    if not isinstance(materialized, MaterializeFailed):
        return "leave"
    if processing_started_at is None:
        return "retry"
    try:
        started = datetime.fromisoformat(processing_started_at)
    except ValueError:
        return "retry"
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    if (now - started).total_seconds() > grace_seconds:
        return "reset"
    return "retry"


def _enqueue_completed_processing(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: DownloadDB,
    ctx: CratediggerContext,
) -> ImportJob | None:
    """Submit completed-download processing to the shared import queue."""
    if state.processing_started_at is None:
        raise ValueError("processing must be marked before importer enqueue")
    if entry.import_folder is None:
        entry.import_folder = (
            state.current_path
            or canonical_folder_for_row(entry, ctx.cfg.slskd_download_dir)
        )
    staged_album = StagedAlbum.from_entry(
        entry,
        default_path=canonical_folder_for_row(
            entry,
            ctx.cfg.slskd_download_dir,
        ),
    )
    materialized = _materialize_processing_dir(
        entry,
        staged_album,
        ctx,
        persist_current_path=False,
    )
    if not isinstance(materialized, Materialized):
        action = materialize_failure_action(
            materialized,
            state.processing_started_at,
            datetime.now(timezone.utc),
        )
        if action == "reset":
            detail = (
                "Completed download could not be materialized within "
                f"{PROCESSING_MATERIALIZE_GRACE_S}s of processing start; "
                "resetting to wanted for re-download"
            )
            logger.error(
                "MATERIALIZE GRACE EXPIRED: request_id=%s %s - %s — %s",
                request_id,
                entry.artist,
                entry.title,
                detail,
            )
            dl_info = _build_download_info(entry)
            prepared_outcome = _prepare_have_evidence_before_failure_log(
                request_id,
                entry.mb_release_id,
                ctx,
            )
            db.log_download(
                request_id=request_id,
                soulseek_username=dl_info.username,
                filetype=dl_info.filetype,
                outcome="failed",
                error_message=detail,
            )
            transitions.require_transition_applied(transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                    attempt_type="download",
                ),
            ))
            _enrich_have_evidence_after_failure(
                request_id,
                entry.mb_release_id,
                ctx,
                prepared_outcome=prepared_outcome,
            )
            return None
        logger.warning(
            "Completed download for request %s could not be materialized "
            "for import preview; leaving it for the next poll cycle",
            request_id,
        )
        return None
    entry.import_folder = staged_album.current_path
    job = db.enqueue_import_job(
        IMPORT_JOB_AUTOMATION,
        request_id=request_id,
        dedupe_key=automation_import_dedupe_key(request_id),
        payload=automation_import_payload(),
        message=f"Automation import queued for {entry.artist} - {entry.title}",
    )
    if job.deduped:
        logger.info(
            "Automation import already active for request %s "
            "(job %s)",
            request_id,
            job.id,
        )
    else:
        logger.info(
            "Queued automation import for request %s as job %s",
            request_id,
            job.id,
        )
    return job


def _processing_path_ready_for_importer(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: DownloadDB,
    ctx: CratediggerContext,
) -> bool:
    """Fail closed before enqueueing a job that cannot resume local files.

    Thin wrapper around the ONE shared staged-path-readiness decision
    (``lib.download_materialization._evaluate_staged_path_readiness``, issue
    #509) — the same decision ``_materialize_processing_dir`` uses for
    its own non-canonical branch, so this pre-enqueue gate and the
    materialize step it precedes can never drift apart again. This
    wrapper only translates the tagged result into this caller's
    ``bool`` contract and applies this gate's own, deliberately
    different reaction to failure: an IMMEDIATE reset to 'wanted', not
    the grace-windowed retry/reset ``materialize_failure_action``
    applies when the same tag surfaces later from
    ``_enqueue_completed_processing``'s own materialize call. That
    difference is intentional and tested (``test_poll_missing_
    persisted_current_path_resets_to_wanted``) — this gate exists to
    fail closed before even attempting a heavier materialize/enqueue,
    not to duplicate the grace window a first materialize attempt
    already earned.
    """
    if state.processing_started_at is None or state.current_path is None:
        return True

    current_path_location = classify_processing_path(
        current_path=state.current_path,
        artist=entry.artist,
        title=entry.title,
        year=entry.year,
        request_id=request_id,
        staging_dir=ctx.cfg.beets_staging_dir,
        slskd_download_dir=ctx.cfg.slskd_download_dir,
        attempt_fingerprint=attempt_fingerprint(
            [(f.username, f.filename) for f in entry.files],
        ),
    )
    if current_path_location.kind == "canonical":
        # The canonical processing folder may not exist yet — the
        # importer materializes it from the completed slskd files as
        # its first step. Nothing to check here.
        return True

    staged_album = StagedAlbum.from_entry(entry, default_path=state.current_path)
    result = _evaluate_staged_path_readiness(
        entry, staged_album, current_path_location, db,
    )
    if isinstance(result, Materialized):
        return True
    if isinstance(result, MaterializeGuarded):
        return False

    assert isinstance(result, MaterializeFailed)
    transitions.require_transition_applied(transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
        ),
    ))
    return False


def poll_active_downloads(ctx: CratediggerContext) -> None:
    """Poll slskd for status of all downloading albums.

    For each album with status='downloading':
    1. Reconstruct GrabListEntry from DB + ActiveDownloadState
    2. Re-derive slskd transfer IDs
    3. Mark files with vanished transfers as errored (synthetic status)
    4. Poll file status for remaining files
    5. If all complete → process_completed_album()
    6. If timeout exceeded → cancel, log, reset to wanted
    7. If errors → retry individual files (persisted, max 5 retries per file)
    """
    db = ctx.pipeline_db_source._get_db()
    downloading = db.get_downloading()

    # One bulk snapshot for the entire poll cycle — avoids per-file API
    # calls. Fetched BEFORE event ingestion, deliberately: a transfer the
    # snapshot shows Completed finished before the snapshot, and therefore
    # before the ingest below — so its DownloadFileComplete event is in
    # the feed and the file reaches processing stamped. The reverse order
    # left a cycle-length race where same-cycle completions processed
    # unstamped.
    cycle_snapshot = None
    if downloading:
        cycle_snapshot = _get_all_downloads_snapshot(
            ctx.slskd, purpose="poll cycle snapshot")

    # Issue #146: stamp authoritative local paths from slskd's
    # DownloadFileComplete events before processing. Runs even with no
    # downloading rows so the cursor keeps tracking the feed. An ingest
    # failure stamps nothing this cycle — completions ride the
    # materialize grace window — and never blocks polling.
    try:
        from lib.slskd_events import ingest_download_file_events
        ingest_result = ingest_download_file_events(db, ctx.slskd, downloading)
        logger.info(ingest_result.to_log_line())
        if ingest_result.files_stamped:
            # Re-read ONLY the rows we already hold: a row that turned
            # 'downloading' after the snapshot above (Phase 2 enqueues
            # concurrently) must not be polled against a snapshot that
            # predates its transfers.
            known_ids = {row["id"] for row in downloading}
            downloading = [
                row for row in db.get_downloading()
                if row["id"] in known_ids
            ]
    except Exception:
        logger.exception(
            "SLSKD EVENTS: ingest failed — nothing stamped this cycle; "
            "completions ride the materialize grace window")

    if not downloading:
        return

    logger.info(f"Polling {len(downloading)} active download(s)...")

    if cycle_snapshot is None:
        logger.warning("Failed to get download snapshot — skipping poll cycle")
        return

    for row in downloading:
        request_id = row["id"]
        try:
            _poll_one_active_download(row, db, ctx, cycle_snapshot)
        except Exception:
            # A single bad row (overlong canonical path, missing slskd
            # files raising past our inner guards, etc.) must never
            # starve the rest of the poll cycle.
            logger.exception(
                "Unhandled exception processing downloading request %s — "
                "skipping for this poll cycle so other rows still process",
                request_id,
            )


def _poll_one_active_download(
    row: Mapping[str, Any],
    db: DownloadDB,
    ctx: CratediggerContext,
    cycle_snapshot: list[DownloadUser],
) -> None:
    """Build poll facts, persist one reduced state, then dispatch one effect."""
    request_id = row["id"]
    raw_state = row.get("active_download_state")
    state = ActiveDownloadState.from_raw(raw_state) if raw_state else None
    active_import_job = (
        _active_import_job_for_request(db, request_id)
        if state is not None
        else None
    )

    recovery_decision = None
    if (
        state is not None
        and state.processing_started_at is not None
        and active_import_job is None
    ):
        recovery_decision = reconcile_processing_current_path(
            current_path=state.current_path,
            artist=row["artist_name"],
            title=row["album_title"],
            year=str(row["year"] or ""),
            request_id=request_id,
            staging_dir=ctx.cfg.beets_staging_dir,
            slskd_download_dir=ctx.cfg.slskd_download_dir,
            has_entries=directory_has_entries,
            attempt_fingerprint=attempt_fingerprint(
                [(f.username, f.filename) for f in state.files],
            ),
        )

    file_snapshots: list[PollFileSnapshot] = []
    completion_current_path = None
    if (
        state is not None
        and state.processing_started_at is None
        and active_import_job is None
    ):
        initial_entry = _reconstruct_grab_list_entry(row, state)
        completion_current_path = canonical_folder_for_row(
            initial_entry,
            ctx.cfg.slskd_download_dir,
        )
        for file in state.files:
            transfer = match_transfer_for_attempt(
                cycle_snapshot,
                file.filename,
                username=file.username,
                not_before=state.enqueued_at,
            )
            file_snapshots.append(PollFileSnapshot(
                transfer_id=transfer.id if transfer is not None else None,
                state=transfer.state if transfer is not None else None,
                bytes_transferred=(
                    transfer.bytes_transferred if transfer is not None else 0
                ),
                exception=transfer.exception if transfer is not None else None,
            ))

    snapshot = PollCycleSnapshot(
        files=file_snapshots,
        active_import_job_id=(
            active_import_job.id if active_import_job is not None else None
        ),
        active_import_job_status=(
            active_import_job.status if active_import_job is not None else None
        ),
        processing_current_path=(
            recovery_decision.selected_location.path
            if recovery_decision is not None
            and recovery_decision.selected_location is not None
            else None
        ),
        processing_blocked_reason=(
            recovery_decision.blocked_reason
            if recovery_decision is not None
            else None
        ),
        completion_current_path=completion_current_path,
    )
    now = datetime.now(timezone.utc)
    result = reduce_poll_cycle(
        state,
        snapshot,
        now,
        PollCycleConfig(
            remote_queue_timeout=ctx.cfg.remote_queue_timeout,
            stalled_timeout=ctx.cfg.stalled_timeout,
            max_file_retries=MAX_FILE_RETRIES,
        ),
    )

    # The reducer returns the whole observation state, so every valid row
    # persists unconditionally here while this worker still owns a
    # downloading row. Losing that guard means a concurrent transition won;
    # its state and every downstream side effect take precedence.
    if (
        result.state is not None
        and not db.update_download_state_if_downloading(
            request_id,
            result.state.to_json(),
        )
    ):
        return

    verdict = result.verdict
    if verdict.decision == PollCycleDecision.reset_missing_state:
        logger.error(f"Downloading album {request_id} has no active_download_state — "
                     f"resetting to wanted")
        transitions.require_transition_applied(transitions.finalize_request(
            db,
            request_id,
            transitions.RequestTransition.to_wanted(
                from_status="downloading",
            ),
        ))
        return

    if verdict.decision == PollCycleDecision.wait_import_job:
        logger.info(
            "Request %s is waiting on importer job %s (%s)",
            request_id,
            verdict.import_job_id,
            verdict.import_job_status,
        )
        return

    if verdict.decision == PollCycleDecision.wait_processing_recovery:
        assert recovery_decision is not None
        if verdict.reason == "multiple_populated_paths":
            rendered_candidates = ", ".join(
                f"{location.short_label}={location.path}"
                for location in recovery_decision.populated_locations
            )
            logger.error(
                "MID-PROCESS RESUME BLOCKED: request_id=%s %s - %s "
                "found multiple populated recovery paths (%s). "
                "Manual recovery is required.",
                request_id,
                row["artist_name"],
                row["album_title"],
                rendered_candidates,
            )
            return
        if verdict.reason == "legacy_shared_only":
            logger.error(
                "LEGACY STAGED RESUME BLOCKED: request_id=%s %s - %s "
                "persisted current_path=%s could not be resumed, "
                "canonical_path=%s has no files, "
                "and staged_path=%s is ambiguous across editions. "
                "Manual recovery is required.",
                request_id,
                row["artist_name"],
                row["album_title"],
                result.state.current_path if result.state is not None else None,
                recovery_decision.canonical_path,
                recovery_decision.legacy_shared_path,
            )
            return
        raise AssertionError(f"unknown processing recovery block: {verdict.reason}")

    state = result.state
    assert state is not None
    transfer_ids = {
        (file.username, file.filename): observation.transfer_id
        for file, observation in zip(state.files, snapshot.files)
        if observation.transfer_id is not None
    }
    entry = _reconstruct_grab_list_entry(
        row,
        state,
        transfer_ids=transfer_ids,
    )

    if verdict.decision == PollCycleDecision.processing:
        if not _processing_path_ready_for_importer(
            entry,
            request_id,
            state,
            db,
            ctx,
        ):
            return
        _enqueue_completed_processing(entry, request_id, state, db, ctx)
        return

    if verdict.decision == PollCycleDecision.wait_fresh_vanished:
        logger.info(
            "Request %s has fresh planned ownership but no visible "
            "slskd transfers yet; deferring vanished-transfer reset",
            request_id,
        )
        return

    if verdict.decision == PollCycleDecision.timeout_vanished:
        _timeout_album(
            entry,
            request_id,
            _vanished_timeout_reason(entry.files),
            ctx,
        )
        return

    if verdict.decision == PollCycleDecision.timeout_remote_queue:
        _timeout_album(entry, request_id, verdict.reason, ctx)
        return

    if verdict.decision == PollCycleDecision.complete:
        logger.info(f"Download complete: {entry.artist} - {entry.title}")
        _enqueue_completed_processing(entry, request_id, state, db, ctx)
        return

    if verdict.decision == PollCycleDecision.timeout_all_errored:
        _timeout_album(entry, request_id, verdict.reason, ctx)
        return

    if verdict.decision == PollCycleDecision.timeout_stalled:
        _timeout_album(entry, request_id, verdict.reason, ctx)
        return

    if verdict.decision == PollCycleDecision.retry_files:
        for retry_filename in verdict.files_to_retry:
            for file in entry.files:
                if file.filename == retry_filename:
                    retries_used = file.retry or 0
                    logger.info(f"Re-enqueue failed file "
                                f"({retries_used}/{MAX_FILE_RETRIES} retries): "
                                f"{retry_filename}")
                    requeue = slskd_do_enqueue(
                        file.username,
                        [{"filename": file.filename, "size": file.size}],
                        file.file_dir, ctx,
                        request_id=request_id,
                        attempt_fp=attempt_fingerprint(
                            [(f.username, f.filename) for f in entry.files]),
                    )
                    if not requeue:
                        logger.warning(f"Failed to re-enqueue file: {retry_filename}")
                    break

        refreshed = db.get_request(request_id)
        if refreshed and refreshed["status"] != "downloading":
            return

    elif verdict.decision != PollCycleDecision.in_progress:
        assert_never(verdict.decision)

    # Still in progress — log and continue to next album
    enqueued_at = datetime.fromisoformat(state.enqueued_at)
    if enqueued_at.tzinfo is None:
        enqueued_at = enqueued_at.replace(tzinfo=timezone.utc)
    elapsed_seconds = (now - enqueued_at).total_seconds()
    files_done = sum(
        1 for file in state.files
        if file.last_state == "Completed, Succeeded"
    )
    logger.info(f"In progress: {entry.artist} - {entry.title} "
                f"({files_done}/{len(entry.files)} files, "
                f"{elapsed_seconds/60:.1f}min elapsed)")


# === Top-level orchestration ===

def grab_most_wanted(
    albums: "list[AlbumRecord]",
    search_and_queue: Callable[
        ...,
        "tuple[dict[int, GrabListEntry], list[AlbumRecord], list[AlbumRecord]]",
    ],
    ctx: CratediggerContext,
) -> int:
    """Search, enqueue, persist download state, return immediately.

    Does NOT block waiting for downloads. Download monitoring happens
    in poll_active_downloads() on subsequent runs.
    """
    grab_list, failed_search, failed_grab = search_and_queue(albums)

    total_albums = len(grab_list)
    logger.info(f"Total Downloads added: {total_albums}")
    for album_id in grab_list:
        entry = grab_list[album_id]
        logger.info(f"Album: {entry.title} Artist: {entry.artist}")

        # Legacy/test fallback: production find_download workers claim
        # ownership before the slskd enqueue. Keep this only for callers that
        # do not provide the worker-safe ownership collaborator.
        request_id = entry.db_request_id
        if request_id and getattr(ctx, "download_ownership", None) is None:
            state = build_active_download_state(entry)
            db = ctx.pipeline_db_source._get_db()
            transitions.require_transition_applied(transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_downloading(
                    from_status="wanted",
                    state_json=state.to_json(),
                ),
            ))
            logger.info(f"  Set status=downloading, {len(entry.files)} files tracked")

    logger.info(f"Failed to grab: {len(failed_grab)}")
    for album in failed_grab:
        logger.info(f"Album: {album.title} Artist: {album.artist_name}")

    count = len(failed_search) + len(failed_grab)
    for album in failed_search:
        logger.info(f"Search failed for Album: {album.title} - Artist: {album.artist_name}")
    for album in failed_grab:
        logger.info(f"Download failed for Album: {album.title} - Artist: {album.artist_name}")

    return count
