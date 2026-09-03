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
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    assert_never,
    runtime_checkable,
)

import msgspec

from lib import download_processing, transitions
from lib.current_library_evidence import HaveEnrichment, HavePreparation
from lib.dispatch import _build_download_info
from lib.download_processing import (
    CompletionDeferred,
    CompletionResult,
    ProcessAlbumFn,
)
from lib.download_reconstruction import (
    reconstruct_grab_list_entry as _reconstruct_grab_list_entry,
)
from lib.grab_list import DownloadFile, GrabListEntry
from lib.import_execution import (
    CancellationToken,
    ExecutionCancelled,
    ExecutionOwnerProof,
)
from lib.import_queue import (
    AutomationHandoffResult,
    ImportJob,
)
from lib.pipeline_db._shared import ADVISORY_LOCK_NAMESPACE_IMPORT
from lib.processing_paths import (
    attempt_fingerprint_of_files,
    attempt_fingerprint_or_none,
    canonical_folder_for_row,
    processing_albums_dir,
)
from lib.quality import (
    ActiveDownloadFileState,
    ActiveDownloadState,
    CooldownConfig,
    FileFailureDetail,
    PollCycleConfig,
    PollCycleDecision,
    PollCycleSnapshot,
    PollFileSnapshot,
    extract_usernames,
    reduce_poll_cycle,
)
from lib.slskd_client import DownloadUser
from lib.slskd_events import _parse_event_timestamp
from lib.slskd_transfers import (
    _get_all_downloads_snapshot,
    cancel_and_delete,
    match_transfer_for_attempt,
    slskd_do_enqueue,
)
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

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool: ...

    def log_download(
        self,
        request_id: int,
        *,
        soulseek_username: str | None = None,
        contributor_usernames: Sequence[str] | None = None,
        filetype: str | None = None,
        outcome: DownloadLogOutcome | None = None,
        beets_detail: str | None = None,
        error_message: str | None = None,
        transfer_detail: Any = None,
    ) -> int: ...

    def handoff_automation_import(
        self,
        *,
        request_id: int,
        expected_enqueued_at: str,
        canonical_path: str,
        message: str,
    ) -> AutomationHandoffResult: ...

    def get_import_job_candidate_evidence_id(
        self, import_job_id: int,
    ) -> int | None: ...

    def set_download_log_candidate_evidence(
        self,
        download_log_id: int,
        evidence_id: int | None,
        *,
        direct_attribution: bool = False,
        contributor_usernames: Sequence[str] | None = None,
    ) -> None: ...

MAX_FILE_RETRIES = 5


# === ActiveDownloadState building ===

def build_active_download_state(
    entry: GrabListEntry,
    *,
    enqueued_at: str | None = None,
    last_progress_at: str | None = None,
) -> ActiveDownloadState:
    """Build an ActiveDownloadState from a GrabListEntry.

    Callers can pass the original timing witnesses when persisting updated
    retry state across polling cycles. Processor ownership and its canonical
    path are added only by the atomic handoff command.

    ``attempt_fingerprint`` (issue #1196 item 1) is computed here from
    ``entry.files`` through
    ``lib.processing_paths.attempt_fingerprint_or_none`` -- literally the
    same function ``lib.enqueue._enqueue_with_claim_outcome`` calls to
    compute the ``attempt_fp`` written onto every
    ``slskd_transfer_ledger`` row this attempt writes, there from
    ``claim.entry.files``. Every production caller passes either that
    SAME ``entry`` (the initial claim,
    ``lib.enqueue._claim_initial_download_ownership``) or one whose
    ``.files`` is content-identical to it (a reconstructed entry built
    from an accepted-downloads subset that, by construction, is only
    ever persisted once every planned file across every disc was
    accepted -- ``lib.enqueue._persist_claimed_download_state``). Equal
    inputs through one shared formula is what makes the two fingerprints
    agree; before issue #1278 the formula itself was written out twice.
    """
    enqueued_at_value = enqueued_at or datetime.now(UTC).isoformat()
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
        processing_started_at=None,
        current_path=None,
        attempt_fingerprint=attempt_fingerprint_or_none(entry.files),
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
    prepare_fn: Callable[..., HavePreparation] | None = None,
) -> HavePreparation | None:
    """Prepare canonical HAVE before the failure row establishes history.

    Returns ``None`` when the cycle's enrichment budget is already spent, so
    nothing was attempted — distinct from every outcome the preparation
    itself can reach.
    """
    if ctx.evidence_enrichment_budget <= 0:
        return None
    try:
        if prepare_fn is None:
            from lib.current_library_evidence import (
                prepare_current_evidence_for_failure,
            )
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
        outcome = HavePreparation.FAILED
        logger.warning(
            "HAVE evidence preparation crashed for request %s",
            request_id,
            exc_info=True,
        )
    if outcome.charges_budget:
        ctx.evidence_enrichment_budget -= 1
        logger.warning(
            "HAVE evidence preparation for request %s: %s",
            request_id,
            outcome.value,
        )
    return outcome


def _enrich_have_evidence_after_failure(
    request_id: int,
    mb_release_id: str,
    ctx: CratediggerContext,
    *,
    prepared_outcome: HavePreparation | None,
    enrich_fn: Callable[..., HaveEnrichment] | None = None,
) -> None:
    """Fill missing HAVE evidence after failure bookkeeping completes.

    A failed download never reaches preview — the only other place HAVE
    spectral/V0 evidence gets completed — but the request's on-disk copy is
    right there to measure. Budgeted per cycle so failure bursts never
    balloon the loop; a complete row costs nothing and is not budgeted.
    Never lets an enrichment error disturb failure bookkeeping.
    """
    if (
        prepared_outcome != HavePreparation.READY
        or ctx.evidence_enrichment_budget <= 0
    ):
        return
    try:
        if enrich_fn is None:
            from lib.current_library_evidence import (
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
    if outcome.charges_budget:
        ctx.evidence_enrichment_budget -= 1
        logger.info(
            "HAVE evidence enrichment for request %s: %s",
            request_id,
            outcome.value,
        )


def _owns_downloading_incarnation(
    db: DownloadDB,
    *,
    request_id: int,
    expected_enqueued_at: str,
) -> bool:
    """Re-read the exact downloader witness under the caller's IMPORT lock."""
    row = db.get_request(request_id)
    if (
        row is None
        or row.get("status") != "downloading"
        or row.get("active_automation_import_job_id") is not None
    ):
        return False
    raw_state = row.get("active_download_state")
    if raw_state is None:
        return False
    try:
        current = ActiveDownloadState.from_raw(raw_state)
    except (
        TypeError,
        ValueError,
        msgspec.DecodeError,
        msgspec.ValidationError,
    ):
        return False
    return current.enqueued_at == expected_enqueued_at


def _timeout_album(
    entry: GrabListEntry,
    request_id: int,
    reason: str,
    ctx: CratediggerContext,
    *,
    expected_enqueued_at: str,
    prepare_fn: Callable[..., HavePreparation] | None = None,
    enrich_fn: Callable[..., HaveEnrichment] | None = None,
) -> bool:
    """Cancel and reset only while the exact download incarnation is current."""
    db = ctx.pipeline_db_source._get_db()
    with db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_IMPORT,
        request_id,
    ) as acquired:
        if not acquired or not _owns_downloading_incarnation(
            db,
            request_id=request_id,
            expected_enqueued_at=expected_enqueued_at,
        ):
            return False

        cancel_and_delete(entry.files, ctx)

        total = len(entry.files)
        completed = sum(
            1
            for file in entry.files
            if file.status and file.status.state == "Completed, Succeeded"
        )

        dl_info = _build_download_info(entry)
        reason = _enrich_timeout_reason(reason, entry.files)
        transfer_detail = msgspec.to_builtins(
            _file_failure_details(entry.files),
        )

        logger.info(
            "DOWNLOAD TIMEOUT: %s - %s (%s/%s files done, reason=%s)",
            entry.artist,
            entry.title,
            completed,
            total,
            reason,
        )

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
            contributor_usernames=dl_info.contributor_usernames,
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
        return True


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
    key (slskd never expires removed history). Matching with the historical
    ``match_transfer`` (since renamed and made private as
    ``_match_transfer_all_history``) here — no attempt boundary — let a
    months-old
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
    failure mode). The initial ``get_downloading`` read is deliberately
    unguarded: a pipeline-DB failure propagates to
    ``lib/convergence.py``'s registered-step isolation (issue #1312;
    pinned in ``tests/test_slskd_sweep_exception_contracts.py``).
    The purge always still runs regardless (the pre-existing
    behavior). The write goes through the status-and-attempt-
    guarded ``update_download_state_if_downloading`` — mirroring the poll
    path's ownership guard — so a row a concurrent action moved out of
    ``downloading`` or replaced with a newer attempt is never rewritten.
    MUST be called before ``purge_completed_transfers``; that ordering is
    owned by the end-of-cycle registry in
    ``lib/convergence.py::CONVERGENCE_STEPS``.
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
            if _parse_event_timestamp(state.enqueued_at) is None:
                logger.warning(
                    "HARVEST: request %s has invalid enqueued_at witness "
                    "— excluding terminal transfer evidence",
                    request_id,
                )
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
                    request_id,
                    state.to_json(),
                    expected_enqueued_at=state.enqueued_at):
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
    state: ActiveDownloadState,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    process_album_fn: ProcessAlbumFn | None = None,
    cancellation_token: CancellationToken | None = None,
    owner_proof: ExecutionOwnerProof | None = None,
) -> CompletionResult:
    """Run exact-owner processing for a completed album.

    ``process_album_fn`` is an opt-in DI seam for tests that exercise the
    importer mapping without going through the full
    ``process_completed_album`` body. Defaults to the real production
    function.

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

    if state.processing_started_at is None or state.current_path is None:
        return CompletionDeferred(
            detail="processing_owner_handoff_incomplete",
        )

    try:
        return _process(
            entry,
            ctx,
            import_job_id=import_job_id,
            cancellation_token=cancellation_token,
            owner_proof=owner_proof,
        )
    except ExecutionCancelled:
        raise
    except Exception:
        logger.exception(f"Error processing completed download {entry.artist} - {entry.title} "
                         f"— will retry local processing next cycle")
        return CompletionDeferred(detail="unhandled_exception_during_local_processing")


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
            contributor_usernames=dl_info.contributor_usernames,
            filetype=dl_info.filetype or state.filetype,
            download_path=source_path,
            beets_detail=detail,
            outcome=outcome,
            error_message=error_message,
        ),
    )


def _enqueue_completed_processing(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: DownloadDB,
    ctx: CratediggerContext,
) -> ImportJob | None:
    """Commit exact processor ownership without touching the filesystem."""
    canonical_path = canonical_folder_for_row(
        entry,
        processing_albums_dir(ctx.cfg.processing_dir),
    )
    result = db.handoff_automation_import(
        request_id=request_id,
        expected_enqueued_at=state.enqueued_at,
        canonical_path=canonical_path,
        message=f"Automation import queued for {entry.artist} - {entry.title}",
    )
    if result.committed:
        assert result.job is not None
        logger.info(
            "Transferred request %s to automation import job %s",
            request_id,
            result.job.id,
        )
        return result.job
    logger.info(
        "Automation handoff rejected for request %s: %s",
        request_id,
        result.outcome,
    )
    return None


def _decode_valid_download_incarnations(
    rows: Sequence[AlbumRequestRow],
    *,
    phase: str,
) -> list[tuple[AlbumRequestRow, ActiveDownloadState]]:
    """Decode rows with valid enqueue witnesses, preserving DB order."""
    valid: list[tuple[AlbumRequestRow, ActiveDownloadState]] = []
    for row in rows:
        request_id = row["id"]
        raw_state = row.get("active_download_state")
        if raw_state is None:
            logger.warning(
                "POLL ADMISSION: request %s has no active_download_state "
                "during %s — excluding without reset",
                request_id,
                phase,
            )
            continue
        try:
            state = ActiveDownloadState.from_raw(raw_state)
        except Exception:
            logger.warning(
                "POLL ADMISSION: request %s has malformed "
                "active_download_state during %s — excluding without reset",
                request_id,
                phase,
                exc_info=True,
            )
            continue
        witness = state.enqueued_at
        if not witness:
            logger.warning(
                "POLL ADMISSION: request %s has an empty enqueued_at witness "
                "during %s — excluding without reset",
                request_id,
                phase,
            )
            continue
        if _parse_event_timestamp(witness) is None:
            logger.warning(
                "POLL ADMISSION: request %s has invalid enqueued_at witness "
                "%r during %s — excluding without reset",
                request_id,
                witness,
                phase,
            )
            continue
        valid.append((row, state))
    return valid


def _admit_download_incarnations(
    pre_snapshot: Sequence[tuple[AlbumRequestRow, ActiveDownloadState]],
    refreshed_rows: Sequence[AlbumRequestRow],
    *,
    refreshed_phase: str = "post-ingest refresh",
) -> list[tuple[AlbumRequestRow, ActiveDownloadState]]:
    """Admit only exact refreshed incarnations witnessed before the snapshot."""
    pre_snapshot_pairs = {
        (row["id"], state.enqueued_at)
        for row, state in pre_snapshot
    }
    admitted: list[tuple[AlbumRequestRow, ActiveDownloadState]] = []
    for row, state in _decode_valid_download_incarnations(
        refreshed_rows,
        phase=refreshed_phase,
    ):
        pair = (row["id"], state.enqueued_at)
        if pair not in pre_snapshot_pairs:
            logger.info(
                "POLL ADMISSION: request %s incarnation %r was not present "
                "before the transfer snapshot — excluding this cycle",
                row["id"],
                state.enqueued_at,
            )
            continue
        admitted.append((row, state))
    return admitted


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
    downloading_before_snapshot = db.get_downloading()
    pre_snapshot_incarnations = _decode_valid_download_incarnations(
        downloading_before_snapshot,
        phase="pre-snapshot read",
    )

    # One bulk snapshot for the entire poll cycle — avoids per-file API
    # calls. Fetched BEFORE event ingestion, deliberately: a transfer the
    # snapshot shows Completed finished before the snapshot, and therefore
    # before the ingest below — so its DownloadFileComplete event is in
    # the feed and the file reaches processing stamped. The reverse order
    # left a cycle-length race where same-cycle completions processed
    # unstamped.
    cycle_snapshot = None
    if pre_snapshot_incarnations:
        cycle_snapshot = _get_all_downloads_snapshot(
            ctx.slskd, purpose="poll cycle snapshot")

    # Issue #146: stamp authoritative local paths from slskd's
    # DownloadFileComplete events before processing. Runs even with no
    # downloading rows so the cursor keeps tracking the feed. An ingest
    # failure stamps nothing this cycle. Polling still publishes processor
    # ownership for a completed transfer; the processor then enforces the
    # event-stamped source-path contract.
    try:
        from lib.slskd_events import ingest_download_file_events
        ingest_result = ingest_download_file_events(db, ctx.slskd)
        logger.info(ingest_result.to_log_line())
    except Exception:
        logger.exception(
            "SLSKD EVENTS: ingest failed — nothing stamped this cycle; "
            "processor source-path validation remains authoritative")

    # Refresh after EVERY ingest outcome, including a raised ingest error.
    # Admission is the exact (request_id, enqueued_at text) pair that existed
    # before the transfer snapshot. Request ID alone would let a same-row B
    # attempt consume A's snapshot.
    try:
        refreshed = db.get_downloading()
    except Exception:
        logger.exception(
            "POLL ADMISSION: downloading-row refresh failed after event "
            "ingest — skipping all polling this cycle",
        )
        return
    downloading = _admit_download_incarnations(
        pre_snapshot_incarnations,
        refreshed,
    )

    if not downloading:
        return

    logger.info(f"Polling {len(downloading)} active download(s)...")

    if cycle_snapshot is None:
        logger.warning("Failed to get download snapshot — skipping poll cycle")
        return

    for row, state in downloading:
        request_id = row["id"]
        try:
            _poll_one_active_download(row, state, db, ctx, cycle_snapshot)
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
    row: AlbumRequestRow,
    state: ActiveDownloadState,
    db: DownloadDB,
    ctx: CratediggerContext,
    cycle_snapshot: list[DownloadUser],
) -> None:
    """Build poll facts, persist one reduced state, then dispatch one effect."""
    request_id = row["id"]

    file_snapshots: list[PollFileSnapshot] = []
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

    snapshot = PollCycleSnapshot(files=file_snapshots)
    now = datetime.now(UTC)
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

    reduced_state = result.state
    assert reduced_state is not None
    verdict = result.verdict
    if verdict.decision == PollCycleDecision.reset_missing_state:
        raise AssertionError(
            "admitted poll incarnation unexpectedly reduced as missing state",
        )
    state = reduced_state
    transfer_ids = {
        (file.username, file.filename): observation.transfer_id
        for file, observation in zip(state.files, snapshot.files, strict=False)
        if observation.transfer_id is not None
    }
    entry = _reconstruct_grab_list_entry(
        row,
        state,
        transfer_ids=transfer_ids,
    )

    # A retry-count increment is evidence that a retry was actually attempted,
    # not merely observed as eligible. Serialize the witnessed state write and
    # slskd effect under IMPORT so lock contention cannot consume retry budget.
    if verdict.decision == PollCycleDecision.retry_files:
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            request_id,
        ) as acquired:
            if not acquired or not _owns_downloading_incarnation(
                db,
                request_id=request_id,
                expected_enqueued_at=state.enqueued_at,
            ):
                return
            if not db.update_download_state_if_downloading(
                request_id,
                state.to_json(),
                expected_enqueued_at=state.enqueued_at,
            ):
                return
            if not _owns_downloading_incarnation(
                db,
                request_id=request_id,
                expected_enqueued_at=state.enqueued_at,
            ):
                return
            for retry_filename in verdict.files_to_retry:
                for file in entry.files:
                    if file.filename == retry_filename:
                        retries_used = file.retry or 0
                        logger.info(
                            "Re-enqueue failed file (%s/%s retries): %s",
                            retries_used,
                            MAX_FILE_RETRIES,
                            retry_filename,
                        )
                        requeue = slskd_do_enqueue(
                            file.username,
                            [{"filename": file.filename, "size": file.size}],
                            file.file_dir,
                            ctx,
                            request_id=request_id,
                            attempt_fp=attempt_fingerprint_of_files(
                                entry.files),
                            # A retry is still part of this exact attempt.
                            not_before=state.enqueued_at,
                        )
                        if not requeue:
                            logger.warning(
                                "Failed to re-enqueue file: %s",
                                retry_filename,
                            )
                        break
        return

    # Non-retry observations are persisted before their separately fenced
    # downstream action. Losing this CAS means a concurrent status transition
    # or newer attempt won; its state and effects take precedence.
    if not db.update_download_state_if_downloading(
        request_id,
        state.to_json(),
        expected_enqueued_at=state.enqueued_at,
    ):
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
            expected_enqueued_at=state.enqueued_at,
        )
        return

    if verdict.decision == PollCycleDecision.timeout_remote_queue:
        _timeout_album(
            entry,
            request_id,
            verdict.reason,
            ctx,
            expected_enqueued_at=state.enqueued_at,
        )
        return

    if verdict.decision == PollCycleDecision.complete:
        logger.info(f"Download complete: {entry.artist} - {entry.title}")
        _enqueue_completed_processing(entry, request_id, state, db, ctx)
        return

    if verdict.decision == PollCycleDecision.timeout_all_errored:
        _timeout_album(
            entry,
            request_id,
            verdict.reason,
            ctx,
            expected_enqueued_at=state.enqueued_at,
        )
        return

    if verdict.decision == PollCycleDecision.timeout_stalled:
        _timeout_album(
            entry,
            request_id,
            verdict.reason,
            ctx,
            expected_enqueued_at=state.enqueued_at,
        )
        return

    if verdict.decision != PollCycleDecision.in_progress:
        assert_never(verdict.decision)

    # Still in progress — log and continue to next album
    enqueued_at = datetime.fromisoformat(state.enqueued_at)
    if enqueued_at.tzinfo is None:
        enqueued_at = enqueued_at.replace(tzinfo=UTC)
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
    albums: list[AlbumRecord],
    search_and_queue: Callable[
        ...,
        tuple[dict[int, GrabListEntry], list[AlbumRecord], list[AlbumRecord]],
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

    logger.info(f"Failed to grab: {len(failed_grab)}")
    for album in failed_grab:
        logger.info(f"Album: {album.title} Artist: {album.artist_name}")

    count = len(failed_search) + len(failed_grab)
    for album in failed_search:
        logger.info(f"Search failed for Album: {album.title} - Artist: {album.artist_name}")
    for album in failed_grab:
        logger.info(f"Download failed for Album: {album.title} - Artist: {album.artist_name}")

    return count
