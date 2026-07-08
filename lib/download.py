"""Download polling — the poll state machine and search orchestration.

All functions receive a CratediggerContext instead of reading
module-level globals. Split (issue #146 phase 3): staging /
materialization / validation-dispatch live in
lib/download_processing.py; slskd transfer helpers in
lib/slskd_transfers.py; event-feed ingestion in
lib/slskd_events.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from contextlib import AbstractContextManager
from typing import (Any, Callable, Protocol, TYPE_CHECKING, assert_never,
                    runtime_checkable)


from lib import download_processing
from lib.download_processing import (
    Completed,
    CompletionDeferred,
    CompletionDispatched,
    CompletionFailed,
    CompletionResult,
    Materialized,
    MaterializeFailed,
    MaterializeGuarded,
    MaterializeResult,
    _canonical_import_folder_path,
    _evaluate_staged_path_readiness,
    _materialize_processing_dir,
)
from lib.download_recovery import (
    classify_processing_path,
    reconcile_processing_current_path,
)
from lib.grab_list import GrabListEntry, DownloadFile
from lib.processing_paths import attempt_fingerprint, directory_has_entries
from lib.quality import (ActiveDownloadState, ActiveDownloadFileState,
                         CooldownConfig,
                         DownloadDecision,
                         decide_download_action,
                         extract_usernames)
from lib import transitions
from lib.dispatch import _build_download_info
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    ImportJob,
    automation_import_dedupe_key,
    automation_import_payload,
)
from lib.slskd_client import DownloadUser, TransferSnapshot
from lib.slskd_transfers import (
    _all_files_remotely_queued,
    _get_all_downloads_snapshot,
    cancel_and_delete,
    downloads_all_done,
    rederive_transfer_ids,
    slskd_do_enqueue,
    slskd_download_status,
)
from lib.staged_album import StagedAlbum

if TYPE_CHECKING:
    from lib.context import CratediggerContext
    from lib.pipeline_db import DownloadLogOutcome

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

    def get_downloading(self) -> list[dict[str, Any]]: ...

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def check_and_apply_cooldown(
        self, username: str, config: CooldownConfig | None = None,
    ) -> bool: ...

    def update_download_state(
        self, request_id: int, state_json: str,
    ) -> None: ...

    def update_download_state_current_path(
        self, request_id: int, current_path: str | None,
    ) -> None: ...

    def log_download(
        self,
        request_id: int,
        *,
        soulseek_username: str | None = None,
        filetype: str | None = None,
        outcome: DownloadLogOutcome | None = None,
        error_message: str | None = None,
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
    ) -> dict[str, Any] | None: ...

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
        beets_scenario: str,
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



# === GrabListEntry reconstruction from DB ===

def reconstruct_grab_list_entry(
    request: dict[str, Any],
    state: ActiveDownloadState,
) -> GrabListEntry:
    """Rebuild GrabListEntry from a DB row + persisted download state.

    Does NOT set slskd transfer IDs — those are ephemeral and must be
    re-derived from the live slskd API by the caller.
    """
    files = []
    for f in state.files:
        restored_status = _restored_terminal_status(
            f.last_state,
            f.bytes_transferred,
        )
        files.append(DownloadFile(
            filename=f.filename,
            id="",                  # Must be re-derived from slskd API
            file_dir=f.file_dir,
            username=f.username,
            size=f.size,
            disk_no=f.disk_no,
            disk_count=f.disk_count,
            retry=f.retry_count,
            bytes_transferred=f.bytes_transferred,
            last_state=f.last_state,
            status=restored_status,
            local_path=f.local_path,
        ))
    year = request.get("year")
    from lib.import_manifest import manifest_trace_summary
    logger.info(
        "MANIFEST-TRACE reconstruct request=%s %s current_path=%s",
        request["id"],
        manifest_trace_summary(files),
        state.current_path,
    )
    return GrabListEntry(
        album_id=request["id"],
        files=files,
        filetype=state.filetype,
        title=request["album_title"],
        artist=request["artist_name"],
        year=str(year) if year else "",
        mb_release_id=request.get("mb_release_id") or "",
        db_request_id=request["id"],
        db_source=request.get("source"),
        db_search_filetype_override=request.get("search_filetype_override"),
        db_target_format=request.get("target_format"),
        import_folder=state.current_path,
    )


def _restored_terminal_status(
    last_state: str | None,
    bytes_transferred: int,
) -> TransferSnapshot | None:
    """Rehydrate terminal slskd observations persisted in JSONB state.

    slskd's ``includeRemoved`` snapshot can stop exposing terminal transfer
    rows between poll cycles. Once we have seen a terminal state, keep that
    evidence actionable instead of downgrading it to an invisible transfer.
    """
    if not last_state or not last_state.startswith("Completed,"):
        return None
    return TransferSnapshot(state=last_state, bytes_transferred=bytes_transferred)



# === Async download polling ===

def _timeout_album(
    entry: GrabListEntry,
    request_id: int,
    reason: str,
    ctx: CratediggerContext,
) -> None:
    """Handle download timeout: cancel, log, reset to wanted."""
    cancel_and_delete(entry.files, ctx)

    total = len(entry.files)
    completed = sum(1 for f in entry.files
                    if f.status and f.status.state == "Completed, Succeeded")

    dl_info = _build_download_info(entry)

    logger.info(f"DOWNLOAD TIMEOUT: {entry.artist} - {entry.title} "
                f"({completed}/{total} files done, reason={reason})")

    db = ctx.pipeline_db_source._get_db()
    db.log_download(
        request_id=request_id,
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        outcome="timeout",
        error_message=reason,
    )
    for username in extract_usernames(entry.files):
        if db.check_and_apply_cooldown(username):
            ctx.cooled_down_users.add(username)
    transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
        ),
    )


def _persist_updated_download_state(
    db: DownloadDB,
    request_id: int,
    entry: GrabListEntry,
    state: ActiveDownloadState,
) -> None:
    """Persist retry counters or processing markers back to JSONB."""
    db.update_download_state(
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
    )


_NON_PROGRESS_STATES = {
    "",
    "Queued, Remotely",
    "Completed, Cancelled",
    "Completed, TimedOut",
    "Completed, Errored",
    "Completed, Rejected",
    "Completed, Aborted",
}


def _capture_download_progress(
    downloads: list[DownloadFile],
    state: ActiveDownloadState,
    now: datetime,
) -> bool:
    """Record byte/state progress from fresh slskd status snapshots.

    Returns True when any file made observable forward progress this cycle.
    """
    progress_made = False
    for file in downloads:
        if not file.status:
            continue

        current_state = file.status.state
        current_bytes = file.status.bytes_transferred
        previous_bytes = file.bytes_transferred or 0
        previous_state = file.last_state or ""

        if current_bytes > previous_bytes:
            progress_made = True
        elif current_state != previous_state and current_state not in _NON_PROGRESS_STATES:
            progress_made = True

        file.bytes_transferred = current_bytes
        file.last_state = current_state or file.last_state

    if progress_made:
        state.last_progress_at = now.isoformat()

    return progress_made


def _run_completed_processing(
    entry: GrabListEntry,
    request_id: int,
    state: ActiveDownloadState,
    db: DownloadDB,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    process_album_fn: "Callable[..., CompletionResult] | None" = None,
) -> CompletionResult:
    """Run or resume local post-download processing for a completed album.

    ``process_album_fn`` is an opt-in DI seam for tests that exercise the
    outer transition flow without going through the full
    ``process_completed_album`` body. Defaults to the real production
    function so callers in ``scripts/importer.py`` are unchanged.

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
            entry.import_folder = _canonical_import_folder_path(
                entry,
                ctx.cfg.slskd_download_dir,
            )
        state.processing_started_at = datetime.now(timezone.utc).isoformat()
        _persist_updated_download_state(db, request_id, entry, state)

    try:
        result = _process(
            entry,
            [],
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
        if refreshed and refreshed["status"] == "downloading":
            logger.info(f"  process_completed_album succeeded without "
                        f"setting status — setting imported")
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_imported(
                    from_status="downloading",
                ),
            )
        return result

    if isinstance(result, CompletionFailed):
        refreshed = db.get_request(request_id)
        if refreshed and refreshed["status"] == "downloading":
            logger.warning(f"  process_completed_album failed without "
                           f"setting status — resetting to wanted")
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                    attempt_type="download",
                ),
            )
        return result

    assert_never(result)


def _active_import_job_for_request(
    db: DownloadDB, request_id: int,
) -> dict[str, Any] | None:
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
) -> Any:
    """Submit completed-download processing to the shared import queue."""
    if state.processing_started_at is None:
        if entry.import_folder is None:
            entry.import_folder = _canonical_import_folder_path(
                entry,
                ctx.cfg.slskd_download_dir,
            )
        state.processing_started_at = datetime.now(timezone.utc).isoformat()
        _persist_updated_download_state(db, request_id, entry, state)
    if entry.import_folder is None:
        entry.import_folder = (
            state.current_path
            or _canonical_import_folder_path(entry, ctx.cfg.slskd_download_dir)
        )
    staged_album = StagedAlbum.from_entry(
        entry,
        default_path=_canonical_import_folder_path(
            entry,
            ctx.cfg.slskd_download_dir,
        ),
    )
    materialized = _materialize_processing_dir(entry, staged_album, ctx)
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
            db.log_download(
                request_id=request_id,
                soulseek_username=dl_info.username,
                filetype=dl_info.filetype,
                outcome="failed",
                error_message=detail,
            )
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                    attempt_type="download",
                ),
            )
            return None
        logger.warning(
            "Completed download for request %s could not be materialized "
            "for import preview; leaving it for the next poll cycle",
            request_id,
        )
        return None
    entry.import_folder = staged_album.current_path
    state.current_path = staged_album.current_path
    _persist_updated_download_state(db, request_id, entry, state)
    job = db.enqueue_import_job(
        IMPORT_JOB_AUTOMATION,
        request_id=request_id,
        dedupe_key=automation_import_dedupe_key(request_id),
        payload=automation_import_payload(),
        message=f"Automation import queued for {entry.artist} - {entry.title}",
    )
    if getattr(job, "deduped", False):
        logger.info(
            "Automation import already queued/running for request %s "
            "(job %s)",
            request_id,
            getattr(job, "id", "?"),
        )
    else:
        logger.info(
            "Queued automation import for request %s as job %s",
            request_id,
            getattr(job, "id", "?"),
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
    (``lib.download_processing._evaluate_staged_path_readiness``, issue
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
    transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
        ),
    )
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
    row: dict[str, Any],
    db: DownloadDB,
    ctx: CratediggerContext,
    cycle_snapshot: list[DownloadUser],
) -> None:
    """Process one ``downloading`` row.

    Extracted from ``poll_active_downloads`` so the per-row try/except
    guard at the call site is the single seam where unhandled
    exceptions get contained. Inside, ``return`` has the same semantics
    as the original ``continue`` had inline.
    """
    request_id = row["id"]
    raw_state = row.get("active_download_state")
    if not raw_state:
        # Crash recovery: downloading with no state means process_completed_album
        # crashed on a previous run. Reset to wanted so it gets re-searched.
        logger.error(f"Downloading album {request_id} has no active_download_state — "
                     f"resetting to wanted")
        transitions.finalize_request(
            db,
            request_id,
            transitions.RequestTransition.to_wanted(
                from_status="downloading",
            ),
        )
        return

    state = ActiveDownloadState.from_raw(raw_state)
    active_import_job = _active_import_job_for_request(db, request_id)
    if active_import_job is not None:
        job_id = (
            active_import_job.get("id")
            if isinstance(active_import_job, dict)
            else getattr(active_import_job, "id", "?")
        )
        job_status = (
            active_import_job.get("status")
            if isinstance(active_import_job, dict)
            else getattr(active_import_job, "status", "?")
        )
        logger.info(
            "Request %s is waiting on importer job %s (%s)",
            request_id,
            job_id,
            job_status,
        )
        return
    if state.processing_started_at is not None:
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
        if recovery_decision.blocked_reason == "multiple_populated_paths":
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
        if recovery_decision.blocked_reason == "legacy_shared_only":
            logger.error(
                "LEGACY STAGED RESUME BLOCKED: request_id=%s %s - %s "
                "persisted current_path=%s could not be resumed, "
                "canonical_path=%s has no files, "
                "and staged_path=%s is ambiguous across editions. "
                "Manual recovery is required.",
                request_id,
                row["artist_name"],
                row["album_title"],
                state.current_path,
                recovery_decision.canonical_path,
                recovery_decision.legacy_shared_path,
            )
            return
        assert recovery_decision.selected_location is not None
        selected_path = recovery_decision.selected_location.path
        if selected_path != state.current_path:
            state.current_path = selected_path
            db.update_download_state_current_path(
                request_id,
                state.current_path,
            )
    entry = reconstruct_grab_list_entry(row, state)

    if state.processing_started_at is not None:
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

    # Re-derive transfer IDs from pre-fetched snapshot
    if not rederive_transfer_ids(
        entry,
        ctx.slskd,
        snapshot=cycle_snapshot,
        not_before=state.enqueued_at,
    ):
        logger.warning(f"API error re-deriving transfers for {entry.artist} - {entry.title} "
                       f"— will retry next cycle")
        return

    enqueued_at = datetime.fromisoformat(state.enqueued_at)
    if enqueued_at.tzinfo is None:
        enqueued_at = enqueued_at.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    elapsed_seconds = (now - enqueued_at).total_seconds()

    # Check if all transfers have vanished (slskd restart, user offline).
    # A restored terminal status from a previous poll is still visible
    # evidence; do not erase it just because slskd no longer lists the
    # removed transfer row.
    all_vanished = all(f.id == "" and f.status is None for f in entry.files)
    if all_vanished:
        if elapsed_seconds < 60:
            logger.info(
                "Request %s has fresh planned ownership but no visible "
                "slskd transfers yet; deferring vanished-transfer reset",
                request_id,
            )
            return
        _timeout_album(entry, request_id, "all transfers vanished from slskd", ctx)
        return

    # Mark files with vanished transfers as errored. Preserve restored
    # terminal statuses (Completed, Rejected/Errored/etc.) from previous
    # poll cycles so the reducer can report the real terminal failure.
    for f in entry.files:
        if f.id == "" and f.status is None:
            f.status = TransferSnapshot(state="Completed, Errored")

    # Track total album age separately from stall/progress timing.
    # Poll live status only for transfers that are still active in slskd.
    files_requiring_status = [
        f for f in entry.files
        if f.id and not (f.status and f.status.state.startswith("Completed,"))
    ]
    if files_requiring_status and not slskd_download_status(
            files_requiring_status, snapshot=cycle_snapshot):
        logger.warning(f"API error polling {entry.artist} - {entry.title} — "
                      f"will retry next cycle")
        return

    album_done, problems, queued = downloads_all_done(entry.files)
    statusful_files = [f for f in entry.files if f.status is not None]
    state_changed = _capture_download_progress(statusful_files, state, now)

    all_remote_queued = _all_files_remotely_queued(entry.files, queued)
    error_filenames = [f.filename for f in problems] if problems is not None else None
    file_retries = {f.filename: (f.retry or 0) for f in entry.files}

    progress_at = state.last_progress_at or state.enqueued_at
    idle_seconds = (now - datetime.fromisoformat(progress_at)).total_seconds()

    verdict = decide_download_action(
        album_done=album_done,
        error_filenames=error_filenames,
        total_files=len(entry.files),
        all_remote_queued=all_remote_queued,
        elapsed_seconds=elapsed_seconds,
        idle_seconds=idle_seconds,
        remote_queue_timeout=ctx.cfg.remote_queue_timeout,
        stalled_timeout=ctx.cfg.stalled_timeout,
        file_retries=file_retries,
        max_file_retries=MAX_FILE_RETRIES,
        processing_started=False,
    )

    if verdict.decision == DownloadDecision.timeout_remote_queue:
        _timeout_album(entry, request_id, verdict.reason, ctx)
        return

    if verdict.decision == DownloadDecision.complete:
        logger.info(f"Download complete: {entry.artist} - {entry.title}")
        _enqueue_completed_processing(entry, request_id, state, db, ctx)
        return

    if verdict.decision == DownloadDecision.timeout_all_errored:
        _timeout_album(entry, request_id, verdict.reason, ctx)
        return

    if verdict.decision == DownloadDecision.timeout_stalled:
        _timeout_album(entry, request_id, verdict.reason, ctx)
        return

    if verdict.decision == DownloadDecision.retry_files:
        for retry_filename in verdict.files_to_retry:
            for df in entry.files:
                if df.filename == retry_filename:
                    retries_used = (df.retry or 0) + 1
                    df.retry = retries_used
                    logger.info(f"Re-enqueue failed file "
                                f"({retries_used}/{MAX_FILE_RETRIES} retries): "
                                f"{retry_filename}")
                    # Find the problem file for username/size/dir
                    file = next((f for f in entry.files if f.filename == retry_filename), None)
                    if file:
                        requeue = slskd_do_enqueue(
                            file.username,
                            [{"filename": file.filename, "size": file.size}],
                            file.file_dir, ctx)
                        state_changed = True
                        if requeue:
                            df.id = requeue[0].id
                            df.bytes_transferred = 0
                            df.last_state = None
                            state.last_progress_at = now.isoformat()
                        else:
                            logger.warning(f"Failed to re-enqueue file: {retry_filename}")
                    break

        refreshed = db.get_request(request_id)
        if refreshed and refreshed["status"] != "downloading":
            return

    # In progress — persist state and log
    refreshed = db.get_request(request_id)
    if refreshed and refreshed["status"] != "downloading":
        return
    if state_changed:
        _persist_updated_download_state(db, request_id, entry, state)

    # Still in progress — log and continue to next album
    files_done = sum(1 for f in entry.files
                    if f.status and f.status.state == "Completed, Succeeded")
    logger.info(f"In progress: {entry.artist} - {entry.title} "
                f"({files_done}/{len(entry.files)} files, "
                f"{elapsed_seconds/60:.1f}min elapsed)")


# === Top-level orchestration ===

def grab_most_wanted(albums: list[Any],
                     search_and_queue: Callable[..., tuple[dict, list, list]],
                     ctx: CratediggerContext) -> int:
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
            transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_downloading(
                    from_status="wanted",
                    state_json=state.to_json(),
                ),
            )
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
