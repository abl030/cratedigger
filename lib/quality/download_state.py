"""Pure download-state types and reducers.

``reduce_poll_cycle`` owns the complete asynchronous poll transition from
persisted state plus caller-supplied observations to a new whole state and one
typed verdict. This module performs no DB, slskd, filesystem, clock, logging,
or callback work; ``lib.download`` builds snapshots, persists the returned
state under its ownership guard, and dispatches verdict side effects.

The same module also owns the narrower historical download-action and uploader
cooldown decisions, along with the JSONB wire types used to persist active
download evidence.
"""

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import msgspec

from lib.quality.evidence_types import V0ProbeEvidence


# --- Download state reducer (pure decision for async poller) ---

class DownloadDecision(enum.Enum):
    """High-level decision from the download state reducer."""
    in_progress = "in_progress"
    complete = "complete"
    retry_files = "retry_files"
    timeout_remote_queue = "timeout_remote_queue"
    timeout_stalled = "timeout_stalled"
    timeout_all_errored = "timeout_all_errored"
    processing = "processing"


@dataclass(frozen=True)
class DownloadVerdict:
    """Result of decide_download_action — typed decision for the poller."""
    decision: DownloadDecision
    files_to_retry: list[str] = field(default_factory=list)
    reason: str = ""


class PollCycleDecision(enum.Enum):
    """One side effect (or deliberate wait) selected for a poll cycle."""

    reset_missing_state = "reset_missing_state"
    wait_import_job = "wait_import_job"
    wait_processing_recovery = "wait_processing_recovery"
    wait_fresh_vanished = "wait_fresh_vanished"
    timeout_vanished = "timeout_vanished"
    in_progress = "in_progress"
    complete = "complete"
    retry_files = "retry_files"
    timeout_remote_queue = "timeout_remote_queue"
    timeout_stalled = "timeout_stalled"
    timeout_all_errored = "timeout_all_errored"
    processing = "processing"


@dataclass(frozen=True)
class PollCycleVerdict:
    """Side-effect instruction returned by :func:`reduce_poll_cycle`."""

    decision: PollCycleDecision
    files_to_retry: list[str] = field(default_factory=list)
    reason: str = ""
    import_job_id: int | None = None
    import_job_status: str | None = None


@dataclass(frozen=True)
class PollFileSnapshot:
    """Current-cycle slskd facts for one persisted file, in file order."""

    transfer_id: str | None = None
    state: str | None = None
    bytes_transferred: int = 0
    exception: str | None = None


@dataclass(frozen=True)
class PollCycleSnapshot:
    """All impure observations supplied to the pure poll-cycle reducer."""

    files: list[PollFileSnapshot] = field(default_factory=list)
    active_import_job_id: int | None = None
    active_import_job_status: str | None = None
    processing_current_path: str | None = None
    processing_blocked_reason: str | None = None
    completion_current_path: str | None = None


@dataclass(frozen=True)
class PollCycleConfig:
    """Scalar policy facts needed by the poll-cycle reducer."""

    remote_queue_timeout: int
    stalled_timeout: int
    max_file_retries: int
    vanished_grace_seconds: int = 60


@dataclass(frozen=True)
class PollCycleResult:
    """The complete state to persist and the one effect to dispatch."""

    state: "ActiveDownloadState | None"
    verdict: PollCycleVerdict


# --- User cooldown system (issue #39) ---

@dataclass(frozen=True)
class CooldownConfig:
    """Tunables for global user cooldown system.

    All cooldown thresholds and durations live here so they're trivial to tune.
    """
    failure_threshold: int = 5
    cooldown_days: int = 3
    failure_outcomes: frozenset[str] = frozenset({"timeout", "failed", "rejected"})
    lookback_window: int = 5


def should_cooldown(outcomes: list[str],
                    config: CooldownConfig = CooldownConfig()) -> bool:
    """Decide whether a user should be put on cooldown.

    Args:
        outcomes: Recent download outcomes for this user, newest first.
        config: Tunable thresholds.

    Returns True if the first `lookback_window` outcomes are all failures.
    """
    window = outcomes[:config.lookback_window]
    if len(window) < config.failure_threshold:
        return False
    return all(o in config.failure_outcomes for o in window)


def decide_download_action(
    *,
    album_done: bool,
    error_filenames: list[str] | None,
    total_files: int,
    all_remote_queued: bool,
    elapsed_seconds: float,
    idle_seconds: float,
    remote_queue_timeout: int,
    stalled_timeout: int,
    file_retries: dict[str, int],
    max_file_retries: int,
    processing_started: bool,
) -> DownloadVerdict:
    """Pure download state reducer — no I/O, no DB, no slskd.

    Takes a snapshot of the download state and returns a typed decision
    that the poller acts on.
    """
    if processing_started:
        return DownloadVerdict(DownloadDecision.processing)

    if album_done and error_filenames is None:
        return DownloadVerdict(DownloadDecision.complete)

    # Remote queue timeout: all files waiting on peer, total elapsed exceeded
    if all_remote_queued and elapsed_seconds >= remote_queue_timeout:
        return DownloadVerdict(
            DownloadDecision.timeout_remote_queue,
            reason=f"remote_queue_timeout {remote_queue_timeout}s exceeded")

    # Error handling
    if error_filenames is not None:
        if len(error_filenames) == total_files:
            return DownloadVerdict(
                DownloadDecision.timeout_all_errored,
                reason=f"all {total_files} files errored")

        # Check which files can be retried
        files_to_retry = []
        for fn in error_filenames:
            retries = file_retries.get(fn, 0)
            if retries >= max_file_retries:
                return DownloadVerdict(
                    DownloadDecision.timeout_stalled,
                    reason=f"file exceeded retry limit after "
                           f"{max_file_retries} retries: {fn}")
            files_to_retry.append(fn)

        if files_to_retry:
            return DownloadVerdict(
                DownloadDecision.retry_files,
                files_to_retry=files_to_retry)

    # Stall detection (only when not all remotely queued)
    if not all_remote_queued and idle_seconds >= stalled_timeout:
        return DownloadVerdict(
            DownloadDecision.timeout_stalled,
            reason=f"no download progress for {idle_seconds:.0f}s "
                   f"(stalled_timeout {stalled_timeout}s)")

    return DownloadVerdict(DownloadDecision.in_progress)


@dataclass(frozen=True)
class SpectralMeasurement:
    """One spectral analysis result pair."""
    grade: Optional[str] = None
    bitrate_kbps: Optional[int] = None

    @staticmethod
    def from_parts(grade: Optional[str], bitrate_kbps: Optional[int]) -> "SpectralMeasurement | None":
        """Build a measurement when any spectral data exists, else None."""
        if grade is None and bitrate_kbps is None:
            return None
        return SpectralMeasurement(grade=grade, bitrate_kbps=bitrate_kbps)


# ---------------------------------------------------------------------------
# Download info — typed replacement for the untyped dl_info dict
# ---------------------------------------------------------------------------

class ActiveDownloadFileState(msgspec.Struct, omit_defaults=True):
    """Per-file state persisted for active downloads.

    Wire boundary: the ``album_requests.active_download_state`` JSONB column
    (nested under ``ActiveDownloadState.files``). ``omit_defaults=True``
    reproduces the pre-refactor "absent key when None" shape for the optional
    fields (``disk_no``, ``disk_count``, ``last_state``, ``local_path``). The
    hand-rolled encoder additionally always emitted ``retry_count`` and
    ``bytes_transferred`` even at ``0``; those are now omitted at ``0`` and
    the strict decoder restores the ``0`` default — lossless both ways.
    See issue #467.
    """
    username: str
    filename: str           # Full soulseek path (backslashes)
    file_dir: str           # Download directory on source user's system
    size: int               # File size in bytes
    disk_no: int | None = None
    disk_count: int | None = None
    retry_count: int = 0
    bytes_transferred: int = 0
    last_state: str | None = None
    # slskd's real per-transfer failure reason (issue #564), e.g.
    # "Transfer rejected: Banned", "Read error: Connection reset by
    # peer". Persisted alongside ``last_state`` so a download-failure
    # message can name the actual cause instead of a generic timeout.
    last_exception: str | None = None
    # Authoritative post-rename local path from slskd's
    # DownloadFileComplete event (issue #146 phase 1). Persisted because
    # multi-file albums complete file-by-file across poll cycles while
    # each event is consumed exactly once.
    local_path: str | None = None


class FileFailureDetail(msgspec.Struct):
    """One file's failure detail, audited on a download-timeout
    ``download_log`` row (issue #564 C7).

    Wire boundary: ``download_log.transfer_detail`` JSONB (migration
    043). ``summarize_file_failures`` (``lib/download.py``) already
    composes a deduplicated, human-readable summary for the operator-
    facing ``error_message`` column; this Struct is the full per-file
    detail behind that summary — which peer, which file, the exact
    terminal state/exception, bytes transferred, and retry count —
    queryable without re-deriving it from ``active_download_state``
    history that no longer exists once the request self-heals back to
    ``wanted``.
    """
    username: str
    filename: str
    last_state: str | None = None
    last_exception: str | None = None
    bytes_transferred: int = 0
    retry_count: int = 0


class ActiveDownloadState(msgspec.Struct, omit_defaults=True):
    """State persisted to DB for an album being actively downloaded.

    Wire boundary: the ``album_requests.active_download_state`` JSONB column.
    Decode happens at exactly one site — ``from_dict`` / ``from_json``
    (``msgspec.convert`` / ``msgspec.json.decode``) — which strict-validates
    field types, so int-vs-str drift raises ``msgspec.ValidationError``
    instead of being silently coerced (as the old ``int(d["size"])`` decoder
    did). ``omit_defaults=True`` reproduces the old "absent key when None"
    shape for the optional timestamp fields; ``current_path`` is now omitted
    when ``None`` (the hand-rolled encoder emitted it as ``null``) — the
    strict decoder restores ``None`` either way. See issue #467.
    """
    filetype: str                         # "flac", "mp3 v0", etc.
    enqueued_at: str                      # ISO8601 UTC timestamp
    files: list[ActiveDownloadFileState]
    last_progress_at: str | None = None
    processing_started_at: str | None = None
    # Set immediately before ``run_import_one(...)`` is invoked on the
    # auto-import path. Distinguishes "files moved to staged path but
    # subprocess never launched" (None — safe to retry) from "subprocess
    # may already have written to beets" (set — manual recovery required).
    # See ``docs/advisory-locks.md`` and the resume-block guards in
    # ``lib/download_materialization.py::_log_post_move_resume_blocked``.
    import_subprocess_started_at: str | None = None
    current_path: str | None = None

    def to_json(self) -> str:
        return msgspec.json.encode(self).decode()

    @staticmethod
    def from_dict(d: dict[str, object]) -> "ActiveDownloadState":
        return msgspec.convert(d, type=ActiveDownloadState)

    @staticmethod
    def from_json(s: str) -> "ActiveDownloadState":
        return msgspec.json.decode(s, type=ActiveDownloadState)

    @staticmethod
    def from_raw(raw: object) -> "ActiveDownloadState":
        """Coerce an ``album_requests.active_download_state`` column value,
        whatever shape it arrives in, into a state.

        psycopg2 decodes JSONB to a ``dict``; raw SQL / re-serialized state
        arrives as a JSON ``str``. Collapses the identical
        ``from_dict(x) if isinstance(x, dict) else from_json(str(x))`` dance
        at every call site that reads this column — ``lib/download.py``,
        ``lib/download_materialization.py``, ``lib/slskd_events.py``,
        ``scripts/importer.py``, and both sites in
        ``scripts/import_preview_worker.py`` (issue #510).

        Raises ``ValueError`` for anything else (including ``None``) —
        load-bearing for ``scripts/import_preview_worker.py``, which calls
        this directly on ``row.get("active_download_state")`` with no
        earlier falsy guard at one of its two call sites.
        """
        if isinstance(raw, dict):
            return ActiveDownloadState.from_dict(raw)
        if isinstance(raw, str):
            return ActiveDownloadState.from_json(raw)
        raise ValueError(
            "active_download_state must be a dict or JSON string, "
            f"got {type(raw).__name__}"
        )


_TERMINAL_ERROR_STATES = frozenset({
    "Completed, Cancelled",
    "Completed, TimedOut",
    "Completed, Errored",
    "Completed, Rejected",
    "Completed, Aborted",
})

_NON_PROGRESS_STATES = frozenset({
    "",
    "Queued, Remotely",
    *_TERMINAL_ERROR_STATES,
})


def _copy_download_file_state(
    file: ActiveDownloadFileState,
    *,
    retry_count: int | None = None,
    bytes_transferred: int | None = None,
    last_state: str | None = None,
    last_exception: str | None = None,
) -> ActiveDownloadFileState:
    return ActiveDownloadFileState(
        username=file.username,
        filename=file.filename,
        file_dir=file.file_dir,
        size=file.size,
        disk_no=file.disk_no,
        disk_count=file.disk_count,
        retry_count=(file.retry_count if retry_count is None else retry_count),
        bytes_transferred=(
            file.bytes_transferred
            if bytes_transferred is None
            else bytes_transferred
        ),
        last_state=file.last_state if last_state is None else last_state,
        last_exception=(
            file.last_exception
            if last_exception is None
            else last_exception
        ),
        local_path=file.local_path,
    )


def _copy_download_state(
    state: ActiveDownloadState,
    *,
    files: list[ActiveDownloadFileState] | None = None,
    last_progress_at: str | None = None,
    processing_started_at: str | None = None,
    current_path: str | None = None,
) -> ActiveDownloadState:
    return ActiveDownloadState(
        filetype=state.filetype,
        enqueued_at=state.enqueued_at,
        files=state.files if files is None else files,
        last_progress_at=(
            state.last_progress_at
            if last_progress_at is None
            else last_progress_at
        ),
        processing_started_at=(
            state.processing_started_at
            if processing_started_at is None
            else processing_started_at
        ),
        import_subprocess_started_at=state.import_subprocess_started_at,
        current_path=state.current_path if current_path is None else current_path,
    )


def _datetime_from_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def reduce_poll_cycle(
    persisted_state: ActiveDownloadState | None,
    snapshot: PollCycleSnapshot,
    now: datetime,
    cfg: PollCycleConfig,
) -> PollCycleResult:
    """Reduce one poll cycle without I/O, mutation, clocks, or callbacks."""

    if persisted_state is None:
        return PollCycleResult(
            state=None,
            verdict=PollCycleVerdict(PollCycleDecision.reset_missing_state),
        )

    if snapshot.active_import_job_id is not None:
        return PollCycleResult(
            state=persisted_state,
            verdict=PollCycleVerdict(
                PollCycleDecision.wait_import_job,
                import_job_id=snapshot.active_import_job_id,
                import_job_status=snapshot.active_import_job_status,
            ),
        )

    if persisted_state.processing_started_at is not None:
        if snapshot.processing_blocked_reason is not None:
            return PollCycleResult(
                state=persisted_state,
                verdict=PollCycleVerdict(
                    PollCycleDecision.wait_processing_recovery,
                    reason=snapshot.processing_blocked_reason,
                ),
            )
        recovered_state = _copy_download_state(
            persisted_state,
            current_path=(
                snapshot.processing_current_path
                if snapshot.processing_current_path is not None
                else persisted_state.current_path
            ),
        )
        return PollCycleResult(
            state=recovered_state,
            verdict=PollCycleVerdict(PollCycleDecision.processing),
        )

    if len(snapshot.files) != len(persisted_state.files):
        raise ValueError(
            "poll snapshot file count must match persisted state: "
            f"{len(snapshot.files)} != {len(persisted_state.files)}"
        )

    missing = [
        observation.transfer_id is None
        and not (
            file.last_state is not None
            and file.last_state.startswith("Completed,")
        )
        for file, observation in zip(persisted_state.files, snapshot.files)
    ]
    elapsed_seconds = (
        now - _datetime_from_iso(persisted_state.enqueued_at)
    ).total_seconds()
    if all(missing):
        decision = (
            PollCycleDecision.wait_fresh_vanished
            if elapsed_seconds < cfg.vanished_grace_seconds
            else PollCycleDecision.timeout_vanished
        )
        return PollCycleResult(
            state=persisted_state,
            verdict=PollCycleVerdict(decision),
        )

    observed_progress = False
    new_files: list[ActiveDownloadFileState] = []
    for file, observation, vanished in zip(
        persisted_state.files,
        snapshot.files,
        missing,
    ):
        if vanished:
            current_state = "Completed, Errored"
            current_bytes = file.bytes_transferred
            current_exception = file.last_exception
        elif observation.transfer_id is None:
            current_state = file.last_state
            current_bytes = file.bytes_transferred
            current_exception = file.last_exception
        else:
            current_state = observation.state or file.last_state
            current_bytes = observation.bytes_transferred
            current_exception = observation.exception or file.last_exception

        if current_bytes > file.bytes_transferred:
            observed_progress = True
        elif (
            current_state != (file.last_state or "")
            and (current_state or "") not in _NON_PROGRESS_STATES
        ):
            observed_progress = True

        new_files.append(_copy_download_file_state(
            file,
            bytes_transferred=current_bytes,
            last_state=current_state,
            last_exception=current_exception,
        ))

    new_state = _copy_download_state(
        persisted_state,
        files=new_files,
        last_progress_at=(
            now.isoformat()
            if observed_progress
            else persisted_state.last_progress_at
        ),
    )
    states = [file.last_state for file in new_files]
    album_done = bool(new_files) and all(
        state == "Completed, Succeeded" for state in states
    )
    error_filenames = [
        file.filename
        for file in new_files
        if file.last_state in _TERMINAL_ERROR_STATES
    ]
    all_remote_queued = bool(new_files) and all(
        state == "Queued, Remotely" for state in states
    )
    progress_at = new_state.last_progress_at or new_state.enqueued_at
    idle_seconds = (now - _datetime_from_iso(progress_at)).total_seconds()
    action = decide_download_action(
        album_done=album_done,
        error_filenames=error_filenames or None,
        total_files=len(new_files),
        all_remote_queued=all_remote_queued,
        elapsed_seconds=elapsed_seconds,
        idle_seconds=idle_seconds,
        remote_queue_timeout=cfg.remote_queue_timeout,
        stalled_timeout=cfg.stalled_timeout,
        file_retries={file.filename: file.retry_count for file in new_files},
        max_file_retries=cfg.max_file_retries,
        processing_started=False,
    )
    decision = PollCycleDecision(action.decision.value)

    if decision == PollCycleDecision.retry_files:
        retry_names = set(action.files_to_retry)
        new_state = _copy_download_state(
            new_state,
            files=[
                _copy_download_file_state(
                    file,
                    retry_count=file.retry_count + 1,
                )
                if file.filename in retry_names
                else file
                for file in new_state.files
            ],
        )
    elif decision == PollCycleDecision.complete:
        new_state = _copy_download_state(
            new_state,
            processing_started_at=now.isoformat(),
            current_path=(
                snapshot.completion_current_path
                if snapshot.completion_current_path is not None
                else new_state.current_path
            ),
        )

    return PollCycleResult(
        state=new_state,
        verdict=PollCycleVerdict(
            decision,
            files_to_retry=action.files_to_retry,
            reason=action.reason,
        ),
    )


@dataclass
class DownloadInfo:
    """Audio quality metadata extracted from downloaded files.

    Replaces the untyped dl_info dict that was passed through cratedigger.py,
    album_source.py, and pipeline_db.py. Every field that ends up in
    download_log has a typed slot here.
    """
    # Soulseek source
    username: Optional[str] = None
    filetype: Optional[str] = None
    bitrate: Optional[int] = None           # bps (e.g. 320000)
    sample_rate: Optional[int] = None
    bit_depth: Optional[int] = None
    is_vbr: Optional[bool] = None
    # Conversion tracking
    was_converted: bool = False
    original_filetype: Optional[str] = None
    # Quality verification
    slskd_filetype: Optional[str] = None    # captured source filetype
    actual_filetype: Optional[str] = None   # after conversion
    actual_min_bitrate: Optional[int] = None
    # Spectral analysis
    download_spectral: SpectralMeasurement | None = None
    current_spectral: SpectralMeasurement | None = None
    existing_min_bitrate: Optional[int] = None
    # Verified lossless override (from import_one.py)
    verified_lossless_override: Optional[bool] = None
    # Full import_one.py result (JSON string)
    import_result: Optional[str] = None
    # Full validation result (JSON string)
    validation_result: Optional[str] = None
    # Final format on disk after verified-lossless target conversion
    final_format: Optional[str] = None
    # V0 probe evidence
    v0_probe: Optional["V0ProbeEvidence"] = None
    existing_v0_probe: Optional["V0ProbeEvidence"] = None
