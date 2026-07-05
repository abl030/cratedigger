"""Download poll-state types + the cooldown/download-action reducers.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477),
refreshed onto main to carry ``ActiveDownloadFileState`` /
``ActiveDownloadState`` as ``msgspec.Struct`` (issue #467) instead of the
hand-rolled ``@dataclass`` with ``to_dict``/``from_dict``/``to_json``/
``from_json`` the original split carved. Pure move: every definition is
AST-identical to the current monolith.
"""

import enum
from dataclasses import dataclass, field
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
    # Authoritative post-rename local path from slskd's
    # DownloadFileComplete event (issue #146 phase 1). Persisted because
    # multi-file albums complete file-by-file across poll cycles while
    # each event is consumed exactly once.
    local_path: str | None = None


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
    # ``lib/download.py::_log_post_move_resume_blocked``.
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
        """Coerce a ``album_requests.active_download_state`` column value,
        whatever shape it currently arrives in, into a state.

        psycopg2 decodes JSONB to a ``dict``; raw SQL / re-serialized state
        arrives as a JSON ``str``; an already-decoded ``ActiveDownloadState``
        is returned unchanged (idempotent). Collapses the four identical
        ``from_dict(x) if isinstance(x, dict) else from_json(str(x))``
        call sites in ``lib/download.py``, ``lib/download_processing.py``,
        ``scripts/importer.py``, and ``lib/slskd_events.py`` (issue #510).

        Raises ``ValueError`` for anything else (including ``None``) —
        load-bearing for ``scripts/import_preview_worker.py``, which calls
        this directly on ``row.get("active_download_state")`` with no
        earlier falsy guard at one of its two call sites.
        """
        if isinstance(raw, ActiveDownloadState):
            return raw
        if isinstance(raw, dict):
            return ActiveDownloadState.from_dict(raw)
        if isinstance(raw, str):
            return ActiveDownloadState.from_json(raw)
        raise ValueError(
            "active_download_state must be a dict, JSON string, or "
            f"ActiveDownloadState, got {type(raw).__name__}"
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
    slskd_filetype: Optional[str] = None    # what slskd reported
    slskd_bitrate: Optional[int] = None
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
