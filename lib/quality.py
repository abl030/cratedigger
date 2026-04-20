"""Quality decision logic for the download pipeline.

Pure functions — no database, no filesystem, no external dependencies.
Used by cratedigger.py and import_one.py, tested directly against real audio fixtures.
"""

import configparser
import enum
import json
import re
from dataclasses import dataclass, field, asdict
from enum import IntEnum, StrEnum
from typing import Any, Literal, Optional

import msgspec

QUALITY_UPGRADE_TIERS = "lossless,mp3 v0,mp3 320"
QUALITY_LOSSLESS = "lossless"

_UUID_RE = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$", re.IGNORECASE)
_NUMERIC_RE = re.compile(r"^\d+$")


def detect_release_source(id_str: str) -> Literal["musicbrainz", "discogs", "unknown"]:
    """Detect whether a release ID is MusicBrainz (UUID) or Discogs (numeric)."""
    if not id_str:
        return "unknown"
    if _UUID_RE.match(id_str):
        return "musicbrainz"
    if _NUMERIC_RE.match(id_str):
        return "discogs"
    return "unknown"

# Deprecated aliases — keep for old code that references them
QUALITY_FLAC_ONLY = QUALITY_LOSSLESS


def search_tiers(search_filetype_override: str | None,
                 config_allowed: list[str]) -> tuple[list[str], bool]:
    """Return (filetypes_to_search, allow_catch_all) from a search_filetype_override.

    NULL override = use global config + allow catch-all fallback.
    Any CSV override = search exactly those tiers, no catch-all.
    """
    if not search_filetype_override:
        return list(config_allowed), True
    return [t.strip() for t in search_filetype_override.split(",")], False


def effective_search_tiers(
    search_filetype_override: str | None,
    target_format: str | None,
    config_allowed: list[str],
) -> tuple[list[str], bool]:
    """Compute effective search tiers merging both override sources.

    Priority: search_filetype_override > target_format > config defaults.
    """
    if search_filetype_override:
        return search_tiers(search_filetype_override, config_allowed)
    if target_format:
        return search_tiers(target_format, config_allowed)
    return search_tiers(None, config_allowed)


def should_clear_lossless_search_override(
    *,
    new_target_format: str | None,
    old_target_format: str | None,
    search_filetype_override: str | None,
) -> bool:
    """Should changing intent clear a stale lossless-only search override?

    This only clears the transient override when the user is explicitly
    turning off a previously requested lossless-on-disk intent.
    """
    old_keep_lossless = old_target_format in ("flac", "lossless")
    return (
        new_target_format is None
        and old_keep_lossless
        and search_filetype_override == QUALITY_LOSSLESS
    )


def resolve_user_requeue_override(existing_override: str | None) -> str:
    """Pick ``search_filetype_override`` for a user-initiated requeue.

    Preserves a stricter existing override — e.g. ``"lossless"`` set by the
    quality gate after a CBR 320 import — so user actions (Upgrade button,
    status reset back to wanted, ban-source) don't re-open search tiers the
    gate intentionally closed. Falls back to :data:`QUALITY_UPGRADE_TIERS`
    only when no override is currently set.

    Without this, clicking Upgrade on an imported album would overwrite a
    gate-narrowed ``"lossless"`` with the full ``"lossless,mp3 v0,mp3 320"``
    ladder, re-enqueuing MP3 320 sources that the pipeline has already
    established can't produce an upgrade — each one gets rejected as a
    downgrade and the user sees a loop.
    """
    return existing_override or QUALITY_UPGRADE_TIERS


QUALITY_MIN_BITRATE_KBPS = 210  # V0 floor — below this triggers upgrade
TRANSCODE_MIN_BITRATE_KBPS = 210  # V0 from genuine lossless is always >= this


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


# ---------------------------------------------------------------------------
# Harness wire-boundary types — msgspec.Struct with strict `str` validation.
#
# Beets' Discogs plugin returns integer album_id / track_id values; beets'
# MusicBrainz plugin returns UUID strings. Every downstream consumer in the
# pipeline compares these against DB-stored TEXT release IDs with `==`, so a
# mixed-type wire format silently fails (that was the "mbid_not_found" bug
# for every Discogs validation — fixed in PR #98, guarded here).
#
# The harness normalises IDs to str via `_id_str` in beets_harness.py before
# emitting. These Structs declare `str` and msgspec validates at decode time
# — an int on the wire raises `msgspec.ValidationError` inside
# `lib/beets.py::beets_validate`, which surfaces it as `result.error`. Loud
# failure instead of a silent miss.
#
# Why msgspec.Struct (not @dataclass):
#   1. msgspec.json.decode(blob, type=Foo) validates types at the boundary.
#      @dataclass has no runtime schema enforcement; you'd have to hand-roll
#      a from_dict that coerces every field, and that coercion becomes
#      defensive cruft downstream.
#   2. Wire-shape changes are detected by tests, not by production bugs.
#   3. Near-zero overhead.
#
# Every wire-boundary type in this module is a ``msgspec.Struct`` — the
# harness ones below PLUS ``ImportResult`` / ``PostflightInfo`` /
# ``ConversionInfo`` / ``SpectralDetail`` / ``AudioQualityMeasurement`` /
# ``MovedSibling`` / ``ValidationResult`` further down, which all round-trip
# through ``download_log`` JSONB and/or subprocess stdout (issue #141
# unified on one encoder: ``msgspec.json.encode``). Types constructed
# entirely from in-process Python (e.g. ``QualityRankConfig``,
# ``CratediggerConfig``) stay as ``@dataclass`` — their inputs are already
# typed, not a wire protocol.
# ---------------------------------------------------------------------------


class HarnessItem(msgspec.Struct):
    """Local file as seen by the beets harness during matching."""
    path: str = ""
    title: str = ""
    artist: str = ""
    album: str = ""
    track: int = 0
    disc: int = 0
    length: float = 0.0
    bitrate: Optional[int] = None
    format: str = ""
    mb_trackid: str = ""
    data_source: str = ""


class HarnessTrackInfo(msgspec.Struct):
    """MusicBrainz / Discogs track info as seen by the beets harness.

    `track_id` and `release_track_id` are declared `str`; msgspec raises
    ValidationError if beets leaks an int through (regression guard for
    the PR #98 bug).
    """
    title: str = ""
    artist: str = ""
    index: Optional[int] = None
    medium: Optional[int] = None
    medium_index: Optional[int] = None
    medium_total: Optional[int] = None
    length: float = 0.0
    track_id: str = ""
    release_track_id: str = ""
    track_alt: Optional[str] = None
    disctitle: Optional[str] = None
    data_source: str = ""


class TrackMapping(msgspec.Struct):
    """Which local item matched which MB/Discogs track."""
    item: HarnessItem = msgspec.field(default_factory=HarnessItem)
    track: HarnessTrackInfo = msgspec.field(default_factory=HarnessTrackInfo)


class CandidateSummary(msgspec.Struct, rename={"mbid": "album_id"}):
    """Full beets candidate match data for audit logging.

    Stores everything the harness sends — every field from AlbumInfo,
    the distance breakdown, track mapping, and extra items/tracks with
    full detail.

    Wire ↔ attribute mapping: the harness emits the JSON key `album_id`
    (beets' own field name); this Struct exposes it as `.mbid` for
    continuity with existing Python callers. msgspec handles both the
    rename and the strict `str` validation.

    JSONB format note: rows written by `ValidationResult.to_json()`
    AFTER commit 48914ca (PR #100) use the key `album_id`; earlier rows
    use `mbid`. No production code round-trips old rows back through
    `ValidationResult.from_dict` (web routes parse the raw dict), so
    this is a forward-only format change. If you ever need to decode
    pre-48914ca rows via msgspec, either pre-rename the key or add
    `"mbid"` as a secondary key on the Struct.
    """
    # Core identity
    mbid: str = ""
    artist: str = ""
    album: str = ""
    distance: float = 0.0
    distance_breakdown: dict[str, float] = {}
    is_target: bool = False
    # AlbumInfo metadata
    albumdisambig: str = ""
    year: Optional[int] = None
    original_year: Optional[int] = None
    country: Optional[str] = None
    label: Optional[str] = None
    catalognum: Optional[str] = None
    media: Optional[str] = None
    mediums: Optional[int] = None
    albumtype: Optional[str] = None
    albumtypes: list[str] = []
    albumstatus: Optional[str] = None
    releasegroup_id: str = ""
    release_group_title: str = ""
    va: bool = False
    language: Optional[str] = None
    script: Optional[str] = None
    data_source: str = ""
    barcode: str = ""
    asin: str = ""
    # Tracks and mapping
    track_count: int = 0
    tracks: list[HarnessTrackInfo] = []
    mapping: list[TrackMapping] = []
    extra_items: list[HarnessItem] = []
    extra_tracks: list[HarnessTrackInfo] = []


class ChooseMatchMessage(msgspec.Struct):
    """Full schema of the harness `choose_match` JSON message. Decoded in
    one shot at the wire boundary (`lib/beets.py::beets_validate`) via
    `msgspec.convert(msg, type=ChooseMatchMessage)` — any type drift in
    any nested field raises `msgspec.ValidationError` immediately.
    """
    task_id: int = 0
    path: str = ""
    cur_artist: str = ""
    cur_album: str = ""
    item_count: int = 0
    items: list[HarnessItem] = []
    recommendation: str = "none"
    candidate_count: int = 0
    candidates: list[CandidateSummary] = []


class ValidationResult(msgspec.Struct):
    """Structured result from beets validation + audio integrity check.

    Accumulated through the validation pipeline:
    1. beets_validate() populates candidates, distance, scenario
    2. Audio integrity check may set scenario=audio_corrupt + corrupt_files
    3. cratedigger.py populates source info (username, folder, failed_path, denylisted)

    Stored in download_log.validation_result (JSONB) for complete auditability.
    Wire-boundary type per ``.claude/rules/code-quality.md``: encode via
    ``msgspec.json.encode``, decode via ``msgspec.convert`` — symmetric.
    """
    valid: bool = False
    distance: Optional[float] = None
    scenario: Optional[str] = None
    detail: Optional[str] = None
    mbid_found: bool = False
    target_mbid: Optional[str] = None
    candidate_count: int = 0
    candidates: list[CandidateSummary] = []
    # Local file info (from harness choose_match items)
    items: list[dict] = []
    local_track_count: Optional[int] = None
    recommendation: Optional[str] = None        # beets confidence: "strong", "medium", "none"
    path: Optional[str] = None                  # album path being validated
    # Source info (populated by cratedigger.py)
    soulseek_username: Optional[str] = None
    download_folder: Optional[str] = None
    failed_path: Optional[str] = None
    denylisted_users: list[str] = []
    # Audio integrity
    corrupt_files: list[str] = []
    error: Optional[str] = None

    def to_json(self) -> str:
        """Serialize to JSON string via msgspec.json.encode."""
        return msgspec.json.encode(self).decode()

    @classmethod
    def from_dict(cls, d: dict) -> "ValidationResult":
        """Construct from a dict — strict-typed decode at the boundary.

        Every nested ``CandidateSummary`` / ``TrackMapping`` / ``HarnessItem``
        / ``HarnessTrackInfo`` is validated against its declared types.
        """
        return msgspec.convert(d, type=cls)

    @classmethod
    def from_json(cls, s: str) -> "ValidationResult":
        """Deserialize from JSON string."""
        return msgspec.json.decode(s.encode(), type=cls)


@dataclass
class SpectralContext:
    """Gathered spectral analysis data for both new and existing files.

    Returned by the spectral gathering function, consumed by the
    spectral_import_decision() pure function.
    """
    needs_check: bool = False
    grade: Optional[str] = None
    bitrate: Optional[int] = None
    suspect_pct: float = 0.0
    existing_min_bitrate: Optional[int] = None
    existing_spectral_bitrate: Optional[int] = None
    existing_spectral_grade: Optional[str] = None


IMPORT_RESULT_SENTINEL = "__IMPORT_RESULT__"


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

@dataclass
class ActiveDownloadFileState:
    """Per-file state persisted for active downloads."""
    username: str
    filename: str           # Full soulseek path (backslashes)
    file_dir: str           # Download directory on source user's system
    size: int               # File size in bytes
    disk_no: int | None = None
    disk_count: int | None = None
    retry_count: int = 0
    bytes_transferred: int = 0
    last_state: str | None = None

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {
            "username": self.username,
            "filename": self.filename,
            "file_dir": self.file_dir,
            "size": self.size,
            "retry_count": self.retry_count,
            "bytes_transferred": self.bytes_transferred,
        }
        if self.disk_no is not None:
            d["disk_no"] = self.disk_no
        if self.disk_count is not None:
            d["disk_count"] = self.disk_count
        if self.last_state is not None:
            d["last_state"] = self.last_state
        return d

    @staticmethod
    def from_dict(d: dict[str, object]) -> "ActiveDownloadFileState":
        return ActiveDownloadFileState(
            username=str(d["username"]),
            filename=str(d["filename"]),
            file_dir=str(d["file_dir"]),
            size=int(d["size"]),  # type: ignore[arg-type]
            disk_no=int(d["disk_no"]) if d.get("disk_no") is not None else None,  # type: ignore[arg-type]
            disk_count=int(d["disk_count"]) if d.get("disk_count") is not None else None,  # type: ignore[arg-type]
            retry_count=int(d.get("retry_count", 0)),  # type: ignore[arg-type]
            bytes_transferred=int(d.get("bytes_transferred", 0)),  # type: ignore[arg-type]
            last_state=(
                str(d["last_state"])
                if d.get("last_state") is not None
                else None
            ),
        )


@dataclass
class ActiveDownloadState:
    """State persisted to DB for an album being actively downloaded."""
    filetype: str                         # "flac", "mp3 v0", etc.
    enqueued_at: str                      # ISO8601 UTC timestamp
    files: list[ActiveDownloadFileState]
    last_progress_at: str | None = None
    processing_started_at: str | None = None

    def to_json(self) -> str:
        data: dict[str, object] = {
            "filetype": self.filetype,
            "enqueued_at": self.enqueued_at,
            "files": [f.to_dict() for f in self.files],
        }
        if self.last_progress_at is not None:
            data["last_progress_at"] = self.last_progress_at
        if self.processing_started_at is not None:
            data["processing_started_at"] = self.processing_started_at
        return json.dumps(data)

    @staticmethod
    def from_dict(d: dict[str, object]) -> "ActiveDownloadState":
        files_raw = d.get("files")
        assert isinstance(files_raw, list)
        return ActiveDownloadState(
            filetype=str(d["filetype"]),
            enqueued_at=str(d["enqueued_at"]),
            files=[ActiveDownloadFileState.from_dict(f) for f in files_raw],
            last_progress_at=(
                str(d["last_progress_at"])
                if d.get("last_progress_at") is not None
                else None
            ),
            processing_started_at=(
                str(d["processing_started_at"])
                if d.get("processing_started_at") is not None
                else None
            ),
        )

    @staticmethod
    def from_json(s: str) -> "ActiveDownloadState":
        return ActiveDownloadState.from_dict(json.loads(s))


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


# ---------------------------------------------------------------------------
# Audio quality measurement — ground truth from ffprobe + spectral
# ---------------------------------------------------------------------------

class AudioQualityMeasurement(msgspec.Struct, frozen=True):
    """What we actually measured about a set of audio files.

    Ground truth from ffprobe and spectral analysis. Used by decision functions
    to compare new downloads against existing files and determine quality gate
    outcomes. Wire-boundary type per ``.claude/rules/code-quality.md`` —
    appears in ``ImportResult.{new_measurement,existing_measurement}`` and
    crosses both the harness stdout and ``download_log.import_result`` JSONB
    boundaries.

    Fields:
        min_bitrate_kbps:      min per-track bitrate (kbps), None if unmeasurable
        avg_bitrate_kbps:      mean per-track bitrate (kbps), None if unmeasured.
                               Preferred by the rank model for VBR codecs — see
                               RankBitrateMetric and measurement_rank(). Additive;
                               legacy callers that only populate min_bitrate_kbps
                               still work (measurement_rank() falls back to min).
        median_bitrate_kbps:   median per-track bitrate (kbps), None if
                               unmeasured. Used when
                               RankBitrateMetric.MEDIAN is configured —
                               robust against per-track outliers (intro/outro
                               silence, hidden tracks, very short interludes)
                               that can pull MIN or AVG away from the typical
                               track quality. measurement_rank() falls back
                               to min when this is None.
        format:                codec label or bare codec name that drives the
                               quality_rank() classifier. Accepts either an
                               explicit label from ImportResult.final_format
                               ("opus 128", "mp3 v0", "mp3 320", "flac") or a
                               bare codec string from beets items.format ("MP3",
                               "Opus", "FLAC", "AAC"). None → UNKNOWN rank.
        is_cbr:                True if all tracks have the same bitrate
        spectral_grade:        spectral analysis result (genuine/marginal/suspect)
        spectral_bitrate_kbps: estimated original bitrate from spectral cliff
        verified_lossless:     True if imported from spectral-verified genuine lossless
        was_converted_from:    source format before conversion (flac/m4a/wav), None if MP3
    """
    min_bitrate_kbps: Optional[int] = None
    avg_bitrate_kbps: Optional[int] = None
    median_bitrate_kbps: Optional[int] = None
    format: Optional[str] = None
    is_cbr: bool = False
    spectral_grade: Optional[str] = None
    spectral_bitrate_kbps: Optional[int] = None
    verified_lossless: bool = False
    was_converted_from: Optional[str] = None


# ---------------------------------------------------------------------------
# Codec-aware quality rank model (issue #60)
# ---------------------------------------------------------------------------
#
# The pipeline needs to compare audio quality across codecs (Opus 128 ≈ MP3 V0)
# and apply a tier floor when verified_lossless targets would otherwise bypass
# all guardrails (e.g. a FLAC → Opus 64 target replacing a genuine MP3 V0).
#
# Every numeric threshold, codec set, and policy knob lives in QualityRankConfig.
# Grep the decision path for a bare kbps value and you should find zero hits
# outside log strings — everything routes through cfg.quality_ranks.<field>.
#
# Spectral cliff detection and transcode_detection() continue to use min
# bitrate regardless of QualityRankConfig.bitrate_metric — those care about
# the worst track, not the album average. Rank classification is different
# because a single quiet track in a legitimately encoded VBR album should not
# drag the whole album down a rank.


class RankBitrateMetric(StrEnum):
    """Which per-album bitrate statistic feeds into quality_rank() classification.

    MIN    — minimum per-track bitrate. Legacy behavior. Conservative and prone
             to VBR false negatives on albums with genuinely quiet tracks.
    AVG    — album-mean per-track bitrate. Recommended for VBR codecs. Default.
    MEDIAN — middle per-track bitrate. Robust against per-track outliers
             (intro/outro silence, hidden tracks, very short interludes) where
             a single low track would drag MIN down and a few skewed tracks
             could pull AVG away from the typical track quality.

    measurement_rank() is the only function that dispatches on this enum.
    Each metric has a matching field on AudioQualityMeasurement / AlbumInfo;
    when the configured metric's field is None, measurement_rank() falls back
    to min_bitrate_kbps so legacy callers still classify correctly.
    """
    MIN = "min"
    AVG = "avg"
    MEDIAN = "median"


class QualityRank(IntEnum):
    """Perceptual quality bands. IntEnum so > / >= comparisons work naturally.

    Integer spacing leaves room for inserting new bands without reshuffling.
    Nothing persists the integer value — rank is always recomputed from a
    measurement + config, so a future insertion is safe.
    """
    UNKNOWN     = 0
    POOR        = 20
    ACCEPTABLE  = 30
    GOOD        = 40
    EXCELLENT   = 50
    TRANSPARENT = 60
    LOSSLESS    = 100


_RANK_NAME_TO_VALUE: dict[str, QualityRank] = {
    "unknown":     QualityRank.UNKNOWN,
    "poor":        QualityRank.POOR,
    "acceptable":  QualityRank.ACCEPTABLE,
    "good":        QualityRank.GOOD,
    "excellent":   QualityRank.EXCELLENT,
    "transparent": QualityRank.TRANSPARENT,
    "lossless":    QualityRank.LOSSLESS,
}


@dataclass(frozen=True)
class CodecRankBands:
    """Bitrate thresholds (kbps) for a single codec family.

    A measurement's rank is the highest band whose threshold the configured
    metric meets or exceeds. Thresholds must be monotonically non-increasing:
    transparent >= excellent >= good >= acceptable >= 0.
    Values below ``acceptable`` are classified POOR.
    """
    transparent: int
    excellent: int
    good: int
    acceptable: int

    def __post_init__(self) -> None:
        if not (self.transparent >= self.excellent >= self.good
                >= self.acceptable >= 0):
            raise ValueError(
                f"CodecRankBands must be monotonic "
                f"(transparent >= excellent >= good >= acceptable >= 0): {self}")

    def rank_for(self, bitrate_kbps: Optional[int]) -> QualityRank:
        """Classify a bitrate against this codec's band table."""
        if bitrate_kbps is None:
            return QualityRank.UNKNOWN
        if bitrate_kbps >= self.transparent:
            return QualityRank.TRANSPARENT
        if bitrate_kbps >= self.excellent:
            return QualityRank.EXCELLENT
        if bitrate_kbps >= self.good:
            return QualityRank.GOOD
        if bitrate_kbps >= self.acceptable:
            return QualityRank.ACCEPTABLE
        return QualityRank.POOR


@dataclass(frozen=True)
class QualityRankConfig:
    """Every knob for the codec-aware rank model.

    This is the ONLY place numeric quality thresholds live. If you grep the
    rank decision path for a hardcoded kbps value you should find zero hits
    outside log strings — everything routes through cfg.quality_ranks.<field>.

    Defaults are documented in docs/quality-ranks.md. Summary:

    - Opus ``transparent=112``: ``ffmpeg -b:a 128k`` unconstrained VBR averages
      120-135 kbps on typical music; 112 leaves headroom for sparse material.
      ``excellent=88`` matches Opus 96 quality (hydrogenaudio/Kamedo2 4.65/5).
    - MP3 VBR ``transparent=210``: matches the legacy
      ``QUALITY_MIN_BITRATE_KBPS`` constant; V2 averages ~190 → excellent at 170.
    - MP3 CBR ``transparent=320``: unverifiable CBR is only transparent at 320.
    - AAC ``transparent=192``: hydrogenaudio consensus ceiling for music.
    """
    # --- Policy ---
    bitrate_metric: RankBitrateMetric = RankBitrateMetric.AVG
    gate_min_rank: QualityRank = QualityRank.EXCELLENT
    within_rank_tolerance_kbps: int = 5

    # --- Per-codec band tables ---
    # Defaults are tuned to preserve the legacy 210 kbps gate threshold for
    # bare-codec MP3 VBR measurements (old QUALITY_MIN_BITRATE_KBPS) while
    # adding perceptual tiers above and below. Explicit labels like "mp3 v0"
    # / "opus 128" bypass the band tables via the V-level / declared-bitrate
    # resolution steps — those classify by contract.
    opus:    CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=112, excellent=88, good=64, acceptable=48))
    mp3_vbr: CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=245, excellent=210, good=170, acceptable=130))
    mp3_cbr: CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=320, excellent=256, good=192, acceptable=128))
    aac:     CodecRankBands = field(default_factory=lambda: CodecRankBands(
        transparent=192, excellent=144, good=112, acceptable=80))

    # --- LAME VBR V-level → rank (10-tuple indexed by V0..V9) ---
    # V-level semantics are a LAME encoder contract, but surfacing them here
    # keeps the entire policy in one dataclass per the no-magic-numbers rule.
    mp3_vbr_levels: tuple[QualityRank, ...] = (
        QualityRank.TRANSPARENT,  # V0
        QualityRank.EXCELLENT,    # V1
        QualityRank.EXCELLENT,    # V2
        QualityRank.GOOD,         # V3
        QualityRank.GOOD,         # V4
        QualityRank.ACCEPTABLE,   # V5
        QualityRank.ACCEPTABLE,   # V6
        QualityRank.ACCEPTABLE,   # V7
        QualityRank.ACCEPTABLE,   # V8
        QualityRank.ACCEPTABLE,   # V9
    )

    # --- Lossless codec identity ---
    lossless_codecs: frozenset[str] = frozenset({"flac", "lossless", "alac", "wav"})

    # --- Mixed-format album precedence (worst codec wins for classification) ---
    # When an album has multiple formats on disk (rare), pick the lowest-rank
    # codec as the album's "canonical" codec so the rank stays conservative.
    mixed_format_precedence: tuple[str, ...] = ("mp3", "aac", "opus", "flac")

    @classmethod
    def defaults(cls) -> "QualityRankConfig":
        return cls()

    # ------------------------------------------------------------------
    # [Quality Ranks] config.ini parsing
    # ------------------------------------------------------------------

    @classmethod
    def from_ini(
        cls,
        parser: configparser.RawConfigParser,
        section: str = "Quality Ranks",
    ) -> "QualityRankConfig":
        """Parse a [Quality Ranks] section into a QualityRankConfig.

        Every key is optional — missing keys fall back to the field's default
        value, so users can customize one codec or one band without writing
        out the entire section.

        Key names (all lowercase, codec-prefixed for bands):

            bitrate_metric            = min | avg | median
            gate_min_rank             = unknown|poor|acceptable|good|excellent|transparent|lossless
            within_rank_tolerance_kbps = <int>
            <codec>.<band>            = <int>
              codecs: opus, mp3_vbr, mp3_cbr, aac
              bands:  transparent, excellent, good, acceptable
            mp3_vbr_levels            = <rank>,<rank>,... (exactly 10, V0..V9)
            lossless_codecs           = <codec>,<codec>,... (set, lowercased)
            mixed_format_precedence   = <codec>,<codec>,... (ordered list, lowercased)

        Invalid values raise ValueError at parse time with a diagnostic that
        names the offending key. Missing section silently returns defaults.
        """
        base = cls.defaults()
        if not parser.has_section(section):
            return base

        def _get_str(key: str, default: str) -> str:
            raw = parser.get(section, key, fallback=None)
            if raw is None or raw.strip() == "":
                return default
            return raw.strip()

        def _get_int(key: str, default: int) -> int:
            raw = parser.get(section, key, fallback=None)
            if raw is None or raw.strip() == "":
                return default
            try:
                return int(raw.strip())
            except ValueError as exc:
                raise ValueError(
                    f"[{section}] {key}: expected integer, got {raw!r}") from exc

        # --- Policy ---
        metric_str = _get_str("bitrate_metric", base.bitrate_metric.value).lower()
        try:
            metric = RankBitrateMetric(metric_str)
        except ValueError as exc:
            raise ValueError(
                f"[{section}] bitrate_metric: expected one of "
                f"{[m.value for m in RankBitrateMetric]}, got {metric_str!r}"
            ) from exc

        rank_str = _get_str("gate_min_rank", base.gate_min_rank.name.lower()).lower()
        if rank_str not in _RANK_NAME_TO_VALUE:
            raise ValueError(
                f"[{section}] gate_min_rank: expected one of "
                f"{sorted(_RANK_NAME_TO_VALUE.keys())}, got {rank_str!r}")
        gate_min_rank = _RANK_NAME_TO_VALUE[rank_str]

        tolerance = _get_int("within_rank_tolerance_kbps", base.within_rank_tolerance_kbps)
        if tolerance < 0:
            raise ValueError(
                f"[{section}] within_rank_tolerance_kbps: must be >= 0, got {tolerance}")

        # --- Codec bands ---
        def _get_bands(codec: str, default: CodecRankBands) -> CodecRankBands:
            return CodecRankBands(
                transparent=_get_int(f"{codec}.transparent", default.transparent),
                excellent=_get_int(f"{codec}.excellent", default.excellent),
                good=_get_int(f"{codec}.good", default.good),
                acceptable=_get_int(f"{codec}.acceptable", default.acceptable),
            )

        try:
            opus = _get_bands("opus", base.opus)
            mp3_vbr = _get_bands("mp3_vbr", base.mp3_vbr)
            mp3_cbr = _get_bands("mp3_cbr", base.mp3_cbr)
            aac = _get_bands("aac", base.aac)
        except ValueError as exc:
            # CodecRankBands.__post_init__ raises on non-monotonic; re-wrap
            # with section context.
            raise ValueError(f"[{section}] invalid codec bands: {exc}") from exc

        # --- Collection fields (issue #65) ---
        # CSV with leading/trailing whitespace tolerated. An unset key (raw
        # is None) and an empty `key = ` both fall through to the default;
        # an explicit list with no usable values (e.g. just commas) is a
        # config error so the user gets a diagnostic instead of silently
        # losing all lossless codecs.
        def _split_csv(key: str) -> list[str] | None:
            raw = parser.get(section, key, fallback=None)
            if raw is None or raw.strip() == "":
                return None
            parts = [p.strip() for p in raw.split(",")]
            parts = [p for p in parts if p]
            if not parts:
                raise ValueError(
                    f"[{section}] {key}: list contains no usable entries "
                    f"(got {raw!r})")
            return parts

        # mp3_vbr_levels — exactly 10 entries, each a QualityRank name.
        levels_parts = _split_csv("mp3_vbr_levels")
        if levels_parts is None:
            mp3_vbr_levels = base.mp3_vbr_levels
        else:
            if len(levels_parts) != 10:
                raise ValueError(
                    f"[{section}] mp3_vbr_levels: expected exactly 10 ranks "
                    f"(V0..V9), got {len(levels_parts)}")
            parsed_levels: list[QualityRank] = []
            for i, name in enumerate(levels_parts):
                key_norm = name.lower()
                if key_norm not in _RANK_NAME_TO_VALUE:
                    raise ValueError(
                        f"[{section}] mp3_vbr_levels: entry {i} (V{i}) "
                        f"is {name!r}, expected one of "
                        f"{sorted(_RANK_NAME_TO_VALUE.keys())}")
                parsed_levels.append(_RANK_NAME_TO_VALUE[key_norm])
            mp3_vbr_levels = tuple(parsed_levels)

        # lossless_codecs — frozenset, lowercased, deduplicated.
        lossless_parts = _split_csv("lossless_codecs")
        if lossless_parts is None:
            lossless_codecs = base.lossless_codecs
        else:
            lossless_codecs = frozenset(p.lower() for p in lossless_parts)

        # mixed_format_precedence — ordered tuple, lowercased. Order matters
        # because _reduce_album_format takes the first match.
        precedence_parts = _split_csv("mixed_format_precedence")
        if precedence_parts is None:
            mixed_format_precedence = base.mixed_format_precedence
        else:
            mixed_format_precedence = tuple(p.lower() for p in precedence_parts)

        return cls(
            bitrate_metric=metric,
            gate_min_rank=gate_min_rank,
            within_rank_tolerance_kbps=tolerance,
            opus=opus,
            mp3_vbr=mp3_vbr,
            mp3_cbr=mp3_cbr,
            aac=aac,
            mp3_vbr_levels=mp3_vbr_levels,
            lossless_codecs=lossless_codecs,
            mixed_format_precedence=mixed_format_precedence,
        )

    # ------------------------------------------------------------------
    # JSON round-trip (used by the import_one.py argv protocol)
    # ------------------------------------------------------------------

    def to_json(self) -> str:
        """Serialize to JSON for the --quality-rank-config harness argv."""
        payload: dict[str, Any] = {
            "bitrate_metric": self.bitrate_metric.value,
            "gate_min_rank": int(self.gate_min_rank),
            "within_rank_tolerance_kbps": self.within_rank_tolerance_kbps,
            "opus": asdict(self.opus),
            "mp3_vbr": asdict(self.mp3_vbr),
            "mp3_cbr": asdict(self.mp3_cbr),
            "aac": asdict(self.aac),
            "mp3_vbr_levels": [int(r) for r in self.mp3_vbr_levels],
            "lossless_codecs": sorted(self.lossless_codecs),
            "mixed_format_precedence": list(self.mixed_format_precedence),
        }
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> "QualityRankConfig":
        """Inverse of to_json().

        Missing keys / invalid enum values raise ValueError with a
        QualityRankConfig-qualified diagnostic so the harness operator can
        identify which field corrupted the argv round-trip. Used by
        harness/import_one.py to deserialize the --quality-rank-config argv
        blob emitted by dispatch_import_core().
        """
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"QualityRankConfig.from_json: invalid JSON: {exc}") from exc
        try:
            return cls(
                bitrate_metric=RankBitrateMetric(payload["bitrate_metric"]),
                gate_min_rank=QualityRank(int(payload["gate_min_rank"])),
                within_rank_tolerance_kbps=int(payload["within_rank_tolerance_kbps"]),
                opus=CodecRankBands(**payload["opus"]),
                mp3_vbr=CodecRankBands(**payload["mp3_vbr"]),
                mp3_cbr=CodecRankBands(**payload["mp3_cbr"]),
                aac=CodecRankBands(**payload["aac"]),
                mp3_vbr_levels=tuple(
                    QualityRank(int(r)) for r in payload["mp3_vbr_levels"]),
                lossless_codecs=frozenset(payload["lossless_codecs"]),
                mixed_format_precedence=tuple(payload["mixed_format_precedence"]),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError(
                f"QualityRankConfig.from_json: failed to reconstruct config: "
                f"{type(exc).__name__}: {exc}") from exc


# Known codec family names produced by _codec_family_of().
_KNOWN_CODEC_FAMILIES: frozenset[str] = frozenset(
    {"opus", "mp3", "aac", "flac", "alac", "wav", "lossless", "unknown"})


def _codec_family_of(format_hint: Optional[str]) -> str:
    """First token of format, lowercased — "opus 128" → "opus", "MP3" → "mp3"."""
    if format_hint is None:
        return "unknown"
    first = format_hint.strip().lower().split(None, 1)
    if not first or not first[0]:
        return "unknown"
    token = first[0]
    if token in _KNOWN_CODEC_FAMILIES:
        return token
    return "unknown"


def _parse_vbr_level(format_hint: str) -> Optional[int]:
    """Parse V-level from a label like "mp3 v0" / "mp3 v9". Returns None otherwise."""
    parts = format_hint.strip().lower().split()
    if len(parts) < 2 or parts[0] != "mp3":
        return None
    quality = parts[1]
    if len(quality) >= 2 and quality[0] == "v" and quality[1:].isdigit():
        level = int(quality[1:])
        if 0 <= level <= 9:
            return level
    return None


def _parse_bitrate_label(format_hint: str) -> Optional[int]:
    """Parse a numeric bitrate from a label like "opus 128" / "mp3 320"."""
    parts = format_hint.strip().lower().split()
    if len(parts) < 2:
        return None
    quality = parts[1]
    if quality.isdigit():
        return int(quality)
    return None


def quality_rank(
    format_hint: Optional[str],
    bitrate_kbps: Optional[int],
    is_cbr: bool,
    cfg: QualityRankConfig,
) -> QualityRank:
    """Classify a measurement into a QualityRank (pure, no I/O).

    Args:
        format_hint: Either a label like "opus 128" / "mp3 v0" / "mp3 320" /
            "flac" (from ImportResult.final_format / album_requests.final_format)
            OR a bare codec string like "MP3" / "Opus" / "FLAC" / "AAC" (from
            beets items.format). None → UNKNOWN.
        bitrate_kbps: The bitrate value to classify. The caller has already
            selected this value per cfg.bitrate_metric — this function does
            NOT dispatch on the metric. Use measurement_rank() as the entry
            point for measurements.
        is_cbr: True if all tracks share the same bitrate. Affects MP3 family
            routing (VBR vs CBR bands).
        cfg: Rank bands and policy.

    Resolution order:
        1. format_hint is None and bitrate_kbps is None → UNKNOWN.
        2. First token of format_hint in cfg.lossless_codecs → LOSSLESS.
        3. Explicit VBR label ("mp3 v0"): index into cfg.mp3_vbr_levels.
           Label is self-certifying — bitrate is irrelevant here.
        4. Explicit bitrate label ("opus 128"): classify declared bitrate
           against the matching codec's CodecRankBands. The label is a
           contract — we converted to this target, so the declaration wins
           over any measured bitrate.
        5. Bare codec name ("MP3" / "Opus" / "AAC"): classify the measured
           bitrate_kbps against the matching band table. "MP3" + is_cbr=True
           → cfg.mp3_cbr, otherwise cfg.mp3_vbr. Opus and AAC always use
           their own VBR-ish bands.
        6. Unknown codec → UNKNOWN (never promote garbage).
    """
    if format_hint is None and bitrate_kbps is None:
        return QualityRank.UNKNOWN

    family = _codec_family_of(format_hint)

    # Step 2 — lossless
    if family in cfg.lossless_codecs:
        return QualityRank.LOSSLESS

    # Step 3 — explicit VBR V-level label
    if format_hint is not None:
        vbr_level = _parse_vbr_level(format_hint)
        if vbr_level is not None:
            return cfg.mp3_vbr_levels[vbr_level]

    # Step 4 — explicit bitrate label ("opus 128" / "mp3 320" / "aac 192")
    if format_hint is not None:
        declared = _parse_bitrate_label(format_hint)
        if declared is not None:
            if family == "opus":
                return cfg.opus.rank_for(declared)
            if family == "mp3":
                # A "mp3 320"-style label is by convention CBR.
                return cfg.mp3_cbr.rank_for(declared)
            if family == "aac":
                return cfg.aac.rank_for(declared)
            return QualityRank.UNKNOWN

    # Step 5 — bare codec name + measured bitrate
    if family == "opus":
        return cfg.opus.rank_for(bitrate_kbps)
    if family == "aac":
        return cfg.aac.rank_for(bitrate_kbps)
    if family == "mp3":
        if is_cbr:
            return cfg.mp3_cbr.rank_for(bitrate_kbps)
        return cfg.mp3_vbr.rank_for(bitrate_kbps)

    # Step 6 — unknown codec, refuse to promote
    return QualityRank.UNKNOWN


def measurement_rank(
    m: AudioQualityMeasurement,
    cfg: QualityRankConfig,
) -> QualityRank:
    """Pick the configured bitrate metric from m and classify it.

    This is the ONLY function that dispatches on cfg.bitrate_metric. Each
    metric has a matching field on AudioQualityMeasurement: AVG → avg,
    MEDIAN → median, MIN → min.

    Falls back to min_bitrate_kbps when the configured metric's field is
    None — so legacy measurements (which only populate min) continue to
    classify correctly regardless of the configured policy.
    """
    return quality_rank(m.format, _selected_bitrate(m, cfg), m.is_cbr, cfg)


def _selected_bitrate(m: AudioQualityMeasurement,
                      cfg: QualityRankConfig) -> Optional[int]:
    """Return the bitrate value measurement_rank() would classify for m.

    Used by compare_quality() for the same-rank, same-codec tiebreaker.
    Keeps the metric dispatch in one place — compare_quality does not
    peek into m.avg / m.median / m.min directly.
    """
    # Use `==` not `is`: RankBitrateMetric is a StrEnum, and the project
    # has modules that get loaded under two names (``lib.quality`` and
    # ``quality`` via the PYTHONPATH ``lib`` entry). When a cfg constructed
    # in one module is compared with an enum member from the other, they
    # are equal by string value but NOT the same object. Identity-compare
    # here silently fell through to min_bitrate, breaking the AVG policy
    # in the web simulator for VBR albums (issue #93 post-deploy probe).
    if cfg.bitrate_metric == RankBitrateMetric.AVG and m.avg_bitrate_kbps is not None:
        return m.avg_bitrate_kbps
    if (cfg.bitrate_metric == RankBitrateMetric.MEDIAN
            and m.median_bitrate_kbps is not None):
        return m.median_bitrate_kbps
    return m.min_bitrate_kbps


def _is_explicit_label(format_hint: Optional[str]) -> bool:
    """True if format_hint carries an explicit quality contract (VBR or bitrate).

    "mp3 v0" / "opus 128" / "mp3 320" are contracts. "MP3" / "Opus" / "FLAC"
    are bare codec names from beets items.format. Within the same rank tier,
    a contract + anything is equivalent — only bare-vs-bare compares on bitrate.
    """
    if format_hint is None:
        return False
    if _parse_vbr_level(format_hint) is not None:
        return True
    if _parse_bitrate_label(format_hint) is not None:
        return True
    return False


def comparison_format_hint(
    *,
    explicit_format: str | None = None,
    target_format: str | None = None,
    verified_lossless_target: str | None = None,
    converted_count: int = 0,
    is_transcode: bool = False,
    native_codec_family: str | None = None,
) -> str | None:
    """Format hint to use for the pre-import quality comparison.

    This keeps production import_one.py and the simulator on the same rules:
    compare the quality of what would actually end up on disk, not just the
    temporary V0 verification artifact.
    """
    if explicit_format is not None:
        return explicit_format
    if target_format in ("flac", "lossless"):
        return "flac"
    if converted_count > 0 and not is_transcode:
        return verified_lossless_target or "mp3 v0"
    if converted_count > 0:
        return "MP3"
    return native_codec_family


def compare_quality(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
) -> Literal["better", "worse", "equivalent"]:
    """Codec-aware quality comparison.

    Primary key is the QualityRank. Within the same rank:
    - LOSSLESS → always "equivalent" (bitrate variance has no quality meaning).
    - Different codec families → "equivalent" (Opus 128 vs MP3 V0 are
      perceptually indistinguishable at the TRANSPARENT band).
    - Same codec family, either side carries an explicit label ("mp3 v0" /
      "opus 128" / "mp3 320") → "equivalent". Labels are quality contracts
      and within the same rank tier are perceptually equivalent regardless of
      bitrate deltas (a 207 kbps V0 on lo-fi and a 245 kbps V0 on dense material
      are both TRANSPARENT — this is the lo-fi genuine V0 case).
    - Same codec family, both bare codec names → compare the configured metric
      with cfg.within_rank_tolerance_kbps tolerance.

    Pure function. No I/O, no hardcoded numbers — every threshold comes from cfg.
    """
    new_rank = measurement_rank(new, cfg)
    existing_rank = measurement_rank(existing, cfg)

    if new_rank > existing_rank:
        return "better"
    if new_rank < existing_rank:
        return "worse"

    # Same rank. LOSSLESS is always equivalent — FLAC bitrates vary with sample
    # rate and bit depth, not quality.
    if new_rank == QualityRank.LOSSLESS:
        return "equivalent"

    new_family = _codec_family_of(new.format)
    existing_family = _codec_family_of(existing.format)

    # Different codec families at the same rank: perceptually equivalent.
    if new_family != existing_family:
        return "equivalent"

    # Same codec family. If either side has an explicit label, the label is
    # authoritative — within the same rank tier they are equivalent.
    if _is_explicit_label(new.format) or _is_explicit_label(existing.format):
        return "equivalent"

    # Both bare codec names — compare the chosen metric with tolerance.
    new_br = _selected_bitrate(new, cfg)
    existing_br = _selected_bitrate(existing, cfg)
    if new_br is None or existing_br is None:
        return "equivalent"
    delta = new_br - existing_br
    if abs(delta) <= cfg.within_rank_tolerance_kbps:
        return "equivalent"
    return "better" if delta > 0 else "worse"


# ---------------------------------------------------------------------------
# Structured result from import_one.py
# ---------------------------------------------------------------------------

class ConversionInfo(msgspec.Struct):
    """FLAC→V0 conversion details and process artifacts.

    Wire-boundary type per ``.claude/rules/code-quality.md`` — nested in
    ``ImportResult`` which crosses both the harness stdout and JSONB edges.
    """
    converted: int = 0
    failed: int = 0
    was_converted: bool = False
    original_filetype: Optional[str] = None
    target_filetype: Optional[str] = None
    post_conversion_min_bitrate: Optional[int] = None  # min bitrate after lossless→V0
    is_transcode: bool = False  # True if FLAC was actually a transcode
    final_format: Optional[str] = None  # e.g. "opus 128", "mp3 v2", "aac 128"


class SpectralDetail(msgspec.Struct):
    """Per-track spectral analysis detail.

    The album-level spectral grades and bitrates now live on
    AudioQualityMeasurement (new_measurement/existing_measurement on ImportResult).
    This carries the per-track detail data that doesn't fit on a measurement.
    Wire-boundary type per ``.claude/rules/code-quality.md``.
    """
    cliff_freq_hz: Optional[int] = None
    suspect_pct: float = 0.0
    per_track: list[dict] = []  # per-track grade/hf_deficit/cliff
    existing_suspect_pct: float = 0.0


# Issue #133: ``DisambiguationFailure`` / ``SelectorFailure`` were two
# duplicated types with identical shape, scattered across lib.quality
# and lib.release_cleanup. They are now a single
# ``lib.beets_album_op.BeetsOpFailure``; these aliases preserve existing
# imports (``from lib.quality import DisambiguationFailure`` in the
# harness, tests/helpers.py, etc). The unified type added a ``selector``
# field (default ``""``) so old JSON rows with only ``{reason, detail}``
# still decode cleanly via ``msgspec.convert(..., type=PostflightInfo)``
# inside ``_postflight_from_dict`` — msgspec defaults fill in any
# missing key.
from lib.beets_album_op import BeetsOpFailure as DisambiguationFailure


class MovedSibling(msgspec.Struct, frozen=True):
    """Issue #132 P2 / issue #133: record of a sibling album whose
    ``beet move`` successfully relocated its files during post-import
    canonicalization.

    ``album_id`` is the beets numeric primary key for the sibling.
    ``new_path`` is the on-disk directory after the move.
    ``mb_albumid`` / ``discogs_albumid`` are the two columns from
    beets' ``albums`` table at emit time — the harness resolves them
    so the dispatcher doesn't need a second beets DB connection when
    propagating the new path to the pipeline DB.

    Every field matters for propagation: if the sibling's release id
    matches a tracked ``album_requests`` row, its ``imported_path``
    gets updated so the UI stops pointing at the pre-move directory.
    Untracked siblings (no matching pipeline row) are no-ops at the
    dispatcher — propagation is best-effort.

    Wire-boundary type per ``.claude/rules/code-quality.md`` §
    "Wire-boundary types". Decoded from harness stdout JSON AND from
    ``download_log.import_result`` JSONB on every web API read. The
    strict-typed decode at ``_postflight_from_dict`` raises
    ``msgspec.ValidationError`` if a future harness change emits
    ``album_id`` as a string or drops a required field, rather than
    silently corrupting downstream state (the PR #98 / issue #99
    lesson). Encoded symmetrically via ``msgspec.json.encode`` —
    same policy both directions.
    """
    album_id: int
    new_path: str
    mb_albumid: str = ""
    discogs_albumid: str = ""


class PostflightInfo(msgspec.Struct):
    """Beets post-import verification data.

    Wire-boundary type per ``.claude/rules/code-quality.md`` — nested
    in ``ImportResult.postflight``, flows through ``download_log``.
    """
    beets_id: Optional[int] = None
    track_count: Optional[int] = None
    imported_path: Optional[str] = None
    bad_extensions: list[str] = []  # files with non-audio extensions
    disambiguated: bool = False  # True if beet move ran cleanly to fix %aunique paths
    # Issue #127: when ``beet move`` was attempted but did not exit cleanly
    # (TimeoutExpired, OSError, or non-zero rc), this carries a typed
    # failure record. The album was still imported to beets — only the
    # post-import path-disambiguation move failed. ``None`` means either
    # ``disambiguated=True`` (clean success) or that no move was
    # attempted (no duplicate kept).
    disambiguation_failure: Optional[DisambiguationFailure] = None
    # Issue #132 P2 / issue #133: siblings canonicalized post-import.
    # Empty for non-``kept_duplicate`` imports; populated when the
    # harness's ``_canonicalize_siblings`` moved one or more siblings
    # to re-evaluate ``%aunique`` on their paths. The dispatcher
    # propagates each entry's ``new_path`` to the pipeline DB (the
    # tracked request row, if any, for that sibling's release).
    moved_siblings: list[MovedSibling] = []


def _postflight_from_dict(d: Optional[dict]) -> PostflightInfo:
    """Decode a ``postflight`` dict into a ``PostflightInfo`` Struct.

    Strict-typed decode via ``msgspec.convert``: every nested
    ``DisambiguationFailure`` / ``MovedSibling`` is validated against
    its declared types at the boundary (the PR #98 / issue #99 lesson).
    Missing keys default per the Struct —
    ``disambiguation_failure=None``, ``moved_siblings=[]``.

    One pre-convert hedge: if ``moved_siblings`` arrives as a non-list
    (malformed legacy JSONB — observed shapes: scalar, dict, null),
    it's coerced to ``[]`` so the rest of the row still decodes.
    Everything else raises on type drift rather than silently
    corrupting state.
    """
    if not d:
        return PostflightInfo()
    pf_d = dict(d)
    if "moved_siblings" in pf_d and not isinstance(pf_d["moved_siblings"], list):
        pf_d["moved_siblings"] = []
    return msgspec.convert(pf_d, type=PostflightInfo)


class ImportResult(msgspec.Struct):
    """Structured result emitted by import_one.py as JSON.

    Carries every piece of data that crosses the subprocess boundary
    from import_one.py back to cratedigger.py. Stored in download_log.import_result
    for complete auditability.

    new_measurement / existing_measurement carry the coherent quality state
    for the download and what was on disk. The same AudioQualityMeasurement
    type flows through decision functions and the audit trail.

    Wire-boundary type per ``.claude/rules/code-quality.md``: encode via
    ``msgspec.json.encode``, decode via ``msgspec.convert`` — symmetric.
    The pre-#141 asymmetry (``json.dumps(asdict(self))`` outbound,
    ``msgspec.convert`` inbound) forced ``MovedSibling`` et al to be
    ``@dataclass`` so ``asdict`` could recurse; unifying on
    ``msgspec.json.encode`` let every type become a Struct with one rule.
    """
    version: int = 2
    exit_code: int = 0
    decision: Optional[str] = None      # from import_quality_decision() or error label
    already_in_beets: bool = False
    new_measurement: Optional[AudioQualityMeasurement] = None
    existing_measurement: Optional[AudioQualityMeasurement] = None
    conversion: ConversionInfo = msgspec.field(default_factory=ConversionInfo)
    spectral: SpectralDetail = msgspec.field(default_factory=SpectralDetail)
    postflight: PostflightInfo = msgspec.field(default_factory=PostflightInfo)
    beets_log: list[str] = []  # beets stderr lines from import
    error: Optional[str] = None
    # Target-conversion audit trail — V0 bitrate that proved genuineness
    v0_verification_bitrate: Optional[int] = None
    final_format: Optional[str] = None  # configured target, None means keep V0/MP3

    def to_json(self) -> str:
        """Serialize to JSON string via msgspec.json.encode."""
        return msgspec.json.encode(self).decode()

    def to_sentinel_line(self) -> str:
        """Format as the stdout sentinel line for subprocess communication."""
        return IMPORT_RESULT_SENTINEL + self.to_json()

    @classmethod
    def _migrate_v1(cls, d: dict) -> "ImportResult":
        """Migrate version 1 (QualityInfo + SpectralInfo) to version 2 (measurements).

        v1 rows in production (~226 on doc2 as of 2026-04) carry
        ``quality`` and ``spectral`` sub-objects instead of measurements.
        This method reshapes the dict into v2 form, then routes through
        ``from_dict`` so the strict-typed decode happens in one place.
        """
        quality = d.get("quality") or {}
        spectral = d.get("spectral") or {}
        conv_d = dict(d.get("conversion") or {})

        # Migrate process fields from QualityInfo → ConversionInfo
        conv_d.setdefault("post_conversion_min_bitrate",
                          quality.get("post_conversion_min_bitrate"))
        conv_d.setdefault("is_transcode", quality.get("is_transcode", False))

        # Build measurements from scattered fields. v1 rows predate the
        # avg/median bitrate fields (issue #60 / #64) — leaving them at the
        # default None makes measurement_rank() fall back to min, which is
        # the same behavior the v1 row was originally classified under.
        new_measurement: dict[str, Any] = {
            "min_bitrate_kbps": quality.get("new_min_bitrate"),
            "spectral_grade": spectral.get("grade"),
            "spectral_bitrate_kbps": spectral.get("bitrate"),
            "verified_lossless": quality.get(
                "will_be_verified_lossless", False),
            "was_converted_from": (conv_d.get("original_filetype")
                                   if conv_d.get("was_converted") else None),
        }
        existing_measurement: Optional[dict[str, Any]] = None
        if quality.get("prev_min_bitrate") is not None:
            existing_measurement = {
                "min_bitrate_kbps": quality.get("prev_min_bitrate"),
                "spectral_grade": spectral.get("existing_grade"),
                "spectral_bitrate_kbps": spectral.get("existing_bitrate"),
            }

        normalised: dict[str, Any] = {
            "version": 2,
            "exit_code": d.get("exit_code", 0),
            "decision": d.get("decision"),
            "already_in_beets": d.get("already_in_beets", False),
            "new_measurement": new_measurement,
            "existing_measurement": existing_measurement,
            "conversion": conv_d,
            "spectral": {
                "cliff_freq_hz": spectral.get("cliff_freq_hz"),
                "suspect_pct": spectral.get("suspect_pct", 0.0),
                "per_track": spectral.get("per_track", []),
                "existing_suspect_pct": spectral.get(
                    "existing_suspect_pct", 0.0),
            },
            "postflight": d.get("postflight") or {},
            "beets_log": d.get("beets_log", []),
            "error": d.get("error"),
        }
        return cls.from_dict(normalised)

    @classmethod
    def from_dict(cls, d: dict) -> "ImportResult":
        """Construct from a dict (e.g. parsed JSON).

        Handles both old (v1 with quality/spectral sub-objects) and new
        (v2 with measurements) formats for backward compat with existing
        download_log JSONB rows. The v2 path uses ``msgspec.convert`` for
        strict-typed decode; two pre-convert hedges preserve the
        pre-#141 ``_postflight_from_dict`` tolerance:

        1. A falsy non-object ``postflight`` (``null``, ``[]``, ``""``) —
           observed on very old malformed rows — is coerced to ``{}``
           so the PostflightInfo defaults materialise. Pre-#141 the
           loader's ``if not d: return PostflightInfo()`` guard treated
           these as absent; strict ``msgspec.convert`` would raise
           ``ValidationError`` and the callers we patched in the codex
           P2 fix would swallow it and drop the whole ``ImportResult``,
           silently losing every other field.
        2. A non-list ``postflight.moved_siblings`` (malformed legacy
           JSONB) falls back to ``[]``.

        Both hedges fire only for data shapes we've actually seen in
        production; genuine type drift on a declared field still raises
        ``msgspec.ValidationError`` at the boundary.
        """
        # Old format: has "quality" key, no "new_measurement"
        if "quality" in d and "new_measurement" not in d:
            return cls._migrate_v1(d)
        if "postflight" in d:
            pf = d["postflight"]
            if not isinstance(pf, dict):
                d = {**d, "postflight": {}}
            elif ("moved_siblings" in pf
                  and not isinstance(pf["moved_siblings"], list)):
                d = {**d, "postflight": {**pf, "moved_siblings": []}}
        return msgspec.convert(d, type=cls)

    @classmethod
    def from_json(cls, s: str) -> "ImportResult":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(s))


def parse_import_result(stdout_text: str) -> Optional[ImportResult]:
    """Extract ImportResult from import_one.py stdout.

    Scans from the last line backward for the sentinel prefix.
    Returns None if no result found (crash, old version, etc) —
    including when the payload fails strict typed decode post-#141
    (``msgspec.ValidationError``). The callers treat a None here the
    same as "no sentinel line", degrading gracefully instead of
    crashing the whole cycle on a single bad harness emission.
    """
    for line in reversed(stdout_text.strip().split("\n")):
        if line.startswith(IMPORT_RESULT_SENTINEL):
            try:
                return ImportResult.from_json(line[len(IMPORT_RESULT_SENTINEL):])
            except (json.JSONDecodeError, TypeError, KeyError,
                    msgspec.ValidationError):
                return None
    return None


# ---------------------------------------------------------------------------
# Pre-import spectral decision (MP3/CBR path in process_completed_album)
# ---------------------------------------------------------------------------

def spectral_gate_trigger(
    *,
    is_flac: bool,
    is_cbr: bool | None,
    is_vbr: bool | None = None,
    avg_bitrate_kbps: int | None = None,
    vbr_threshold_kbps: int,
) -> str:
    """Decide whether the preimport spectral gate would run on this file.

    Mirrors ``lib.preimport._needs_spectral_check`` but operates on the
    simulator's booleans (``is_flac`` / ``is_cbr``) instead of a filetype
    string, so ``full_pipeline_decision`` and the web UI Decisions tab can
    explain why the gate fired (or didn't) for a given file.

    Returns one of:
        "skipped_flac"          — FLACs use convert → V0 → transcode_detection,
                                  not the MP3 preimport spectral gate
        "skipped_vbr_high_avg"  — VBR MP3 with avg bitrate >= threshold;
                                  genuine V0 falls through without analysis
        "would_run"             — CBR MP3, unknown VBR, or VBR MP3 with avg
                                  below / equal to / unknown

    When ``is_vbr`` is None but ``is_cbr`` is known, ``is_vbr`` is derived
    as ``not is_cbr``. Callers that have genuine ambiguity (mutagen failed
    to read bitrate_mode) pass ``is_vbr=None`` AND ``is_cbr=None`` and the
    function routes to "would_run" — the conservative choice.
    """
    if is_flac:
        return "skipped_flac"
    if is_vbr is None and is_cbr is not None:
        is_vbr = not is_cbr
    if not is_vbr:
        return "would_run"
    if avg_bitrate_kbps is not None and avg_bitrate_kbps >= vbr_threshold_kbps:
        return "skipped_vbr_high_avg"
    return "would_run"


def preimport_audio_gate(audio_check_mode: str, audio_corrupt: bool) -> str:
    """Decide the outcome of the preimport audio-integrity gate.

    Mirrors the first check in ``lib.preimport.run_preimport_gates``:
    ``validate_audio`` runs an ffmpeg full-decode pass unless the operator
    has set ``[Beets Validation] audio_check = off``.

    Returns one of:
        "skipped_off"     — cfg.audio_check_mode == "off", validate_audio is not called
        "reject_corrupt"  — validate_audio reported one or more failed files
        "pass"            — validation ran and every file decoded cleanly

    Keeping this as its own pure helper lets ``full_pipeline_decision`` and
    the Decisions tab document a distinct "you have audio_check off" path,
    which is a common source of surprise when an obvious-looking corrupt
    download gets through in one deployment but not another.
    """
    if audio_check_mode == "off":
        return "skipped_off"
    return "reject_corrupt" if audio_corrupt else "pass"


def preimport_nested_gate(import_mode: str, has_nested_audio: bool) -> str:
    """Decide the outcome of the preimport nested-folder gate.

    Mirrors ``lib.import_dispatch.dispatch_import_from_db``'s fail-fast
    rejection of nested force/manual imports: the preimport gates recurse,
    but the downstream ``harness/import_one.py`` still uses ``os.listdir``
    for bitrate measurement and conversion. A nested force/manual import
    would pass the gates and then produce an empty/misclassified measurement.

    The auto path is already flattened by ``process_completed_album`` before
    dispatch runs, so the gate is only relevant for ``import_mode`` of
    ``"force"`` or ``"manual"``.

    Returns one of:
        "skipped_auto"   — auto path; nested detection doesn't run here
        "reject_nested"  — force/manual path, nested audio files present
        "pass"           — force/manual path, flat layout
    """
    if import_mode == "auto":
        return "skipped_auto"
    return "reject_nested" if has_nested_audio else "pass"


def spectral_import_decision(spectral_grade, spectral_bitrate, existing_spectral_bitrate,
                             existing_min_bitrate=None):
    """Decide whether to import a download based on spectral analysis.

    Called in process_completed_album() for non-FLAC downloads after
    spectral analysis runs on the downloaded files.

    Returns one of:
        "import"          — spectral says genuine/marginal, proceed
        "import_upgrade"  — spectral says suspect but better than existing
        "import_no_exist" — spectral says suspect but nothing on disk yet
        "reject"          — spectral says suspect and not better than existing

    Inputs:
        spectral_grade:             "genuine" | "marginal" | "suspect" | "likely_transcode"
        spectral_bitrate:           estimated bitrate from cliff detection (kbps), or None
        existing_spectral_bitrate:  spectral estimate of what's already in beets (kbps), or 0/None
        existing_min_bitrate:       container bitrate from beets (kbps), fallback when
                                    existing files are genuine (no spectral estimate)
    """
    if spectral_grade not in ("suspect", "likely_transcode"):
        return "import"

    new_q = spectral_bitrate or 0
    # Fall back to container bitrate when existing files have no spectral estimate
    # (genuine files have no cliff → no estimated bitrate)
    existing_q = existing_spectral_bitrate or existing_min_bitrate or 0

    if new_q and existing_q and new_q <= existing_q:
        return "reject"
    elif new_q and existing_q and new_q > existing_q:
        return "import_upgrade"
    elif not existing_q:
        return "import_no_exist"
    else:
        return "import"


# ---------------------------------------------------------------------------
# import_one.py decisions (FLAC conversion path)
# ---------------------------------------------------------------------------

def import_quality_decision(
    new: AudioQualityMeasurement,
    existing: "AudioQualityMeasurement | None",
    is_transcode: bool = False,
    cfg: "QualityRankConfig | None" = None,
) -> str:
    """Decide whether to import based on codec-aware quality comparison (issue #60).

    Called in import_one.py after FLAC→V0 conversion (if applicable)
    and before running the beets harness.

    Uses compare_quality() which classifies both measurements into
    QualityRank bands (via quality_rank/measurement_rank), so cross-codec
    comparisons (Opus 128 vs MP3 V0) are correctly treated as equivalent.

    The verified_lossless bypass is now a tier-gated preference:
    ``verified_lossless=True`` still forces an import when the verdict is
    "better" or "equivalent", but NOT when it would be a downgrade — this
    blocks a deliberately too-low ``verified_lossless_target`` (e.g. Opus
    64) from replacing a good existing album.

    Returns one of:
        "import"              — new files are better (or no existing), proceed
        "downgrade"           — new files are worse, skip (exit 5)
        "transcode_upgrade"   — transcode but better than existing, import + denylist (exit 6)
        "transcode_downgrade" — transcode and not better, skip + denylist (exit 6)
        "transcode_first"     — transcode but nothing on disk yet, import (exit 6)

    Args:
        new: measurement of the new download
        existing: measurement of what's already in beets, or None
                  (caller resolves override_min_bitrate into existing.min_bitrate_kbps)
        is_transcode: True if FLAC→V0 produced a transcode (from transcode_detection)
        cfg: QualityRankConfig. Defaults to QualityRankConfig.defaults().
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    if existing is None:
        return "transcode_first" if is_transcode else "import"

    verdict = compare_quality(new, existing, cfg)

    # verified_lossless is a soft preference: "better" or "equivalent" still
    # import, but "worse" is blocked regardless of verified_lossless status.
    # This prevents a deliberately too-low verified-lossless target from
    # blindly replacing a good existing album (issue #60 acceptance criterion).
    if new.verified_lossless and verdict in ("better", "equivalent"):
        return "transcode_upgrade" if is_transcode else "import"

    if verdict == "better":
        return "transcode_upgrade" if is_transcode else "import"

    # "worse" or "equivalent" without verified_lossless bypass → reject.
    return "transcode_downgrade" if is_transcode else "downgrade"


def transcode_detection(converted_count, post_conversion_min_bitrate,
                        spectral_grade=None,
                        cfg: "QualityRankConfig | None" = None):
    """Detect whether a FLAC→V0 conversion produced a transcode.

    Called in import_one.py after convert_flac_to_v0().

    Returns True if the converted files are likely transcodes
    (MP3 wrapped in FLAC container).

    Inputs:
        converted_count:             number of FLAC files converted
        post_conversion_min_bitrate: min bitrate after conversion (kbps), or None
        spectral_grade:              album spectral grade, or None if unavailable
        cfg:                         QualityRankConfig — the spectral-fallback
                                     threshold is taken from
                                     ``cfg.mp3_vbr.excellent``. When omitted,
                                     falls back to the legacy
                                     ``TRANSCODE_MIN_BITRATE_KBPS`` constant
                                     (210) so existing callers stay
                                     bit-for-bit compatible. Issue #66.
    """
    if converted_count == 0:
        return False
    if post_conversion_min_bitrate is None:
        return False
    # When spectral data is available, it's authoritative
    if spectral_grade is not None:
        # Cliff detected = transcode regardless of bitrate
        if spectral_grade in ("suspect", "likely_transcode"):
            return True
        # No cliff = not a transcode (lo-fi lossless produces low V0 bitrates)
        return False
    # No spectral data — fall back to bitrate threshold. Derived from cfg
    # so the threshold tracks gate retuning automatically: an operator who
    # lowers mp3_vbr.excellent to accept lower-quality V0 also implicitly
    # lowers what counts as "credible V0" for the spectral fallback.
    threshold = (cfg.mp3_vbr.excellent if cfg is not None
                 else TRANSCODE_MIN_BITRATE_KBPS)
    return post_conversion_min_bitrate < threshold


# ---------------------------------------------------------------------------
# Verified lossless derivation (post-import, used by album_source.py)
# ---------------------------------------------------------------------------

_LOSSLESS_EXTS = {"flac", "m4a", "wav", "alac"}


def determine_verified_lossless(
    target_format: Optional[str],
    spectral_grade: Optional[str],
    converted_count: int,
    is_transcode: bool,
) -> bool:
    """Single source of truth for verified lossless status (pure).

    Two paths:
    1. target_format="lossless"/"flac" (lossless kept on disk): verified if
       spectral says genuine or marginal, or if no spectral ran (None).
       The lossless source IS on disk — no conversion needed to prove it.
    2. Default (lossless→V0/target): verified if we actually converted
       lossless files AND spectral didn't flag them as transcodes.
    """
    if target_format in ("flac", "lossless"):
        return spectral_grade in ("genuine", "marginal", None)
    return converted_count > 0 and not is_transcode


def is_verified_lossless(was_converted: bool, original_filetype: Optional[str],
                         spectral_grade: Optional[str]) -> bool:
    """Legacy derivation for album_source.py fallback path.

    Used when import_one.py didn't set verified_lossless_override
    (old download_log rows). Delegates to determine_verified_lossless
    for the standard (non-FLAC-on-disk) case.

    Stricter than determine_verified_lossless: requires spectral_grade="genuine"
    exactly, and validates the original filetype was lossless.
    """
    if not was_converted or original_filetype is None or spectral_grade != "genuine":
        return False
    return original_filetype.lower() in _LOSSLESS_EXTS


# ---------------------------------------------------------------------------
# Post-import quality gate (runs after successful import in cratedigger.py)
# ---------------------------------------------------------------------------

def gate_rank(
    current: AudioQualityMeasurement,
    cfg: "QualityRankConfig",
) -> QualityRank:
    """Rank used by ``quality_gate_decision()`` — measurement rank with the
    spectral clamp applied.

    This is the single source of truth for "what rank does the gate see".
    Both ``quality_gate_decision()`` and the simulator (``pipeline-cli quality``)
    call this so the displayed rank label and the actual gate verdict can
    never disagree.

    Spectral clamp: when the current measurement carries a spectral estimate
    (set upstream only when the grade is suspect/likely_transcode — see
    ``_check_quality_gate_core()``), classify that estimate against the MP3
    VBR band table and take the lower rank. This catches fake 320s and
    legacy low-spectral transcodes.
    """
    rank = measurement_rank(current, cfg)
    if current.spectral_bitrate_kbps is not None:
        spectral_rank = quality_rank(
            "mp3", current.spectral_bitrate_kbps, is_cbr=False, cfg=cfg)
        if spectral_rank < rank:
            rank = spectral_rank
    return rank


def quality_gate_decision(
    current: AudioQualityMeasurement,
    cfg: "QualityRankConfig | None" = None,
) -> str:
    """Codec-aware post-import quality gate (issue #60).

    Classifies ``current`` via ``gate_rank()`` (which applies the spectral
    clamp) and compares against ``cfg.gate_min_rank``.

    Returns one of: "accept", "requeue_upgrade", "requeue_lossless".

    Args:
        current: measurement of the files now on disk (from beets DB + spectral)
        cfg: QualityRankConfig. Defaults to QualityRankConfig.defaults().
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    rank = gate_rank(current, cfg)

    if rank == QualityRank.UNKNOWN or rank < cfg.gate_min_rank:
        return "requeue_upgrade"
    if (not current.verified_lossless and current.is_cbr
            and rank < QualityRank.LOSSLESS):
        return "requeue_lossless"
    return "accept"


# ---------------------------------------------------------------------------
# Dispatch logic — extracted from import_dispatch.py for testability
# ---------------------------------------------------------------------------


@dataclass
class DispatchAction:
    """What actions to take after import_one.py returns a decision."""
    mark_done: bool = False
    record_rejection: bool = False
    denylist: bool = False
    requeue: bool = False
    cleanup: bool = True
    trigger_notifiers: bool = False
    run_quality_gate: bool = False


def dispatch_action(decision: str) -> DispatchAction:
    """Map an ImportResult.decision string to the set of actions to take (pure).

    Encodes the if/elif dispatch chain from dispatch_import().
    """
    if decision in ("import", "preflight_existing"):
        return DispatchAction(mark_done=True, trigger_notifiers=True,
                              run_quality_gate=True, cleanup=True)
    elif decision == "downgrade":
        return DispatchAction(record_rejection=True, denylist=True, cleanup=True)
    elif decision in ("transcode_upgrade", "transcode_first"):
        return DispatchAction(mark_done=True, denylist=True, requeue=True,
                              trigger_notifiers=True, cleanup=True)
    elif decision == "transcode_downgrade":
        return DispatchAction(record_rejection=True, denylist=True, requeue=True,
                              cleanup=True)
    else:  # import_failed, conversion_failed, mbid_missing, crash, etc.
        return DispatchAction(record_rejection=True)


SPECTRAL_TRANSCODE_GRADES: frozenset[str] = frozenset({"suspect", "likely_transcode"})
"""Spectral grades that authorize the spectral bitrate as an override input.

Only these grades mean "this is a transcode and the spectral cliff is a
legitimate low-bound on original quality". Genuine/marginal/error/None/unknown
grades must leave the container bitrate untouched — a genuine lo-fi file
(e.g. Mountain Goats boombox) can produce a low spectral cliff estimate that
is NOT a quality signal and would falsely drag the import comparison down.
See issue #61 for the motivating incident.
"""


def compute_effective_override_bitrate(
    container_bitrate: int | None,
    spectral_bitrate: int | None,
    spectral_grade: str | None,
) -> int | None:
    """Compute the grade-aware effective override bitrate.

    Spectral bitrate only participates when ``spectral_grade`` is in
    ``SPECTRAL_TRANSCODE_GRADES`` (``suspect`` / ``likely_transcode``). For any
    other grade — ``genuine``, ``marginal``, ``error``, ``None``, or an unknown
    future value — the spectral input is ignored and the container bitrate is
    returned untouched.

    When spectral is authorized, the function returns the lower of the two
    available values (conservative). Used by ``dispatch_import()`` to derive
    ``--override-min-bitrate`` for ``import_one.py`` and by the quality gate
    to determine whether to apply a spectral override to the gate bitrate.
    """
    if spectral_grade not in SPECTRAL_TRANSCODE_GRADES:
        return container_bitrate
    if container_bitrate is None and spectral_bitrate is None:
        return None
    if container_bitrate is None:
        return spectral_bitrate
    if spectral_bitrate is None:
        return container_bitrate
    return min(container_bitrate, spectral_bitrate)


def extract_usernames(files: Any) -> set[str]:
    """Extract unique non-empty usernames from a list of file objects."""
    return {f.username for f in files if f.username}


def rejected_download_tier(dl_info: "DownloadInfo") -> str | None:
    """Determine which search_filetype_override tier a rejected download corresponds to.

    Maps from DownloadInfo properties to the tier string used in search_filetype_override
    CSV (e.g. "flac", "mp3 v0", "mp3 320").
    """
    slskd_ft = (dl_info.slskd_filetype or dl_info.filetype or "").lower().strip()
    if slskd_ft in LOSSLESS_CODECS or dl_info.was_converted:
        return "lossless"
    if "mp3" in slskd_ft:
        if dl_info.is_vbr:
            return "mp3 v0"
        bitrate = dl_info.bitrate
        if bitrate is None:
            return None
        kbps = bitrate // 1000 if bitrate > 1000 else bitrate
        return f"mp3 {kbps}"
    return None


def narrow_override_on_downgrade(search_filetype_override: str | None,
                                 dl_info: "DownloadInfo") -> str | None:
    """Remove the rejected filetype tier from search_filetype_override after downgrade.

    When a download is rejected as a downgrade (existing quality >= download),
    searching for the same tier again will produce the same result. Remove it
    to prevent infinite retry loops (e.g. downloading genuine CBR 320 six times).

    Returns the narrowed override string, or None if no change is needed.
    """
    if not search_filetype_override:
        return None
    tier = rejected_download_tier(dl_info)
    if not tier:
        return None
    tiers = [t.strip() for t in search_filetype_override.split(",")]
    if tier not in tiers:
        return None
    narrowed = [t for t in tiers if t != tier]
    if not narrowed:
        return None  # Don't remove the last tier
    return ",".join(narrowed)


def rejection_backfill_override(
    *,
    is_cbr: bool,
    min_bitrate_kbps: int | None,
    spectral_grade: str | None,
    verified_lossless: bool,
    cfg: "QualityRankConfig | None" = None,
) -> str | None:
    """Backfill search_filetype_override for pre-quality-gate albums stuck in download loops.

    When a download is rejected (e.g. downgrade) and search_filetype_override is NULL,
    albums with decent quality on disk keep downloading the same tier forever
    because the quality gate only fires after successful imports.

    Returns QUALITY_LOSSLESS when the on-disk rank is at or above
    ``cfg.gate_min_rank`` (the same threshold the post-import quality gate
    uses) — the only upgrade left is a verified lossless source.

    ``cfg`` defaults to ``QualityRankConfig.defaults()``. Threading the live
    runtime cfg keeps backfill in lockstep with the gate when an operator
    tunes ``gate_min_rank``.
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()
    if verified_lossless:
        return None
    if spectral_grade != "genuine":
        return None
    if min_bitrate_kbps is None:
        return None
    rank = quality_rank("mp3", min_bitrate_kbps, is_cbr=is_cbr, cfg=cfg)
    if rank >= cfg.gate_min_rank:
        return QUALITY_LOSSLESS
    return None


# ---------------------------------------------------------------------------
# AudioFileSpec — single source of truth for filetype identity
# ---------------------------------------------------------------------------

# Extension → default codec. Most are 1:1; .m4a is ambiguous (resolved by heuristic).
_EXT_TO_CODEC: dict[str, str] = {
    "mp3": "mp3",
    "flac": "flac",
    "ogg": "ogg",
    "opus": "opus",
    "aac": "aac",
    "m4a": "aac",   # default; override to "alac" via heuristic
    "wma": "wma",
    "wav": "wav",
}

# Config DSL name → (codec, canonical extension)
_CONFIG_NAME_TO_CODEC: dict[str, tuple[str, str]] = {
    "mp3": ("mp3", "mp3"),
    "flac": ("flac", "flac"),
    "ogg": ("ogg", "ogg"),
    "opus": ("opus", "opus"),
    "aac": ("aac", "aac"),
    "alac": ("alac", "m4a"),
    "wma": ("wma", "wma"),
    "wav": ("wav", "wav"),
    "m4a": ("aac", "m4a"),
}

# Codec → canonical extension (for filename construction)
CODEC_TO_EXT: dict[str, str] = {
    "mp3": "mp3",
    "flac": "flac",
    "ogg": "ogg",
    "opus": "opus",
    "aac": "aac",
    "alac": "m4a",
    "wma": "wma",
    "wav": "wav",
}

# Canonical set of audio extensions (bare: "mp3", "flac", "m4a", ...)
AUDIO_EXTENSIONS: frozenset[str] = frozenset(_EXT_TO_CODEC.keys())

# Same but dotted (".mp3", ".flac", ".m4a", ...) for os.path.splitext consumers
AUDIO_EXTENSIONS_DOTTED: frozenset[str] = frozenset(f".{e}" for e in AUDIO_EXTENSIONS)

# Codecs that are lossless by definition
LOSSLESS_CODECS: frozenset[str] = frozenset({"flac", "alac", "wav"})

# Sentinel: matches any audio file (used for catch-all "download anything" mode)
# Assigned after AudioFileSpec class definition below
CATCH_ALL_SPEC: "AudioFileSpec"


def _m4a_codec_heuristic(
    bitrate: Optional[int],
    bit_depth: Optional[int],
    sample_rate: Optional[int],
) -> str:
    """Guess whether a .m4a file is ALAC or AAC from slskd metadata.

    ALAC (lossless): bitRate > 700kbps, or bitDepth present.
    AAC (lossy): typically 64-320kbps.
    """
    if bit_depth is not None and bit_depth > 0:
        return "alac"
    if bitrate is not None and bitrate >= 700:
        return "alac"
    return "aac"


@dataclass(frozen=True)
class AudioFileSpec:
    """Single source of truth for filetype identity.

    Two forms:
    A. Filter (from config): codec + quality set, audio metadata None.
       Created via parse_filetype_config("mp3 v0").
    B. Identity (from slskd): codec + audio metadata set, quality None.
       Created via file_identity(slskd_file_dict).

    filetype_matches(identity, filter) replaces verify_filetype().
    """
    codec: str
    extension: str
    quality: Optional[str] = None
    bitrate: Optional[int] = None
    sample_rate: Optional[int] = None
    bit_depth: Optional[int] = None
    is_variable_bitrate: Optional[bool] = None

    @property
    def lossless(self) -> bool:
        """True for codecs that are lossless by definition."""
        return self.codec in LOSSLESS_CODECS

    @property
    def config_string(self) -> str:
        """Reconstruct the config DSL string, e.g. 'mp3 v0', 'alac'."""
        if self.quality:
            return f"{self.codec} {self.quality}"
        return self.codec


# Now that AudioFileSpec is defined, create the sentinel
CATCH_ALL_SPEC = AudioFileSpec(codec="*", extension="*")


def parse_filetype_config(config_str: str) -> AudioFileSpec:
    """Parse a config DSL string like 'mp3 v0' or 'alac' into AudioFileSpec.

    This is the FILTER form — quality is set, audio metadata is not.
    Use '*' or 'any' for catch-all mode (matches any audio file).
    Use 'lossless' to match any lossless codec (flac, alac, wav).
    """
    parts = config_str.strip().split(" ", 1)
    name = parts[0].lower()

    if name in ("*", "any"):
        return CATCH_ALL_SPEC
    if name == "lossless":
        return AudioFileSpec(codec="lossless", extension="*")

    quality = parts[1].strip() if len(parts) > 1 else None
    codec, extension = _CONFIG_NAME_TO_CODEC.get(name, (name, name))
    return AudioFileSpec(codec=codec, extension=extension, quality=quality)


def file_identity(file: dict[str, Any] | Any) -> AudioFileSpec:
    """Construct an AudioFileSpec from a raw slskd file dict.

    This is the IDENTITY form — audio metadata is set, quality is not.
    """
    filename = file["filename"]
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    bitrate = file.get("bitRate")
    sample_rate = file.get("sampleRate")
    bit_depth = file.get("bitDepth")
    is_vbr = file.get("isVariableBitRate")

    codec = _EXT_TO_CODEC.get(ext, ext)

    if ext == "m4a":
        codec = _m4a_codec_heuristic(bitrate, bit_depth, sample_rate)

    return AudioFileSpec(
        codec=codec,
        extension=ext,
        bitrate=bitrate,
        sample_rate=sample_rate,
        bit_depth=bit_depth,
        is_variable_bitrate=is_vbr,
    )


def filetype_matches(identity: AudioFileSpec, filter_spec: AudioFileSpec) -> bool:
    """Does a file identity match a filetype filter?

    Replaces the old verify_filetype() internals.  Pure function.
    """
    if filter_spec.codec == "*":
        return True

    # "lossless" virtual tier — matches any lossless codec
    if filter_spec.codec == "lossless":
        return identity.codec in LOSSLESS_CODECS

    if identity.codec != filter_spec.codec:
        return False

    if filter_spec.quality is None:
        return True

    quality = filter_spec.quality

    # Bitdepth/samplerate pair (e.g. "24/96")
    if "/" in quality:
        parts = quality.split("/")
        try:
            req_depth = parts[0]
            req_rate = str(int(float(parts[1]) * 1000))
        except (ValueError, IndexError):
            return False
        if identity.bit_depth is not None and identity.sample_rate is not None:
            return (str(identity.bit_depth) == req_depth and
                    str(identity.sample_rate) == req_rate)
        return False

    # VBR preset (e.g. "v0", "v2")
    if quality.lower() in ("v0", "v2"):
        if identity.bitrate is None:
            return False
        cbr_values = {128, 160, 192, 224, 256, 320}
        is_vbr = identity.bitrate not in cbr_values
        if identity.is_variable_bitrate is not None:
            is_vbr = identity.is_variable_bitrate
        if not is_vbr:
            return False
        if quality.lower() == "v0":
            return 220 <= identity.bitrate <= 280
        else:
            return 170 <= identity.bitrate <= 220

    # Minimum bitrate (e.g. "256+")
    if quality.endswith("+"):
        try:
            min_bitrate = int(quality[:-1])
        except ValueError:
            return False
        return identity.bitrate is not None and identity.bitrate >= min_bitrate

    # Exact bitrate (e.g. "320")
    return identity.bitrate is not None and str(identity.bitrate) == quality


# ---------------------------------------------------------------------------
# Filetype verification — legacy bridge
# ---------------------------------------------------------------------------


def verify_filetype(file: dict[str, Any] | Any, allowed_filetype: str) -> bool:
    """Check whether a slskd file dict matches an allowed filetype specification.

    Legacy bridge — delegates to filetype_matches(file_identity(), parse_filetype_config()).
    """
    identity = file_identity(file)
    filter_spec = parse_filetype_config(allowed_filetype)
    return filetype_matches(identity, filter_spec)


# ---------------------------------------------------------------------------
# Decision tree metadata — consumed by the web UI diagram
# ---------------------------------------------------------------------------

def get_decision_tree(
    cfg: "QualityRankConfig | None" = None,
) -> dict[str, Any]:
    """Return the full pipeline decision structure as data.

    The web UI renders this as a diagram. Contract tests verify this matches
    the actual decision functions. When a function changes, update this too —
    the tests will catch divergence.

    ``cfg`` drives the thresholds that depend on the runtime rank model.
    When omitted, ``QualityRankConfig.defaults()`` is used so the legacy
    "show the hardcoded defaults" behavior still works. The web route at
    ``web/routes/pipeline.py:get_pipeline_constants`` passes the live
    runtime cfg so operators who retune ``mp3_vbr.excellent`` see the
    Decisions tab update in lockstep with ``transcode_detection()``
    (issue #66 follow-up).
    """
    effective_cfg = cfg if cfg is not None else QualityRankConfig.defaults()
    # The spectral-fallback threshold for transcode_detection() is derived
    # from cfg.mp3_vbr.excellent (see #66). Expose it as the "live" constant
    # so the web UI's Decisions tab reflects the runtime rank model instead
    # of a stale module-level default.
    transcode_threshold = effective_cfg.mp3_vbr.excellent
    return {
        "constants": {
            "QUALITY_MIN_BITRATE_KBPS": QUALITY_MIN_BITRATE_KBPS,
            "TRANSCODE_MIN_BITRATE_KBPS": transcode_threshold,
            "QUALITY_UPGRADE_TIERS": QUALITY_UPGRADE_TIERS,
        },
        "paths": ["flac", "mp3"],
        "path_labels": {"flac": "FLAC path", "mp3": "MP3 path"},
        "stages": [
            {
                "id": "preimport_audio",
                "title": "Preimport Audio Integrity",
                "path": "preimport",
                "function": "preimport_audio_gate",
                "when": "Every import path, before any FLAC/MP3 branching "
                        "(lib.preimport.run_preimport_gates step 1)",
                "inputs": ["cfg.audio_check_mode", "validate_audio() result"],
                "rules": [
                    {"condition": "audio_check_mode = off",
                     "result": "skipped_off", "color": "amber",
                     "effect": "gate disabled by config, nothing is rejected here"},
                    {"condition": "validate_audio reports no failed files",
                     "result": "pass", "color": "green"},
                    {"condition": "validate_audio reports one or more failed files",
                     "result": "reject_corrupt", "color": "red",
                     "effect": "import rejected, files moved to failed_imports/"},
                ],
                "outcomes": ["pass", "reject_corrupt", "skipped_off"],
                "note": "Runs ffmpeg full-decode on every audio file. Flip to "
                        "'off' under [Beets Validation] audio_check to bypass "
                        "the gate (e.g. while debugging a false positive).",
            },
            {
                "id": "preimport_nested",
                "title": "Preimport Nested-Layout Gate",
                "path": "preimport",
                "function": "preimport_nested_gate",
                "when": "Force-import and manual-import paths only — the auto "
                        "path flattens downloads before dispatch",
                "inputs": ["import_mode", "inspect_local_files().has_nested_audio"],
                "rules": [
                    {"condition": "import_mode = auto",
                     "result": "skipped_auto", "color": "green",
                     "effect": "auto path flattens upstream; nested detection "
                               "is not a gate here"},
                    {"condition": "force/manual + flat layout",
                     "result": "pass", "color": "green"},
                    {"condition": "force/manual + audio files in subfolders",
                     "result": "reject_nested", "color": "red",
                     "effect": "harness.import_one uses os.listdir for "
                               "bitrate/convert; nested layouts would silently "
                               "misclassify — reject with 'flatten the folder' detail"},
                ],
                "outcomes": ["pass", "reject_nested", "skipped_auto"],
                "note": "Only the force/manual paths hit this gate; the auto "
                        "pipeline flattens downloads in process_completed_album "
                        "before dispatch runs.",
            },
            {
                "id": "flac_spectral",
                "title": "Spectral Analysis",
                "path": "flac",
                "function": "spectral_check.analyze_album",
                "when": "Raw FLAC files before conversion",
                "inputs": ["audio files (sox bandpass 12-20kHz)"],
                "rules": [
                    {"condition": "HF deficit < {HF_DEFICIT_MARGINAL}dB",
                     "result": "genuine", "color": "green"},
                    {"condition": "{HF_DEFICIT_MARGINAL}-{HF_DEFICIT_SUSPECT}dB",
                     "result": "marginal", "color": "amber"},
                    {"condition": ">= {HF_DEFICIT_SUSPECT}dB or cliff",
                     "result": "suspect", "color": "red"},
                ],
                "note": "Album grade: only 'suspect' counts — "
                        ">={ALBUM_SUSPECT_PCT}% suspect = album suspect. "
                        "100% marginal = album genuine",
            },
            {
                "id": "flac_convert",
                "title": "Convert FLAC \u2192 V0",
                "path": "flac",
                "function": "convert_flac_to_v0",
                "when": "FLAC files present",
                "inputs": ["FLAC audio files"],
                "rules": [
                    {"condition": "ffmpeg -q:a 0 (VBR V0)",
                     "result": "MP3 V0 files", "color": "green"},
                ],
                "note": "Post-conversion min bitrate measured across all tracks",
            },
            {
                "id": "transcode",
                "title": "Transcode Detection",
                "path": "flac",
                "function": "transcode_detection",
                "when": "After FLAC \u2192 V0 conversion",
                "inputs": ["converted_count", "post_conversion_min_bitrate",
                           "spectral_grade"],
                "rules": [
                    {"condition": "spectral = suspect/likely_transcode",
                     "result": "is_transcode = true", "color": "red",
                     "effect": "cliff detected = transcode regardless of bitrate"},
                    {"condition": "spectral = genuine/marginal",
                     "result": "is_transcode = false", "color": "green",
                     "effect": "no cliff = not transcode (lo-fi OK)"},
                    {"condition": f"no spectral: post_conv_br < {transcode_threshold}kbps",
                     "result": "is_transcode = true", "color": "red",
                     "effect": "fallback when spectral unavailable"},
                ],
                "note": f"Spectral grade is authoritative when available. "
                        f"Bitrate threshold ({transcode_threshold}kbps, "
                        f"derived from cfg.mp3_vbr.excellent) is fallback only",
            },
            {
                "id": "verified_lossless",
                "title": "Verified Lossless",
                "path": "flac",
                "function": "will_be_verified_lossless",
                "when": "After transcode detection",
                "inputs": ["converted_count", "is_transcode"],
                "rules": [
                    {"condition": "converted > 0 AND NOT is_transcode",
                     "result": "will_be_verified_lossless = true",
                     "color": "green"},
                    {"condition": "is_transcode OR not converted",
                     "result": "will_be_verified_lossless = false",
                     "color": "amber"},
                ],
            },
            {
                "id": "target_conversion",
                "title": "Target Conversion (Optional)",
                "path": "flac",
                "function": "convert_lossless",
                "when": "After verified lossless, if verified_lossless_target is set",
                "inputs": ["verified_lossless", "verified_lossless_target",
                           "original lossless files"],
                "rules": [
                    {"condition": "verified_lossless AND target configured",
                     "result": "lossless → configured target (V0 discarded)",
                     "color": "green",
                     "effect": "V0 bitrate stored as v0_verification_bitrate"},
                    {"condition": "NOT verified_lossless OR no target configured",
                     "result": "Keep V0 files (standard path)",
                     "color": "amber"},
                ],
                "note": "V0 exists only to verify genuineness. The final target "
                        "may be Opus, MP3, AAC, or any other supported format.",
            },
            {
                "id": "mp3_spectral_gate",
                "title": "Spectral Gate Trigger",
                "path": "mp3",
                "function": "spectral_gate_trigger",
                "when": "MP3 downloads (pre-analysis decision, issue #93)",
                "inputs": ["is_cbr", "is_vbr", "avg_bitrate",
                           "cfg.mp3_vbr.excellent"],
                "rules": [
                    {"condition": "CBR MP3",
                     "result": "would_run", "color": "amber",
                     "effect": "classic transcode-cliff case"},
                    {"condition": "VBR MP3 AND avg unknown",
                     "result": "would_run", "color": "amber",
                     "effect": "conservative default"},
                    {"condition": f"VBR MP3 AND avg < {transcode_threshold}kbps",
                     "result": "would_run", "color": "amber",
                     "effect": "fake V0 transcode territory — analyze"},
                    {"condition": f"VBR MP3 AND avg >= {transcode_threshold}kbps",
                     "result": "skipped_vbr_high_avg", "color": "green",
                     "effect": "genuine V0 range — skip to Quality Comparison"},
                ],
                "outcomes": ["would_run", "skipped_vbr_high_avg",
                             "skipped_flac"],
                "note": f"Threshold ({transcode_threshold}kbps) comes from "
                        f"cfg.mp3_vbr.excellent — same V0 boundary "
                        f"transcode_detection() uses (single source of truth)",
            },
            {
                "id": "mp3_spectral",
                "title": "Spectral Decision",
                "path": "mp3",
                "function": "spectral_import_decision",
                "when": "MP3 downloads where the gate trigger said would_run",
                "inputs": ["spectral_grade", "spectral_bitrate",
                           "existing_spectral_bitrate"],
                "rules": [
                    {"condition": "grade is genuine or marginal",
                     "result": "import", "color": "green"},
                    {"condition": "suspect/likely_transcode AND new_br <= existing",
                     "result": "reject", "color": "red",
                     "effect": "denylist source"},
                    {"condition": "suspect/likely_transcode AND new_br > existing",
                     "result": "import_upgrade", "color": "amber",
                     "effect": "import + denylist"},
                    {"condition": "suspect/likely_transcode AND no existing",
                     "result": "import_no_exist", "color": "amber",
                     "effect": "import (something > nothing)"},
                ],
                "outcomes": ["import", "import_upgrade", "import_no_exist",
                             "reject"],
            },
            {
                "id": "import_decision",
                "title": "Quality Comparison",
                "path": "shared",
                "function": "import_quality_decision",
                "when": "All downloads before beets import",
                "inputs": ["new: AudioQualityMeasurement",
                           "existing: AudioQualityMeasurement | None",
                           "is_transcode"],
                "rules": [
                    {"condition": "new.verified_lossless = true AND compare_quality(new, existing) in {better,equivalent}",
                     "result": "import", "color": "green",
                     "effect": "verified-lossless imports only when it is not worse"},
                    {"condition": "compare_quality(new, existing) = better AND is_transcode",
                     "result": "transcode_upgrade", "color": "amber",
                     "effect": "import + denylist + keep searching"},
                    {"condition": "compare_quality(new, existing) = better AND NOT is_transcode",
                     "result": "import", "color": "green"},
                    {"condition": "compare_quality(new, existing) in {worse,equivalent} AND is_transcode",
                     "result": "transcode_downgrade", "color": "red",
                     "effect": "reject + denylist"},
                    {"condition": "compare_quality(new, existing) in {worse,equivalent}",
                     "result": "downgrade", "color": "red",
                     "effect": "reject"},
                    {"condition": "existing is None AND is_transcode",
                     "result": "transcode_first", "color": "amber",
                     "effect": "import (something > nothing) + denylist"},
                ],
                "outcomes": ["import", "downgrade", "transcode_upgrade",
                             "transcode_downgrade", "transcode_first",
                             "preflight_existing"],
                "note": "Caller resolves override_min_bitrate into "
                        "existing.min_bitrate_kbps",
            },
            {
                "id": "quality_gate",
                "title": "Post-Import Quality Gate",
                "path": "shared",
                "function": "quality_gate_decision",
                "when": "After successful beets import",
                "inputs": ["current: AudioQualityMeasurement"],
                "rules": [
                    {"condition": "rank = measurement_rank(current); spectral clamp applies only when spectral grade is suspect/likely_transcode",
                     "result": "(computed)", "color": "green",
                     "effect": "spectral only lowers the rank when the current on-disk file looks transcode-like"},
                    {"condition": "rank = UNKNOWN OR rank < cfg.gate_min_rank",
                     "result": "requeue_upgrade", "color": "amber",
                     "effect": f"search {QUALITY_UPGRADE_TIERS}"},
                    {"condition": "current.is_cbr AND NOT current.verified_lossless AND rank < LOSSLESS",
                     "result": "requeue_lossless", "color": "amber",
                     "effect": "search lossless only"},
                    {"condition": "else",
                     "result": "accept", "color": "green",
                     "effect": "done"},
                ],
                "outcomes": ["accept", "requeue_upgrade", "requeue_lossless"],
            },
            {
                "id": "dispatch",
                "title": "Import Dispatch",
                "path": "shared",
                "function": "dispatch_action",
                "when": "After import_one.py returns a decision",
                "inputs": ["ImportResult.decision"],
                "rules": [
                    {"condition": "import / preflight_existing",
                     "result": "mark_done + quality_gate", "color": "green",
                     "effect": "imported, run quality gate"},
                    {"condition": "downgrade",
                     "result": "record_rejection + denylist", "color": "red",
                     "effect": "not an upgrade, denylist source"},
                    {"condition": "transcode_upgrade / transcode_first",
                     "result": "mark_done + denylist + requeue", "color": "amber",
                     "effect": "imported but transcode, keep searching"},
                    {"condition": "transcode_downgrade",
                     "result": "record_rejection + denylist + requeue", "color": "red",
                     "effect": "transcode not an upgrade, keep searching"},
                    {"condition": "other (error/crash/timeout)",
                     "result": "record_rejection", "color": "red",
                     "effect": "import failed"},
                ],
                "outcomes": ["import", "preflight_existing", "downgrade",
                             "transcode_upgrade", "transcode_first",
                             "transcode_downgrade", "conversion_failed",
                             "import_failed", "mbid_missing"],
            },
        ],
    }


# ---------------------------------------------------------------------------
# Full pipeline decision — combines all three stages
# ---------------------------------------------------------------------------

def full_pipeline_decision(
    # File properties
    is_flac,
    min_bitrate,
    is_cbr,
    # VBR + avg bitrate for the preimport spectral gate trigger (issue #93).
    # ``is_vbr`` defaults to ``not is_cbr`` when omitted so legacy callers
    # retain current behavior. ``avg_bitrate`` gates VBR MP3 through spectral
    # when below cfg.mp3_vbr.excellent — genuine V0 (~245kbps avg) skips,
    # fake V0 (~180kbps avg) gets analyzed.
    is_vbr: bool | None = None,
    avg_bitrate: int | None = None,
    # Spectral analysis
    spectral_grade=None,
    spectral_bitrate=None,
    # Existing state
    existing_min_bitrate=None,
    existing_avg_bitrate: int | None = None,
    existing_spectral_bitrate=None,
    override_min_bitrate=None,
    existing_format: str | None = None,
    existing_is_cbr: bool = False,
    # Post-conversion (FLAC path only)
    post_conversion_min_bitrate=None,
    converted_count=0,
    # Pipeline state
    verified_lossless=False,
    # Verified lossless target format (e.g. "opus 128", "mp3 v2")
    verified_lossless_target=None,
    # Target format (user intent — "flac" skips conversion)
    target_format=None,
    # New download format label (codec-aware, passed through to measurements)
    new_format: str | None = None,
    # Preimport gates (issue #91). Default to a passing audio check + the auto
    # path so legacy simulator calls don't change behavior.
    audio_check_mode: str = "normal",
    audio_corrupt: bool = False,
    import_mode: str = "auto",
    has_nested_audio: bool = False,
    # Rank-model config (defaults() for legacy callers)
    cfg: "QualityRankConfig | None" = None,
):
    """Run the full decision chain and return the final outcome.

    This simulates what happens when a download completes and flows through
    process_completed_album → import_one.py → _check_quality_gate.

    Codec-aware: when ``new_format`` / ``existing_format`` are supplied, the
    simulator classifies both measurements via quality_rank() — matching
    production dispatch behavior. Legacy callers that omit them still get
    sensible defaults derived from ``is_flac``/``target_format``/``is_cbr``.

    Returns a dict:
        {
            "stage0_spectral_gate": str,  # would spectral analysis run?
            "stage1_spectral": str,       # pre-import spectral decision (None when gate skipped)
            "stage2_import": str,         # import/downgrade/transcode decision
            "stage3_quality_gate": str,   # post-import quality gate decision
            "final_status": str,          # what the pipeline DB ends up as
            "imported": bool,             # whether files were imported to beets
            "denylisted": bool,           # whether source user gets denylisted
            "keep_searching": bool,       # whether the system keeps looking for better
        }
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()
    result: dict[str, Any] = {
        "preimport_audio": None,
        "preimport_nested": None,
        "stage0_spectral_gate": None,
        "stage1_spectral": None,
        "stage2_import": None,
        "stage3_quality_gate": None,
        "final_status": None,
        "imported": False,
        "denylisted": False,
        "keep_searching": False,
        "target_final_format": None,
    }

    # --- Preimport gates (issue #91) ---
    # Ordering mirrors the live flow: lib.import_dispatch.dispatch_import_from_db
    # checks inspection.has_nested_audio *before* calling run_preimport_gates,
    # so a force/manual import of a nested corrupt folder is rejected as
    # nested_layout (not audio_corrupt). The nested gate returns "skipped_auto"
    # on the auto path, which is a no-op — the auto pipeline flattens
    # downloads upstream in process_completed_album, so audio integrity is
    # the first real reject.
    #
    # Post-reject state also mirrors the two live paths:
    #   * Auto-import rejects call reject_and_requeue() which transitions
    #     the request back to "wanted" and bumps the validation attempt
    #     counter → final_status="wanted", keep_searching=True.
    #   * Force/manual-import rejects call _record_rejection_and_maybe_requeue
    #     with requeue=False — the request's current status (often "manual"
    #     or "imported") is left untouched → final_status=None (unchanged),
    #     keep_searching=False.
    nested_outcome = preimport_nested_gate(import_mode, has_nested_audio)
    result["preimport_nested"] = nested_outcome
    if nested_outcome == "reject_nested":
        # Force/manual-only reject — status stays whatever it was.
        result["final_status"] = None
        result["keep_searching"] = False
        return result

    audio_outcome = preimport_audio_gate(audio_check_mode, audio_corrupt)
    result["preimport_audio"] = audio_outcome
    if audio_outcome == "reject_corrupt":
        if import_mode == "auto":
            # Auto-path rejects call reject_and_requeue(), which denylists
            # every source username (album_source.py:280). Mirror that side
            # effect in the simulator so the Decisions tab and
            # pipeline-cli quality don't underreport what the live pipeline
            # actually does on an audio_corrupt reject.
            result["final_status"] = "wanted"
            result["keep_searching"] = True
            result["denylisted"] = True
        else:
            # Force/manual: no status transition, no denylist (live helper
            # _record_rejection_and_maybe_requeue leaves denylisting to the
            # caller's action.denylist, which audio_corrupt rejects don't set).
            result["final_status"] = None
            result["keep_searching"] = False
        return result

    # --- Stage 0: Spectral gate trigger (issue #93) ---
    # Mirrors lib.preimport._needs_spectral_check. Tells the operator
    # whether the preimport spectral gate would even run on this file,
    # so a VBR MP3 transcode masquerading as V0 (avg < threshold) is
    # distinguishable from genuine V0 in simulator output.
    gate = spectral_gate_trigger(
        is_flac=bool(is_flac),
        is_cbr=is_cbr,
        is_vbr=is_vbr,
        avg_bitrate_kbps=avg_bitrate,
        vbr_threshold_kbps=cfg.mp3_vbr.excellent,
    )
    result["stage0_spectral_gate"] = gate

    # --- Stage 1: Pre-import spectral (MP3/CBR path) ---
    # For FLACs, spectral runs inside import_one.py instead, but the
    # logic is the same: detect transcodes before importing.
    #
    # Only run stage 1 when the gate would actually execute. For VBR MP3
    # with high avg bitrate, production skips spectral entirely — so even
    # if the caller supplies a spectral_grade, simulating that gate firing
    # would misrepresent production behavior.
    stage0_gates_stage1 = gate == "would_run" or is_flac
    if spectral_grade and stage0_gates_stage1:
        result["stage1_spectral"] = spectral_import_decision(
            spectral_grade, spectral_bitrate, existing_spectral_bitrate or 0)

        if result["stage1_spectral"] == "reject":
            result["final_status"] = "wanted"  # stays wanted, denylist user
            result["denylisted"] = True
            result["keep_searching"] = True
            return result

    # --- Stage 2: Import decision ---
    # Existing measurement — carries format if the caller provided one,
    # otherwise defaults to "MP3" so legacy simulator scenarios (which only
    # carry a min_bitrate) still classify against the MP3 VBR/CBR band
    # tables. Production always provides a real format via BeetsDB.
    #
    # avg/median fall back to min_bitrate when the caller didn't supply a
    # separate avg (legacy scenarios). Supplying existing_avg_bitrate matters
    # under the default cfg.bitrate_metric=AVG policy — otherwise a VBR album
    # with avg=245 but min=180 gets ranked off min=180 (GOOD instead of
    # TRANSPARENT) and downstream comparisons misrepresent production.
    effective_existing_format = existing_format if existing_format is not None else "MP3"
    _effective_existing_min = (override_min_bitrate
                               if override_min_bitrate is not None
                               else existing_min_bitrate)
    _effective_existing_avg = (override_min_bitrate
                               if override_min_bitrate is not None
                               else (existing_avg_bitrate
                                     if existing_avg_bitrate is not None
                                     else existing_min_bitrate))
    existing_m = (AudioQualityMeasurement(
                      min_bitrate_kbps=_effective_existing_min,
                      avg_bitrate_kbps=_effective_existing_avg,
                      median_bitrate_kbps=_effective_existing_avg,
                      format=effective_existing_format,
                      is_cbr=existing_is_cbr)
                  if existing_min_bitrate is not None else None)

    if is_flac and target_format in ("flac", "lossless"):
        # FLAC kept on disk (no conversion).
        stage2_new_format = new_format or "flac"
        new_m = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=min_bitrate,
            median_bitrate_kbps=min_bitrate,
            format=stage2_new_format)
        result["stage2_import"] = import_quality_decision(
            new_m, existing_m, cfg=cfg)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"
            result["keep_searching"] = True
            return result
        result["imported"] = True

        # Genuine FLAC on disk is verified lossless (for quality gate)
        if spectral_grade in ("genuine", "marginal", None):
            verified_lossless = True

        gate_bitrate = min_bitrate
        gate_avg_bitrate = min_bitrate  # FLAC: lossless, avg == min is fine
        gate_cbr = False
        gate_format = stage2_new_format  # "flac"
    elif is_flac:
        # FLAC path: convert first, then decide
        is_transcode = transcode_detection(
            converted_count, post_conversion_min_bitrate,
            spectral_grade=spectral_grade, cfg=cfg)
        import_br = post_conversion_min_bitrate if post_conversion_min_bitrate else min_bitrate

        will_be_verified = (converted_count > 0 and not is_transcode)
        stage2_new_format = comparison_format_hint(
            explicit_format=new_format,
            verified_lossless_target=(
                verified_lossless_target if will_be_verified else None),
            converted_count=converted_count,
            is_transcode=is_transcode,
        )
        new_m = AudioQualityMeasurement(
            min_bitrate_kbps=import_br,
            avg_bitrate_kbps=import_br,
            median_bitrate_kbps=import_br,
            format=stage2_new_format,
            verified_lossless=will_be_verified)
        result["stage2_import"] = import_quality_decision(
            new_m, existing_m, is_transcode, cfg=cfg)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"  # keeps existing
            result["keep_searching"] = True
            return result
        elif result["stage2_import"] == "transcode_downgrade":
            result["final_status"] = "wanted"
            result["denylisted"] = True
            result["keep_searching"] = True
            return result
        elif result["stage2_import"] in ("transcode_upgrade", "transcode_first"):
            result["imported"] = True
            result["denylisted"] = True
            result["keep_searching"] = True
            # Still runs quality gate after import
        else:
            result["imported"] = True

        # For genuine FLAC→V0, set verified_lossless
        if (converted_count > 0 and not is_transcode and
                spectral_grade in ("genuine", "marginal", None)):
            verified_lossless = True

        # Target format conversion: if verified lossless + target configured,
        # use the target label for the quality gate (e.g. "opus 128") so the
        # rank model classifies against the actual on-disk contract.
        if verified_lossless and verified_lossless_target:
            result["target_final_format"] = verified_lossless_target
            gate_format = verified_lossless_target
        else:
            gate_format = stage2_new_format

        # Use post-conversion bitrate for quality gate. The simulator
        # doesn't take a separate post-conversion avg, so avg == min here;
        # in production the real avg comes from beets after import.
        gate_bitrate = post_conversion_min_bitrate or min_bitrate
        gate_avg_bitrate = gate_bitrate
        gate_cbr = False  # V0 conversion always produces VBR
    else:
        # MP3 path: import directly. No format label for native MP3 downloads
        # unless the caller provided one — the rank model falls back to the
        # bare-codec bitrate classification via `new_format=None`.
        #
        # Use the caller-supplied avg_bitrate when present (falls back to
        # min_bitrate otherwise). Under the default cfg.bitrate_metric=AVG
        # policy a VBR V0 at min=200/avg=245 must rank on avg=245 — otherwise
        # the import/downgrade comparison and the post-import gate both see
        # the wrong tier.
        stage2_new_format = comparison_format_hint(
            explicit_format=new_format,
            native_codec_family="MP3",
        )
        _mp3_avg = avg_bitrate if avg_bitrate is not None else min_bitrate
        new_m = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=_mp3_avg,
            median_bitrate_kbps=_mp3_avg,
            format=stage2_new_format,
            is_cbr=is_cbr)
        result["stage2_import"] = import_quality_decision(
            new_m, existing_m, cfg=cfg)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"  # keeps existing
            result["keep_searching"] = True
            return result

        result["imported"] = True
        gate_bitrate = min_bitrate
        gate_avg_bitrate = _mp3_avg
        gate_cbr = is_cbr
        gate_format = stage2_new_format

    # --- Stage 3: Post-import quality gate ---
    gate_spectral_bitrate = None
    effective_gate_bitrate = compute_effective_override_bitrate(
        gate_bitrate, spectral_bitrate, spectral_grade)
    if (gate_bitrate is not None
            and effective_gate_bitrate is not None
            and effective_gate_bitrate < gate_bitrate):
        gate_spectral_bitrate = spectral_bitrate
    gate_m = AudioQualityMeasurement(
        min_bitrate_kbps=gate_bitrate,
        avg_bitrate_kbps=gate_avg_bitrate,
        median_bitrate_kbps=gate_avg_bitrate,
        format=gate_format,
        is_cbr=gate_cbr,
        verified_lossless=verified_lossless,
        spectral_bitrate_kbps=gate_spectral_bitrate)
    result["stage3_quality_gate"] = quality_gate_decision(gate_m, cfg=cfg)

    if result["stage3_quality_gate"] == "accept":
        result["final_status"] = "imported"
    elif result["stage3_quality_gate"] == "requeue_upgrade":
        result["final_status"] = "wanted"
        result["denylisted"] = True
        result["keep_searching"] = True
    elif result["stage3_quality_gate"] == "requeue_lossless":
        result["final_status"] = "wanted"
        result["keep_searching"] = True

    return result


# --- Repair / orphan detection (pure functions) ---

@dataclass(frozen=True)
class OrphanInfo:
    """A detected inconsistency in pipeline DB state."""
    request_id: int
    issue_type: str  # "corrupt_downloading", "orphaned_download"
    detail: str


@dataclass(frozen=True)
class RepairAction:
    """Suggested repair for a detected inconsistency."""
    request_id: int
    action: str  # "reset_to_wanted", "manual_review"
    detail: str


def find_orphaned_downloads(
    db_rows: list[dict[str, Any]],
    active_transfers: set[tuple[str, str]],
) -> list[OrphanInfo]:
    """Detect downloading rows whose slskd transfers no longer exist. Pure — no I/O.

    Args:
        db_rows: album_requests rows (must include status, active_download_state).
        active_transfers: set of (username, filename) tuples from slskd API.

    Returns OrphanInfo for each downloading row where NONE of its files
    appear in active_transfers.
    """
    issues: list[OrphanInfo] = []
    for row in db_rows:
        if row["status"] != "downloading":
            continue
        state = row.get("active_download_state")
        if not state:
            continue  # corrupt_downloading — handled by find_inconsistencies
        files = state.get("files", [])
        if not files:
            continue
        has_active = any(
            (f.get("username"), f.get("filename")) in active_transfers
            for f in files
        )
        if not has_active:
            usernames = sorted(set(f.get("username", "?") for f in files))
            issues.append(OrphanInfo(
                request_id=row["id"],
                issue_type="orphaned_download",
                detail=f"no active slskd transfers (users: {', '.join(usernames)})"))
    return issues


def find_inconsistencies(db_rows: list[dict[str, Any]]) -> list[OrphanInfo]:
    """Detect inconsistent rows in album_requests. Pure — no I/O.

    Checks:
    - downloading row with no active_download_state (corrupt crash recovery)

    ``imported_path`` is NOT checked against status: it means "files are on
    disk at this path" and survives a status=wanted re-queue (transcode
    upgrade, quality-gate upgrade search). Clearing it on status=wanted
    would wipe the correct beets destination for any album the pipeline is
    actively searching for a better version of. See issue #93.
    """
    issues: list[OrphanInfo] = []
    for row in db_rows:
        rid = row["id"]
        status = row["status"]
        state = row.get("active_download_state")

        if status == "downloading" and not state:
            issues.append(OrphanInfo(
                request_id=rid,
                issue_type="corrupt_downloading",
                detail="downloading with no active_download_state"))

    return issues


def suggest_repair(issue: OrphanInfo) -> RepairAction:
    """Suggest a repair action for a detected inconsistency. Pure."""
    if issue.issue_type in ("corrupt_downloading", "orphaned_download"):
        return RepairAction(
            request_id=issue.request_id,
            action="reset_to_wanted",
            detail="Reset downloading row to wanted (transfers gone)")
    else:
        return RepairAction(
            request_id=issue.request_id,
            action="manual_review",
            detail=f"Unknown issue type: {issue.issue_type}")
