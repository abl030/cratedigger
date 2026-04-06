"""Quality decision logic for the download pipeline.

Pure functions — no database, no filesystem, no external dependencies.
Used by soularr.py and import_one.py, tested directly against real audio fixtures.
"""

import enum
import json
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

QUALITY_UPGRADE_TIERS = "flac,mp3 v0,mp3 320"
QUALITY_FLAC_ONLY = "flac"

# Friendly names for CLI/web UI → search_filetype_override DB values
INTENT_NAMES: dict[str, str | None] = {
    "best_effort": None,
    "flac_only": QUALITY_FLAC_ONLY,
    "flac": QUALITY_FLAC_ONLY,
    "upgrade": QUALITY_UPGRADE_TIERS,
}


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


@dataclass
class HarnessItem:
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


@dataclass
class HarnessTrackInfo:
    """MusicBrainz track info as seen by the beets harness."""
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


@dataclass
class TrackMapping:
    """Which local item matched which MB track."""
    item: HarnessItem = field(default_factory=HarnessItem)
    track: HarnessTrackInfo = field(default_factory=HarnessTrackInfo)

    @classmethod
    def from_dict(cls, d: dict) -> "TrackMapping":
        return cls(
            item=HarnessItem(**d["item"]) if "item" in d else HarnessItem(),
            track=HarnessTrackInfo(**d["track"]) if "track" in d else HarnessTrackInfo(),
        )


@dataclass
class CandidateSummary:
    """Full beets candidate match data for audit logging.

    Stores everything the harness sends — every field from AlbumInfo,
    the distance breakdown, track mapping, and extra items/tracks
    with full detail.
    """
    # Core identity
    mbid: str = ""
    artist: str = ""
    album: str = ""
    distance: float = 0.0
    distance_breakdown: dict[str, float] = field(default_factory=dict)
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
    albumtypes: list[str] = field(default_factory=list)
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
    tracks: list[HarnessTrackInfo] = field(default_factory=list)
    mapping: list[TrackMapping] = field(default_factory=list)
    extra_items: list[HarnessItem] = field(default_factory=list)
    extra_tracks: list[HarnessTrackInfo] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> "CandidateSummary":
        """Deserialize from a dict, constructing typed inner objects."""
        tracks = [HarnessTrackInfo(**t) for t in d.get("tracks", [])]
        mapping = [TrackMapping.from_dict(m) for m in d.get("mapping", [])]
        extra_items = [HarnessItem(**i) for i in d.get("extra_items", [])]
        extra_tracks = [HarnessTrackInfo(**t) for t in d.get("extra_tracks", [])]
        return cls(
            mbid=d.get("mbid", d.get("album_id", "")),
            artist=d.get("artist", ""),
            album=d.get("album", ""),
            distance=d.get("distance", 0.0),
            distance_breakdown=d.get("distance_breakdown", {}),
            is_target=d.get("is_target", False),
            albumdisambig=d.get("albumdisambig", ""),
            year=d.get("year"),
            original_year=d.get("original_year"),
            country=d.get("country"),
            label=d.get("label"),
            catalognum=d.get("catalognum"),
            media=d.get("media"),
            mediums=d.get("mediums"),
            albumtype=d.get("albumtype"),
            albumtypes=d.get("albumtypes", []),
            albumstatus=d.get("albumstatus"),
            releasegroup_id=d.get("releasegroup_id", ""),
            release_group_title=d.get("release_group_title", ""),
            va=d.get("va", False),
            language=d.get("language"),
            script=d.get("script"),
            data_source=d.get("data_source", ""),
            barcode=d.get("barcode", ""),
            asin=d.get("asin", ""),
            track_count=d.get("track_count", 0),
            tracks=tracks,
            mapping=mapping,
            extra_items=extra_items,
            extra_tracks=extra_tracks,
        )


@dataclass
class ValidationResult:
    """Structured result from beets validation + audio integrity check.

    Accumulated through the validation pipeline:
    1. beets_validate() populates candidates, distance, scenario
    2. Audio integrity check may set scenario=audio_corrupt + corrupt_files
    3. soularr.py populates source info (username, folder, failed_path, denylisted)

    Stored in download_log.validation_result (JSONB) for complete auditability.
    """
    valid: bool = False
    distance: Optional[float] = None
    scenario: Optional[str] = None
    detail: Optional[str] = None
    mbid_found: bool = False
    target_mbid: Optional[str] = None
    candidate_count: int = 0
    candidates: list[CandidateSummary] = field(default_factory=list)
    # Local file info (from harness choose_match items)
    items: list[dict] = field(default_factory=list)
    local_track_count: Optional[int] = None
    recommendation: Optional[str] = None        # beets confidence: "strong", "medium", "none"
    path: Optional[str] = None                  # album path being validated
    # Source info (populated by soularr.py)
    soulseek_username: Optional[str] = None
    download_folder: Optional[str] = None
    failed_path: Optional[str] = None
    denylisted_users: list[str] = field(default_factory=list)
    # Audio integrity
    corrupt_files: list[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(asdict(self))

    @classmethod
    def from_dict(cls, d: dict) -> "ValidationResult":
        """Construct from a dict (e.g. parsed JSON)."""
        candidates = [
            CandidateSummary.from_dict(c) for c in d.get("candidates", [])
        ]
        return cls(
            valid=d.get("valid", False),
            distance=d.get("distance"),
            scenario=d.get("scenario"),
            detail=d.get("detail"),
            mbid_found=d.get("mbid_found", False),
            target_mbid=d.get("target_mbid"),
            candidate_count=d.get("candidate_count", 0),
            candidates=candidates,
            items=d.get("items", []),
            local_track_count=d.get("local_track_count"),
            recommendation=d.get("recommendation"),
            path=d.get("path"),
            soulseek_username=d.get("soulseek_username"),
            download_folder=d.get("download_folder"),
            failed_path=d.get("failed_path"),
            denylisted_users=d.get("denylisted_users", []),
            corrupt_files=d.get("corrupt_files", []),
            error=d.get("error"),
        )

    @classmethod
    def from_json(cls, s: str) -> "ValidationResult":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(s))


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

    Replaces the untyped dl_info dict that was passed through soularr.py,
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
    # Final format on disk (e.g. "opus 128" when Opus conversion used)
    final_format: Optional[str] = None


# ---------------------------------------------------------------------------
# Audio quality measurement — ground truth from ffprobe + spectral
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AudioQualityMeasurement:
    """What we actually measured about a set of audio files.

    Ground truth from ffprobe and spectral analysis. Used by decision functions
    to compare new downloads against existing files and determine quality gate
    outcomes.

    Fields:
        min_bitrate_kbps:      min track bitrate (kbps), None if unmeasurable
        is_cbr:                True if all tracks have the same bitrate
        spectral_grade:        spectral analysis result (genuine/marginal/suspect)
        spectral_bitrate_kbps: estimated original bitrate from spectral cliff
        verified_lossless:     True if imported from spectral-verified genuine lossless
        was_converted_from:    source format before conversion (flac/m4a/wav), None if MP3
    """
    min_bitrate_kbps: Optional[int] = None
    is_cbr: bool = False
    spectral_grade: Optional[str] = None
    spectral_bitrate_kbps: Optional[int] = None
    verified_lossless: bool = False
    was_converted_from: Optional[str] = None


# ---------------------------------------------------------------------------
# Structured result from import_one.py
# ---------------------------------------------------------------------------

@dataclass
class ConversionInfo:
    """FLAC→V0 conversion details and process artifacts."""
    converted: int = 0
    failed: int = 0
    was_converted: bool = False
    original_filetype: Optional[str] = None
    target_filetype: Optional[str] = None
    post_conversion_min_bitrate: Optional[int] = None  # min bitrate after lossless→V0
    is_transcode: bool = False  # True if FLAC was actually a transcode
    final_format: Optional[str] = None  # "opus 128" when Opus conversion used


@dataclass
class SpectralDetail:
    """Per-track spectral analysis detail.

    The album-level spectral grades and bitrates now live on
    AudioQualityMeasurement (new_measurement/existing_measurement on ImportResult).
    This carries the per-track detail data that doesn't fit on a measurement.
    """
    cliff_freq_hz: Optional[int] = None
    suspect_pct: float = 0.0
    per_track: list[dict] = field(default_factory=list)  # per-track grade/hf_deficit/cliff
    existing_suspect_pct: float = 0.0


@dataclass
class PostflightInfo:
    """Beets post-import verification data."""
    beets_id: Optional[int] = None
    track_count: Optional[int] = None
    imported_path: Optional[str] = None
    bad_extensions: list[str] = field(default_factory=list)  # files with non-audio extensions
    disambiguated: bool = False  # True if beet move ran to fix %aunique paths


@dataclass
class ImportResult:
    """Structured result emitted by import_one.py as JSON.

    Carries every piece of data that crosses the subprocess boundary
    from import_one.py back to soularr.py. Stored in download_log.import_result
    for complete auditability.

    new_measurement / existing_measurement carry the coherent quality state
    for the download and what was on disk. The same AudioQualityMeasurement
    type flows through decision functions and the audit trail.
    """
    version: int = 2
    exit_code: int = 0
    decision: Optional[str] = None      # from import_quality_decision() or error label
    already_in_beets: bool = False
    new_measurement: Optional[AudioQualityMeasurement] = None
    existing_measurement: Optional[AudioQualityMeasurement] = None
    conversion: ConversionInfo = field(default_factory=ConversionInfo)
    spectral: SpectralDetail = field(default_factory=SpectralDetail)
    postflight: PostflightInfo = field(default_factory=PostflightInfo)
    beets_log: list[str] = field(default_factory=list)  # beets stderr lines from import
    error: Optional[str] = None
    # Opus audit trail — V0 bitrate that proved genuineness, final format on disk
    v0_verification_bitrate: Optional[int] = None
    final_format: Optional[str] = None  # "opus 128", None means V0/MP3 as before

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(asdict(self))

    def to_sentinel_line(self) -> str:
        """Format as the stdout sentinel line for subprocess communication."""
        return IMPORT_RESULT_SENTINEL + self.to_json()

    @classmethod
    def _migrate_v1(cls, d: dict) -> "ImportResult":
        """Migrate version 1 (QualityInfo + SpectralInfo) to version 2 (measurements)."""
        quality = d.get("quality") or {}
        spectral = d.get("spectral") or {}
        conv_d = dict(d.get("conversion") or {})

        # Migrate process fields from QualityInfo → ConversionInfo
        conv_d.setdefault("post_conversion_min_bitrate",
                          quality.get("post_conversion_min_bitrate"))
        conv_d.setdefault("is_transcode", quality.get("is_transcode", False))

        # Build measurements from scattered fields
        new_m = AudioQualityMeasurement(
            min_bitrate_kbps=quality.get("new_min_bitrate"),
            spectral_grade=spectral.get("grade"),
            spectral_bitrate_kbps=spectral.get("bitrate"),
            verified_lossless=quality.get("will_be_verified_lossless", False),
            was_converted_from=(conv_d.get("original_filetype")
                                if conv_d.get("was_converted") else None),
        )
        existing_m: Optional[AudioQualityMeasurement] = None
        if quality.get("prev_min_bitrate") is not None:
            existing_m = AudioQualityMeasurement(
                min_bitrate_kbps=quality.get("prev_min_bitrate"),
                spectral_grade=spectral.get("existing_grade"),
                spectral_bitrate_kbps=spectral.get("existing_bitrate"),
            )

        return cls(
            version=2,
            exit_code=d.get("exit_code", 0),
            decision=d.get("decision"),
            already_in_beets=d.get("already_in_beets", False),
            new_measurement=new_m,
            existing_measurement=existing_m,
            conversion=ConversionInfo(**conv_d),
            spectral=SpectralDetail(
                cliff_freq_hz=spectral.get("cliff_freq_hz"),
                suspect_pct=spectral.get("suspect_pct", 0.0),
                per_track=spectral.get("per_track", []),
                existing_suspect_pct=spectral.get("existing_suspect_pct", 0.0),
            ),
            postflight=(PostflightInfo(**d["postflight"])
                        if "postflight" in d else PostflightInfo()),
            beets_log=d.get("beets_log", []),
            error=d.get("error"),
        )

    @classmethod
    def from_dict(cls, d: dict) -> "ImportResult":
        """Construct from a dict (e.g. parsed JSON).

        Handles both old (v1 with quality/spectral sub-objects) and new
        (v2 with measurements) formats for backward compat with existing
        download_log JSONB rows.
        """
        # Old format: has "quality" key, no "new_measurement"
        if "quality" in d and "new_measurement" not in d:
            return cls._migrate_v1(d)

        new_m_d = d.get("new_measurement")
        new_m = AudioQualityMeasurement(**new_m_d) if new_m_d else None
        ex_m_d = d.get("existing_measurement")
        ex_m = AudioQualityMeasurement(**ex_m_d) if ex_m_d else None

        return cls(
            version=d.get("version", 2),
            exit_code=d.get("exit_code", 0),
            decision=d.get("decision"),
            already_in_beets=d.get("already_in_beets", False),
            new_measurement=new_m,
            existing_measurement=ex_m,
            conversion=(ConversionInfo(**d["conversion"])
                        if "conversion" in d else ConversionInfo()),
            spectral=(SpectralDetail(**d["spectral"])
                      if "spectral" in d else SpectralDetail()),
            postflight=(PostflightInfo(**d["postflight"])
                        if "postflight" in d else PostflightInfo()),
            beets_log=d.get("beets_log", []),
            error=d.get("error"),
            v0_verification_bitrate=d.get("v0_verification_bitrate"),
            final_format=d.get("final_format"),
        )

    @classmethod
    def from_json(cls, s: str) -> "ImportResult":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(s))


def parse_import_result(stdout_text: str) -> Optional[ImportResult]:
    """Extract ImportResult from import_one.py stdout.

    Scans from the last line backward for the sentinel prefix.
    Returns None if no result found (crash, old version, etc).
    """
    for line in reversed(stdout_text.strip().split("\n")):
        if line.startswith(IMPORT_RESULT_SENTINEL):
            try:
                return ImportResult.from_json(line[len(IMPORT_RESULT_SENTINEL):])
            except (json.JSONDecodeError, TypeError, KeyError):
                return None
    return None


# ---------------------------------------------------------------------------
# Pre-import spectral decision (MP3/CBR path in process_completed_album)
# ---------------------------------------------------------------------------

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

def import_quality_decision(new: AudioQualityMeasurement,
                            existing: "AudioQualityMeasurement | None",
                            is_transcode: bool = False) -> str:
    """Decide whether to import based on bitrate comparison.

    Called in import_one.py after FLAC→V0 conversion (if applicable)
    and before running the beets harness.

    Returns one of:
        "import"              — new files are better (or no existing), proceed
        "downgrade"           — new files are worse, skip (exit 5)
        "transcode_upgrade"   — transcode but better than existing, import + denylist (exit 6)
        "transcode_downgrade" — transcode and not better, skip + denylist (exit 6)
        "transcode_first"     — transcode but nothing on disk yet, import (exit 6)

    Inputs:
        new:           measurement of the new download
        existing:      measurement of what's already in beets, or None
                       (caller resolves override_min_bitrate into existing.min_bitrate_kbps)
        is_transcode:  True if FLAC→V0 produced a transcode (from transcode_detection)
    """
    # Genuine FLAC→V0 always wins — V0 bitrate is numerically lower than
    # CBR 320 but objectively better quality (verified lossless source).
    if new.verified_lossless:
        return "import"

    existing_br = existing.min_bitrate_kbps if existing is not None else None

    if existing_br is not None and new.min_bitrate_kbps is not None:
        if new.min_bitrate_kbps <= existing_br:
            if is_transcode:
                return "transcode_downgrade"
            return "downgrade"
        else:
            if is_transcode:
                return "transcode_upgrade"
            return "import"
    elif existing is None and is_transcode:
        return "transcode_first"
    else:
        return "import"


def transcode_detection(converted_count, post_conversion_min_bitrate,
                        spectral_grade=None):
    """Detect whether a FLAC→V0 conversion produced a transcode.

    Called in import_one.py after convert_flac_to_v0().

    Returns True if the converted files are likely transcodes
    (MP3 wrapped in FLAC container).

    Inputs:
        converted_count:            number of FLAC files converted
        post_conversion_min_bitrate: min bitrate after conversion (kbps), or None
        spectral_grade:             album spectral grade, or None if unavailable
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
    # No spectral data — fall back to bitrate threshold
    return post_conversion_min_bitrate < TRANSCODE_MIN_BITRATE_KBPS


# ---------------------------------------------------------------------------
# Verified lossless derivation (post-import, used by album_source.py)
# ---------------------------------------------------------------------------

def is_verified_lossless(was_converted: bool, original_filetype: Optional[str],
                         spectral_grade: Optional[str]) -> bool:
    """Determine if an import should be marked as verified lossless.

    True when we converted a genuine lossless source to V0 — the gold standard.
    Accepts any lossless format: flac, m4a (ALAC), wav.

    Inputs:
        was_converted:     True if lossless files were converted to MP3 V0
        original_filetype: filetype before conversion (e.g. "flac", "m4a", "wav")
        spectral_grade:    spectral analysis grade of the source files
    """
    if not was_converted or original_filetype is None or spectral_grade != "genuine":
        return False
    ext = original_filetype.lower()
    # Extensions that are lossless — includes m4a (ALAC) since the pipeline
    # only converts m4a files that were downloaded as ALAC
    lossless_exts = {"flac", "m4a", "wav", "alac"}
    return ext in lossless_exts


# ---------------------------------------------------------------------------
# Post-import quality gate (runs after successful import in soularr.py)
# ---------------------------------------------------------------------------

def quality_gate_decision(current: AudioQualityMeasurement) -> str:
    """Pure decision logic for the post-import quality gate.

    Returns one of: "accept", "requeue_upgrade", "requeue_flac".

    Input:
        current: measurement of the files now on disk (from beets DB + spectral)
    """
    gate_br = current.min_bitrate_kbps
    if gate_br is None:
        return "requeue_upgrade"

    # Spectral bitrate overrides if lower (catches fake 320s)
    if current.spectral_bitrate_kbps is not None and current.spectral_bitrate_kbps < gate_br:
        gate_br = current.spectral_bitrate_kbps

    # Verified lossless overrides low bitrate (quiet/simple music is fine)
    if current.verified_lossless and gate_br < QUALITY_MIN_BITRATE_KBPS:
        gate_br = QUALITY_MIN_BITRATE_KBPS  # force pass

    if gate_br < QUALITY_MIN_BITRATE_KBPS:
        return "requeue_upgrade"
    elif not current.verified_lossless and current.is_cbr:
        return "requeue_flac"
    else:
        return "accept"


# ---------------------------------------------------------------------------
# Dispatch logic — extracted from import_dispatch.py for testability
# ---------------------------------------------------------------------------


@dataclass
class DispatchAction:
    """What actions to take after import_one.py returns a decision."""
    mark_done: bool = False
    mark_failed: bool = False
    denylist: bool = False
    requeue: bool = False
    cleanup: bool = True
    trigger_meelo: bool = False
    run_quality_gate: bool = False


def dispatch_action(decision: str) -> DispatchAction:
    """Map an ImportResult.decision string to the set of actions to take (pure).

    Encodes the if/elif dispatch chain from dispatch_import().
    """
    if decision in ("import", "preflight_existing"):
        return DispatchAction(mark_done=True, trigger_meelo=True,
                              run_quality_gate=True, cleanup=True)
    elif decision == "downgrade":
        return DispatchAction(mark_failed=True, denylist=True, cleanup=True)
    elif decision in ("transcode_upgrade", "transcode_first"):
        return DispatchAction(mark_done=True, denylist=True, requeue=True,
                              trigger_meelo=True, cleanup=True)
    elif decision == "transcode_downgrade":
        return DispatchAction(mark_failed=True, denylist=True, requeue=True,
                              cleanup=True)
    else:  # import_failed, conversion_failed, mbid_missing, crash, etc.
        return DispatchAction(mark_failed=True)


def compute_effective_override_bitrate(
    container_bitrate: int | None,
    spectral_bitrate: int | None,
) -> int | None:
    """Compute the effective override bitrate from container and spectral data.

    Returns the lower of the two when both are available (more conservative).
    Used by dispatch_import() to pass --override-min-bitrate to import_one.py,
    and by _check_quality_gate() for spectral override.
    """
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
    if slskd_ft == "flac" or dl_info.was_converted:
        return "flac"
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
) -> str | None:
    """Backfill search_filetype_override for pre-quality-gate albums stuck in download loops.

    When a download is rejected (e.g. downgrade) and search_filetype_override is NULL,
    albums with decent quality on disk keep downloading the same tier forever
    because the quality gate only fires after successful imports.

    Returns QUALITY_FLAC_ONLY when the on-disk state is good enough that only
    a verified lossless source would be an upgrade. Returns None otherwise.
    """
    if verified_lossless:
        return None
    if spectral_grade != "genuine":
        return None
    if min_bitrate_kbps is None:
        return None
    if min_bitrate_kbps >= QUALITY_MIN_BITRATE_KBPS:
        return QUALITY_FLAC_ONLY
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
    """
    parts = config_str.strip().split(" ", 1)
    name = parts[0].lower()

    if name in ("*", "any"):
        return CATCH_ALL_SPEC

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

def get_decision_tree() -> dict[str, Any]:
    """Return the full pipeline decision structure as data.

    The web UI renders this as a diagram. Contract tests verify this matches
    the actual decision functions. When a function changes, update this too —
    the tests will catch divergence.
    """
    return {
        "constants": {
            "QUALITY_MIN_BITRATE_KBPS": QUALITY_MIN_BITRATE_KBPS,
            "TRANSCODE_MIN_BITRATE_KBPS": TRANSCODE_MIN_BITRATE_KBPS,
            "QUALITY_UPGRADE_TIERS": QUALITY_UPGRADE_TIERS,
        },
        "paths": ["flac", "mp3"],
        "path_labels": {"flac": "FLAC path", "mp3": "MP3 path"},
        "stages": [
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
                    {"condition": f"no spectral: post_conv_br < {TRANSCODE_MIN_BITRATE_KBPS}kbps",
                     "result": "is_transcode = true", "color": "red",
                     "effect": "fallback when spectral unavailable"},
                ],
                "note": f"Spectral grade is authoritative when available. "
                        f"Bitrate threshold ({TRANSCODE_MIN_BITRATE_KBPS}kbps) is fallback only",
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
                "id": "opus_conversion",
                "title": "Opus Conversion (Optional)",
                "path": "flac",
                "function": "convert_lossless_to_opus",
                "when": "After verified lossless, if opus_conversion enabled",
                "inputs": ["verified_lossless", "opus_conversion config",
                           "original FLAC files"],
                "rules": [
                    {"condition": "verified_lossless AND opus_conversion enabled",
                     "result": "FLAC → Opus 128kbps (V0 discarded)",
                     "color": "green",
                     "effect": "V0 bitrate stored as v0_verification_bitrate"},
                    {"condition": "NOT verified_lossless OR opus disabled",
                     "result": "Keep V0 files (standard path)",
                     "color": "amber"},
                ],
                "note": "V0 exists only to verify genuineness. "
                        "Opus 128 is ~50% smaller than V0 for identical quality.",
            },
            {
                "id": "mp3_spectral",
                "title": "CBR Spectral Check",
                "path": "mp3",
                "function": "spectral_import_decision",
                "when": "CBR MP3 downloads only (VBR skips this)",
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
                "id": "mp3_vbr_note",
                "title": "VBR MP3",
                "path": "mp3",
                "function": "(no spectral check)",
                "when": "VBR MP3 downloads",
                "inputs": [],
                "rules": [
                    {"condition": "VBR bitrate IS the quality signal",
                     "result": "skip to Quality Comparison", "color": "green"},
                ],
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
                    {"condition": "new.verified_lossless = true",
                     "result": "import", "color": "green",
                     "effect": "V0 from genuine FLAC always wins"},
                    {"condition": "new > existing AND is_transcode",
                     "result": "transcode_upgrade", "color": "amber",
                     "effect": "import + denylist + keep searching"},
                    {"condition": "new > existing AND NOT is_transcode",
                     "result": "import", "color": "green"},
                    {"condition": "new <= existing AND is_transcode",
                     "result": "transcode_downgrade", "color": "red",
                     "effect": "reject + denylist"},
                    {"condition": "new <= existing",
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
                    {"condition": "gate_br = min(current.min_bitrate_kbps, current.spectral_bitrate_kbps)",
                     "result": "(computed)", "color": "green",
                     "effect": "spectral overrides container if lower"},
                    {"condition": f"current.verified_lossless AND gate_br < "
                                  f"{QUALITY_MIN_BITRATE_KBPS}",
                     "result": f"gate_br = {QUALITY_MIN_BITRATE_KBPS}",
                     "color": "green",
                     "effect": "lo-fi pass"},
                    {"condition": f"gate_br < {QUALITY_MIN_BITRATE_KBPS}kbps",
                     "result": "requeue_upgrade", "color": "amber",
                     "effect": f"search {QUALITY_UPGRADE_TIERS}"},
                    {"condition": "current.is_cbr AND NOT current.verified_lossless",
                     "result": "requeue_flac", "color": "amber",
                     "effect": "search flac only"},
                    {"condition": "else",
                     "result": "accept", "color": "green",
                     "effect": "done"},
                ],
                "outcomes": ["accept", "requeue_upgrade", "requeue_flac"],
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
                     "result": "mark_failed + denylist", "color": "red",
                     "effect": "not an upgrade, denylist source"},
                    {"condition": "transcode_upgrade / transcode_first",
                     "result": "mark_done + denylist + requeue", "color": "amber",
                     "effect": "imported but transcode, keep searching"},
                    {"condition": "transcode_downgrade",
                     "result": "mark_failed + denylist + requeue", "color": "red",
                     "effect": "transcode not an upgrade, keep searching"},
                    {"condition": "other (error/crash/timeout)",
                     "result": "mark_failed", "color": "red",
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
    # Spectral analysis
    spectral_grade=None,
    spectral_bitrate=None,
    # Existing state
    existing_min_bitrate=None,
    existing_spectral_bitrate=None,
    override_min_bitrate=None,
    # Post-conversion (FLAC path only)
    post_conversion_min_bitrate=None,
    converted_count=0,
    # Pipeline state
    verified_lossless=False,
    # Opus conversion
    opus_conversion=False,
    # Target format (user intent — "flac" skips conversion)
    target_format=None,
):
    """Run the full decision chain and return the final outcome.

    This simulates what happens when a download completes and flows through
    process_completed_album → import_one.py → _check_quality_gate.

    Returns a dict:
        {
            "stage1_spectral": str,     # pre-import spectral decision
            "stage2_import": str,       # import/downgrade/transcode decision
            "stage3_quality_gate": str,  # post-import quality gate decision
            "final_status": str,        # what the pipeline DB ends up as
            "imported": bool,           # whether files were imported to beets
            "denylisted": bool,         # whether source user gets denylisted
            "keep_searching": bool,     # whether the system keeps looking for better
        }
    """
    result = {
        "stage1_spectral": None,
        "stage2_import": None,
        "stage3_quality_gate": None,
        "final_status": None,
        "imported": False,
        "denylisted": False,
        "keep_searching": False,
        "opus_final_format": None,
    }

    # --- Stage 1: Pre-import spectral (MP3/CBR path) ---
    # For FLACs, spectral runs inside import_one.py instead, but the
    # logic is the same: detect transcodes before importing.
    if spectral_grade:
        result["stage1_spectral"] = spectral_import_decision(
            spectral_grade, spectral_bitrate, existing_spectral_bitrate or 0)

        if result["stage1_spectral"] == "reject":
            result["final_status"] = "wanted"  # stays wanted, denylist user
            result["denylisted"] = True
            result["keep_searching"] = True
            return result

    # --- Stage 2: Import decision ---
    existing_m = (AudioQualityMeasurement(
                      min_bitrate_kbps=override_min_bitrate
                      if override_min_bitrate is not None
                      else existing_min_bitrate)
                  if existing_min_bitrate is not None else None)

    if is_flac and target_format == "flac":
        # FLAC kept on disk (no conversion) — use raw FLAC bitrate.
        # Don't set verified_lossless on new_m for comparison — that would
        # auto-win over existing FLAC at the same bitrate. Use plain bitrate
        # comparison. verified_lossless is set on the album after import.
        new_m = AudioQualityMeasurement(min_bitrate_kbps=min_bitrate)
        result["stage2_import"] = import_quality_decision(new_m, existing_m)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"
            result["keep_searching"] = True
            return result
        result["imported"] = True

        # Genuine FLAC on disk is verified lossless (for quality gate)
        if spectral_grade in ("genuine", "marginal", None):
            verified_lossless = True

        gate_bitrate = min_bitrate
        gate_cbr = False
    elif is_flac:
        # FLAC path: convert first, then decide
        is_transcode = transcode_detection(converted_count, post_conversion_min_bitrate,
                                           spectral_grade=spectral_grade)
        import_br = post_conversion_min_bitrate if post_conversion_min_bitrate else min_bitrate

        will_be_verified = (converted_count > 0 and not is_transcode)
        new_m = AudioQualityMeasurement(min_bitrate_kbps=import_br,
                                        verified_lossless=will_be_verified)
        result["stage2_import"] = import_quality_decision(
            new_m, existing_m, is_transcode)

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

        # Opus conversion: if verified lossless + enabled, final format is Opus 128
        if verified_lossless and opus_conversion:
            result["opus_final_format"] = "opus 128"

        # Use post-conversion bitrate for quality gate
        gate_bitrate = post_conversion_min_bitrate or min_bitrate
        gate_cbr = False  # V0 conversion always produces VBR
    else:
        # MP3 path: import directly
        new_m = AudioQualityMeasurement(min_bitrate_kbps=min_bitrate)
        result["stage2_import"] = import_quality_decision(new_m, existing_m)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"  # keeps existing
            result["keep_searching"] = True
            return result

        result["imported"] = True
        gate_bitrate = min_bitrate
        gate_cbr = is_cbr

    # --- Stage 3: Post-import quality gate ---
    gate_m = AudioQualityMeasurement(min_bitrate_kbps=gate_bitrate, is_cbr=gate_cbr,
                                     verified_lossless=verified_lossless,
                                     spectral_bitrate_kbps=spectral_bitrate)
    result["stage3_quality_gate"] = quality_gate_decision(gate_m)

    if result["stage3_quality_gate"] == "accept":
        result["final_status"] = "imported"
    elif result["stage3_quality_gate"] == "requeue_upgrade":
        result["final_status"] = "wanted"
        result["denylisted"] = True
        result["keep_searching"] = True
    elif result["stage3_quality_gate"] == "requeue_flac":
        result["final_status"] = "wanted"
        result["keep_searching"] = True

    return result


# --- Repair / orphan detection (pure functions) ---

@dataclass(frozen=True)
class OrphanInfo:
    """A detected inconsistency in pipeline DB state."""
    request_id: int
    issue_type: str  # "corrupt_downloading", "stale_imported_path"
    detail: str


@dataclass(frozen=True)
class RepairAction:
    """Suggested repair for a detected inconsistency."""
    request_id: int
    action: str  # "reset_to_wanted", "clear_imported_path", "manual_review"
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
    - wanted/manual row with stale imported_path
    """
    issues: list[OrphanInfo] = []
    for row in db_rows:
        rid = row["id"]
        status = row["status"]
        state = row.get("active_download_state")
        path = row.get("imported_path")

        if status == "downloading" and not state:
            issues.append(OrphanInfo(
                request_id=rid,
                issue_type="corrupt_downloading",
                detail="downloading with no active_download_state"))

        if status in ("wanted", "manual") and path:
            issues.append(OrphanInfo(
                request_id=rid,
                issue_type="stale_imported_path",
                detail=f"status={status} but imported_path={path}"))

    return issues


def suggest_repair(issue: OrphanInfo) -> RepairAction:
    """Suggest a repair action for a detected inconsistency. Pure."""
    if issue.issue_type in ("corrupt_downloading", "orphaned_download"):
        return RepairAction(
            request_id=issue.request_id,
            action="reset_to_wanted",
            detail="Reset downloading row to wanted (transfers gone)")
    elif issue.issue_type == "stale_imported_path":
        return RepairAction(
            request_id=issue.request_id,
            action="clear_imported_path",
            detail="Clear stale imported_path on non-imported row")
    else:
        return RepairAction(
            request_id=issue.request_id,
            action="manual_review",
            detail=f"Unknown issue type: {issue.issue_type}")
