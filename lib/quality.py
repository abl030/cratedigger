"""Quality decision logic for the download pipeline.

Pure functions — no database, no filesystem, no external dependencies.
Used by soularr.py and import_one.py, tested directly against real audio fixtures.
"""

import json
from dataclasses import dataclass, field, asdict
from typing import Optional

QUALITY_UPGRADE_TIERS = "flac,mp3 v0,mp3 320"
QUALITY_MIN_BITRATE_KBPS = 210  # V0 floor — below this triggers upgrade
TRANSCODE_MIN_BITRATE_KBPS = 210  # V0 from genuine lossless is always >= this

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

    def get(self, key: str, default: object = None) -> object:
        """Dict-style .get() for backward compatibility with bv_result dict access."""
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> object:
        """Dict-style ["key"] access for backward compatibility."""
        return getattr(self, key)


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


IMPORT_RESULT_SENTINEL = "__IMPORT_RESULT__"


# ---------------------------------------------------------------------------
# Download info — typed replacement for the untyped dl_info dict
# ---------------------------------------------------------------------------

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
    spectral_grade: Optional[str] = None
    spectral_bitrate: Optional[int] = None
    existing_min_bitrate: Optional[int] = None
    existing_spectral_bitrate: Optional[int] = None
    # Full import_one.py result (JSON string)
    import_result: Optional[str] = None
    # Full validation result (JSON string)
    validation_result: Optional[str] = None


# ---------------------------------------------------------------------------
# Structured result from import_one.py
# ---------------------------------------------------------------------------

@dataclass
class ConversionInfo:
    """FLAC→V0 conversion details."""
    converted: int = 0
    failed: int = 0
    was_converted: bool = False
    original_filetype: Optional[str] = None
    target_filetype: Optional[str] = None


@dataclass
class QualityInfo:
    """Bitrate and quality decision data."""
    new_min_bitrate: Optional[int] = None
    prev_min_bitrate: Optional[int] = None
    post_conversion_min_bitrate: Optional[int] = None
    is_transcode: bool = False
    will_be_verified_lossless: bool = False


@dataclass
class SpectralInfo:
    """Spectral analysis results for new and existing files."""
    grade: Optional[str] = None
    bitrate: Optional[int] = None
    cliff_freq_hz: Optional[int] = None
    suspect_pct: float = 0.0
    per_track: list[dict] = field(default_factory=list)  # per-track grade/hf_deficit/cliff
    existing_grade: Optional[str] = None
    existing_bitrate: Optional[int] = None
    existing_suspect_pct: float = 0.0


@dataclass
class PostflightInfo:
    """Beets post-import verification data."""
    beets_id: Optional[int] = None
    track_count: Optional[int] = None
    imported_path: Optional[str] = None
    bad_extensions: list[str] = field(default_factory=list)  # files with non-audio extensions


@dataclass
class ImportResult:
    """Structured result emitted by import_one.py as JSON.

    Carries every piece of data that crosses the subprocess boundary
    from import_one.py back to soularr.py. Stored in download_log.import_result
    for complete auditability.
    """
    version: int = 1
    exit_code: int = 0
    decision: Optional[str] = None      # from import_quality_decision() or error label
    already_in_beets: bool = False
    conversion: ConversionInfo = field(default_factory=ConversionInfo)
    quality: QualityInfo = field(default_factory=QualityInfo)
    spectral: SpectralInfo = field(default_factory=SpectralInfo)
    postflight: PostflightInfo = field(default_factory=PostflightInfo)
    beets_log: list[str] = field(default_factory=list)  # beets stderr lines from import
    error: Optional[str] = None

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(asdict(self))

    def to_sentinel_line(self) -> str:
        """Format as the stdout sentinel line for subprocess communication."""
        return IMPORT_RESULT_SENTINEL + self.to_json()

    @classmethod
    def from_dict(cls, d: dict) -> "ImportResult":
        """Construct from a dict (e.g. parsed JSON)."""
        return cls(
            version=d.get("version", 1),
            exit_code=d.get("exit_code", 0),
            decision=d.get("decision"),
            already_in_beets=d.get("already_in_beets", False),
            conversion=ConversionInfo(**d["conversion"]) if "conversion" in d else ConversionInfo(),
            quality=QualityInfo(**d["quality"]) if "quality" in d else QualityInfo(),
            spectral=SpectralInfo(**d["spectral"]) if "spectral" in d else SpectralInfo(),
            postflight=PostflightInfo(**d["postflight"]) if "postflight" in d else PostflightInfo(),
            beets_log=d.get("beets_log", []),
            error=d.get("error"),
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

def spectral_import_decision(spectral_grade, spectral_bitrate, existing_spectral_bitrate):
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
    """
    if spectral_grade not in ("suspect", "likely_transcode"):
        return "import"

    new_q = spectral_bitrate or 0
    existing_q = existing_spectral_bitrate or 0

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

def import_quality_decision(new_min_bitrate, existing_min_bitrate, override_min_bitrate=None,
                            is_transcode=False, will_be_verified_lossless=False):
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
        new_min_bitrate:      min bitrate of new files (kbps)
        existing_min_bitrate: min bitrate in beets for this MBID (kbps), or None
        override_min_bitrate: pipeline DB override (when beets value is wrong), or None
        is_transcode:         True if FLAC→V0 produced sub-210kbps output
        will_be_verified_lossless: True if genuine FLAC was converted to V0
    """
    # Genuine FLAC→V0 always wins — V0 bitrate is numerically lower than
    # CBR 320 but objectively better quality (verified lossless source).
    if will_be_verified_lossless:
        return "import"

    effective_existing = override_min_bitrate if override_min_bitrate is not None else existing_min_bitrate

    if effective_existing is not None and new_min_bitrate is not None:
        if new_min_bitrate <= effective_existing:
            if is_transcode:
                return "transcode_downgrade"
            return "downgrade"
        else:
            if is_transcode:
                return "transcode_upgrade"
            return "import"
    elif existing_min_bitrate is None and is_transcode:
        return "transcode_first"
    else:
        return "import"


def transcode_detection(converted_count, post_conversion_min_bitrate):
    """Detect whether a FLAC→V0 conversion produced a transcode.

    Called in import_one.py after convert_flac_to_v0().

    Returns True if the converted files are likely transcodes
    (MP3 wrapped in FLAC container).

    Inputs:
        converted_count:            number of FLAC files converted
        post_conversion_min_bitrate: min bitrate after conversion (kbps), or None
    """
    if converted_count == 0:
        return False
    if post_conversion_min_bitrate is None:
        return False
    return post_conversion_min_bitrate < TRANSCODE_MIN_BITRATE_KBPS


# ---------------------------------------------------------------------------
# Verified lossless derivation (post-import, used by album_source.py)
# ---------------------------------------------------------------------------

def is_verified_lossless(was_converted: bool, original_filetype: Optional[str],
                         spectral_grade: Optional[str]) -> bool:
    """Determine if an import should be marked as verified lossless.

    True only when we converted a genuine FLAC to V0 — the gold standard.

    Inputs:
        was_converted:     True if FLAC files were converted to MP3 V0
        original_filetype: filetype before conversion (e.g. "flac")
        spectral_grade:    spectral analysis grade of the source files
    """
    return (bool(was_converted)
            and original_filetype is not None
            and original_filetype.lower() == "flac"
            and spectral_grade == "genuine")


# ---------------------------------------------------------------------------
# Post-import quality gate (runs after successful import in soularr.py)
# ---------------------------------------------------------------------------

def quality_gate_decision(min_bitrate, is_cbr, verified_lossless, spectral_bitrate=None):
    """Pure decision logic for the post-import quality gate.

    Returns one of: "accept", "requeue_upgrade", "requeue_flac".

    Inputs:
        min_bitrate:      min track bitrate in kbps (from beets DB)
        is_cbr:           True if all tracks have the same bitrate
        verified_lossless: True if imported from spectral-verified genuine FLAC
        spectral_bitrate: estimated original bitrate from spectral cliff detection (kbps)
    """
    gate_br = min_bitrate

    # Spectral bitrate overrides if lower (catches fake 320s)
    if spectral_bitrate is not None and spectral_bitrate < gate_br:
        gate_br = spectral_bitrate

    # Verified lossless overrides low bitrate (quiet/simple music is fine)
    if verified_lossless and gate_br < QUALITY_MIN_BITRATE_KBPS:
        gate_br = QUALITY_MIN_BITRATE_KBPS  # force pass

    if gate_br < QUALITY_MIN_BITRATE_KBPS:
        return "requeue_upgrade"
    elif not verified_lossless and is_cbr:
        return "requeue_flac"
    else:
        return "accept"


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
    if is_flac:
        # FLAC path: convert first, then decide
        is_transcode = transcode_detection(converted_count, post_conversion_min_bitrate)
        import_br = post_conversion_min_bitrate if post_conversion_min_bitrate else min_bitrate

        will_be_verified = (converted_count > 0 and not is_transcode)
        result["stage2_import"] = import_quality_decision(
            import_br, existing_min_bitrate, override_min_bitrate, is_transcode,
            will_be_verified_lossless=will_be_verified)

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

        # Use post-conversion bitrate for quality gate
        gate_bitrate = post_conversion_min_bitrate or min_bitrate
        gate_cbr = False  # V0 conversion always produces VBR
    else:
        # MP3 path: import directly
        result["stage2_import"] = import_quality_decision(
            min_bitrate, existing_min_bitrate, override_min_bitrate)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"  # keeps existing
            result["keep_searching"] = True
            return result

        result["imported"] = True
        gate_bitrate = min_bitrate
        gate_cbr = is_cbr

    # --- Stage 3: Post-import quality gate ---
    result["stage3_quality_gate"] = quality_gate_decision(
        gate_bitrate, gate_cbr, verified_lossless, spectral_bitrate)

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
