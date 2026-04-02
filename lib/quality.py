"""Quality decision logic for the download pipeline.

Pure functions — no database, no filesystem, no external dependencies.
Used by soularr.py and import_one.py, tested directly against real audio fixtures.
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

_logger = logging.getLogger("soularr")

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
    # Verified lossless override (from import_one.py)
    verified_lossless_override: Optional[bool] = None
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
    disambiguated: bool = False  # True if beet move ran to fix %aunique paths


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


# ---------------------------------------------------------------------------
# Filetype verification — moved from soularr.py for testability
# ---------------------------------------------------------------------------


def verify_filetype(file: dict[str, Any] | Any, allowed_filetype: str) -> bool:
    """Check whether a slskd file dict matches an allowed filetype specification.

    Handles: bare extension ("mp3"), exact bitrate ("mp3 320"),
    bitdepth/samplerate ("flac 24/96"), VBR presets ("mp3 v0", "mp3 v2"),
    and minimum bitrate ("aac 256+").
    """
    current_filetype = file["filename"].split(".")[-1]
    bitdepth = None
    samplerate = None
    bitrate = None

    if "bitRate" in file:
        bitrate = file["bitRate"]
    if "sampleRate" in file:
        samplerate = file["sampleRate"]
    if "bitDepth" in file:
        bitdepth = file["bitDepth"]

    # Check if the types match up for the current files type and the current type from the config
    if current_filetype == allowed_filetype.split(" ")[0]:
        # Check if the current type from the config specifies other attributes than the filetype (bitrate etc)
        if " " in allowed_filetype:
            selected_attributes = allowed_filetype.split(" ")[1]
            # If it is a bitdepth/samplerate pair instead of a simple bitrate
            if "/" in selected_attributes:
                selected_bitdepth = selected_attributes.split("/")[0]
                try:
                    selected_samplerate = str(int(float(selected_attributes.split("/")[1]) * 1000))
                except (ValueError, IndexError):
                    _logger.warning("Invalid samplerate in selected_attributes")
                    return False

                if bitdepth and samplerate:
                    return str(bitdepth) == str(selected_bitdepth) and str(samplerate) == str(selected_samplerate)
                return False
            # If it is a VBR quality preset (e.g. "mp3 v0", "mp3 v2")
            elif selected_attributes.lower() in ("v0", "v2"):
                if bitrate:
                    cbr_values = {128, 160, 192, 224, 256, 320}
                    is_vbr = bitrate not in cbr_values
                    # Prefer isVariableBitRate flag from slskd if available
                    if "isVariableBitRate" in file:
                        is_vbr = file["isVariableBitRate"]
                    if not is_vbr:
                        return False
                    if selected_attributes.lower() == "v0":
                        return 220 <= bitrate <= 280
                    else:  # v2
                        return 170 <= bitrate <= 220
                return False
            # If it is a minimum bitrate (e.g. "aac 256+", "ogg 256+", "opus 192+")
            elif selected_attributes.endswith("+"):
                try:
                    min_bitrate = int(selected_attributes[:-1])
                except ValueError:
                    _logger.warning(f"Invalid minimum bitrate in allowed_filetype: {allowed_filetype}")
                    return False
                if bitrate:
                    return bitrate >= min_bitrate
                return False
            # If it is an exact bitrate
            else:
                selected_bitrate = selected_attributes
                if bitrate:
                    if str(bitrate) == str(selected_bitrate):
                        return True
                return False
        # If no bitrate or other info then it is a match so return true
        else:
            return True
    else:
        return False


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
                "inputs": ["new_min_bitrate", "existing_min_bitrate",
                           "override_min_bitrate", "is_transcode",
                           "will_be_verified_lossless"],
                "rules": [
                    {"condition": "will_be_verified_lossless = true",
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
                    {"condition": "no existing AND is_transcode",
                     "result": "transcode_first", "color": "amber",
                     "effect": "import (something > nothing) + denylist"},
                ],
                "outcomes": ["import", "downgrade", "transcode_upgrade",
                             "transcode_downgrade", "transcode_first",
                             "preflight_existing"],
                "note": "effective_existing = override_min_bitrate ?? "
                        "existing_min_bitrate",
            },
            {
                "id": "quality_gate",
                "title": "Post-Import Quality Gate",
                "path": "shared",
                "function": "quality_gate_decision",
                "when": "After successful beets import",
                "inputs": ["beets_min_bitrate", "is_cbr",
                           "verified_lossless", "spectral_bitrate"],
                "rules": [
                    {"condition": "gate_br = min(beets_br, spectral_br)",
                     "result": "(computed)", "color": "green",
                     "effect": "spectral overrides container if lower"},
                    {"condition": f"verified_lossless AND gate_br < "
                                  f"{QUALITY_MIN_BITRATE_KBPS}",
                     "result": f"gate_br = {QUALITY_MIN_BITRATE_KBPS}",
                     "color": "green",
                     "effect": "lo-fi pass"},
                    {"condition": f"gate_br < {QUALITY_MIN_BITRATE_KBPS}kbps",
                     "result": "requeue_upgrade", "color": "amber",
                     "effect": f"search {QUALITY_UPGRADE_TIERS}"},
                    {"condition": "CBR AND NOT verified_lossless",
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
        is_transcode = transcode_detection(converted_count, post_conversion_min_bitrate,
                                           spectral_grade=spectral_grade)
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
