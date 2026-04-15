"""Spectral quality verification for audio files.

Detects transcoded/upsampled audio using sox bandpass filtering and
spectral gradient analysis. Works on FLAC and MP3 files.

Requires: sox in PATH.
"""

import math
import os
import subprocess
from dataclasses import dataclass, field
from typing import Optional

# --- Thresholds ---
HF_DEFICIT_SUSPECT = 60.0   # dB — above this = suspect (no cliff needed)
HF_DEFICIT_MARGINAL = 40.0  # dB — above this = marginal
CLIFF_THRESHOLD_DB_PER_KHZ = -12.0  # steeper than this = cliff
MIN_CLIFF_SLICES = 2        # consecutive steep slices to confirm cliff
ALBUM_SUSPECT_PCT = 60.0    # % of tracks that must be suspect for album flag

# 500Hz slices from 12kHz to 20kHz
SLICE_FREQS = list(range(12000, 20000, 500))
SLICE_WIDTH = 500
DB_FLOOR = -140.0

# LAME lowpass table (from source code) — maps cliff frequency to original bitrate
LAME_LOWPASS = [
    (15100, 96),
    (15600, 112),
    (17000, 128),
    (17500, 160),
    (18600, 192),
    (19400, 224),
    (19700, 256),
    (20500, 320),
]

from lib.quality import AUDIO_EXTENSIONS_DOTTED as AUDIO_EXTENSIONS


# --- Data classes ---

@dataclass
class TrackResult:
    grade: str                                  # "genuine" | "marginal" | "suspect" | "error"
    hf_deficit_db: float = 0.0
    cliff_detected: bool = False
    cliff_freq_hz: Optional[int] = None
    estimated_bitrate_kbps: Optional[int] = None
    error: Optional[str] = None


@dataclass
class AlbumResult:
    grade: str                                  # "genuine" | "suspect" | "likely_transcode"
    estimated_bitrate_kbps: Optional[int] = None
    suspect_pct: float = 0.0
    tracks: list = field(default_factory=list)


# --- Core functions ---

def parse_rms_from_stat(stderr_output):
    """Parse RMS amplitude from sox stat stderr output. Returns float or None."""
    for line in stderr_output.split("\n"):
        if "RMS     amplitude:" in line:
            try:
                return float(line.split()[-1])
            except (ValueError, IndexError):
                return None
    return None


def rms_to_db(rms):
    """Convert RMS amplitude to dB. Returns DB_FLOOR for zero/negative."""
    if rms <= 0:
        return DB_FLOOR
    return 20.0 * math.log10(rms)


def detect_cliff(slices, threshold_db_per_khz=CLIFF_THRESHOLD_DB_PER_KHZ,
                 min_slices=MIN_CLIFF_SLICES, slice_width_hz=SLICE_WIDTH):
    """Detect spectral cliff from a list of {"freq": Hz, "db": dB} slices.

    Returns the frequency (Hz) where the cliff starts, or None.
    """
    if len(slices) < 2:
        return None

    khz_step = slice_width_hz / 1000.0
    cliff_count = 0
    cliff_start = None

    for i in range(1, len(slices)):
        grad = (slices[i]["db"] - slices[i - 1]["db"]) / khz_step
        if grad < threshold_db_per_khz:
            if cliff_count == 0:
                cliff_start = slices[i - 1]["freq"]
            cliff_count += 1
            if cliff_count >= min_slices:
                return cliff_start
        else:
            cliff_count = 0
            cliff_start = None

    return None


def estimate_bitrate_from_cliff(cliff_freq_hz):
    """Estimate original bitrate from cliff frequency using LAME lowpass table.

    The cliff appears at or just below the encoder's lowpass frequency.
    We map cliff frequency ranges to original bitrates.

    Returns estimated bitrate in kbps, or None if no cliff.
    """
    if cliff_freq_hz is None:
        return None

    # Range-based lookup: cliff frequency → original bitrate
    # Ranges derived from LAME lowpass table midpoints
    if cliff_freq_hz < 15400:
        return 96
    elif cliff_freq_hz < 17250:   # 15400-17250 → 128 (lowpass 17000)
        return 128
    elif cliff_freq_hz < 18050:   # 17250-18050 → 160 (lowpass 17500)
        return 160
    elif cliff_freq_hz < 19000:   # 18050-19000 → 192 (lowpass 18600)
        return 192
    elif cliff_freq_hz < 19550:   # 19000-19550 → 256 (lowpass 19700)
        return 256
    else:
        return 320


def classify_track(hf_deficit_db, cliff_freq_hz):
    """Classify a single track based on HF deficit and cliff detection.

    Returns a TrackResult.
    """
    cliff_detected = cliff_freq_hz is not None
    estimated_br = estimate_bitrate_from_cliff(cliff_freq_hz)

    if cliff_detected:
        grade = "suspect"
    elif hf_deficit_db >= HF_DEFICIT_SUSPECT:
        grade = "suspect"
    elif hf_deficit_db >= HF_DEFICIT_MARGINAL:
        grade = "marginal"
    else:
        grade = "genuine"

    return TrackResult(
        grade=grade,
        hf_deficit_db=hf_deficit_db,
        cliff_detected=cliff_detected,
        cliff_freq_hz=cliff_freq_hz,
        estimated_bitrate_kbps=estimated_br,
    )


def classify_album(track_results):
    """Classify album from list of TrackResults. Returns (grade, suspect_pct)."""
    if not track_results:
        return "genuine", 0.0

    suspect = sum(1 for t in track_results if t.grade == "suspect")
    total = len(track_results)
    pct = suspect / total * 100.0

    if pct >= 75:
        grade = "likely_transcode"
    elif pct >= ALBUM_SUSPECT_PCT:
        grade = "suspect"
    else:
        grade = "genuine"

    return grade, pct


# --- Sox interaction ---

def _get_band_rms(filepath, lo_hz, hi_hz, trim_seconds=30):
    """Get RMS amplitude of audio filtered to a frequency band via sox."""
    cmd = ["sox", filepath, "-n"]
    if trim_seconds:
        cmd.extend(["trim", "0", str(trim_seconds)])
    cmd.extend(["sinc", "%d-%d" % (lo_hz, hi_hz), "stat"])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return parse_rms_from_stat(result.stderr)


def analyze_track(filepath, trim_seconds=30):
    """Analyze a single audio file for spectral quality.

    Runs 17 sox commands (1 reference band + 16 test slices).
    Returns a TrackResult.
    """
    try:
        # Reference band: 1-4kHz
        ref_rms = _get_band_rms(filepath, 1000, 4000, trim_seconds)
        if ref_rms is None or ref_rms < 0.000001:
            return TrackResult(grade="genuine", hf_deficit_db=0.0)

        ref_db = rms_to_db(ref_rms)

        # 16 test slices from 12-20kHz
        slices = []
        for freq in SLICE_FREQS:
            rms = _get_band_rms(filepath, freq, freq + SLICE_WIDTH, trim_seconds)
            db = rms_to_db(rms) if rms is not None else DB_FLOOR
            slices.append({"freq": freq, "db": db})

        # Cliff detection
        cliff_freq = detect_cliff(slices)

        # HF deficit: avg of top 4 slices (18-20kHz) vs reference
        hf_slices = slices[-4:]  # 18000, 18500, 19000, 19500
        avg_hf_db = sum(s["db"] for s in hf_slices) / len(hf_slices)
        hf_deficit = ref_db - avg_hf_db

        return classify_track(hf_deficit, cliff_freq)

    except FileNotFoundError:
        return TrackResult(grade="error", error="sox not found")
    except subprocess.TimeoutExpired:
        return TrackResult(grade="error", error="sox timeout")
    except Exception as e:
        return TrackResult(grade="error", error=str(e))


def analyze_album(folder_path, trim_seconds=30):
    """Analyze all audio files in a folder (walks subdirectories).

    Returns an AlbumResult with album-level grade and per-track results.

    Walks subdirectories so multi-disc layouts (``Album/CD1/*.flac``) are
    analyzed as one album. The auto-import path always passes a flattened
    folder so recursion is a no-op there; force/manual-import and
    post-conversion callers can point at user folders with nested discs.
    """
    files = []
    for root, _dirs, names in os.walk(folder_path):
        for f in sorted(names):
            if os.path.splitext(f)[1].lower() in AUDIO_EXTENSIONS:
                files.append(os.path.join(root, f))

    track_results = []
    for filepath in files:
        result = analyze_track(filepath, trim_seconds)
        if result.grade != "error":
            track_results.append(result)

    grade, suspect_pct = classify_album(track_results)

    # Album-level estimated bitrate: min of all track estimates (worst case).
    # Even a single bad track means the album has a quality problem worth upgrading.
    estimates = [t.estimated_bitrate_kbps for t in track_results
                 if t.estimated_bitrate_kbps is not None]
    album_estimated = min(estimates) if estimates else None

    return AlbumResult(
        grade=grade,
        estimated_bitrate_kbps=album_estimated,
        suspect_pct=suspect_pct,
        tracks=track_results,
    )
