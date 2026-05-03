"""Spectral quality verification for audio files.

Detects transcoded/upsampled audio using sox bandpass filtering and
spectral gradient analysis. Works on FLAC, MP3, OGG, Opus, and WAV
natively; AAC/M4A/ALAC/WMA are decoded through ffmpeg first because
sox in our nix shell has no handler for those containers.

Requires: sox in PATH (always); ffmpeg in PATH (for AAC/ALAC/WMA).
"""

import math
import os
import subprocess
import tempfile
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
    """Parse RMS amplitude from sox stat stderr output. Returns float or None.

    Rejects NaN and inf — those are sentinels for sox internal failures
    (filter-rejected band, decoder produced no samples, etc.) and would
    otherwise short-circuit every threshold comparison to False, silently
    grading the track 'genuine'. Same failure shape as the codec-blindness
    bug; same fix shape (fail closed instead of silent-pass)."""
    for line in stderr_output.split("\n"):
        if "RMS     amplitude:" in line:
            try:
                v = float(line.split()[-1])
            except (ValueError, IndexError):
                return None
            if math.isnan(v) or math.isinf(v):
                return None
            return v
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

# Extensions sox can decode natively in our nix shell (see `sox --help`).
# Anything outside this set must be transcoded via ffmpeg first or sox will
# emit "FAIL formats: no handler for file extension X" and produce no RMS.
_SOX_NATIVE_EXTS: frozenset[str] = frozenset({
    ".mp3", ".flac", ".ogg", ".opus", ".wav", ".aif", ".aiff", ".au",
})


class _DecodeFailedError(Exception):
    """sox/ffmpeg failed to decode the file — distinct from genuine silence.

    Raised by ``_get_band_rms`` when sox exits non-zero with no RMS line in
    its stderr (e.g. "FAIL formats: no handler for file extension `m4a'"),
    or by ``_ffmpeg_to_wav`` when ffmpeg can't open the source. The caller
    must surface this as ``grade='error'`` rather than letting the missing
    measurement fall through the silent-track early-out as ``'genuine'``.
    """


def _safe_path(filepath: str) -> str:
    """Prefix relative paths with './' so sox/ffmpeg never see a leading
    dash as a flag. Soulseek peers control filenames; ``-evil.flac``
    arriving via slskd would otherwise be parsed as an argv flag by both
    binaries (list-form ``subprocess.run`` blocks shell injection but not
    argv-flag confusion). Absolute paths are passed through unchanged."""
    if filepath.startswith(("/", "./")):
        return filepath
    return "./" + filepath


def _get_band_rms(filepath, lo_hz, hi_hz, trim_seconds=30):
    """Get RMS amplitude of audio filtered to a frequency band via sox.

    Returns the measured RMS (float, possibly ~0 for silent input). Raises
    ``_DecodeFailedError`` when sox returned no RMS line OR exited non-zero
    — both are decode-side failures, distinct from a genuinely silent
    track (which still returns a valid near-zero RMS). Conflating the two
    silently grades undecodable input as 'genuine' (the codec-blindness
    bug class this fix closes for the rc=0 leg too)."""
    cmd = ["sox", _safe_path(filepath), "-n"]
    if trim_seconds:
        cmd.extend(["trim", "0", str(trim_seconds)])
    cmd.extend(["sinc", "%d-%d" % (lo_hz, hi_hz), "stat"])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    rms = parse_rms_from_stat(result.stderr)
    if rms is None:
        last_line = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else f"sox exit {result.returncode}"
        raise _DecodeFailedError(last_line)
    return rms


def _ffmpeg_to_wav(src, dst, trim_seconds=30):
    """Decode src to WAV at dst (trimmed to trim_seconds).

    One ffmpeg call per file replaces 17 ffmpeg calls (one per sox band)
    when AAC/ALAC/WMA inputs reach analyze_track. Probe bounds
    (``-analyzeduration`` / ``-probesize``) cap atom-table parsing so a
    hostile MP4 with deeply-nested moov boxes can't spin until timeout;
    the 30s wall clock backstops anything that slips past. Output is
    forced to 48kHz/2ch — spectral analysis tops at 20kHz so anything
    higher is wasted I/O. Raises ``_DecodeFailedError`` on any failure."""
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y",
        "-analyzeduration", "5M", "-probesize", "5M",
        "-i", _safe_path(src),
    ]
    if trim_seconds:
        cmd.extend(["-t", str(trim_seconds)])
    cmd.extend(["-ar", "48000", "-ac", "2", "-f", "wav", "-bitexact", dst])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        last_line = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else f"ffmpeg exit {result.returncode}"
        raise _DecodeFailedError(f"ffmpeg: {last_line}")


def analyze_track(filepath, trim_seconds=30):
    """Analyze a single audio file for spectral quality.

    Runs 17 sox commands (1 reference band + 16 test slices). Non-sox
    formats (.m4a/.aac/.alac/.wma) are decoded once to a temp WAV inside
    a per-track ``TemporaryDirectory`` (auto-cleaned, not racable by other
    uids since we own the directory). Returns a TrackResult.
    """
    try:
        ext = os.path.splitext(filepath)[1].lower()
        if ext not in _SOX_NATIVE_EXTS:
            with tempfile.TemporaryDirectory(prefix="spectral_") as tmpdir:
                tmp_wav = os.path.join(tmpdir, "audio.wav")
                _ffmpeg_to_wav(filepath, tmp_wav, trim_seconds=trim_seconds)
                # Already trimmed by ffmpeg; skip sox's redundant trim.
                return _analyze_decoded(tmp_wav, sox_trim=0)
        return _analyze_decoded(filepath, sox_trim=trim_seconds)

    except _DecodeFailedError as e:
        return TrackResult(grade="error", error=f"decode failed: {e}")
    except FileNotFoundError as e:
        return TrackResult(grade="error", error=f"binary not found: {e}")
    except subprocess.TimeoutExpired:
        return TrackResult(grade="error", error="sox/ffmpeg timeout")
    except Exception as e:
        return TrackResult(grade="error", error=str(e))


def _analyze_decoded(sox_input, sox_trim):
    """Run the 17 sox calls against a decode-ready file. Extracted from
    analyze_track so the sox-native and ffmpeg-fallback paths share one
    body. Reference-band None RMS now grades 'error' (was 'genuine' as the
    silent-track early-out — see the rc=0 leg in _get_band_rms)."""
    # Reference band: 1-4kHz. None RMS at the reference is a decode-side
    # failure (band would have musical content); reserve the silent-track
    # early-out for genuinely near-zero RMS only.
    try:
        ref_rms = _get_band_rms(sox_input, 1000, 4000, sox_trim)
    except _DecodeFailedError:
        raise
    if ref_rms < 0.000001:
        return TrackResult(grade="genuine", hf_deficit_db=0.0)

    ref_db = rms_to_db(ref_rms)

    slices = []
    for freq in SLICE_FREQS:
        # In-band slices CAN legitimately measure as silent (genuine
        # rolloff), so a missing measurement here is just floored, not
        # a decode failure. Different semantics from the reference band.
        try:
            rms = _get_band_rms(sox_input, freq, freq + SLICE_WIDTH, sox_trim)
            db = rms_to_db(rms)
        except _DecodeFailedError:
            db = DB_FLOOR
        slices.append({"freq": freq, "db": db})

    cliff_freq = detect_cliff(slices)
    hf_slices = slices[-4:]
    avg_hf_db = sum(s["db"] for s in hf_slices) / len(hf_slices)
    hf_deficit = ref_db - avg_hf_db
    return classify_track(hf_deficit, cliff_freq)


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
    error_count = 0
    for filepath in files:
        result = analyze_track(filepath, trim_seconds)
        if result.grade == "error":
            error_count += 1
        else:
            track_results.append(result)

    # Fail closed when every audio file errored — empty track_results from
    # a non-empty input list is the same silent-genuine bug class the
    # codec fix targets, just at the album level. classify_album's
    # empty-list branch returns 'genuine' (which is correct for "no audio
    # files at all" — e.g. a docs-only folder), so we have to distinguish
    # the two cases here, before delegating.
    if files and not track_results:
        return AlbumResult(
            grade="error", suspect_pct=0.0, tracks=[], estimated_bitrate_kbps=None,
        )

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
