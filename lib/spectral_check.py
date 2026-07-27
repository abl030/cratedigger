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
from typing import TypedDict

from lib.quality import (
    AUDIO_EXTENSIONS_DOTTED as AUDIO_EXTENSIONS,
)
from lib.quality import (
    CODEC_FAMILY_AAC,
    CODEC_FAMILY_LOSSLESS,
    CODEC_FAMILY_MP3,
    CODEC_FAMILY_OPUS,
    CODEC_FAMILY_OTHER,
    CODEC_FAMILY_VORBIS,
    CodecFamily,
)

# --- Thresholds ---
HF_DEFICIT_SUSPECT = 60.0   # dB — above this = suspect (no cliff needed)
HF_DEFICIT_MARGINAL = 40.0  # dB — above this = marginal
CLIFF_THRESHOLD_DB_PER_KHZ = -12.0  # steeper than this = cliff
MIN_CLIFF_SLICES = 2        # consecutive steep slices to confirm cliff
ALBUM_SUSPECT_PCT = 60.0    # % of tracks that must be suspect for album flag

# 500Hz slices from 12kHz to 20kHz
# ISSUE #829 PHASE 5 PR1: this window, and detect_cliff() below, are the
# decision-path primitives every existing tier/bucket/decision derives from.
# They must NOT change here — widening the window shifts cliff detections by
# measured 3-6 percentage points and introduces a ~10% false-cliff rate on
# genuine lossless near 20kHz (see the Phase 5 plan). The 20-22kHz extension
# slices below are a SEPARATE, additive capture that never feeds detect_cliff.
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

# --- Extension-slice capture (issue #829 Phase 5 PR1) ---
#
# Four additional 500Hz-wide slices above SLICE_FREQS, captured with the same
# production primitives (_get_band_rms / rms_to_db / _ffmpeg_to_wav /
# _SOX_NATIVE_EXTS) but NEVER passed to detect_cliff. 20000Hz is captured
# for PR3's ceiling leg (not consumed by any decision yet); the ultrasonic
# deficit statistic itself only averages the three slices in
# ULTRASONIC_DEFICIT_SLICE_FREQS, matching the frozen reference scorer
# (calibration-tmp/measurements/score_v3.py, ``_window_legs``'s
# ``U = ref_db - mean(v[18:21])``, 2026-07-26).
EXTENSION_SLICE_FREQS = [20000, 20500, 21000, 21500]
ULTRASONIC_DEFICIT_SLICE_FREQS = [20500, 21000, 21500]

# Bumped whenever the measurement this module produces changes shape.
# Rows measured by this code carry ``spectral_measurement_version=2``;
# legacy rows (measured before this capture shipped) stay NULL and keep
# their old semantics — forward-only, no backfill (scope.md).
SPECTRAL_MEASUREMENT_VERSION = 2

# Extension-only, extension-based codec family classification (issue #829
# Phase 5 PR1 capture). This is deliberately simple: the six measured
# families the calibration work established (mp3/aac/opus/vorbis/lossless/
# other) collapse cleanly onto file extension for every container EXCEPT
# ``.ogg`` (Vorbis or Opus) and ``.m4a`` (AAC or ALAC) — both genuinely
# ambiguous by extension alone, so they are deliberately absent from this
# dict and always probed instead (see ``codec_family_from_extension``).
#
# Scoped to exactly ``AUDIO_EXTENSIONS_DOTTED`` — the only extensions
# ``analyze_album`` ever passes to ``codec_family_from_extension`` (see its
# file-enumeration filter below). ``.aif``/``.aiff``/``.au``/``.alac``/
# ``.ape`` are not in that set, so entries for them here would be
# unreachable dead code (round 3 review finding E).
_CODEC_FAMILY_BY_EXT: dict[str, CodecFamily] = {
    ".mp3": CODEC_FAMILY_MP3,
    ".aac": CODEC_FAMILY_AAC,
    ".opus": CODEC_FAMILY_OPUS,
    ".flac": CODEC_FAMILY_LOSSLESS,
    ".wav": CODEC_FAMILY_LOSSLESS,
}

# The two containers where extension cannot determine the codec family.
_AMBIGUOUS_CODEC_EXTS: frozenset[str] = frozenset({".ogg", ".m4a"})

# Lossy families native_codec_format_label can return, normalised to this
# module's lowercase vocabulary (it returns bare "MP3" for mp3).
_LOSSY_LABEL_TO_CODEC_FAMILY: dict[str, CodecFamily] = {
    "mp3": CODEC_FAMILY_MP3,
    "aac": CODEC_FAMILY_AAC,
    "opus": CODEC_FAMILY_OPUS,
    "vorbis": CODEC_FAMILY_VORBIS,
}


def codec_family_from_extension(filepath: str) -> CodecFamily:
    """Normalise a file's extension (probing the real codec where the
    extension is ambiguous) into one of six measured codec families:
    ``mp3`` / ``aac`` / ``opus`` / ``vorbis`` / ``lossless`` / ``other``.

    Extension alone resolves every container in ``_CODEC_FAMILY_BY_EXT``.
    ``.ogg`` (Vorbis or Opus) and ``.m4a`` (AAC or ALAC) are genuinely
    ambiguous, so both are probed via the SAME ffprobe invocation the repo
    already uses for this exact class of ambiguity
    (``lib.measurement.ffprobe_audio_codec_name`` — the probe
    ``lib.measurement.has_supported_lossless_audio`` calls to tell
    AAC-in-M4A from ALAC-in-M4A), folded through the repo's existing
    probed-codec -> lossy-family mapping
    (``lib.quality.compare.native_codec_format_label``). issue #829
    BLOCKING 2: guessing ``.ogg`` -> vorbis unconditionally reproduces
    exactly the codec-blind bug class #829 exists to fix — an Opus stream
    in an .ogg container would be scored on Vorbis's decision-grade ladder
    in PR2, which cannot apply to Opus (audit-only, unconditional per the
    plan). Both imports are deferred (function-local) to keep this leaf
    module's import-time footprint unchanged for every extension that
    doesn't need them.
    """
    ext = os.path.splitext(filepath)[1].lower()
    if ext not in _AMBIGUOUS_CODEC_EXTS:
        return _CODEC_FAMILY_BY_EXT.get(ext, CODEC_FAMILY_OTHER)

    from lib.measurement import ffprobe_audio_codec_name
    from lib.quality.compare import native_codec_format_label

    probed = ffprobe_audio_codec_name(_safe_path(filepath))
    if probed in ("alac", "flac"):
        return CODEC_FAMILY_LOSSLESS
    # No extension fallback: when ffprobe cannot identify the stream,
    # ``probed`` is None and this must degrade honestly to "other" rather
    # than guessing from the (ambiguous, by construction) container
    # extension — ``native_codec_format_label(None, "m4a")`` falls through
    # to its unconditional ext->label table and returns "aac" for EVERY
    # unprobeable .m4a, including an ALAC file ffprobe merely failed to
    # read. That is exactly the codec-blind guess class issue #829 exists
    # to fix (round 3 review finding B).
    lossy_label = native_codec_format_label(probed)
    if lossy_label is None:
        return CODEC_FAMILY_OTHER
    return _LOSSY_LABEL_TO_CODEC_FAMILY.get(
        lossy_label.lower(), CODEC_FAMILY_OTHER,
    )


def compute_ultrasonic_deficit_db(
    ref_db: float,
    extension_slices: "list[_Slice]",
) -> float | None:
    """Level-invariant ultrasonic deficit for one track.

    ``U = ref_db(1-4kHz) - mean(20.5-22kHz slices)`` — the reference band
    normalises against the track's own midband level, which is what makes
    the statistic comparable across masters (a quiet record's genuine
    ultrasonic content is otherwise indistinguishable from a loud record's
    launder leakage). Reference implementation:
    calibration-tmp/measurements/score_v3.py's ``_window_legs`` (frozen
    2026-07-26, issue #829) — this mirrors its ``U`` line exactly, averaging
    only the three ``ULTRASONIC_DEFICIT_SLICE_FREQS`` slices (20000Hz is
    captured but deliberately excluded here, matching score_v3).

    Returns ``None`` when any of the three required slices is missing —
    this statistic is not consumed by any decision in PR1, so a partial
    measurement fails soft rather than fabricating a value.
    """
    by_freq = {s["freq"]: s["db"] for s in extension_slices}
    values = [
        by_freq[freq] for freq in ULTRASONIC_DEFICIT_SLICE_FREQS
        if freq in by_freq
    ]
    if len(values) != len(ULTRASONIC_DEFICIT_SLICE_FREQS):
        return None
    return ref_db - (sum(values) / len(values))

# --- Data classes ---

class _Slice(TypedDict):
    """One 500Hz-wide band measurement: center frequency + measured dB."""
    freq: int
    db: float


@dataclass
class TrackResult:
    grade: str                                  # "genuine" | "marginal" | "suspect" | "error"
    hf_deficit_db: float = 0.0
    cliff_detected: bool = False
    cliff_freq_hz: int | None = None
    estimated_bitrate_kbps: int | None = None
    error: str | None = None
    # issue #829 Phase 5 PR1 capture — never fed into grade/cliff_detected.
    codec_family: CodecFamily = CODEC_FAMILY_OTHER
    ultrasonic_deficit_db: float | None = None


@dataclass
class AlbumResult:
    grade: str                                  # "genuine" | "suspect" | "likely_transcode"
    estimated_bitrate_kbps: int | None = None
    suspect_pct: float = 0.0
    tracks: list[TrackResult] = field(default_factory=list[TrackResult])
    # issue #829 Phase 5 PR1 capture — never consumed by ``grade``/
    # ``estimated_bitrate_kbps`` above; purely additive facts for the
    # evidence row. ``cliff_hz`` is the raw worst-case cliff frequency
    # (the same value ``estimated_bitrate_kbps`` is derived from — see
    # ``analyze_album``); ``ultrasonic_deficit_db`` is the per-track mean;
    # ``codec_family`` is the first track's family (albums are homogeneous
    # in practice); ``spectral_measurement_version`` is always stamped
    # ``SPECTRAL_MEASUREMENT_VERSION`` whenever this function runs.
    cliff_hz: int | None = None
    codec_family: CodecFamily | None = None
    ultrasonic_deficit_db: float | None = None
    spectral_measurement_version: int | None = None


# --- Core functions ---

def parse_rms_from_stat(stderr_output: str) -> float | None:
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


def rms_to_db(rms: float) -> float:
    """Convert RMS amplitude to dB. Returns DB_FLOOR for zero/negative."""
    if rms <= 0:
        return DB_FLOOR
    return 20.0 * math.log10(rms)


def detect_cliff(
    slices: list[_Slice],
    threshold_db_per_khz: float = CLIFF_THRESHOLD_DB_PER_KHZ,
    min_slices: int = MIN_CLIFF_SLICES,
    slice_width_hz: int = SLICE_WIDTH,
) -> int | None:
    """Detect spectral cliff from a list of {"freq": Hz, "db": dB} slices.

    Returns the frequency (Hz) where the cliff starts, or None.
    """
    if len(slices) < 2:
        return None

    khz_step = slice_width_hz / 1000.0
    cliff_count = 0
    cliff_start: int | None = None

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


def estimate_bitrate_from_cliff(cliff_freq_hz: int | None) -> int | None:
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


def classify_track(
    hf_deficit_db: float,
    cliff_freq_hz: int | None,
    *,
    codec_family: CodecFamily = CODEC_FAMILY_OTHER,
    ultrasonic_deficit_db: float | None = None,
) -> TrackResult:
    """Classify a single track based on HF deficit and cliff detection.

    ``codec_family``/``ultrasonic_deficit_db`` are pure passengers (issue
    #829 Phase 5 PR1 capture) — never read by the grade/cliff_detected
    decision above.

    Returns a TrackResult.
    """
    cliff_detected = cliff_freq_hz is not None
    estimated_br = estimate_bitrate_from_cliff(cliff_freq_hz)

    if cliff_detected or hf_deficit_db >= HF_DEFICIT_SUSPECT:
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
        codec_family=codec_family,
        ultrasonic_deficit_db=ultrasonic_deficit_db,
    )


def classify_album(
    track_results: list[TrackResult],
) -> tuple[str, float]:
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


def aggregate_album_spectral_capture(
    track_results: "list[TrackResult]",
) -> "tuple[int | None, CodecFamily | None, float | None]":
    """Aggregate per-track issue #829 Phase 5 PR1 capture facts to
    album-level ``(cliff_hz, codec_family, ultrasonic_deficit_db)``.

    Pure and additive — never read by ``classify_album`` above or fed back
    into any decision.

    * ``cliff_hz``: same "worst case" convention as
      ``estimated_bitrate_kbps`` — the lowest cliff frequency among tracks
      where one was detected (a lower cliff Hz is always the more
      aggressive lowpass, so it's the same track that would drive a
      worst-case bitrate estimate).
    * ``ultrasonic_deficit_db``: arithmetic mean across tracks with a valid
      per-track deficit (score_v3's album statistic).
    * ``codec_family``: the first track's family — albums are homogeneous
      in practice; matches the existing ``files[0]``-based convention used
      elsewhere for evidence-level codec/container.
    """
    cliff_candidates = [
        t.cliff_freq_hz for t in track_results if t.cliff_freq_hz is not None
    ]
    album_cliff_hz = min(cliff_candidates) if cliff_candidates else None

    deficits = [
        t.ultrasonic_deficit_db for t in track_results
        if t.ultrasonic_deficit_db is not None
    ]
    album_ultrasonic_deficit_db = (
        sum(deficits) / len(deficits) if deficits else None
    )

    album_codec_family = (
        track_results[0].codec_family if track_results else None
    )

    return album_cliff_hz, album_codec_family, album_ultrasonic_deficit_db


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


def _get_band_rms(
    filepath: str,
    lo_hz: int,
    hi_hz: int,
    trim_seconds: int = 30,
) -> float:
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
    cmd.extend(["sinc", f"{lo_hz:d}-{hi_hz:d}", "stat"])
    # errors="replace": sox echoes filename + tag bytes; non-UTF-8 metadata
    # would otherwise raise UnicodeDecodeError during capture.
    result = subprocess.run(cmd, capture_output=True, text=True,
                            errors="replace", timeout=60, check=False)
    rms = parse_rms_from_stat(result.stderr)
    if rms is None:
        last_line = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else f"sox exit {result.returncode}"
        raise _DecodeFailedError(last_line)
    return rms


def _ffmpeg_to_wav(src: str, dst: str, trim_seconds: int = 30) -> None:
    """Decode src to WAV at dst (trimmed to trim_seconds).

    One ffmpeg call per file replaces 21 ffmpeg calls (one per sox band —
    1 reference + 16 in-window + 4 extension slices, issue #829 Phase 5
    PR1) when AAC/ALAC/WMA inputs reach analyze_track. Probe bounds
    (``-analyzeduration`` / ``-probesize``) cap atom-table parsing so a
    hostile MP4 with deeply-nested moov boxes can't spin until timeout;
    the 30s wall clock backstops anything that slips past.

    Output is forced to 48kHz/2ch — NOT purely a size optimisation.
    ``EXTENSION_SLICE_FREQS`` measures up to 22000Hz, so 48kHz (Nyquist
    24kHz) is the minimum sample rate that keeps every extension band
    genuinely measurable for every ffmpeg-routed container; anything
    lower reproduces issue #829 BLOCKING 3's fabricated-deficit bug for
    every AAC/ALAC/WMA file, not just low-sample-rate sox-native ones.
    It also keeps parity with the calibration instrument that measured
    the ``ultrasonic_deficit_db`` thresholds PR3 will gate on
    (calibration-tmp/measurements/score_v3.py) — lowering this sample
    rate would shift ``ultrasonic_deficit_db`` by more than PR3's entire
    ``U>=62`` threshold margin with no test in this module failing, since
    nothing here pins the sample-rate choice itself. Raises
    ``_DecodeFailedError`` on any failure."""
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y",
        "-analyzeduration", "5M", "-probesize", "5M",
        "-i", _safe_path(src), "-map", "0:a",
    ]
    if trim_seconds:
        cmd.extend(["-t", str(trim_seconds)])
    cmd.extend(["-ar", "48000", "-ac", "2", "-f", "wav", "-bitexact", dst])
    result = subprocess.run(cmd, capture_output=True, text=True,
                            errors="replace", timeout=30, check=False)
    if result.returncode != 0:
        last_line = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else f"ffmpeg exit {result.returncode}"
        raise _DecodeFailedError(f"ffmpeg: {last_line}")


def analyze_track(filepath: str, trim_seconds: int = 30) -> TrackResult:
    """Analyze a single audio file for spectral quality.

    Runs 21 sox commands (1 reference band + 16 in-window test slices + 4
    extension slices — issue #829 Phase 5 PR1). Non-sox formats
    (.m4a/.aac/.alac/.wma) are decoded once to a temp WAV inside a per-track
    ``TemporaryDirectory`` (auto-cleaned, not racable by other uids since we
    own the directory). Returns a TrackResult.

    ``codec_family`` is derived from the ORIGINAL ``filepath`` extension
    (not the decode-ready path, which for non-native containers is a
    temporary WAV that has already lost the source extension) and attached
    to whatever ``TrackResult`` the decode/analysis path produces, error
    branches included — it's a passenger fact, not a decision input.
    """
    codec_family = codec_family_from_extension(filepath)
    try:
        ext = os.path.splitext(filepath)[1].lower()
        if ext not in _SOX_NATIVE_EXTS:
            with tempfile.TemporaryDirectory(prefix="spectral_") as tmpdir:
                tmp_wav = os.path.join(tmpdir, "audio.wav")
                _ffmpeg_to_wav(filepath, tmp_wav, trim_seconds=trim_seconds)
                # Already trimmed by ffmpeg; skip sox's redundant trim.
                result = _analyze_decoded(tmp_wav, sox_trim=0)
        else:
            result = _analyze_decoded(filepath, sox_trim=trim_seconds)
        result.codec_family = codec_family
        return result

    except _DecodeFailedError as e:
        return TrackResult(
            grade="error", error=f"decode failed: {e}",
            codec_family=codec_family,
        )
    except FileNotFoundError as e:
        return TrackResult(
            grade="error", error=f"binary not found: {e}",
            codec_family=codec_family,
        )
    except subprocess.TimeoutExpired:
        return TrackResult(
            grade="error", error="sox/ffmpeg timeout",
            codec_family=codec_family,
        )
    except Exception as e:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return TrackResult(
            grade="error", error=str(e), codec_family=codec_family,
        )


def _analyze_decoded(sox_input: str, sox_trim: int) -> TrackResult:
    """Run the 21 sox calls against a decode-ready file (1 reference + 16
    in-window + 4 extension slices, issue #829 Phase 5 PR1). Extracted
    from analyze_track so the sox-native and ffmpeg-fallback paths share
    one body. Reference-band None RMS now grades 'error' (was 'genuine' as
    the silent-track early-out — see the rc=0 leg in _get_band_rms)."""
    # Reference band: 1-4kHz. None RMS at the reference is a decode-side
    # failure (band would have musical content); reserve the silent-track
    # early-out for genuinely near-zero RMS only.
    ref_rms = _get_band_rms(sox_input, 1000, 4000, sox_trim)
    if ref_rms < 0.000001:
        return TrackResult(grade="genuine", hf_deficit_db=0.0)

    ref_db = rms_to_db(ref_rms)

    slices: list[_Slice] = []
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

    # detect_cliff() gets ONLY the 16 in-window slices above — never the
    # extension slices below. Widening that input would shift cliff
    # detections (see the SLICE_FREQS comment); the extension capture is
    # deliberately a separate, additive pass.
    cliff_freq = detect_cliff(slices)
    hf_slices = slices[-4:]
    avg_hf_db = sum(s["db"] for s in hf_slices) / len(hf_slices)
    hf_deficit = ref_db - avg_hf_db

    # Extension slices (issue #829 Phase 5 PR1) — deliberately NOT the same
    # floor-on-decode-failure treatment as the in-band slices above. Once
    # the reference band has already decoded successfully, a decode
    # failure THIS far above 20kHz is sox refusing an out-of-Nyquist band
    # ("filter frequency must be less than sample-rate/2" — any sox-native
    # file below ~44.1kHz hits this on some or all of these slices); a
    # genuinely near-silent band measures a valid near-zero RMS and never
    # raises. Flooring an unmeasurable band to DB_FLOOR fabricated a
    # deficit near 115dB against a real 44.1kHz control's ~23dB on a real
    # 32kHz WAV (issue #829 BLOCKING 3) — indistinguishable from a launder
    # under PR3's U>=62 gate. Excluding the slice entirely instead makes
    # compute_ultrasonic_deficit_db's missing-slice -> None branch
    # reachable, so an unmeasurable band reports "not measured", not a
    # fabricated deficit.
    ext_slices: list[_Slice] = []
    for freq in EXTENSION_SLICE_FREQS:
        try:
            rms = _get_band_rms(sox_input, freq, freq + SLICE_WIDTH, sox_trim)
        except _DecodeFailedError:
            continue
        ext_slices.append({"freq": freq, "db": rms_to_db(rms)})
    ultrasonic_deficit_db = compute_ultrasonic_deficit_db(ref_db, ext_slices)

    return classify_track(
        hf_deficit, cliff_freq,
        ultrasonic_deficit_db=ultrasonic_deficit_db,
    )


def analyze_album(folder_path: str, trim_seconds: int = 30) -> AlbumResult:
    """Analyze all audio files in a folder (walks subdirectories).

    Returns an AlbumResult with album-level grade and per-track results.

    Walks subdirectories so multi-disc layouts (``Album/CD1/*.flac``) are
    analyzed as one album. The auto-import path always passes a flattened
    folder so recursion is a no-op there; force-import and
    post-conversion callers can point at user folders with nested discs.
    """
    files: list[str] = []
    for root, _dirs, names in os.walk(folder_path):
        for f in sorted(names):
            if os.path.splitext(f)[1].lower() in AUDIO_EXTENSIONS:
                files.append(os.path.join(root, f))

    track_results: list[TrackResult] = []
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
        # issue #829 Phase 5 PR1 review round 2, should-fix 11:
        # spectral_measurement_version signals "cliff_hz/ultrasonic_deficit_db
        # were actually measured by this code" — an all-errored album
        # measured neither, so it stays NULL here (codec_family is still
        # legitimately derivable from the file extension alone, independent
        # of whether the spectral measurement itself succeeded).
        return AlbumResult(
            grade="error", suspect_pct=0.0, tracks=[], estimated_bitrate_kbps=None,
            # `files` is already proven truthy by the enclosing `if` above
            # (issue #829 Phase 5 PR1 review round 2, should-fix 14 — the
            # old `if files else None` guard here was dead).
            codec_family=codec_family_from_extension(files[0]),
        )

    grade, suspect_pct = classify_album(track_results)

    # Album-level estimated bitrate: min of all track estimates (worst case).
    # Even a single bad track means the album has a quality problem worth upgrading.
    estimates = [t.estimated_bitrate_kbps for t in track_results
                 if t.estimated_bitrate_kbps is not None]
    album_estimated = min(estimates) if estimates else None

    album_cliff_hz, album_codec_family, album_ultrasonic_deficit_db = (
        aggregate_album_spectral_capture(track_results)
    )

    return AlbumResult(
        grade=grade,
        estimated_bitrate_kbps=album_estimated,
        suspect_pct=suspect_pct,
        tracks=track_results,
        cliff_hz=album_cliff_hz,
        codec_family=album_codec_family,
        ultrasonic_deficit_db=album_ultrasonic_deficit_db,
        spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
    )
