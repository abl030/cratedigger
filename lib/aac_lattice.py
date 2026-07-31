"""AAC MDCT frame-lattice detector — the productized Derrien port.

Olivier Derrien's detector (JAES 2019, 67(3) 116-123) recovers the **MDCT
frame lattice** an AAC encoder left behind. AAC quantises MDCT coefficients on
a fixed 1024-sample frame grid; after decoding to PCM the grid is invisible in
the waveform, but re-analysing the PCM at the *right* offset recovers
scalefactor-band structure a signal that was never AAC-encoded does not have.
The statistic is a probability over a sweep of all 1024 candidate offsets: a
genuine file's sweep is flat, an AAC-derived file's sweep spikes at the true
offset.

This module is a typed production port of
``docs/research/calibration-data/derrien/aacdet.py.frozen`` — itself a numpy
port of the author's reference Matlab, validated against it (worst absolute
difference 3.3e-16 against Octave running the unmodified ``detect_aac.m``).
The frozen file stays frozen; this module reproduces its mathematics and is
held to the frozen port's own self-validation by
``tests/test_aac_lattice.py`` (issue #829 invariant A-I2).

Two statistics the frozen port carries are deliberately NOT ported, both
measured dead in ``docs/research/calibration-data/derrien-refinement/`` and
retired by its conclusion 4 ("Any future production port should compute
``mode=high`` only and drop NAC"):

* the Yang-style **NAC probe** — track AUC 0.760 against ``proba``'s 0.995,
  and no combination beats ``proba`` alone;
* the **``mode=low``** frame selection — track AUC 0.644, with ``proba``
  saturating at exactly 1.0000 on *both* classes.

Two further deliberate differences from the frozen port:

* **scipy is not a dependency.** The frozen port used ``scipy.special`` for
  the per-band significance thresholds; this module computes ``erfinv`` by
  bracketed bisection over the standard library's ``math.erf``, which needs
  no new closure entry. ``tests/test_aac_lattice.py`` pins the round trip.
* **soundfile is not a dependency.** The frozen port read audio through
  libsndfile; this module decodes through the ffmpeg already required by
  ``lib/spectral_check.py`` and reads the resulting float32 WAV directly.
  The [-1, 1) normalisation libsndfile applied is reproduced exactly, which
  matters: the power-law quantisation lattice is NOT scale-invariant.

Cost: roughly 49 s of CPU per track at the shipped ``nb_win=8`` /
``nb_sf=8``, single-threaded (measured, ``derrien/README.md`` § Cost). Callers
own the cohort gate and the per-album track cap.

Requires: ffmpeg in PATH.
"""

from __future__ import annotations

import logging
import math
import os
import struct
import subprocess
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache

import numpy as np
import numpy.typing as npt

from lib.quality import (
    AUDIO_EXTENSIONS_DOTTED,
    AacLatticeCapture,
    AacLatticeTrackScore,
)
from lib.spectral_check import _safe_path

logger = logging.getLogger("cratedigger")

F64 = npt.NDArray[np.float64]
I64 = npt.NDArray[np.int64]

# --- Codec geometry (init_aac.m / detect_aac.m) ----------------------------

IBLEN_LONG = 1024
IBLEN_SHORT = 128
NB_SHORT_WIN = IBLEN_LONG // IBLEN_SHORT          # 8
OFFSET_SHORT = (IBLEN_LONG - IBLEN_SHORT) // 2    # 448

# init_aac.m scalefactor-band start indices (1-based in Matlab).
_SWB_LONG_44_48: tuple[int, ...] = (
    1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 49, 57, 65, 73, 81, 89, 97, 109,
    121, 133, 145, 161, 177, 197, 217, 241, 265, 293, 321, 353, 385, 417, 449,
    481, 513, 545, 577, 609, 641, 673, 705, 737, 769, 801, 833, 865, 897, 929,
)
_SWB_LONG_32: tuple[int, ...] = (*_SWB_LONG_44_48, 961, 993)
_SWB_SHORT: tuple[int, ...] = (
    1, 5, 9, 13, 17, 21, 29, 37, 45, 57, 69, 81, 97, 113,
)

# The band tables above exist for exactly these sample rates. 96 kHz has no
# table, so the detector cannot score hi-res input at all — the frozen port
# raises there, and 33 of each arm's 215 paired-corpus tracks errored out for
# this reason (derrien/README.md § Data).
SUPPORTED_RATES_KHZ: frozenset[int] = frozenset({32, 44, 48})

# main.m's defaults, kept exactly: the measured cost, the published
# operating point, and every committed research number assume them.
DEFAULT_NB_WIN = 8
DEFAULT_NB_SF = 8
_SF_MIN = 0.3
_SF_MAX = 0.7
_REF_PROBA = 1e-2

# Per-album cap. The offset-concentration rule needs 4 tracks recovering one
# offset (derrien-refinement/README.md § "The offset-concentration rule"); 6
# leaves headroom for per-track errors without paying for a whole album at
# ~49 s/track.
MAX_SCORED_TRACKS = 6

_DECODE_TIMEOUT_SECONDS = 300


class AacLatticeError(Exception):
    """Base: this track could not be scored. Recorded, never raised at callers."""


class AacLatticeUnsupportedRateError(AacLatticeError):
    """The sample rate has no AAC scalefactor-band table (e.g. 96 kHz)."""


class AacLatticeDecodeError(AacLatticeError):
    """ffmpeg could not produce analysable PCM for this track."""


class AacLatticeTooShortError(AacLatticeError):
    """Fewer than three 1024-sample blocks of audio — no frame to select."""


# --- Significance thresholds (main.m), without scipy -----------------------

def erfinv(y: float) -> float:
    """Inverse error function by bracketed bisection over ``math.erf``.

    ``erf`` is strictly increasing, so bisection converges to the adjacent
    double and needs no rational approximation, no polishing step, and above
    all no scipy in the production closure. It runs at most a few dozen times
    per sample rate (the result is cached), so its cost is irrelevant.
    """
    if not -1.0 < y < 1.0:
        raise ValueError(f"erfinv domain is (-1, 1): {y}")
    if y == 0.0:
        return 0.0
    sign = 1.0 if y > 0 else -1.0
    target = abs(y)
    lo = 0.0
    hi = 1.0
    while math.erf(hi) < target:
        hi *= 2.0
        if hi > 64.0:  # erf(6) is already 1.0 in double precision
            return sign * hi
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if mid <= lo or mid >= hi:
            break
        if math.erf(mid) < target:
            lo = mid
        else:
            hi = mid
    return sign * 0.5 * (lo + hi)


def _tau_for_width(width: float, ref_proba: float = _REF_PROBA) -> float:
    """main.m's per-band significance threshold under the uniform-error null."""
    mu = 1.0 / 12.0
    sig = math.sqrt(1.0 / width / 180.0)
    inner = math.erf(mu / sig / math.sqrt(2.0)) - 2.0 * ref_proba
    return mu - math.sqrt(2.0) * sig * erfinv(inner)


def tau_tables(
    low_l: I64, high_l: I64, low_s: I64, high_s: I64,
    ref_proba: float = _REF_PROBA,
) -> tuple[F64, F64]:
    """Long- and short-window per-band thresholds (main.m)."""
    widths_l = (high_l - low_l + 1).astype(np.float64)
    widths_s = (high_s - low_s + 1).astype(np.float64) * NB_SHORT_WIN
    tau_l = np.array(
        [_tau_for_width(float(w), ref_proba) for w in widths_l],
        dtype=np.float64,
    )
    tau_s = np.array(
        [_tau_for_width(float(w), ref_proba) for w in widths_s],
        dtype=np.float64,
    )
    return tau_l, tau_s


# --- Band tables and windows (init_aac.m) ----------------------------------

def _bands(low_1based: tuple[int, ...], top: int) -> tuple[I64, I64]:
    low: I64 = np.asarray(low_1based, dtype=np.int64) - 1
    high: I64 = np.empty_like(low)
    high[:-1] = low[1:] - 1
    high[-1] = top - 1
    return low, high


@dataclass(frozen=True)
class _Windows:
    """The four sine window shapes AAC switches between."""

    long: F64
    start: F64
    stop: F64
    short: F64


def init_aac(fs_khz: int) -> tuple[I64, I64, I64, I64, _Windows]:
    """Band tables and the four sine window shapes for a sample rate (kHz)."""
    if fs_khz == 32:
        lo_l = _SWB_LONG_32
    elif fs_khz in (44, 48):
        lo_l = _SWB_LONG_44_48
    else:
        raise AacLatticeUnsupportedRateError(
            f"unsupported sample rate {fs_khz} kHz"
        )
    low_l, high_l = _bands(lo_l, IBLEN_LONG)
    low_s, high_s = _bands(_SWB_SHORT, IBLEN_SHORT)

    n = 2 * IBLEN_LONG
    wl_2048 = np.sin(
        np.pi * (2 * np.arange(0, n // 2, dtype=np.int64) + 1) / n / 2
    )
    wr_2048 = np.sin(
        np.pi * (2 * np.arange(n // 2, n, dtype=np.int64) + 1) / n / 2
    )
    n = 2 * IBLEN_SHORT
    wl_256 = np.sin(
        np.pi * (2 * np.arange(0, n // 2, dtype=np.int64) + 1) / n / 2
    )
    wr_256 = np.sin(
        np.pi * (2 * np.arange(n // 2, n, dtype=np.int64) + 1) / n / 2
    )

    windows = _Windows(
        long=np.concatenate([wl_2048, wr_2048]),
        start=np.concatenate(
            [wl_2048, np.ones(OFFSET_SHORT), wr_256, np.zeros(OFFSET_SHORT)]
        ),
        stop=np.concatenate(
            [np.zeros(OFFSET_SHORT), wl_256, np.ones(OFFSET_SHORT), wr_2048]
        ),
        short=np.concatenate([wl_256, wr_256]),
    )
    return low_l, high_l, low_s, high_s, windows


# --- The transform and the detector (MDCT.m / detect_aac.m) ----------------

def mdct(x: F64) -> F64:
    """MDCT.m, batched over leading axes. Last axis 2N samples -> N bins."""
    n2 = x.shape[-1]
    n = n2 // 2
    t = np.arange(n2, dtype=np.int64)
    f = np.arange(n, dtype=np.int64)
    y = x * np.exp(-1j * np.pi * t / (2 * n))
    y = np.fft.fft(y, axis=-1)[..., :n]
    return np.real(y * np.exp(-1j * np.pi * (f + 0.5) * (n + 1) / (2 * n)))


def _frame_matrix(seg: F64, offsets: I64) -> F64:
    """(n_off, n_frames, 2048) block from per-frame segments.

    ``seg`` is (n_frames, >= 2048 + max_offset); segment f starts at sample
    ``(frame_f - 1) * IBLEN_LONG`` of the channel, matching detect_aac.m.
    """
    idx = (
        offsets[:, None, None]
        + np.arange(2 * IBLEN_LONG, dtype=np.int64)[None, None, :]
    )
    return seg[None, :, :][
        np.zeros(len(offsets), dtype=np.int64)[:, None, None],
        np.arange(seg.shape[0], dtype=np.int64)[None, :, None],
        idx,
    ]


class BandPlan:
    """Precomputed bin->band mapping for one window family.

    ``group`` is the number of MDCT blocks pooled into one band (1 for long
    windows, 8 for short windows, matching detect_aac.m's reshape).
    """

    def __init__(self, low: I64, high: I64, nbins: int, group: int) -> None:
        nb = len(low) - 1                      # detect_aac.m skips the last band
        self.nb = nb
        lab: I64 = np.full(group * nbins, -1, dtype=np.int64)
        for b in range(nb):
            for g in range(group):
                lab[g * nbins + low[b]: g * nbins + high[b] + 1] = b
        keep = np.nonzero(lab >= 0)[0]
        order = np.argsort(lab[keep], kind="stable")
        self.perm: I64 = keep[order]
        sortlab: I64 = lab[self.perm]
        self.starts: I64 = np.searchsorted(sortlab, np.arange(nb, dtype=np.int64))
        self.width: F64 = np.bincount(sortlab, minlength=nb).astype(np.float64)
        self.bin_band: I64 = sortlab
        self.group = group
        self.nbins = nbins


def band_counts(
    coef: F64, plan: BandPlan, sf: F64, tau: F64,
) -> tuple[I64, int]:
    """Hits/trials per leading index, for one window family.

    ``coef`` is (..., group, nbins). Returns (hits, trials) where ``hits`` has
    the shape of the leading axes and ``trials`` is a scalar.
    """
    lead = coef.shape[:-2]
    a = np.abs(coef).reshape((*lead, plan.group * plan.nbins))[..., plan.perm]
    seg_max = np.maximum.reduceat(a, plan.starts, axis=-1)
    with np.errstate(divide="ignore"):
        sf_max = 4 * np.log2(seg_max) + 16.0 / 3.0 + 60.0        # (..., nb)
    a34 = a ** 0.75
    scale = np.exp2(-3.0 / 16.0 * (sf_max[..., None] * sf - 60.0))  # (..., nb, nsf)
    v = a34[..., None] * scale[..., plan.bin_band, :]               # (..., K, nsf)
    e = np.rint(v)
    e -= v
    e *= e
    err = np.add.reduceat(e, plan.starts, axis=-2) / plan.width[:, None]
    hits = np.count_nonzero(err < tau[:plan.nb, None], axis=(-2, -1))
    return hits, plan.nb * len(sf)


def detect_aac(
    seg: F64,
    offsets: I64,
    sf: F64,
    plans: tuple[BandPlan, BandPlan],
    windows: _Windows,
    tau_l: F64,
    tau_s: F64,
    chunk: int = 32,
) -> F64:
    """detect_aac.m over every candidate offset. Returns (len(offsets),)."""
    plan_l, plan_s = plans
    out: F64 = np.empty(len(offsets), dtype=np.float64)
    nf = seg.shape[0]
    long_windows = (windows.long, windows.start, windows.stop)
    for c0 in range(0, len(offsets), chunk):
        off = offsets[c0:c0 + chunk]
        blk = _frame_matrix(seg, off)                         # (no, nf, 2048)
        hits: I64 = np.empty((4, len(off), nf), dtype=np.int64)
        trials: I64 = np.empty(4, dtype=np.int64)
        for w, window in enumerate(long_windows):
            co = mdct(blk * window)[:, :, None, :]             # (no, nf, 1, 1024)
            hits[w], trials[w] = band_counts(co, plan_l, sf, tau_l)
        sh = np.stack(
            [
                blk[
                    ...,
                    OFFSET_SHORT + k * IBLEN_SHORT:
                    OFFSET_SHORT + (k + 2) * IBLEN_SHORT,
                ] * windows.short
                for k in range(NB_SHORT_WIN)
            ],
            axis=2,
        )                                                      # (no, nf, 8, 256)
        hits[3], trials[3] = band_counts(mdct(sh), plan_s, sf, tau_s)
        proba = hits / trials[:, None, None]
        best = np.argmax(proba, axis=0)                        # (no, nf)
        take_h = np.take_along_axis(hits, best[None], axis=0)[0]
        take_t = trials[best]
        out[c0:c0 + chunk] = take_h.sum(axis=1) / take_t.sum(axis=1)
    return out


def select_frames(block_energy: F64, nb_win: int) -> I64:
    """main.m frame selection: the ``nb_win`` highest-energy frames.

    ``mode='low'`` is deliberately absent — measured dead
    (derrien-refinement/README.md § "The NAC probe and ``mode=low``").
    """
    nmax = len(block_energy) - 2
    if nmax < 1:
        raise AacLatticeTooShortError("fewer than three 1024-sample blocks")
    energy = block_energy[:nmax] + block_energy[1:nmax + 1]
    with np.errstate(divide="ignore"):
        db = 10 * np.log10(energy)
    order = np.argsort(db)[::-1]
    return order[:nb_win] + 1


# --- Precomputed per-rate tables -------------------------------------------

@dataclass(frozen=True)
class _RateTables:
    """Everything ``detect_aac`` needs for one sample rate."""

    windows: _Windows
    plans: tuple[BandPlan, BandPlan]
    tau_l: F64
    tau_s: F64


@lru_cache(maxsize=8)
def _rate_tables(fs_khz: int) -> _RateTables:
    low_l, high_l, low_s, high_s, windows = init_aac(fs_khz)
    tau_l, tau_s = tau_tables(low_l, high_l, low_s, high_s)
    return _RateTables(
        windows=windows,
        plans=(
            BandPlan(low_l, high_l, IBLEN_LONG, 1),
            BandPlan(low_s, high_s, IBLEN_SHORT, NB_SHORT_WIN),
        ),
        tau_l=tau_l,
        tau_s=tau_s,
    )


# --- Decoding ---------------------------------------------------------------

@dataclass(frozen=True)
class _WavPcm:
    """A float32 WAV on disk: where its samples are and how to read them."""

    path: str
    data_offset: int
    frames: int
    channels: int
    sample_rate: int


def _parse_float_wav(path: str) -> _WavPcm:
    """Parse a RIFF/WAVE header written by ``ffmpeg -c:a pcm_f32le``.

    The stdlib ``wave`` module rejects IEEE-float WAV outright, and the
    float path is what makes the decode exact for any source bit depth up to
    24 bits (a 24-bit sample is representable in float32 without loss).
    """
    if sys.byteorder != "little":
        raise AacLatticeDecodeError("float WAV reader requires a little-endian host")
    size = os.path.getsize(path)
    with open(path, "rb") as fh:
        riff = fh.read(12)
        if len(riff) < 12 or riff[0:4] != b"RIFF" or riff[8:12] != b"WAVE":
            raise AacLatticeDecodeError("decoded output is not a RIFF/WAVE file")
        channels = 0
        sample_rate = 0
        bits = 0
        while True:
            header = fh.read(8)
            if len(header) < 8:
                raise AacLatticeDecodeError("decoded WAV has no data chunk")
            chunk_id = header[0:4]
            chunk_size = struct.unpack("<I", header[4:8])[0]
            if chunk_id == b"fmt ":
                body = fh.read(chunk_size)
                if len(body) < 16:
                    raise AacLatticeDecodeError("decoded WAV has a truncated fmt chunk")
                audio_format, channels, sample_rate = struct.unpack("<HHI", body[0:8])
                bits = struct.unpack("<H", body[14:16])[0]
                if audio_format not in (3, 0xFFFE) or bits != 32:
                    raise AacLatticeDecodeError(
                        f"decoded WAV is not 32-bit float (format={audio_format}, "
                        f"bits={bits})"
                    )
            elif chunk_id == b"data":
                data_offset = fh.tell()
                available = max(0, size - data_offset)
                data_size = min(chunk_size, available) if chunk_size else available
                break
            else:
                fh.seek(chunk_size + (chunk_size & 1), os.SEEK_CUR)
        if channels < 1 or sample_rate < 1:
            raise AacLatticeDecodeError("decoded WAV declares no channels or rate")
        return _WavPcm(
            path=path,
            data_offset=data_offset,
            frames=data_size // (channels * 4),
            channels=channels,
            sample_rate=sample_rate,
        )


def _decode_to_float_wav(src: str, dst: str) -> _WavPcm:
    """Decode ``src`` to a native-rate float32 WAV at ``dst``.

    Deliberately NOT ``lib.spectral_check._ffmpeg_to_wav``: that decoder
    forces 48 kHz / 2 channels / 30 seconds, and all three are wrong here.
    The lattice lives on the source's OWN 1024-sample grid, so resampling
    destroys it outright; the frame-selection pass wants the whole track; and
    the M/S channel combination needs the source's own channel count.

    ``-map 0:a`` is the repository's canonical audio mapping, enforced by
    ``tests/test_ffmpeg_audio_map_audit.py``'s deliberately narrow grammar. A
    file carrying more than one audio stream is refused by the WAV muxer and
    surfaces as a per-track ``AacLatticeDecodeError``, which is the right
    answer: two interleaved streams have no single frame lattice.
    """
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y",
        "-analyzeduration", "5M", "-probesize", "5M",
        "-i", _safe_path(src), "-map", "0:a",
        "-c:a", "pcm_f32le", "-f", "wav", "-bitexact", dst,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, errors="replace",
            timeout=_DECODE_TIMEOUT_SECONDS, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise AacLatticeDecodeError("ffmpeg decode timed out") from exc
    except OSError as exc:
        raise AacLatticeDecodeError(f"ffmpeg could not run: {exc}") from exc
    if result.returncode != 0:
        stderr = result.stderr.strip().splitlines()
        detail = stderr[-1] if stderr else f"ffmpeg exit {result.returncode}"
        raise AacLatticeDecodeError(f"ffmpeg: {detail}")
    return _parse_float_wav(dst)


def _read_frames(wav: _WavPcm, start_frame: int, count: int) -> F64:
    """Read ``count`` interleaved frames as float64, zero-padded at EOF."""
    with open(wav.path, "rb") as fh:
        fh.seek(wav.data_offset + start_frame * wav.channels * 4)
        raw = fh.read(count * wav.channels * 4)
    flat = np.frombuffer(raw, dtype=np.float32).astype(np.float64)
    usable = (len(flat) // wav.channels) * wav.channels
    block = flat[:usable].reshape(-1, wav.channels)
    if block.shape[0] < count:
        pad = np.zeros((count - block.shape[0], wav.channels), dtype=np.float64)
        block = np.concatenate([block, pad], axis=0)
    return block


def _channel_views(block: F64, channels: int) -> dict[str, F64]:
    """L/R/M/S (or mono) views of an interleaved (frames, channels) block."""
    if channels == 1:
        return {"mono": block[:, 0]}
    left = block[:, 0]
    right = block[:, 1]
    return {"L": left, "R": right, "M": left + right, "S": left - right}


def _block_energies(wav: _WavPcm, chunk_blocks: int = 4096) -> dict[str, F64]:
    """Streaming per-1024-block energy for L, R, M=L+R, S=L-R (or mono)."""
    nblk = wav.frames // IBLEN_LONG
    keys = ("mono",) if wav.channels == 1 else ("L", "R", "M", "S")
    acc: dict[str, F64] = {
        k: np.empty(nblk, dtype=np.float64) for k in keys
    }
    done = 0
    while done < nblk:
        want = min(chunk_blocks, nblk - done)
        block = _read_frames(wav, done * IBLEN_LONG, want * IBLEN_LONG)
        for key, samples in _channel_views(block, wav.channels).items():
            acc[key][done:done + want] = (
                samples.reshape(want, IBLEN_LONG) ** 2
            ).sum(axis=1)
        done += want
    return acc


def _read_segments(wav: _WavPcm, frames: I64, span: int) -> dict[str, F64]:
    """Per-frame contiguous segments starting at ``(frame-1) * 1024``."""
    segments = [
        _read_frames(wav, int(frame - 1) * IBLEN_LONG, span) for frame in frames
    ]
    stacked = np.stack(segments, axis=0)               # (nf, span, nch)
    if wav.channels == 1:
        return {"mono": stacked[:, :, 0]}
    left = stacked[:, :, 0]
    right = stacked[:, :, 1]
    return {"L": left, "R": right, "M": left + right, "S": left - right}


# --- Per-track analysis -----------------------------------------------------

@dataclass(frozen=True)
class AacLatticeAnalysis:
    """One track's recovered lattice. Never crosses JSON; the evidence
    Struct ``AacLatticeTrackScore`` is the persisted wire shape."""

    offset: int
    z: float
    proba: float
    sample_rate: int
    channels: int


def analyze_track(
    path: str,
    *,
    nb_win: int = DEFAULT_NB_WIN,
    nb_sf: int = DEFAULT_NB_SF,
) -> AacLatticeAnalysis:
    """Recover the MDCT frame lattice of one decoded audio file.

    Raises ``AacLatticeUnsupportedRateError`` (no band table for the rate —
    96 kHz is the live case), ``AacLatticeDecodeError``, or
    ``AacLatticeTooShortError``. Callers record those as per-track evidence.
    """
    with tempfile.TemporaryDirectory(prefix="aac_lattice_") as tmpdir:
        wav = _decode_to_float_wav(path, os.path.join(tmpdir, "audio.wav"))
        fs_khz = round(wav.sample_rate / 1000)
        if fs_khz not in SUPPORTED_RATES_KHZ:
            raise AacLatticeUnsupportedRateError(
                f"unsupported sample rate {fs_khz} kHz"
            )
        tables = _rate_tables(fs_khz)
        block_energy = _block_energies(wav)
        select_key = "mono" if wav.channels == 1 else "M"
        frames = select_frames(block_energy[select_key], nb_win)
        offsets: I64 = np.arange(IBLEN_LONG, dtype=np.int64)
        span = 2 * IBLEN_LONG + int(offsets.max())
        segments = _read_segments(wav, frames, span)

    sf: F64 = np.linspace(_SF_MIN, _SF_MAX, nb_sf)
    if wav.channels == 1:
        picks = [("mono", "mono")]
    else:
        idx = frames - 1
        mean_db = {
            key: float(np.log10(np.maximum(
                block_energy[key][idx] + block_energy[key][idx + 1], 1e-300,
            )).mean())
            for key in ("L", "R", "M", "S")
        }
        picks = [
            ("LR", "L" if mean_db["L"] >= mean_db["R"] else "R"),
            ("MS", "M" if mean_db["M"] >= mean_db["S"] else "S"),
        ]

    profiles: dict[str, F64] = {}
    for tag, channel in picks:
        profiles[tag] = detect_aac(
            segments[channel], offsets, sf, tables.plans, tables.windows,
            tables.tau_l, tables.tau_s,
        )
    best_tag = max(profiles, key=lambda k: float(profiles[k].max()))
    profile = profiles[best_tag]
    peak = float(profile.max())
    median = float(np.median(profile))
    std = float(profile.std())
    return AacLatticeAnalysis(
        offset=int(offsets[int(np.argmax(profile))]),
        z=float((peak - median) / std) if std > 0 else 0.0,
        proba=peak,
        sample_rate=wav.sample_rate,
        channels=wav.channels,
    )


# --- Per-album measurement --------------------------------------------------

def album_audio_files(folder: str) -> list[str]:
    """Every audio file under ``folder``, in deterministic relative-path order.

    Deterministic selection is the whole point: the ``MAX_SCORED_TRACKS`` cap
    means "which tracks" must not depend on filesystem iteration order, or the
    same album measured twice recovers a different offset population.
    """
    found: list[str] = []
    for root, _dirs, names in os.walk(folder):
        for name in names:
            if os.path.splitext(name)[1].lower() in AUDIO_EXTENSIONS_DOTTED:
                found.append(os.path.relpath(os.path.join(root, name), folder))
    return sorted(found)


TrackAnalyzer = Callable[[str], AacLatticeAnalysis]


def measure_album_aac_lattice(
    folder: str,
    *,
    max_scored_tracks: int = MAX_SCORED_TRACKS,
    analyze_fn: TrackAnalyzer | None = None,
) -> AacLatticeCapture:
    """Score tracks until ``max_scored_tracks`` succeed or files run out.

    Per-track failures are RECORDED, never raised: a 96 kHz track, an
    undecodable file, or a detector exception must not cost the album its
    measurement, and above all must never fail the preview job (issue #829
    invariant A-I4). Only the caller's own composition guard covers a failure
    of this function itself.

    ``analyze_fn`` is the sanctioned kwarg-DI seam
    (``.claude/rules/code-quality.md`` § "Picking a strategy"): it lets the
    generated fault-isolation property drive this real recording loop over a
    generated fault space without paying for ffmpeg per example. Production
    always takes the default.
    """
    analyzer = analyze_fn or analyze_track
    scores: list[AacLatticeTrackScore] = []
    scored = 0
    for relative_path in album_audio_files(folder):
        if scored >= max_scored_tracks:
            break
        try:
            analysis = analyzer(os.path.join(folder, relative_path))
        except Exception as exc:  # noqa: BLE001 - per-track evidence, never fatal
            logger.info(
                "AAC LATTICE: %s could not be scored: %s: %s",
                relative_path, type(exc).__name__, exc,
            )
            scores.append(AacLatticeTrackScore(
                filename=relative_path,
                error=f"{type(exc).__name__}: {exc}"[:200],
            ))
            continue
        scores.append(AacLatticeTrackScore(
            filename=relative_path,
            offset=analysis.offset,
            z=analysis.z,
            proba=analysis.proba,
        ))
        scored += 1
    return AacLatticeCapture.from_tracks(scores)
