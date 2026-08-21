"""Physical audio evidence for a coalesced Discogs composite (issue #1237).

Beets coalesces consecutive flat Discogs sub-positions (e.g. ``16.1``/
``16.2``) into ONE physical track (``lib/library_completeness.py::
discogs_manifest`` reproduces that grouping). The installed file behind
such a component is either one continuous recording that already covers
the WHOLE declared program (Discogs' overlapping-duration convention) or
a genuinely short rip missing part of it. Declared sub-track durations
cannot tell these apart -- that ambiguity is issue #1237's root cause.
This module answers the question from the installed AUDIO itself: does
the file contain more than one distinct audio segment?

An "internal silence gap" is a contiguous run of >=5s below -45 dBFS
followed anywhere afterward by >=10s of non-silence before end of file.
A gap proves the file holds at least two distinct audio parts.

Accepted residual (stated in issue #1237, deliberate -- do not "fix" by
counting or title-sniffing): a composite with THREE declared parts,
genuinely missing only the third, still shows one qualifying gap (part 1
| silence | part 2) and is judged complete. Under-detection here is
preferred to a false alarm.

Never a decision by itself: the census (``lib/library_completeness.py``)
surfaces this as evidence on an operator-facing finding; nothing here
mutates Beets, deletes a file, or triggers a re-acquire.
"""
from __future__ import annotations

import subprocess
from collections.abc import Sequence
from itertools import groupby
from typing import Final

import numpy as np

#: Silence threshold in dBFS -- issue #1237's stated bound.
SILENCE_DBFS: Final[float] = -45.0
#: Minimum contiguous silent run, in whole seconds, to count as a gap.
MIN_SILENCE_SECONDS: Final[int] = 5
#: Minimum contiguous non-silent run AFTER a qualifying gap, in whole
#: seconds, required somewhere before end of file.
MIN_TRAILING_AUDIO_SECONDS: Final[int] = 10
#: Decode sample rate. Per-second RMS needs nothing higher; low enough to
#: keep the daily census's audio pass fast.
_DECODE_SAMPLE_RATE: Final[int] = 8000
_FFMPEG_TIMEOUT_SECONDS: Final[int] = 120
_FULL_SCALE_16_BIT: Final[float] = 32768.0
#: dBFS floor assigned to a perfectly-silent (zero RMS) block, where
#: ``log10(0)`` is undefined.
_MIN_DBFS: Final[float] = -120.0


class CompositeAudioReadError(RuntimeError):
    """The composite file could not be decoded -- never guessed either way."""


def gap_decision_from_silence_flags(flags: Sequence[bool]) -> bool:
    """Pure decision over one bool-per-second silence flag sequence.

    ``True`` iff some contiguous run of ``>= MIN_SILENCE_SECONDS`` silent
    seconds is followed, anywhere later in ``flags``, by a contiguous run
    of ``>= MIN_TRAILING_AUDIO_SECONDS`` non-silent seconds.
    """
    runs = [(is_silent, sum(1 for _ in group)) for is_silent, group in groupby(flags)]
    for index, (is_silent, length) in enumerate(runs):
        if not is_silent or length < MIN_SILENCE_SECONDS:
            continue
        if any(
            not later_silent and later_length >= MIN_TRAILING_AUDIO_SECONDS
            for later_silent, later_length in runs[index + 1:]
        ):
            return True
    return False


def _per_second_dbfs(samples: np.ndarray, sample_rate: int) -> list[float]:
    total_seconds = samples.size // sample_rate
    levels: list[float] = []
    for second in range(total_seconds):
        block = samples[second * sample_rate:(second + 1) * sample_rate].astype(np.float64)
        rms = float(np.sqrt(np.mean(np.square(block))))
        levels.append(
            20.0 * float(np.log10(rms / _FULL_SCALE_16_BIT)) if rms > 0 else _MIN_DBFS
        )
    return levels


def _decode_mono_pcm16(path: str) -> np.ndarray:
    """Decode ``path`` to mono s16le PCM at :data:`_DECODE_SAMPLE_RATE`."""
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", path,
        "-map", "0:a",
        "-ac", "1",
        "-ar", str(_DECODE_SAMPLE_RATE),
        "-f", "s16le",
        "-acodec", "pcm_s16le",
        "-",
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, check=False,
            timeout=_FFMPEG_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise CompositeAudioReadError(f"ffmpeg not found on PATH: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise CompositeAudioReadError(
            f"ffmpeg timed out decoding {path}: {exc}"
        ) from exc
    if proc.returncode != 0:
        tail = proc.stderr.decode("utf-8", errors="replace").strip().splitlines()[-5:]
        raise CompositeAudioReadError(
            f"ffmpeg failed decoding {path} (rc={proc.returncode}): {' / '.join(tail)}"
        )
    samples = np.frombuffer(proc.stdout, dtype="<i2")
    if samples.size == 0:
        raise CompositeAudioReadError(f"composite audio decoded to zero samples: {path}")
    return samples


def detect_composite_silence_gap(path: str) -> bool:
    """Production entry point: decode ``path`` and evaluate its per-second
    RMS against :func:`gap_decision_from_silence_flags`.

    Raises :class:`CompositeAudioReadError` on any decode failure (missing
    ffmpeg, nonzero exit, an empty/zero-sample decode, or a timeout) --
    callers must treat that as ``unknown``, never as a silent guess.
    """
    samples = _decode_mono_pcm16(path)
    levels = _per_second_dbfs(samples, _DECODE_SAMPLE_RATE)
    flags = [level < SILENCE_DBFS for level in levels]
    return gap_decision_from_silence_flags(flags)
