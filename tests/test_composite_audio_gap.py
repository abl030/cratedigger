"""Physical composite-audio gap detection (issue #1237).

Two tiers, deliberately separated:

* :func:`gap_decision_from_silence_flags` is a PURE function over a
  per-second silence-flag sequence -- fast, Hypothesis-friendly, no
  subprocess. Its property is the pin+property PAIR for the decision
  itself.
* :func:`detect_composite_silence_gap` bridges real audio (via ffmpeg) to
  that pure decision. It is pinned with a handful of DETERMINISTIC,
  synthetic tone+silence fixtures rather than a generated property --
  spawning ffmpeg once per Hypothesis example would violate the "no heavy
  subprocess per generated example" rule.
"""
from __future__ import annotations

import math
import os
import struct
import tempfile
import unittest
import wave
from itertools import groupby

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.composite_audio_gap import (
    MIN_SILENCE_SECONDS,
    MIN_TRAILING_AUDIO_SECONDS,
    CompositeAudioReadError,
    detect_composite_silence_gap,
    gap_decision_from_silence_flags,
)

_FIXTURE_SAMPLE_RATE = 8000
_TONE_FREQUENCY_HZ = 440.0
_TONE_AMPLITUDE = 16000


def _write_synthetic_wav(path: str, segments: list[tuple[str, float]]) -> None:
    """Write a mono 16-bit PCM WAV alternating ``"tone"``/``"silence"``
    segments, each given in whole seconds. ffmpeg decodes plain WAV
    without needing to be invoked to CREATE the fixture.
    """
    with wave.open(path, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(_FIXTURE_SAMPLE_RATE)
        for kind, seconds in segments:
            frame_count = int(seconds * _FIXTURE_SAMPLE_RATE)
            if kind == "tone":
                samples = [
                    int(_TONE_AMPLITUDE * math.sin(
                        2 * math.pi * _TONE_FREQUENCY_HZ * i / _FIXTURE_SAMPLE_RATE
                    ))
                    for i in range(frame_count)
                ]
            elif kind == "silence":
                samples = [0] * frame_count
            else:
                raise ValueError(f"unknown segment kind: {kind!r}")
            handle.writeframesraw(struct.pack(f"<{len(samples)}h", *samples))


class TestGapDecisionFromSilenceFlags(unittest.TestCase):
    """Pure decision pins -- the boundary itself is exercised separately
    below with real audio."""

    def test_qualifying_gap_followed_by_enough_trailing_audio_is_true(self) -> None:
        flags = [False] * 3 + [True] * 5 + [False] * 10
        self.assertTrue(gap_decision_from_silence_flags(flags))

    def test_no_silence_at_all_is_false(self) -> None:
        self.assertFalse(gap_decision_from_silence_flags([False] * 20))

    def test_all_silence_is_false(self) -> None:
        self.assertFalse(gap_decision_from_silence_flags([True] * 20))

    def test_gap_too_short_is_false(self) -> None:
        flags = [False] * 3 + [True] * (MIN_SILENCE_SECONDS - 1) + [False] * 20
        self.assertFalse(gap_decision_from_silence_flags(flags))

    def test_trailing_audio_too_short_is_false(self) -> None:
        flags = [False] * 3 + [True] * MIN_SILENCE_SECONDS + [False] * (
            MIN_TRAILING_AUDIO_SECONDS - 1
        )
        self.assertFalse(gap_decision_from_silence_flags(flags))

    def test_exact_boundary_lengths_are_true(self) -> None:
        flags = [False] * 3 + [True] * MIN_SILENCE_SECONDS + [False] * MIN_TRAILING_AUDIO_SECONDS
        self.assertTrue(gap_decision_from_silence_flags(flags))

    def test_second_gap_qualifies_even_when_first_does_not(self) -> None:
        # A too-short gap followed by a too-short trailing run, then a
        # SEPARATE qualifying gap later in the file.
        flags = (
            [False] * 2
            + [True] * (MIN_SILENCE_SECONDS - 1)
            + [False] * 2
            + [True] * MIN_SILENCE_SECONDS
            + [False] * MIN_TRAILING_AUDIO_SECONDS
        )
        self.assertTrue(gap_decision_from_silence_flags(flags))

    def test_empty_flags_is_false(self) -> None:
        self.assertFalse(gap_decision_from_silence_flags([]))


def _reference_gap_decision(flags: list[bool]) -> bool:
    """Independent reference oracle -- computed differently from
    production (index-scan over explicit run boundaries here; production
    groups with :func:`itertools.groupby` then scans forward), so a
    Hypothesis-found divergence is a genuine, not tautological, defect.
    """
    n = len(flags)
    i = 0
    while i < n:
        if not flags[i]:
            i += 1
            continue
        start = i
        while i < n and flags[i]:
            i += 1
        if i - start < MIN_SILENCE_SECONDS:
            continue
        j = i
        while j < n:
            if flags[j]:
                j += 1
                continue
            audio_start = j
            while j < n and not flags[j]:
                j += 1
            if j - audio_start >= MIN_TRAILING_AUDIO_SECONDS:
                return True
    return False


@given(st.lists(st.booleans(), max_size=60))
@example(
    # The exact >= boundary: a silent run of exactly MIN_SILENCE_SECONDS.
    # A random burst does not reliably land on this single point --
    # pinned per the "pin the decisive world as an @example" rule after a
    # planted off-by-one mutant (`< ` -> `<=`) survived an un-pinned run.
    [False] * 3 + [True] * MIN_SILENCE_SECONDS + [False] * MIN_TRAILING_AUDIO_SECONDS,
)
def _property_gap_decision_matches_independent_reference(flags: list[bool]) -> None:
    assert gap_decision_from_silence_flags(flags) == _reference_gap_decision(flags)


@given(st.lists(st.booleans(), max_size=60))
def _property_gap_decision_needs_a_silent_run_and_a_trailing_run(flags: list[bool]) -> None:
    """Structural invariant independent of both implementations: a run-
    length decomposition must contain SOME silent run >= MIN_SILENCE and a
    LATER non-silent run >= MIN_TRAILING for the decision to be True.
    """
    runs = [(is_silent, sum(1 for _ in group)) for is_silent, group in groupby(flags)]
    decision = gap_decision_from_silence_flags(flags)
    if decision:
        assert any(
            is_silent and length >= MIN_SILENCE_SECONDS
            and any(
                not later_silent and later_length >= MIN_TRAILING_AUDIO_SECONDS
                for later_silent, later_length in runs[index + 1:]
            )
            for index, (is_silent, length) in enumerate(runs)
        )


class TestGapDecisionGenerated(unittest.TestCase):
    def test_matches_independent_reference(self) -> None:
        _property_gap_decision_matches_independent_reference()

    def test_needs_a_silent_run_and_a_trailing_run(self) -> None:
        _property_gap_decision_needs_a_silent_run_and_a_trailing_run()


class TestDetectCompositeSilenceGap(unittest.TestCase):
    """Real-ffmpeg-decode pins over synthetic tone/digital-silence fixtures
    (issue #1237's own instruction: "Pin the thresholds with synthetic
    audio fixtures, tones + digital silence")."""

    def _fixture(self, segments: list[tuple[str, float]]) -> str:
        handle, path = tempfile.mkstemp(suffix=".wav")
        os.close(handle)
        self.addCleanup(os.remove, path)
        _write_synthetic_wav(path, segments)
        return path

    def test_bouncing_souls_shaped_gap_is_detected(self) -> None:
        """Mirrors the live 461206 evidence shape: audio, a multi-second
        silence, then a substantial trailing audio segment."""
        path = self._fixture([
            ("tone", 3.0), ("silence", 6.0), ("tone", 12.0),
        ])
        self.assertTrue(detect_composite_silence_gap(path))

    def test_continuous_tone_with_no_gap_is_not_detected(self) -> None:
        path = self._fixture([("tone", 10.0)])
        self.assertFalse(detect_composite_silence_gap(path))

    def test_gap_with_insufficient_trailing_audio_is_not_detected(self) -> None:
        path = self._fixture([("tone", 3.0), ("silence", 6.0), ("tone", 4.0)])
        self.assertFalse(detect_composite_silence_gap(path))

    def test_gap_shorter_than_threshold_is_not_detected(self) -> None:
        path = self._fixture([("tone", 3.0), ("silence", 3.0), ("tone", 12.0)])
        self.assertFalse(detect_composite_silence_gap(path))

    def test_undecodable_file_raises_never_guesses(self) -> None:
        handle, path = tempfile.mkstemp(suffix=".flac")
        os.close(handle)
        self.addCleanup(os.remove, path)
        with open(path, "wb") as fh:
            fh.write(b"not actually audio data")
        with self.assertRaises(CompositeAudioReadError):
            detect_composite_silence_gap(path)

    def test_missing_file_raises_never_guesses(self) -> None:
        with self.assertRaises(CompositeAudioReadError):
            detect_composite_silence_gap("/nonexistent/composite.flac")


if __name__ == "__main__":
    unittest.main()
