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

import numpy as np
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.composite_audio_gap import (
    MIN_SILENCE_SECONDS,
    MIN_TRAILING_AUDIO_SECONDS,
    SILENCE_DBFS,
    CompositeAudioReadError,
    detect_composite_silence_gap,
    gap_decision_from_silence_flags,
)

_FIXTURE_SAMPLE_RATE = 8000
_TONE_FREQUENCY_HZ = 440.0
_TONE_AMPLITUDE = 16000
_FULL_SCALE_16_BIT = 32767.0


def _write_level_controlled_wav(
    path: str, segments: list[tuple[float, float]], *, seed: int = 0,
) -> None:
    """Write mono 16-bit PCM alternating segments of GAUSSIAN NOISE at
    explicit dBFS levels: each segment is ``(level_dbfs, seconds)``.

    Issue #1237 review C4: digital-zero silence and near-full-scale tones
    (``_write_synthetic_wav`` below) are silent/non-silent at ANY plausible
    ``SILENCE_DBFS`` threshold, so they cannot prove the constant's exact
    value is load-bearing. This generator produces a REALISTIC noise floor
    at a level chosen relative to the real ``-45.0`` threshold, so a
    mutated threshold and the real one classify it differently.
    """
    rng = np.random.default_rng(seed)
    with wave.open(path, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(_FIXTURE_SAMPLE_RATE)
        for level_dbfs, seconds in segments:
            frame_count = int(seconds * _FIXTURE_SAMPLE_RATE)
            rms = _FULL_SCALE_16_BIT * (10.0 ** (level_dbfs / 20.0))
            samples = np.clip(
                rng.normal(0.0, rms, frame_count), -32768, 32767,
            ).astype("<i2")
            handle.writeframesraw(samples.tobytes())


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

    # Issue #1237 review D2: every fixture below is a LITERAL 5/10, never
    # ``MIN_SILENCE_SECONDS``/``MIN_TRAILING_AUDIO_SECONDS``. Building a
    # boundary fixture FROM the same mutable symbol production reads makes
    # it agree by construction -- a mutated constant reconstructs the exact
    # same (now-wrong) boundary and the pin can never catch it. The literal
    # values below match the constants' current values (5, 10); if either
    # constant's intended value ever legitimately changes, these literals
    # must be updated by hand, which is the point.

    def test_gap_too_short_is_false(self) -> None:
        flags = [False] * 3 + [True] * 4 + [False] * 20
        self.assertFalse(gap_decision_from_silence_flags(flags))

    def test_trailing_audio_too_short_is_false(self) -> None:
        flags = [False] * 3 + [True] * 5 + [False] * 9
        self.assertFalse(gap_decision_from_silence_flags(flags))

    def test_exact_boundary_lengths_are_true(self) -> None:
        flags = [False] * 3 + [True] * 5 + [False] * 10
        self.assertTrue(gap_decision_from_silence_flags(flags))

    def test_second_gap_qualifies_even_when_first_does_not(self) -> None:
        # A too-short gap followed by a too-short trailing run, then a
        # SEPARATE qualifying gap later in the file.
        flags = (
            [False] * 2
            + [True] * 4
            + [False] * 2
            + [True] * 5
            + [False] * 10
        )
        self.assertTrue(gap_decision_from_silence_flags(flags))

    def test_empty_flags_is_false(self) -> None:
        self.assertFalse(gap_decision_from_silence_flags([]))


def _reference_gap_decision(flags: list[bool]) -> bool:
    """Independent reference oracle -- computed differently from
    production (index-scan over explicit run boundaries here; production
    groups with :func:`itertools.groupby` then scans forward), so a
    Hypothesis-found divergence is a genuine, not tautological, defect.

    Issue #1237 review D2: the two thresholds below are LITERAL 5/10, not
    ``MIN_SILENCE_SECONDS``/``MIN_TRAILING_AUDIO_SECONDS``. An oracle that
    reads the same mutable symbol production reads agrees with a mutated
    constant by construction -- it would recompute its own boundary from
    the identical mutated value and never diverge. A genuinely independent
    oracle must hardcode the constants' current, correct values.
    """
    min_silence_seconds = 5
    min_trailing_audio_seconds = 10
    n = len(flags)
    i = 0
    while i < n:
        if not flags[i]:
            i += 1
            continue
        start = i
        while i < n and flags[i]:
            i += 1
        if i - start < min_silence_seconds:
            continue
        j = i
        while j < n:
            if flags[j]:
                j += 1
                continue
            audio_start = j
            while j < n and not flags[j]:
                j += 1
            if j - audio_start >= min_trailing_audio_seconds:
                return True
    return False


@given(st.lists(st.booleans(), max_size=60))
@example(
    # The exact >= boundary: a silent run of exactly 5 (MIN_SILENCE_SECONDS'
    # current value, hardcoded per issue #1237 review D2 -- see
    # ``_reference_gap_decision``'s own docstring for why).
    [False] * 3 + [True] * 5 + [False] * 10,
)
@example(
    # Issue #1237 review D2: just BELOW the silence threshold (4, not 5) --
    # a mutated ``MIN_SILENCE_SECONDS`` <= 4 would make production accept
    # this gap while the hardcoded-literal reference still refuses it.
    [False] * 3 + [True] * 4 + [False] * 20,
)
@example(
    # Issue #1237 review D2: just BELOW the trailing-audio threshold (9,
    # not 10) -- a mutated ``MIN_TRAILING_AUDIO_SECONDS`` <= 9 would make
    # production accept this trailing run while the hardcoded-literal
    # reference still refuses it.
    [False] * 3 + [True] * 5 + [False] * 9,
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

    def test_silence_threshold_constant_is_pinned(self) -> None:
        """Direct value pin -- a first, cheap line of defense alongside the
        two behavioral fixtures below (issue #1237 review C4).
        """
        self.assertEqual(SILENCE_DBFS, -45.0)

    def test_realistic_noise_floor_below_threshold_is_a_detected_gap(self) -> None:
        """Issue #1237 review C4: a REALISTIC near-silent noise floor at
        -50dBFS (fixed literal, NOT derived from ``SILENCE_DBFS`` -- a
        derived value would shift with any mutation to the constant and
        prove nothing) sits strictly BETWEEN the real -45dBFS threshold
        and a -65dBFS regression. Digital-zero silence cannot discriminate
        this: it is silent under any plausible threshold, and a stricter
        (more negative) mutant would silently reintroduce the founding
        Bouncing Souls false positive by failing to recognise real analog
        noise floors as silence.
        """
        handle, path = tempfile.mkstemp(suffix=".wav")
        os.close(handle)
        self.addCleanup(os.remove, path)
        _write_level_controlled_wav(path, [
            (-3.0, 3.0), (-50.0, 6.0), (-3.0, 12.0),
        ], seed=1)
        self.assertTrue(detect_composite_silence_gap(path))

    def test_realistic_noise_floor_above_threshold_is_not_silent(self) -> None:
        """Brackets the threshold from the OTHER side: -40dBFS (fixed
        literal) is louder than the real -45dBFS threshold and must NOT
        register as silence, even though it is still quiet relative to
        full-scale audio. Together with the -50dBFS fixture above, this
        pair kills both a stricter (-65dBFS) and a looser (e.g. -25dBFS)
        threshold mutation.
        """
        handle, path = tempfile.mkstemp(suffix=".wav")
        os.close(handle)
        self.addCleanup(os.remove, path)
        _write_level_controlled_wav(path, [
            (-3.0, 3.0), (-40.0, 6.0), (-3.0, 12.0),
        ], seed=2)
        self.assertFalse(detect_composite_silence_gap(path))

    def test_zero_frame_file_raises_never_silently_accuses(self) -> None:
        """Issue #1237 review D6: a VALID zero-duration WAV decodes with
        ffmpeg exit 0 and zero stdout bytes -- a genuinely different path
        from the corrupt-file case above (which fails ffmpeg's own exit
        code). Without the ``samples.size < _DECODE_SAMPLE_RATE`` guard,
        this degrades to an empty flags list and
        ``gap_decision_from_silence_flags([])`` is False, silently
        accusing a file that was never readable at all.
        """
        handle, path = tempfile.mkstemp(suffix=".wav")
        os.close(handle)
        self.addCleanup(os.remove, path)
        with wave.open(path, "wb") as h:
            h.setnchannels(1)
            h.setsampwidth(2)
            h.setframerate(_FIXTURE_SAMPLE_RATE)
            h.writeframesraw(b"")
        with self.assertRaises(CompositeAudioReadError):
            detect_composite_silence_gap(path)

    def test_sub_second_file_raises_never_silently_accuses(self) -> None:
        """Issue #1237 review D6: a genuinely SHORT (0.5s) but otherwise
        valid, decodable file must also raise -- not silently decide
        "no gap" (an accusation) from a degenerate less-than-one-second
        RMS pass.
        """
        handle, path = tempfile.mkstemp(suffix=".wav")
        os.close(handle)
        self.addCleanup(os.remove, path)
        _write_synthetic_wav(path, [("tone", 0.5)])
        with self.assertRaises(CompositeAudioReadError):
            detect_composite_silence_gap(path)


if __name__ == "__main__":
    unittest.main()
