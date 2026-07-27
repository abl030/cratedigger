"""Generated tests — issue #829 Phase 5 PR1 spectral capture.

PR1 adds four measured facts to ``album_quality_evidence``
(``cliff_hz``/``codec_family``/``ultrasonic_deficit_db``/
``spectral_measurement_version``) alongside the existing spectral tuple.
The PR's whole point is that this is CAPTURE ONLY — the four fields must
round-trip through persistence exactly, and must never change what any
import/quality decision produces.

Three invariants, each shipped as pin + generated property per
``.claude/rules/code-quality.md``:

1. The four capture fields round-trip through the evidence read/write
   boundary exactly (complements the real-PG Rule A pin in
   ``tests/test_pipeline_db.py::TestAlbumQualityEvidenceStorage`` — this
   property patrols the world space via ``FakePipelineDB``, per the
   established convention that generated tests here use the fake, not
   real PG: see ``tests/_hypothesis_profiles.py``).
2. ``full_pipeline_decision_from_evidence`` produces a bit-identical
   decision dict whether or not the four fields are populated, on EITHER
   side of the comparison (``candidate`` and ``current``) and across
   varied mutation values (review round 2, should-fix 5).
3. The four 20-22kHz extension-slice dB values never change
   ``analyze_track``'s in-window outcome (grade/cliff_detected/
   cliff_freq_hz/estimated_bitrate_kbps/hf_deficit_db) — complements the
   deterministic pin in
   ``tests/test_spectral_check.py::TestExtensionSlicesNeverFeedCliffDetection``
   (review round 2, should-fix 4).

Every checker is a module-level pure function with a known-bad self-test
proving it actually trips on a planted violation.
"""

import math
import os
import sys
import unittest
import uuid
from collections.abc import Callable
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st
import msgspec

from lib.quality import (
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    CodecFamily,
    full_pipeline_decision_from_evidence,
)
from lib.spectral_check import EXTENSION_SLICE_FREQS, SLICE_FREQS, TrackResult
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence
from tests.test_quality_generated import wild_ready_candidate_evidence


Decider = Callable[
    [AlbumQualityEvidence, "AlbumQualityEvidence | None"],
    "dict[str, object]",
]


CaptureFieldsWorld = tuple[
    "int | None", "CodecFamily | None", "float | None", "int | None"
]

_CODEC_FAMILIES: tuple[CodecFamily, ...] = (
    "mp3", "aac", "opus", "vorbis", "lossless", "other",
)


@st.composite
def _capture_field_worlds(draw: st.DrawFn) -> CaptureFieldsWorld:
    cliff_hz = draw(st.one_of(
        st.none(), st.integers(min_value=0, max_value=22000),
    ))
    codec_family = draw(st.one_of(
        st.none(), st.sampled_from(_CODEC_FAMILIES),
    ))
    ultrasonic_deficit_db = draw(st.one_of(
        st.none(),
        st.floats(
            min_value=-50.0, max_value=150.0,
            allow_nan=False, allow_infinity=False,
        ),
    ))
    spectral_measurement_version = draw(st.one_of(
        st.none(), st.integers(min_value=1, max_value=5),
    ))
    return cliff_hz, codec_family, ultrasonic_deficit_db, spectral_measurement_version


def _floats_equal(a: "float | None", b: "float | None") -> bool:
    if a is None or b is None:
        return a is b
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# Invariant 1: capture fields round-trip through the evidence boundary.
# ---------------------------------------------------------------------------

def capture_fields_match(
    expected: CaptureFieldsWorld,
    measurement: AudioQualityMeasurement,
) -> bool:
    """Invariant checker: does ``measurement`` carry exactly ``expected``'s
    four issue #829 Phase 5 PR1 capture fields (cliff_hz, codec_family,
    ultrasonic_deficit_db, spectral_measurement_version)?
    """
    cliff_hz, codec_family, ultrasonic_deficit_db, version = expected
    return (
        measurement.cliff_hz == cliff_hz
        and measurement.codec_family == codec_family
        and _floats_equal(measurement.ultrasonic_deficit_db, ultrasonic_deficit_db)
        and measurement.spectral_measurement_version == version
    )


def _round_trip_through_fake_db(world: CaptureFieldsWorld) -> AudioQualityMeasurement:
    """Upsert an evidence row carrying ``world``'s capture fields into a
    fresh FakePipelineDB and return the reloaded measurement."""
    cliff_hz, codec_family, ultrasonic_deficit_db, version = world
    db = FakePipelineDB()
    evidence = make_album_quality_evidence(
        mb_release_id=f"generated-capture-{uuid.uuid4()}",
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=192,
            avg_bitrate_kbps=192,
            median_bitrate_kbps=192,
            format="MP3",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            cliff_hz=cliff_hz,
            codec_family=codec_family,
            ultrasonic_deficit_db=ultrasonic_deficit_db,
            spectral_measurement_version=version,
        ),
    )
    db.upsert_album_quality_evidence(evidence)
    loaded = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert loaded is not None
    return loaded.measurement


class TestCaptureFieldsMatchCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: capture_fields_match must trip on a mismatch."""

    def test_checker_passes_on_exact_match(self):
        m = AudioQualityMeasurement(
            cliff_hz=16500, codec_family="mp3",
            ultrasonic_deficit_db=42.5, spectral_measurement_version=2,
        )
        self.assertTrue(capture_fields_match((16500, "mp3", 42.5, 2), m))

    def test_checker_trips_on_wrong_cliff_hz(self):
        m = AudioQualityMeasurement(cliff_hz=16500, codec_family="mp3")
        self.assertFalse(capture_fields_match((99999, "mp3", None, None), m))

    def test_checker_trips_on_wrong_codec_family(self):
        m = AudioQualityMeasurement(cliff_hz=16500, codec_family="mp3")
        self.assertFalse(capture_fields_match((16500, "aac", None, None), m))

    def test_checker_trips_on_wrong_ultrasonic_deficit(self):
        m = AudioQualityMeasurement(ultrasonic_deficit_db=42.5)
        self.assertFalse(capture_fields_match((None, None, 99.0, None), m))

    def test_checker_trips_on_wrong_version(self):
        m = AudioQualityMeasurement(spectral_measurement_version=2)
        self.assertFalse(capture_fields_match((None, None, None, 3), m))


class TestCaptureFieldsRoundTripFakeDB(unittest.TestCase):
    """Pin + generated property: the four capture fields round-trip
    exactly through upsert/find (FakePipelineDB — the real-PG Rule A pin
    lives in tests/test_pipeline_db.py, per the established split)."""

    def test_pin_all_four_fields_populated(self):
        world: CaptureFieldsWorld = (16500, "mp3", 42.5, 2)
        self.assertTrue(
            capture_fields_match(world, _round_trip_through_fake_db(world))
        )

    def test_pin_all_four_fields_none(self):
        world: CaptureFieldsWorld = (None, None, None, None)
        self.assertTrue(
            capture_fields_match(world, _round_trip_through_fake_db(world))
        )

    @given(world=_capture_field_worlds())
    def test_round_trips_across_generated_worlds(self, world: CaptureFieldsWorld):
        self.assertTrue(
            capture_fields_match(world, _round_trip_through_fake_db(world))
        )


# ---------------------------------------------------------------------------
# Invariant 2: the four capture fields never change a decision.
# ---------------------------------------------------------------------------
#
# Review round 2 (should-fix 5): the original version only ever mutated
# `candidate` and always passed `current=None`, but
# `full_pipeline_decision_from_evidence` also reads `current.measurement`
# (`lib/quality/pipeline.py` — the existing_spectral_grade/bitrate lines).
# A decider that read `current.measurement.codec_family` would have
# survived the old checker. This version mutates BOTH sides with an
# arbitrary (Hypothesis-drawn, not fixed) mutation tuple.

def _mutated_capture_measurement(
    measurement: AudioQualityMeasurement,
    mutation: CaptureFieldsWorld,
) -> AudioQualityMeasurement:
    if measurement.spectral_grade is None:
        # The four capture fields are one atomic fact WITH spectral_grade
        # (AudioQualityMeasurement.new_row_validation_errors, should-fix
        # 6) — no real producer ever sets them without a grade, so
        # mutating them here would build a shape
        # full_pipeline_decision_from_evidence's own evidence-readiness
        # check now explicitly rejects, not a world any production path
        # can reach.
        return measurement
    cliff_hz, codec_family, ultrasonic_deficit_db, version = mutation
    return msgspec.structs.replace(
        measurement,
        cliff_hz=cliff_hz,
        codec_family=codec_family,
        ultrasonic_deficit_db=ultrasonic_deficit_db,
        spectral_measurement_version=version,
    )


def decision_ignores_capture_fields(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None",
    mutation: CaptureFieldsWorld,
    *,
    decider: Decider = full_pipeline_decision_from_evidence,
) -> bool:
    """Invariant checker: does ``decider`` produce the same decision dict
    for ``(candidate, current)`` whether or not EITHER side's measurement
    carries the issue #829 Phase 5 PR1 capture fields, for an arbitrary
    ``mutation`` tuple?

    ``decider`` is injectable ONLY so the known-bad self-tests below can
    prove this checker actually trips — production always calls it with
    the real ``full_pipeline_decision_from_evidence`` default.
    """
    baseline = decider(candidate, current)
    mutated_candidate = msgspec.structs.replace(
        candidate,
        measurement=_mutated_capture_measurement(candidate.measurement, mutation),
    )
    mutated_current = (
        msgspec.structs.replace(
            current,
            measurement=_mutated_capture_measurement(
                current.measurement, mutation,
            ),
        )
        if current is not None else None
    )
    mutated = decider(mutated_candidate, mutated_current)
    return baseline == mutated


def _decoy_decider_reads_candidate_capture_fields(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """A decider that (wrongly) lets candidate.cliff_hz influence its
    output — used only to prove the checker can detect that."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    if candidate.measurement.cliff_hz is not None:
        result["final_status"] = "CORRUPTED_BY_CANDIDATE_CLIFF_HZ"
    return result


def _decoy_decider_reads_current_capture_fields(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """A decider that (wrongly) lets current.codec_family influence its
    output — proves the checker covers the `current` side too, not just
    `candidate` (should-fix 5)."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    if current is not None and current.measurement.codec_family is not None:
        result["final_status"] = "CORRUPTED_BY_CURRENT_CODEC_FAMILY"
    return result


_DEFAULT_MUTATION: CaptureFieldsWorld = (17000, "mp3", 61.0, 2)


class TestDecisionIgnoresCaptureFieldsCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: decision_ignores_capture_fields must trip when
    a decider (wrongly) reads one of the new fields, on EITHER side."""

    def test_checker_trips_on_a_decider_that_reads_candidate_cliff_hz(self):
        evidence = make_album_quality_evidence(
            mb_release_id="capture-fields-checker-selftest-candidate",
        )
        self.assertFalse(
            decision_ignores_capture_fields(
                evidence, None, _DEFAULT_MUTATION,
                decider=_decoy_decider_reads_candidate_capture_fields,
            )
        )

    def test_checker_trips_on_a_decider_that_reads_current_codec_family(self):
        candidate = make_album_quality_evidence(
            mb_release_id="capture-fields-checker-selftest-current-candidate",
        )
        current = make_album_quality_evidence(
            mb_release_id="capture-fields-checker-selftest-current-current",
        )
        self.assertFalse(
            decision_ignores_capture_fields(
                candidate, current, _DEFAULT_MUTATION,
                decider=_decoy_decider_reads_current_capture_fields,
            )
        )

    def test_checker_passes_for_the_real_decider(self):
        candidate = make_album_quality_evidence(
            mb_release_id="capture-fields-checker-selftest-real-candidate",
        )
        current = make_album_quality_evidence(
            mb_release_id="capture-fields-checker-selftest-real-current",
        )
        self.assertTrue(
            decision_ignores_capture_fields(
                candidate, current, _DEFAULT_MUTATION,
            )
        )


class TestNewCaptureFieldsNeverChangeDecision(unittest.TestCase):
    """Pin + generated property: full_pipeline_decision_from_evidence is
    blind to the four issue #829 Phase 5 PR1 capture fields, on the
    candidate AND current side."""

    def test_pin_genuine_mp3_import_with_a_current_side(self):
        candidate = make_album_quality_evidence(
            mb_release_id="capture-fields-pin-genuine-mp3",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
            ),
        )
        current = make_album_quality_evidence(
            mb_release_id="capture-fields-pin-genuine-mp3-current",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=192,
                avg_bitrate_kbps=192,
                median_bitrate_kbps=192,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
        )
        self.assertTrue(
            decision_ignores_capture_fields(
                candidate, current, _DEFAULT_MUTATION,
            )
        )

    def test_pin_likely_transcode_reject(self):
        evidence = make_album_quality_evidence(
            mb_release_id="capture-fields-pin-transcode",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
        )
        self.assertTrue(
            decision_ignores_capture_fields(evidence, None, _DEFAULT_MUTATION)
        )

    @given(
        candidate=wild_ready_candidate_evidence(),
        current=st.one_of(st.none(), wild_ready_candidate_evidence()),
        mutation=_capture_field_worlds(),
    )
    def test_never_changes_decision_across_generated_worlds(
        self, candidate, current, mutation,
    ):
        self.assertTrue(
            decision_ignores_capture_fields(candidate, current, mutation)
        )


# ---------------------------------------------------------------------------
# Invariant 3: extension-slice content never changes analyze_track's
# in-window outcome (review round 2, should-fix 4 — the PAIR for the pin in
# tests/test_spectral_check.py::TestExtensionSlicesNeverFeedCliffDetection).
# ---------------------------------------------------------------------------

# Same flat, no-cliff, no-hf-deficit fixture as the deterministic pin.
_REF_DB_VALUE = -20.0
_NO_CLIFF_IN_WINDOW_DB = -20.0


def _in_window_outcome(track: TrackResult) -> tuple:
    """The subset of TrackResult facts extension-slice content must never
    affect."""
    return (
        track.grade, track.cliff_detected, track.cliff_freq_hz,
        track.estimated_bitrate_kbps, track.hf_deficit_db,
    )


def _side_effect_with_extension_dbs(
    extension_dbs: tuple[float, float, float, float],
):
    """Mocked ``subprocess.run`` side_effect: fixed flat no-cliff in-window
    slices (matching the deterministic pin's fixture) plus the given 4
    extension-band dB values."""
    band_db: dict[int, float] = {1000: _REF_DB_VALUE}
    band_db.update((f, _NO_CLIFF_IN_WINDOW_DB) for f in SLICE_FREQS)
    band_db.update(zip(EXTENSION_SLICE_FREQS, extension_dbs))

    def _rms_for_db(db: float) -> float:
        return 10 ** (db / 20.0)

    def side_effect(cmd, **_kwargs):
        sinc_idx = cmd.index("sinc")
        lo_hz = int(cmd[sinc_idx + 1].split("-")[0])
        db = band_db.get(lo_hz, _REF_DB_VALUE)
        return MagicMock(
            stderr="RMS     amplitude:     %.8f\n" % _rms_for_db(db),
            returncode=0,
        )

    return side_effect


def extension_dbs_do_not_change_in_window_outcome(
    extension_dbs: tuple[float, float, float, float],
) -> bool:
    """Invariant checker: does varying the 4 extension-slice dB values
    change analyze_track's in-window outcome versus an all-floored
    baseline?"""
    from lib.spectral_check import analyze_track

    with patch(
        "lib.spectral_check.subprocess.run",
        side_effect=_side_effect_with_extension_dbs((-140.0, -140.0, -140.0, -140.0)),
    ):
        baseline = analyze_track("/fake/baseline.flac", trim_seconds=30)
    with patch(
        "lib.spectral_check.subprocess.run",
        side_effect=_side_effect_with_extension_dbs(extension_dbs),
    ):
        varied = analyze_track("/fake/varied.flac", trim_seconds=30)
    return _in_window_outcome(baseline) == _in_window_outcome(varied)


class TestInWindowOutcomeCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: _in_window_outcome must trip on a differing
    in-window fact, and must NOT be sensitive to a capture-only field
    (codec_family) that isn't part of the in-window outcome."""

    def test_checker_trips_on_a_differing_grade(self):
        a = TrackResult(grade="genuine", hf_deficit_db=10.0)
        b = TrackResult(grade="suspect", hf_deficit_db=10.0)
        self.assertNotEqual(_in_window_outcome(a), _in_window_outcome(b))

    def test_checker_trips_on_a_differing_cliff_freq(self):
        a = TrackResult(grade="suspect", cliff_detected=True, cliff_freq_hz=16000)
        b = TrackResult(grade="suspect", cliff_detected=True, cliff_freq_hz=18000)
        self.assertNotEqual(_in_window_outcome(a), _in_window_outcome(b))

    def test_checker_ignores_codec_family_by_design(self):
        a = TrackResult(grade="genuine", hf_deficit_db=10.0, codec_family="mp3")
        b = TrackResult(grade="genuine", hf_deficit_db=10.0, codec_family="opus")
        self.assertEqual(_in_window_outcome(a), _in_window_outcome(b))


class TestExtensionSlicesNeverFeedCliffDetectionProperty(unittest.TestCase):
    """Generated property companion to the deterministic pin in
    tests/test_spectral_check.py::TestExtensionSlicesNeverFeedCliffDetection."""

    @given(extension_dbs=st.tuples(*(
        st.floats(min_value=-140.0, max_value=0.0,
                  allow_nan=False, allow_infinity=False)
        for _ in range(4)
    )))
    def test_extension_band_content_never_changes_in_window_outcome(
        self, extension_dbs,
    ):
        self.assertTrue(
            extension_dbs_do_not_change_in_window_outcome(extension_dbs)
        )


if __name__ == "__main__":
    unittest.main()
