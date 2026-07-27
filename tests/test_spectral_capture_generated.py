"""Generated tests — issue #829 Phase 5 PR1 spectral capture.

PR1 adds four measured facts to ``album_quality_evidence``
(``cliff_hz``/``codec_family``/``ultrasonic_deficit_db``/
``spectral_measurement_version``) alongside the existing spectral tuple.
The PR's whole point is that this is CAPTURE ONLY — the four fields must
round-trip through persistence exactly, and must never change what any
import/quality decision produces.

Two invariants, each shipped as pin + generated property per
``.claude/rules/code-quality.md``:

1. The four capture fields round-trip through the evidence read/write
   boundary exactly (complements the real-PG Rule A pin in
   ``tests/test_pipeline_db.py::TestAlbumQualityEvidenceStorage`` — this
   property patrols the world space via ``FakePipelineDB``, per the
   established convention that generated tests here use the fake, not
   real PG: see ``tests/_hypothesis_profiles.py``).
2. ``full_pipeline_decision_from_evidence`` produces a bit-identical
   decision dict whether or not the four fields are populated.

Both checkers are module-level pure functions with a known-bad self-test
proving each one actually trips on a planted violation.
"""

import math
import os
import sys
import unittest
import uuid
from collections.abc import Callable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st
import msgspec

from lib.quality import (
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    full_pipeline_decision_from_evidence,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence
from tests.test_quality_generated import wild_ready_candidate_evidence


Decider = Callable[
    [AlbumQualityEvidence, "AlbumQualityEvidence | None"],
    "dict[str, object]",
]


CaptureFieldsWorld = tuple[
    "int | None", "str | None", "float | None", "int | None"
]

_CODEC_FAMILIES = ("mp3", "aac", "opus", "vorbis", "lossless", "other")


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

def _mutated_capture_measurement(
    measurement: AudioQualityMeasurement,
) -> AudioQualityMeasurement:
    return msgspec.structs.replace(
        measurement,
        cliff_hz=17000,
        codec_family="mp3",
        ultrasonic_deficit_db=61.0,
        spectral_measurement_version=2,
    )


def decision_ignores_capture_fields(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
    *,
    decider: Decider = full_pipeline_decision_from_evidence,
) -> bool:
    """Invariant checker: does ``decider`` produce the same decision dict
    for ``candidate`` whether or not its measurement carries the issue
    #829 Phase 5 PR1 capture fields?

    ``decider`` is injectable ONLY so the known-bad self-test below can
    prove this checker actually trips — production always calls it with
    the real ``full_pipeline_decision_from_evidence`` default.
    """
    baseline = decider(candidate, current)
    mutated_candidate = msgspec.structs.replace(
        candidate,
        measurement=_mutated_capture_measurement(candidate.measurement),
    )
    mutated = decider(mutated_candidate, current)
    return baseline == mutated


def _decoy_decider_reads_capture_fields(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """A decider that (wrongly) lets cliff_hz influence its output — used
    only to prove decision_ignores_capture_fields can detect that."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    if candidate.measurement.cliff_hz is not None:
        result["final_status"] = "CORRUPTED_BY_CLIFF_HZ"
    return result


class TestDecisionIgnoresCaptureFieldsCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: decision_ignores_capture_fields must trip when
    a decider (wrongly) reads one of the new fields."""

    def test_checker_trips_on_a_decider_that_reads_cliff_hz(self):
        evidence = make_album_quality_evidence(
            mb_release_id="capture-fields-checker-selftest",
        )
        self.assertFalse(
            decision_ignores_capture_fields(
                evidence, None, decider=_decoy_decider_reads_capture_fields,
            )
        )

    def test_checker_passes_for_the_real_decider(self):
        evidence = make_album_quality_evidence(
            mb_release_id="capture-fields-checker-selftest-real",
        )
        self.assertTrue(decision_ignores_capture_fields(evidence, None))


class TestNewCaptureFieldsNeverChangeDecision(unittest.TestCase):
    """Pin + generated property: full_pipeline_decision_from_evidence is
    blind to the four issue #829 Phase 5 PR1 capture fields."""

    def test_pin_genuine_mp3_import(self):
        evidence = make_album_quality_evidence(
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
        self.assertTrue(decision_ignores_capture_fields(evidence, None))

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
        self.assertTrue(decision_ignores_capture_fields(evidence, None))

    @given(candidate=wild_ready_candidate_evidence())
    def test_never_changes_decision_across_generated_worlds(self, candidate):
        self.assertTrue(decision_ignores_capture_fields(candidate, None))


if __name__ == "__main__":
    unittest.main()
