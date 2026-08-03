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
   decision dict whether or not the CURRENT side's proof-leg fields
   (``ultrasonic_deficit_db`` / ``spectral_measurement_version``) are
   populated, across varied mutation values. The candidate side left this
   invariant's scope in PR3, which promoted those two fields to the
   ultrasonic proof leg's inputs.
3. The four 20-22kHz extension-slice dB values never change
   ``analyze_track``'s in-window outcome (grade/cliff_detected/
   cliff_freq_hz/estimated_bitrate_kbps/hf_deficit_db) — complements the
   deterministic pin in
   ``tests/test_spectral_check.py::TestExtensionSlicesNeverFeedCliffDetection``
   (review round 2, should-fix 4).
4. A carried spectral fact and its capture facts move atomically.
5. The per-track HF-deficit grade ladder is exactly the two shipped
   MEASURED constants (issue #829 Phase 5 PR3's 65/69, replacing the
   guessed 40/60), both boundaries inclusive, with a detected cliff
   dominating at any deficit.

Every checker is a module-level pure function with a known-bad self-test
proving it actually trips on a planted violation.
"""

import math
import os
import shutil
import sys
import tempfile
import unittest
import uuid
from collections.abc import Callable
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_db import AlbumInfo
from lib.quality import (
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    CodecFamily,
    EvidenceProvenance,
    full_pipeline_decision_from_evidence,
)
from lib.quality_evidence import (
    backfill_current_evidence_from_album_info,
    snapshot_audio_files,
)
from lib.spectral_check import (
    EXTENSION_SLICE_FREQS,
    HF_DEFICIT_MARGINAL,
    HF_DEFICIT_SUSPECT,
    SLICE_FREQS,
    TrackResult,
    classify_track,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row
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
        preserve_spectral_measurement_version=True,
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
# Invariant 2: the CURRENT side's proof-leg capture fields never change a
# decision.
# ---------------------------------------------------------------------------
#
# Scope, narrowed twice as the capture fields were promoted. PR1 captured
# four fields and this invariant covered all four on both sides, because
# PR1 changed no decision. PR2b promoted ``cliff_hz`` and ``codec_family``
# to decision inputs on both sides (they are what let a spectral number be
# read in its own codec's terms instead of through LAME's MP3 encoder
# table, download 37946); their effect is patrolled in the positive
# direction by ``tests/test_spectral_decision_seam_generated.py``.
#
# PR3 promotes ``ultrasonic_deficit_db`` and
# ``spectral_measurement_version`` — on the CANDIDATE side only. The
# ultrasonic leg gates a PROMOTION, and the installed side is never
# promoted by this decision: its own proof, if it holds one, is already
# the acquisition ceiling. So the surviving invariant is that the CURRENT
# side's proof-leg facts stay inert, which is the half that can silently
# rot — nothing else in the decider has any reason to read them, and a
# stray read there would let an installed album's measurement change what
# a candidate is allowed to become.
#
# Review round 2 (should-fix 5) originally widened this checker from
# candidate-only to both sides. That widening is what makes the narrowed
# version worth keeping rather than deleting.

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
    _cliff_hz, _codec_family, ultrasonic_deficit_db, version = mutation
    # ``cliff_hz``/``codec_family`` are deliberately NOT mutated: PR2b made
    # them decision inputs, so mutating them here would assert the opposite
    # of what the decider is now required to do.
    return msgspec.structs.replace(
        measurement,
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
    for ``(candidate, current)`` whether or not the CURRENT side's
    measurement carries the proof-leg capture fields
    (``ultrasonic_deficit_db`` / ``spectral_measurement_version``), for an
    arbitrary ``mutation`` tuple?

    The candidate side left this invariant's scope in PR3, which promoted
    those two fields to the ultrasonic proof leg's inputs. A candidate
    with no ``current`` is therefore vacuously true here, and the property
    below always supplies one.

    ``decider`` is injectable ONLY so the known-bad self-tests below can
    prove this checker actually trips — production always calls it with
    the real ``full_pipeline_decision_from_evidence`` default.
    """
    baseline = decider(candidate, current)
    mutated_current = (
        msgspec.structs.replace(
            current,
            measurement=_mutated_capture_measurement(
                current.measurement, mutation,
            ),
        )
        if current is not None else None
    )
    mutated = decider(candidate, mutated_current)
    return baseline == mutated


def _decoy_decider_reads_current_ultrasonic_deficit(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """A decider that (wrongly) lets current.ultrasonic_deficit_db
    influence its output. The realistic rot this invariant guards: PR3
    made the CANDIDATE's deficit a legitimate decision input, so the next
    reader of that field is one keystroke from reading the installed
    album's."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    if (
        current is not None
        and current.measurement.ultrasonic_deficit_db is not None
    ):
        result["final_status"] = "CORRUPTED_BY_CURRENT_ULTRASONIC_DEFICIT"
    return result


def _decoy_decider_reads_current_capture_fields(
    candidate: AlbumQualityEvidence,
    current: "AlbumQualityEvidence | None" = None,
) -> "dict[str, object]":
    """A decider that (wrongly) lets current.spectral_measurement_version
    influence its output — proves the checker covers the `current` side
    too, not just `candidate` (should-fix 5)."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    if (
        current is not None
        and current.measurement.spectral_measurement_version is not None
    ):
        result["current_measurement_version"] = (
            current.measurement.spectral_measurement_version
        )
    return result


_DEFAULT_MUTATION: CaptureFieldsWorld = (17000, "mp3", 61.0, 3)


class TestDecisionIgnoresCaptureFieldsCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: decision_ignores_capture_fields must trip when
    a decider (wrongly) reads either proof-leg field on the CURRENT
    side."""

    def test_checker_trips_on_a_decider_that_reads_current_ultrasonic(self):
        candidate = make_album_quality_evidence(
            mb_release_id="capture-fields-selftest-current-ultra-candidate",
        )
        current = make_album_quality_evidence(
            mb_release_id="capture-fields-selftest-current-ultra-current",
        )
        self.assertFalse(
            decision_ignores_capture_fields(
                candidate, current, _DEFAULT_MUTATION,
                decider=_decoy_decider_reads_current_ultrasonic_deficit,
            )
        )

    def test_checker_trips_on_a_decider_that_reads_current_version(self):
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
    blind to the CURRENT side's proof-leg capture fields
    (``ultrasonic_deficit_db`` / ``spectral_measurement_version``).

    ``cliff_hz`` and ``codec_family`` left this invariant's scope in PR2b
    and the CANDIDATE side left it in PR3, both by deliberate promotion;
    their behaviour is patrolled positively in
    ``tests/test_spectral_decision_seam_generated.py`` and
    ``tests/test_quality_classification.py::TestUltrasonicProofGateV3``."""

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
        candidate = make_album_quality_evidence(
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
        current = make_album_quality_evidence(
            mb_release_id="capture-fields-pin-transcode-current",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
        )
        self.assertTrue(
            decision_ignores_capture_fields(
                candidate, current, _DEFAULT_MUTATION,
            )
        )

    @given(
        candidate=wild_ready_candidate_evidence(),
        current=wild_ready_candidate_evidence(),
        mutation=_capture_field_worlds(),
    )
    def test_never_changes_decision_across_generated_worlds(
        self, candidate, current, mutation,
    ):
        # ``current`` is never None here: the invariant is about the
        # current side, and a vacuous world proves nothing.
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
    band_db.update(zip(EXTENSION_SLICE_FREQS, extension_dbs, strict=True))

    def _rms_for_db(db: float) -> float:
        return 10 ** (db / 20.0)

    def side_effect(cmd, **_kwargs):
        sinc_idx = cmd.index("sinc")
        lo_hz = int(cmd[sinc_idx + 1].split("-")[0])
        db = band_db.get(lo_hz, _REF_DB_VALUE)
        return MagicMock(
            stderr=f"RMS     amplitude:     {_rms_for_db(db):.8f}\n",
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


# ---------------------------------------------------------------------------
# Invariant 4: the four capture fields are one atomic fact WITH
# spectral_grade at the widest boundary this PR touches — the REAL
# ``backfill_current_evidence_from_album_info`` writer composed with the
# REAL upsert guard over a REAL ``FakePipelineDB`` (round 3 review finding
# C: the BLOCKING-1 regression class had no patrolling generated property,
# only the deterministic pins in
# tests/test_quality_evidence.py::test_v3_touch_rebuild_carries_same_fingerprint_source_facts
# and ::test_same_snapshot_repair_preserves_installed_facts).
# ---------------------------------------------------------------------------

_SPECTRAL_GRADES = ("genuine", "marginal", "suspect", "likely_transcode", "error")

# The two branches of backfill_current_evidence_from_album_info that carry
# a stored spectral grade forward (lib/quality_evidence.py): a "source"
# subject always carries (any snapshot), and an "installed" subject
# preserves only on an unchanged (same-address) snapshot. installed+carried
# is not a legal stored shape (AudioQualityMeasurement.new_row_validation_errors).
_CARRY_BRANCHES: tuple[tuple[str, EvidenceProvenance], ...] = (
    ("source", "carried"),
    ("installed", "measured"),
)


@st.composite
def _atomic_backfill_worlds(draw: st.DrawFn):
    subject, expected_provenance = draw(st.sampled_from(_CARRY_BRANCHES))
    grade = draw(st.sampled_from(_SPECTRAL_GRADES))
    bitrate = draw(st.one_of(st.none(), st.integers(min_value=32, max_value=1000)))
    capture = draw(_capture_field_worlds())
    return subject, expected_provenance, grade, bitrate, capture


def spectral_and_capture_facts_move_together(
    before: AudioQualityMeasurement,
    after: AudioQualityMeasurement,
    *,
    expected_provenance: EvidenceProvenance,
) -> bool:
    """Invariant checker (round 3 review finding C): when a real backfill
    branch carries/preserves ``before``'s spectral_grade into ``after``,
    the four issue #829 Phase 5 capture facts must ALSO match ``before``'s
    exact values, with ``after.spectral_provenance`` exactly
    ``expected_provenance`` — the eight columns move as one atomic fact,
    never a stale/partial subset of it.
    """
    return (
        after.spectral_grade == before.spectral_grade
        and after.spectral_bitrate_kbps == before.spectral_bitrate_kbps
        and after.spectral_provenance == expected_provenance
        and after.cliff_hz == before.cliff_hz
        and after.codec_family == before.codec_family
        and _floats_equal(after.ultrasonic_deficit_db, before.ultrasonic_deficit_db)
        and after.spectral_measurement_version == before.spectral_measurement_version
    )


class TestSpectralAndCaptureFactsMoveTogetherCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip on a stale/partial
    capture-field subset, and pass on an exact atomic transfer."""

    def test_checker_passes_on_exact_atomic_transfer(self):
        before = AudioQualityMeasurement(
            spectral_grade="genuine", spectral_bitrate_kbps=192,
            cliff_hz=16500, codec_family="mp3",
            ultrasonic_deficit_db=42.0, spectral_measurement_version=2,
        )
        after = AudioQualityMeasurement(
            spectral_grade="genuine", spectral_bitrate_kbps=192,
            spectral_provenance="carried",
            cliff_hz=16500, codec_family="mp3",
            ultrasonic_deficit_db=42.0, spectral_measurement_version=2,
        )
        self.assertTrue(
            spectral_and_capture_facts_move_together(
                before, after, expected_provenance="carried",
            )
        )

    def test_checker_trips_when_cliff_hz_is_stranded_behind_the_grade(self):
        """This is the exact BLOCKING-1 regression shape: grade/bitrate
        carry correctly but a capture field is left behind (None) instead
        of moving with them."""
        before = AudioQualityMeasurement(
            spectral_grade="genuine", spectral_bitrate_kbps=192,
            cliff_hz=16500, codec_family="mp3",
            ultrasonic_deficit_db=42.0, spectral_measurement_version=2,
        )
        after = AudioQualityMeasurement(
            spectral_grade="genuine", spectral_bitrate_kbps=192,
            spectral_provenance="carried",
            cliff_hz=None, codec_family="mp3",
            ultrasonic_deficit_db=42.0, spectral_measurement_version=2,
        )
        self.assertFalse(
            spectral_and_capture_facts_move_together(
                before, after, expected_provenance="carried",
            )
        )

    def test_checker_trips_on_wrong_provenance(self):
        before = AudioQualityMeasurement(
            spectral_grade="genuine", cliff_hz=16500, codec_family="mp3",
        )
        after = AudioQualityMeasurement(
            spectral_grade="genuine", spectral_provenance="measured",
            cliff_hz=16500, codec_family="mp3",
        )
        self.assertFalse(
            spectral_and_capture_facts_move_together(
                before, after, expected_provenance="carried",
            )
        )


class TestBackfillCurrentEvidenceCaptureFactsAreAtomic(unittest.TestCase):
    """Pin + generated property: drives the REAL
    ``backfill_current_evidence_from_album_info`` against a REAL
    ``FakePipelineDB`` (composition, not a mocked writer — code-quality.md
    "Never mock our own writers in a composed test") over generated
    spectral-grade/bitrate/capture-field worlds, for both surviving carry
    branches (source always; installed only on an unchanged snapshot).
    Complements the deterministic pins in tests/test_quality_evidence.py."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="backfill_atomic_capture_")
        with open(os.path.join(cls.tmpdir, "01.mp3"), "wb") as fh:
            fh.write(b"backfill-atomic-capture-track-1")
        with open(os.path.join(cls.tmpdir, "02.mp3"), "wb") as fh:
            fh.write(b"backfill-atomic-capture-track-2")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def _run_backfill(self, world) -> bool:
        subject, expected_provenance, grade, bitrate, capture = world
        cliff_hz, codec_family, ultrasonic_deficit_db, version = capture

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        existing = make_album_quality_evidence(
            mb_release_id="mb-atomic-capture",
            source_path=self.tmpdir,
            files=snapshot_audio_files(self.tmpdir),
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=190,
                avg_bitrate_kbps=190,
                median_bitrate_kbps=190,
                format="MP3",
                spectral_grade=grade,
                spectral_bitrate_kbps=bitrate,
                spectral_subject=subject,
                spectral_provenance="measured",
                cliff_hz=cliff_hz,
                codec_family=codec_family,
                ultrasonic_deficit_db=ultrasonic_deficit_db,
                spectral_measurement_version=version,
            ),
        )
        db.upsert_album_quality_evidence(existing)
        persisted = db.find_album_quality_evidence(
            mb_release_id=existing.mb_release_id,
            snapshot_fingerprint=existing.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id=existing.mb_release_id,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=190,
                avg_bitrate_kbps=190,
                median_bitrate_kbps=190,
                is_cbr=False,
                album_path=self.tmpdir,
                format="MP3",
            ),
        )
        assert result.evidence is not None
        # Re-query the STORED row (the real upsert guard already ran) —
        # not just the in-Python EvidenceBuildResult, per Rule A.
        reloaded = db.find_album_quality_evidence(
            mb_release_id=result.evidence.mb_release_id,
            snapshot_fingerprint=result.evidence.snapshot_fingerprint,
        )
        assert reloaded is not None
        return spectral_and_capture_facts_move_together(
            existing.measurement, reloaded.measurement,
            expected_provenance=expected_provenance,
        )

    def test_pin_source_subject_carries_atomically(self):
        world = (
            "source", "carried", "genuine", 192,
            (16500, "mp3", 42.0, 2),
        )
        self.assertTrue(self._run_backfill(world))

    def test_pin_installed_subject_preserves_atomically_on_same_snapshot(self):
        world = (
            "installed", "measured", "likely_transcode", 128,
            (14000, "opus", 51.5, 2),
        )
        self.assertTrue(self._run_backfill(world))

    @given(world=_atomic_backfill_worlds())
    def test_capture_facts_move_atomically_across_generated_worlds(self, world):
        self.assertTrue(self._run_backfill(world))


# ---------------------------------------------------------------------------
# Invariant 5: the HF-deficit grade ladder is exactly the two shipped
# MEASURED constants, and a detected cliff always dominates it.
#
# Issue #829 Phase 5 PR3 replaced the guessed 40/60 dB thresholds with the
# measured 65/69 pair. The risk a bare pin cannot cover is the ladder
# drifting away from the constants it is supposed to embody — a branch
# that hardcodes a literal, an inverted comparison, or a boundary that
# stops being inclusive. This property reads the constants and requires
# the real classifier to agree with them at every deficit, including the
# ones no pin lists.
# ---------------------------------------------------------------------------

Classifier = Callable[..., TrackResult]


def hf_deficit_ladder_follows_the_shipped_constants(
    hf_deficit_db: float,
    cliff_freq_hz: "int | None",
    *,
    classifier: Classifier = classify_track,
) -> bool:
    """Invariant checker: the grade is the shipped constants' own ladder.

    Both boundaries are inclusive (``>=``) and the constants are READ,
    never restated — a checker spelling 65/69 as literals would pass a
    production module that had drifted to any other pair, which is the
    whole failure mode. A detected cliff dominates: it forces ``suspect``
    at any deficit, because the cliff leg is an independent detection.

    ``classifier`` is injectable ONLY so the known-bad self-tests can
    plant a drifted ladder; production always uses the default.
    """
    grade = classifier(
        hf_deficit_db=hf_deficit_db, cliff_freq_hz=cliff_freq_hz,
    ).grade
    if cliff_freq_hz is not None:
        return grade == "suspect"
    if hf_deficit_db >= HF_DEFICIT_SUSPECT:
        expected = "suspect"
    elif hf_deficit_db >= HF_DEFICIT_MARGINAL:
        expected = "marginal"
    else:
        expected = "genuine"
    return grade == expected


def _decoy_classifier_with_the_old_guessed_thresholds(
    hf_deficit_db: float,
    cliff_freq_hz: "int | None",
    **_kwargs: object,
) -> TrackResult:
    """The pre-PR3 ladder: 40 dB marginal, 60 dB suspect. Used only to
    prove the checker trips on a drifted pair."""
    if cliff_freq_hz is not None or hf_deficit_db >= 60.0:
        grade = "suspect"
    elif hf_deficit_db >= 40.0:
        grade = "marginal"
    else:
        grade = "genuine"
    return TrackResult(grade=grade, hf_deficit_db=hf_deficit_db)


def _decoy_classifier_with_an_exclusive_boundary(
    hf_deficit_db: float,
    cliff_freq_hz: "int | None",
    **_kwargs: object,
) -> TrackResult:
    """The off-by-one ladder: strictly-greater instead of at-or-above, so
    a deficit landing exactly on a constant grades one tier better."""
    if cliff_freq_hz is not None or hf_deficit_db > HF_DEFICIT_SUSPECT:
        grade = "suspect"
    elif hf_deficit_db > HF_DEFICIT_MARGINAL:
        grade = "marginal"
    else:
        grade = "genuine"
    return TrackResult(grade=grade, hf_deficit_db=hf_deficit_db)


def _decoy_classifier_that_lets_the_deficit_outrank_the_cliff(
    hf_deficit_db: float,
    cliff_freq_hz: "int | None",
    **_kwargs: object,
) -> TrackResult:
    """A ladder where a low deficit rescues a cliffed track — the
    fail-OPEN direction the cliff leg exists to prevent."""
    del cliff_freq_hz
    if hf_deficit_db >= HF_DEFICIT_SUSPECT:
        grade = "suspect"
    elif hf_deficit_db >= HF_DEFICIT_MARGINAL:
        grade = "marginal"
    else:
        grade = "genuine"
    return TrackResult(grade=grade, hf_deficit_db=hf_deficit_db)


class TestHfDeficitLadderCheckerSelfTest(unittest.TestCase):
    """Known-bad self-tests: the checker must trip on a drifted pair, an
    exclusive boundary, and a cliff the deficit is allowed to outrank."""

    def test_checker_passes_for_the_real_classifier(self):
        for deficit in (0.0, 64.9, 65.0, 68.9, 69.0, 200.0):
            with self.subTest(deficit=deficit):
                self.assertTrue(
                    hf_deficit_ladder_follows_the_shipped_constants(
                        deficit, None,
                    )
                )
        self.assertTrue(
            hf_deficit_ladder_follows_the_shipped_constants(0.0, 16000)
        )

    def test_checker_trips_on_the_old_guessed_thresholds(self):
        self.assertFalse(
            hf_deficit_ladder_follows_the_shipped_constants(
                50.0, None,
                classifier=_decoy_classifier_with_the_old_guessed_thresholds,
            )
        )

    def test_checker_trips_on_an_exclusive_boundary(self):
        self.assertFalse(
            hf_deficit_ladder_follows_the_shipped_constants(
                HF_DEFICIT_SUSPECT, None,
                classifier=_decoy_classifier_with_an_exclusive_boundary,
            )
        )

    def test_checker_trips_when_a_low_deficit_rescues_a_cliffed_track(self):
        self.assertFalse(
            hf_deficit_ladder_follows_the_shipped_constants(
                0.0, 16000,
                classifier=(
                    _decoy_classifier_that_lets_the_deficit_outrank_the_cliff
                ),
            )
        )


class TestHfDeficitLadderFollowsTheShippedConstants(unittest.TestCase):
    """Pin + generated property for the measured 65/69 HF-deficit ladder.

    The deterministic boundary pins live in
    ``tests/test_spectral_check.py::TestClassifyTrack``; this patrols the
    whole deficit line, cliffed and uncliffed."""

    def test_pin_the_measured_boundaries(self):
        for deficit, expected in (
            (64.9, "genuine"), (65.0, "marginal"),
            (68.9, "marginal"), (69.0, "suspect"),
        ):
            with self.subTest(deficit=deficit):
                self.assertEqual(
                    classify_track(
                        hf_deficit_db=deficit, cliff_freq_hz=None,
                    ).grade,
                    expected,
                )

    @given(
        hf_deficit_db=st.floats(
            min_value=-200.0, max_value=200.0,
            allow_nan=False, allow_infinity=False,
        ),
        cliff_freq_hz=st.one_of(
            st.none(), st.integers(min_value=0, max_value=24000),
        ),
    )
    def test_across_generated_worlds(self, hf_deficit_db, cliff_freq_hz):
        self.assertTrue(
            hf_deficit_ladder_follows_the_shipped_constants(
                hf_deficit_db, cliff_freq_hz,
            )
        )


if __name__ == "__main__":
    unittest.main()
