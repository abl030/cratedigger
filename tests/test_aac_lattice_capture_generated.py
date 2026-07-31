"""Generated tests — issue #829 AAC-lattice leg PR-A capture.

PR-A measures the AAC MDCT frame lattice on the promotion-plausible cohort and
persists it on ``album_quality_evidence``. The PR's whole point is that this is
CAPTURE ONLY. Three of its four invariants live here as pin + generated
property + known-bad self-test (the fourth, A-I2 detector fidelity, is
deterministic by nature and lives in ``tests/test_aac_lattice.py``):

* **A-I1 capture inertness** — persisting or decoding lattice evidence never
  changes any decision. ``full_pipeline_decision_from_evidence`` must produce a
  bit-identical decision dict whether or not EITHER side's evidence carries a
  lattice capture. Nothing reads it until PR-B's proof leg.
* **A-I3 round trip** — the capture survives the evidence read/write boundary
  exactly. The authoritative Rule A pin is the real-PostgreSQL round trip in
  ``tests/test_pipeline_db.py::TestAlbumQualityEvidenceStorage``; this property
  patrols the world space through ``FakePipelineDB``, per the established
  split (see ``tests/test_spectral_capture_generated.py``).
* **A-I4 failure isolation** — a detector error on any or all tracks is
  recorded as evidence and never fails the preview job. The deterministic pin
  drives REAL ffmpeg over REAL failing inputs
  (``tests/test_aac_lattice.py::TestAlbumMeasurementWithRealFailures``); the
  properties here patrol the recording loop and the preview composition guard
  over a generated fault space.

Every checker is a module-level pure function with a known-bad self-test
proving it actually trips on a planted violation.
"""

from __future__ import annotations

import inspect
import math
import os
import sys
import tempfile
import unittest
import uuid
from collections.abc import Callable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec
from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.aac_lattice import (
    MAX_SCORED_TRACKS,
    AacLatticeAnalysis,
    AacLatticeDecodeError,
    AacLatticeTooShortError,
    AacLatticeUnsupportedRateError,
    TrackAnalyzer,
    measure_album_aac_lattice,
)
from lib.config import CratediggerConfig
from lib.import_preview import measure_and_persist_candidate_evidence
from lib.measurement import (
    AAC_LATTICE_GATED_SPECTRAL_GRADES,
    ExistingSpectralAuditLookup,
    PreimportMeasurement,
    measure_aac_lattice,
    measure_preimport_state,
)
from lib.quality import (
    AacLatticeCapture,
    AacLatticeTrackScore,
    AlbumQualityEvidence,
    SpectralAnalysisDetail,
    full_pipeline_decision_from_evidence,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence
from tests.test_quality_generated import wild_ready_candidate_evidence

Decider = Callable[
    [AlbumQualityEvidence, "AlbumQualityEvidence | None"],
    "dict[str, object]",
]
AlbumMeasurer = Callable[[str, TrackAnalyzer], AacLatticeCapture]
PreimportRunner = Callable[..., PreimportMeasurement]


# ---------------------------------------------------------------------------
# Shared strategies
# ---------------------------------------------------------------------------

@st.composite
def _track_scores(draw: st.DrawFn, filename: str) -> AacLatticeTrackScore:
    if draw(st.booleans()):
        return AacLatticeTrackScore(
            filename=filename,
            error=draw(st.sampled_from([
                "AacLatticeUnsupportedRateError: unsupported sample rate 96 kHz",
                "AacLatticeDecodeError: ffmpeg: Invalid data found",
                "AacLatticeTooShortError: fewer than three 1024-sample blocks",
                "RuntimeError: something else entirely",
            ])),
        )
    return AacLatticeTrackScore(
        filename=filename,
        offset=draw(st.integers(min_value=0, max_value=1023)),
        z=draw(st.floats(
            min_value=-5.0, max_value=200.0,
            allow_nan=False, allow_infinity=False,
        )),
        proba=draw(st.floats(
            min_value=0.0, max_value=1.0,
            allow_nan=False, allow_infinity=False,
        )),
    )


@st.composite
def _lattice_captures(draw: st.DrawFn) -> AacLatticeCapture:
    count = draw(st.integers(min_value=0, max_value=MAX_SCORED_TRACKS))
    tracks = [
        draw(_track_scores(f"{index:02d} track.flac"))
        for index in range(count)
    ]
    return AacLatticeCapture.from_tracks(tracks)


def _floats_equal(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is b
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)


# ---------------------------------------------------------------------------
# A-I3: the capture round-trips through the evidence boundary exactly.
# ---------------------------------------------------------------------------

def capture_round_trips(
    expected: AacLatticeCapture | None,
    loaded: AacLatticeCapture | None,
) -> bool:
    """Invariant checker: is ``loaded`` byte-for-byte ``expected``?

    Compared field by field rather than by ``==`` so a float that survived
    a lossy numeric column (the exact failure a REAL scalar column would
    have produced for ``max_z``) is still caught by the tolerance, and the
    per-track array is compared as its own decoded rows.
    """
    if expected is None or loaded is None:
        return expected is loaded
    return (
        list(loaded.tracks) == list(expected.tracks)
        and loaded.modal_offset == expected.modal_offset
        and loaded.modal_count == expected.modal_count
        and loaded.scored_tracks == expected.scored_tracks
        and _floats_equal(loaded.max_z, expected.max_z)
    )


def _round_trip_through_fake_db(
    capture: AacLatticeCapture | None,
) -> AacLatticeCapture | None:
    db = FakePipelineDB()
    evidence = make_album_quality_evidence(
        mb_release_id=f"generated-lattice-{uuid.uuid4()}",
        aac_lattice=capture,
    )
    db.upsert_album_quality_evidence(evidence)
    loaded = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert loaded is not None
    return loaded.aac_lattice


class TestCaptureRoundTripCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: capture_round_trips must trip on any drift."""

    _CAPTURE = AacLatticeCapture.from_tracks([
        AacLatticeTrackScore(filename="01.flac", offset=960, z=28.5, proba=0.12),
        AacLatticeTrackScore(filename="02.flac", error="boom"),
    ])

    def test_checker_passes_on_an_exact_copy(self) -> None:
        self.assertTrue(capture_round_trips(self._CAPTURE, self._CAPTURE))

    def test_checker_passes_on_two_nones(self) -> None:
        self.assertTrue(capture_round_trips(None, None))

    def test_checker_trips_when_a_capture_becomes_none(self) -> None:
        self.assertFalse(capture_round_trips(self._CAPTURE, None))

    def test_checker_trips_on_a_dropped_track_row(self) -> None:
        stripped = msgspec.structs.replace(
            self._CAPTURE, tracks=self._CAPTURE.tracks[:1],
        )
        self.assertFalse(capture_round_trips(self._CAPTURE, stripped))

    def test_checker_trips_on_a_lost_error_string(self) -> None:
        """The failure evidence is the point: an error row that comes back
        blank is indistinguishable from a track nobody looked at."""
        blanked = msgspec.structs.replace(
            self._CAPTURE,
            tracks=[
                self._CAPTURE.tracks[0],
                AacLatticeTrackScore(filename="02.flac"),
            ],
        )
        self.assertFalse(capture_round_trips(self._CAPTURE, blanked))

    def test_checker_trips_on_single_precision_max_z(self) -> None:
        """A REAL (float4) column would have rounded 28.5 -> 28.5 but a
        typical z like 28.53125001 to ~7 digits. Pin that the checker sees
        that class of loss."""
        rounded = msgspec.structs.replace(self._CAPTURE, max_z=28.500001)
        self.assertFalse(capture_round_trips(self._CAPTURE, rounded))

    def test_checker_trips_on_a_dropped_modal_offset(self) -> None:
        stripped = msgspec.structs.replace(self._CAPTURE, modal_offset=None)
        self.assertFalse(capture_round_trips(self._CAPTURE, stripped))


class TestCaptureRoundTripsThroughEvidence(unittest.TestCase):
    """Pin + generated property for A-I3 through ``FakePipelineDB``. The
    authoritative real-PostgreSQL pin is
    ``tests/test_pipeline_db.py::TestAlbumQualityEvidenceStorage``."""

    def test_pin_apple_shaped_capture(self) -> None:
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename=f"{index:02d}.flac", offset=960,
                z=25.0 + index, proba=0.12,
            )
            for index in range(4)
        ] + [AacLatticeTrackScore(
            filename="05.flac",
            error="AacLatticeUnsupportedRateError: unsupported sample rate 96 kHz",
        )])
        self.assertTrue(
            capture_round_trips(capture, _round_trip_through_fake_db(capture))
        )

    def test_pin_never_measured(self) -> None:
        self.assertTrue(capture_round_trips(None, _round_trip_through_fake_db(None)))

    def test_pin_measured_but_nothing_scored(self) -> None:
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(filename="01.flac", error="a"),
            AacLatticeTrackScore(filename="02.flac", error="b"),
        ])
        self.assertEqual(capture.scored_tracks, 0)
        self.assertTrue(
            capture_round_trips(capture, _round_trip_through_fake_db(capture))
        )

    @given(capture=st.one_of(st.none(), _lattice_captures()))
    def test_round_trips_across_generated_worlds(
        self, capture: AacLatticeCapture | None,
    ) -> None:
        self.assertTrue(
            capture_round_trips(capture, _round_trip_through_fake_db(capture))
        )


class TestCaptureSurvivesALatticelessRePersist(unittest.TestCase):
    """The upsert's preserve guard, at the widest boundary it touches: a
    later writer for the SAME content address that never ran the cohort gate
    must not erase a lattice measured on the exact same bytes. Mirrors the
    V0 tuple's guard, not the spectral one."""

    def _evidence(self, capture: AacLatticeCapture | None):
        return make_album_quality_evidence(
            mb_release_id="mbid-lattice-preserve",
            aac_lattice=capture,
        )

    def test_pin_a_latticeless_rewrite_preserves_the_stored_capture(
        self,
    ) -> None:
        db = FakePipelineDB()
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac", offset=960, z=28.0, proba=0.13,
            ),
        ])
        db.upsert_album_quality_evidence(self._evidence(capture))
        db.upsert_album_quality_evidence(self._evidence(None))
        stored = db.find_album_quality_evidence(
            mb_release_id="mbid-lattice-preserve",
            snapshot_fingerprint=self._evidence(None).snapshot_fingerprint,
        )
        assert stored is not None
        self.assertTrue(capture_round_trips(capture, stored.aac_lattice))

    def test_pin_a_fresh_capture_replaces_the_stored_one_wholesale(
        self,
    ) -> None:
        db = FakePipelineDB()
        first = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac", offset=960, z=28.0, proba=0.13,
            ),
        ])
        second = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(filename="01.flac", error="boom"),
        ])
        db.upsert_album_quality_evidence(self._evidence(first))
        db.upsert_album_quality_evidence(self._evidence(second))
        stored = db.find_album_quality_evidence(
            mb_release_id="mbid-lattice-preserve",
            snapshot_fingerprint=self._evidence(None).snapshot_fingerprint,
        )
        assert stored is not None
        self.assertTrue(capture_round_trips(second, stored.aac_lattice))


# ---------------------------------------------------------------------------
# A-I1: the INSTALLED side's lattice capture never changes a decision.
#
# PR-A's version of this invariant covered BOTH sides, because PR-A decided
# nothing. PR-B (the proof leg) deliberately makes the CANDIDATE's capture a
# decision input, so the two-sided claim is now false by design — the fuzz
# burst falsified it on a ``max_z=13`` world within minutes of the leg
# landing, which is the property doing exactly its job.
#
# What survives, and is permanent: the INSTALLED album's own lattice is never
# a decision input. The leg gates a PROMOTION and the installed side is never
# promoted by this decision; its own proof, if it has one, was minted when it
# was the candidate and is already the acquisition ceiling (decision 21).
#
# Equivalence for the retired half: the candidate side's inertness is now
# conditional on the leg, and is covered by
# ``tests/test_quality_generated.py`` — ``the_lattice_leg_only_ever_subtracts``
# (L2, the pure decision) and ``a_non_denying_lattice_leg_leaves_the_decision_
# untouched`` (L2b, the whole evidence decider, over the same
# ``wild_ready_candidate_evidence`` world space). Both cover PASSED as well as
# withheld — a clean lattice is as inert as an unmeasured one — so between
# them the retired claim survives everywhere except a genuine denial, which is
# the only place PR-B intends it not to. Restating either here would be a
# parallel copy of a live invariant.
# ---------------------------------------------------------------------------

def decision_ignores_the_installed_lattice(
    candidate: AlbumQualityEvidence,
    current: AlbumQualityEvidence | None,
    capture: AacLatticeCapture | None,
    *,
    decider: Decider = full_pipeline_decision_from_evidence,
) -> bool:
    """Invariant checker: does ``decider`` produce the same decision dict for
    ``(candidate, current)`` whether or not the INSTALLED side's evidence
    carries ``capture``?

    Deliberately one-sided since PR-B. A decider that let the installed
    album's own frame lattice bound what a candidate is allowed to become
    would be reading evidence about the wrong bytes entirely.

    ``decider`` is injectable ONLY so the known-bad self-test below can prove
    this checker actually trips; production always uses the real default.
    """
    baseline = decider(candidate, current)
    mutated = decider(
        candidate,
        msgspec.structs.replace(current, aac_lattice=capture)
        if current is not None else None,
    )
    return baseline == mutated


def _decoy_decider_reads_the_current_lattice(
    candidate: AlbumQualityEvidence,
    current: AlbumQualityEvidence | None = None,
) -> dict[str, object]:
    """The installed album's own lattice must never bound what a candidate is
    allowed to become — not in PR-A, and not in PR-B either."""
    result: dict[str, object] = dict(
        full_pipeline_decision_from_evidence(candidate, current)
    )
    if current is not None and current.aac_lattice is not None:
        result["final_status"] = "CORRUPTED_BY_CURRENT_AAC_LATTICE"
    return result


_APPLE_SHAPED_CAPTURE = AacLatticeCapture.from_tracks([
    AacLatticeTrackScore(
        filename=f"{index:02d}.flac", offset=960, z=25.0 + index, proba=0.12,
    )
    for index in range(4)
])


class TestDecisionIgnoresLatticeCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip on a decider that reads
    the installed album's lattice, and pass for the real decider."""

    def _pair(self, tag: str):
        return (
            make_album_quality_evidence(mb_release_id=f"{tag}-candidate"),
            make_album_quality_evidence(mb_release_id=f"{tag}-current"),
        )

    def test_checker_trips_on_a_decider_that_reads_the_current(self) -> None:
        candidate, current = self._pair("lattice-selftest-current")
        self.assertFalse(
            decision_ignores_the_installed_lattice(
                candidate, current, _APPLE_SHAPED_CAPTURE,
                decider=_decoy_decider_reads_the_current_lattice,
            )
        )

    def test_checker_passes_for_the_real_decider(self) -> None:
        candidate, current = self._pair("lattice-selftest-real")
        self.assertTrue(
            decision_ignores_the_installed_lattice(
                candidate, current, _APPLE_SHAPED_CAPTURE,
            )
        )

    def test_the_apple_shaped_capture_really_denies(self) -> None:
        """The fixture is only evidence if the production leg reads it the
        way its name asserts — otherwise this whole class proves that a
        decider ignores a capture nothing would have acted on anyway."""
        from lib.quality import aac_lattice_proof_leg
        leg = aac_lattice_proof_leg(_APPLE_SHAPED_CAPTURE)
        self.assertEqual(leg.outcome, "denied")
        self.assertEqual(leg.reason, "offset_concentration")


class TestInstalledLatticeNeverChangesDecision(unittest.TestCase):
    """Pin + generated property for A-I1, as PR-B narrowed it."""

    def test_pin_a_denying_shaped_capture_on_the_installed_album(
        self,
    ) -> None:
        """The load-bearing world: the INSTALLED album's lattice screams
        Apple launder and the candidate's decision does not move a hair.

        An installed album is not up for promotion, so its lattice is
        evidence about bytes this decision is not deciding on. Before PR-B
        this pin also covered the candidate side; that half now lives with
        the leg (see this section's header)."""
        candidate = make_album_quality_evidence(
            mb_release_id="lattice-pin-import-candidate",
        )
        current = make_album_quality_evidence(
            mb_release_id="lattice-pin-import-current",
        )
        baseline = full_pipeline_decision_from_evidence(candidate, current)
        self.assertTrue(
            decision_ignores_the_installed_lattice(
                candidate, current, _APPLE_SHAPED_CAPTURE,
            )
        )
        # Guard the guard: a decision dict that never says anything would
        # make the equality above vacuous.
        self.assertIn("imported", baseline)

    def test_pin_an_all_failed_capture(self) -> None:
        candidate = make_album_quality_evidence(
            mb_release_id="lattice-pin-failed-candidate",
        )
        current = make_album_quality_evidence(
            mb_release_id="lattice-pin-failed-current",
        )
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(filename="01.flac", error="boom"),
        ])
        self.assertTrue(
            decision_ignores_the_installed_lattice(candidate, current, capture)
        )

    @given(
        candidate=wild_ready_candidate_evidence(),
        current=wild_ready_candidate_evidence(),
        capture=st.one_of(st.none(), _lattice_captures()),
    )
    def test_never_changes_decision_across_generated_worlds(
        self,
        candidate: AlbumQualityEvidence,
        current: AlbumQualityEvidence,
        capture: AacLatticeCapture | None,
    ) -> None:
        self.assertTrue(
            decision_ignores_the_installed_lattice(candidate, current, capture)
        )


# ---------------------------------------------------------------------------
# A-I4: a detector fault is recorded, never fatal.
# ---------------------------------------------------------------------------

# The exception classes the real detector raises, plus generic ones: the
# invariant is "ANY exception", not "the three we thought of".
_FAULTS: tuple[type[Exception], ...] = (
    AacLatticeUnsupportedRateError,
    AacLatticeDecodeError,
    AacLatticeTooShortError,
    RuntimeError,
    ValueError,
    OSError,
    MemoryError,
)


def _analyzer_for_pattern(
    pattern: list[type[Exception] | None],
) -> TrackAnalyzer:
    """A per-track analyzer following ``pattern``: ``None`` scores, a class
    raises it. Injected through the sanctioned ``analyze_fn`` kwarg seam so
    the REAL recording loop runs without paying for ffmpeg per example."""
    remaining = list(pattern)

    def analyze(path: str) -> AacLatticeAnalysis:
        fault = remaining.pop(0) if remaining else None
        if fault is not None:
            raise fault(f"planted fault for {os.path.basename(path)}")
        return AacLatticeAnalysis(
            offset=960, z=27.5, proba=0.12, sample_rate=44100, channels=2,
        )

    return analyze


def _real_album_measurer(
    folder: str, analyzer: TrackAnalyzer,
) -> AacLatticeCapture:
    return measure_album_aac_lattice(folder, analyze_fn=analyzer)


def _decoy_album_measurer_that_propagates(
    folder: str, analyzer: TrackAnalyzer,
) -> AacLatticeCapture:
    """The bug shape: a loop with no per-track guard, so the first 96 kHz
    track costs the whole album its measurement."""
    from lib.aac_lattice import album_audio_files
    scores: list[AacLatticeTrackScore] = []
    for relative_path in album_audio_files(folder):
        analysis = analyzer(os.path.join(folder, relative_path))
        scores.append(AacLatticeTrackScore(
            filename=relative_path, offset=analysis.offset,
            z=analysis.z, proba=analysis.proba,
        ))
    return AacLatticeCapture.from_tracks(scores)


def album_capture_records_every_attempt(
    folder: str,
    filenames: list[str],
    pattern: list[type[Exception] | None],
    *,
    measurer: AlbumMeasurer = _real_album_measurer,
) -> bool:
    """Invariant checker (A-I4): whatever the per-track analyzer does, the
    album measurement returns, records one row per attempted file in sorted
    order, honours the scored-track cap, and produces a self-consistent
    capture. A raised exception is itself a violation.
    """
    try:
        capture = measurer(folder, _analyzer_for_pattern(pattern))
    except Exception:  # noqa: BLE001 - escaping at all is the violation
        return False
    if capture.validation_errors():
        return False
    if capture.scored_tracks > MAX_SCORED_TRACKS:
        return False
    attempted = [track.filename for track in capture.tracks]
    if attempted != sorted(attempted):
        return False
    expected_prefix = sorted(filenames)[:len(attempted)]
    if attempted != expected_prefix:
        return False
    for index, track in enumerate(capture.tracks):
        planted = pattern[index] if index < len(pattern) else None
        if planted is None and track.error is not None:
            return False
        if planted is not None and not track.error:
            return False
    return True


class TestAlbumCaptureCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip on an unguarded loop."""

    def test_checker_passes_for_the_real_loop(self) -> None:
        with tempfile.TemporaryDirectory(prefix="lattice_selftest_") as root:
            names = ["01.flac", "02.flac"]
            for name in names:
                with open(os.path.join(root, name), "wb") as fh:
                    fh.write(b"x")
            self.assertTrue(album_capture_records_every_attempt(
                root, names, [AacLatticeUnsupportedRateError, None],
            ))

    def test_checker_trips_on_a_loop_with_no_per_track_guard(self) -> None:
        with tempfile.TemporaryDirectory(prefix="lattice_selftest_") as root:
            names = ["01.flac", "02.flac"]
            for name in names:
                with open(os.path.join(root, name), "wb") as fh:
                    fh.write(b"x")
            self.assertFalse(album_capture_records_every_attempt(
                root, names, [AacLatticeUnsupportedRateError, None],
                measurer=_decoy_album_measurer_that_propagates,
            ))


class TestAlbumCaptureRecordsEveryFault(unittest.TestCase):
    """Pin + generated property for A-I4's recording loop. The pin that drives
    REAL ffmpeg over REAL failing inputs is
    ``tests/test_aac_lattice.py::TestAlbumMeasurementWithRealFailures``."""

    def test_pin_every_track_faults(self) -> None:
        with tempfile.TemporaryDirectory(prefix="lattice_allfault_") as root:
            names = [f"{index:02d}.flac" for index in range(4)]
            for name in names:
                with open(os.path.join(root, name), "wb") as fh:
                    fh.write(b"x")
            self.assertTrue(album_capture_records_every_attempt(
                root, names, [AacLatticeUnsupportedRateError] * 4,
            ))

    @given(
        pattern=st.lists(
            st.one_of(st.none(), st.sampled_from(_FAULTS)),
            min_size=0, max_size=MAX_SCORED_TRACKS + 3,
        ),
    )
    def test_records_every_attempt_across_generated_fault_worlds(
        self, pattern: list[type[Exception] | None],
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="lattice_faults_") as root:
            names = [f"{index:02d}.flac" for index in range(len(pattern))]
            for name in names:
                with open(os.path.join(root, name), "wb") as fh:
                    fh.write(b"x")
            self.assertTrue(
                album_capture_records_every_attempt(root, names, pattern)
            )


# --- the preview composition guard -----------------------------------------

_GENUINE_LOSSLESS_AUDIT = SpectralAnalysisDetail(
    attempted=True,
    grade="genuine",
    bitrate_kbps=None,
    cliff_hz=None,
    codec_family="lossless",
    ultrasonic_deficit_db=41.0,
    spectral_measurement_version=2,
)


def _run_preimport(
    folder: str,
    measure_fn: Callable[[str], AacLatticeCapture],
    *,
    runner: PreimportRunner,
) -> PreimportMeasurement:
    return runner(
        path=folder,
        mb_release_id="mbid-lattice-composition",
        label="Lattice Artist - Lattice Album",
        download_filetype="flac",
        download_min_bitrate_bps=900_000,
        download_is_vbr=False,
        cfg=CratediggerConfig(audio_check_mode="off"),
        spectral_detail_analyzer=lambda _path: _GENUINE_LOSSLESS_AUDIT,
        existing_spectral_resolver=lambda _mbid: ExistingSpectralAuditLookup(),
        aac_lattice_measure_fn=measure_fn,
    )


def _decoy_runner_without_the_composition_guard(
    **kwargs: object,
) -> PreimportMeasurement:
    """The bug shape at the composition boundary: the preview worker calls
    the lattice measurement unguarded, so a failure of the measurement
    machinery itself takes the whole preview job down with it."""
    measure_fn = kwargs.pop("aac_lattice_measure_fn")
    assert callable(measure_fn)
    path = kwargs["path"]
    assert isinstance(path, str)
    measure_fn(path)
    return measure_preimport_state(**kwargs)  # pyright: ignore[reportArgumentType]


def preimport_survives_lattice_fault(
    folder: str,
    measure_fn: Callable[[str], AacLatticeCapture],
    *,
    expected_audio_files: int,
    runner: PreimportRunner = measure_preimport_state,
) -> bool:
    """Invariant checker (A-I4, composition): whatever the lattice
    measurement does, ``measure_preimport_state`` returns with every OTHER
    fact intact. A failure must cost the album its lattice and nothing else.
    """
    try:
        measurement = runner(
            path=folder,
            mb_release_id="mbid-lattice-composition",
            label="Lattice Artist - Lattice Album",
            download_filetype="flac",
            download_min_bitrate_bps=900_000,
            download_is_vbr=False,
            cfg=CratediggerConfig(audio_check_mode="off"),
            spectral_detail_analyzer=lambda _path: _GENUINE_LOSSLESS_AUDIT,
            existing_spectral_resolver=(
                lambda _mbid: ExistingSpectralAuditLookup()
            ),
            aac_lattice_measure_fn=measure_fn,
        )
    except Exception:  # noqa: BLE001 - escaping at all is the violation
        return False
    return (
        measurement.lossless_candidate
        and measurement.audio_file_count == expected_audio_files
        and measurement.download_spectral is not None
        and measurement.download_spectral.grade == "genuine"
        and not measurement.audio_corrupt
    )


class _LosslessFolder:
    """A flat FLAC-extension folder. The files never reach ffmpeg: the
    lattice measurement is injected and audio validation is off."""

    def __init__(self, count: int = 3) -> None:
        self.count = count
        self._tmp: tempfile.TemporaryDirectory[str] | None = None

    def __enter__(self) -> str:
        self._tmp = tempfile.TemporaryDirectory(prefix="lattice_preimport_")
        for index in range(self.count):
            with open(
                os.path.join(self._tmp.name, f"{index:02d}.flac"), "wb",
            ) as fh:
                fh.write(b"x")
        return self._tmp.name

    def __exit__(self, *exc: object) -> None:
        if self._tmp is not None:
            self._tmp.cleanup()


class TestPreimportSurvivesCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must trip when the composition guard
    is missing, and pass for the real ``measure_preimport_state``."""

    @staticmethod
    def _always_raises(_folder: str) -> AacLatticeCapture:
        raise MemoryError("the detector fell over")

    def test_checker_passes_for_the_real_measurement(self) -> None:
        with _LosslessFolder() as folder:
            self.assertTrue(preimport_survives_lattice_fault(
                folder, self._always_raises, expected_audio_files=3,
            ))

    def test_checker_trips_without_the_composition_guard(self) -> None:
        with _LosslessFolder() as folder:
            self.assertFalse(preimport_survives_lattice_fault(
                folder, self._always_raises, expected_audio_files=3,
                runner=_decoy_runner_without_the_composition_guard,
            ))


class TestPreimportSurvivesLatticeFaults(unittest.TestCase):
    """Pin + generated property for A-I4's composition guard."""

    def test_pin_a_total_measurement_failure_costs_only_the_lattice(
        self,
    ) -> None:
        def explode(_folder: str) -> AacLatticeCapture:
            raise RuntimeError("numpy is on fire")

        with _LosslessFolder() as folder:
            self.assertTrue(preimport_survives_lattice_fault(
                folder, explode, expected_audio_files=3,
            ))
            measurement = _run_preimport(
                folder, explode, runner=measure_preimport_state,
            )
            self.assertIsNone(measurement.aac_lattice)

    def test_pin_a_successful_measurement_reaches_the_measurement_struct(
        self,
    ) -> None:
        """Must-still-work guard: the isolation must not fail closed on the
        path the gate exists to serve."""
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac", offset=960, z=28.0, proba=0.13,
            ),
        ])
        with _LosslessFolder() as folder:
            measurement = _run_preimport(
                folder, lambda _folder: capture, runner=measure_preimport_state,
            )
            self.assertTrue(capture_round_trips(capture, measurement.aac_lattice))

    @settings(max_examples=40)
    @given(fault=st.sampled_from(_FAULTS))
    def test_survives_every_generated_fault(
        self, fault: type[Exception],
    ) -> None:
        def raise_fault(_folder: str) -> AacLatticeCapture:
            raise fault("planted composition fault")

        with _LosslessFolder() as folder:
            self.assertTrue(preimport_survives_lattice_fault(
                folder, raise_fault, expected_audio_files=3,
            ))


# ---------------------------------------------------------------------------
# The cohort gate: the measurement fires exactly on the promotion-plausible
# cohort, and only for a caller that asked to pay for it.
# ---------------------------------------------------------------------------

class _MeasurementRecorder:
    """Records the folders it was asked to measure. Not a stand-in for
    anything external — the production function is what it replaces through
    the sanctioned kwarg seam, and the assertion target is whether the gate
    called it at all."""

    def __init__(self) -> None:
        self.folders: list[str] = []

    def __call__(self, folder: str) -> AacLatticeCapture:
        self.folders.append(folder)
        return AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac", offset=960, z=28.0, proba=0.13,
            ),
        ])


def gate_fires_exactly_on_the_cohort(
    folder: str,
    *,
    filetype: str,
    grade: str,
) -> bool:
    """Invariant checker: the AAC-lattice measurement runs iff the candidate
    is a lossless container AND its album spectral grade is in
    ``AAC_LATTICE_GATED_SPECTRAL_GRADES``.

    The expected cohort is READ from the production constant, never restated
    as literals — a checker spelling genuine/marginal by hand would pass a
    module whose gate had drifted to any other set, which is the whole
    failure mode.
    """
    recorder = _MeasurementRecorder()
    measurement = measure_preimport_state(
        path=folder,
        mb_release_id="mbid-lattice-gate",
        label="Gate Artist - Gate Album",
        download_filetype=filetype,
        download_min_bitrate_bps=900_000,
        download_is_vbr=False,
        cfg=CratediggerConfig(audio_check_mode="off"),
        spectral_detail_analyzer=lambda _path: msgspec.structs.replace(
            _GENUINE_LOSSLESS_AUDIT, grade=grade,
        ),
        existing_spectral_resolver=lambda _mbid: ExistingSpectralAuditLookup(),
        aac_lattice_measure_fn=recorder,
    )
    expected = (
        measurement.lossless_candidate
        and grade in AAC_LATTICE_GATED_SPECTRAL_GRADES
    )
    measured = bool(recorder.folders)
    if measured != expected:
        return False
    if measured and recorder.folders != [folder]:
        return False
    return (measurement.aac_lattice is not None) == expected


class TestCohortGateCheckerSelfTest(unittest.TestCase):
    """Known-bad self-test: the checker must reject both directions of a
    drifted gate."""

    def test_checker_passes_on_the_shipped_gate(self) -> None:
        with _LosslessFolder() as folder:
            for filetype, grade in (
                ("flac", "genuine"), ("flac", "suspect"), ("mp3", "genuine"),
            ):
                with self.subTest(filetype=filetype, grade=grade):
                    self.assertTrue(gate_fires_exactly_on_the_cohort(
                        folder, filetype=filetype, grade=grade,
                    ))

    def test_checker_trips_when_the_measurement_never_runs(self) -> None:
        """A gate that measured nothing at all would pass a checker that only
        asserted "never measures outside the cohort"."""
        with _LosslessFolder() as folder:
            measurement = measure_preimport_state(
                path=folder,
                mb_release_id="mbid-lattice-gate",
                label="Gate Artist - Gate Album",
                download_filetype="flac",
                download_min_bitrate_bps=900_000,
                download_is_vbr=False,
                cfg=CratediggerConfig(audio_check_mode="off"),
                spectral_detail_analyzer=lambda _path: _GENUINE_LOSSLESS_AUDIT,
                existing_spectral_resolver=(
                    lambda _mbid: ExistingSpectralAuditLookup()
                ),
            )
            # No measure fn supplied: this IS the classify contract's world,
            # and it must produce no capture...
            self.assertIsNone(measurement.aac_lattice)
            # ...while the checker, which supplies one, requires that a
            # lossless/genuine album DOES get measured.
            self.assertTrue(gate_fires_exactly_on_the_cohort(
                folder, filetype="flac", grade="genuine",
            ))


class TestCohortGateFiresExactlyOnTheCohort(unittest.TestCase):
    """Pin + generated property for the cohort gate."""

    def test_pin_the_cohort_is_exactly_genuine_and_marginal(self) -> None:
        """The property above derives its expectation from this same
        constant, so it cannot see the set itself drifting — widening it to
        include ``suspect`` stays green everywhere else. The literal here is
        legitimate because the authority is an operator design decision, not
        a producer: "measure only the promotion-plausible cohort — candidate
        fileset contains lossless containers AND album spectral grade in
        {genuine, marginal}" —
        https://github.com/abl030/cratedigger/issues/829#issuecomment-5144283616
        """
        self.assertEqual(
            AAC_LATTICE_GATED_SPECTRAL_GRADES,
            frozenset({"genuine", "marginal"}),
        )

    def test_pin_lossless_and_genuine_is_measured(self) -> None:
        with _LosslessFolder() as folder:
            self.assertTrue(gate_fires_exactly_on_the_cohort(
                folder, filetype="flac", grade="genuine",
            ))

    def test_pin_a_transcode_grade_is_not_measured(self) -> None:
        """An album the spectral gate already sees through has nothing left
        for the lattice to add, and the cost is the whole reason to skip."""
        with _LosslessFolder() as folder:
            self.assertTrue(gate_fires_exactly_on_the_cohort(
                folder, filetype="flac", grade="likely_transcode",
            ))

    def test_pin_a_lossy_container_is_not_measured(self) -> None:
        with _LosslessFolder() as folder:
            self.assertTrue(gate_fires_exactly_on_the_cohort(
                folder, filetype="mp3", grade="genuine",
            ))

    @settings(max_examples=40)
    @given(
        filetype=st.sampled_from(["flac", "wav", "mp3", "opus", "ogg"]),
        grade=st.sampled_from([
            "genuine", "marginal", "suspect", "likely_transcode", "error",
        ]),
    )
    def test_gate_fires_exactly_on_the_cohort_across_generated_worlds(
        self, filetype: str, grade: str,
    ) -> None:
        with _LosslessFolder() as folder:
            self.assertTrue(gate_fires_exactly_on_the_cohort(
                folder, filetype=filetype, grade=grade,
            ))


class TestOnlyTheEvidenceProducerPaysForTheLattice(unittest.TestCase):
    """Seam pin: the measure-and-persist surface enables the capture by
    default, and the read-only classify contract does not.

    ``measure_and_persist_candidate_evidence``'s default is a definition-time
    captured dependency, so this asserts the captured default itself rather
    than patching the module binding
    (``.claude/rules/code-quality.md`` § "Picking a strategy")."""

    def test_the_evidence_producer_defaults_to_the_real_measurement(
        self,
    ) -> None:
        default = inspect.signature(
            measure_and_persist_candidate_evidence,
        ).parameters["aac_lattice_measure_fn"].default
        self.assertIs(default, measure_aac_lattice)

    def test_measure_preimport_state_defaults_to_measuring_nothing(
        self,
    ) -> None:
        """The classify contract calls ``measure_preimport_state`` without a
        measure fn; the default must therefore be "do not measure"."""
        default = inspect.signature(
            measure_preimport_state,
        ).parameters["aac_lattice_measure_fn"].default
        self.assertIsNone(default)

    def test_the_classify_contract_passes_no_measure_fn(self) -> None:
        """A lossless, genuine album measured through the classify path
        produces no lattice — the wrong-match triage UI and the CLI inspector
        are synchronous and must not block on it."""
        with _LosslessFolder() as folder:
            measurement = measure_preimport_state(
                path=folder,
                mb_release_id="mbid-lattice-classify",
                label="Classify Artist - Classify Album",
                download_filetype="flac",
                download_min_bitrate_bps=900_000,
                download_is_vbr=False,
                cfg=CratediggerConfig(audio_check_mode="off"),
                spectral_detail_analyzer=lambda _path: _GENUINE_LOSSLESS_AUDIT,
                existing_spectral_resolver=(
                    lambda _mbid: ExistingSpectralAuditLookup()
                ),
            )
        self.assertTrue(measurement.lossless_candidate)
        assert measurement.download_spectral is not None
        self.assertEqual(measurement.download_spectral.grade, "genuine")
        self.assertIsNone(measurement.aac_lattice)


if __name__ == "__main__":
    unittest.main()
