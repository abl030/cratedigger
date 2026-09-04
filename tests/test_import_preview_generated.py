"""Generated invariants for the two preview lanes in ``lib/import_preview.py``.

1. The curator bad-hash gate fires through the real measure-and-persist
   preview lane. The deterministic composition pins live in
   ``tests/test_import_preview.py::TestBadHashGateReachesPreviewLanes``; this
   property patrols the world space around them — track counts, seeded-row
   kinds (exact match / different digest / same digest under another format /
   empty table) — driving the REAL ``measure_and_persist_candidate_evidence``
   so a regression that stops the lane passing its DB handle into
   ``measure_preimport_state``'s ``bad_hash_db`` port (the pre-fix world: both
   lanes passed ``db=None``, leaving every curator-reported hash unreachable)
   cannot ship green.

2. The read-only classify surface (``preview_import_from_path``) agrees
   with the real evidence decider (``candidate_preimport_reject_fact``) on
   which of audio_corrupt / bad_audio_hash / nested_layout wins, for every
   combination of the three facts (issue #1355 item 1's residual, "Batch
   C"). Deterministic pins live in ``tests/test_import_preview.py``
   (``test_corrupt_and_nested_agrees_with_the_evidence_decider_on_audio_corrupt``,
   ``test_nested_only_still_reports_nested_layout_with_flatten_detail``);
   this property patrols the world space so a future re-ordering of the
   classify surface's own precedence cannot silently disagree with the
   decider again.
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.audio_hash import hash_audio_content
from lib.dispatch.types import ImportOneRun
from lib.import_preview import (
    ImportPreviewResult,
    measure_and_persist_candidate_evidence,
    preview_import_from_path,
)
from lib.measurement import (
    ExistingSpectralAuditLookup,
    LocalFileInspection,
    PreimportMeasurement,
)
from lib.pipeline_db import BadAudioHashInput
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    SpectralAnalysisDetail,
    candidate_preimport_reject_fact,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.evidence_helpers import (
    build_parity_candidate_evidence,
    make_audio_corrupt_validation_report,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from tests.test_import_preview import (
    _PREVIEW_SOURCE_ROOT,
    _preview_runtime_config,
)

_FIXTURE_MP3 = (
    Path(__file__).parent / "fixtures" / "audio_hash" / "sine_440.mp3"
)

# What the seeded ``bad_audio_hashes`` table holds relative to the world's
# tracks. Only ``match`` may fire the gate: the lookup keys on the exact
# (hash_value, audio_format) pair.
_SEED_KINDS = ("match", "different_digest", "wrong_format", "empty_table")


def bad_hash_gate_violations(
    *,
    match_expected: bool,
    expected_bad_hash_id: int | None,
    preview: ImportPreviewResult,
    evidence_rows: list[object],
    harness_called: bool,
) -> list[str]:
    """Accumulating checker: every clause evaluates, ordering cannot mask one."""
    violations: list[str] = []
    if len(evidence_rows) != 1:
        violations.append(
            f"expected exactly one persisted evidence row, got {len(evidence_rows)}"
        )
        return violations
    evidence = evidence_rows[0]
    matched_id = getattr(evidence, "matched_bad_audio_hash_id", None)
    if match_expected:
        if preview.decision != "bad_audio_hash":
            violations.append(
                "matching world must decide bad_audio_hash, got "
                f"{preview.decision!r}"
            )
        if matched_id != expected_bad_hash_id:
            violations.append(
                "matching world must persist the matched bad-hash id "
                f"{expected_bad_hash_id!r}, got {matched_id!r}"
            )
        if harness_called:
            violations.append(
                "matching world must short-circuit before the harness"
            )
    else:
        if matched_id is not None:
            violations.append(
                f"non-matching world persisted a bad-hash match: {matched_id!r}"
            )
        if not harness_called:
            violations.append(
                "non-matching world must continue through the harness"
            )
    return violations


class TestBadHashGateThroughPreviewLaneGenerated(unittest.TestCase):
    @given(
        n_tracks=st.integers(min_value=1, max_value=3),
        seed_kind=st.sampled_from(_SEED_KINDS),
        decoy_first=st.booleans(),
    )
    @example(n_tracks=1, seed_kind="match", decoy_first=False)
    @example(n_tracks=1, seed_kind="match", decoy_first=True)
    @example(n_tracks=1, seed_kind="wrong_format", decoy_first=False)
    @example(n_tracks=3, seed_kind="different_digest", decoy_first=False)
    @example(n_tracks=1, seed_kind="empty_table", decoy_first=False)
    def test_gate_fires_iff_a_seeded_row_matches_track_bytes(
        self, n_tracks: int, seed_kind: str, decoy_first: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            artist_name="Artist",
            album_title="Album",
        ))
        digest = hash_audio_content(_FIXTURE_MP3, "mp3")
        if decoy_first and seed_kind != "empty_table":
            # Shift the matching row's id off 1, so the expected id can
            # only come from the seeded row itself — never from the count
            # ``add_bad_audio_hashes`` returns or a hardcoded constant.
            db.add_bad_audio_hashes(
                request_id=42,
                reported_username="curator",
                reason="unrelated rip",
                hashes=[BadAudioHashInput(
                    hash_value=b"\x01" * len(digest), audio_format="ogg")],
            )
        expected_bad_hash_id: int | None = None
        if seed_kind == "match":
            db.add_bad_audio_hashes(
                request_id=42,
                reported_username="curator",
                reason="exemplar bad rip",
                hashes=[BadAudioHashInput(
                    hash_value=digest, audio_format="mp3")],
            )
            expected_bad_hash_id = db.bad_audio_hashes[-1].id
        elif seed_kind == "different_digest":
            db.add_bad_audio_hashes(
                request_id=42,
                reported_username="curator",
                reason="some other rip",
                hashes=[BadAudioHashInput(
                    hash_value=b"\x00" * len(digest), audio_format="mp3")],
            )
        elif seed_kind == "wrong_format":
            # Same bytes reported under another format: the lookup keys on
            # the exact (hash_value, audio_format) pair, so this never fires.
            db.add_bad_audio_hashes(
                request_id=42,
                reported_username="curator",
                reason="same bytes, other format",
                hashes=[BadAudioHashInput(
                    hash_value=digest, audio_format="flac")],
            )
        match_expected = seed_kind == "match"
        download_log_id = db.log_download(42, outcome="rejected")

        # try/finally, NOT addCleanup: inside a ``@given`` body addCleanup
        # defers removal past every example and leaks one tmpfs world per
        # example (issue #1214; enforced by test_given_body_cleanup_audit).
        source = tempfile.mkdtemp(dir=_PREVIEW_SOURCE_ROOT)
        try:
            for index in range(n_tracks):
                shutil.copy(
                    _FIXTURE_MP3,
                    os.path.join(source, f"{index + 1:02d} - Track.mp3"),
                )

            harness_run = ImportOneRun(
                command=("import_one",), returncode=0, stdout="", stderr="",
                import_result=ImportResult(
                    decision="import",
                    source_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=320, avg_bitrate_kbps=320,
                        median_bitrate_kbps=320, format="MP3",
                    ),
                ),
            )
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(pipeline_db_enabled=True),
            ), patch(
                "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()
            ), patch(
                "lib.import_preview.run_import_one", return_value=harness_run,
            ) as mock_run:
                preview = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    download_log_id=download_log_id,
                    spectral_detail_analyzer=(
                        lambda _path: SpectralAnalysisDetail(
                            attempted=True, grade="genuine", bitrate_kbps=320,
                            spectral_measurement_version=(
                                SPECTRAL_MEASUREMENT_VERSION
                            ),
                        )
                    ),
                    existing_spectral_resolver=(
                        lambda _release_id: ExistingSpectralAuditLookup()
                    ),
                )
        finally:
            shutil.rmtree(source, ignore_errors=True)

        self.assertEqual(preview.verdict, "evidence_ready")
        violations = bad_hash_gate_violations(
            match_expected=match_expected,
            expected_bad_hash_id=expected_bad_hash_id,
            preview=preview,
            evidence_rows=list(db.album_quality_evidence.values()),
            harness_called=mock_run.called,
        )
        self.assertEqual(violations, [])


class TestBadHashGateCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: every clause of the checker trips on the exact
    world that violates it, with its own message."""

    def _ready_preview(self, decision: str) -> ImportPreviewResult:
        return ImportPreviewResult(
            mode="path", verdict="evidence_ready", decision=decision,
        )

    def _evidence(self, matched_id: int | None) -> object:
        class _Row:
            matched_bad_audio_hash_id = matched_id
        return _Row()

    def test_missing_evidence_row_trips(self):
        violations = bad_hash_gate_violations(
            match_expected=True, expected_bad_hash_id=1,
            preview=self._ready_preview("bad_audio_hash"),
            evidence_rows=[], harness_called=False,
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("exactly one persisted evidence row", violations[0])

    def test_match_world_wrong_decision_trips(self):
        violations = bad_hash_gate_violations(
            match_expected=True, expected_bad_hash_id=1,
            preview=self._ready_preview("import"),
            evidence_rows=[self._evidence(1)], harness_called=False,
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("must decide bad_audio_hash", violations[0])

    def test_match_world_unpersisted_id_trips(self):
        violations = bad_hash_gate_violations(
            match_expected=True, expected_bad_hash_id=1,
            preview=self._ready_preview("bad_audio_hash"),
            evidence_rows=[self._evidence(None)], harness_called=False,
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("must persist the matched bad-hash id", violations[0])

    def test_match_world_harness_run_trips(self):
        violations = bad_hash_gate_violations(
            match_expected=True, expected_bad_hash_id=1,
            preview=self._ready_preview("bad_audio_hash"),
            evidence_rows=[self._evidence(1)], harness_called=True,
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("short-circuit before the harness", violations[0])

    def test_non_match_world_phantom_match_trips(self):
        violations = bad_hash_gate_violations(
            match_expected=False, expected_bad_hash_id=None,
            preview=self._ready_preview("import"),
            evidence_rows=[self._evidence(7)], harness_called=True,
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("persisted a bad-hash match", violations[0])

    def test_non_match_world_skipped_harness_trips(self):
        violations = bad_hash_gate_violations(
            match_expected=False, expected_bad_hash_id=None,
            preview=self._ready_preview("import"),
            evidence_rows=[self._evidence(None)], harness_called=False,
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("must continue through the harness", violations[0])

    def test_accumulation_reports_every_tripped_clause(self):
        violations = bad_hash_gate_violations(
            match_expected=True, expected_bad_hash_id=1,
            preview=self._ready_preview("import"),
            evidence_rows=[self._evidence(None)], harness_called=True,
        )
        self.assertEqual(len(violations), 3)


_REJECT_FACTS = ("audio_corrupt", "bad_audio_hash", "nested_layout")


def preview_reject_precedence_violations(
    *, expected_fact: str | None, preview_decision: str,
) -> list[str]:
    """Accumulating checker: two independent clauses so a world violating
    both cannot mask either message.

    ``preview_import_from_path``'s confident-reject ``decision`` must equal
    the real evidence decider's ``candidate_preimport_reject_fact`` whenever
    the decider names one of the three facts this checker polices
    (``empty_fileset`` is out of scope — it was never routed through the
    early-check bug C1 fixes). Issue #1355 item 1's residual, "Batch C".
    """
    violations: list[str] = []
    if expected_fact in _REJECT_FACTS and preview_decision != expected_fact:
        violations.append(
            f"decider says {expected_fact!r}, preview_import_from_path said "
            f"{preview_decision!r}"
        )
    if expected_fact is None and preview_decision in _REJECT_FACTS:
        violations.append(
            "decider found no reject fact among audio_corrupt/bad_audio_hash/"
            f"nested_layout, but preview_import_from_path confident_reject'd "
            f"on {preview_decision!r}"
        )
    return violations


class TestPreviewClassifyAgreesWithEvidenceDeciderGenerated(unittest.TestCase):
    """C1 (#1355 item 1 residual): the read-only classify surface must agree
    with production's real decider on which reject fact wins.

    Drives the REAL ``preview_import_from_path`` with a mocked measurement
    (the same leaf-seam ``measure_preimport_state``/``inspect_local_files``
    mocks the deterministic pins in ``tests/test_import_preview.py`` use —
    measurement's own correctness is covered by ``tests/test_measurement.py``,
    not this property) and compares its ``decision`` against
    ``candidate_preimport_reject_fact`` fed the equivalent evidence row.
    """

    @given(
        audio_corrupt=st.booleans(),
        bad_audio_hash=st.booleans(),
        nested_layout=st.booleans(),
    )
    @example(audio_corrupt=True, bad_audio_hash=False, nested_layout=True)
    @example(audio_corrupt=True, bad_audio_hash=True, nested_layout=True)
    @example(audio_corrupt=False, bad_audio_hash=True, nested_layout=True)
    @example(audio_corrupt=False, bad_audio_hash=False, nested_layout=True)
    @example(audio_corrupt=False, bad_audio_hash=False, nested_layout=False)
    def test_classify_surface_agrees_with_the_evidence_decider(
        self, audio_corrupt: bool, bad_audio_hash: bool, nested_layout: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id="mbid-42", artist_name="Artist",
            album_title="Album",
        ))

        # try/finally, NOT addCleanup: inside a ``@given`` body addCleanup
        # defers removal past every example and leaks one tmpfs world per
        # example (issue #1214; enforced by test_given_body_cleanup_audit).
        source = tempfile.mkdtemp(dir=_PREVIEW_SOURCE_ROOT)
        try:
            if audio_corrupt:
                measurement = PreimportMeasurement(
                    audio_corrupt=True,
                    corrupt_files=["01.mp3"],
                    audio_validation=make_audio_corrupt_validation_report(
                        "01.mp3",
                    ),
                    matched_bad_hash_id=(7 if bad_audio_hash else None),
                    matched_bad_track_path=(
                        "01.mp3" if bad_audio_hash else None
                    ),
                    folder_layout=("nested" if nested_layout else "flat"),
                    audio_file_count=1,
                )
            else:
                measurement = PreimportMeasurement(
                    matched_bad_hash_id=(7 if bad_audio_hash else None),
                    matched_bad_track_path=(
                        "01.mp3" if bad_audio_hash else None
                    ),
                    folder_layout=("nested" if nested_layout else "flat"),
                    audio_file_count=1,
                )
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB(),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="mp3", min_bitrate_bps=128000, is_vbr=False,
                    has_nested_audio=nested_layout,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=measurement,
            ), patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db, request_id=42, path=source,
                )
        finally:
            shutil.rmtree(source, ignore_errors=True)

        evidence = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=128, is_cbr=False,
            audio_corrupt=audio_corrupt,
            folder_layout=("nested" if nested_layout else "flat"),
            matched_bad_audio_hash_id=(7 if bad_audio_hash else None),
            matched_bad_audio_hash_path=(
                "01.mp3" if bad_audio_hash else None
            ),
        )
        expected_fact = candidate_preimport_reject_fact(evidence)
        self.assertIsNotNone(preview.decision)
        assert preview.decision is not None  # narrows for the checker call
        violations = preview_reject_precedence_violations(
            expected_fact=expected_fact, preview_decision=preview.decision,
        )
        self.assertEqual(violations, [])
        if expected_fact in _REJECT_FACTS:
            mock_run.assert_not_called()


class TestPreviewRejectPrecedenceCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: every clause of the checker trips on the exact
    world that violates it, with its own message, and stays quiet on a
    legitimate agreement."""

    def test_disagreement_clause_trips(self):
        violations = preview_reject_precedence_violations(
            expected_fact="audio_corrupt", preview_decision="nested_layout",
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("decider says 'audio_corrupt'", violations[0])

    def test_false_positive_clause_trips(self):
        violations = preview_reject_precedence_violations(
            expected_fact=None, preview_decision="nested_layout",
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("decider found no reject fact", violations[0])

    def test_agreement_world_is_quiet(self):
        violations = preview_reject_precedence_violations(
            expected_fact="bad_audio_hash", preview_decision="bad_audio_hash",
        )
        self.assertEqual(violations, [])

    def test_no_fact_and_no_reject_is_quiet(self):
        violations = preview_reject_precedence_violations(
            expected_fact=None, preview_decision="would_import",
        )
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
