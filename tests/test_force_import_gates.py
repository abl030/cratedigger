"""Pre-import gate coverage at the ``lib.measurement`` boundary.

Historically this module's dispatch-via-legacy-branch tests asserted that
``dispatch_import_from_db`` ran the spectral/audio gates inline. After the
2026-05-15-002 importer-never-measures refactor (U4) the importer no
longer measures: preview owns candidate-evidence production, dispatch
trusts the evidence row, and the legacy direct-measurement branch in
``_dispatch_import_from_db_locked`` was deleted.

The behavioral contract those tests defended (force-import gets
spectral-gated) still holds — preview now enforces it. Coverage for the
preview/importer pipeline shape lives in ``tests/test_import_queue.py``
and ``tests/test_integration_slices.py``. After U8 the legacy
``run_preimport_gates`` shim has been deleted; the remaining tests here
cover the pure ``lib.measurement`` helpers (``inspect_local_files``,
``measure_preimport_state``, ``repair_mp3_headers``) that both preview
and auto-import still use.

U8 equivalence proof for deleted/migrated tests:
- ``TestPreimportGateDoesNotDecideQuality`` was deleted. The guarantee it
  protected — that preimport doesn't decide quality — is now structural:
  ``measure_preimport_state`` has no decision branches at all (returns a
  fact-only ``PreimportMeasurement``). Quality decisions live in
  ``full_pipeline_decision_from_evidence`` and are pinned by
  ``tests/test_quality_classification.py::TestLiveBugReproductions`` +
  ``TestLiveBugReproductionsThroughEvidencePipeline``.
- Audio-corrupt / bad-hash rejection behavior covered by:
  * ``tests/test_import_preview.py::test_audio_corrupt_is_confident_reject_without_denylist_side_effects``
  * ``tests/test_import_preview.py::test_bad_audio_hash_is_confident_reject_without_denylist_side_effects``
  * ``tests/test_integration_slices.py::TestBadAudioHashSlice``
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.beets_db import AlbumInfo
from lib.config import CratediggerConfig
from lib.measurement import (
    ExistingSpectralAuditLookup,
    ExistingSpectralResolver,
    LocalFileInspection,
    PreimportMeasurement,
    measure_preimport_state,
)
from lib.quality import SpectralAnalysisDetail, SpectralMeasurement
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_integration_slices import _mock_beets_db


def _have_state_is_never_candidate(
    *,
    persisted_current_grade: str | None,
    persisted_current_bitrate: int | None,
    existing_spectral: SpectralMeasurement | None,
    download_spectral: SpectralMeasurement | None,
) -> bool:
    """Invariant A checker: on-disk state is never adopted from the candidate.

    Holds iff: when there is no real existing measurement, NO on-disk state is
    written at all (grade AND bitrate stay None); when there is one, the on-disk
    state equals that REAL existing measurement — grade AND bitrate. The
    candidate's ``download_spectral`` is never adopted as HAVE state (#815).
    """
    del download_spectral  # named to make the anti-adoption contrast explicit
    if existing_spectral is None:
        return (
            persisted_current_grade is None
            and persisted_current_bitrate is None
        )
    return (
        persisted_current_grade == existing_spectral.grade
        and persisted_current_bitrate == existing_spectral.bitrate_kbps
    )


def _analyze_result(grade: str, bitrate: int | None, suspect_pct: float = 0.0,
                    cliff_count: int = 0):
    """Build a SimpleNamespace mimicking spectral_check.analyze_album's return."""
    tracks = [SimpleNamespace(cliff_detected=True) for _ in range(cliff_count)]
    return SimpleNamespace(
        grade=grade,
        estimated_bitrate_kbps=bitrate,
        suspect_pct=suspect_pct,
        tracks=tracks,
    )


class TestNeedsSpectralCheckDecisions(unittest.TestCase):
    """Pure-function coverage for ``_needs_spectral_check``.

    The equivalent tests used to live on the deleted
    ``TestGatherSpectralContextFunction`` (lossless runs, VBR skips, CBR runs).
    Keeping them as pure input/output assertions here so the auto path's
    branch-selection logic stays covered without re-introducing the old
    SpectralContext plumbing.

    Signature (see lib/measurement.py::_needs_spectral_check):
        _needs_spectral_check(filetype, is_vbr, avg_bitrate_kbps,
                              vbr_threshold_kbps, lossless_candidate) -> bool

    The VBR branch is gated on ``avg_bitrate_kbps < vbr_threshold_kbps``
    so transcodes uploaded as fake V0 (avg ~180kbps) are still analyzed.
    Genuine V0 (avg ~245kbps+) falls through unchanged.
    """

    # Threshold matches cfg.quality_ranks.mp3_vbr.excellent default (210).
    THRESHOLD = 210

    def _run(self, filetype, is_vbr, avg_kbps=None, threshold=None):
        from lib.measurement import _needs_spectral_check
        return _needs_spectral_check(
            filetype, is_vbr,
            lossless_candidate=any(
                codec in {"flac", "wav", "alac"}
                for codec in filetype.lower().replace(",", " ").split()
            ),
            avg_bitrate_kbps=avg_kbps,
            vbr_threshold_kbps=threshold if threshold is not None else self.THRESHOLD,
        )

    def test_unambiguous_lossless_containers_always_run(self):
        """Affirmative verification requires preview-time source evidence."""
        for filetype in ("flac", "wav", "alac"):
            with self.subTest(filetype=filetype):
                self.assertTrue(self._run(filetype, False))
                self.assertTrue(self._run(filetype, None))
                self.assertTrue(self._run(filetype, True))
                self.assertTrue(self._run(filetype, True, avg_kbps=150))

    def test_aac_in_m4a_is_not_a_lossless_candidate(self):
        """The M4A container must not promote its lossy AAC codec."""
        from lib.measurement import has_supported_lossless_audio

        probes: list[str] = []

        def probe(path: str) -> str:
            probes.append(path)
            return "aac"

        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "01.m4a").write_bytes(b"aac")
            self.assertFalse(
                has_supported_lossless_audio(
                    "m4a",
                    [Path(folder) / "01.m4a"],
                    codec_probe=probe,
                )
            )
            self.assertEqual(probes, [str(Path(folder) / "01.m4a")])

    def test_alac_in_m4a_requires_preview_spectral_analysis(self):
        """An ALAC stream in M4A still enters affirmative verification."""
        from lib.measurement import (
            _needs_spectral_check,
            has_supported_lossless_audio,
        )

        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "01.m4a").write_bytes(b"alac")
            lossless_candidate = has_supported_lossless_audio(
                "m4a",
                [Path(folder) / "01.m4a"],
                codec_probe=lambda _path: "alac",
            )
            self.assertTrue(
                _needs_spectral_check(
                    "m4a",
                    False,
                    lossless_candidate=lossless_candidate,
                    avg_bitrate_kbps=None,
                    vbr_threshold_kbps=self.THRESHOLD,
                )
            )

    def test_m4a_probe_failure_is_distinct_from_positive_aac_and_alac(self):
        from lib.measurement import AudioCodecProbeError, has_supported_lossless_audio

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "01.m4a"
            path.write_bytes(b"audio")
            self.assertFalse(
                has_supported_lossless_audio(
                    "m4a", [path], codec_probe=lambda _path: "aac",
                )
            )
            self.assertTrue(
                has_supported_lossless_audio(
                    "m4a", [path], codec_probe=lambda _path: "alac",
                )
            )
            with self.assertRaises(AudioCodecProbeError):
                has_supported_lossless_audio(
                    "m4a", [path], codec_probe=lambda _path: None,
                )

    def test_cbr_mp3_always_runs(self):
        """CBR MP3 always runs spectral — avg bitrate irrelevant."""
        self.assertTrue(self._run("mp3", False))
        self.assertTrue(self._run("mp3", False, avg_kbps=320))
        self.assertTrue(self._run("mp3", False, avg_kbps=128))

    def test_unknown_vbr_mp3_always_runs(self):
        """is_vbr=None → run (conservative). measure_preimport_state
        reinspects first, so None here means truly unresolvable."""
        self.assertTrue(self._run("mp3", None))
        self.assertTrue(self._run("mp3", None, avg_kbps=245))

    def test_mixed_mp3_flac_runs_for_lossless_member(self):
        """A lossless member still requires an affirmative candidate scan."""
        self.assertTrue(self._run("flac, mp3", False))

    def test_empty_filetype_skips(self):
        self.assertFalse(self._run("", False))

    def test_vbr_threshold_table(self):
        """VBR branch: gate only when avg is unknown or < threshold."""
        CASES = [
            # (desc, avg_kbps, expected)
            ("avg unknown → gate (conservative)",          None, True),
            ("go_team case — avg 182 < 210 → gate",         182, True),
            ("live issue #93 avg 182kbps transcode",        182, True),
            ("just below threshold — 200 → gate",           200, True),
            ("at threshold — 210 is NOT below → skip",      210, False),
            ("genuine V0 avg ~245 → skip",                  245, False),
            ("genuine V0 avg ~260 → skip",                  260, False),
            ("very low 96kbps → gate",                       96, True),
        ]
        for desc, avg, expected in CASES:
            with self.subTest(desc=desc, avg=avg):
                got = self._run("mp3", True, avg_kbps=avg)
                self.assertEqual(
                    got, expected,
                    f"VBR avg={avg} expected {expected}, got {got}")


class TestInspectLocalFilesRecursive(unittest.TestCase):
    """inspect_local_files() must walk subdirectories so multi-disc layouts
    (``Album/CD1/*.mp3``) classify correctly — otherwise the spectral gate
    silently skips nested force-imports.
    """

    def test_multi_disc_layout_detects_mp3(self):
        """Audio files under a subdirectory must be discovered."""
        import os
        from lib.measurement import inspect_local_files

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            with open(os.path.join(cd1, "01 - track.mp3"), "wb") as f:
                f.write(b"fake")
            inspection = inspect_local_files(tmpdir)
            self.assertIn("mp3", inspection.filetype,
                          "subdirectory MP3 must be discovered")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_inspect_reports_avg_bitrate(self):
        """inspect_local_files must also return avg_bitrate_bps across all
        MP3 files so measure_preimport_state can decide whether to gate a
        VBR upload against cfg.quality_ranks.mp3_vbr.excellent.

        A VBR MP3 transcode at avg 182kbps (issue #93, The Go! Team) must be
        distinguishable from a genuine V0 at avg ~245kbps. Container min
        alone is not enough — lo-fi V0 can have low-bitrate silent tracks
        that look identical to a transcode's min.
        """
        import os
        from unittest.mock import patch
        from lib.measurement import inspect_local_files

        tmpdir = tempfile.mkdtemp()
        try:
            paths = []
            for i in range(3):
                p = os.path.join(tmpdir, f"{i:02}.mp3")
                with open(p, "wb") as f:
                    f.write(b"fake mp3")
                paths.append(p)

            # Simulate three tracks: two at ~240kbps, one at ~260kbps → avg 247.
            def fake_mp3_open(path):
                mapping = {
                    paths[0]: 240_000,
                    paths[1]: 240_000,
                    paths[2]: 260_000,
                }
                return SimpleNamespace(info=SimpleNamespace(
                    bitrate=mapping[path], bitrate_mode=2))  # VBR

            with patch("mutagen.mp3.MP3", side_effect=fake_mp3_open):
                inspection = inspect_local_files(tmpdir)

            self.assertIsNotNone(inspection.avg_bitrate_bps,
                                 "avg_bitrate_bps must be populated for MP3")
            assert inspection.avg_bitrate_bps is not None
            self.assertEqual(inspection.avg_bitrate_bps, (240_000 + 240_000 + 260_000) // 3)
            self.assertEqual(inspection.min_bitrate_bps, 240_000)
            self.assertTrue(inspection.is_vbr)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_inspect_avg_bitrate_none_when_no_mp3(self):
        """Non-MP3 downloads leave avg_bitrate_bps=None (no mutagen walk)."""
        import os
        from lib.measurement import inspect_local_files

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.flac"), "wb") as f:
                f.write(b"fake flac")
            inspection = inspect_local_files(tmpdir)
            self.assertIsNone(inspection.avg_bitrate_bps,
                              "avg_bitrate_bps stays None without any MP3 to read")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_validate_audio_recurses_into_subdirs(self):
        """validate_audio must walk subdirectories so nested discs are decoded.

        Auto path always passes flat folders, but force-import can point
        at user folders with ``Album/CD1/*.mp3``. If validate_audio only lists
        the root, no nested file is decoded and corrupt audio silently passes.
        """
        import os
        from lib.util import validate_audio

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            with open(os.path.join(cd1, "01.mp3"), "wb") as f:
                f.write(b"bad mp3 bytes")
            result = validate_audio(tmpdir, "normal")
            self.assertFalse(
                result.valid,
                "nested corrupt MP3 must trigger audio rejection")
            self.assertTrue(
                any("01.mp3" in name for name, _ in result.failed_files),
                f"failed_files must include the nested file, got {result.failed_files}")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_analyze_album_recurses_into_subdirs(self):
        """analyze_album must walk subdirectories so nested discs are analyzed.

        Without recursion, a multi-disc folder returns an empty result that
        looks like 'genuine' (no tracks = no cliffs), and the spectral gate
        silently passes a potential transcode on force-import.
        """
        import os
        from unittest.mock import patch
        from lib.spectral_check import analyze_album

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            with open(os.path.join(cd1, "01.mp3"), "wb") as f:
                f.write(b"fake")
            with patch("lib.spectral_check.analyze_track") as mock_track:
                mock_track.return_value = SimpleNamespace(
                    grade="suspect", error=None,
                    estimated_bitrate_kbps=128,
                    cliff_detected=True, cliff_freq_hz=12000,
                )
                _ = analyze_album(tmpdir)
            self.assertEqual(
                mock_track.call_count, 1,
                "analyze_album must reach the nested file (call_count=0 means "
                "it only listed the root)")
            called_path = mock_track.call_args[0][0]
            self.assertIn("CD1", called_path,
                          "analyze_album must call analyze_track with the nested path")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestNoCandidateSpectralAdoptedAsHave(unittest.TestCase):
    """Issue #815 Invariant A (bail): when the on-disk HAVE audit yields no
    measurement, the candidate download's spectral is NEVER adopted as the
    request's on-disk (``current_spectral_*``) state.

    The May-12 world (dl 11380): a rejected fake-320 candidate measured
    ``likely_transcode``/128 while the genuine 192 copy's on-disk audit
    produced nothing (stale ``album_path`` / analyzer error). Pre-#815 the
    ``_persist_spectral_state`` "reasonable proxy" branch adopted the
    candidate's grade; the evidence seeder froze it and it later drove a real
    library downgrade. The prior ``TestAutoPathPreservesSpectralPropagation``
    asserted the OPPOSITE (``current_spectral_grade == "suspect"``); that
    adoption behaviour is deliberately removed, so this pin flips it: nothing
    is written and the container bitrate remains the HAVE fallback.
    """

    CANDIDATE = "/tmp/candidate-815"
    EXISTING = "/tmp/existing-815"

    def _measure(
        self,
        *,
        candidate_grade: str = "likely_transcode",
        candidate_bitrate: int | None = 128,
        existing_outcome: str,  # "measured" | "none" | "raises"
        existing_grade: str = "genuine",
        existing_bitrate: int | None = 160,
    ) -> tuple[FakePipelineDB, PreimportMeasurement]:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, min_bitrate=192,
            current_spectral_grade=None, current_spectral_bitrate=None,
        ))
        cfg = CratediggerConfig(audio_check_mode="off")

        def analyze(path: str) -> SpectralAnalysisDetail:
            if path == self.EXISTING:
                if existing_outcome == "raises":
                    raise RuntimeError("stale album_path: sox found nothing")
                return SpectralAnalysisDetail(
                    attempted=True, grade=existing_grade,
                    bitrate_kbps=existing_bitrate)
            return SpectralAnalysisDetail(
                attempted=True, grade=candidate_grade,
                bitrate_kbps=candidate_bitrate)

        def resolver(_mbid: str) -> ExistingSpectralAuditLookup:
            if existing_outcome == "none":
                return ExistingSpectralAuditLookup(
                    path=None, min_bitrate_kbps=192)
            return ExistingSpectralAuditLookup(
                path=self.EXISTING, min_bitrate_kbps=192)

        typed_resolver: ExistingSpectralResolver = resolver
        measurement = measure_preimport_state(
            path=self.CANDIDATE,
            mb_release_id="mbid-123",
            label="Mark DeNardo - Fake 320",
            download_filetype="mp3",
            download_min_bitrate_bps=320_000,
            download_is_vbr=False,
            cfg=cfg,
            db=db,  # type: ignore[arg-type]
            request_id=42,
            precomputed_inspection=LocalFileInspection(
                filetype="mp3", min_bitrate_bps=320_000, is_vbr=False),
            spectral_detail_analyzer=analyze,
            existing_spectral_resolver=typed_resolver,
        )
        return db, measurement

    def _assert_no_adoption(
        self, db: FakePipelineDB, measurement: PreimportMeasurement,
    ) -> None:
        # The candidate WAS measured — candidate state is unaffected.
        assert measurement.download_spectral is not None
        self.assertEqual(measurement.download_spectral.grade, "likely_transcode")
        self.assertEqual(measurement.download_spectral.bitrate_kbps, 128)
        # The on-disk audit produced nothing → no HAVE measurement.
        self.assertIsNone(measurement.existing_spectral)
        # BAIL: the candidate's grade is NEVER written as on-disk state.
        row = db.request(42)
        self.assertIsNone(row.get("current_spectral_grade"))
        self.assertIsNone(row.get("current_spectral_bitrate"))
        # The container bitrate (192) remains the HAVE fallback for the decision.
        self.assertEqual(measurement.existing_min_bitrate, 192)

    def test_path_missing_shape_writes_no_have_state(self):
        # Beets reports a 192 copy but its files are not on disk → no path.
        db, measurement = self._measure(existing_outcome="none")
        self._assert_no_adoption(db, measurement)

    def test_analyzer_exception_shape_writes_no_have_state(self):
        # The existing files resolve but the HAVE audit raises → no measurement.
        db, measurement = self._measure(existing_outcome="raises")
        self._assert_no_adoption(db, measurement)

    @given(
        candidate_grade=st.sampled_from((
            "genuine", "marginal", "suspect", "likely_transcode",
        )),
        candidate_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=500)),
        existing_outcome=st.sampled_from(("measured", "none", "raises")),
        existing_grade=st.sampled_from(
            ("genuine", "suspect", "likely_transcode")),
        existing_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=500)),
    )
    @example(
        candidate_grade="likely_transcode",
        candidate_bitrate=128,
        existing_outcome="none",
        existing_grade="genuine",
        existing_bitrate=None,
    )
    @example(
        candidate_grade="likely_transcode",
        candidate_bitrate=128,
        existing_outcome="raises",
        existing_grade="genuine",
        existing_bitrate=160,
    )
    def test_candidate_spectral_never_becomes_have_state(
        self,
        candidate_grade: str,
        candidate_bitrate: int | None,
        existing_outcome: str,
        existing_grade: str,
        existing_bitrate: int | None,
    ) -> None:
        """Issue #815 Invariant A (bail) property. Across generated candidate
        grade × bitrate × existing-audit outcome (measured / none / raises)
        worlds driving the real ``measure_preimport_state``, the request's
        on-disk (HAVE) spectral state, when present, always equals a REAL
        existing measurement — grade AND bitrate — and never the candidate's.
        When the on-disk audit yields nothing, no HAVE state is written."""
        db, measurement = self._measure(
            candidate_grade=candidate_grade,
            candidate_bitrate=candidate_bitrate,
            existing_outcome=existing_outcome,
            existing_grade=existing_grade,
            existing_bitrate=existing_bitrate,
        )
        row = db.request(42)
        raw_grade = row.get("current_spectral_grade")
        persisted_grade = raw_grade if isinstance(raw_grade, str) else None
        raw_bitrate = row.get("current_spectral_bitrate")
        persisted_bitrate = raw_bitrate if isinstance(raw_bitrate, int) else None
        # The candidate WAS measured — candidate state is unaffected.
        assert measurement.download_spectral is not None
        self.assertEqual(measurement.download_spectral.grade, candidate_grade)
        self.assertEqual(
            measurement.download_spectral.bitrate_kbps, candidate_bitrate)
        # The on-disk state is never the candidate's — it equals the real
        # existing measurement (grade + bitrate), or is absent entirely.
        self.assertTrue(_have_state_is_never_candidate(
            persisted_current_grade=persisted_grade,
            persisted_current_bitrate=persisted_bitrate,
            existing_spectral=measurement.existing_spectral,
            download_spectral=measurement.download_spectral,
        ))
        # Explicit anti-adoption: no existing measurement → nothing written.
        if measurement.existing_spectral is None:
            self.assertIsNone(persisted_grade)
            self.assertIsNone(persisted_bitrate)

    def test_have_state_checker_trips_on_adopted_candidate(self):
        # Known-bad self-test: a request whose on-disk grade was adopted from
        # the candidate (no existing measurement) must trip the checker.
        self.assertFalse(_have_state_is_never_candidate(
            persisted_current_bitrate=128,
            persisted_current_grade="likely_transcode",
            existing_spectral=None,
            download_spectral=SpectralMeasurement(
                grade="likely_transcode", bitrate_kbps=128),
        ))
        # ...and it holds on the correct bail case.
        self.assertTrue(_have_state_is_never_candidate(
            persisted_current_grade=None,
            persisted_current_bitrate=None,
            existing_spectral=None,
            download_spectral=SpectralMeasurement(
                grade="likely_transcode", bitrate_kbps=128),
        ))
        # It also trips when only the bitrate leaked from the candidate.
        self.assertFalse(_have_state_is_never_candidate(
            persisted_current_grade=None,
            persisted_current_bitrate=128,
            existing_spectral=None,
            download_spectral=SpectralMeasurement(
                grade="likely_transcode", bitrate_kbps=128),
        ))


class TestRepairMp3HeadersRecurses(unittest.TestCase):
    """repair_mp3_headers must walk subdirectories — otherwise nested MP3s
    with fixable header issues reach ffmpeg unrepaired and falsely reject.
    """

    def test_mp3val_called_on_nested_file(self):
        import os
        from unittest.mock import patch, MagicMock
        from lib.util import repair_mp3_headers

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            nested = os.path.join(cd1, "01.mp3")
            with open(nested, "wb") as f:
                f.write(b"fake")
            with patch("lib.util.sp.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="")
                repair_mp3_headers(tmpdir)
            called_paths = [c[0][0][-1] for c in mock_run.call_args_list]
            self.assertTrue(
                any(nested == p for p in called_paths),
                f"mp3val must be called on nested {nested}, got {called_paths}")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestAudioFailuresPreserveSubdirContext(unittest.TestCase):
    """When validate_audio walks subdirectories, the failed-file list must
    record the path relative to the audit root so multi-disc layouts don't
    collapse ``CD1/01.mp3`` and ``CD2/01.mp3`` into the same entry.
    """

    def test_nested_failures_keep_subdir_in_name(self):
        import os
        from unittest.mock import patch
        from lib.util import validate_audio

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            cd2 = os.path.join(tmpdir, "CD2")
            os.makedirs(cd1)
            os.makedirs(cd2)
            with open(os.path.join(cd1, "01.mp3"), "wb") as f:
                f.write(b"x")
            with open(os.path.join(cd2, "01.mp3"), "wb") as f:
                f.write(b"x")
            # Both files fail
            with patch("lib.util.sp.run") as mock_run:
                from unittest.mock import MagicMock
                mock_run.return_value = MagicMock(
                    returncode=1, stderr="Invalid data")
                result = validate_audio(tmpdir, "normal")
            names = [name for name, _err in result.failed_files]
            self.assertIn("CD1/01.mp3", names,
                          f"CD1 path must survive in failed_files, got {names}")
            self.assertIn("CD2/01.mp3", names,
                          f"CD2 path must survive in failed_files, got {names}")


        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestPreimportDoesNotReadRequestSpectral(unittest.TestCase):
    """Missing HAVE analysis cannot be replaced by request-row stamps."""

    def test_stored_spectral_ignored_when_beets_lookup_empty(self):
        db = FakePipelineDB()
        # Request row has stored spectral: on-disk is actually a 128 transcode,
        # even though beets reports 320 as the container min_bitrate.
        db.seed_request(make_request_row(
            id=42,
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=128,
        ))
        # Beets knows the album exists at 320 but its album_path is not on
        # disk, so _analyze_existing returns (320, None) — no measured spectral.
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3", is_cbr=True,
            album_path="/Beets/NonexistentPath")
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch("lib.measurement.spectral_analyze",
                   return_value=_analyze_result(
                       "likely_transcode", 192, 80.0, 5)), \
             patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            measurement = measure_preimport_state(
                path="/tmp/dl",
                mb_release_id="mbid-123",
                label="Test",
                download_filetype="mp3",
                download_min_bitrate_bps=192_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
            )

        self.assertIsNone(measurement.existing_spectral)
        # Measurement never decides — the importer's full pipeline owns
        # the spectral comparison.
        self.assertFalse(measurement.audio_corrupt)


class TestUnknownVbrResolvesViaInspection(unittest.TestCase):
    """When the caller passes ``is_vbr=None`` (auto-path resumed download
    or force-path mutagen failure), the gate must attempt to resolve VBR
    via filesystem inspection before deciding whether to run spectral.
    Skipping spectral unconditionally on None was a bypass for resumed CBR
    MP3 downloads rebuilt from ``ActiveDownloadState`` — the auto path's
    protection must not depend on slskd metadata being preserved.
    """

    def test_auto_path_resumed_download_reinspects_to_keep_spectral(self):
        """is_vbr=None → filesystem inspection fills it in → spectral runs."""
        import os
        from unittest.mock import patch
        from lib.measurement import LocalFileInspection, measure_preimport_state

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        cfg = CratediggerConfig(audio_check_mode="off")

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
                f.write(b"x")
            inspected = LocalFileInspection(
                filetype="mp3", min_bitrate_bps=320_000, is_vbr=False)
            with patch("lib.measurement.inspect_local_files",
                       return_value=inspected), \
                 patch("lib.measurement.spectral_analyze") as mock_spectral:
                mock_spectral.return_value = SimpleNamespace(
                    grade="genuine", estimated_bitrate_kbps=None,
                    suspect_pct=0.0, tracks=[])
                measure_preimport_state(
                    path=tmpdir,
                    mb_release_id="",
                    label="Test",
                    download_filetype="mp3",
                    download_min_bitrate_bps=None,
                    download_is_vbr=None,   # simulates resumed download
                    cfg=cfg,
                    db=db,  # type: ignore[arg-type]
                    request_id=1,
                )
            self.assertEqual(
                mock_spectral.call_count, 1,
                "resumed download with mp3 files on disk must still get "
                "spectral gating after inspection resolves is_vbr=False")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_low_avg_vbr_mp3_runs_spectral(self):
        """Issue #93: VBR MP3 at avg 182kbps (below 210 threshold) MUST gate.

        The Go! Team - Are You Ready for More?: uploaded as VBR MP3 with
        126min / 182avg kbps. Current gate skips all VBR MP3 → transcode
        imports through. Post-fix: the gate runs spectral because avg
        (182) < cfg.quality_ranks.mp3_vbr.excellent (210) → transcode
        correctly caught.
        """
        import os
        from unittest.mock import patch
        from lib.measurement import LocalFileInspection, measure_preimport_state

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        cfg = CratediggerConfig(audio_check_mode="off")

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
                f.write(b"x")
            # Inspected: VBR MP3, avg 182kbps — the live issue #93 shape.
            inspected = LocalFileInspection(
                filetype="mp3",
                min_bitrate_bps=126_000,
                avg_bitrate_bps=182_000,
                is_vbr=True,
            )
            with patch("lib.measurement.inspect_local_files",
                       return_value=inspected), \
                 patch("lib.measurement.spectral_analyze") as mock_spectral:
                mock_spectral.return_value = SimpleNamespace(
                    grade="likely_transcode",
                    estimated_bitrate_kbps=96,
                    suspect_pct=80.0,
                    tracks=[SimpleNamespace(cliff_detected=True)
                            for _ in range(5)])
                measurement = measure_preimport_state(
                    path=tmpdir,
                    mb_release_id="",   # no existing album
                    label="Go! Team - Are You Ready for More?",
                    download_filetype="mp3",
                    download_min_bitrate_bps=126_000,
                    download_is_vbr=True,
                    cfg=cfg,
                    db=db,  # type: ignore[arg-type]
                    request_id=1,
                )
            self.assertEqual(
                mock_spectral.call_count, 1,
                "VBR MP3 at avg 182kbps (< 210kbps threshold) must run "
                "spectral — this is the live issue #93 bug: skipping all "
                "VBR MP3 would let transcodes through")
            # Grade came back likely_transcode → should populate download_spectral
            self.assertIsNotNone(
                measurement.download_spectral,
                "download_spectral must be populated after gate runs")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_high_avg_vbr_mp3_skips_spectral(self):
        """Genuine V0 at avg 245kbps (>= 210 threshold) must keep skipping.

        Guard: the threshold fix must not over-gate. Genuine V0 uploads
        have high avg bitrates; trusting the VBR metadata here preserves
        current behavior and avoids unnecessary ~8s-per-track spectral
        analysis on every genuine VBR download.
        """
        import os
        from unittest.mock import patch
        from lib.measurement import LocalFileInspection, measure_preimport_state

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        cfg = CratediggerConfig(audio_check_mode="off")

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
                f.write(b"x")
            inspected = LocalFileInspection(
                filetype="mp3",
                min_bitrate_bps=220_000,
                avg_bitrate_bps=245_000,   # genuine V0 range
                is_vbr=True,
            )
            with patch("lib.measurement.inspect_local_files",
                       return_value=inspected), \
                 patch("lib.measurement.spectral_analyze") as mock_spectral:
                measure_preimport_state(
                    path=tmpdir,
                    mb_release_id="",
                    label="Genuine V0 Album",
                    download_filetype="mp3",
                    download_min_bitrate_bps=220_000,
                    download_is_vbr=True,
                    cfg=cfg,
                    db=db,  # type: ignore[arg-type]
                    request_id=1,
                )
            self.assertEqual(
                mock_spectral.call_count, 0,
                "genuine V0 (avg 245kbps >= 210kbps) must skip spectral "
                "to avoid wasted analysis on good files")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_vbr_mp3_without_avg_still_gates(self):
        """VBR MP3 with avg=None → still gate (conservative).

        When mutagen can't compute avg (corrupt files, empty folder), the
        gate must fall through to running spectral rather than skipping.
        Matches the ``is_vbr=None`` handling — err on the side of analyzing.
        """
        import os
        from unittest.mock import patch
        from lib.measurement import LocalFileInspection, measure_preimport_state

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        cfg = CratediggerConfig(audio_check_mode="off")

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
                f.write(b"x")
            inspected = LocalFileInspection(
                filetype="mp3",
                min_bitrate_bps=None,
                avg_bitrate_bps=None,   # mutagen couldn't read
                is_vbr=True,
            )
            with patch("lib.measurement.inspect_local_files",
                       return_value=inspected), \
                 patch("lib.measurement.spectral_analyze") as mock_spectral:
                mock_spectral.return_value = SimpleNamespace(
                    grade="genuine", estimated_bitrate_kbps=None,
                    suspect_pct=0.0, tracks=[])
                measure_preimport_state(
                    path=tmpdir,
                    mb_release_id="",
                    label="Unknown Avg",
                    download_filetype="mp3",
                    download_min_bitrate_bps=None,
                    download_is_vbr=True,
                    cfg=cfg,
                    db=db,  # type: ignore[arg-type]
                    request_id=1,
                )
            self.assertEqual(
                mock_spectral.call_count, 1,
                "VBR MP3 with unknown avg must still gate — conservative "
                "default; genuine VBR uploads produce 'genuine' spectral "
                "grades and fall through")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_unresolvable_vbr_still_gates(self):
        """is_vbr=None AND inspection also returns None → still gate.

        The conservative default: genuine VBR uploads produce 'genuine'
        spectral grades and fall through to import; forcing a genuine-VBR
        upload through the gate is cheap and safe.
        """
        from unittest.mock import patch
        from lib.measurement import LocalFileInspection, measure_preimport_state

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch("lib.measurement.inspect_local_files",
                   return_value=LocalFileInspection(
                       filetype="mp3", is_vbr=None)), \
             patch("lib.measurement.spectral_analyze") as mock_spectral:
            mock_spectral.return_value = SimpleNamespace(
                grade="genuine", estimated_bitrate_kbps=None,
                suspect_pct=0.0, tracks=[])
            measure_preimport_state(
                path="/tmp/dl",
                mb_release_id="",
                label="Test",
                download_filetype="mp3",
                download_min_bitrate_bps=None,
                download_is_vbr=None,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=1,
            )
        self.assertEqual(
            mock_spectral.call_count, 1,
            "still gate when inspection can't resolve VBR; genuine grade "
            "falls through to import")


class TestFallbackSkippedWhenBeetsFindsNoAlbum(unittest.TestCase):
    """When BeetsDB returns no album at all (deleted, not yet imported, or
    lookup failed), measure_preimport_state must NOT fabricate 'existing'
    state from stale album_requests.min_bitrate — doing so would let the
    importer reject a valid redownload against state that doesn't exist on
    disk.
    """

    def test_no_beets_album_means_no_fallback(self):
        from lib.measurement import measure_preimport_state

        db = FakePipelineDB()
        # Request row has leftover state from a prior import that no longer
        # exists in beets (user deleted it, beets DB corrupt, etc.).
        db.seed_request(make_request_row(
            id=42,
            min_bitrate=192,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=128,
        ))
        cfg = CratediggerConfig(audio_check_mode="off")

        # BeetsDB returns None → album not in beets.
        def _mock_beets_db_no_album():
            mock_beets = MagicMock()
            mock_beets.get_album_info.return_value = None
            mock_cls = MagicMock()
            mock_cls.return_value.__enter__ = MagicMock(return_value=mock_beets)
            mock_cls.return_value.__exit__ = MagicMock(return_value=False)
            return mock_cls

        # Download: suspect 192kbps. If the fallback (incorrectly) fired,
        # it would set existing_min from stale min_bitrate=192 and
        # existing_spectral from stale 128. With the fallback correctly
        # skipped (beets has no album → nothing on disk), the measurement
        # leaves existing_* unset for the importer to read as "no existing".
        with patch("lib.measurement.spectral_analyze",
                   return_value=_analyze_result(
                       "likely_transcode", 192, 80.0, 5)), \
             patch("lib.beets_db.BeetsDB", _mock_beets_db_no_album()):
            measurement = measure_preimport_state(
                path="/tmp/dl",
                mb_release_id="mbid-123",
                label="Test",
                download_filetype="mp3",
                download_min_bitrate_bps=192_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
            )

        self.assertIsNone(
            measurement.existing_min_bitrate,
            "existing_min_bitrate must stay None when beets has no album")
        self.assertIsNone(
            measurement.existing_spectral,
            "existing_spectral must stay None when beets has no album")


if __name__ == "__main__":
    unittest.main()
