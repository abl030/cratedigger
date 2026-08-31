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

from hypothesis import example, given
from hypothesis import strategies as st

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
from lib.spectral_check import AlbumResult, TrackResult
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
                    cliff_count: int = 0) -> AlbumResult:
    """Build a real AlbumResult mimicking spectral_check.analyze_album's return."""
    tracks = [
        TrackResult(grade="suspect", cliff_detected=True)
        for _ in range(cliff_count)
    ]
    return AlbumResult(
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
        _needs_spectral_check(filetype, *, lossless_candidate) -> bool

    Issue #1145 removed the VBR skip. The gate reads the CODEC and nothing
    else: a lossless candidate runs, an MP3 runs, everything else skips.

    **Equivalence note for the deleted pins.** ``test_vbr_threshold_table``
    covered which averages skipped and which scanned; that whole axis is
    replaced by ``test_every_mp3_runs_whatever_its_declared_mode`` asserting
    that none skip. ``TestVbrScanThresholdIsInclusive`` (added by #1144 to
    pin the ``<=`` boundary) is deleted outright — the boundary it defended
    no longer exists, and the rounding behaviour it was protecting is pinned
    at its own source by
    ``tests/test_media_readiness.py::test_exactly_representable_rate_is_not_floored_one_low``.
    """

    def _run(self, filetype, lossless_candidate=None):
        from lib.measurement import _needs_spectral_check
        if lossless_candidate is None:
            lossless_candidate = any(
                codec in {"flac", "wav", "alac"}
                for codec in filetype.lower().replace(",", " ").split()
            )
        return _needs_spectral_check(
            filetype, lossless_candidate=lossless_candidate)

    def test_unambiguous_lossless_containers_always_run(self):
        """Affirmative verification requires preview-time source evidence."""
        for filetype in ("flac", "wav", "alac"):
            with self.subTest(filetype=filetype):
                self.assertTrue(self._run(filetype))

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
                    "m4a", lossless_candidate=lossless_candidate,
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

    def test_every_mp3_runs_whatever_its_declared_mode(self):
        """Issue #1145: measurement decides; no presumption.

        The retired skip trusted two facts that prove nothing about
        provenance — the declared VBR mode (the encoder's own Xing/Info
        header) and the album average (genuinely measured, but a transcode
        re-encoded high genuinely has a high one) — to decide that a file
        did not need measuring. With an ``mp3 vN`` contract mintable from a
        peer's own LAME tag, that let a forged ``-V 0`` skip the one
        measurement that would catch it and self-certify TRANSPARENT. The
        helper no longer takes either fact, so this asserts the whole
        surviving MP3 rule: it runs.
        """
        for filetype in ("mp3", "MP3", "mp3, mp3"):
            with self.subTest(filetype=filetype):
                self.assertTrue(self._run(filetype))

    def test_the_gate_takes_no_mode_or_bitrate_input(self):
        """The parameters are gone, not merely ignored.

        An ignored parameter is an invitation to reconnect it, and the point
        of #1145 is that this gate has no business reading either fact.
        """
        import inspect

        from lib.measurement import _needs_spectral_check

        parameters = inspect.signature(_needs_spectral_check).parameters
        self.assertEqual(set(parameters), {"filetype", "lossless_candidate"})

    def test_mixed_mp3_flac_runs_for_lossless_member(self):
        """A lossless member still requires an affirmative candidate scan."""
        self.assertTrue(self._run("flac, mp3"))

    def test_empty_filetype_skips(self):
        self.assertFalse(self._run(""))

    def test_non_mp3_lossy_codecs_still_never_run(self):
        """Must-still-work: removing the MP3 skip widened nothing else.

        AAC, Opus, Vorbis and WMA have no calibrated cliff policy, so the
        preimport gate does not fire for them and did not start to. A
        regression here would put every Opus download through an ~8s/track
        analysis whose result no decision may consume.
        """
        for filetype in ("aac", "opus", "vorbis", "wma", "m4a", "ogg"):
            with self.subTest(filetype=filetype):
                self.assertFalse(
                    self._run(filetype, lossless_candidate=False))


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

    def test_detected_flac_beats_misleading_ogg_suffix(self):
        """The preview quality boundary must use canonical media identity."""
        import os

        from lib.measurement import inspect_local_files
        from tests.audio_fixtures import make_test_flac

        with tempfile.TemporaryDirectory() as tmpdir:
            disguised = os.path.join(tmpdir, "01 - Track.ogg")
            source = os.path.join(tmpdir, "source.flac")
            make_test_flac(source, duration=1)
            os.replace(source, disguised)

            inspection = inspect_local_files(tmpdir)

            self.assertEqual(inspection.filetype, "flac")

    def test_detected_aac_beats_misleading_flac_suffix(self):
        """A lossless-looking filename cannot manufacture a lossless candidate."""
        import os
        import subprocess

        from lib.measurement import has_supported_lossless_audio, inspect_local_files

        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source.m4a")
            disguised = os.path.join(tmpdir, "01 - Track.flac")
            subprocess.run(
                [
                    "ffmpeg", "-v", "error", "-nostdin", "-f", "lavfi",
                    "-i", "sine=frequency=440:duration=0.2", "-c:a", "aac", source,
                ], check=True, timeout=30,
            )
            os.replace(source, disguised)

            inspection = inspect_local_files(tmpdir)

            self.assertEqual(inspection.filetype, "aac")
            self.assertFalse(
                has_supported_lossless_audio(inspection.filetype, [Path(disguised)]),
            )

    def test_inspect_reports_min_bitrate_and_mode(self):
        """What survives of the retired avg-bitrate pins (issue #1145).

        Equivalence note: ``test_inspect_reports_avg_bitrate`` and
        ``test_inspect_avg_bitrate_none_when_no_mp3`` existed to prove
        ``inspect_local_files`` produced the album mean the VBR scan
        threshold read. The threshold is gone and so is the field. The
        remaining facts the inspection owes — the minimum and the declared
        mode, both still consumed by ``measure_preimport_state`` — are
        asserted here over the same three-track world.
        """
        import os
        from unittest.mock import patch

        from lib.measurement import inspect_local_files

        tmpdir = tempfile.mkdtemp()
        try:
            paths = []
            for index in range(3):
                path = os.path.join(tmpdir, f"{index:02}.mp3")
                with open(path, "wb") as handle:
                    handle.write(b"fake mp3")
                paths.append(path)

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

            self.assertEqual(inspection.min_bitrate_bps, 240_000)
            self.assertTrue(inspection.is_vbr)
            self.assertEqual(inspection.filetype, "mp3")
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
                mock_track.return_value = TrackResult(
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
    measurement, the candidate download's spectral is NEVER exposed as the
    measurement's HAVE (``existing_spectral``) state.

    The May-12 world (dl 11380): a rejected fake-320 candidate measured
    ``likely_transcode``/128 while the genuine 192 copy's on-disk audit
    produced nothing (stale ``album_path`` / analyzer error). Pre-#815 the
    old ``_persist_spectral_state`` "reasonable proxy" branch adopted the
    candidate's grade; the evidence seeder froze it and it later drove a
    real library downgrade. That request-stamp write path is deleted
    outright — ``measure_preimport_state`` takes no request-writing DB at
    all — so this class pins the invariant at the surviving output
    boundary: ``existing_spectral`` is a real existing measurement or
    absent, never the candidate's. The persisted-evidence analog is owned
    by ``persist_exact_current_spectral_from_attempt``, which consumes
    exactly this output.
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
    ) -> PreimportMeasurement:
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
            precomputed_inspection=LocalFileInspection(
                filetype="mp3", min_bitrate_bps=320_000, is_vbr=False),
            spectral_detail_analyzer=analyze,
            existing_spectral_resolver=typed_resolver,
        )
        return measurement

    def _assert_no_adoption(self, measurement: PreimportMeasurement) -> None:
        # The candidate WAS measured — candidate state is unaffected.
        assert measurement.download_spectral is not None
        self.assertEqual(measurement.download_spectral.grade, "likely_transcode")
        self.assertEqual(measurement.download_spectral.bitrate_kbps, 128)
        # BAIL: the on-disk audit produced nothing → no HAVE measurement,
        # and the candidate's grade is never exposed in its place.
        self.assertIsNone(measurement.existing_spectral)
        # The container bitrate (192) remains the HAVE fallback for the decision.
        self.assertEqual(measurement.existing_min_bitrate, 192)

    def test_path_missing_shape_exposes_no_have_state(self):
        # Beets reports a 192 copy but its files are not on disk → no path.
        self._assert_no_adoption(self._measure(existing_outcome="none"))

    def test_analyzer_exception_shape_exposes_no_have_state(self):
        # The existing files resolve but the HAVE audit raises → no measurement.
        self._assert_no_adoption(self._measure(existing_outcome="raises"))

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
        worlds driving the real ``measure_preimport_state``, the exposed
        HAVE (``existing_spectral``) state, when present, always equals the
        REAL existing measurement — grade AND bitrate — and never the
        candidate's. When the on-disk audit yields nothing, no HAVE state
        is exposed at all."""
        measurement = self._measure(
            candidate_grade=candidate_grade,
            candidate_bitrate=candidate_bitrate,
            existing_outcome=existing_outcome,
            existing_grade=existing_grade,
            existing_bitrate=existing_bitrate,
        )
        # The analyzer's ground truth for the EXISTING path in this world.
        expected_existing = (
            SpectralMeasurement(
                grade=existing_grade, bitrate_kbps=existing_bitrate)
            if existing_outcome == "measured"
            else None
        )
        # The candidate WAS measured — candidate state is unaffected.
        assert measurement.download_spectral is not None
        self.assertEqual(measurement.download_spectral.grade, candidate_grade)
        self.assertEqual(
            measurement.download_spectral.bitrate_kbps, candidate_bitrate)
        # The exposed HAVE state is never the candidate's — it equals the
        # real existing measurement (grade + bitrate), or is absent.
        exposed = measurement.existing_spectral
        self.assertTrue(_have_state_is_never_candidate(
            persisted_current_grade=exposed.grade if exposed else None,
            persisted_current_bitrate=(
                exposed.bitrate_kbps if exposed else None),
            existing_spectral=expected_existing,
            download_spectral=measurement.download_spectral,
        ))
        # Explicit anti-adoption: no existing measurement → nothing exposed.
        if expected_existing is None:
            self.assertIsNone(exposed)

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
        from unittest.mock import MagicMock, patch

        from lib.media_readiness import repair_mp3_headers

        tmpdir = tempfile.mkdtemp()
        try:
            cd1 = os.path.join(tmpdir, "CD1")
            os.makedirs(cd1)
            nested = os.path.join(cd1, "01.mp3")
            with open(nested, "wb") as f:
                f.write(b"fake")
            with patch("lib.media_readiness.subprocess.run") as mock_run:
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
                mock_spectral.return_value = AlbumResult(
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
                )
            self.assertEqual(
                mock_spectral.call_count, 1,
                "resumed download with mp3 files on disk must still get "
                "spectral gating after inspection resolves is_vbr=False")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_low_avg_vbr_mp3_runs_spectral(self):
        """Issue #93's live album still gates, by the #1145 rule.

        The Go! Team - Are You Ready for More?: uploaded as VBR MP3 with
        126min / 182avg kbps. The original #93 gate skipped every VBR MP3
        and this transcode imported through; the threshold that fixed it
        scanned this album because 182 fell below 210. Both are history —
        it is scanned now for being an MP3 at all — and the album stays
        pinned because it is the real-world shape the whole lane exists
        for.
        """
        import os
        from unittest.mock import patch

        from lib.measurement import LocalFileInspection, measure_preimport_state

        cfg = CratediggerConfig(audio_check_mode="off")

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
                f.write(b"x")
            # Inspected: VBR MP3, avg 182kbps — the live issue #93 shape.
            inspected = LocalFileInspection(
                filetype="mp3",
                min_bitrate_bps=126_000,
                is_vbr=True,
            )
            with patch("lib.measurement.inspect_local_files",
                       return_value=inspected), \
                 patch("lib.measurement.spectral_analyze") as mock_spectral:
                mock_spectral.return_value = AlbumResult(
                    grade="likely_transcode",
                    estimated_bitrate_kbps=96,
                    suspect_pct=80.0,
                    tracks=[TrackResult(grade="suspect", cliff_detected=True)
                            for _ in range(5)])
                measurement = measure_preimport_state(
                    path=tmpdir,
                    mb_release_id="",   # no existing album
                    label="Go! Team - Are You Ready for More?",
                    download_filetype="mp3",
                    download_min_bitrate_bps=126_000,
                    download_is_vbr=True,
                    cfg=cfg,
                )
            self.assertEqual(
                mock_spectral.call_count, 1,
                "an MP3 must run spectral — this is the live issue #93 bug: "
                "skipping VBR MP3 let this transcode through")
            # Grade came back likely_transcode → should populate download_spectral
            self.assertIsNotNone(
                measurement.download_spectral,
                "download_spectral must be populated after gate runs")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_high_avg_vbr_mp3_is_scanned_too(self):
        """Issue #1145 at the real measurement seam, not the pure helper.

        Equivalence note: this replaces ``test_high_avg_vbr_mp3_skips_spectral``
        (the genuine-V0 skip) and ``test_the_scan_boundary_is_the_configured_gate_not_a_band``
        (which pinned that ``lib/measurement.py`` read the threshold, and
        was written for this PR's own F4 review finding). Neither has a
        subject any more: there is no threshold to read and no average that
        skips. What both were really protecting — that
        ``measure_preimport_state``'s own gate call decides correctly, and
        that its behaviour cannot silently follow a rank band — is asserted
        here across the averages that used to straddle the boundary.

        A mutant that reintroduces ANY average- or mode-based skip at this
        site fails on the 245 and 320 rows.
        """
        import os
        from unittest.mock import patch

        from lib.measurement import LocalFileInspection, measure_preimport_state

        cfg = CratediggerConfig(audio_check_mode="off")
        for avg_kbps in (96, 182, 210, 211, 245, 320):
            with self.subTest(avg_kbps=avg_kbps):
                tmpdir = tempfile.mkdtemp()
                try:
                    with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                        handle.write(b"x")
                    inspected = LocalFileInspection(
                        filetype="mp3",
                        min_bitrate_bps=avg_kbps * 1000,
                        is_vbr=True,
                    )
                    with patch("lib.measurement.inspect_local_files",
                               return_value=inspected), \
                         patch("lib.measurement.spectral_analyze") as spectral:
                        spectral.return_value = AlbumResult(
                            grade="genuine", estimated_bitrate_kbps=None,
                            suspect_pct=0.0, tracks=[])
                        measure_preimport_state(
                            path=tmpdir,
                            mb_release_id="",
                            label="Genuine V0 Album",
                            download_filetype="mp3",
                            download_min_bitrate_bps=avg_kbps * 1000,
                            download_is_vbr=True,
                            cfg=cfg,
                        )
                    self.assertEqual(
                        spectral.call_count, 1,
                        "every MP3 is scanned since #1145 — a peer-declared "
                        "mode and average buy no exemption")
                finally:
                    import shutil
                    shutil.rmtree(tmpdir, ignore_errors=True)

    def test_a_non_mp3_lossy_download_is_still_not_scanned(self):
        """Must-still-work at the same seam: nothing else widened.

        The paired control for the pin above. Removing the MP3 skip must not
        start scanning Opus, whose cliff carries no calibrated policy and
        whose scan no decision may consume.
        """
        import os
        from unittest.mock import patch

        from lib.measurement import LocalFileInspection, measure_preimport_state

        cfg = CratediggerConfig(audio_check_mode="off")
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.opus"), "wb") as handle:
                handle.write(b"x")
            inspected = LocalFileInspection(
                filetype="opus", min_bitrate_bps=128_000, is_vbr=True)
            with patch("lib.measurement.inspect_local_files",
                       return_value=inspected), \
                 patch("lib.measurement.spectral_analyze") as spectral:
                spectral.return_value = AlbumResult(
                    grade="genuine", estimated_bitrate_kbps=None,
                    suspect_pct=0.0, tracks=[])
                measure_preimport_state(
                    path=tmpdir,
                    mb_release_id="",
                    label="Opus Album",
                    download_filetype="opus",
                    download_min_bitrate_bps=128_000,
                    download_is_vbr=True,
                    cfg=cfg,
                )
            self.assertEqual(spectral.call_count, 0)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_mp3_with_no_readable_bitrate_still_gates(self):
        """An unreadable MP3 is still an MP3, so it is still scanned.

        Equivalence note: renamed from ``test_vbr_mp3_without_avg_still_gates``,
        which framed this as the conservative arm of the avg-based skip
        ("avg=None → gate anyway"). There is no avg input and no skip any
        more, so the surviving fact is simpler and strictly stronger — the
        codec alone decides, and mutagen failing to read a bitrate cannot
        change the codec.
        """
        import os
        from unittest.mock import patch

        from lib.measurement import LocalFileInspection, measure_preimport_state

        cfg = CratediggerConfig(audio_check_mode="off")

        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as f:
                f.write(b"x")
            inspected = LocalFileInspection(
                filetype="mp3",
                min_bitrate_bps=None,   # mutagen couldn't read
                is_vbr=True,
            )
            with patch("lib.measurement.inspect_local_files",
                       return_value=inspected), \
                 patch("lib.measurement.spectral_analyze") as mock_spectral:
                mock_spectral.return_value = AlbumResult(
                    grade="genuine", estimated_bitrate_kbps=None,
                    suspect_pct=0.0, tracks=[])
                measure_preimport_state(
                    path=tmpdir,
                    mb_release_id="",
                    label="Unreadable Bitrate",
                    download_filetype="mp3",
                    download_min_bitrate_bps=None,
                    download_is_vbr=True,
                    cfg=cfg,
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

        cfg = CratediggerConfig(audio_check_mode="off")

        with patch("lib.measurement.inspect_local_files",
                   return_value=LocalFileInspection(
                       filetype="mp3", is_vbr=None)), \
             patch("lib.measurement.spectral_analyze") as mock_spectral:
            mock_spectral.return_value = AlbumResult(
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
            )

        self.assertIsNone(
            measurement.existing_min_bitrate,
            "existing_min_bitrate must stay None when beets has no album")
        self.assertIsNone(
            measurement.existing_spectral,
            "existing_spectral must stay None when beets has no album")


if __name__ == "__main__":
    unittest.main()
