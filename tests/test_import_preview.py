"""Tests for unified import preview service."""

import configparser
import os
import shutil
import tempfile
import unittest
from contextlib import AbstractContextManager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

from lib.config import CratediggerConfig
from lib.current_library_evidence import (
    current_spectral_evidence_reusable,
    enrich_current_v0_research_for_preview,
    persist_exact_current_spectral_from_attempt,
)
from lib.dispatch.types import ImportOneRun
from lib.import_preview import (
    ImportPreviewValues,
    _lossless_candidate_spectral_failure,
    _prefer_successful_spectral_detail,
    compose_attempt_spectral_audit,
    measure_and_persist_candidate_evidence,
    preview_import_from_path,
    preview_import_from_values,
)
from lib.measurement import (
    AudioCodecProbeError,
    LocalFileInspection,
    PreimportMeasurement,
)
from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    ImportResult,
    QualityRankConfig,
    SpectralAnalysisDetail,
    SpectralDetail,
    TargetQualityContract,
    V0ProbeEvidence,
    candidate_preimport_reject_fact,
    full_pipeline_decision,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    snapshot_audio_files,
    snapshot_fingerprint,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.dispatch_helpers import (
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.evidence_helpers import (
    build_parity_candidate_evidence,
    make_album_quality_evidence,
    make_audio_corrupt_validation_report,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import hermetic_beets_config_defaults, make_request_row

_HERMETIC_BEETS_DEFAULTS: AbstractContextManager[tuple[str, str]] | None = None


def setUpModule() -> None:
    global _HERMETIC_BEETS_DEFAULTS
    _HERMETIC_BEETS_DEFAULTS = hermetic_beets_config_defaults()
    _HERMETIC_BEETS_DEFAULTS.__enter__()


def tearDownModule() -> None:
    assert _HERMETIC_BEETS_DEFAULTS is not None
    _HERMETIC_BEETS_DEFAULTS.__exit__(None, None, None)


_PREVIEW_RUNTIME = tempfile.TemporaryDirectory()
_PREVIEW_SOURCE_ROOT = os.path.join(_PREVIEW_RUNTIME.name, "slskd")
_PREVIEW_PROCESSING_ROOT = os.path.join(_PREVIEW_RUNTIME.name, "processing")
os.mkdir(_PREVIEW_SOURCE_ROOT)
os.mkdir(_PREVIEW_PROCESSING_ROOT, 0o700)
os.mkdir(os.path.join(_PREVIEW_PROCESSING_ROOT, "albums"), 0o700)
os.mkdir(os.path.join(_PREVIEW_PROCESSING_ROOT, "preview"), 0o700)


def _preview_config() -> CratediggerConfig:
    ini = configparser.ConfigParser()
    ini["Beets Validation"] = {
        "harness_path": "/fake/harness/run_beets_harness.sh",
        "audio_check": "off",
    }
    ini["Pipeline DB"] = {"enabled": "true"}
    ini["Slskd"] = {"download_dir": _PREVIEW_SOURCE_ROOT}
    ini["Paths"] = {"processing_dir": _PREVIEW_PROCESSING_ROOT}
    return CratediggerConfig.from_ini(ini)


def _preview_runtime_config(
    *,
    beets_harness_path: str = "",
    pipeline_db_enabled: bool = False,
    verified_lossless_target: str = "",
) -> CratediggerConfig:
    """Direct-test config with the same private-root contract as Nix."""
    return CratediggerConfig(
        slskd_download_dir=_PREVIEW_SOURCE_ROOT,
        processing_dir=_PREVIEW_PROCESSING_ROOT,
        beets_harness_path=beets_harness_path,
        pipeline_db_enabled=pipeline_db_enabled,
        verified_lossless_target=verified_lossless_target,
    )


class TestSpectralAuditMerge(unittest.TestCase):
    def test_have_reuse_requires_a_decision_usable_grade(self):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        cases = (
            ("genuine", True),
            ("marginal", True),
            ("suspect", True),
            ("likely_transcode", True),
            ("error", False),
            ("", False),
            (None, False),
        )
        for grade, expected in cases:
            with self.subTest(grade=grade):
                evidence = make_album_quality_evidence(
                    measurement=AudioQualityMeasurement(
                        spectral_grade=grade,
                        spectral_subject=(
                            "installed" if grade is not None else None
                        ),
                        spectral_provenance=(
                            "measured" if grade is not None else None
                        ),
                        spectral_measurement_version=(
                            SPECTRAL_MEASUREMENT_VERSION
                            if grade is not None
                            else None
                        ),
                    ),
                )
                self.assertEqual(
                    current_spectral_evidence_reusable(evidence),
                    expected,
                )

    def test_family_in_the_armed_forces_legacy_have_is_not_reused(self):
        """A usable old grade cannot bypass a same-generation HAVE scan."""

        evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="suspect",
                spectral_bitrate_kbps=128,
                spectral_subject="installed",
                spectral_provenance="measured",
                spectral_measurement_version=None,
            ),
        )

        self.assertFalse(current_spectral_evidence_reusable(evidence))

    def test_preserved_source_legacy_have_is_reused_without_relabeling(self):
        """R19 source evidence is usable although its analyzer is legacy."""

        from lib.quality import AlbumQualityEvidenceFile

        evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            files=[AlbumQualityEvidenceFile(
                relative_path="01.opus",
                size_bytes=1,
                mtime_ns=1,
                extension="opus",
                container="opus",
                codec="opus",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=123,
                avg_bitrate_kbps=123,
                median_bitrate_kbps=123,
                format="Opus",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=232,
                spectral_subject="source",
                spectral_provenance="carried",
                spectral_measurement_version=None,
                was_converted_from="flac",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )

        self.assertTrue(current_spectral_evidence_reusable(evidence))
        self.assertIsNone(evidence.measurement.spectral_measurement_version)

    def test_preserved_source_error_grade_is_not_reused(self):
        """R19 does not make an analyzer error a policy grade."""

        evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=123,
                avg_bitrate_kbps=123,
                median_bitrate_kbps=123,
                format="Opus",
                spectral_grade="error",
                spectral_subject="source",
                spectral_provenance="carried",
                spectral_measurement_version=None,
                was_converted_from="flac",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )

        self.assertFalse(current_spectral_evidence_reusable(evidence))

    def test_lossless_candidate_requires_a_successful_usable_grade(self):
        cases = (
            ("absent", None),
            ("not_attempted", SpectralAnalysisDetail(attempted=False)),
            (
                "error",
                SpectralAnalysisDetail(
                    attempted=True,
                    error="RuntimeError: decoder failed",
                ),
            ),
            ("grade_none", SpectralAnalysisDetail(attempted=True, grade=None)),
            ("grade_error", SpectralAnalysisDetail(attempted=True, grade="error")),
        )
        for name, candidate in cases:
            with self.subTest(name=name):
                failure = _lossless_candidate_spectral_failure(
                    PreimportMeasurement(
                        lossless_candidate=True,
                        spectral_audit=SpectralDetail(candidate=candidate),
                    ),
                    lossless_candidate=True,
                )
                self.assertIsNotNone(failure)

    def test_wav_conversion_preserves_source_spectral(self):
        """WAV→Opus is a lossless-source derivative, just like FLAC→Opus."""
        from lib.current_library_evidence import preserve_existing_source_spectral
        from lib.quality import (
            EVIDENCE_SUBJECT_SOURCE,
            AlbumQualityEvidenceFile,
        )

        evidence = make_album_quality_evidence(
            mb_release_id="wav-derived",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.opus",
                size_bytes=1,
                mtime_ns=1,
                extension="opus",
                container="opus",
                codec="opus",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="Opus",
                spectral_grade="genuine",
                spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                spectral_provenance="carried",
                was_converted_from="wav",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )

        self.assertTrue(preserve_existing_source_spectral(evidence))

    def test_native_alac_m4a_remeasures_instead_of_preserving_source(self):
        """An .m4a snapshot needs its ALAC codec fact, not its extension."""
        from lib.current_library_evidence import preserve_existing_source_spectral
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityEvidenceFile

        evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            mb_release_id="native-alac-m4a",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.m4a",
                size_bytes=1,
                mtime_ns=1,
                extension="m4a",
                container="m4a",
                codec="m4a",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=700,
                avg_bitrate_kbps=750,
                median_bitrate_kbps=725,
                format="ALAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
                spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                spectral_provenance="carried",
                spectral_measurement_version=None,
                was_converted_from="flac",
            ),
            codec="m4a",
            container="m4a",
            storage_format="ALAC",
        )

        self.assertFalse(preserve_existing_source_spectral(evidence))
        self.assertFalse(current_spectral_evidence_reusable(evidence))

    def test_source_anchor_alone_does_not_preserve_source_spectral(self):
        """An anchor identifies provenance, not an irreplaceable derivative."""
        from lib.current_library_evidence import preserve_existing_source_spectral
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityV0Metric

        evidence = make_album_quality_evidence(
            mb_release_id="anchor-only",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=129,
                avg_bitrate_kbps=129,
                median_bitrate_kbps=129,
                format="Opus",
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=187,
                avg_bitrate_kbps=213,
                median_bitrate_kbps=210,
                subject=EVIDENCE_SUBJECT_SOURCE,
                provenance="carried",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.assertFalse(preserve_existing_source_spectral(evidence))

    def test_proof_alone_does_not_preserve_source_spectral(self):
        """A proof alone does not prove the installed bytes are derivative."""
        from lib.current_library_evidence import preserve_existing_source_spectral
        from lib.quality import VerifiedLosslessProof

        evidence = make_album_quality_evidence(
            mb_release_id="proof-only",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=129,
                avg_bitrate_kbps=129,
                median_bitrate_kbps=129,
                format="Opus",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier="request_seed",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.assertFalse(preserve_existing_source_spectral(evidence))

    def test_native_row_without_lineage_is_not_preserved(self):
        """A native copy with no lossless lineage is scanned normally."""
        from lib.current_library_evidence import preserve_existing_source_spectral

        evidence = make_album_quality_evidence(
            mb_release_id="native-mp3",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
            ),
        )
        self.assertFalse(preserve_existing_source_spectral(evidence))

    def test_candidate_measured_error_yields_to_harness_success(self):
        measured = SpectralAnalysisDetail(
            attempted=True, error="RuntimeError: measured failed")
        harness = SpectralAnalysisDetail(
            attempted=True, grade="genuine", suspect_pct=0.0)

        self.assertIs(
            _prefer_successful_spectral_detail(measured, harness), harness)

    def test_composition_keeps_preview_have_over_harness_duplicate(self):
        measured = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="likely_transcode", bitrate_kbps=224),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="likely_transcode", bitrate_kbps=224),
        )
        harness = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=228),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=122),
        )

        composed = compose_attempt_spectral_audit(measured, harness)

        assert composed.existing is not None
        self.assertEqual(composed.existing.grade, "likely_transcode")
        self.assertEqual(composed.existing.bitrate_kbps, 224)

    def test_existing_measured_error_yields_to_harness_success(self):
        measured = SpectralAnalysisDetail(
            attempted=True, grade="suspect",
            error="TypeError: malformed track detail")
        harness = SpectralAnalysisDetail(
            attempted=True, grade="suspect", bitrate_kbps=128,
            suspect_pct=60.0)

        self.assertIs(
            _prefer_successful_spectral_detail(measured, harness), harness)


class TestImportPreviewValues(unittest.TestCase):
    def test_existing_spectral_grade_field_preserves_struct_positional_order(self):
        fields = list(ImportPreviewValues.__struct_fields__)
        self.assertLess(
            fields.index("existing_spectral_bitrate"),
            fields.index("existing_spectral_grade"),
        )

    def test_values_preview_delegates_to_full_pipeline_shape(self):
        values = ImportPreviewValues(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            is_vbr=True,
        )

        preview = preview_import_from_values(values)

        direct = full_pipeline_decision(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            is_vbr=True,
        )
        self.assertEqual(preview.simulation, direct)
        self.assertEqual(preview.verdict, "would_import")
        self.assertIn("stage2_import:import", preview.stage_chain)

    def test_values_preview_classifies_spectral_reject_as_confident(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=False,
                min_bitrate=320,
                is_cbr=True,
                # Both codecs and both grades are stated (issue #829 Phase 5
                # PR2b): a spectral number carries no class without a codec
                # whose ladder can read it and an authorizing album verdict.
                new_format="MP3",
                existing_format="MP3",
                existing_min_bitrate=128,
                spectral_grade="suspect",
                spectral_bitrate=96,
                existing_spectral_grade="likely_transcode",
                existing_spectral_bitrate=128,
            )
        )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertTrue(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "spectral_reject")

    def test_values_preview_keeps_import_that_quality_gate_would_requeue(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=False,
                min_bitrate=160,
                avg_bitrate=160,
                is_cbr=False,
                is_vbr=True,
                new_format="MP3",
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertTrue(preview.would_import)
        self.assertFalse(preview.confident_reject)
        self.assertFalse(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "requeue_upgrade")
        self.assertEqual(
            preview.stage_chain,
            [
                "preimport_nested:pass",
                "preimport_audio:pass",
                "stage0_spectral_gate:would_run",
                "stage2_import:import",
                "stage3_quality_gate:requeue_upgrade",
            ],
        )

    def test_values_preview_classifies_provisional_lossless_upgrade(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="suspect",
                spectral_bitrate=160,
                post_conversion_min_bitrate=228,
                post_conversion_is_cbr=False,
                converted_count=12,
                candidate_v0_probe_avg=228,
                candidate_v0_probe_kind="lossless_source_v0",
                existing_v0_probe_avg=171,
                verified_lossless_target="opus 128",
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertEqual(preview.reason, "provisional_lossless_upgrade")
        self.assertIn(
            "stage2_import:provisional_lossless_upgrade",
            preview.stage_chain,
        )
        assert preview.simulation is not None
        self.assertEqual(preview.simulation["target_final_format"], "opus 128")

    def test_values_preview_high_v0_override_imports_verified(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="likely_transcode",
                spectral_bitrate=160,
                post_conversion_min_bitrate=237,
                post_conversion_is_cbr=False,
                converted_count=12,
                candidate_v0_probe_avg=276,
                candidate_v0_probe_min=237,
                candidate_v0_probe_kind="lossless_source_v0",
                verified_lossless_target="opus 128",
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertEqual(preview.reason, "import")
        self.assertIn("stage2_import:import", preview.stage_chain)
        assert preview.simulation is not None
        self.assertTrue(preview.simulation["verified_lossless"])
        self.assertEqual(preview.simulation["final_status"], "imported")
        self.assertFalse(preview.simulation["keep_searching"])

    def test_values_preview_prefers_provisional_over_stage1_reject(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="likely_transcode",
                spectral_bitrate=128,
                existing_spectral_bitrate=160,
                post_conversion_min_bitrate=228,
                post_conversion_is_cbr=False,
                converted_count=12,
                candidate_v0_probe_avg=228,
                candidate_v0_probe_min=228,
                candidate_v0_probe_kind="lossless_source_v0",
                existing_v0_probe_avg=171,
            )
        )

        self.assertEqual(preview.verdict, "would_import")
        self.assertFalse(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "provisional_lossless_upgrade")
        # A lossless container yields no kbps class (issue #829 Phase 5
        # PR2b), so Stage 1 withholds instead of rejecting. The preserved
        # contract is the one this test is named for: the provisional lane
        # still owns the verdict for a suspect lossless source.
        self.assertIn("stage1_spectral:import_no_exist", preview.stage_chain)
        self.assertIn(
            "stage2_import:provisional_lossless_upgrade",
            preview.stage_chain,
        )

    def test_values_preview_classifies_suspect_lossless_downgrade(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=True,
                is_cbr=False,
                spectral_grade="suspect",
                spectral_bitrate=160,
                post_conversion_min_bitrate=175,
                post_conversion_is_cbr=False,
                converted_count=12,
                candidate_v0_probe_avg=175,
                candidate_v0_probe_kind="lossless_source_v0",
                existing_v0_probe_avg=171,
            )
        )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertTrue(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "suspect_lossless_downgrade")

    def test_values_preview_classifies_lossless_source_locked(self):
        # Lossy candidate (is_flac=False) facing existing with comparable
        # lossless-source V0 probe — preview must classify as confident
        # reject so the importer never schedules it. Parallel to the
        # suspect_lossless_downgrade case above.
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=False,
                is_cbr=False,
                is_vbr=True,
                min_bitrate=176,
                avg_bitrate=205,
                spectral_grade="likely_transcode",
                spectral_bitrate=128,
                existing_min_bitrate=116,
                existing_avg_bitrate=131,
                existing_format="opus",
                existing_v0_probe_avg=240,
            )
        )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertTrue(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "lossless_source_locked")


class TestImportPreviewPath(unittest.TestCase):
    def _db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            status="unsearchable",
            min_bitrate=180,
            current_lossless_source_v0_probe_min_bitrate=128,
            current_lossless_source_v0_probe_avg_bitrate=171,
            current_lossless_source_v0_probe_median_bitrate=169,
            artist_name="Artist",
            album_title="Album",
        ))
        return db

    def _source_dir(self) -> str:
        source = tempfile.mkdtemp()
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"not real audio but never inspected in this test")
        return source

    def _beets_current(self, source: str) -> FakeBeetsDB:
        from lib.beets_db import AlbumInfo

        beets = FakeBeetsDB(library_root=source)
        beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            is_cbr=True,
            album_path=source,
            format="MP3",
        ))
        return beets

    def _seed_current_without_v0(
        self,
        db: FakePipelineDB,
        source: str,
    ):
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            source_path=source,
            files=snapshot_audio_files(source),
            v0_metric=None,
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(42, stored.id)
        return stored

    def test_aac_m4a_preview_entrypoints_reuse_measurement_codec_probe(self):
        """Neither preview path may repeat M4A classification after measure."""
        from lib.dispatch.types import ImportOneRun
        from lib.measurement import ExistingSpectralAuditLookup

        run = ImportOneRun(
            command=("import_one",),
            returncode=0,
            stdout="",
            stderr="",
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=256,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=256,
                    format="AAC",
                ),
            ),
        )
        for entrypoint in ("worker", "direct"):
            with self.subTest(entrypoint=entrypoint):
                db = self._db()
                source = tempfile.mkdtemp()
                try:
                    with open(os.path.join(source, "01.m4a"), "wb") as handle:
                        handle.write(b"aac")
                    fake_beets = FakeBeetsDB()
                    with patch(
                        "lib.config.read_runtime_config",
                        return_value=_preview_config(),
                    ), patch(
                        "lib.beets_db.BeetsDB",
                        lambda fake_beets=fake_beets, **_kwargs: fake_beets,
                    ), patch(
                        "lib.measurement.ffprobe_audio_codec_name",
                        return_value="aac",
                    ) as codec_probe, patch(
                        "lib.import_preview.run_import_one",
                        return_value=run,
                    ):
                        if entrypoint == "worker":
                            result = measure_and_persist_candidate_evidence(
                                db,
                                request_id=42,
                                path=source,
                                run_import_fn=lambda **_kwargs: run,
                                existing_spectral_resolver=(
                                    lambda _release_id: ExistingSpectralAuditLookup()
                                ),
                            )
                        else:
                            result = preview_import_from_path(
                                db,
                                request_id=42,
                                path=source,
                            )

                    self.assertNotEqual(result.decision, "spectral_analysis_failed")
                    self.assertEqual(codec_probe.call_count, 1)
                finally:
                    shutil.rmtree(source, ignore_errors=True)

    def test_m4a_codec_probe_failure_is_measurement_failed_before_harness(self):
        from lib.measurement import ExistingSpectralAuditLookup

        db = self._db()
        source = tempfile.mkdtemp()
        harness_called = False
        try:
            with open(os.path.join(source, "01.m4a"), "wb") as handle:
                handle.write(b"unknown-codec")

            def run_import(**_kwargs: Any):
                nonlocal harness_called
                harness_called = True
                raise AssertionError("harness must not run after codec probe failure")

            fake_beets = FakeBeetsDB()
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ), patch(
                "lib.measurement.ffprobe_audio_codec_name",
                return_value=None,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    run_import_fn=run_import,
                    existing_spectral_resolver=(
                        lambda _release_id: ExistingSpectralAuditLookup()
                    ),
                )

            self.assertEqual(result.verdict, "measurement_failed")
            self.assertEqual(result.decision, "measurement_crashed")
            self.assertIn("codec probe", result.detail or "")
            self.assertFalse(harness_called)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def _crashed_run(self):
        """The live 2026-07-18 shape: a stage-2 crash mid-mint left a partial
        ImportResult with a real source_measurement but no proof/target."""
        from lib.dispatch.types import ImportOneRun

        return ImportOneRun(
            command=("import_one",),
            returncode=99,
            stdout="",
            stderr="",
            import_result=ImportResult(
                exit_code=99,
                decision="crash",
                error="AttributeError: 'Namespace' object has no attribute 'filetype'",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=767,
                    avg_bitrate_kbps=851,
                    median_bitrate_kbps=847,
                    format="FLAC",
                    spectral_grade="genuine",
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
            ),
        )

    def _assert_nothing_persisted(self, db: FakePipelineDB, source: str) -> None:
        stored = db.find_album_quality_evidence(
            mb_release_id="mbid-42",
            snapshot_fingerprint=snapshot_fingerprint(
                snapshot_audio_files(source)),
        )
        self.assertIsNone(
            stored, "a crashed ImportResult must never persist evidence")

    def test_worker_stage2_crash_is_measurement_failed_not_evidence_ready(self):
        from lib.measurement import ExistingSpectralAuditLookup

        db = self._db()
        source = self._source_dir()
        run = self._crashed_run()
        try:
            fake_beets = FakeBeetsDB()
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    import_job_id=7,
                    run_import_fn=lambda **_kwargs: run,
                    existing_spectral_resolver=(
                        lambda _release_id: ExistingSpectralAuditLookup()
                    ),
                )

            self.assertEqual(result.verdict, "measurement_failed")
            self.assertEqual(result.decision, "crash")
            self.assertIn("filetype", result.detail or "")
            self._assert_nothing_persisted(db, source)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_path_preview_stage2_crash_never_persists_evidence(self):
        db = self._db()
        source = self._source_dir()
        run = self._crashed_run()
        try:
            fake_beets = FakeBeetsDB()
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=run,
            ):
                result = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    import_job_id=7,
                    persist_candidate_evidence=True,
                )

            self.assertNotEqual(result.verdict, "evidence_ready")
            self._assert_nothing_persisted(db, source)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_loader_rebuilds_blank_source_path_current_evidence(self):
        """A blank-path HAVE row must be rebuilt, not reused authoritatively.

        download_log 37206 (French Quarter): the linked current evidence was
        a legacy backfill with ``source_path=''``; every enrichment guard
        refused it, so preview kept handing the importer a spectrally blind
        HAVE side. The preview loader must rebuild such rows from beets so
        the same preview's enrichment can complete them.
        """
        from lib.beets_db import AlbumInfo
        from lib.current_library_evidence import load_current_evidence_for_preview
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path="",
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=186,
                    avg_bitrate_kbps=194,
                    median_bitrate_kbps=194,
                    format="MP3",
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=3,
                min_bitrate_kbps=186,
                avg_bitrate_kbps=194,
                median_bitrate_kbps=194,
                is_cbr=False,
                album_path=source,
                format="MP3",
            ))
            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
                result = load_current_evidence_for_preview(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root="",
                    preloaded_evidence=stored,
                )

            self.assertEqual(result.status, "ready", result.reason)
            current = result.evidence
            assert current is not None
            self.assertEqual(current.source_path, source)
            linked_id = db.get_request_current_evidence_id(42)
            self.assertEqual(linked_id, stored.id)
            linked = db.load_album_quality_evidence_by_id(linked_id)
            assert linked is not None
            self.assertEqual(linked.source_path, source)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_loader_rebuilds_v1_current_evidence_for_import_attempt(self):
        """An actual import attempt must decide from a fresh v4 HAVE row."""
        from lib.beets_db import AlbumInfo
        from lib.current_library_evidence import load_current_evidence_for_preview
        from tests.fakes import FakeBeetsDB

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=256,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=256,
                    format="AAC",
                    is_cbr=True,
                ),
                lineage_version=1,
                on_disk_v0_research_attempted=True,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            fake_beets = FakeBeetsDB()
            fake_beets.set_album_info("mbid-42", AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                is_cbr=True,
                album_path=source,
                format="AAC",
            ))
            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: fake_beets):
                result = load_current_evidence_for_preview(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                    preloaded_evidence=stored,
                )

            self.assertEqual(result.status, "ready")
            current = result.evidence
            assert current is not None
            self.assertEqual(current.id, stored.id)
            self.assertEqual(current.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
            self.assertEqual(current.measurement.format, "AAC")
            self.assertEqual(current.measurement.avg_bitrate_kbps, 256)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_persists_qigong_current_spectral_snapshot(self):
        """Qigong: the exact installed HAVE scan becomes durable evidence."""
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                ),
                lineage_version=1,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=stored,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                    suspect_pct=52.17,
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "ready")
            assert result.evidence is not None
            self.assertEqual(result.evidence.measurement.spectral_grade, "genuine")
            self.assertEqual(
                result.evidence.measurement.spectral_bitrate_kbps,
                96,
            )
            self.assertEqual(result.evidence.id, stored.id)
            self.assertEqual(
                result.evidence.snapshot_fingerprint,
                stored.snapshot_fingerprint,
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_fresh_have_audit_overwrites_stale_installed_grade(self):
        """Issue #815 fresh-audit-wins pin (Shugo Tokumaru EXIT, request 4351).

        An installed-subject evidence row carrying a STALE likely_transcode/128
        (a legacy landmine seeded on a matched fingerprint — a state a clean
        forward run can never produce) is re-persisted to the fresh genuine/160
        audit of the exact same bytes. Pre-#815 the fill-only-if-NULL early
        return discarded the fresh audit; the stale 128 then decided the
        dl 37742 import and a fake-320 replaced the genuine 192 copy.
        """
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=192,
                    avg_bitrate_kbps=192,
                    median_bitrate_kbps=192,
                    format="MP3",
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=128,
                    spectral_subject="installed",
                    spectral_provenance="measured",
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=stored,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=160,
                    suspect_pct=30.0,
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "ready")
            assert result.evidence is not None
            self.assertEqual(
                result.evidence.measurement.spectral_grade, "genuine")
            self.assertEqual(
                result.evidence.measurement.spectral_bitrate_kbps, 160)
            # The overwrite is durable and stamped measured/installed.
            reloaded = db.load_album_quality_evidence_by_id(stored.id)
            assert reloaded is not None
            self.assertEqual(reloaded.measurement.spectral_grade, "genuine")
            self.assertEqual(reloaded.measurement.spectral_bitrate_kbps, 160)
            self.assertEqual(
                reloaded.measurement.spectral_subject, "installed")
            self.assertEqual(
                reloaded.measurement.spectral_provenance, "measured")
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_fresh_audit_never_overwrites_lossless_source_carried_grade(self):
        """R19 must-still-work: a lossless-sourced row that already carries a
        source-subject grade is NEVER overwritten by an installed-derivative
        fresh audit, even under #815 fresh-audit-wins."""
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityV0Metric

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=129,
                    avg_bitrate_kbps=129,
                    median_bitrate_kbps=129,
                    format="MP3",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=140,
                    spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                    spectral_provenance="carried",
                    was_converted_from="flac",
                ),
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=187,
                    avg_bitrate_kbps=213,
                    median_bitrate_kbps=210,
                    subject=EVIDENCE_SUBJECT_SOURCE,
                    provenance="carried",
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=stored,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=200,
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "skipped")
            reloaded = db.load_album_quality_evidence_by_id(stored.id)
            assert reloaded is not None
            self.assertEqual(reloaded.measurement.spectral_grade, "suspect")
            self.assertEqual(
                reloaded.measurement.spectral_subject, EVIDENCE_SUBJECT_SOURCE)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_never_persists_onto_lossless_sourced_row(self):
        """A source anchor alone leaves the readable installed bytes scannable."""
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityV0Metric

        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-6108",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=129,
                    avg_bitrate_kbps=129,
                    median_bitrate_kbps=129,
                    format="Opus",
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                ),
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=187,
                    avg_bitrate_kbps=213,
                    median_bitrate_kbps=210,
                    subject=EVIDENCE_SUBJECT_SOURCE,
                    provenance="carried",
                ),
                codec="opus",
                container="opus",
                storage_format="Opus",
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(42, stored.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=stored,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=128,
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "ready", result.reason)
            refreshed = db.load_album_quality_evidence_by_id(stored.id)
            assert refreshed is not None
            self.assertEqual(refreshed.measurement.spectral_grade, "genuine")
            self.assertEqual(refreshed.measurement.spectral_subject, "installed")
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_accepts_moved_path_with_the_exact_fingerprint(self):
        db = self._db()
        source = self._source_dir()
        other = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)
            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                ),
                measured_existing_path=other,
            )

            self.assertEqual(result.status, "ready")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertEqual(persisted.measurement.spectral_grade, "genuine")
            self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 96)
            self.assertEqual(persisted.source_path, source)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)
            shutil.rmtree(other, ignore_errors=True)

    def test_attempt_scan_empty_path_is_stale_not_an_empty_digest_match(self):
        db = self._db()
        source = tempfile.mkdtemp()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=[],
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "stale")
            self.assertEqual(
                result.reason,
                "attempt HAVE path does not match current evidence fingerprint",
            )
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_missing_path_is_stale(self):
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                ),
                measured_existing_path=os.path.join(source, "missing"),
            )

            self.assertEqual(result.status, "stale")
            self.assertEqual(
                result.reason,
                "attempt HAVE path does not match current evidence fingerprint",
            )
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_mismatched_path_is_stale(self):
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)
            with open(os.path.join(source, "02.mp3"), "wb") as handle:
                handle.write(b"different snapshot")

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "stale")
            self.assertEqual(
                result.reason,
                "attempt HAVE path does not match current evidence fingerprint",
            )
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_attempt_scan_snapshot_error_is_failed_with_its_detail(self):
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)
            os.unlink(os.path.join(source, "01.mp3"))
            os.symlink(
                os.path.join(source, "vanished.mp3"),
                os.path.join(source, "01.mp3"),
            )

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "failed")
            self.assertTrue((result.reason or "").startswith(
                "SnapshotAudioFilesError: could not stat audio file "
            ))
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_fresh_have_failure_overrides_stored_spectral_success(self):
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade="genuine",
                    spectral_subject="installed",
                    spectral_provenance="measured",
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)

            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=42,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    error="RuntimeError: fresh HAVE scan failed",
                ),
                measured_existing_path=source,
            )

            self.assertEqual(result.status, "incomplete")
            self.assertIn("fresh HAVE scan failed", result.reason or "")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_worker_wires_have_scan_into_current_evidence(self):
        db = self._db()
        source = self._source_dir()
        try:
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-42",
                source_path=source,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            current = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(42, current.id)
            measurement = PreimportMeasurement(
                audio_corrupt=True,
                corrupt_files=["01.mp3"],
                audio_validation=make_audio_corrupt_validation_report(
                    "01.mp3",
                ),
                folder_layout="flat",
                audio_file_count=1,
                existing_spectral_path=source,
                spectral_audit=SpectralDetail(
                    candidate=SpectralAnalysisDetail(
                        attempted=True,
                        grade="likely_transcode",
                        bitrate_kbps=96,
                    ),
                    existing=SpectralAnalysisDetail(
                        attempted=True,
                        grade="genuine",
                        bitrate_kbps=96,
                        spectral_measurement_version=(
                            SPECTRAL_MEASUREMENT_VERSION
                        ),
                    ),
                ),
            )
            candidate = make_album_quality_evidence(
                mb_release_id="mbid-42-candidate"
            )
            with patch(
                "lib.beets_db.BeetsDB",
                return_value=self._beets_current(source),
            ), patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(filetype="mp3"),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=measurement,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    persist_measurement_fn=(
                        lambda *args, **kwargs: EvidenceBuildResult(
                            candidate,
                            "ready",
                        )
                    ),
                )

            self.assertEqual(result.verdict, "evidence_ready")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertEqual(persisted.measurement.spectral_grade, "genuine")
            self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 96)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_attempt_is_persisted_once_after_failure(self):
        db = self._db()
        source = self._source_dir()
        calls: list[str] = []
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def failed_probe(path: str):
                calls.append(path)
                raise RuntimeError("ffmpeg failed")

            first = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=failed_probe,
            )
            second = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=failed_probe,
            )

            self.assertEqual(first.status, "ready")
            self.assertEqual(second.status, "ready")
            self.assertEqual(calls, [source])
            assert second.evidence is not None
            self.assertTrue(second.evidence.on_disk_v0_research_attempted)
            self.assertIsNone(second.evidence.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_persists_neutral_metric(self):
        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None
            result = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=lambda _path: V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                ),
            )

            self.assertEqual(result.status, "ready")
            assert result.evidence is not None
            self.assertTrue(result.evidence.on_disk_v0_research_attempted)
            assert result.evidence.v0_metric is not None
            self.assertEqual(
                result.evidence.v0_metric.subject,
                "installed",
            )
            self.assertEqual(result.evidence.v0_metric.provenance, "measured")
            self.assertEqual(result.evidence.v0_metric.avg_bitrate_kbps, 259)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_requires_exact_current_snapshot(self):
        db = self._db()
        source = self._source_dir()
        calls: list[str] = []
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def probe(path: str):
                calls.append(path)

            wrong_id = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id + 1,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=probe,
            )
            with open(os.path.join(source, "01.mp3"), "ab") as handle:
                handle.write(b"changed")
            stale = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=probe,
            )

            self.assertEqual(wrong_id.status, "stale")
            self.assertEqual(stale.status, "stale")
            self.assertEqual(calls, [])
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_releases_claim_when_probe_changes_files(self):
        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def mutating_probe(path: str) -> V0ProbeEvidence:
                with open(os.path.join(path, "01.mp3"), "ab") as handle:
                    handle.write(b"changed during probe")
                return V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                )

            result = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=mutating_probe,
            )

            self.assertEqual(result.status, "stale")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertFalse(persisted.on_disk_v0_research_attempted)
            self.assertIsNone(persisted.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_loader_rejects_have_when_v0_probe_changes_files(self):
        """A stale enrichment result must invalidate the whole preview HAVE."""
        from lib.current_library_evidence import load_current_evidence_for_preview

        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)

            def mutating_probe(path: str) -> V0ProbeEvidence:
                with open(os.path.join(path, "01.mp3"), "ab") as handle:
                    handle.write(b"changed during wrapper probe")
                return V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                )

            def mutating_enrichment(*args: Any, **kwargs: Any):
                return enrich_current_v0_research_for_preview(
                    *args,
                    **kwargs,
                    probe_fn=mutating_probe,
                )

            with patch(
                "lib.beets_db.BeetsDB",
                return_value=self._beets_current(source),
            ):
                result = load_current_evidence_for_preview(
                    db,
                    request_id=42,
                    mb_release_id="mbid-42",
                    quality_ranks=QualityRankConfig.defaults(),
                    beets_library_root=source,
                    preloaded_evidence=current,
                    enrich_current_fn=mutating_enrichment,
                )

            self.assertEqual(result.status, "stale")
            self.assertIsNone(result.evidence)
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertFalse(persisted.on_disk_v0_research_attempted)
            self.assertIsNone(persisted.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_v0_research_releases_claim_when_current_link_changes(self):
        db = self._db()
        source = self._source_dir()
        try:
            current = self._seed_current_without_v0(db, source)
            assert current.id is not None

            def relinking_probe(_path: str) -> V0ProbeEvidence:
                db.set_request_current_evidence(42, None)
                return V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=201,
                    avg_bitrate_kbps=259,
                    median_bitrate_kbps=255,
                )

            result = enrich_current_v0_research_for_preview(
                db,
                request_id=42,
                expected_evidence_id=current.id,
                expected_snapshot_fingerprint=current.snapshot_fingerprint,
                current_album_path=source,
                probe_fn=relinking_probe,
            )

            self.assertEqual(result.status, "stale")
            persisted = db.load_album_quality_evidence_by_id(current.id)
            assert persisted is not None
            self.assertFalse(persisted.on_disk_v0_research_attempted)
            self.assertIsNone(persisted.v0_metric)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def _direct_preview_override(self, db: FakePipelineDB) -> int | None:
        source = self._source_dir()
        run = SimpleNamespace(
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=245,
                    median_bitrate_kbps=245,
                    format="MP3",
                ),
            )
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="flac",
                    min_bitrate_bps=900000,
                    is_vbr=False,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                ),
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=run,
            ) as mock_run:
                preview_import_from_path(db, request_id=42, path=source)
            return mock_run.call_args.kwargs["override_min_bitrate"]
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_direct_preview_no_fk_ignores_request_spectral_floor(self):
        db = self._db()
        db.request(42).update(
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=96,
        )

        self.assertIsNone(self._direct_preview_override(db))

    def test_direct_preview_authoritative_empty_ignores_stale_scalars(self):
        db = self._db()
        db.request(42).update(
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=96,
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade=None,
                spectral_bitrate_kbps=None,
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        # Exact absence discards the stale linked row wholesale: no HAVE
        # bitrate/spectral/V0/override input may survive into the dry run.
        self.assertIsNone(self._direct_preview_override(db))

    def test_direct_preview_ambiguous_current_fails_before_measurement(self):
        db = self._db()
        source = self._source_dir()
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_ids_for_release("mbid-42", [1, 2])
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.beets_db.BeetsDB",
                lambda **_kwargs: fake_beets,
            ), patch(
                "lib.import_preview.measure_preimport_state",
            ) as mock_measure:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            self.assertEqual(preview.decision, "current_evidence_failed")
            self.assertIn("ambiguous_current", preview.detail or "")
            mock_measure.assert_not_called()
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_worker_stale_have_enrichment_fails_before_measurement(
        self,
    ):
        """A lost HAVE authority cannot degrade into an absent comparison."""
        db = self._db()
        source = self._source_dir()
        try:
            def stale_current(*_args: Any, **_kwargs: Any) -> EvidenceBuildResult:
                return EvidenceBuildResult(
                    None,
                    "stale",
                    "current files changed during V0 probe",
                )

            with patch(
                "lib.import_preview.inspect_local_files",
            ) as inspect, patch(
                "lib.import_preview.run_import_one",
            ) as run_import:
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    current_evidence_loader=stale_current,
                )

            self.assertEqual(result.verdict, "measurement_failed")
            self.assertEqual(result.decision, "current_evidence_failed")
            self.assertIn("stale", result.detail or "")
            inspect.assert_not_called()
            run_import.assert_not_called()
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_direct_preview_rebuilds_changed_or_poisoned_link_before_use(self):
        for poisoned_identity in (False, True):
            with self.subTest(poisoned_identity=poisoned_identity):
                db = self._db()
                candidate = self._source_dir()
                current = self._source_dir()
                fake_beets = FakeBeetsDB()
                try:
                    linked = make_album_quality_evidence(
                        mb_release_id=(
                            "other-exact-release"
                            if poisoned_identity
                            else "mbid-42"
                        ),
                        files=snapshot_audio_files(current),
                        measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=320,
                            avg_bitrate_kbps=320,
                            format="MP3",
                            spectral_grade="likely_transcode",
                            spectral_bitrate_kbps=96,
                            spectral_subject="source",
                            spectral_provenance="measured",
                        ),
                    )
                    db.upsert_album_quality_evidence(linked)
                    stored = db.find_album_quality_evidence(
                        mb_release_id=linked.mb_release_id,
                        snapshot_fingerprint=linked.snapshot_fingerprint,
                    )
                    assert stored is not None and stored.id is not None
                    db.set_request_current_evidence(42, stored.id)
                    if not poisoned_identity:
                        with open(os.path.join(current, "01.mp3"), "ab") as fh:
                            fh.write(b"changed-current-bytes")
                    from lib.beets_db import AlbumInfo
                    fake_beets.set_album_info(
                        "mbid-42",
                        AlbumInfo(
                            album_id=1,
                            track_count=1,
                            min_bitrate_kbps=128,
                            avg_bitrate_kbps=128,
                            median_bitrate_kbps=128,
                            is_cbr=True,
                            album_path=current,
                            format="MP3",
                        ),
                    )
                    run = SimpleNamespace(import_result=ImportResult(
                        decision="import",
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=245,
                            avg_bitrate_kbps=245,
                            format="MP3",
                        ),
                    ))
                    with patch(
                        "lib.config.read_runtime_config",
                        return_value=_preview_runtime_config(
                            beets_harness_path="/fake/harness/run_beets_harness.sh",
                            pipeline_db_enabled=True,
                        ),
                    ), patch(
                        "lib.beets_db.BeetsDB",
                        lambda fake_beets=fake_beets, **_kwargs: fake_beets,
                    ), patch(
                        "lib.import_preview.inspect_local_files",
                        return_value=LocalFileInspection(filetype="mp3"),
                    ), patch(
                        "lib.import_preview.measure_preimport_state",
                        return_value=PreimportMeasurement(
                            folder_layout="flat",
                            audio_file_count=1,
                        ),
                    ) as mock_measure, patch(
                        "lib.import_preview.run_import_one",
                        return_value=run,
                    ) as mock_run:
                        preview_import_from_path(
                            db,
                            request_id=42,
                            path=candidate,
                        )

                    measurement_args = mock_measure.call_args.kwargs
                    self.assertFalse(
                        measurement_args["preserve_existing_source_spectral"]
                    )
                    if poisoned_identity:
                        self.assertIsNone(
                            measurement_args["existing_spectral_evidence"].grade
                        )
                    refreshed = db.load_album_quality_evidence_by_id(
                        db.get_request_current_evidence_id(42)
                    )
                    assert refreshed is not None
                    self.assertEqual(refreshed.mb_release_id, "mbid-42")
                    self.assertEqual(refreshed.source_path, current)
                    self.assertEqual(
                        refreshed.snapshot_fingerprint,
                        snapshot_fingerprint(snapshot_audio_files(current)),
                    )
                    self.assertNotEqual(
                        mock_run.call_args.kwargs["override_min_bitrate"],
                        320,
                    )
                    self.assertIsNone(
                        mock_run.call_args.kwargs["existing_v0_probe"]
                    )
                finally:
                    shutil.rmtree(candidate, ignore_errors=True)
                    shutil.rmtree(current, ignore_errors=True)

    def test_real_path_preview_runs_harness_dry_run_without_db_writes(self):
        db = self._db()
        source = self._source_dir()
        before = sorted(os.listdir(source))
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                           spectral_audit=SpectralDetail(
                               candidate=SpectralAnalysisDetail(
                                   attempted=True,
                                   grade="likely_transcode",
                                   bitrate_kbps=224,
                               ),
                               existing=SpectralAnalysisDetail(
                                   attempted=True,
                                   grade="likely_transcode",
                                   bitrate_kbps=224,
                               ),
                           ),
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=245,
                                   avg_bitrate_kbps=245,
                                   median_bitrate_kbps=245,
                                   format="MP3",
                               ),
                               spectral=SpectralDetail(
                                   candidate=SpectralAnalysisDetail(
                                       attempted=True,
                                       grade="genuine",
                                       bitrate_kbps=228,
                                   ),
                                   existing=SpectralAnalysisDetail(
                                       attempted=True,
                                       grade="genuine",
                                       bitrate_kbps=122,
                                   ),
                               ),
                           )
                       )) as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "would_import")
            self.assertEqual(sorted(os.listdir(source)), before)
            self.assertEqual(db.download_logs, [])
            self.assertEqual(db.denylist, [])
            self.assertTrue(mock_run.call_args.kwargs["dry_run"])
            self.assertIsNone(mock_run.call_args.kwargs["request_id"])
            self.assertNotIn("beets_library_root", mock_run.call_args.kwargs)
            assert preview.import_result is not None
            assert preview.import_result.spectral.existing is not None
            self.assertEqual(
                preview.import_result.spectral.existing.grade,
                "likely_transcode",
            )
            self.assertEqual(
                preview.import_result.spectral.existing.bitrate_kbps,
                224,
            )
            # The request-row V0 stamps are audit-only.  With no linked
            # current evidence, preview must not reconstruct a policy probe
            # from those legacy scalars.
            self.assertIsNone(mock_run.call_args.kwargs["existing_v0_probe"])
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_path_preview_persists_candidate_evidence_for_job_owner(self):
        """Post-migration 021: preview persists candidate evidence and wires
        the ``import_jobs.candidate_evidence_id`` FK. Loading via the FK
        chain returns the persisted row.
        """
        db = self._db()
        job = db.enqueue_import_job(
            "force_import",
            request_id=42,
            dedupe_key="force:42:/tmp/source",
            payload={"download_log_id": 1, "failed_path": "/tmp/source"},
        )
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=245,
                                   avg_bitrate_kbps=245,
                                   median_bitrate_kbps=245,
                                   format="MP3",
                               ),
                           )
                       )):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=False,
                    import_job_id=job.id,
                    persist_candidate_evidence=True,
                )

            self.assertEqual(preview.verdict, "would_import")
            evidence_id = db.get_import_job_candidate_evidence_id(job.id)
            self.assertIsNotNone(evidence_id)
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertEqual(loaded.measurement.avg_bitrate_kbps, 245)
            self.assertEqual([f.relative_path for f in loaded.files], ["01.mp3"])
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_path_preview_persists_candidate_evidence_for_download_log_owner(self):
        db = self._db()
        download_log_id = db.log_download(
            request_id=42,
            outcome="rejected",
            validation_result={"failed_path": "/tmp/failed"},
        )
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=245,
                                   avg_bitrate_kbps=245,
                                   median_bitrate_kbps=245,
                                   format="MP3",
                               ),
                           )
                       )):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=True,
                    download_log_id=download_log_id,
                    persist_candidate_evidence=True,
                )

            self.assertEqual(preview.verdict, "would_import")
            evidence_id = db.get_download_log_candidate_evidence_id(
                download_log_id
            )
            self.assertIsNotNone(evidence_id)
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertEqual(loaded.measurement.avg_bitrate_kbps, 245)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_configured_target_round_trips_when_request_target_is_null(self):
        db = self._db()
        download_log_id = db.log_download(
            request_id=42,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": "/tmp/config-target",
            },
        )
        source = tempfile.mkdtemp()
        with open(os.path.join(source, "01.flac"), "wb") as handle:
            handle.write(b"flac")
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                    verified_lossless_target="opus 128",
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="flac",
                    min_bitrate_bps=800000,
                    is_vbr=False,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                ),
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=SimpleNamespace(
                    import_result=ImportResult(
                        decision="downgrade",
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=800,
                            avg_bitrate_kbps=820,
                            median_bitrate_kbps=810,
                            format="FLAC",
                        ),
                        target_quality_contract=(
                            TargetQualityContract.from_explicit_label(
                                "opus 128"
                            )
                        ),
                    )
                ),
            ):
                preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=True,
                    download_log_id=download_log_id,
                    persist_candidate_evidence=True,
                )

            evidence_id = db.get_download_log_candidate_evidence_id(
                download_log_id
            )
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertEqual(loaded.measurement.format, "FLAC")
            self.assertEqual(loaded.target_format, "opus 128")
            self.assertFalse(loaded.target_is_cbr)
            self.assertEqual(loaded.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
            wrong_match = db.get_wrong_matches()[0]
            self.assertEqual(
                wrong_match["evidence_target_format"], "opus 128"
            )
            self.assertFalse(wrong_match["evidence_target_is_cbr"])
            self.assertEqual(
                wrong_match["evidence_lineage_version"],
                CURRENT_EVIDENCE_LINEAGE_VERSION)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_audit_survives_evidence_persistence_failure(self):
        db = self._db()
        source = self._source_dir()
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="suspect", bitrate_kbps=128),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="genuine"),
        )
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True)), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(filetype="mp3")), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           audio_corrupt=True,
                           corrupt_files=["01.mp3"],
                           audio_validation=(
                               make_audio_corrupt_validation_report("01.mp3")
                           ),
                           folder_layout="flat",
                           audio_file_count=1,
                           spectral_audit=audit,
                       )):
                preview = measure_and_persist_candidate_evidence(
                    db, request_id=42, path=source,
                    persist_measurement_fn=(
                        lambda *args, **kwargs: (_ for _ in ()).throw(
                            RuntimeError("database unavailable"))
                    ),
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            self.assertEqual(preview.decision, "evidence_persist_failed")
            assert preview.import_result is not None
            self.assertEqual(preview.import_result.spectral, audit)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_badlands_corruption_outranks_lossless_spectral_failure(self):
        """dl 37604: a corrupt FLAC candidate is completed integrity
        evidence, not an infrastructure-class spectral measurement failure.
        """
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ProcessIdentity,
        )
        from scripts.import_preview_worker import _AutomationPreviewDB

        db = self._db()
        self.assertTrue(
            db.reset_to_wanted(42, expected_status="unsearchable")
        )
        job = handoff_automation_owner(db, 42)
        lease = ExecutionLeaseSnapshot(
            host_boot_id="test-boot",
            invocation_id="badlands-preview",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=7001, start_ticks=70001),
        )
        claimed = claim_next_import_preview_job(db, worker_id="preview",
        execution_lease=lease,)
        assert claimed is not None and claimed.id == job.id
        preview_db = _AutomationPreviewDB(db, lease)
        source = tempfile.mkdtemp()
        with open(os.path.join(source, "01.flac"), "wb") as handle:
            handle.write(b"truncated lossless bytes")
        decode_error = (
            "01.flac: Cannot determine format of input 0:0 after EOF; "
            "Invalid data found when processing input"
        )
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True,
                grade="error",
                error="ffmpeg could not decode corrupt source",
            ),
            existing=SpectralAnalysisDetail(
                attempted=True,
                grade="suspect",
            ),
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="flac",
                    min_bitrate_bps=900_000,
                    is_vbr=False,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    audio_corrupt=True,
                    corrupt_files=["01.flac"],
                    audio_validation=make_audio_corrupt_validation_report(
                        "01.flac",
                        detail=decode_error,
                    ),
                    audio_error=decode_error,
                    folder_layout="flat",
                    audio_file_count=1,
                    filetype_band="flac",
                    lossless_candidate=True,
                    min_bitrate_kbps=900,
                    is_vbr=False,
                    spectral_audit=audit,
                ),
            ), patch("lib.import_preview.run_import_one") as mock_run:
                preview = measure_and_persist_candidate_evidence(
                    preview_db,  # pyright: ignore[reportArgumentType]
                    request_id=42,
                    path=source,
                    import_job_id=job.id,
                )

            self.assertEqual(preview.verdict, "evidence_ready")
            self.assertEqual(preview.decision, "audio_corrupt")
            mock_run.assert_not_called()
            evidence_id = db.get_import_job_candidate_evidence_id(job.id)
            self.assertIsNotNone(evidence_id)
            evidence = db.load_album_quality_evidence_by_id(evidence_id)
            assert evidence is not None
            self.assertTrue(evidence.audio_corrupt)
            self.assertEqual(evidence.audio_error, decode_error)
            self.assertEqual(
                [(file.relative_path, file.decode_ok) for file in evidence.files],
                [("01.flac", False)],
            )
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_zero_streaminfo_flac_persists_lossless_candidate_evidence(self):
        """Preview preserves the repaired private FLAC's lossless identity."""
        from pathlib import Path

        from lib.dispatch.types import ImportOneRun
        from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
        from lib.measurement import ExistingSpectralAuditLookup
        from scripts.import_preview_worker import _AutomationPreviewDB
        from tests.audio_fixtures import make_test_flac
        from tests.test_media_readiness import _zero_flac_duration_metadata

        db = self._db()
        self.assertTrue(db.reset_to_wanted(42, expected_status="unsearchable"))
        job = handoff_automation_owner(db, 42)
        lease = ExecutionLeaseSnapshot(
            host_boot_id="zero-streaminfo-preview", invocation_id="preview",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=7002, start_ticks=70002),
        )
        claimed = claim_next_import_preview_job(
            db, worker_id="preview", execution_lease=lease,
        )
        assert claimed is not None and claimed.id == job.id
        source = tempfile.mkdtemp()
        try:
            actual = os.path.join(source, "source.flac")
            track = os.path.join(source, "01 - Track.ogg")
            make_test_flac(actual, duration=1)
            os.replace(actual, track)
            _zero_flac_duration_metadata(Path(track))
            run = ImportOneRun(
                command=("import_one",), returncode=0, stdout="", stderr="",
                import_result=ImportResult(
                    decision="import",
                    source_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=700, avg_bitrate_kbps=700,
                        median_bitrate_kbps=700, format="FLAC",
                    ),
                ),
            )
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch("lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()):
                preview = measure_and_persist_candidate_evidence(
                    _AutomationPreviewDB(db, lease),  # pyright: ignore[reportArgumentType]
                    request_id=42, path=source, import_job_id=job.id,
                    run_import_fn=lambda **_kwargs: run,
                    existing_spectral_resolver=(
                        lambda _release_id: ExistingSpectralAuditLookup()
                    ),
                )

            self.assertEqual(preview.verdict, "evidence_ready")
            evidence_id = db.get_import_job_candidate_evidence_id(job.id)
            assert evidence_id is not None
            evidence = db.load_album_quality_evidence_by_id(evidence_id)
            assert evidence is not None
            self.assertEqual(evidence.measurement.format, "FLAC")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_production_preview_prepares_have_before_candidate_ready(self):
        order: list[str] = []

        class RecordingPipelineDB(FakePipelineDB):
            def claim_current_v0_research_attempt(
                self,
                *,
                request_id: int,
                expected_evidence_id: int,
                expected_snapshot_fingerprint: str,
            ) -> bool:
                order.append("prepare_have")
                super().claim_current_v0_research_attempt(
                    request_id=request_id,
                    expected_evidence_id=expected_evidence_id,
                    expected_snapshot_fingerprint=expected_snapshot_fingerprint,
                )
                # Model a concurrent preview winning the claim. The loader
                # must reload that committed marker without running ffmpeg;
                # this test is about orchestration order, not probe behavior.
                return False

        db = RecordingPipelineDB()
        request = self._db().get_request(42)
        assert request is not None
        db.seed_request(request)
        source = self._source_dir()

        def measure(*args, **kwargs):
            order.append("measure_candidate")
            return PreimportMeasurement(
                audio_corrupt=True,
                corrupt_files=["01.mp3"],
                audio_validation=make_audio_corrupt_validation_report(
                    "01.mp3",
                ),
                folder_layout="flat",
                audio_file_count=1,
            )

        def persist(*args, **kwargs):
            order.append("persist_candidate")
            return EvidenceBuildResult(
                make_album_quality_evidence(mb_release_id="candidate-ready"),
                "ready",
            )

        try:
            self._seed_current_without_v0(db, source)
            with patch(
                "lib.beets_db.BeetsDB",
                return_value=self._beets_current(source),
            ), patch(
                "lib.config.read_runtime_config",
                return_value=_preview_runtime_config(
                    beets_harness_path="/fake/harness/run_beets_harness.sh",
                    pipeline_db_enabled=True,
                ),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(filetype="mp3"),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                side_effect=measure,
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    persist_measurement_fn=persist,
                )

            self.assertEqual(result.verdict, "evidence_ready")
            self.assertEqual(
                order,
                ["prepare_have", "measure_candidate", "persist_candidate"],
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_audit_survives_harness_crash_and_no_json(self):
        from lib.dispatch.types import ImportOneRun
        db = self._db()
        source = self._source_dir()
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade="suspect", bitrate_kbps=128),
            existing=SpectralAnalysisDetail(
                attempted=True, error="existing decode failed"),
        )
        measurement = PreimportMeasurement(
            folder_layout="flat", audio_file_count=1,
            spectral_audit=audit,
        )
        common = (
            patch("lib.config.read_runtime_config", return_value=_preview_runtime_config(
                beets_harness_path="/fake/harness/run_beets_harness.sh",
                pipeline_db_enabled=True)),
            patch("lib.import_preview.inspect_local_files",
                  return_value=LocalFileInspection(filetype="mp3")),
            patch("lib.import_preview.measure_preimport_state",
                  return_value=measurement),
        )
        try:
            for decision, run_value in (
                ("harness_crashed", RuntimeError("harness exploded")),
                ("no_json_result", ImportOneRun(
                    command=(), returncode=1, stdout="",
                    stderr="no sentinel", import_result=None)),
            ):
                def run_import(
                    *args: Any,
                    run_value: ImportOneRun | Exception = run_value,
                    **kwargs: Any,
                ) -> ImportOneRun:
                    if isinstance(run_value, Exception):
                        raise run_value
                    return run_value

                with common[0], common[1], common[2]:
                    preview = measure_and_persist_candidate_evidence(
                        db, request_id=42, path=source,
                        run_import_fn=run_import,
                    )
                self.assertEqual(preview.decision, decision)
                assert preview.import_result is not None
                self.assertEqual(preview.import_result.spectral, audit)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_same_size_source_replacement_uses_private_snapshot_evidence(self):
        db = self._db()
        job = db.enqueue_import_job(
            "force_import",
            request_id=42,
            dedupe_key="force:42:/tmp/source",
            payload={"download_log_id": 1, "failed_path": "/tmp/source"},
        )
        source = self._source_dir()

        def run_preview(*args, **kwargs):
            with open(os.path.join(source, "01.mp3"), "r+b") as handle:
                handle.write(b"X" * len(b"not real audio but never inspected in this test"))
            return SimpleNamespace(
                import_result=ImportResult(
                    decision="import",
                    source_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=245,
                        avg_bitrate_kbps=245,
                        median_bitrate_kbps=245,
                        format="MP3",
                    ),
                )
            )

        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       side_effect=run_preview):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                    force=False,
                    import_job_id=job.id,
                    persist_candidate_evidence=True,
                )

            self.assertEqual(preview.verdict, "would_import")
            # Evidence addresses the bytes copied before the same-size
            # external replacement, so its identity cannot be spoofed by a
            # post-measurement pathname inventory.
            self.assertIsNotNone(db.get_import_job_candidate_evidence_id(job.id))
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_audio_corrupt_is_confident_reject_without_denylist_side_effects(self):
        """U6: preview surfaces the five folder/audio-integrity facts as a
        confident_reject. Spectral / codec rank / V0 are NEVER decided in
        preview — those live in the importer's
        ``full_pipeline_decision_from_evidence``. Preview must also NEVER
        touch the denylist (importer owns that on reject via U11).
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=128000,
                           is_vbr=False,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           audio_corrupt=True,
                           corrupt_files=["01.mp3"],
                           audio_validation=(
                               make_audio_corrupt_validation_report("01.mp3")
                           ),
                           folder_layout="flat",
                           audio_file_count=0,
                       )), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "confident_reject")
            self.assertTrue(preview.cleanup_eligible)
            self.assertEqual(preview.decision, "audio_corrupt")
            self.assertEqual(db.denylist, [])
            mock_run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_bad_audio_hash_is_confident_reject_without_denylist_side_effects(self):
        """U6: preview must surface ``bad_audio_hash`` as confident_reject
        without writing to the denylist. The importer's unified reject path
        (U11) owns the denylist write.
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=128000,
                           is_vbr=False,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           matched_bad_hash_id=7,
                           matched_bad_track_path="01.mp3",
                           folder_layout="flat",
                           audio_file_count=0,
                       )), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "confident_reject")
            self.assertTrue(preview.cleanup_eligible)
            self.assertEqual(preview.decision, "bad_audio_hash")
            self.assertEqual(db.denylist, [])
            mock_run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_corrupt_and_nested_agrees_with_the_evidence_decider_on_audio_corrupt(
        self,
    ):
        """C1 (issue #1355 item 1 residual): a candidate that is both
        corrupt and nested must agree with the real evidence decider
        (``candidate_preimport_reject_fact``, which both quality-decision
        twins call). Before this fix, ``preview_import_from_path`` rejected
        on ``inspection.has_nested_audio`` before measurement ever ran, so
        this exact world displayed ``nested_layout`` here while
        ``full_pipeline_decision_from_evidence`` denylisted it as
        ``audio_corrupt``.

        Also carries a matched bad-audio-hash fact (a third true fact,
        lower priority than both corrupt and nested) to prove the
        audio_corrupt detail composition isn't overwritten by the
        lower-priority branches beneath it.
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=128000,
                           is_vbr=False,
                           has_nested_audio=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           audio_corrupt=True,
                           corrupt_files=["sub/01.mp3"],
                           audio_validation=(
                               make_audio_corrupt_validation_report("sub/01.mp3")
                           ),
                           matched_bad_hash_id=99,
                           matched_bad_track_path="decoy.mp3",
                           folder_layout="nested",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "confident_reject")
            self.assertTrue(preview.cleanup_eligible)
            self.assertEqual(preview.decision, "audio_corrupt")
            self.assertEqual(preview.detail, "1 files failed ffmpeg decode")
            self.assertEqual(db.denylist, [])
            mock_run.assert_not_called()

            evidence = build_parity_candidate_evidence(
                is_flac=False, min_bitrate=128, is_cbr=False,
                audio_corrupt=True, folder_layout="nested",
                matched_bad_audio_hash_id=99,
                matched_bad_audio_hash_path="decoy.mp3",
            )
            self.assertEqual(
                candidate_preimport_reject_fact(evidence), preview.decision,
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_nested_only_still_reports_nested_layout_with_flatten_detail(self):
        """Must-still-work guard: a nested-but-not-corrupt candidate keeps
        reporting ``nested_layout`` (with the operator-facing "flatten the
        folder" detail) once nested rejection moved from an early
        ``inspection``-only check into the post-measurement four-fact block.
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=128000,
                           is_vbr=False,
                           has_nested_audio=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="nested",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "confident_reject")
            self.assertTrue(preview.cleanup_eligible)
            self.assertEqual(preview.decision, "nested_layout")
            self.assertEqual(
                preview.detail,
                "Audio files are in subdirectories — flatten the folder "
                "before import.",
            )
            self.assertEqual(db.denylist, [])
            mock_run.assert_not_called()

            evidence = build_parity_candidate_evidence(
                is_flac=False, min_bitrate=128, is_cbr=False,
                audio_corrupt=False, folder_layout="nested",
            )
            self.assertEqual(
                candidate_preimport_reject_fact(evidence), preview.decision,
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_measurement_crash_degrades_to_uncertain_instead_of_raising(self):
        """A crashing measurement (e.g. an unprobeable .m4a raising
        AudioCodecProbeError) must degrade to the measurement_failed
        preview verdict like the worker path — never escape as an
        exception the web route would surface as a 500.
        """
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="m4a",
                           min_bitrate_bps=256000,
                           is_vbr=False,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       side_effect=AudioCodecProbeError(
                           "ffprobe could not read 01.m4a")), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            self.assertEqual(preview.decision, "measurement_crashed")
            self.assertIn("AudioCodecProbeError", preview.detail or "")
            mock_run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_conversion_source_corruption_becomes_persisted_evidence(self):
        """A conversion-time decode failure rejoins the unified decider path."""
        db = self._db()
        download_log_id = db.log_download(request_id=42, outcome="failed")
        source = self._source_dir()
        report = AudioValidationReport(
            outcome="audio_corrupt",
            files_checked=1,
            files_failed=1,
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="01.mp3",
                    category="decode_error",
                    return_code=69,
                    stderr_excerpt="invalid frame",
                ),
            ],
        )
        result = ImportResult(
            decision="conversion_failed",
            error="1 conversion failed",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                format="MP3",
            ),
        )
        result.conversion.failed = 1
        result.conversion.source_validation = report
        result.conversion.source_validation_failed_paths = ["01.mp3"]
        run = ImportOneRun(
            command=("import_one.py",),
            returncode=1,
            stdout=result.to_sentinel_line(),
            stderr="one concise conversion summary",
            import_result=result,
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="mp3",
                    min_bitrate_bps=256000,
                    is_vbr=True,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                    filetype_band="mp3",
                    min_bitrate_kbps=256,
                ),
            ):
                preview = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    download_log_id=download_log_id,
                    run_import_fn=lambda **_kwargs: run,
                )

            self.assertEqual(preview.verdict, "evidence_ready")
            self.assertEqual(preview.decision, "audio_corrupt")
            evidence_id = db.get_download_log_candidate_evidence_id(
                download_log_id
            )
            loaded = db.load_album_quality_evidence_by_id(evidence_id)
            assert loaded is not None
            self.assertTrue(loaded.audio_corrupt)
            self.assertEqual(loaded.audio_validation, report)
            self.assertFalse(loaded.files[0].decode_ok)
            self.assertEqual(db.denylist, [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_conversion_world_failure_keeps_typed_measurement_audit(self):
        """An unavailable decoder is not persisted as bad content."""
        db = self._db()
        source = self._source_dir()
        report = AudioValidationReport(
            outcome="measurement_failed",
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="01.mp3",
                    category="process_unavailable",
                    stderr_excerpt="ffmpeg missing",
                ),
            ],
        )
        result = ImportResult(
            decision="conversion_failed",
            error="conversion failed",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                format="MP3",
            ),
        )
        result.conversion.failed = 1
        result.conversion.source_validation = report
        run = ImportOneRun(
            command=("import_one.py",),
            returncode=1,
            stdout=result.to_sentinel_line(),
            stderr="one concise conversion summary",
            import_result=result,
        )
        try:
            with patch(
                "lib.config.read_runtime_config",
                return_value=_preview_config(),
            ), patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(
                    filetype="mp3",
                    min_bitrate_bps=256000,
                    is_vbr=True,
                ),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=1,
                    filetype_band="mp3",
                    min_bitrate_kbps=256,
                ),
            ):
                preview = measure_and_persist_candidate_evidence(
                    db,
                    request_id=42,
                    path=source,
                    run_import_fn=lambda **_kwargs: run,
                )

            self.assertEqual(preview.verdict, "measurement_failed")
            assert preview.failure is not None
            self.assertEqual(preview.failure.audio_validation, report)
            self.assertEqual(db.album_quality_evidence, {})
            self.assertEqual(db.denylist, [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_preview_legacy_path_does_not_call_run_preimport_gates(self):
        """U6/U8 anti-regression: the legacy ``run_preimport_gates`` shim
        was deleted in U8. If a future change reintroduces it (in
        lib.measurement or as a re-export from lib.import_preview), this
        guard fires.
        """
        import lib.import_preview as ip
        import lib.measurement as pi
        self.assertFalse(
            hasattr(ip, "run_preimport_gates"),
            "lib.import_preview must not re-export run_preimport_gates — "
            "preview measures only",
        )
        self.assertFalse(
            hasattr(pi, "run_preimport_gates"),
            "lib.measurement must not export run_preimport_gates — the shim "
            "was deleted in U8",
        )

    def test_missing_path_is_uncertain_not_cleanup_eligible(self):
        preview = preview_import_from_path(
            self._db(),
            request_id=42,
            path="/tmp/definitely-missing-cratedigger-preview",
        )

        self.assertEqual(preview.verdict, "uncertain")
        self.assertEqual(preview.decision, "path_missing")
        self.assertFalse(preview.cleanup_eligible)

    def test_path_preview_keeps_import_that_quality_gate_would_requeue(self):
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=_preview_runtime_config(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=160000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.measure_preimport_state",
                       return_value=PreimportMeasurement(
                           folder_layout="flat",
                           audio_file_count=1,
                       )), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               source_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=160,
                                   avg_bitrate_kbps=160,
                                   median_bitrate_kbps=160,
                                   format="mp3",
                               ),
                           )
                       )):
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "would_import")
            self.assertEqual(preview.decision, "import")
            self.assertEqual(preview.reason, "requeue_upgrade")
            self.assertFalse(preview.cleanup_eligible)
            self.assertEqual(
                preview.stage_chain,
                ["stage2_import:import", "stage3_quality_gate:requeue_upgrade"],
            )
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)


if TYPE_CHECKING:
    from typing import cast

    from lib.import_preview import ImportPreviewDB as _PreviewDB
    from lib.pipeline_db import PipelineDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_preview_protocol: _PreviewDB = cast("PipelineDB", None)
    _fake_db_satisfies_preview_protocol: _PreviewDB = cast("FakePipelineDB", None)


class TestBadHashGateReachesPreviewLanes(unittest.TestCase):
    """The curator bad-rip hash gate must fire through BOTH preview lanes.

    Composition pin (widest-boundary rule): the gate's unit tests in
    ``tests/test_measurement.py`` drive ``_check_bad_audio_hashes`` with a
    live DB handle, but production's only ``measure_preimport_state``
    callers are the two preview lanes — so the lanes must actually pass
    their DB handle through as the bad-hash port. The pre-fix world passed
    ``db=None`` from both lanes, leaving 48 curator-reported hashes
    unreachable (0 matches ever recorded in live evidence/download_log).
    """

    def _seeded_world(self) -> tuple[FakePipelineDB, str, int]:
        """Request 42 + a source album whose track bytes are a reported rip."""
        from pathlib import Path

        from lib.audio_hash import hash_audio_content
        from lib.pipeline_db import BadAudioHashInput

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            artist_name="Artist",
            album_title="Album",
        ))
        fixture = os.path.join(
            os.path.dirname(__file__), "fixtures", "audio_hash",
            "sine_440.mp3",
        )
        digest = hash_audio_content(Path(fixture), "mp3")
        # A decoy row first, so the matching row's id is NOT the row count
        # add_bad_audio_hashes returns — the expected id must come from the
        # seeded row itself (reader finding on the first review round: a
        # mutant hardcoding bad_hash_id=1 survived the count binding).
        db.add_bad_audio_hashes(
            request_id=42,
            reported_username="curator",
            reason="unrelated rip",
            hashes=[BadAudioHashInput(
                hash_value=b"\x01" * len(digest), audio_format="mp3")],
        )
        db.add_bad_audio_hashes(
            request_id=42,
            reported_username="curator",
            reason="exemplar bad rip",
            hashes=[BadAudioHashInput(hash_value=digest, audio_format="mp3")],
        )
        bad_hash_id = db.bad_audio_hashes[-1].id
        self.assertGreater(bad_hash_id, 1)
        source = tempfile.mkdtemp(dir=_PREVIEW_SOURCE_ROOT)
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        shutil.copy(fixture, os.path.join(source, "01 - Track.mp3"))
        return db, source, bad_hash_id

    def _single_evidence_row(self, db: FakePipelineDB):
        rows = list(db.album_quality_evidence.values())
        self.assertEqual(len(rows), 1)
        return rows[0]

    def test_measure_and_persist_lane_rejects_matching_hash(self):
        from lib.measurement import ExistingSpectralAuditLookup

        db, source, bad_hash_id = self._seeded_world()
        download_log_id = db.log_download(42, outcome="rejected")
        with patch(
            "lib.config.read_runtime_config",
            return_value=_preview_runtime_config(pipeline_db_enabled=True),
        ), patch(
            "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()
        ), patch("lib.import_preview.run_import_one") as mock_run:
            preview = measure_and_persist_candidate_evidence(
                db,
                request_id=42,
                path=source,
                download_log_id=download_log_id,
                spectral_detail_analyzer=lambda _path: SpectralAnalysisDetail(
                    attempted=True, grade="genuine", bitrate_kbps=320,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                ),
                existing_spectral_resolver=(
                    lambda _release_id: ExistingSpectralAuditLookup()
                ),
            )

        self.assertEqual(preview.verdict, "evidence_ready")
        self.assertEqual(preview.decision, "bad_audio_hash")
        mock_run.assert_not_called()
        evidence = self._single_evidence_row(db)
        self.assertEqual(evidence.matched_bad_audio_hash_id, bad_hash_id)
        assert evidence.matched_bad_audio_hash_path is not None
        self.assertTrue(
            evidence.matched_bad_audio_hash_path.endswith("01 - Track.mp3"))

    def test_classify_lane_rejects_matching_hash(self):
        db, source, bad_hash_id = self._seeded_world()
        with patch(
            "lib.config.read_runtime_config",
            return_value=_preview_runtime_config(pipeline_db_enabled=True),
        ), patch(
            "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()
        ), patch("lib.import_preview.run_import_one") as mock_run:
            preview = preview_import_from_path(
                db,
                request_id=42,
                path=source,
            )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertEqual(preview.decision, "bad_audio_hash")
        self.assertTrue(preview.cleanup_eligible)
        assert preview.detail is not None
        self.assertIn(f"bad_audio_hash id={bad_hash_id}", preview.detail)
        mock_run.assert_not_called()


class TestMeasurementCollaboratorBoundary(unittest.TestCase):
    """A broken measurement collaborator maps to ``measurement_failed``.

    Pins the cd-rip verifier's lazy import staying INSIDE
    ``_measure_lane_world``'s collaborator-failure envelope: a broken
    world (numpy/verifier import failure) must surface as a
    ``measurement_failed`` result the caller re-derives from — never an
    exception escaping the lane (invariant 11). The skeleton extraction
    briefly moved this import outside the envelope; this is the pin the
    correction shipped without.
    """

    def test_verifier_import_failure_maps_to_measurement_failed(self):
        import types

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            artist_name="Artist",
            album_title="Album",
        ))
        fixture = os.path.join(
            os.path.dirname(__file__), "fixtures", "audio_hash",
            "sine_440.mp3",
        )
        source = tempfile.mkdtemp(dir=_PREVIEW_SOURCE_ROOT)
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        shutil.copy(fixture, os.path.join(source, "01 - Track.mp3"))

        broken = types.ModuleType("lib.cd_rip_verifier")
        with patch(
            "lib.config.read_runtime_config",
            return_value=_preview_runtime_config(pipeline_db_enabled=True),
        ), patch(
            "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()
        ), patch.dict(
            "sys.modules", {"lib.cd_rip_verifier": broken}
        ), patch("lib.import_preview.run_import_one") as mock_run:
            preview = measure_and_persist_candidate_evidence(
                db,
                request_id=42,
                path=source,
            )

        self.assertEqual(preview.verdict, "measurement_failed")
        self.assertEqual(preview.decision, "measurement_crashed")
        assert preview.detail is not None
        self.assertTrue(preview.detail.startswith("ImportError"))
        mock_run.assert_not_called()


class TestLanePolicySeam(unittest.TestCase):
    """Seam pins for the extracted lane skeleton's policy split (#1278).

    The mutant-runner round on the skeleton extraction proved most of the
    lane policy was constrained by nothing — the split existed only in
    prose. These are seam/adapter tests: they pin the exact arguments each
    lane hands the shared stages, so a wiring mutant (dropping a capture
    fn, swapping a loader, widening the injected seam's call shape) fails
    here instead of shipping green. Everything is driven through the
    sanctioned kwarg-DI seams (``measure_fn``, ``run_import_fn``,
    ``current_evidence_loader``) — no owned-function patches.
    """

    def _world(self) -> tuple[FakePipelineDB, str]:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            artist_name="Artist",
            album_title="Album",
        ))
        fixture = os.path.join(
            os.path.dirname(__file__), "fixtures", "audio_hash",
            "sine_440.mp3",
        )
        source = tempfile.mkdtemp(dir=_PREVIEW_SOURCE_ROOT)
        self.addCleanup(shutil.rmtree, source, ignore_errors=True)
        shutil.copy(fixture, os.path.join(source, "01 - Track.mp3"))
        return db, source

    def _installed_album(self) -> str:
        """A separate installed-album dir so HAVE evidence is content-
        addressed apart from the candidate source (a shared fingerprint
        would let candidate persistence write over the HAVE row and
        satisfy refresh assertions for the wrong reason)."""
        fixture = os.path.join(
            os.path.dirname(__file__), "fixtures", "audio_hash",
            "sine_440.ogg",
        )
        installed = tempfile.mkdtemp(dir=_PREVIEW_SOURCE_ROOT)
        self.addCleanup(shutil.rmtree, installed, ignore_errors=True)
        shutil.copy(fixture, os.path.join(installed, "01 - Track.ogg"))
        return installed

    def _benign_measurement(self, **overrides: object) -> PreimportMeasurement:
        import msgspec

        base = PreimportMeasurement(
            audio_file_count=1,
            filetype_band="mp3",
            min_bitrate_kbps=320,
            is_vbr=False,
            spectral_audit=SpectralDetail(
                candidate=SpectralAnalysisDetail(
                    attempted=True, grade="genuine", bitrate_kbps=320,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                ),
                existing=SpectralAnalysisDetail(attempted=False),
            ),
        )
        if not overrides:
            return base
        return msgspec.structs.replace(base, **overrides)

    def _rejecting_measurement(
        self, **overrides: object,
    ) -> PreimportMeasurement:
        return self._benign_measurement(
            audio_corrupt=True,
            corrupt_files=["01 - Track.mp3"],
            audio_validation=make_audio_corrupt_validation_report(
                "01 - Track.mp3", detail="decode error",
            ),
            audio_error="decode error",
            **overrides,
        )

    def _harness_must_not_run(self, **_kwargs: object) -> ImportOneRun:
        self.fail("the dry-run harness must not run in this world")

    def test_lane_measurement_policy_kwargs(self):
        """Each lane hands measurement exactly its documented policy."""
        from lib.cd_rip_verifier import verify_cd_rip
        from lib.measurement import ExistingSpectralAuditLookup, measure_aac_lattice

        def analyzer(_path: str) -> SpectralAnalysisDetail:
            return SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=320,
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            )

        def resolver(_release_id: str) -> ExistingSpectralAuditLookup:
            return ExistingSpectralAuditLookup()

        rejecting = self._rejecting_measurement()
        for lane in ("worker", "classify"):
            with self.subTest(lane=lane):
                db, source = self._world()
                # Stale linked HAVE evidence with a real persisted grade,
                # while beets holds no album: the loader answers
                # ``empty_current`` and the lanes must RESET the stale
                # persisted detail to attempted=False rather than letting
                # it leak into measurement (mutant-runner survivor C).
                installed = self._installed_album()
                stale = make_album_quality_evidence(
                    mb_release_id="mbid-42",
                    source_path=installed,
                    files=snapshot_audio_files(installed),
                    measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=96, avg_bitrate_kbps=96,
                        median_bitrate_kbps=96, format="MP3",
                        spectral_grade="suspect", spectral_bitrate_kbps=96,
                    ),
                )
                db.upsert_album_quality_evidence(stale)
                stored_stale = db.find_album_quality_evidence(
                    mb_release_id=stale.mb_release_id,
                    snapshot_fingerprint=stale.snapshot_fingerprint,
                )
                assert stored_stale is not None and stored_stale.id is not None
                db.set_request_current_evidence(42, stored_stale.id)
                download_log_id = db.log_download(42, outcome="rejected")
                recorded: dict[str, object] = {}

                def record_measure(
                    _sink: dict[str, object] = recorded,
                    **kwargs: object,
                ) -> PreimportMeasurement:
                    _sink.clear()
                    _sink.update(kwargs)
                    return rejecting

                with patch(
                    "lib.config.read_runtime_config",
                    return_value=_preview_runtime_config(
                        pipeline_db_enabled=True),
                ), patch(
                    "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()
                ):
                    if lane == "worker":
                        measure_and_persist_candidate_evidence(
                            db,
                            request_id=42,
                            path=source,
                            download_log_id=download_log_id,
                            spectral_detail_analyzer=analyzer,
                            existing_spectral_resolver=resolver,
                            measure_fn=record_measure,
                            run_import_fn=self._harness_must_not_run,
                        )
                    else:
                        preview_import_from_path(
                            db, request_id=42, path=source,
                            measure_fn=record_measure,
                        )

                self.assertIs(recorded["bad_hash_db"], db)
                self.assertEqual(
                    recorded["existing_spectral_evidence"],
                    SpectralAnalysisDetail(attempted=False),
                    "empty-current world must reset stale persisted detail",
                )
                self.assertIs(recorded["reuse_existing_spectral_evidence"], False)
                self.assertIs(
                    recorded["preserve_existing_source_spectral"], False)
                self.assertIsNotNone(recorded["precomputed_inspection"])
                if lane == "worker":
                    self.assertIs(recorded["spectral_detail_analyzer"], analyzer)
                    self.assertIs(recorded["existing_spectral_resolver"], resolver)
                    self.assertIs(
                        recorded["aac_lattice_measure_fn"], measure_aac_lattice)
                    self.assertIs(recorded["cd_rip_verify_fn"], verify_cd_rip)
                else:
                    self.assertIsNone(recorded["spectral_detail_analyzer"])
                    self.assertIsNone(recorded["existing_spectral_resolver"])
                    self.assertIsNone(recorded["aac_lattice_measure_fn"])
                    self.assertIsNone(recorded["cd_rip_verify_fn"])

    def test_reusable_current_evidence_sets_the_reuse_flag(self):
        """A decision-usable current-generation HAVE grade reaches
        measurement as ``reuse_existing_spectral_evidence=True`` (the
        re-project-without-rescanning wiring; mutant-runner survivor #2)."""
        db, source = self._world()
        installed = self._installed_album()
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            source_path=installed,
            files=snapshot_audio_files(installed),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=96, avg_bitrate_kbps=96,
                median_bitrate_kbps=96, format="MP3",
                spectral_grade="suspect", spectral_bitrate_kbps=96,
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(42, stored.id)
        beets = FakeBeetsDB(library_root=installed)
        from lib.beets_db import AlbumInfo
        beets.set_album_info("mbid-42", AlbumInfo(
            album_id=1, track_count=1, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, is_cbr=True,
            album_path=installed, format="MP3",
        ))
        recorded: dict[str, object] = {}

        def record_measure(**kwargs: object) -> PreimportMeasurement:
            recorded.update(kwargs)
            return self._rejecting_measurement()

        with patch(
            "lib.config.read_runtime_config",
            return_value=_preview_runtime_config(pipeline_db_enabled=True),
        ), patch(
            "lib.beets_db.BeetsDB", lambda _b=beets, **_kwargs: _b
        ):
            preview_import_from_path(
                db, request_id=42, path=source, measure_fn=record_measure,
            )

        self.assertIs(recorded["reuse_existing_spectral_evidence"], True)
        reused = recorded["existing_spectral_evidence"]
        assert isinstance(reused, SpectralAnalysisDetail)
        self.assertEqual(reused.grade, "suspect")

    def test_worker_lane_failure_source_path_mapping(self):
        """AudioValidation failure reports the raw path; generic reports the
        operator-facing display path (the preserved lane A asymmetry)."""
        from lib.quality import AudioValidationMeasurementError

        report = AudioValidationReport(
            outcome="measurement_failed",
            diagnostics=[AudioToolDiagnostic(
                category="decode_timeout",
                relative_path="01 - Track.mp3",
            )],
        )
        cases = [
            (
                AudioValidationMeasurementError(report),
                "raw",
            ),
            (RuntimeError("collaborator failed"), "display"),
        ]
        for exc, expected in cases:
            with self.subTest(exc=type(exc).__name__):
                db, source = self._world()

                def raise_exc(
                    _exc: BaseException = exc, **_kwargs: object,
                ) -> PreimportMeasurement:
                    raise _exc

                with patch(
                    "lib.config.read_runtime_config",
                    return_value=_preview_runtime_config(
                        pipeline_db_enabled=True),
                ), patch(
                    "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()
                ):
                    preview = measure_and_persist_candidate_evidence(
                        db,
                        request_id=42,
                        path=source,
                        source_display_path="/audit/display-path",
                        measure_fn=raise_exc,
                        run_import_fn=self._harness_must_not_run,
                    )
                self.assertEqual(preview.verdict, "measurement_failed")
                self.assertEqual(
                    preview.source_path,
                    source if expected == "raw" else "/audit/display-path",
                )

    def test_worker_lane_harness_argument_seam(self):
        """The dry-run harness receives the lane's real decision inputs, and
        the injected seam keeps its historical no-token call shape."""
        from lib.quality import EVIDENCE_SUBJECT_SOURCE, AlbumQualityV0Metric

        db, source = self._world()
        download_log_id = db.log_download(42, outcome="rejected")
        installed = self._installed_album()
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-42",
            source_path=installed,
            files=snapshot_audio_files(installed),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=317,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=187,
                avg_bitrate_kbps=213,
                median_bitrate_kbps=210,
                subject=EVIDENCE_SUBJECT_SOURCE,
                provenance="carried",
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None

        def loader(_db: object, **_kwargs: object) -> EvidenceBuildResult:
            return EvidenceBuildResult(
                stored, "ready", None, current_album_path=installed,
            )

        recorded: dict[str, object] = {}

        def run_import_fn(**kwargs: object) -> ImportOneRun:
            recorded.update(kwargs)
            return ImportOneRun(
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
            "lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()
        ):
            preview = measure_and_persist_candidate_evidence(
                db,
                request_id=42,
                path=source,
                force=False,
                download_log_id=download_log_id,
                run_import_fn=run_import_fn,
                current_evidence_loader=loader,
                measure_fn=lambda **_kwargs: self._benign_measurement(),
                runtime_config=_preview_runtime_config(
                    pipeline_db_enabled=True),
            )

        self.assertEqual(preview.verdict, "evidence_ready")
        self.assertIs(recorded["force"], False)
        self.assertEqual(recorded["override_min_bitrate"], 317)
        self.assertIsNotNone(
            recorded["existing_v0_probe"],
            "a current evidence row with a V0 metric must reach the harness",
        )
        # quality_evidence_action_file is None for a lossy world by design;
        # the lossless sidecar handoff is patrolled by
        # tests/test_preview_manifest_generated.py (#859).
        self.assertIn("quality_evidence_action_file", recorded)
        self.assertNotIn(
            "cancellation_token", recorded,
            "the injected seam keeps its historical no-token call shape",
        )

    def test_classify_lane_skips_enrichment_but_refreshes_have(self):
        """The loader split, pinned in both directions: classify never runs
        V0 enrichment, yet still refreshes exact-current HAVE spectral."""
        from lib.current_library_evidence import load_current_evidence_for_preview
        from lib.quality import SpectralMeasurement

        fresh_existing = SpectralAnalysisDetail(
            attempted=True, grade="genuine", bitrate_kbps=320,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )
        for lane in ("classify", "worker"):
            with self.subTest(lane=lane):
                db, source = self._world()
                installed = self._installed_album()
                # Seed a stale persisted grade at a STALE analyzer
                # generation so it is not reusable and the fresh-audit
                # refresh is both required and observable — the builder's
                # defaults (grade "genuine", current generation) made an
                # earlier version of this assertion vacuous
                # (mutant-runner survivor #12).
                evidence = make_album_quality_evidence(
                    mb_release_id="mbid-42",
                    source_path=installed,
                    files=snapshot_audio_files(installed),
                    measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=96, avg_bitrate_kbps=96,
                        median_bitrate_kbps=96, format="MP3",
                        spectral_grade="suspect", spectral_bitrate_kbps=96,
                        spectral_measurement_version=(
                            SPECTRAL_MEASUREMENT_VERSION - 1
                        ),
                    ),
                    preserve_spectral_measurement_version=True,
                )
                db.upsert_album_quality_evidence(evidence)
                stored = db.find_album_quality_evidence(
                    mb_release_id=evidence.mb_release_id,
                    snapshot_fingerprint=evidence.snapshot_fingerprint,
                )
                assert stored is not None and stored.id is not None
                db.set_request_current_evidence(42, stored.id)
                beets = FakeBeetsDB(library_root=installed)
                from lib.beets_db import AlbumInfo
                beets.set_album_info("mbid-42", AlbumInfo(
                    album_id=1, track_count=1, min_bitrate_kbps=320,
                    avg_bitrate_kbps=320, is_cbr=True,
                    album_path=installed, format="MP3",
                ))
                measurement = self._rejecting_measurement(
                    existing_spectral=SpectralMeasurement(
                        grade="genuine", bitrate_kbps=320),
                    existing_spectral_path=installed,
                    spectral_audit=SpectralDetail(
                        candidate=SpectralAnalysisDetail(
                            attempted=True, grade="genuine",
                            bitrate_kbps=320,
                            spectral_measurement_version=(
                                SPECTRAL_MEASUREMENT_VERSION
                            ),
                        ),
                        existing=fresh_existing,
                    ),
                )
                enrich_calls: list[int] = []
                stored_ready = EvidenceBuildResult(
                    stored, "ready", None, current_album_path=installed,
                )

                def record_enrich(
                    *_args: object,
                    _calls: list[int] = enrich_calls,
                    _ready: EvidenceBuildResult = stored_ready,
                    **_kwargs: object,
                ) -> EvidenceBuildResult:
                    _calls.append(1)
                    return _ready

                def enriching_loader(
                    inner_db: FakePipelineDB,
                    *,
                    request_id: int,
                    mb_release_id: str,
                    quality_ranks: QualityRankConfig,
                    beets_library_root: str,
                    preloaded_evidence: object,
                ) -> EvidenceBuildResult:
                    del preloaded_evidence
                    return load_current_evidence_for_preview(
                        inner_db,
                        request_id=request_id,
                        mb_release_id=mb_release_id,
                        quality_ranks=quality_ranks,
                        beets_library_root=beets_library_root,
                        preloaded_evidence=None,
                        enrich_current_fn=record_enrich,
                    )

                with patch(
                    "lib.config.read_runtime_config",
                    return_value=_preview_runtime_config(
                        pipeline_db_enabled=True),
                ), patch(
                    "lib.beets_db.BeetsDB", lambda _b=beets, **_kwargs: _b
                ):
                    if lane == "classify":
                        preview_import_from_path(
                            db, request_id=42, path=source,
                            measure_fn=lambda _m=measurement, **_kwargs: _m,
                        )
                    else:
                        measure_and_persist_candidate_evidence(
                            db,
                            request_id=42,
                            path=source,
                            download_log_id=db.log_download(
                                42, outcome="rejected"),
                            measure_fn=lambda _m=measurement, **_kwargs: _m,
                            current_evidence_loader=enriching_loader,
                            run_import_fn=self._harness_must_not_run,
                        )
                refreshed = db.load_album_quality_evidence_by_id(stored.id)
                assert refreshed is not None
                if lane == "classify":
                    # Authorize-only: enrichment's research claim never
                    # fires, so the claim's own durable mark stays clear.
                    self.assertFalse(
                        refreshed.on_disk_v0_research_attempted,
                        "classify lane must not run V0 enrichment",
                    )
                else:
                    self.assertEqual(enrich_calls, [1])
                self.assertEqual(
                    refreshed.measurement.spectral_grade, "genuine",
                    f"{lane} lane must refresh exact-current HAVE spectral",
                )


class TestOwnedProcessingNormalization(unittest.TestCase):
    """Issue #853: repair belongs after private processing publication."""

    def test_repaired_processing_album_persists_matching_evidence(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=853, mb_release_id="issue-853"))
        album = tempfile.mkdtemp(
            prefix="issue-853-",
            dir=os.path.join(_PREVIEW_PROCESSING_ROOT, "albums"),
        )
        track = os.path.join(album, "01.mp3")
        with open(track, "wb") as handle:
            handle.write(b"unrepaired")
        persisted_files: list[object] = []

        def repair(path: str) -> None:
            self.assertEqual(path, album)
            with open(track, "wb") as handle:
                handle.write(b"repaired")

        try:
            with patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(filetype="mp3"),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    audio_corrupt=True,
                    folder_layout="flat",
                    audio_file_count=1,
                ),
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=853,
                    path=album,
                    runtime_config=_preview_runtime_config(),
                    current_evidence_loader=lambda *_args, **_kwargs: EvidenceBuildResult(
                        None, "empty_current",
                    ),
                    persist_measurement_fn=lambda *_args, **kwargs: (
                        persisted_files.extend(kwargs["files"])
                        or EvidenceBuildResult(
                            make_album_quality_evidence(mb_release_id="issue-853"),
                            "ready",
                        )
                    ),
                    repair_fn=repair,
                )
            self.assertEqual(result.verdict, "evidence_ready")
            with open(track, "rb") as handle:
                self.assertEqual(handle.read(), b"repaired")
            self.assertEqual(len(persisted_files), 1)
            self.assertEqual(
                getattr(persisted_files[0], "size_bytes"),  # noqa: B009 - callback payload is object-typed
                len(b"repaired"),
            )
        finally:
            shutil.rmtree(album, ignore_errors=True)



class TestPreviewDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy ImportPreviewDB."""

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.import_preview import ImportPreviewDB
        from lib.pipeline_db import PipelineDB

        self.assertTrue(issubclass(PipelineDB, ImportPreviewDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.import_preview import ImportPreviewDB

        self.assertTrue(issubclass(FakePipelineDB, ImportPreviewDB))

    def test_preview_protocol_extends_evidence_protocol(self) -> None:
        """Preview forwards its handle into the evidence persisters."""
        from lib.import_preview import ImportPreviewDB
        from lib.quality_evidence import QualityEvidenceDB

        self.assertTrue(issubclass(ImportPreviewDB, QualityEvidenceDB))


if __name__ == "__main__":
    unittest.main()
