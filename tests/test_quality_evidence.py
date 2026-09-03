"""Tests for album-quality evidence construction helpers.

Migration 021 re-keyed evidence from ``(owner_type, owner_id)`` to
``(mb_release_id, snapshot_fingerprint)``. These tests exercise the new
content-addressed writers and the FK-chain readers.
"""

from __future__ import annotations

import copy
import os
import shutil
import tempfile
import unittest
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar
from unittest.mock import MagicMock

import msgspec

from lib.beets_db import AlbumInfo
from lib.measurement import PreimportMeasurement
from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    AccurateRipBitMatch,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    CdRipBitVerification,
    CdTocIdentity,
    ImportResult,
    SpectralAnalysisDetail,
    SpectralDetail,
    V0ProbeEvidence,
    VerifiedLosslessProof,
    full_pipeline_decision_from_evidence,
)
from lib.quality_evidence import (
    CandidateEvidencePersistenceReceipt,
    audio_snapshot_matches,
    backfill_current_evidence_from_album_info,
    candidate_evidence_from_persistence_receipt,
    candidate_evidence_persistence_receipt_semantic_error,
    current_evidence_preserves_source_spectral,
    current_evidence_rebuild_reasons,
    current_spectral_evidence_policy_usable,
    evidence_from_album_info,
    evidence_from_import_result,
    evidence_from_measurement,
    persist_candidate_evidence_from_measurement,
    propagate_candidate_evidence_to_current,
    snapshot_audio_files,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.evidence_helpers import (
    make_album_quality_evidence,
    make_audio_corrupt_validation_report,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestQualityEvidenceConstruction(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        with open(os.path.join(self.root, "02.mp3"), "wb") as handle:
            handle.write(b"audio 2")
        with open(os.path.join(self.root, "01.mp3"), "wb") as handle:
            handle.write(b"audio 1")

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_persistence_receipt_projection_rejects_forged_not_attempted_tuple(
        self,
    ) -> None:
        """#1030: a typed receipt is not trusted merely because it decoded."""
        evidence = make_album_quality_evidence(
            source_path=self.root,
            files=snapshot_audio_files(self.root),
        )
        forged = CandidateEvidencePersistenceReceipt(
            evidence_id=1,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
            spectral_write_intent="merge",
            spectral_outcome="not_attempted",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )

        with self.assertRaisesRegex(ValueError, "receipt.*semantic"):
            candidate_evidence_from_persistence_receipt(evidence, forged)

    def test_failed_and_empty_receipts_drop_analyzer_capture_passengers(self) -> None:
        """#1030: no tuple means no generation or capture metadata either."""
        for outcome, error in (("failed", "analyzer failed"), ("empty", None)):
            with self.subTest(outcome=outcome):
                db = FakePipelineDB()
                db.seed_request(make_request_row(id=42, mb_release_id="release-1"))
                download_log_id = db.log_download(
                    request_id=42,
                    outcome="rejected",
                )
                detail = SpectralAnalysisDetail(
                    attempted=True,
                    error=error,
                    cliff_hz=15_500,
                    codec_family="mp3",
                    ultrasonic_deficit_db=12.0,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                )
                files = snapshot_audio_files(self.root)
                persisted = persist_candidate_evidence_from_measurement(
                    db,
                    mb_release_id="release-1",
                    source_path=self.root,
                    measurement=PreimportMeasurement(
                        min_bitrate_kbps=128,
                        audio_file_count=len(files),
                        filetype_band="mp3",
                        spectral_audit=SpectralDetail(candidate=detail),
                    ),
                    download_log_id=download_log_id,
                    files=files,
                )

                self.assertEqual(persisted.status, "ready", persisted.reason)
                receipt = persisted.persistence_receipt
                assert receipt is not None
                self.assertEqual(receipt.spectral_outcome, outcome)
                self.assertTrue(all(
                    getattr(receipt, field) is None
                    for field in (
                        "spectral_grade",
                        "spectral_bitrate_kbps",
                        "spectral_subject",
                        "spectral_provenance",
                        "cliff_hz",
                        "codec_family",
                        "ultrasonic_deficit_db",
                        "spectral_measurement_version",
                    )
                ))

    def test_analyzer_error_grade_is_a_failed_attempt_without_a_tuple(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="release-1"))
        download_log_id = db.log_download(request_id=42, outcome="rejected")
        files = snapshot_audio_files(self.root)

        persisted = persist_candidate_evidence_from_measurement(
            db,
            mb_release_id="release-1",
            source_path=self.root,
            measurement=PreimportMeasurement(
                min_bitrate_kbps=128,
                audio_file_count=len(files),
                filetype_band="mp3",
                spectral_audit=SpectralDetail(
                    candidate=SpectralAnalysisDetail(
                        attempted=True,
                        grade="error",
                        spectral_measurement_version=(
                            SPECTRAL_MEASUREMENT_VERSION
                        ),
                    )
                ),
            ),
            download_log_id=download_log_id,
            files=files,
        )

        self.assertEqual(persisted.status, "ready", persisted.reason)
        receipt = persisted.persistence_receipt
        assert receipt is not None
        self.assertEqual(receipt.spectral_outcome, "failed")
        self.assertIsNone(receipt.spectral_grade)
        self.assertIsNone(receipt.spectral_measurement_version)

    def test_persistence_receipt_semantic_state_machine_is_exact(self) -> None:
        base = CandidateEvidencePersistenceReceipt(
            evidence_id=1,
            snapshot_fingerprint="snapshot",
            spectral_write_intent="merge",
            spectral_outcome="not_attempted",
        )
        measured = msgspec.structs.replace(
            base,
            spectral_write_intent="replace",
            spectral_outcome="measured",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )
        valid = [
            base,
            measured,
            msgspec.structs.replace(
                base,
                spectral_write_intent="replace",
                spectral_outcome="failed",
            ),
            msgspec.structs.replace(
                base,
                spectral_write_intent="replace",
                spectral_outcome="empty",
            ),
        ]
        for receipt in valid:
            with self.subTest(valid=receipt):
                self.assertIsNone(
                    candidate_evidence_persistence_receipt_semantic_error(
                        receipt
                    )
                )

        float_generation = CandidateEvidencePersistenceReceipt(
            evidence_id=1,
            snapshot_fingerprint="snapshot",
            spectral_write_intent="replace",
            spectral_outcome="measured",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            spectral_measurement_version=float(  # pyright: ignore[reportArgumentType]
                SPECTRAL_MEASUREMENT_VERSION
            ),
        )
        invalid_codec = CandidateEvidencePersistenceReceipt(
            evidence_id=1,
            snapshot_fingerprint="snapshot",
            spectral_write_intent="replace",
            spectral_outcome="measured",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            codec_family="invalid",  # pyright: ignore[reportArgumentType]
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )
        invalid = [
            msgspec.structs.replace(base, evidence_id=True),
            msgspec.structs.replace(base, spectral_outcome="measured"),
            msgspec.structs.replace(base, spectral_outcome="failed"),
            msgspec.structs.replace(base, spectral_outcome="empty"),
            msgspec.structs.replace(
                base,
                spectral_write_intent="replace",
            ),
            msgspec.structs.replace(base, spectral_grade="genuine"),
            msgspec.structs.replace(measured, spectral_grade="error"),
            msgspec.structs.replace(measured, spectral_subject="installed"),
            msgspec.structs.replace(measured, spectral_provenance="carried"),
            msgspec.structs.replace(
                measured,
                spectral_measurement_version=None,
            ),
            float_generation,
            invalid_codec,
        ]
        for receipt in invalid:
            with self.subTest(invalid=receipt):
                self.assertIsNotNone(
                    candidate_evidence_persistence_receipt_semantic_error(
                        receipt
                    )
                )

    def test_invalid_attempt_does_not_mutate_same_address_or_candidate_fk(
        self,
    ) -> None:
        """Semantic refusal precedes canonical upsert and owner-FK writes."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="release-1"))
        download_log_id = db.log_download(request_id=42, outcome="rejected")
        files = snapshot_audio_files(self.root)
        canonical = make_album_quality_evidence(
            mb_release_id="release-1",
            source_path=self.root,
            files=files,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                format="MP3",
                spectral_grade="suspect",
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
        )
        db.upsert_album_quality_evidence(canonical)
        before = db.find_album_quality_evidence(
            mb_release_id="release-1",
            snapshot_fingerprint=canonical.snapshot_fingerprint,
        )
        assert before is not None and before.id is not None
        db.set_download_log_candidate_evidence(download_log_id, before.id)
        job = db.enqueue_import_job(
            "force_import",
            request_id=42,
            payload={
                "download_log_id": download_log_id,
                "failed_path": self.root,
            },
        )
        self.assertIsNone(db.get_import_job_candidate_evidence_id(job.id))

        refused = persist_candidate_evidence_from_measurement(
            db,
            mb_release_id="release-1",
            source_path=self.root,
            measurement=PreimportMeasurement(
                min_bitrate_kbps=128,
                audio_file_count=len(files),
                filetype_band="mp3",
                spectral_audit=SpectralDetail(
                    candidate=SpectralAnalysisDetail(
                        attempted=True,
                        grade="alien",
                        spectral_measurement_version=(
                            SPECTRAL_MEASUREMENT_VERSION
                        ),
                    )
                ),
            ),
            download_log_id=download_log_id,
            import_job_id=job.id,
            files=files,
        )

        self.assertEqual(refused.status, "failed")
        self.assertIn("receipt semantic", refused.reason or "")
        after = db.find_album_quality_evidence(
            mb_release_id="release-1",
            snapshot_fingerprint=canonical.snapshot_fingerprint,
        )
        assert after is not None
        self.assertEqual(after.id, before.id)
        self.assertEqual(after.measurement.spectral_grade, "suspect")
        self.assertEqual(
            db.get_download_log_candidate_evidence_id(download_log_id),
            before.id,
        )
        self.assertIsNone(db.get_import_job_candidate_evidence_id(job.id))

    def test_legacy_v0_subject_is_rejected_at_strict_wire_boundary(self):
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {
                    "subject": "lossless_source",
                    "provenance": "measured",
                    "avg_bitrate_kbps": 245,
                },
                type=AlbumQualityV0Metric,
                strict=True,
            )

    def test_out_of_vocabulary_codec_family_is_rejected_at_strict_wire_boundary(
        self,
    ):
        """issue #829 Phase 5 round 3 review finding G: ``CodecFamily`` is a
        wire-boundary ``Literal`` over exactly six values
        (mp3/aac/opus/vorbis/lossless/other, per
        ``lib.quality.evidence_types.CodecFamily``). ``"wma"`` is a real
        format this pipeline handles but deliberately NOT one of the six
        measured families (``codec_family_from_extension`` maps it to
        "other") — feeding it at the strict msgspec boundary must raise,
        per ``.claude/rules/code-quality.md`` § Wire-boundary types."""
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {"codec_family": "wma"},
                type=AudioQualityMeasurement,
                strict=True,
            )

    def test_installed_carried_facts_are_invalid_v4_evidence(self):
        evidence = make_album_quality_evidence(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="carried",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="installed",
                provenance="carried",
                avg_bitrate_kbps=245,
            ),
        )

        errors = evidence.storage_validation_errors()
        self.assertIn("installed spectral evidence cannot be carried", errors)
        self.assertIn("installed v0 evidence cannot be carried", errors)

    def test_import_result_builds_neutral_candidate_evidence(self):
        result = evidence_from_import_result(
            mb_release_id="mb-candidate-1",
            source_path=self.root,
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=237,
                    avg_bitrate_kbps=245,
                    median_bitrate_kbps=244,
                    format="FLAC",
                ),
                v0_probe=V0ProbeEvidence(
                    kind="lossless_source_v0",
                    avg_bitrate_kbps=245,
                ),
            ),
        )

        self.assertTrue(result.available)
        assert result.evidence is not None
        self.assertEqual(
            [file.relative_path for file in result.evidence.files],
            ["01.mp3", "02.mp3"],
        )
        self.assertEqual(result.evidence.mb_release_id, "mb-candidate-1")
        self.assertTrue(result.evidence.snapshot_fingerprint)
        assert result.evidence.v0_metric is not None
        self.assertEqual(result.evidence.v0_metric.subject, "source")

    def test_non_lossless_candidate_keeps_source_and_research_probe_separate(self):
        result = evidence_from_import_result(
            mb_release_id="mb-mp3-1",
            source_path=self.root,
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=237,
                    avg_bitrate_kbps=247,
                    median_bitrate_kbps=246,
                    format="Opus",
                ),
                v0_probe=V0ProbeEvidence(
                    kind="native_lossy_research_v0",
                    min_bitrate_kbps=180,
                    avg_bitrate_kbps=211,
                    median_bitrate_kbps=214,
                ),
            ),
        )

        self.assertTrue(result.available)
        assert result.evidence is not None
        assert result.evidence.v0_metric is not None, (
            "The actual research probe must persist in typed v0 evidence."
        )
        self.assertEqual(
            result.evidence.v0_metric.subject,
            "installed",
        )
        self.assertEqual(result.evidence.measurement.format, "Opus")
        self.assertEqual(result.evidence.measurement.min_bitrate_kbps, 237)
        self.assertEqual(result.evidence.v0_metric.min_bitrate_kbps, 180)
        self.assertEqual(result.evidence.v0_metric.avg_bitrate_kbps, 211)
        self.assertEqual(result.evidence.v0_metric.median_bitrate_kbps, 214)

    def test_empty_fileset_is_explicit_outcome(self):
        empty = tempfile.mkdtemp()
        try:
            result = evidence_from_import_result(
                mb_release_id="mb-empty-1",
                source_path=empty,
                import_result=ImportResult(
                    decision="import",
                    source_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=245,
                        format="MP3",
                    ),
                ),
            )
        finally:
            shutil.rmtree(empty, ignore_errors=True)

        self.assertFalse(result.available)
        self.assertEqual(result.status, "empty_fileset")

    def test_measurement_only_reject_evidence_has_no_target_policy(self):
        report = make_audio_corrupt_validation_report("01.mp3")
        result = evidence_from_measurement(
            mb_release_id="mb-early-reject",
            source_path=self.root,
            measurement=PreimportMeasurement(
                audio_corrupt=True,
                audio_validation=report,
                corrupt_files=["01.mp3"],
                folder_layout="flat",
                audio_file_count=2,
                filetype_band="mp3",
                min_bitrate_kbps=128,
                is_vbr=False,
            ),
        )

        self.assertEqual(result.status, "ready")
        assert result.evidence is not None
        self.assertIsNone(result.evidence.target_format)
        self.assertIsNone(result.evidence.target_is_cbr)

    def test_early_reject_evidence_never_fabricates_unmeasured_quality(self):
        """Issue #1355 item 2: a reject the preview worker never ran the
        harness for leaves quality genuinely unobserved instead of
        inventing "MP3, 0 kbps" — the four facts this writer builds
        evidence for all take an unmeasured ``min_bitrate_kbps`` and, for
        ``empty_fileset``, an unmeasured ``filetype_band`` too."""
        CASES: list[
            tuple[str, PreimportMeasurement, list[AlbumQualityEvidenceFile] | None, str | None]
        ] = [
            (
                "audio_corrupt",
                PreimportMeasurement(
                    audio_corrupt=True,
                    audio_validation=make_audio_corrupt_validation_report("01.mp3"),
                    corrupt_files=["01.mp3"],
                    folder_layout="flat",
                    audio_file_count=2,
                    filetype_band="mp3",
                    min_bitrate_kbps=None,
                ),
                None,
                "MP3",
            ),
            (
                "bad_audio_hash",
                PreimportMeasurement(
                    matched_bad_hash_id=7,
                    matched_bad_track_path="01.mp3",
                    folder_layout="flat",
                    audio_file_count=2,
                    filetype_band="mp3",
                    min_bitrate_kbps=None,
                ),
                None,
                "MP3",
            ),
            (
                "nested_layout",
                PreimportMeasurement(
                    folder_layout="nested",
                    audio_file_count=2,
                    filetype_band="mp3",
                    min_bitrate_kbps=None,
                ),
                None,
                "MP3",
            ),
            (
                "empty_fileset",
                PreimportMeasurement(
                    folder_layout="flat",
                    audio_file_count=0,
                    filetype_band="",
                    min_bitrate_kbps=None,
                ),
                [],
                None,
            ),
        ]
        for fact, measurement, files, expected_format in CASES:
            with self.subTest(fact=fact):
                result = evidence_from_measurement(
                    mb_release_id=f"mb-{fact}",
                    source_path=self.root,
                    measurement=measurement,
                    files=files,
                )
                self.assertEqual(result.status, "ready")
                assert result.evidence is not None
                self.assertEqual(
                    result.evidence.measurement.format, expected_format,
                    f"{fact}: format must reflect what was actually observed",
                )
                self.assertEqual(result.evidence.storage_format, expected_format)
                self.assertIsNone(
                    result.evidence.measurement.min_bitrate_kbps,
                    f"{fact}: unmeasured bitrate must stay None, never 0",
                )
                self.assertIsNone(result.evidence.measurement.avg_bitrate_kbps)
                self.assertIsNone(result.evidence.measurement.median_bitrate_kbps)

    def test_early_reject_evidence_preserves_a_real_bitrate_hint_unmodified(self):
        """The honesty fix cuts both ways: when the measurement DOES carry a
        real bitrate (a caller-supplied hint), it must pass straight through
        to min/avg/median — never replaced, never zeroed."""
        result = evidence_from_measurement(
            mb_release_id="mb-real-hint",
            source_path=self.root,
            measurement=PreimportMeasurement(
                audio_corrupt=True,
                audio_validation=make_audio_corrupt_validation_report("01.mp3"),
                corrupt_files=["01.mp3"],
                folder_layout="flat",
                audio_file_count=2,
                filetype_band="mp3",
                min_bitrate_kbps=245,
                is_vbr=False,
            ),
        )
        self.assertEqual(result.status, "ready")
        assert result.evidence is not None
        self.assertEqual(result.evidence.measurement.min_bitrate_kbps, 245)
        self.assertEqual(result.evidence.measurement.avg_bitrate_kbps, 245)
        self.assertEqual(result.evidence.measurement.median_bitrate_kbps, 245)
        self.assertTrue(result.evidence.measurement.is_cbr)

    def test_corrupt_path_matching_never_falls_back_to_duplicate_basename(self):
        """Only the exact disc-relative path gets decode_ok=false."""
        files = [
            AlbumQualityEvidenceFile(
                relative_path=f"CD{disc}/01.flac",
                size_bytes=disc,
                mtime_ns=disc,
                extension="flac",
                container="flac",
                codec="flac",
            )
            for disc in (1, 2)
        ]
        result = evidence_from_measurement(
            mb_release_id="mb-duplicate-basenames",
            source_path=self.root,
            measurement=PreimportMeasurement(
                audio_corrupt=True,
                audio_validation=make_audio_corrupt_validation_report(
                    "CD2/01.flac",
                    files_checked=2,
                ),
                corrupt_files=["CD2/01.flac"],
                folder_layout="nested",
                audio_file_count=2,
                filetype_band="flac",
                min_bitrate_kbps=900,
            ),
            files=files,
        )

        self.assertEqual(result.status, "ready")
        assert result.evidence is not None
        self.assertEqual(
            {
                file.relative_path: file.decode_ok
                for file in result.evidence.files
            },
            {"CD1/01.flac": True, "CD2/01.flac": False},
        )

    def test_current_backfill_does_not_seed_request_scalar_proof(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=True))
        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id="mb-current-1",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertTrue(result.available)
        evidence_id = db.get_request_current_evidence_id(42)
        self.assertIsNotNone(evidence_id)
        loaded = db.load_album_quality_evidence_by_id(evidence_id)
        assert loaded is not None
        self.assertIsNone(loaded.verified_lossless_proof)
        self.assertEqual(loaded.mb_release_id, "mb-current-1")

    def test_current_backfill_uses_final_beets_facts_with_carried_source_proof(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        proof = VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral_verified_lossless",
            detail="genuine",
        )

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id="mb-current-2",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
            verified_lossless_proof=proof,
        )

        self.assertTrue(result.available)
        evidence_id = db.get_request_current_evidence_id(42)
        loaded = db.load_album_quality_evidence_by_id(evidence_id)
        assert loaded is not None
        self.assertEqual(loaded.measurement.format, "Opus")
        self.assertEqual(loaded.measurement.min_bitrate_kbps, 121)
        assert loaded.verified_lossless_proof is not None
        self.assertEqual(loaded.verified_lossless_proof.provenance, "carried")

    def test_current_backfill_discards_every_fact_from_poisoned_link(self):
        requested = "11111111-1111-1111-1111-111111111111"
        poisoned = "22222222-2222-2222-2222-222222222222"
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id=requested,
            verified_lossless=False,
        ))
        linked = make_album_quality_evidence(
            mb_release_id=poisoned,
            source_path="/historical/wrong-release",
            files=snapshot_audio_files(self.root),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade="genuine",
                spectral_bitrate_kbps=228,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="measured",
                avg_bitrate_kbps=245,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
            on_disk_v0_research_attempted=True,
        )
        db.upsert_album_quality_evidence(linked)
        stored = db.find_album_quality_evidence(
            mb_release_id=poisoned,
            snapshot_fingerprint=linked.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_request_current_evidence(42, stored.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id=requested,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertEqual(result.status, "ready")
        current_id = db.get_request_current_evidence_id(42)
        current = db.load_album_quality_evidence_by_id(current_id)
        assert current is not None
        self.assertEqual(current.mb_release_id, requested)
        self.assertIsNone(current.measurement.spectral_grade)
        self.assertIsNone(current.measurement.spectral_bitrate_kbps)
        self.assertIsNone(current.measurement.spectral_subject)
        self.assertIsNone(current.measurement.spectral_provenance)
        self.assertIsNone(current.v0_metric)
        self.assertIsNone(current.verified_lossless_proof)
        self.assertFalse(current.on_disk_v0_research_attempted)
        self.assertNotEqual(current.measured_at, linked.measured_at)

    def test_post_import_identity_mismatch_mutates_nothing(self):
        from lib.quality import (
            AccurateRipBitMatch,
            CdRipBitVerification,
            CdTocIdentity,
        )

        requested = "11111111-1111-1111-1111-111111111111"
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id=requested,
            status="imported",
        ))
        original_upsert = db.upsert_album_quality_evidence
        original_link = db.set_request_current_evidence
        db.upsert_album_quality_evidence = MagicMock(wraps=original_upsert)
        db.set_request_current_evidence = MagicMock(wraps=original_link)
        cd_rip = CdRipBitVerification(
            toc=CdTocIdentity([0], 470, "ar-id", "mb-disc"),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/example.bin",
                checksum_version="arv1",
                read_offset_samples=0,
                track_confidences=[8],
                track_checksums=[0x12345678],
                response_sha256="a" * 64,
            ),
        )
        candidate = make_album_quality_evidence(
            mb_release_id="22222222-2222-2222-2222-222222222222",
            files=snapshot_audio_files(self.root),
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )

        result = propagate_candidate_evidence_to_current(
            db,
            request_id=42,
            candidate_evidence=candidate,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=128,
                is_cbr=False,
                album_path=self.root,
                format="MP3",
            ),
        )

        self.assertEqual(result.status, "identity_mismatch")
        self.assertIsNone(result.evidence)
        db.upsert_album_quality_evidence.assert_not_called()
        db.set_request_current_evidence.assert_not_called()

    def test_propagated_ogg_vorbis_preserves_source_spectral_evidence(self):
        """Beets exposes OGG output as ``vorbis``, not the file extension."""
        vorbis_root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, vorbis_root, ignore_errors=True)
        for name in ("01.ogg", "02.ogg"):
            with open(os.path.join(vorbis_root, name), "wb") as handle:
                handle.write(name.encode())

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="mb-vorbis"))
        candidate = make_album_quality_evidence(
            mb_release_id="mb-vorbis",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.flac",
                size_bytes=1,
                mtime_ns=1,
                extension="flac",
                container="flac",
                codec="flac",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=850,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=880,
                format="FLAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=232,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
        )

        result = propagate_candidate_evidence_to_current(
            db,
            request_id=42,
            candidate_evidence=candidate,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                is_cbr=False,
                album_path=vorbis_root,
                format="vorbis",
            ),
        )

        self.assertEqual(result.status, "ready")
        assert result.evidence is not None
        evidence = result.evidence
        self.assertEqual(evidence.measurement.was_converted_from, "flac")
        self.assertEqual(evidence.storage_format, "vorbis")
        self.assertTrue(current_evidence_preserves_source_spectral(evidence))
        self.assertTrue(current_spectral_evidence_policy_usable(evidence))

    def test_current_backfill_cannot_relink_replaced_request(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="replaced",
            current_evidence_id=77,
        ))
        frozen = copy.deepcopy(db.request(42))

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id="mb-replaced",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertEqual(result.status, "stale_request")
        self.assertEqual(db.request(42), frozen)

    def test_later_lossy_backfill_preserves_existing_true_source_proof(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        proof = VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral_verified_lossless",
            detail="genuine",
        )
        seeded = make_album_quality_evidence(
            mb_release_id="mb-current-3",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=116,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
            ),
            verified_lossless_proof=proof,
            storage_format="Opus",
            files=snapshot_audio_files(self.root),
        )
        db.upsert_album_quality_evidence(seeded)
        seeded_id = db.find_album_quality_evidence(
            mb_release_id=seeded.mb_release_id,
            snapshot_fingerprint=seeded.snapshot_fingerprint,
        )
        assert seeded_id is not None and seeded_id.id is not None
        db.set_request_current_evidence(42, seeded_id.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id="mb-current-3",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=112,
                avg_bitrate_kbps=124,
                median_bitrate_kbps=123,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertTrue(result.available)
        evidence_id = db.get_request_current_evidence_id(42)
        loaded = db.load_album_quality_evidence_by_id(evidence_id)
        assert loaded is not None
        self.assertEqual(loaded.measurement.min_bitrate_kbps, 112)
        assert loaded.verified_lossless_proof is not None
        self.assertEqual(loaded.verified_lossless_proof.provenance, "carried")

    def test_lossy_backfill_carries_cd_source_evidence_without_relabelling_bytes(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        cd_rip = CdRipBitVerification(
            provenance="carried",
            source_format="flac",
            toc=CdTocIdentity([0], 470, "ar-id", "mb-disc"),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/example.bin",
                checksum_version="arv2",
                read_offset_samples=108,
                track_confidences=[11],
                track_checksums=[0x12345678],
                response_sha256="a" * 64,
            ),
        )
        seeded = make_album_quality_evidence(
            mb_release_id="mb-current-cd-rip",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=116,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
                was_converted_from="flac",
            ),
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
            storage_format="Opus",
            files=snapshot_audio_files(self.root),
        )
        db.upsert_album_quality_evidence(seeded)
        persisted = db.find_album_quality_evidence(
            mb_release_id=seeded.mb_release_id,
            snapshot_fingerprint=seeded.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id="mb-current-cd-rip",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=112,
                avg_bitrate_kbps=124,
                median_bitrate_kbps=123,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        assert result.evidence is not None
        carried = result.evidence.cd_rip_verification
        assert carried is not None
        self.assertEqual(carried.provenance, "carried")
        self.assertEqual(carried.source_format, "flac")
        self.assertEqual(result.evidence.measurement.format, "Opus")

    def test_post_import_lossy_backfill_clears_existing_true_source_proof(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        proof = VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral_verified_lossless",
            detail="genuine",
        )
        seeded = make_album_quality_evidence(
            mb_release_id="mb-current-4",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=116,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
            ),
            verified_lossless_proof=proof,
            storage_format="Opus",
            files=snapshot_audio_files(self.root),
        )
        db.upsert_album_quality_evidence(seeded)
        persisted = db.find_album_quality_evidence(
            mb_release_id=seeded.mb_release_id,
            snapshot_fingerprint=seeded.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id="mb-current-4",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                is_cbr=False,
                album_path=self.root,
                format="MP3",
            ),
            preserve_existing_verified_lossless_proof=False,
        )

        self.assertTrue(result.available)
        evidence_id = db.get_request_current_evidence_id(42)
        loaded = db.load_album_quality_evidence_by_id(evidence_id)
        assert loaded is not None
        self.assertIsNone(loaded.verified_lossless_proof)

    def test_current_backfill_ignores_all_request_quality_stamps(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=96,
            current_lossless_source_v0_probe_min_bitrate=211,
            current_lossless_source_v0_probe_avg_bitrate=260,
            current_lossless_source_v0_probe_median_bitrate=255,
        ))
        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id="mb-current-5",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertTrue(result.available)
        evidence_id = db.get_request_current_evidence_id(42)
        loaded = db.load_album_quality_evidence_by_id(evidence_id)
        assert loaded is not None
        self.assertIsNone(loaded.measurement.spectral_grade)
        self.assertIsNone(loaded.measurement.spectral_bitrate_kbps)
        self.assertIsNone(loaded.v0_metric)
        self.assertIsNone(loaded.verified_lossless_proof)

    def test_evidence_from_album_info_has_no_stamp_derived_facts(self):
        result = evidence_from_album_info(
            mb_release_id="mb-current-stampless",
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertTrue(result.available)
        assert result.evidence is not None
        self.assertIsNone(result.evidence.measurement.spectral_grade)
        self.assertIsNone(result.evidence.measurement.spectral_bitrate_kbps)
        self.assertIsNone(result.evidence.v0_metric)
        self.assertIsNone(result.evidence.verified_lossless_proof)

    def test_v3_touch_rebuild_carries_same_fingerprint_source_facts(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        proof = VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral_verified_lossless",
        )
        legacy = make_album_quality_evidence(
            mb_release_id="mb-v3-touch",
            source_path=self.root,
            files=snapshot_audio_files(self.root),
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
                spectral_grade="genuine",
                spectral_bitrate_kbps=None,
                spectral_subject="source",
                spectral_provenance="measured",
                # issue #829 Phase 5 PR1 capture facts — must carry
                # alongside spectral_grade (BLOCKING 1: the album_info
                # rebuild path produces None for all four, and the upsert's
                # atomic-pair guard nulls stored good values the instant a
                # carrying grade lands unless the Python replace also
                # carries these).
                cliff_hz=16500,
                codec_family="opus",
                ultrasonic_deficit_db=44.0,
                spectral_measurement_version=2,
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="measured",
                avg_bitrate_kbps=255,
            ),
            verified_lossless_proof=proof,
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(legacy)
        persisted = db.find_album_quality_evidence(
            mb_release_id=legacy.mb_release_id,
            snapshot_fingerprint=legacy.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id=legacy.mb_release_id,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        assert result.evidence is not None
        self.assertEqual(result.evidence.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
        self.assertEqual(result.evidence.measurement.spectral_subject, "source")
        self.assertEqual(result.evidence.measurement.spectral_provenance, "carried")
        assert result.evidence.v0_metric is not None
        self.assertEqual(result.evidence.v0_metric.subject, "source")
        self.assertEqual(result.evidence.v0_metric.provenance, "carried")
        self.assertEqual(
            result.evidence.verified_lossless_proof,
            msgspec.structs.replace(proof, provenance="carried"),
        )
        self.assertEqual(result.evidence.measurement.cliff_hz, 16500)
        self.assertEqual(result.evidence.measurement.codec_family, "opus")
        self.assertEqual(
            result.evidence.measurement.ultrasonic_deficit_db, 44.0
        )
        self.assertEqual(
            result.evidence.measurement.spectral_measurement_version, 2
        )

        # BLOCKING 1 reproduction: the function already persisted via the
        # real upsert path (atomic-pair guard, keyed on
        # EXCLUDED.spectral_grade IS NOT NULL) — re-query to confirm the
        # STORED row, not just the in-Python EvidenceBuildResult, carries
        # the capture fields.
        reloaded = db.find_album_quality_evidence(
            mb_release_id=result.evidence.mb_release_id,
            snapshot_fingerprint=result.evidence.snapshot_fingerprint,
        )
        assert reloaded is not None
        self.assertEqual(reloaded.measurement.spectral_grade, "genuine")
        self.assertEqual(reloaded.measurement.cliff_hz, 16500)
        self.assertEqual(reloaded.measurement.codec_family, "opus")
        self.assertEqual(reloaded.measurement.ultrasonic_deficit_db, 44.0)
        self.assertEqual(reloaded.measurement.spectral_measurement_version, 2)

    def test_v3_touch_normalizes_source_facts_with_unknown_provenance(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        legacy = make_album_quality_evidence(
            mb_release_id="mb-v3-unknown-provenance",
            source_path=self.root,
            files=snapshot_audio_files(self.root),
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="unknown-live-provenance",  # type: ignore[arg-type]
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="unknown-live-provenance",  # type: ignore[arg-type]
                avg_bitrate_kbps=255,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="unknown-live-provenance",  # type: ignore[arg-type]
                source="flac",
                classifier="spectral_verified_lossless",
            ),
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(legacy)
        persisted = db.find_album_quality_evidence(
            mb_release_id=legacy.mb_release_id,
            snapshot_fingerprint=legacy.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id=legacy.mb_release_id,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertTrue(result.available)
        assert result.evidence is not None
        self.assertEqual(result.evidence.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
        self.assertEqual(result.evidence.measurement.spectral_subject, "source")
        self.assertEqual(result.evidence.measurement.spectral_provenance, "carried")
        assert result.evidence.v0_metric is not None
        self.assertEqual(result.evidence.v0_metric.subject, "source")
        self.assertEqual(result.evidence.v0_metric.provenance, "carried")
        assert result.evidence.verified_lossless_proof is not None
        self.assertEqual(
            result.evidence.verified_lossless_proof.provenance,
            "carried",
        )

    def test_v3_touch_drops_ambiguous_facts(self):
        # Off-vocabulary facts cannot legally exist on a v4 row — they drop
        # on conversion whatever the snapshot did. Valid INSTALLED facts on
        # a same-snapshot repair are preserved (see the pin below): facts
        # are invalidated by byte change, not by row repair.
        for suffix, subject, provenance in (
            ("ambiguous", "unknown-live-subject", "unknown-live-provenance"),
        ):
            with self.subTest(subject=subject):
                db = FakePipelineDB()
                db.seed_request(make_request_row(id=42, verified_lossless=False))
                legacy = make_album_quality_evidence(
                    mb_release_id=f"mb-v3-drop-{suffix}",
                    source_path=self.root,
                    files=snapshot_audio_files(self.root),
                    lineage_version=3,
                    measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=121,
                        avg_bitrate_kbps=128,
                        median_bitrate_kbps=127,
                        format="Opus",
                        spectral_grade="genuine",
                        spectral_subject=subject,  # type: ignore[arg-type]
                        spectral_provenance=provenance,  # type: ignore[arg-type]
                    ),
                    v0_metric=AlbumQualityV0Metric(
                        subject=subject,  # type: ignore[arg-type]
                        provenance=provenance,  # type: ignore[arg-type]
                        avg_bitrate_kbps=255,
                    ),
                    storage_format="Opus",
                )
                db.upsert_album_quality_evidence(legacy)
                persisted = db.find_album_quality_evidence(
                    mb_release_id=legacy.mb_release_id,
                    snapshot_fingerprint=legacy.snapshot_fingerprint,
                )
                assert persisted is not None and persisted.id is not None
                db.set_request_current_evidence(42, persisted.id)

                result = backfill_current_evidence_from_album_info(
                    db,
                    request_id=42,
                    mb_release_id=legacy.mb_release_id,
                    album_info=AlbumInfo(
                        album_id=1,
                        track_count=2,
                        min_bitrate_kbps=121,
                        avg_bitrate_kbps=128,
                        median_bitrate_kbps=127,
                        is_cbr=False,
                        album_path=self.root,
                        format="Opus",
                    ),
                )

                self.assertTrue(result.available)
                assert result.evidence is not None
                self.assertEqual(result.evidence.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
                self.assertIsNone(result.evidence.measurement.spectral_grade)
                self.assertIsNone(result.evidence.measurement.spectral_subject)
                self.assertIsNone(result.evidence.measurement.spectral_provenance)
                self.assertIsNone(result.evidence.v0_metric)

    def test_same_snapshot_repair_preserves_installed_facts(self):
        """Identical bytes keep their installed measurements AND research
        anchor. The pre-fix drop left `on_disk_v0_research_attempted=True`
        with no anchor — blinding the async researcher forever (the
        deploy-night Seabear regression, request 2748).
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        legacy = make_album_quality_evidence(
            mb_release_id="mb-v3-installed-keep",
            source_path=self.root,
            files=snapshot_audio_files(self.root),
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
                was_converted_from="flac",
                spectral_grade="likely_transcode",
                spectral_subject="installed",
                spectral_provenance="measured",
                # issue #829 Phase 5 PR1 capture facts — same-address
                # repair must preserve these verbatim alongside
                # spectral_grade (BLOCKING 1).
                cliff_hz=14000,
                codec_family="opus",
                ultrasonic_deficit_db=51.5,
                spectral_measurement_version=2,
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="installed",
                provenance="measured",
                avg_bitrate_kbps=213,
                min_bitrate_kbps=158,
            ),
            on_disk_v0_research_attempted=True,
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(legacy)
        persisted = db.find_album_quality_evidence(
            mb_release_id=legacy.mb_release_id,
            snapshot_fingerprint=legacy.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id=legacy.mb_release_id,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertTrue(result.available)
        assert result.evidence is not None
        self.assertEqual(result.evidence.lineage_version, CURRENT_EVIDENCE_LINEAGE_VERSION)
        m = result.evidence.measurement
        self.assertEqual(m.was_converted_from, "flac")
        self.assertEqual(m.spectral_grade, "likely_transcode")
        self.assertEqual(m.spectral_subject, "installed")
        self.assertEqual(m.spectral_provenance, "measured")
        self.assertEqual(m.cliff_hz, 14000)
        self.assertEqual(m.codec_family, "opus")
        self.assertEqual(m.ultrasonic_deficit_db, 51.5)
        self.assertEqual(m.spectral_measurement_version, 2)
        v0 = result.evidence.v0_metric
        assert v0 is not None
        self.assertEqual(v0.subject, "installed")
        self.assertEqual(v0.provenance, "measured")
        self.assertEqual(v0.avg_bitrate_kbps, 213)
        self.assertTrue(result.evidence.on_disk_v0_research_attempted)

        # BLOCKING 1 reproduction: re-query the STORED row (the function
        # already persisted via the real upsert path) rather than trusting
        # only the in-Python EvidenceBuildResult.
        reloaded = db.find_album_quality_evidence(
            mb_release_id=result.evidence.mb_release_id,
            snapshot_fingerprint=result.evidence.snapshot_fingerprint,
        )
        assert reloaded is not None
        self.assertEqual(reloaded.measurement.cliff_hz, 14000)
        self.assertEqual(reloaded.measurement.codec_family, "opus")
        self.assertEqual(reloaded.measurement.ultrasonic_deficit_db, 51.5)
        self.assertEqual(reloaded.measurement.spectral_measurement_version, 2)

    def test_fingerprint_flip_carries_only_source_facts(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        legacy = make_album_quality_evidence(
            mb_release_id="mb-fingerprint-flip",
            source_path=self.root,
            files=snapshot_audio_files(self.root),
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="measured",
                avg_bitrate_kbps=255,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(legacy)
        persisted = db.find_album_quality_evidence(
            mb_release_id=legacy.mb_release_id,
            snapshot_fingerprint=legacy.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)
        with open(os.path.join(self.root, "01.mp3"), "ab") as handle:
            handle.write(b" changed")

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
            mb_release_id=legacy.mb_release_id,
            album_info=AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=191,
                avg_bitrate_kbps=196,
                median_bitrate_kbps=195,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        assert result.evidence is not None
        self.assertNotEqual(
            result.evidence.snapshot_fingerprint,
            legacy.snapshot_fingerprint,
        )
        self.assertEqual(result.evidence.measurement.min_bitrate_kbps, 191)
        self.assertEqual(result.evidence.measurement.spectral_subject, "source")
        self.assertEqual(result.evidence.measurement.spectral_provenance, "carried")
        assert result.evidence.v0_metric is not None
        self.assertEqual(result.evidence.v0_metric.provenance, "carried")
        assert result.evidence.verified_lossless_proof is not None
        self.assertEqual(
            result.evidence.verified_lossless_proof.provenance,
            "carried",
        )

    def test_duplicate_snapshot_relative_path_is_invalid(self):
        duplicated = AlbumQualityEvidenceFile(
            relative_path="01.mp3",
            size_bytes=1,
            mtime_ns=1,
            extension="mp3",
            container="mp3",
            codec="mp3",
        )
        result = evidence_from_import_result(
            mb_release_id="mb-dup-1",
            source_path=self.root,
            files=[duplicated, duplicated],
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    format="MP3",
                ),
            ),
        )

        self.assertFalse(result.available)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("duplicate snapshot relative_path", result.reason or "")


class TestCaptureFieldsAreOneAtomicFactWithSpectralGrade(unittest.TestCase):
    """issue #829 Phase 5 PR1, review round 2 should-fix 6:
    cliff_hz/codec_family/ultrasonic_deficit_db/spectral_measurement_version
    are the SAME measurement pass as spectral_grade — a row with no grade
    cannot legitimately carry any of them. One-directional: a grade
    WITHOUT the four fields must stay valid (every legacy row)."""

    CAPTURE_FIELD_FACTORIES: ClassVar[
        list[tuple[str, Callable[[], AudioQualityMeasurement]]]
    ] = [
        ("cliff_hz", lambda: AudioQualityMeasurement(
            min_bitrate_kbps=192, format="MP3", cliff_hz=16500,
        )),
        ("codec_family", lambda: AudioQualityMeasurement(
            min_bitrate_kbps=192, format="MP3", codec_family="mp3",
        )),
        ("ultrasonic_deficit_db", lambda: AudioQualityMeasurement(
            min_bitrate_kbps=192, format="MP3", ultrasonic_deficit_db=44.0,
        )),
        ("spectral_measurement_version", lambda: AudioQualityMeasurement(
            min_bitrate_kbps=192, format="MP3", spectral_measurement_version=2,
        )),
    ]

    def test_any_capture_field_without_a_grade_is_rejected(self):
        for field_name, make_measurement in self.CAPTURE_FIELD_FACTORIES:
            with self.subTest(field=field_name):
                errors = make_measurement().new_row_validation_errors()
                self.assertTrue(
                    any("require a spectral grade" in e for e in errors),
                    f"{field_name} without spectral_grade must be rejected, "
                    f"got errors={errors}",
                )

    def test_no_capture_fields_and_no_grade_is_valid(self):
        measurement = AudioQualityMeasurement(min_bitrate_kbps=192, format="MP3")
        self.assertEqual(measurement.new_row_validation_errors(), [])

    def test_grade_with_all_four_capture_fields_is_valid(self):
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=192,
            format="MP3",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            cliff_hz=16500,
            codec_family="mp3",
            ultrasonic_deficit_db=44.0,
            spectral_measurement_version=2,
        )
        self.assertEqual(measurement.new_row_validation_errors(), [])

    def test_grade_without_any_capture_field_stays_valid_legacy_shape(self):
        """Forward-only, no backfill (scope.md): every pre-PR1 row has a
        real spectral_grade and all four new fields NULL forever."""
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=192,
            format="MP3",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
        )
        self.assertEqual(measurement.new_row_validation_errors(), [])


class TestBlankSourcePathPolicy(unittest.TestCase):
    """A blank ``source_path`` is action-incomplete (download_log 37206).

    A row without a recorded path can never be re-verified against disk
    nor enriched with HAVE spectral — every persist guard compares against
    ``source_path``. Treating it as complete let the French Quarter import
    decide spectrally blind forever.
    """

    def test_policy_incomplete_reasons_flags_blank_source_path(self):
        for desc, path in (("empty", ""), ("whitespace", "   ")):
            with self.subTest(desc=desc):
                evidence = make_album_quality_evidence(source_path=path)
                self.assertTrue(
                    any(
                        "source_path" in reason
                        for reason in evidence.policy_incomplete_reasons()
                    ),
                    f"{desc} source_path must be an incomplete reason",
                )

    def test_policy_incomplete_reasons_accepts_real_source_path(self):
        evidence = make_album_quality_evidence(source_path="/library/album")
        self.assertEqual(evidence.policy_incomplete_reasons(), [])

    def test_decider_refuses_blank_source_path_candidate(self):
        blank = make_album_quality_evidence(source_path="")
        with self.assertRaises(ValueError):
            full_pipeline_decision_from_evidence(blank, None)

    def test_decider_refuses_blank_source_path_current(self):
        complete = make_album_quality_evidence(source_path="/library/album")
        blank = make_album_quality_evidence(source_path="")
        with self.assertRaises(ValueError):
            full_pipeline_decision_from_evidence(complete, blank)


class TestPolicyIncompleteReasonsQualityFlag(unittest.TestCase):
    """Issue #1355 item 2: ``require_quality_measurement`` gates ONLY the
    format/bitrate checks, defaulting to the original strict behavior so
    every caller that never learned about the new parameter is unaffected.
    """

    def _unmeasured(self) -> AlbumQualityEvidence:
        base = make_album_quality_evidence(source_path="/library/album")
        return msgspec.structs.replace(
            base,
            measurement=msgspec.structs.replace(
                base.measurement,
                format=None,
                min_bitrate_kbps=None,
                avg_bitrate_kbps=None,
                median_bitrate_kbps=None,
            ),
            storage_format=None,
        )

    def test_default_still_requires_quality_measurement(self):
        unmeasured = self._unmeasured()
        self.assertEqual(
            unmeasured.policy_incomplete_reasons(),
            unmeasured.policy_incomplete_reasons(
                require_quality_measurement=True),
        )
        self.assertTrue(unmeasured.policy_incomplete_reasons())

    def test_false_drops_only_the_quality_reasons(self):
        unmeasured = self._unmeasured()
        self.assertEqual(
            unmeasured.policy_incomplete_reasons(
                require_quality_measurement=False),
            [],
        )

    def test_false_still_reports_structural_and_path_reasons(self):
        blank_and_unmeasured = msgspec.structs.replace(
            self._unmeasured(), source_path="",
        )
        reasons = blank_and_unmeasured.policy_incomplete_reasons(
            require_quality_measurement=False)
        self.assertTrue(
            any("source_path" in reason for reason in reasons),
            "the source_path check must survive the quality bypass",
        )
        self.assertFalse(
            any("format" in reason or "bitrate" in reason for reason in reasons),
            "the quality checks must not survive the bypass",
        )

    def test_current_evidence_rebuild_reasons_requires_quality_by_default(self):
        """``current_evidence_rebuild_reasons`` never passes the new kwarg
        explicitly — its safety depends entirely on the strict default."""
        unmeasured = self._unmeasured()
        self.assertTrue(current_evidence_rebuild_reasons(unmeasured))


class TestAudioSnapshotMatches(unittest.TestCase):
    """Snapshot equality must ignore mtime_ns.

    virtiofs has been observed to return slightly different
    ``st_mtime_ns`` between back-to-back ``stat`` calls on the same
    file. The comparison key is (relative_path, size_bytes, extension,
    container, codec); mtime_ns stays in the struct as a forensic
    field but does not participate in equality.
    """

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        with open(os.path.join(self.root, "01.mp3"), "wb") as f:
            f.write(b"track 1 audio content")
        with open(os.path.join(self.root, "02.mp3"), "wb") as f:
            f.write(b"track 2 audio content")

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_snapshot_matches_after_mtime_only_change(self):
        """Touching a file (size unchanged) must not invalidate the snapshot."""
        captured = snapshot_audio_files(self.root)
        for entry in os.listdir(self.root):
            full = os.path.join(self.root, entry)
            stat = os.stat(full)
            os.utime(full, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000))

        self.assertTrue(
            audio_snapshot_matches(self.root, captured),
            "mtime-only changes must not be treated as a source mismatch — "
            "this caused the importer→preview infinite loop",
        )

    def test_snapshot_mismatch_when_size_differs(self):
        """A real content change (size delta) must still be detected."""
        captured = snapshot_audio_files(self.root)
        with open(os.path.join(self.root, "01.mp3"), "ab") as f:
            f.write(b"appended bytes")

        self.assertFalse(audio_snapshot_matches(self.root, captured))

    def test_snapshot_mismatch_when_file_removed(self):
        captured = snapshot_audio_files(self.root)
        os.remove(os.path.join(self.root, "02.mp3"))

        self.assertFalse(audio_snapshot_matches(self.root, captured))

    def test_snapshot_mismatch_when_file_added(self):
        captured = snapshot_audio_files(self.root)
        with open(os.path.join(self.root, "03.mp3"), "wb") as f:
            f.write(b"new track")

        self.assertFalse(audio_snapshot_matches(self.root, captured))

    def test_snapshot_matches_unchanged_files(self):
        """Sanity: an unchanged tree always matches."""
        captured = snapshot_audio_files(self.root)
        self.assertTrue(audio_snapshot_matches(self.root, captured))

    def test_missing_directory_never_matches_an_empty_snapshot(self):
        """Source presence is part of action freshness even for empty facts."""
        missing = os.path.join(self.root, "missing")
        self.assertFalse(audio_snapshot_matches(missing, []))


if TYPE_CHECKING:
    from lib.import_execution import ExecutionLeaseSnapshot
    from lib.pipeline_db import PipelineDB
    from lib.quality_evidence import QualityEvidenceDB as _EvidenceDB
    from tests.fakes import FakePipelineDB as _FakeDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    def _assert_evidence_protocol_parity(
        pipeline: PipelineDB,
        fake: _FakeDB,
        lease: ExecutionLeaseSnapshot,
    ) -> None:
        _pipeline_protocol: _EvidenceDB = pipeline
        _fake_protocol: _EvidenceDB = fake
        _pipeline_candidate_cas: bool = (
            pipeline.set_import_job_candidate_evidence(
                1,
                2,
                expected_execution_lease=lease,
            )
        )
        _fake_candidate_cas: bool = (
            fake.set_import_job_candidate_evidence(
                1,
                2,
                expected_execution_lease=lease,
            )
        )
        del (
            _pipeline_protocol,
            _fake_protocol,
            _pipeline_candidate_cas,
            _fake_candidate_cas,
        )


class TestEvidenceDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy QualityEvidenceDB."""

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.pipeline_db import PipelineDB
        from lib.quality_evidence import QualityEvidenceDB

        self.assertTrue(issubclass(PipelineDB, QualityEvidenceDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.quality_evidence import QualityEvidenceDB
        from tests.fakes import FakePipelineDB

        self.assertTrue(issubclass(FakePipelineDB, QualityEvidenceDB))

    def test_cleanup_protocol_extends_evidence_protocol(self) -> None:
        """The cleanup service forwards its handle into the evidence
        loaders, so its protocol must declare this surface too."""
        from lib.quality_evidence import QualityEvidenceDB
        from lib.wrong_match_cleanup_service import WrongMatchCleanupDB

        self.assertTrue(issubclass(WrongMatchCleanupDB, QualityEvidenceDB))


if __name__ == "__main__":
    unittest.main()
