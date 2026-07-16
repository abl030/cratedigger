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
from typing import TYPE_CHECKING

from lib.beets_db import AlbumInfo
from lib.measurement import PreimportMeasurement
from lib.quality import (
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    ImportResult,
    V0ProbeEvidence,
    VerifiedLosslessProof,
    full_pipeline_decision_from_evidence,
)
from lib.quality_evidence import (
    audio_snapshot_matches,
    backfill_current_evidence_from_album_info,
    evidence_from_import_result,
    evidence_from_measurement,
    snapshot_audio_files,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


class TestQualityEvidenceConstruction(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        with open(os.path.join(self.root, "02.mp3"), "wb") as handle:
            handle.write(b"audio 2")
        with open(os.path.join(self.root, "01.mp3"), "wb") as handle:
            handle.write(b"audio 1")

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

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
        self.assertEqual(result.evidence.v0_metric.source_lineage, "lossless_source")

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
            result.evidence.v0_metric.source_lineage,
            "native_lossy_research",
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
        result = evidence_from_measurement(
            mb_release_id="mb-early-reject",
            source_path=self.root,
            measurement=PreimportMeasurement(
                audio_corrupt=True,
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

    def test_current_backfill_seeds_legacy_verified_lossless_proof_once(self):
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
        self.assertTrue(loaded.measurement.verified_lossless)
        assert loaded.verified_lossless_proof is not None
        self.assertEqual(
            loaded.verified_lossless_proof.proof_origin,
            "legacy_request_seed",
        )
        self.assertEqual(loaded.mb_release_id, "mb-current-1")

    def test_current_backfill_uses_final_beets_facts_with_carried_source_proof(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        proof = VerifiedLosslessProof(
            proof_origin="import_result",
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
        self.assertTrue(loaded.measurement.verified_lossless)
        self.assertEqual(loaded.verified_lossless_proof, proof)

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
            proof_origin="candidate_import",
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
                verified_lossless=True,
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
        self.assertTrue(loaded.measurement.verified_lossless)
        self.assertEqual(loaded.verified_lossless_proof, proof)

    def test_post_import_lossy_backfill_clears_existing_true_source_proof(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        proof = VerifiedLosslessProof(
            proof_origin="candidate_import",
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
                verified_lossless=True,
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
        self.assertFalse(loaded.measurement.verified_lossless)
        self.assertIsNone(loaded.verified_lossless_proof)

    def test_current_backfill_seeds_spectral_and_neutral_v0_values(self):
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
        self.assertEqual(loaded.measurement.spectral_grade, "likely_transcode")
        self.assertEqual(loaded.measurement.spectral_bitrate_kbps, 96)
        assert loaded.v0_metric is not None
        self.assertEqual(loaded.v0_metric.source_lineage, "lossless_source")
        self.assertEqual(loaded.v0_metric.avg_bitrate_kbps, 260)

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


if TYPE_CHECKING:
    from typing import cast

    from lib.pipeline_db import PipelineDB
    from lib.quality_evidence import QualityEvidenceDB as _EvidenceDB
    from tests.fakes import FakePipelineDB as _FakeDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_evidence_protocol: _EvidenceDB = cast("PipelineDB", None)
    _fake_db_satisfies_evidence_protocol: _EvidenceDB = cast("_FakeDB", None)


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
