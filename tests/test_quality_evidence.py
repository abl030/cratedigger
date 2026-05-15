"""Tests for album-quality evidence construction helpers."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest

from lib.beets_db import AlbumInfo
from lib.quality import (
    ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
    AlbumQualityEvidenceFile,
    AlbumQualityEvidenceOwner,
    AudioQualityMeasurement,
    ImportResult,
    V0ProbeEvidence,
    VerifiedLosslessProof,
)
from lib.quality_evidence import (
    audio_snapshot_matches,
    backfill_current_evidence_from_album_info,
    evidence_from_import_result,
    request_current_owner,
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
        owner = AlbumQualityEvidenceOwner(
            owner_type="import_job_candidate",
            owner_id=10,
        )
        result = evidence_from_import_result(
            owner=owner,
            source_path=self.root,
            import_result=ImportResult(
                decision="import",
                new_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=237,
                    avg_bitrate_kbps=245,
                    median_bitrate_kbps=244,
                    format="mp3 v0",
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
        assert result.evidence.v0_metric is not None
        self.assertEqual(result.evidence.v0_metric.source_lineage, "lossless_source")

    def test_empty_fileset_is_explicit_outcome(self):
        empty = tempfile.mkdtemp()
        try:
            result = evidence_from_import_result(
                owner=AlbumQualityEvidenceOwner(
                    owner_type="import_job_candidate",
                    owner_id=10,
                ),
                source_path=empty,
                import_result=ImportResult(
                    decision="import",
                    new_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=245,
                        format="mp3 v0",
                    ),
                ),
            )
        finally:
            shutil.rmtree(empty, ignore_errors=True)

        self.assertFalse(result.available)
        self.assertEqual(result.status, "empty_fileset")

    def test_current_backfill_seeds_legacy_verified_lossless_proof_once(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=True))
        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
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
        loaded = db.load_album_quality_evidence(request_current_owner(42))
        assert loaded is not None
        self.assertTrue(loaded.measurement.verified_lossless)
        assert loaded.verified_lossless_proof is not None
        self.assertEqual(
            loaded.verified_lossless_proof.proof_origin,
            "legacy_request_seed",
        )
        self.assertEqual(
            loaded.owner.owner_type,
            ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
        )

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
        loaded = db.load_album_quality_evidence(request_current_owner(42))
        assert loaded is not None
        self.assertEqual(loaded.measurement.format, "Opus")
        self.assertEqual(loaded.measurement.min_bitrate_kbps, 121)
        self.assertTrue(loaded.measurement.verified_lossless)
        self.assertEqual(loaded.verified_lossless_proof, proof)

    def test_later_lossy_backfill_preserves_existing_true_source_proof(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, verified_lossless=False))
        proof = VerifiedLosslessProof(
            proof_origin="candidate_import",
            source="flac",
            classifier="spectral_verified_lossless",
            detail="genuine",
        )
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            owner_id=42,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=116,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
                verified_lossless=True,
            ),
            verified_lossless_proof=proof,
            storage_format="Opus",
        ))

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
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
        loaded = db.load_album_quality_evidence(request_current_owner(42))
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
        db.upsert_album_quality_evidence(make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            owner_id=42,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=116,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
                verified_lossless=True,
            ),
            verified_lossless_proof=proof,
            storage_format="Opus",
        ))

        result = backfill_current_evidence_from_album_info(
            db,
            request_id=42,
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
        loaded = db.load_album_quality_evidence(request_current_owner(42))
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
        loaded = db.load_album_quality_evidence(request_current_owner(42))
        assert loaded is not None
        self.assertEqual(loaded.measurement.spectral_grade, "likely_transcode")
        self.assertEqual(loaded.measurement.spectral_bitrate_kbps, 96)
        assert loaded.v0_metric is not None
        self.assertEqual(loaded.v0_metric.source_lineage, "lossless_source")
        self.assertEqual(loaded.v0_metric.avg_bitrate_kbps, 260)

    def test_duplicate_snapshot_relative_path_is_invalid(self):
        owner = AlbumQualityEvidenceOwner(
            owner_type="import_job_candidate",
            owner_id=10,
        )
        duplicated = AlbumQualityEvidenceFile(
            relative_path="01.mp3",
            size_bytes=1,
            mtime_ns=1,
            extension="mp3",
            container="mp3",
            codec="mp3",
        )
        result = evidence_from_import_result(
            owner=owner,
            source_path=self.root,
            files=[duplicated, duplicated],
            import_result=ImportResult(
                decision="import",
                new_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    format="mp3 v0",
                ),
            ),
        )

        self.assertFalse(result.available)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("duplicate snapshot relative_path", result.reason or "")


class TestAudioSnapshotMatches(unittest.TestCase):
    """Snapshot equality must ignore mtime_ns.

    Real failure: ``process_completed_album`` writes ID3 tags to the
    source files via ``music_tag.save()`` AFTER the preview worker has
    already snapshotted them. The ``save()`` rewrites mtimes by
    nanoseconds, the strict struct equality fails, the importer
    requeues the job to preview as ``"candidate source changed since
    evidence capture"``, preview re-runs, importer re-tags, infinite
    loop. The queue grows but never drains.

    virtiofs adds a second source of mtime jitter — the same file's
    ``stat().st_mtime_ns`` can flicker by a few ns between reads. Any
    fix must also make the snapshot resilient to that.

    Comparison key is (relative_path, size_bytes, extension, container,
    codec). mtime_ns stays in the struct as a forensic field but does
    not participate in equality.
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
        # Simulate music_tag.save() rewriting the file in place: mtime
        # advances even if the byte content (and so size) is identical.
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


if __name__ == "__main__":
    unittest.main()
