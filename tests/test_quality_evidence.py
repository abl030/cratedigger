"""Tests for album-quality evidence construction helpers."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest

from lib.beets_db import AlbumInfo
from lib.quality import (
    ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
    AlbumQualityEvidenceOwner,
    AudioQualityMeasurement,
    ImportResult,
    V0ProbeEvidence,
)
from lib.quality_evidence import (
    backfill_current_evidence_from_album_info,
    evidence_from_import_result,
    request_current_owner,
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


if __name__ == "__main__":
    unittest.main()
