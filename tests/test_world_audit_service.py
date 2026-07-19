"""Deterministic contracts for the read-only cross-engine world audit."""

from __future__ import annotations

import os
import tempfile
import unittest

from lib.beets_db import BeetsWorldAlbum
from lib.quality_evidence import snapshot_audio_files
from lib.world_audit_service import audit_world
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


RELEASE_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


class TestWorldAuditService(unittest.TestCase):
    def test_clean_world_uses_the_shared_invariant_bank(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            album_path = os.path.join(root, "Artist", "Album")
            os.makedirs(album_path)
            track_path = os.path.join(album_path, "01 Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"world-audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=1,
                mb_release_id=RELEASE_A,
                status="imported",
                imported_path=album_path,
            ))
            evidence = make_album_quality_evidence(
                mb_release_id=RELEASE_A,
                source_path=album_path,
                files=snapshot_audio_files(album_path),
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=RELEASE_A,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            self.assertTrue(db.set_request_current_evidence(1, stored.id))

            beets = FakeBeetsDB(library_root=root)
            beets.set_world_albums([BeetsWorldAlbum(
                album_id=7,
                release_ids=(RELEASE_A,),
                album_path=album_path,
                item_paths=(track_path,),
            )])

            report = audit_world(db, beets)

        self.assertEqual(report.status, "clean")
        self.assertEqual(report.violations, ())
        self.assertEqual(report.counts.active_requests, 1)
        self.assertEqual(report.counts.beets_albums, 1)
        self.assertEqual(report.counts.linked_evidence, 1)
        self.assertIn("evidence_disk_coherence", report.audited_invariants)
        self.assertIn(
            "proof_lock_terminality_across_operation",
            report.temporal_invariants_not_auditable,
        )

    def test_known_bad_world_reports_membership_identity_and_authority(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=2,
            mb_release_id=RELEASE_A,
            status="imported",
            imported_path="/missing/imported/path",
        ))
        db.add_denylist(2, "unowned-peer", "manual note")
        beets = FakeBeetsDB()
        beets.set_world_albums([BeetsWorldAlbum(
            album_id=9,
            release_ids=(),
            album_path="",
            item_paths=(),
        )])

        report = audit_world(db, beets)

        self.assertEqual(report.status, "violations")
        codes = {violation.code for violation in report.violations}
        self.assertIn("beets_identity_missing", codes)
        self.assertIn("imported_release_missing", codes)
        self.assertIn("denylist_without_authority", codes)
        self.assertEqual(report.counts.denylist_rows, 1)
        self.assertEqual(report.counts.violations, len(report.violations))

    def test_denylist_audit_includes_frozen_replaced_requests(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=3,
            mb_release_id=RELEASE_A,
            status="replaced",
        ))
        db.add_denylist(3, "ancestor-peer", "manual note")

        report = audit_world(db, FakeBeetsDB())

        self.assertEqual(report.counts.active_requests, 0)
        self.assertEqual(report.counts.denylist_rows, 1)
        self.assertIn(
            "denylist_without_authority",
            {violation.code for violation in report.violations},
        )


if __name__ == "__main__":
    unittest.main()
