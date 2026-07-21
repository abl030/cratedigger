"""Deterministic contracts for the read-only cross-engine world audit."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from lib.beets_db import BeetsDB, BeetsWorldAlbum
from lib.quality_evidence import snapshot_audio_files
from lib.world_audit_service import audit_world
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


RELEASE_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
RELEASE_AMBIGUOUS = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
RELEASE_MISSING = "cccccccc-cccc-cccc-cccc-cccccccccccc"
DISCOGS_MODERN = "1838462"
DISCOGS_LEGACY = "8818"


def _create_beets_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE albums (
            id INTEGER PRIMARY KEY,
            mb_albumid TEXT,
            discogs_albumid INTEGER
        );
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            path BLOB,
            title TEXT,
            track INTEGER,
            disc INTEGER,
            length REAL,
            format TEXT,
            bitrate INTEGER,
            samplerate INTEGER,
            bitdepth INTEGER
        );
    """)
    conn.commit()
    conn.close()


def _insert_album(
    db_path: str,
    *,
    album_id: int,
    item_id: int,
    item_path: str,
    mb_release_id: str | None = None,
    discogs_release_id: int | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO albums (id, mb_albumid, discogs_albumid) VALUES (?, ?, ?)",
        (album_id, mb_release_id, discogs_release_id),
    )
    conn.execute(
        "INSERT INTO items "
        "(id, album_id, path, title, track, disc, length, format, bitrate, "
        "samplerate, bitdepth) VALUES (?, ?, ?, 'Track', 1, 1, 180.0, "
        "'MP3', 256000, 44100, 16)",
        (item_id, album_id, item_path),
    )
    conn.commit()
    conn.close()


def _seed_linked_evidence(
    db: FakePipelineDB,
    *,
    request_id: int,
    release_id: str,
    current_path: str,
    historical_path: str | None = None,
) -> None:
    evidence = make_album_quality_evidence(
        mb_release_id=release_id,
        source_path=historical_path or current_path,
        files=snapshot_audio_files(current_path),
    )
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    assert db.set_request_current_evidence(request_id, stored.id)


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
            beets.set_album_ids_for_release(RELEASE_A, [7])
            beets.set_item_paths(RELEASE_A, [(70, track_path)])
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
        self.assertIn("current_beets_missing", codes)
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

    def test_real_beets_authority_ignores_historical_paths_and_types_failures(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            db_path = os.path.join(root, "beets.db")
            library_root = os.path.join(root, "library")
            _create_beets_db(db_path)

            album_specs = (
                (1, 11, "MB/Moved/01.mp3", RELEASE_A, None),
                (2, 21, "Discogs/Modern/01.mp3", None, int(DISCOGS_MODERN)),
                (3, 31, "Discogs/Legacy/01.mp3", DISCOGS_LEGACY, None),
                (4, 41, "Ambiguous/One/01.mp3", RELEASE_AMBIGUOUS, None),
                (5, 51, "Ambiguous/Two/01.mp3", RELEASE_AMBIGUOUS, None),
            )
            for album_id, item_id, relative, mbid, discogs_id in album_specs:
                absolute = os.path.join(library_root, relative)
                os.makedirs(os.path.dirname(absolute), exist_ok=True)
                with open(absolute, "wb") as handle:
                    handle.write(b"authority-audio")
                _insert_album(
                    db_path,
                    album_id=album_id,
                    item_id=item_id,
                    item_path=relative,
                    mb_release_id=mbid,
                    discogs_release_id=discogs_id,
                )

            db = FakePipelineDB()
            requests = (
                make_request_row(
                    id=1,
                    mb_release_id=RELEASE_A,
                    status="imported",
                    imported_path="/poisoned/request/cache",
                ),
                make_request_row(
                    id=2,
                    mb_release_id=None,
                    discogs_release_id=DISCOGS_MODERN,
                    status="imported",
                    imported_path="/stale/modern/cache",
                ),
                make_request_row(
                    id=3,
                    mb_release_id=None,
                    discogs_release_id=DISCOGS_LEGACY,
                    status="imported",
                    imported_path=None,
                ),
                make_request_row(
                    id=4,
                    mb_release_id=RELEASE_MISSING,
                    status="imported",
                    imported_path="/invented/missing/path",
                ),
                make_request_row(
                    id=5,
                    mb_release_id=RELEASE_AMBIGUOUS,
                    status="imported",
                    imported_path="/invented/ambiguous/path",
                ),
            )
            for request in requests:
                db.seed_request(request)

            for request_id, release_id, relative in (
                (1, RELEASE_A, "MB/Moved"),
                (2, DISCOGS_MODERN, "Discogs/Modern"),
                (3, DISCOGS_LEGACY, "Discogs/Legacy"),
            ):
                current_path = os.path.join(library_root, relative)
                _seed_linked_evidence(
                    db,
                    request_id=request_id,
                    release_id=release_id,
                    current_path=current_path,
                    historical_path=os.path.join(root, "historical", relative),
                )

            with BeetsDB(db_path, library_root=library_root) as beets:
                report = audit_world(db, beets)

        codes = [violation.code for violation in report.violations]
        self.assertEqual(codes, [
            "current_beets_ambiguous",
            "current_beets_missing",
        ])
        self.assertNotIn("imported_path_missing", codes)
        self.assertNotIn("imported_path_mismatch", codes)
        self.assertNotIn("evidence_path_mismatch", codes)
        ambiguous = report.violations[0]
        self.assertIn("multiple_matches", ambiguous.detail)
        self.assertEqual(ambiguous.album_ids, (4, 5))


if __name__ == "__main__":
    unittest.main()
