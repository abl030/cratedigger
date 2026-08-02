"""Deterministic contracts for the read-only cross-engine world audit."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from lib.beets_db import BeetsDB, BeetsWorldAlbum
from lib.quality_evidence import snapshot_audio_files
from lib.world_audit_service import (
    WorldAuditCounts,
    WorldAuditReport,
    audit_world,
    audit_world_from_borrowed_factory,
    audit_world_from_factory,
    build_world_audit_report,
)
from lib.world_invariants import WorldViolation
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
    @staticmethod
    def _codes(report: WorldAuditReport, bucket: str) -> list[str]:
        group = {
            "A": report.groups.a,
            "B": report.groups.b,
            "C": report.groups.c,
        }[bucket]
        return [member.code for member in group.members]

    def test_public_status_distinguishes_clean_observations_and_integrity(self) -> None:
        counts = WorldAuditCounts(0, 0, 0, 0)
        clean = build_world_audit_report(counts=counts, violations=())
        observations = build_world_audit_report(
            counts=counts,
            violations=(WorldViolation(code="current_beets_missing", detail="missing"),),
        )
        bucket_c_only = build_world_audit_report(
            counts=counts,
            violations=(WorldViolation(code="album_empty", detail="empty"),),
        )
        bucket_a_only = build_world_audit_report(
            counts=counts,
            violations=(WorldViolation(code="proof_lock_broken", detail="broken"),),
        )
        mixed = build_world_audit_report(
            counts=counts,
            violations=(
                WorldViolation(code="current_beets_missing", detail="missing"),
                WorldViolation(code="album_empty", detail="empty"),
                WorldViolation(code="proof_lock_broken", detail="broken"),
            ),
        )

        self.assertEqual(clean.status, "clean")
        self.assertEqual(observations.status, "observations_only")
        self.assertEqual(bucket_c_only.status, "observations_only")
        self.assertEqual(bucket_a_only.status, "integrity_failed")
        self.assertEqual(mixed.status, "integrity_failed")
        self.assertEqual(observations.groups.b.count, 1)
        self.assertEqual(mixed.groups.a.count, 1)
        self.assertEqual(mixed.groups.b.count, 1)
        self.assertEqual(mixed.groups.c.count, 1)

    def test_grouping_is_deterministic_and_unknown_codes_fail_closed(self) -> None:
        report = build_world_audit_report(
            counts=WorldAuditCounts(0, 0, 0, 0),
            violations=(
                WorldViolation(code="album_empty", detail="z"),
                WorldViolation(code="future_unclassified_code", detail="future"),
                WorldViolation(code="current_beets_missing", detail="b"),
                WorldViolation(code="album_empty", detail="a"),
            ),
        )

        self.assertEqual(self._codes(report, "A"), ["future_unclassified_code"])
        self.assertEqual(self._codes(report, "B"), ["current_beets_missing"])
        self.assertEqual(self._codes(report, "C"), ["album_empty", "album_empty"])
        self.assertEqual([member.detail for member in report.groups.c.members], ["a", "z"])

    def test_expected_beets_availability_failures_are_incomplete_bucket_b(self) -> None:
        sqlite_failures: list[sqlite3.OperationalError] = []
        for code in (
            sqlite3.SQLITE_AUTH,
            sqlite3.SQLITE_BUSY,
            sqlite3.SQLITE_CANTOPEN,
            sqlite3.SQLITE_IOERR,
            sqlite3.SQLITE_LOCKED,
            sqlite3.SQLITE_PERM,
        ):
            failure = sqlite3.OperationalError(f"sqlite authority failure {code}")
            failure.sqlite_errorcode = code
            sqlite_failures.append(failure)
        for failure in (
            FileNotFoundError("missing"),
            PermissionError("denied"),
            *sqlite_failures,
        ):
            with self.subTest(failure=type(failure).__name__):
                def unavailable_factory(error: Exception = failure) -> FakeBeetsDB:
                    raise error

                report = audit_world_from_factory(
                    FakePipelineDB(),
                    unavailable_factory,
                )

                self.assertFalse(report.complete)
                self.assertEqual(report.status, "observations_only")
                self.assertEqual(
                    self._codes(report, "B"),
                    ["current_beets_authority_unavailable"],
                )

    def test_every_expected_sqlite_query_failure_is_incomplete_bucket_b(self) -> None:
        class QueryFailureBeets(FakeBeetsDB):
            def __init__(self, failure: sqlite3.OperationalError) -> None:
                super().__init__()
                self.failure = failure

            def list_world_albums(self) -> list[BeetsWorldAlbum]:
                raise self.failure

        for code in (
            sqlite3.SQLITE_AUTH,
            sqlite3.SQLITE_BUSY,
            sqlite3.SQLITE_CANTOPEN,
            sqlite3.SQLITE_IOERR,
            sqlite3.SQLITE_LOCKED,
            sqlite3.SQLITE_PERM,
        ):
            with self.subTest(code=code):
                failure = sqlite3.OperationalError(
                    f"sqlite query authority failure {code}"
                )
                failure.sqlite_errorcode = code
                beets = QueryFailureBeets(failure)

                report = audit_world_from_factory(
                    FakePipelineDB(), lambda handle=beets: handle
                )

                self.assertFalse(report.complete)
                self.assertEqual(
                    self._codes(report, "B"),
                    ["current_beets_authority_unavailable"],
                )
                self.assertEqual(beets.close_calls, 1)

    def test_unexpected_beets_failure_remains_a_transport_error(self) -> None:
        def broken_factory() -> FakeBeetsDB:
            raise RuntimeError("programmer defect")

        with self.assertRaisesRegex(RuntimeError, "programmer defect"):
            audit_world_from_factory(FakePipelineDB(), broken_factory)

    def test_sqlite_schema_failure_remains_a_transport_error(self) -> None:
        schema_error = sqlite3.OperationalError("no such column: albums.bad")
        schema_error.sqlite_errorcode = sqlite3.SQLITE_ERROR

        def broken_factory() -> FakeBeetsDB:
            raise schema_error

        with self.assertRaisesRegex(sqlite3.OperationalError, "no such column"):
            audit_world_from_factory(FakePipelineDB(), broken_factory)

    def test_unexpected_query_and_close_failures_propagate(self) -> None:
        class BrokenQueryBeets(FakeBeetsDB):
            def list_world_albums(self) -> list[BeetsWorldAlbum]:
                raise RuntimeError("query programmer defect")

        query_beets = BrokenQueryBeets()
        with self.assertRaisesRegex(RuntimeError, "query programmer defect"):
            audit_world_from_factory(FakePipelineDB(), lambda: query_beets)
        self.assertEqual(query_beets.close_calls, 1)

        class BrokenCloseBeets(FakeBeetsDB):
            def close(self) -> None:
                super().close()
                raise RuntimeError("close programmer defect")

        close_beets = BrokenCloseBeets()
        with self.assertRaisesRegex(RuntimeError, "close programmer defect"):
            audit_world_from_factory(FakePipelineDB(), lambda: close_beets)
        self.assertEqual(close_beets.close_calls, 1)

    def test_pipeline_availability_failure_is_not_misattributed_to_beets(self) -> None:
        class MissingPipelineDB(FakePipelineDB):
            def list_non_replaced_requests(self):
                raise FileNotFoundError("pipeline-side file")

        with self.assertRaisesRegex(FileNotFoundError, "pipeline-side file"):
            audit_world_from_borrowed_factory(
                MissingPipelineDB(),
                FakeBeetsDB,
            )

    def test_factory_owned_beets_handle_is_closed(self) -> None:
        beets = FakeBeetsDB()

        report = audit_world_from_factory(FakePipelineDB(), lambda: beets)

        self.assertEqual(report.status, "clean")
        self.assertEqual(beets.close_calls, 1)

    def test_borrowed_beets_handle_is_not_closed_when_query_is_unavailable(self) -> None:
        class UnavailableBeets(FakeBeetsDB):
            def list_world_albums(self):
                failure = sqlite3.OperationalError("database is locked")
                failure.sqlite_errorcode = sqlite3.SQLITE_LOCKED
                raise failure

        beets = UnavailableBeets()

        report = audit_world_from_borrowed_factory(
            FakePipelineDB(),
            lambda: beets,
        )

        self.assertFalse(report.complete)
        self.assertEqual(
            self._codes(report, "B"),
            ["current_beets_authority_unavailable"],
        )
        self.assertEqual(beets.close_calls, 0)

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
        self.assertEqual(report.groups.a.members, ())
        self.assertEqual(report.groups.b.members, ())
        self.assertEqual(report.groups.c.members, ())
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

        self.assertEqual(report.status, "integrity_failed")
        codes = {
            member.code
            for group in (report.groups.a, report.groups.b, report.groups.c)
            for member in group.members
        }
        self.assertIn("beets_identity_missing", codes)
        self.assertIn("current_beets_missing", codes)
        self.assertIn("denylist_without_authority", codes)
        self.assertEqual(report.counts.denylist_rows, 1)
        self.assertEqual(
            sum(group.count for group in (report.groups.a, report.groups.b, report.groups.c)),
            len(codes),
        )

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
            set(self._codes(report, "A")),
        )

    def test_conflicting_request_identity_is_reported_without_resolution(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4,
            mb_release_id=RELEASE_A,
            discogs_release_id=DISCOGS_MODERN,
            status="imported",
        ))
        beets = FakeBeetsDB()

        report = audit_world(db, beets)

        self.assertIn(
            "request_identity_missing",
            set(self._codes(report, "A")),
        )
        self.assertEqual(beets.resolve_current_release_calls, [])

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
                ),
                make_request_row(
                    id=2,
                    mb_release_id=None,
                    discogs_release_id=DISCOGS_MODERN,
                    status="imported",
                ),
                make_request_row(
                    id=3,
                    mb_release_id=None,
                    discogs_release_id=DISCOGS_LEGACY,
                    status="imported",
                ),
                make_request_row(
                    id=4,
                    mb_release_id=RELEASE_MISSING,
                    status="imported",
                ),
                make_request_row(
                    id=5,
                    mb_release_id=RELEASE_AMBIGUOUS,
                    status="imported",
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

        codes = [
            member.code
            for group in (report.groups.a, report.groups.b, report.groups.c)
            for member in group.members
        ]
        self.assertEqual(codes, [
            "current_beets_ambiguous",
            "current_beets_missing",
        ])
        self.assertNotIn("evidence_path_mismatch", codes)
        ambiguous = report.groups.b.members[0]
        self.assertIn("multiple_matches", ambiguous.detail)
        self.assertEqual(ambiguous.album_ids, (4, 5))


if __name__ == "__main__":
    unittest.main()
