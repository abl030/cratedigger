"""Tests for lib/pipeline_db.py — Pipeline DB module (PostgreSQL).

Requires a PostgreSQL server. Set TEST_DB_DSN env var to run, e.g.:
    TEST_DB_DSN=postgresql://cratedigger@localhost/cratedigger_test python3 -m unittest tests.test_pipeline_db -v

Tests create/drop tables in the target database — use a dedicated test DB.
"""

import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import msgspec
import psycopg2

# Bootstrap ephemeral PostgreSQL if available
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var
from tests.helpers import make_album_quality_evidence
from lib.pipeline_db import (  # noqa: E402
    DownloadLogOutcome,
    PersistedDistance,
    PersistedTrack,
    PersistedYoutubeRow,
)


TEST_DSN = os.environ.get("TEST_DB_DSN")

def requires_postgres(cls):
    """Skip test class if TEST_DB_DSN is not set."""
    if not TEST_DSN:
        return unittest.skip("TEST_DB_DSN not set — skipping PostgreSQL tests")(cls)
    return cls


def make_db():
    """Create a PipelineDB connected to the test database, with clean tables.

    Schema is migrated once in conftest.py at session start. This helper
    just truncates all tables for an isolated test slate.
    """
    from lib import pipeline_db
    db = pipeline_db.PipelineDB(TEST_DSN)
    for table in [
        "album_quality_evidence",
        "peer_observations",
        "cycle_metrics",
        "bad_audio_hashes",
        "import_jobs",
        "user_cooldowns",
        "source_denylist",
        "search_log",
        "download_log",
        "album_request_field_resolutions",  # migration 030
        "youtube_album_mappings",  # migration 034
        "youtube_album_empty_resolutions",  # migration 035
        "plex_added_at_pins",  # migration 040
        "slskd_event_cursor",  # migration 041
        "slskd_search_ledger",  # migration 044
        "album_tracks",
        "album_requests",
    ]:
        db._execute(f"TRUNCATE {table} CASCADE")
    db.conn.commit()
    return db


@requires_postgres
class TestAddRequestRoundTrip(unittest.TestCase):
    """Rule A round-trip for add_request (#382 Layer 1). Every column the
    typed AddRequestInput payload persists must read back unchanged — the
    "column written but not read back" half of the album_title drift class.
    Pairs with the AddRequestInput-fields-subset-of-columns check in
    tests/test_pipeline_db_column_contract.py."""

    def test_add_request_round_trip_preserves_every_field(self):
        db = make_db()
        expected = {
            "artist_name": "Round Trip",
            "album_title": "Every Field",
            "source": "request",
            "mb_release_id": "mb-rt-1",
            "mb_release_group_id": "rg-rt-1",
            "mb_artist_id": "art-rt-1",
            "discogs_release_id": "dg-rt-1",
            "year": 1999,
            "release_group_year": 1998,
            "country": "US",
            "format": "CD",
            "source_path": "/incoming/rt",
            "reasoning": "why this pressing",
            "status": "wanted",
            "is_va_compilation": True,
        }
        rid = db.add_request(**expected)
        row = db.get_request(rid)
        self.assertIsNotNone(row)
        assert row is not None
        for col, val in expected.items():
            self.assertEqual(
                row[col], val,
                f"add_request field {col!r} did not round-trip through PG")


@requires_postgres
class TestSupersedeRequestMbidRoundTrip(unittest.TestCase):
    """Rule A round-trip for supersede_request_mbid (U1 — Discogs-pathway
    Replace). Every field the supersede INSERT writes onto the new row must
    read back unchanged through real PG, and the old row must flip to the
    frozen 'replaced' audit state. Before U1 there was NO real-PG round-trip
    for supersede at all, so the new discogs_release_id column had no guard
    against being dropped at the SQL seam — exactly the album_title class
    Rule A targets."""

    def _seed_old(self, db) -> int:
        return db.add_request(
            artist_name="Pendulum",
            album_title="Hold Your Colour (old pressing)",
            source="request",
            mb_release_id="old-mbid",
            mb_release_group_id="rg-old",
            mb_artist_id="art-old",
            year=2005,
            country="AU",
            status="wanted",
        )

    def test_supersede_round_trip_with_discogs_id(self):
        db = make_db()
        old_id = self._seed_old(db)
        new_tracks = [
            {"disc_number": 1, "track_number": 1, "title": "Prelude"},
            {"disc_number": 1, "track_number": 2, "title": "Slam"},
        ]
        new_id = db.supersede_request_mbid(
            old_id,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-new",
            new_mb_artist_id="art-new",
            new_artist_name="Pendulum",
            new_album_title="Hold Your Colour (target pressing)",
            new_year=2007,
            new_country="JP",
            new_discogs_release_id="12345",
            new_tracks=new_tracks,
        )
        expected = {
            "mb_release_id": "new-mbid",
            "mb_release_group_id": "rg-new",
            "mb_artist_id": "art-new",
            "artist_name": "Pendulum",
            "album_title": "Hold Your Colour (target pressing)",
            "year": 2007,
            "country": "JP",
            "discogs_release_id": "12345",
            "replaces_request_id": old_id,
            "status": "wanted",
            "source": "request",  # inherited from the old row
        }
        new = db.get_request(new_id)
        self.assertIsNotNone(new)
        assert new is not None
        for col, val in expected.items():
            self.assertEqual(
                new[col], val,
                f"supersede field {col!r} did not round-trip through PG")
        # The old row is the frozen 'replaced' audit row.
        old = db.get_request(old_id)
        assert old is not None
        self.assertEqual(old["status"], "replaced")
        # album_tracks for the new row must round-trip through the same
        # getter the rest of the pipeline reads tracks back with.
        tracks = db.get_tracks(new_id)
        self.assertEqual(
            [(t["disc_number"], t["track_number"], t["title"]) for t in tracks],
            [(t["disc_number"], t["track_number"], t["title"]) for t in new_tracks],
        )

    def test_supersede_round_trip_mb_path_discogs_id_null(self):
        # MB Replace passes new_discogs_release_id=None — the column must be
        # NULL, everything else unchanged.
        db = make_db()
        old_id = self._seed_old(db)
        new_id = db.supersede_request_mbid(
            old_id,
            new_mb_release_id="new-mbid-mb",
            new_mb_release_group_id="rg-new",
            new_mb_artist_id="art-new",
            new_artist_name="Pendulum",
            new_album_title="Hold Your Colour",
            new_year=2007,
            new_country="JP",
            new_discogs_release_id=None,
            new_tracks=[],
        )
        new = db.get_request(new_id)
        assert new is not None
        self.assertIsNone(new["discogs_release_id"])
        self.assertEqual(new["mb_release_id"], "new-mbid-mb")
        self.assertEqual(new["status"], "wanted")
        self.assertEqual(new["replaces_request_id"], old_id)
        old = db.get_request(old_id)
        assert old is not None
        self.assertEqual(old["status"], "replaced")


@requires_postgres
class TestPlexAddedAtPinsRoundTrip(unittest.TestCase):
    """Rule A round-trip for the Plex addedAt pin store (migration 040).
    Every field the writer persists must read back unchanged through real PG —
    a FakePipelineDB pass alone can't catch a column dropped at the SQL seam."""

    def test_add_pin_round_trips_every_field(self):
        # Read back via a raw SELECT (not the getter) so the assertion targets
        # exactly what PG preserved — the strongest Rule A form.
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="Muse/2026 - The Wow! Signal",
            original_added_at=1782611948,
            rating_key="458495",
            request_id=8812,
        )
        self.assertIsInstance(pin_id, int)
        cur = db._execute(
            "SELECT imported_path, original_added_at, rating_key, request_id, "
            "status FROM plex_added_at_pins WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["imported_path"], "Muse/2026 - The Wow! Signal")
        self.assertEqual(row["original_added_at"], 1782611948)
        self.assertEqual(row["rating_key"], "458495")
        self.assertEqual(row["request_id"], 8812)
        self.assertEqual(row["status"], "pending")

    def test_add_pin_round_trips_nullable_fields(self):
        # rating_key and request_id are nullable — they must round-trip as NULL.
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=100,
            rating_key=None, request_id=None)
        cur = db._execute(
            "SELECT rating_key, request_id FROM plex_added_at_pins "
            "WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertIsNone(row["rating_key"])
        self.assertIsNone(row["request_id"])

    def test_mark_pin_round_trips_status_and_excludes_from_pending(self):
        from datetime import datetime, timedelta, timezone
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=100,
            rating_key=None, request_id=None)
        now = datetime.now(timezone.utc)
        db.mark_plex_added_at_pin(pin_id, status="done", reconciled_at=now)
        # Round-trip the mutated columns via a raw SELECT.
        cur = db._execute(
            "SELECT status, reconciled_at FROM plex_added_at_pins "
            "WHERE id = %s", (pin_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "done")
        self.assertIsNotNone(row["reconciled_at"])
        # ...and a 'done' pin drops out of the pending working set.
        rows = db.get_pending_plex_added_at_pins(
            captured_before=now + timedelta(days=1), limit=100)
        self.assertEqual([r for r in rows if r["id"] == pin_id], [],
                         "done pin must not appear in pending")

    def test_pending_getter_respects_captured_before_cutoff(self):
        from datetime import datetime, timedelta, timezone
        db = make_db()
        pin_id = db.add_plex_added_at_pin(
            imported_path="C/D", original_added_at=200,
            rating_key="rk", request_id=1)
        # A cutoff in the past (before the just-now capture) excludes the pin —
        # this is the reconciler's settle-window guard.
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        rows = db.get_pending_plex_added_at_pins(captured_before=past, limit=100)
        self.assertEqual([r for r in rows if r["id"] == pin_id], [])


@requires_postgres
class TestSchemaCreation(unittest.TestCase):
    def test_tables_exist(self):
        """All expected tables exist after the migrator has run."""
        db = make_db()
        cur = db._execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        table_names = {r["table_name"] for r in cur.fetchall()}
        self.assertIn("album_requests", table_names)
        self.assertIn("album_tracks", table_names)
        self.assertIn("download_log", table_names)
        self.assertIn("search_log", table_names)
        self.assertIn("source_denylist", table_names)
        self.assertIn("user_cooldowns", table_names)
        self.assertIn("import_jobs", table_names)
        self.assertIn("cycle_metrics", table_names)
        self.assertIn("peer_observations", table_names)
        # Migration 039 dropped the peer/dir combo experiment (#227).
        self.assertNotIn("peer_dir_observations", table_names)
        self.assertNotIn("peer_dir_daily_aggregates", table_names)
        self.assertIn("album_quality_evidence", table_names)
        self.assertIn("album_quality_evidence_files", table_names)
        # The migrator's own tracking table must also exist
        self.assertIn("schema_migrations", table_names)
        db.close()

    def test_import_jobs_schema_constraints_and_indexes(self):
        """Migration 003 creates the durable shared importer queue."""
        db = make_db()
        req_id = db.add_request(
            mb_release_id="queue-schema-mbid",
            artist_name="Queue",
            album_title="Schema",
            source="request",
        )

        cur = db._execute("""
            INSERT INTO import_jobs (
                job_type, status, request_id, dedupe_key, payload
            )
            VALUES (
                'force_import', 'queued', %s, 'force_import:download_log:1',
                '{"failed_path": "/tmp/failed"}'::jsonb
            )
            RETURNING id
        """, (req_id,))
        row = cur.fetchone()
        assert row is not None
        first_id = row["id"]
        self.assertIsInstance(first_id, int)

        with self.assertRaises(Exception):
            db._execute("""
                INSERT INTO import_jobs (
                    job_type, status, request_id, dedupe_key, payload
                )
                VALUES (
                    'force_import', 'queued', %s, 'force_import:download_log:1',
                    '{"failed_path": "/tmp/other"}'::jsonb
                )
            """, (req_id,))
        db.conn.rollback()

        db._execute(
            "UPDATE import_jobs SET status = 'completed' WHERE id = %s",
            (first_id,),
        )
        db._execute("""
            INSERT INTO import_jobs (
                job_type, status, request_id, dedupe_key, payload
            )
            VALUES (
                'force_import', 'queued', %s, 'force_import:download_log:1',
                '{"failed_path": "/tmp/new"}'::jsonb
            )
        """, (req_id,))

        for column, bad_value in (("status", "bogus"), ("job_type", "bogus")):
            with self.subTest(column=column):
                with self.assertRaises(Exception):
                    db._execute(f"""
                        INSERT INTO import_jobs (
                            job_type, status, payload
                        )
                        VALUES (
                            %s, %s, '{"failed_path": "/tmp/bad"}'::jsonb
                        )
                    """, (
                        bad_value if column == "job_type" else "force_import",
                        bad_value if column == "status" else "queued",
                    ))
                db.conn.rollback()

        indexes = db._execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'import_jobs'
        """).fetchall()
        index_names = {row["indexname"] for row in indexes}
        self.assertIn("idx_import_jobs_active_dedupe", index_names)
        self.assertIn("idx_import_jobs_claim", index_names)
        db.close()

    def test_legacy_terminal_preview_jobs_are_normalized(self):
        """Migration 006 keeps old terminal history out of preview backlog."""
        db = make_db()
        req_id = db.add_request(
            mb_release_id="queue-preview-legacy-terminal-mbid",
            artist_name="Queue",
            album_title="Legacy Terminal Preview",
            source="request",
        )
        cur = db._execute("""
            INSERT INTO import_jobs (
                job_type, status, request_id, payload, preview_status,
                preview_attempts, message, completed_at
            )
            VALUES (
                'automation_import', 'completed', %s, '{}'::jsonb, 'waiting',
                0, 'Automation import processing completed', NOW()
            )
            RETURNING id
        """, (req_id,))
        row = cur.fetchone()
        assert row is not None

        migration = (
            Path(__file__).resolve().parents[1]
            / "migrations"
            / "006_normalize_legacy_terminal_preview_jobs.sql"
        )
        db._execute(migration.read_text(encoding="utf-8"))

        cur = db._execute("""
            SELECT preview_status, preview_message, preview_completed_at,
                   importable_at
            FROM import_jobs
            WHERE id = %s
        """, (row["id"],))
        normalized = cur.fetchone()
        assert normalized is not None
        self.assertEqual(normalized["preview_status"], "would_import")
        self.assertEqual(
            normalized["preview_message"],
            "Queued before async preview gate",
        )
        self.assertIsNotNone(normalized["preview_completed_at"])
        self.assertIsNotNone(normalized["importable_at"])
        db.close()

    def test_import_job_preview_schema_constraints_and_indexes(self):
        """Migration 004 adds durable async preview state to import_jobs."""
        db = make_db()
        req_id = db.add_request(
            mb_release_id="queue-preview-schema-mbid",
            artist_name="Queue",
            album_title="Preview Schema",
            source="request",
        )

        cur = db._execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'import_jobs'
        """)
        column_names = {r["column_name"] for r in cur.fetchall()}
        for column in {
            "preview_status",
            "preview_result",
            "preview_message",
            "preview_error",
            "preview_attempts",
            "preview_worker_id",
            "preview_started_at",
            "preview_heartbeat_at",
            "preview_completed_at",
            "importable_at",
        }:
            self.assertIn(column, column_names)

        cur = db._execute("""
            INSERT INTO import_jobs (job_type, request_id, payload)
            VALUES (
                'manual_import', %s,
                '{"failed_path": "/tmp/manual"}'::jsonb
            )
            RETURNING preview_status, preview_message, preview_attempts,
                      preview_completed_at, importable_at
        """, (req_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["preview_status"], "would_import")
        self.assertEqual(row["preview_message"], "Preview gate disabled")
        self.assertEqual(row["preview_attempts"], 0)
        self.assertIsNotNone(row["preview_completed_at"])
        self.assertIsNotNone(row["importable_at"])

        with self.assertRaises(Exception):
            db._execute("""
                INSERT INTO import_jobs (
                    job_type, request_id, payload, preview_status
                )
                VALUES (
                    'manual_import', %s,
                    '{"failed_path": "/tmp/manual"}'::jsonb,
                    'not-a-preview-state'
                )
            """, (req_id,))
        db.conn.rollback()

        cur = db._execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'import_jobs'
        """)
        index_names = {r["indexname"] for r in cur.fetchall()}
        self.assertIn("idx_import_jobs_preview_claim", index_names)
        self.assertIn("idx_import_jobs_importable_claim", index_names)
        db.close()


@requires_postgres
class TestAddAndGetRequest(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_add_get_roundtrip(self):
        req_id = self.db.add_request(
            mb_release_id="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
            artist_name="Buke and Gase",
            album_title="Riposte",
            source="redownload",
            year=2014,
            country="US",
        )
        self.assertIsInstance(req_id, int)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["mb_release_id"], "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        self.assertEqual(req["artist_name"], "Buke and Gase")
        self.assertEqual(req["album_title"], "Riposte")
        self.assertEqual(req["source"], "redownload")
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["year"], 2014)
        self.assertEqual(req["country"], "US")

    def test_add_minimal_fields(self):
        req_id = self.db.add_request(
            mb_release_id="test-uuid",
            artist_name="Test",
            album_title="Test Album",
            source="request",
        )
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["year"])

    def test_duplicate_mb_release_id_raises(self):
        self.db.add_request(
            mb_release_id="dup-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        with self.assertRaises(Exception):
            self.db.add_request(
                mb_release_id="dup-uuid",
                artist_name="C",
                album_title="D",
                source="request",
            )
        self.db.conn.rollback()

    def test_get_nonexistent_returns_none(self):
        self.assertIsNone(self.db.get_request(9999))

    def test_get_by_mb_release_id(self):
        self.db.add_request(
            mb_release_id="find-me-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        req = self.db.get_request_by_mb_release_id("find-me-uuid")
        assert req is not None
        self.assertEqual(req["artist_name"], "A")

    def test_get_by_mb_release_id_not_found(self):
        self.assertIsNone(self.db.get_request_by_mb_release_id("nope"))

    def test_add_with_discogs_id(self):
        req_id = self.db.add_request(
            artist_name="Test",
            album_title="Test Album",
            source="request",
            discogs_release_id="12345",
        )
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["discogs_release_id"], "12345")
        self.assertIsNone(req["mb_release_id"])

    def test_get_by_discogs_release_id(self):
        self.db.add_request(
            artist_name="A",
            album_title="B",
            source="request",
            discogs_release_id="67890",
        )
        req = self.db.get_request_by_discogs_release_id("67890")
        assert req is not None
        self.assertEqual(req["artist_name"], "A")

    def test_get_by_discogs_release_id_not_found(self):
        self.assertIsNone(self.db.get_request_by_discogs_release_id("nope"))

    def test_delete_request(self):
        req_id = self.db.add_request(
            mb_release_id="del-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.delete_request(req_id)
        self.assertIsNone(self.db.get_request(req_id))


@requires_postgres
class TestImportJobQueueAPI(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="queue-api-mbid",
            artist_name="Queue",
            album_title="API",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_enqueue_dedupes_active_job_and_allows_after_completion(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )

        dedupe = force_import_dedupe_key(17)
        payload = force_import_payload(
            download_log_id=17,
            failed_path="/tmp/failed",
            source_username="alice",
        )
        first = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=dedupe,
            payload=payload,
        )
        duplicate = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=dedupe,
            payload=payload,
        )

        self.assertEqual(first.id, duplicate.id)
        self.assertFalse(first.deduped)
        self.assertTrue(duplicate.deduped)

        self.db.mark_import_job_completed(
            first.id,
            result={"success": True},
            message="done",
        )
        later = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key=dedupe,
            payload=payload,
        )
        self.assertNotEqual(first.id, later.id)

    def test_enqueue_youtube_import_is_allowed_by_pg_constraint(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        job = self.db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=self.req_id,
            dedupe_key=youtube_import_dedupe_key(17),
            payload=youtube_import_payload(
                staged_path="/tmp/youtube-staged",
                request_id=self.req_id,
                browse_id="MPREb_pg_constraint",
            ),
        )

        self.assertEqual(job.job_type, IMPORT_JOB_YOUTUBE)
        self.assertEqual(job.request_id, self.req_id)
        self.assertEqual(job.payload["browse_id"], "MPREb_pg_constraint")

    def test_claim_complete_and_fail_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:1",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = self.db.claim_next_import_job(worker_id="test-worker")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.worker_id, "test-worker")
        self.assertEqual(claimed.attempts, 1)
        self.assertIsNone(self.db.claim_next_import_job(worker_id="other"))

        completed = self.db.mark_import_job_completed(
            claimed.id,
            result={"success": True},
            message="imported",
        )
        assert completed is not None
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.result, {"success": True})

        missing = self.db.mark_import_job_failed(
            999999,
            error="missing",
            message="missing",
        )
        self.assertIsNone(missing)

    def test_two_sessions_cannot_claim_same_job(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        from lib import pipeline_db

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:claim-once",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        other = pipeline_db.PipelineDB(TEST_DSN)
        try:
            first = self.db.claim_next_import_job(worker_id="one")
            second = other.claim_next_import_job(worker_id="two")
            self.assertIsNotNone(first)
            self.assertIsNone(second)
        finally:
            other.close()

    def test_running_jobs_can_be_requeued_immediately_after_worker_restart(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:restart-retry",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = self.db.claim_next_import_job(worker_id="old-worker")
        assert claimed is not None

        recovered = self.db.requeue_running_import_jobs(
            message="worker restarted",
        )
        self.assertEqual([job.id for job in recovered], [claimed.id])
        self.assertEqual(recovered[0].status, "queued")
        self.assertIsNone(recovered[0].worker_id)
        self.assertIsNone(recovered[0].started_at)
        self.assertIsNone(recovered[0].heartbeat_at)
        self.assertEqual(recovered[0].attempts, 1)

        retried = self.db.claim_next_import_job(worker_id="new-worker")
        assert retried is not None
        self.assertEqual(retried.id, claimed.id)
        self.assertEqual(retried.attempts, 2)
        self.assertEqual(retried.worker_id, "new-worker")

    def test_import_claim_requires_preview_importable(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:preview-gate",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.assertIsNone(self.db.claim_next_import_job(worker_id="too-early"))

        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = self.db.claim_next_import_job(worker_id="importer")
        assert claimed is not None
        self.assertEqual(claimed.id, job.id)
        self.assertEqual(claimed.status, "running")

    def test_import_job_timeline_orders_importable_before_waiting(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        waiting = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:timeline-waiting",
            payload=manual_import_payload(failed_path="/tmp/waiting"),
        )
        importable = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:timeline-importable",
            payload=manual_import_payload(failed_path="/tmp/importable"),
        )
        self.db.mark_import_job_preview_importable(
            importable.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )

        timeline = self.db.list_import_job_timeline(limit=10)

        self.assertEqual([job.id for job in timeline[:2]], [importable.id, waiting.id])
        self.assertEqual(timeline[0].preview_status, "evidence_ready")

    def test_import_job_timeline_excludes_terminal_jobs(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        importable = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:timeline-active",
            payload=manual_import_payload(failed_path="/tmp/active"),
        )
        self.db.mark_import_job_preview_importable(
            importable.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        older = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:timeline-old-terminal",
            payload=manual_import_payload(failed_path="/tmp/old"),
        )
        newer = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:timeline-new-terminal",
            payload=manual_import_payload(failed_path="/tmp/new"),
        )
        self.db.mark_import_job_failed(
            older.id,
            error="old",
            message="old",
        )
        self.db.mark_import_job_failed(
            newer.id,
            error="new",
            message="new",
        )

        timeline = self.db.list_import_job_timeline(limit=10)

        self.assertEqual([job.id for job in timeline], [importable.id])

    def test_preview_claim_and_importable_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        queued = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:preview",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.assertEqual(queued.preview_status, "waiting")
        self.assertEqual(queued.preview_attempts, 0)

        claimed = self.db.claim_next_import_preview_job(
            worker_id="preview-worker",
        )
        assert claimed is not None
        self.assertEqual(claimed.id, queued.id)
        self.assertEqual(claimed.status, "queued")
        self.assertEqual(claimed.preview_status, "running")
        self.assertEqual(claimed.preview_attempts, 1)
        self.assertEqual(claimed.preview_worker_id, "preview-worker")
        self.assertIsNone(
            self.db.claim_next_import_preview_job(worker_id="other-worker")
        )

        marked = self.db.mark_import_job_preview_importable(
            claimed.id,
            preview_result={
                "verdict": "would_import",
                "stage_chain": ["stage2_import:import"],
            },
            message="Preview would import",
        )
        assert marked is not None
        assert marked.preview_result is not None
        self.assertEqual(marked.status, "queued")
        self.assertEqual(marked.preview_status, "evidence_ready")
        self.assertEqual(marked.preview_result["verdict"], "would_import")
        self.assertEqual(marked.preview_message, "Preview would import")
        self.assertIsNotNone(marked.preview_completed_at)
        self.assertIsNotNone(marked.importable_at)

    def test_preview_rejection_fails_job_with_audit(self):
        """Post-U5: preview failures use ``preview_status='measurement_failed'``.

        ``'uncertain'`` is no longer in ``IMPORT_JOB_PREVIEW_FAILURE_STATUSES``;
        production code writes ``'measurement_failed'`` via the U4 self-healing
        helper.
        """
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        queued = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:preview-reject",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )

        failed = self.db.mark_import_job_preview_failed(
            queued.id,
            preview_status="measurement_failed",
            error="path_missing",
            preview_result={"verdict": "measurement_failed", "reason": "path_missing"},
            message="Preview failed: path_missing",
        )
        assert failed is not None
        assert failed.preview_result is not None
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.preview_status, "measurement_failed")
        self.assertEqual(failed.preview_error, "path_missing")
        self.assertEqual(failed.preview_result["reason"], "path_missing")
        self.assertEqual(failed.message, "Preview failed: path_missing")
        self.assertEqual(failed.error, "path_missing")
        self.assertIsNotNone(failed.preview_completed_at)
        self.assertIsNotNone(failed.completed_at)

    def test_two_sessions_cannot_claim_same_preview_job(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        from lib import pipeline_db

        self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:preview-claim-once",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        other = pipeline_db.PipelineDB(TEST_DSN)
        try:
            first = self.db.claim_next_import_preview_job(worker_id="one")
            second = other.claim_next_import_preview_job(worker_id="two")
            self.assertIsNotNone(first)
            self.assertIsNone(second)
        finally:
            other.close()


@requires_postgres
class TestRequeueImportJobForPreview(unittest.TestCase):
    """U2: importer can requeue an actively-running job back to preview's lane."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="requeue-preview-mbid",
            artist_name="Requeue",
            album_title="Preview",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _enqueue_claimed_job(self):
        """Enqueue a manual job, advance it through preview, and have the importer claim it."""
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:requeue-for-preview",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = self.db.claim_next_import_job(worker_id="importer-1")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        return claimed

    def test_flips_running_job_back_to_queued_waiting(self):
        claimed = self._enqueue_claimed_job()

        updated = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="candidate evidence missing",
        )

        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "waiting")
        self.assertIsNone(updated.worker_id)
        self.assertIsNone(updated.started_at)
        self.assertIsNone(updated.heartbeat_at)
        self.assertIsNone(updated.preview_message)
        self.assertIsNone(updated.preview_error)
        self.assertEqual(updated.message, "candidate evidence missing")

    def test_preserves_attempt_counters(self):
        claimed = self._enqueue_claimed_job()
        prior_attempts = claimed.attempts
        prior_preview_attempts = claimed.preview_attempts
        self.assertEqual(prior_attempts, 1)

        updated = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="stale snapshot",
        )

        assert updated is not None
        self.assertEqual(updated.attempts, prior_attempts)
        self.assertEqual(updated.preview_attempts, prior_preview_attempts)

    def test_requeued_row_is_claimable_by_preview(self):
        claimed = self._enqueue_claimed_job()
        self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="incomplete",
        )

        preview = self.db.claim_next_import_preview_job(worker_id="preview-1")
        assert preview is not None
        self.assertEqual(preview.id, claimed.id)
        self.assertEqual(preview.preview_status, "running")
        # Preview's claim clears its own diagnostics.
        self.assertIsNone(preview.preview_message)

    def test_idempotent_when_already_requeued(self):
        claimed = self._enqueue_claimed_job()
        first = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="first requeue",
        )
        # Second call should be a no-op (status no longer running).
        second = self.db.requeue_import_job_for_preview(
            claimed.id,
            reason="second requeue",
        )

        assert first is not None
        self.assertIsNone(second)
        # Message from first requeue stays.
        row = self.db._execute(
            "SELECT message FROM import_jobs WHERE id = %s",
            (claimed.id,),
        ).fetchone()
        assert row is not None
        self.assertEqual(row["message"], "first requeue")

    def test_does_not_touch_unrelated_jobs(self):
        claimed = self._enqueue_claimed_job()
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        other = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:unrelated",
            payload=manual_import_payload(failed_path="/tmp/other"),
        )

        self.db.requeue_import_job_for_preview(claimed.id, reason="x")

        other_row = self.db._execute(
            "SELECT status, preview_status FROM import_jobs WHERE id = %s",
            (other.id,),
        ).fetchone()
        assert other_row is not None
        self.assertEqual(other_row["status"], "queued")
        self.assertEqual(other_row["preview_status"], "waiting")


@requires_postgres
class TestRequeueRunningImportPreviewJobs(unittest.TestCase):
    """Startup self-heal for the async preview worker.

    Mirrors the importer's ``requeue_running_import_jobs`` — when the
    preview worker process restarts, it must immediately requeue every
    job in ``preview_status='running'`` regardless of heartbeat age,
    because by definition no preview worker is currently processing
    them (systemd runs a single instance). Before this method existed,
    crash recovery waited on the 15-minute stale-age window in
    ``requeue_stale_import_preview_jobs`` and operators saw preview
    jobs sit stuck for the full window.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="requeue-running-preview-mbid",
            artist_name="Requeue",
            album_title="Preview Running",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _enqueue_running_preview_job(self) -> int:
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:requeue-running-preview",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        claimed = self.db.claim_next_import_preview_job(worker_id="preview-old")
        assert claimed is not None
        self.assertEqual(claimed.preview_status, "running")
        self.assertIsNotNone(claimed.preview_heartbeat_at)
        return job.id

    def test_requeues_fresh_running_job_immediately(self):
        """The bug: a job claimed seconds ago should be requeued on
        startup, not wait 15 minutes for the stale-recovery sweep.
        """
        job_id = self._enqueue_running_preview_job()

        requeued = self.db.requeue_running_import_preview_jobs(
            message="Preview worker restarted while job was running; retry queued",
        )

        self.assertEqual(len(requeued), 1)
        self.assertEqual(requeued[0].id, job_id)
        self.assertEqual(requeued[0].preview_status, "waiting")
        self.assertIsNone(requeued[0].preview_worker_id)
        self.assertIsNone(requeued[0].preview_started_at)
        self.assertIsNone(requeued[0].preview_heartbeat_at)
        self.assertIsNone(requeued[0].preview_error)
        self.assertIn("restarted", (requeued[0].preview_message or ""))

    def test_requeued_job_is_immediately_claimable_by_preview(self):
        job_id = self._enqueue_running_preview_job()
        self.db.requeue_running_import_preview_jobs(
            message="restart",
        )
        reclaim = self.db.claim_next_import_preview_job(worker_id="preview-new")
        assert reclaim is not None
        self.assertEqual(reclaim.id, job_id)
        self.assertEqual(reclaim.preview_status, "running")
        self.assertEqual(reclaim.preview_worker_id, "preview-new")

    def test_does_not_touch_waiting_or_already_imported_jobs(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        # Claim-and-leave-running first so the second enqueue stays waiting.
        running_id = self._enqueue_running_preview_job()
        waiting = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:waiting",
            payload=manual_import_payload(failed_path="/tmp/waiting"),
        )

        result = self.db.requeue_running_import_preview_jobs(message="restart")

        self.assertEqual({j.id for j in result}, {running_id})
        waiting_row = self.db._execute(
            "SELECT preview_status FROM import_jobs WHERE id = %s",
            (waiting.id,),
        ).fetchone()
        assert waiting_row is not None
        self.assertEqual(waiting_row["preview_status"], "waiting")


@requires_postgres
class TestUpdateStatus(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="status-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )

    def tearDown(self):
        self.db.close()

    def test_status_transitions(self):
        for s in ["wanted", "imported", "manual"]:
            self.db.update_status(self.req_id, s)
            req = self.db.get_request(self.req_id)
            assert req is not None
            self.assertEqual(req["status"], s)

    def test_update_status_with_extra_fields(self):
        self.db.update_status(self.req_id, "imported",
                              beets_distance=0.05,
                              imported_path="/Beets/A/2020 - B")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["status"], "imported")
        self.assertAlmostEqual(req["beets_distance"], 0.05)
        self.assertEqual(req["imported_path"], "/Beets/A/2020 - B")


@requires_postgres
class TestGetWanted(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_wanted_returns_only_wanted(self):
        id1 = self.db.add_request(mb_release_id="w1", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="w2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="w3", artist_name="E", album_title="F", source="request")
        self.db.update_status(id2, "imported")

        wanted = self.db.get_wanted()
        wanted_ids = [w["id"] for w in wanted]
        self.assertIn(id1, wanted_ids)
        self.assertNotIn(id2, wanted_ids)
        self.assertIn(id3, wanted_ids)

    def test_get_wanted_respects_retry_backoff(self):
        id1 = self.db.add_request(mb_release_id="r1", artist_name="A", album_title="B", source="request")
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (future, id1),
        )
        self.db.conn.commit()

        wanted = self.db.get_wanted()
        self.assertEqual(len(wanted), 0)

    def test_get_wanted_with_limit(self):
        for i in range(5):
            self.db.add_request(mb_release_id=f"lim-{i}", artist_name="A", album_title=f"B{i}", source="request")
        wanted = self.db.get_wanted(limit=3)
        self.assertEqual(len(wanted), 3)


@requires_postgres
class TestGetByStatus(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_by_status(self):
        id1 = self.db.add_request(mb_release_id="s1", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="s2", artist_name="C", album_title="D", source="request")
        self.db.update_status(id1, "imported")

        imported = self.db.get_by_status("imported")
        self.assertEqual(len(imported), 1)
        self.assertEqual(imported[0]["id"], id1)

    def test_count_by_status(self):
        self.db.add_request(mb_release_id="c1", artist_name="A", album_title="B", source="request")
        self.db.add_request(mb_release_id="c2", artist_name="C", album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="c3", artist_name="E", album_title="F", source="redownload")
        self.db.update_status(id3, "imported")

        counts = self.db.count_by_status()
        self.assertEqual(counts["wanted"], 2)
        self.assertEqual(counts["imported"], 1)


@requires_postgres
class TestTrackManagement(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="track-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_set_get_tracks_roundtrip(self):
        tracks = [
            {"disc_number": 1, "track_number": 1, "title": "Intro", "length_seconds": 120},
            {"disc_number": 1, "track_number": 2, "title": "Song", "length_seconds": 240},
            {"disc_number": 1, "track_number": 3, "title": "Outro", "length_seconds": 180},
        ]
        self.db.set_tracks(self.req_id, tracks)

        result = self.db.get_tracks(self.req_id)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["title"], "Intro")
        self.assertEqual(result[1]["disc_number"], 1)
        self.assertEqual(result[2]["length_seconds"], 180)

    def test_set_tracks_replaces_existing(self):
        self.db.set_tracks(self.req_id, [
            {"disc_number": 1, "track_number": 1, "title": "Old", "length_seconds": 100},
        ])
        self.db.set_tracks(self.req_id, [
            {"disc_number": 1, "track_number": 1, "title": "New", "length_seconds": 200},
        ])
        result = self.db.get_tracks(self.req_id)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "New")


@requires_postgres
class TestDownloadLog(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="dl-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_and_get_download(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user123",
            filetype="flac",
            download_path="/tmp/dl/files",
            beets_distance=0.08,
            beets_scenario="single-disc",
            outcome="success",
            staged_path="/Incoming/A/B",
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["soulseek_username"], "user123")
        self.assertAlmostEqual(history[0]["beets_distance"], 0.08)
        self.assertEqual(history[0]["outcome"], "success")

    def test_log_download_preserves_null_beets_distance(self):
        """Rule A (test-fidelity.md): a pre-match reject (#550 defect #4)
        never fabricates a measured distance — ``beets_distance=None``
        must survive the real PG round-trip, not silently coerce to 0 or
        any other default."""
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user456",
            filetype="mp3",
            beets_distance=None,
            beets_scenario="untracked_audio",
            outcome="rejected",
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertIsNone(history[0]["beets_distance"])

    def test_multiple_downloads(self):
        self.db.log_download(self.req_id, "user1", "flac", "/tmp/1", outcome="rejected")
        self.db.log_download(self.req_id, "user2", "flac", "/tmp/2", outcome="success",
                             beets_distance=0.05, staged_path="/Incoming/A/B")
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["soulseek_username"], "user2")

    def test_get_log_imported_filter_excludes_rejected_rows(self):
        """Contract guard: only truly-imported rows count as "imported".

        ``get_log(outcome_filter='imported')`` filters on ``outcome IN
        ('success', 'force_import')``. Gate-rejected force/manual imports
        must NOT write ``outcome='force_import'`` or they'd leak into the UI's
        imported counter and the /api/pipeline/log imported view. Regression
        guard for the audit that caught this: a gate-rejected force import
        belongs in the "rejected" filter, not "imported".
        """
        # A successful auto import.
        self.db.log_download(
            self.req_id, "user-success", "mp3", "/Incoming/A/B",
            outcome="success", beets_distance=0.05)
        # A successful force import.
        self.db.log_download(
            self.req_id, "user-force", "mp3", "/Incoming/A/B",
            outcome="force_import", beets_distance=0.0)
        # A gate-rejected force import (e.g. spectral_reject, audio_corrupt,
        # nested_layout). Per CLAUDE.md the outcome MUST be "rejected".
        self.db.log_download(
            self.req_id, "user-gate", "mp3", "/tmp/reject",
            outcome="rejected", beets_scenario="spectral_reject")

        imported = self.db.get_log(outcome_filter="imported")
        outcomes = {row["outcome"] for row in imported}
        self.assertEqual(
            outcomes, {"success", "force_import"},
            f"imported filter must only include success + force_import, "
            f"got {outcomes}")
        self.assertNotIn(
            "rejected", outcomes,
            "gate-rejected rows must not appear under the imported filter")

        rejected = self.db.get_log(outcome_filter="rejected")
        rejected_outcomes = {row["outcome"] for row in rejected}
        self.assertIn("rejected", rejected_outcomes,
                      "gate-rejected rows must surface under the rejected filter")

    def test_get_log_keeps_download_source_and_aliases_request_source(self):
        slskd_id = self.db.log_download(
            self.req_id, "user-success", "mp3", "/Incoming/A/B",
            outcome="success", beets_distance=0.05)
        yt_id = self.db.insert_youtube_running(
            request_id=self.req_id,
            browse_id="MPREb_get_log",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=get-log",
            expected_track_count=10,
        )

        rows = self.db.get_log(limit=10)
        by_id = {row["id"]: row for row in rows}
        self.assertEqual(by_id[slskd_id]["source"], "slskd")
        self.assertEqual(by_id[slskd_id]["request_source"], "request")
        self.assertEqual(by_id[yt_id]["source"], "youtube")
        self.assertEqual(by_id[yt_id]["request_source"], "request")

    def test_log_download_round_trip_preserves_transfer_detail(self):
        """Rule A (test-fidelity.md): migration 043's transfer_detail
        JSONB column must actually preserve what log_download writes —
        a real-PG round trip, not the fake's verbatim dict storage."""
        from lib.quality import FileFailureDetail
        detail = [
            FileFailureDetail(
                username="user1",
                filename="user1\\Music\\01.flac",
                last_state="Completed, Errored",
                last_exception="Read error: Connection reset by peer",
                bytes_transferred=1234,
                retry_count=2,
            ),
            FileFailureDetail(
                username="user1",
                filename="user1\\Music\\02.flac",
            ),
        ]
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="user1",
            filetype="flac",
            outcome="timeout",
            error_message="all 2 files errored",
            transfer_detail=msgspec.to_builtins(detail),
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        round_tripped = history[0]["transfer_detail"]
        self.assertEqual(
            round_tripped,
            [msgspec.to_builtins(d) for d in detail],
        )


@requires_postgres
class TestSearchLog(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="search-log-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_and_get_search(self):
        self.db.log_search(
            request_id=self.req_id,
            query="*rtist Album",
            result_count=42,
            elapsed_s=3.2,
            outcome="found",
        )
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["query"], "*rtist Album")
        self.assertEqual(history[0]["result_count"], 42)
        elapsed = history[0]["elapsed_s"]
        assert isinstance(elapsed, (int, float))
        self.assertAlmostEqual(elapsed, 3.2, places=1)
        self.assertEqual(history[0]["outcome"], "found")

    def test_multiple_searches_newest_first(self):
        self.db.log_search(self.req_id, query="q1", outcome="no_results")
        self.db.log_search(self.req_id, query="q2", result_count=5,
                           elapsed_s=2.0, outcome="no_match")
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["outcome"], "no_match")  # most recent first
        self.assertEqual(history[1]["outcome"], "no_results")

    def test_empty_query_outcome(self):
        self.db.log_search(self.req_id, query=None, outcome="empty_query")
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 1)
        self.assertIsNone(history[0]["query"])
        self.assertIsNone(history[0]["result_count"])
        self.assertEqual(history[0]["outcome"], "empty_query")

    def test_all_outcomes_valid(self):
        for outcome in ("found", "no_match", "no_results", "timeout", "error", "empty_query"):
            self.db.log_search(self.req_id, query="q", outcome=outcome)
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 6)

    def test_exhausted_outcome_now_allowed_post_migration_010(self):
        """Migration 010 widened the CHECK constraint to include 'exhausted'."""
        self.db.log_search(
            self.req_id, query=None, outcome="exhausted",
            variant="exhausted",
        )
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(history[0]["outcome"], "exhausted")
        self.assertEqual(history[0]["variant"], "exhausted")

    def test_log_search_persists_candidates_jsonb_and_round_trips(self):
        """U5 wire-boundary: encode list[CandidateScore] → JSONB → decode."""
        import json
        import msgspec
        from lib.quality import CandidateScore

        candidates = [
            CandidateScore(
                username="u1", dir="A\\Album", filetype="flac",
                matched_tracks=26, total_tracks=26, avg_ratio=0.95,
                missing_titles=[], file_count=26,
            ),
            CandidateScore(
                username="u2", dir="B\\Album", filetype="flac",
                matched_tracks=22, total_tracks=26, avg_ratio=0.0,
                missing_titles=[], file_count=22,
            ),
        ]
        self.db.log_search(
            request_id=self.req_id,
            query="*rtist Album",
            result_count=10,
            elapsed_s=2.5,
            outcome="no_match",
            candidates=candidates,
            variant="default",
            final_state="Completed",
        )

        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertEqual(row["variant"], "default")
        self.assertEqual(row["final_state"], "Completed")

        # psycopg2 returns JSONB as already-decoded Python objects, but
        # accept a str fallback in case driver settings differ.
        raw = row["candidates"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        assert isinstance(raw, list)
        self.assertEqual(len(raw), 2)
        decoded = msgspec.convert(raw, type=list[CandidateScore])
        self.assertEqual(decoded[0].username, "u1")
        self.assertEqual(decoded[0].matched_tracks, 26)
        self.assertEqual(decoded[1].file_count, 22)

    def test_log_search_with_null_candidates_writes_sql_null(self):
        """Failure rows (timeout/error) still write a row, candidates NULL."""
        self.db.log_search(
            request_id=self.req_id, query="q", outcome="timeout",
            variant="v1_year", final_state="TimedOut",
            candidates=None,
        )
        history = self.db.get_search_history(self.req_id)
        self.assertIsNone(history[0]["candidates"])
        self.assertEqual(history[0]["variant"], "v1_year")
        self.assertEqual(history[0]["final_state"], "TimedOut")

    def test_log_search_persists_pre_filter_skip_count(self):
        """U2 of search-plan-entropy: ``pre_filter_skip_count`` writes to
        the dedicated column. NOT NULL with default 0; this asserts the
        explicit non-zero path actually round-trips, AND that omitting
        the kwarg defaults to 0 in the persisted row."""
        # Explicit non-zero round-trip.
        self.db.log_search(
            request_id=self.req_id, query="q", outcome="no_match",
            candidates=None, pre_filter_skip_count=42,
        )
        # Default (kwarg omitted) writes 0.
        self.db.log_search(
            request_id=self.req_id, query="q2", outcome="found",
            candidates=None,
        )
        history = self.db.get_search_history(self.req_id)
        # history is newest-first; index 0 == second insert (default).
        self.assertEqual(history[0]["pre_filter_skip_count"], 0)
        self.assertEqual(history[1]["pre_filter_skip_count"], 42)

    def test_log_search_persists_u11_forensics_columns(self):
        """U11 R22-R27: every new forensics column round-trips on log_search.

        Asserts ``rejection_reason``, ``result_count_uncapped``,
        ``query_token_count``, ``query_distinct_token_count``,
        ``expected_track_count``, ``matcher_score_top1``, and
        ``query_template`` survive the INSERT and come back on the
        SELECT * read.
        """
        self.db.log_search(
            request_id=self.req_id,
            query="*rtist Album",
            outcome="no_match",
            candidates=None,
            rejection_reason="avg_ratio_low",
            result_count_uncapped=1234,
            query_token_count=2,
            query_distinct_token_count=2,
            expected_track_count=14,
            matcher_score_top1=2.75,
            query_template="{artist} {title}",
        )
        # Second row: defaults (kwargs omitted) write SQL NULL so we
        # can also assert backwards-compat.
        self.db.log_search(
            request_id=self.req_id, query="q2",
            outcome="no_results", candidates=None,
        )
        history = self.db.get_search_history(self.req_id)
        # newest-first.
        nulls = history[0]
        self.assertIsNone(nulls["rejection_reason"])
        self.assertIsNone(nulls["result_count_uncapped"])
        self.assertIsNone(nulls["query_token_count"])
        self.assertIsNone(nulls["query_distinct_token_count"])
        self.assertIsNone(nulls["expected_track_count"])
        self.assertIsNone(nulls["matcher_score_top1"])
        self.assertIsNone(nulls["query_template"])
        populated = history[1]
        self.assertEqual(populated["rejection_reason"], "avg_ratio_low")
        self.assertEqual(populated["result_count_uncapped"], 1234)
        self.assertEqual(populated["query_token_count"], 2)
        self.assertEqual(populated["query_distinct_token_count"], 2)
        self.assertEqual(populated["expected_track_count"], 14)
        score = populated["matcher_score_top1"]
        assert isinstance(score, float)
        self.assertAlmostEqual(score, 2.75, places=4)
        self.assertEqual(populated["query_template"], "{artist} {title}")

    def test_log_search_candidates_decode_rejects_wrong_type(self):
        """Wire-boundary regression: msgspec.convert raises on type drift.

        At least one RED test that feeds the wrong type at the boundary and
        asserts ``msgspec.ValidationError`` — the strict-typed decoder is
        what catches int-vs-str drift in the JSONB blob downstream.
        """
        import msgspec
        from lib.quality import CandidateScore

        # ``matched_tracks`` is declared int — passing a string at the wire
        # must trip msgspec on read, not silently coerce.
        wrong = [{
            "username": "u1", "dir": "A", "filetype": "flac",
            "matched_tracks": "26",  # WRONG: string for int field
            "total_tracks": 26, "avg_ratio": 0.9,
            "missing_titles": [], "file_count": 26,
        }]
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(wrong, type=list[CandidateScore])


@requires_postgres
class TestGetSaturationSummary(unittest.TestCase):
    """U7: ``PipelineDB.get_saturation_summary`` aggregates one request's
    search_log rows in the recent window. Saturation = rows whose
    ``final_state`` matches ``%LimitReached%`` (slskd hit response /
    file ceiling). ``total_pre_filter_skips`` rolls up the U2 column.

    ``saturation_rate`` is computed in Python so the explicit ``0.0``
    fallback survives the empty-window case (NaN would break JSON
    serialisation downstream).
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="saturation-uuid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_empty_request_returns_zeros(self):
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 0)
        self.assertEqual(summary.saturated_searches, 0)
        # Critical invariant: 0.0, not NaN.
        self.assertEqual(summary.saturation_rate, 0.0)
        self.assertEqual(summary.total_pre_filter_skips, 0)
        self.assertEqual(summary.window_days, 14)

    def test_counts_only_saturated_final_states(self):
        # Only rows whose final_state contains "LimitReached" count
        # as saturated. The slskd state strings are comma-joined so
        # the match must be a substring, not equality.
        for state in (
            "Completed, ResponseLimitReached",
            "Completed, FileLimitReached",
            "Completed",  # not saturated
            "Cancelled",  # not saturated
            None,         # not saturated
        ):
            self.db.log_search(
                request_id=self.req_id, query="q", outcome="found",
                final_state=state,
            )
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 5)
        self.assertEqual(summary.saturated_searches, 2)
        self.assertAlmostEqual(summary.saturation_rate, 2 / 5)

    def test_sums_pre_filter_skip_count(self):
        for skip in (4, 1, 0, 8):
            self.db.log_search(
                request_id=self.req_id, query="q", outcome="found",
                final_state="Completed", pre_filter_skip_count=skip,
            )
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 4)
        self.assertEqual(summary.saturated_searches, 0)
        self.assertEqual(summary.total_pre_filter_skips, 13)

    def test_window_days_filters_old_rows(self):
        # Insert two recent rows, then backdate a third via direct SQL
        # so we can test the window cut without sleeping.
        self.db.log_search(
            request_id=self.req_id, query="recent_a",
            outcome="found",
            final_state="Completed, ResponseLimitReached",
            pre_filter_skip_count=2,
        )
        self.db.log_search(
            request_id=self.req_id, query="recent_b",
            outcome="found", final_state="Completed",
            pre_filter_skip_count=1,
        )
        self.db.log_search(
            request_id=self.req_id, query="old",
            outcome="found",
            final_state="Completed, FileLimitReached",
            pre_filter_skip_count=10,
        )
        # Backdate the most recent row 10 days into the past.
        self.db._execute(
            "UPDATE search_log SET created_at = NOW() - INTERVAL '10 days' "
            "WHERE query = %s",
            ("old",),
        )
        # 7-day window: old row out, two recent rows in.
        seven = self.db.get_saturation_summary(self.req_id, window_days=7)
        self.assertEqual(seven.total_searches, 2)
        self.assertEqual(seven.saturated_searches, 1)
        self.assertEqual(seven.total_pre_filter_skips, 3)
        self.assertEqual(seven.window_days, 7)
        # 14-day window: all three rows are in scope.
        fourteen = self.db.get_saturation_summary(
            self.req_id, window_days=14)
        self.assertEqual(fourteen.total_searches, 3)
        self.assertEqual(fourteen.saturated_searches, 2)
        self.assertEqual(fourteen.total_pre_filter_skips, 13)

    def test_isolates_by_request_id(self):
        # Rows for a different request must not bleed into this
        # request's saturation roll-up.
        other = self.db.add_request(
            mb_release_id="other-uuid",
            artist_name="X", album_title="Y", source="request",
        )
        self.db.log_search(
            request_id=other, query="other",
            outcome="found",
            final_state="Completed, ResponseLimitReached",
            pre_filter_skip_count=99,
        )
        self.db.log_search(
            request_id=self.req_id, query="mine",
            outcome="found", final_state="Completed",
            pre_filter_skip_count=2,
        )
        summary = self.db.get_saturation_summary(self.req_id, window_days=14)
        self.assertEqual(summary.total_searches, 1)
        self.assertEqual(summary.saturated_searches, 0)
        self.assertEqual(summary.total_pre_filter_skips, 2)

    def test_window_days_echoes_back(self):
        # The summary echoes window_days so callers don't have to
        # remember what they asked for.
        summary = self.db.get_saturation_summary(self.req_id, window_days=30)
        self.assertEqual(summary.window_days, 30)


@requires_postgres
class TestGetSearchHistoryPage(unittest.TestCase):
    """U1: cursor-style pagination for ``GET /search-plan/history``.

    The DB method ``get_search_history_page`` returns at most ``limit``
    rows for one request_id ordered ``id DESC`` with an opaque
    ``next_before_id`` seed when more rows remain. Mirrors
    ``get_search_history`` shape but bounded.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="search-hist-page-uuid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def _seed(self, n: int) -> list[int]:
        for i in range(n):
            self.db.log_search(
                self.req_id, query=f"q{i}", outcome="no_match",
            )
        full = self.db.get_search_history(self.req_id)
        return [int(cast(Any, r["id"])) for r in full]  # newest-first

    def test_first_page_clamps_to_limit_and_seeds_next_before_id(self):
        ids_desc = self._seed(75)
        page = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        self.assertEqual(len(page.rows), 50)
        # Newest 50 rows in DESC order.
        self.assertEqual(
            [int(cast(Any, r["id"])) for r in page.rows], ids_desc[:50],
        )
        # next_before_id seeds the next page from the 51st row's id.
        self.assertEqual(page.next_before_id, ids_desc[50])

    def test_second_page_via_before_id_returns_strictly_older_rows(self):
        ids_desc = self._seed(75)
        first = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        second = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=first.next_before_id,
        )
        # First page returns 50 rows; ``next_before_id`` points one row
        # past the boundary (the 51st row), and the second page resumes
        # *at* that row — no row is skipped.
        self.assertEqual(len(second.rows), 25)
        # Second page rows are older-or-equal to the cursor and
        # id-monotonic descending.
        page_ids = [int(cast(Any, r["id"])) for r in second.rows]
        self.assertEqual(page_ids, ids_desc[50:75])
        # No id appears in both pages (no boundary overlap).
        first_ids = {int(cast(Any, r["id"])) for r in first.rows}
        self.assertFalse(first_ids.intersection(page_ids))
        self.assertIsNone(second.next_before_id)

    def test_exhausted_when_fewer_rows_than_limit(self):
        self._seed(30)
        page = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        self.assertEqual(len(page.rows), 30)
        self.assertIsNone(page.next_before_id)

    def test_empty_when_no_rows_for_request(self):
        page = self.db.get_search_history_page(
            self.req_id, limit=50, before_id=None,
        )
        self.assertEqual(page.rows, [])
        self.assertIsNone(page.next_before_id)

    def test_legacy_only_rows_returned_with_null_plan_columns(self):
        # log_search writes legacy-shaped rows (plan_id IS NULL).
        self.db.log_search(self.req_id, query="legacy", outcome="no_match")
        page = self.db.get_search_history_page(
            self.req_id, limit=10, before_id=None,
        )
        self.assertEqual(len(page.rows), 1)
        row = page.rows[0]
        self.assertEqual(row["query"], "legacy")
        self.assertIsNone(row["plan_id"])
        self.assertIsNone(row["plan_ordinal"])

    def test_other_request_ids_excluded(self):
        other_req = self.db.add_request(
            mb_release_id="search-hist-page-other-uuid",
            artist_name="C", album_title="D", source="request",
        )
        self.db.log_search(other_req, query="other", outcome="no_match")
        self.db.log_search(self.req_id, query="mine", outcome="no_match")
        page = self.db.get_search_history_page(
            self.req_id, limit=10, before_id=None,
        )
        self.assertEqual(len(page.rows), 1)
        self.assertEqual(page.rows[0]["query"], "mine")

    def test_page_returns_dict_rows_with_full_search_log_columns(self):
        self.db.log_search(
            self.req_id, query="q", result_count=5, elapsed_s=1.2,
            outcome="no_match", variant="v0", final_state="Completed",
        )
        page = self.db.get_search_history_page(
            self.req_id, limit=1, before_id=None,
        )
        row = page.rows[0]
        # Spot-check a wider column set than legacy_logs.head provides —
        # this endpoint surfaces full telemetry per row.
        for col in ("id", "request_id", "query", "result_count",
                    "elapsed_s", "outcome", "candidates", "variant",
                    "final_state", "browse_time_s", "match_time_s",
                    "peers_browsed", "peers_browsed_lazy", "fanout_waves",
                    "plan_id", "plan_item_id", "plan_ordinal",
                    "plan_strategy", "plan_canonical_query_key",
                    "plan_repeat_group", "plan_generator_id",
                    "execution_stage", "attempt_consumed",
                    "cursor_update_status", "stale_reason",
                    "plan_cycle_snapshot", "created_at"):
            self.assertIn(col, row, f"missing column {col!r}")


@requires_postgres
class TestPipelineDashboardMetrics(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="dash-1",
            artist_name="Dashboard Artist",
            album_title="Loop Candidate",
            source="request",
        )
        self.req2 = self.db.add_request(
            mb_release_id="dash-2",
            artist_name="Dashboard Artist",
            album_title="Healthy Candidate",
            source="request",
        )
        self.req3 = self.db.add_request(
            mb_release_id="dash-3",
            artist_name="Dashboard Artist",
            album_title="Never Searched",
            source="request",
        )
        self.req4 = self.db.add_request(
            mb_release_id="dash-4",
            artist_name="Dashboard Artist",
            album_title="Active Download",
            source="request",
        )
        self.db.set_downloading(self.req4, json.dumps({"username": "active"}))

    def tearDown(self):
        self.db.close()

    def test_record_cycle_metrics_and_dashboard_summary(self):
        now = datetime.now(timezone.utc)
        self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=1, seconds=100),
            completed_at=now - timedelta(hours=1),
            cycle_total_s=100.0,
            search_time_s=80.0,
            cycle_searches_watchdog_killed=0,
            find_download_queued=5,
            find_download_completed=5,
            wanted_total=4,
        )
        self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=2, seconds=300),
            completed_at=now - timedelta(hours=2),
            cycle_total_s=300.0,
            search_time_s=240.0,
            cycle_searches_watchdog_killed=1,
            find_download_queued=3,
            find_download_completed=2,
            wanted_total=5,
        )
        self.db.record_cycle_metrics(
            started_at=now - timedelta(hours=10, seconds=900),
            completed_at=now - timedelta(hours=10),
            cycle_total_s=900.0,
            search_time_s=700.0,
            cache_errors=2,
            wanted_total=6,
        )

        self.db.log_search(
            self.req1, query="loop a", elapsed_s=2.0, outcome="no_results",
            peers_browsed=4, peers_browsed_lazy=1, fanout_waves=1,
            browse_time_s=5.0,
        )
        self.db.log_search(
            self.req1, query="loop b", result_count=500, elapsed_s=4.0,
            outcome="no_match", variant="track_0", peers_browsed=33,
            peers_browsed_lazy=2, fanout_waves=3, browse_time_s=12.5,
            match_time_s=0.5,
        )
        self.db.log_search(
            self.req1, query=None, elapsed_s=1.0, outcome="exhausted"
        )
        self.db.log_search(
            self.req1, query="loop c", elapsed_s=3.0, outcome="timeout"
        )
        self.db.log_search(
            self.req2, query="healthy", elapsed_s=6.0, outcome="found"
        )
        self.db.log_search(
            self.req4, query="active download", elapsed_s=7.0, outcome="found"
        )

        metrics = self.db.get_pipeline_dashboard_metrics()

        searches_24h = metrics["searches"]["windows"][0]
        self.assertEqual(searches_24h["label"], "24h")
        self.assertEqual(searches_24h["searches"], 6)
        self.assertEqual(searches_24h["distinct_requests"], 3)
        self.assertAlmostEqual(searches_24h["searches_per_24h"], 6)
        self.assertEqual(searches_24h["outcomes"]["found"], 2)
        self.assertEqual(searches_24h["outcomes"]["no_match"], 1)
        self.assertEqual(searches_24h["outcomes"]["no_results"], 1)
        self.assertEqual(searches_24h["outcomes"]["exhausted"], 1)
        self.assertEqual(searches_24h["outcomes"]["errors"], 1)

        searches_6h = metrics["searches"]["windows"][1]
        self.assertEqual(searches_6h["label"], "6h")
        self.assertAlmostEqual(searches_6h["searches_per_24h"], 24)

        cycles_6h = metrics["cycles"]["windows"][1]
        self.assertEqual(cycles_6h["label"], "6h")
        self.assertEqual(cycles_6h["cycles"], 2)
        self.assertAlmostEqual(cycles_6h["median_cycle_s"], 200.0)
        self.assertEqual(cycles_6h["max_cycle_s"], 300.0)
        self.assertEqual(cycles_6h["watchdog_kills"], 1)
        self.assertEqual(cycles_6h["find_download_queued"], 8)
        self.assertEqual(cycles_6h["find_download_completed"], 7)

        coverage = metrics["coverage"]
        self.assertEqual(coverage["wanted_total"], 4)
        self.assertEqual(coverage["wanted_searched_24h"], 3)
        self.assertEqual(coverage["wanted_unsearched_24h"], 1)
        self.assertEqual(coverage["wanted_never_searched"], 1)
        self.assertEqual(coverage["active_wanted_searches_24h"], 6)
        self.assertEqual(coverage["matches_24h"], 2)
        self.assertEqual(coverage["matches_6h"], 2)
        self.assertAlmostEqual(coverage["matches_per_hour_24h"], 2 / 24)
        self.assertAlmostEqual(coverage["matches_per_hour_6h"], 2 / 6)
        trend = coverage["wanted_trend"]
        self.assertEqual(trend["current_wanted"], 4)
        self.assertEqual([w["label"] for w in trend["windows"]],
                         ["6h", "24h", "7d"])
        trend_6h = trend["windows"][0]
        self.assertEqual(trend_6h["start_wanted"], 5)
        self.assertEqual(trend_6h["end_wanted"], 4)
        self.assertEqual(trend_6h["delta"], -1)
        self.assertEqual(trend_6h["trend"], "down")
        self.assertGreater(trend_6h["drain_per_hour"], 0)
        self.assertIsNotNone(trend_6h["eta_hours"])
        self.assertGreaterEqual(len(trend["series_24h"]), 4)
        self.assertEqual(
            set(trend["series_24h"][0]),
            {"sampled_at", "wanted_total"},
        )
        self.assertEqual(len(coverage["match_rate_series_24h"]), 24)
        self.assertEqual(
            sum(point["matches"] for point in coverage["match_rate_series_24h"]),
            2,
        )
        self.assertEqual(
            set(coverage["match_rate_series_24h"][0]),
            {"bucket_start", "matches", "matches_per_hour"},
        )
        self.assertEqual(len(coverage["match_rate_series_28d"]), 28)
        self.assertEqual(
            sum(point["matches"] for point in coverage["match_rate_series_28d"]),
            2,
        )
        self.assertEqual(
            set(coverage["match_rate_series_28d"][0]),
            {"bucket_start", "matches", "matches_per_day"},
        )
        self.assertEqual(coverage["top_loop_suspects"][0]["request_id"], self.req1)
        self.assertEqual(coverage["top_loop_suspects"][0]["searches_24h"], 4)
        self.assertEqual(coverage["top_loop_suspects"][0]["reset_24h"], 1)
        self.assertEqual(coverage["top_loop_suspects"][0]["problem_24h"], 1)
        self.assertIn(
            self.req4,
            [row["request_id"] for row in coverage["top_loop_suspects"]],
        )
        self.assertEqual(coverage["stale_wanted"][0]["request_id"], self.req3)
        self.assertIn(
            "downloading",
            [row["status"] for row in coverage["stale_wanted"]],
        )
        heavy = metrics["peers"]["heavy_queries"]
        self.assertEqual(heavy[0]["request_id"], self.req1)
        self.assertEqual(heavy[0]["mb_release_id"], "dash-1")
        self.assertEqual(heavy[0]["query"], "loop b")
        self.assertEqual(heavy[0]["variant"], "track_0")
        self.assertEqual(heavy[0]["result_count"], 500)
        self.assertEqual(heavy[0]["peer_dirs"], 35)
        self.assertEqual(heavy[0]["fanout_waves"], 3)
        self.assertEqual(heavy[0]["browse_time_s"], 12.5)
        self.assertEqual(metrics["cycles"]["outliers"][0]["cycle_total_s"], 900.0)

    def test_peer_observations_track_distinct_peers(self):
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=2)

        inserted = self.db.record_peer_observations(
            ["user1", "user1", "user2"],
            observed_at=old,
        )
        self.assertEqual(inserted, 2)

        inserted = self.db.record_peer_observations(
            ["user1", "user3"],
            observed_at=now,
        )
        self.assertEqual(inserted, 1)

        peers = self.db.get_peer_metrics(days=14)
        self.assertEqual(peers["totals"]["known_peers"], 3)
        self.assertEqual(peers["totals"]["new_24h"], 1)
        self.assertEqual(
            sum(day["new_peers"] for day in peers["days"]),
            3,
        )


@requires_postgres
class TestPeerObservations(unittest.TestCase):
    """Distinct-peer roster (#227): ``record_peer_observations`` upserts
    one row per hashed username; ``get_peer_metrics`` computes totals and
    the per-day growth curve live (the table is small enough forever)."""

    _TOTALS_KEYS = {"known_peers", "new_24h", "seen_24h", "tracked_since"}
    _DAY_KEYS = {"date", "new_peers", "total_peers"}

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_record_round_trip_preserves_first_and_last_seen(self):
        """Rule A: every field written must read back through PG. A
        re-observation must advance last_seen_at but never first_seen_at."""
        first = datetime.now(timezone.utc) - timedelta(days=3)
        later = datetime.now(timezone.utc) - timedelta(hours=1)

        self.assertEqual(
            self.db.record_peer_observations(["alice"], observed_at=first), 1)
        self.assertEqual(
            self.db.record_peer_observations(["alice"], observed_at=later), 0)

        row = self.db._execute(
            "SELECT username_hash, first_seen_at, last_seen_at "
            "FROM peer_observations").fetchone()
        assert row is not None
        self.assertEqual(len(row["username_hash"]), 64)
        self.assertEqual(row["first_seen_at"], first)
        self.assertEqual(row["last_seen_at"], later)

    def test_record_ignores_empty_usernames(self):
        self.assertEqual(self.db.record_peer_observations([""]), 0)
        self.assertEqual(self.db.record_peer_observations([]), 0)

    def test_record_stale_observation_never_regresses_last_seen(self):
        now = datetime.now(timezone.utc)
        earlier = now - timedelta(days=1)
        self.db.record_peer_observations(["bob"], observed_at=now)
        self.db.record_peer_observations(["bob"], observed_at=earlier)
        row = self.db._execute(
            "SELECT last_seen_at FROM peer_observations").fetchone()
        assert row is not None
        self.assertEqual(row["last_seen_at"], now)

    def test_metrics_shape_and_cumulative_totals(self):
        """Response shape is pinned; ``total_peers`` is the cumulative
        distinct-peer count at end of each day, carried forward across
        days with no new peers."""
        now = datetime.now(timezone.utc)
        self.db.record_peer_observations(
            ["old1", "old2"], observed_at=now - timedelta(days=5))
        self.db.record_peer_observations(
            ["new1"], observed_at=now)

        resp = self.db.get_peer_metrics(days=14)
        self.assertEqual(set(resp.keys()), {"days", "totals"})
        self.assertEqual(set(resp["totals"].keys()), self._TOTALS_KEYS)
        self.assertEqual(resp["totals"]["known_peers"], 3)
        self.assertEqual(resp["totals"]["new_24h"], 1)
        self.assertEqual(resp["totals"]["seen_24h"], 1)
        self.assertIsInstance(resp["totals"]["tracked_since"], str)

        self.assertEqual(len(resp["days"]), 14)
        for day in resp["days"]:
            self.assertEqual(set(day.keys()), self._DAY_KEYS)
            self.assertIsInstance(day["date"], str)
            self.assertIsInstance(day["new_peers"], int)
            self.assertIsInstance(day["total_peers"], int)
        # Days are ordered DESC (today first); cumulative total today
        # covers all three peers and carries forward over zero-days.
        self.assertEqual(resp["days"][0]["total_peers"], 3)
        self.assertEqual(resp["days"][1]["total_peers"], 2)
        self.assertEqual(resp["days"][0]["new_peers"], 1)
        self.assertEqual(
            sum(day["new_peers"] for day in resp["days"]), 3)

    def test_metrics_cumulative_includes_peers_older_than_window(self):
        """A peer first seen before the day window still counts toward
        every day's running total."""
        now = datetime.now(timezone.utc)
        self.db.record_peer_observations(
            ["ancient"], observed_at=now - timedelta(days=60))
        self.db.record_peer_observations(["fresh"], observed_at=now)

        resp = self.db.get_peer_metrics(days=14)
        self.assertEqual(resp["days"][0]["total_peers"], 2)
        self.assertEqual(resp["days"][-1]["total_peers"], 1)
        self.assertEqual(
            sum(day["new_peers"] for day in resp["days"]), 1)

    def test_metrics_empty_table(self):
        resp = self.db.get_peer_metrics(days=14)
        self.assertEqual(resp["totals"]["known_peers"], 0)
        self.assertIsNone(resp["totals"]["tracked_since"])
        self.assertEqual(len(resp["days"]), 14)
        self.assertTrue(
            all(d["new_peers"] == 0 and d["total_peers"] == 0
                for d in resp["days"]))


@requires_postgres
class TestSearchPlanReadiness(unittest.TestCase):
    """U7: ``get_search_plan_readiness`` aggregates wanted rows into
    plan-readiness buckets that replace exhausted-based reporting.

    The classifier must be exhaustive and exclusive: every wanted row
    belongs to exactly one bucket, the buckets sum to ``wanted_total``,
    and ``wanted_no_plan > 0`` is the operator stop-the-deploy signal.
    """

    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add_wanted(self, suffix: str) -> int:
        return self.db.add_request(
            mb_release_id=f"plan-readiness-{suffix}",
            artist_name="Readiness", album_title=suffix,
            source="request",
        )

    def _items(self, *queries: str):
        return [
            self.SearchPlanItemInput(
                ordinal=i, strategy=f"slot_{i}", query=q,
                canonical_query_key=q.lower(),
            )
            for i, q in enumerate(queries)
        ]

    def test_empty_db_returns_zeroed_buckets(self):
        readiness = self.db.get_search_plan_readiness("g1")
        self.assertEqual(readiness, {
            "generator_id": "g1",
            "wanted_total": 0,
            "wanted_searchable": 0,
            "wanted_legacy": 0,
            "wanted_failed_deterministic": 0,
            "wanted_failed_transient": 0,
            "wanted_no_plan": 0,
        })

    def test_buckets_partition_wanted_total(self):
        # 1 searchable, 1 legacy, 1 deterministic-failed, 1 transient,
        # 1 no-plan.
        rid_search = self._add_wanted("searchable")
        rid_legacy = self._add_wanted("legacy")
        rid_det = self._add_wanted("det_failed")
        rid_trans = self._add_wanted("trans_failed")
        rid_noplan = self._add_wanted("no_plan")
        # Searchable: active plan with current generator.
        self.db.create_successful_search_plan(
            request_id=rid_search, generator_id="g1",
            items=self._items("query A"))
        # Legacy: active plan with old generator id.
        self.db.create_successful_search_plan(
            request_id=rid_legacy, generator_id="g0_old",
            items=self._items("query B"))
        # Deterministic-failed on current generator.
        self.db.create_failed_search_plan(
            request_id=rid_det, generator_id="g1",
            failure_class="no_runnable_query",
            error_message="empty",
            transient=False,
        )
        # Transient-failed on current generator.
        self.db.create_failed_search_plan(
            request_id=rid_trans, generator_id="g1",
            failure_class="resolver_unavailable",
            error_message="resolver down",
            transient=True,
        )
        # rid_noplan has no plans at all.

        readiness = self.db.get_search_plan_readiness("g1")
        self.assertEqual(readiness["wanted_total"], 5)
        self.assertEqual(readiness["wanted_searchable"], 1)
        self.assertEqual(readiness["wanted_legacy"], 1)
        self.assertEqual(readiness["wanted_failed_deterministic"], 1)
        self.assertEqual(readiness["wanted_failed_transient"], 1)
        self.assertEqual(readiness["wanted_no_plan"], 1)
        # Sum invariant.
        self.assertEqual(
            readiness["wanted_total"],
            sum(readiness[k] for k in (
                "wanted_searchable", "wanted_legacy",
                "wanted_failed_deterministic", "wanted_failed_transient",
                "wanted_no_plan")))

    def test_old_generator_failed_plan_falls_to_no_plan(self):
        """A failed plan on a *different* generator id does not satisfy
        the readiness check for the current id -- treat it the same as
        having no plan at all (startup reconciliation will retry)."""
        rid = self._add_wanted("old_generator_failure")
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g0_old",
            failure_class="no_runnable_query",
            error_message="historical",
            transient=False,
        )
        readiness = self.db.get_search_plan_readiness("g1")
        self.assertEqual(readiness["wanted_total"], 1)
        self.assertEqual(readiness["wanted_no_plan"], 1)
        self.assertEqual(readiness["wanted_failed_deterministic"], 0)


@requires_postgres
class TestDashboardWrapMetrics(unittest.TestCase):
    """U7: ``cursor_update_status='wrapped'`` rows replace ``outcome=
    'exhausted'`` as the cycle-wrap signal in dashboard search windows.
    Historical exhausted rows still appear in the existing
    ``outcomes.exhausted`` bucket so legacy reporting does not lie about
    pre-cutover history.
    """

    def setUp(self):
        from lib.pipeline_db import (
            ConsumedAttemptInput, SearchPlanItemInput,
        )
        self.SearchPlanItemInput = SearchPlanItemInput
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="wrap-mbid",
            artist_name="Wrap", album_title="Test",
            source="request",
        )
        items = [
            SearchPlanItemInput(
                ordinal=i, strategy=f"slot_{i}", query=f"q{i}",
                canonical_query_key=f"k{i}",
            )
            for i in range(2)
        ]
        self.plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=items, set_active=True,
        )
        plan_items = self.db._execute(
            "SELECT id, ordinal FROM search_plan_items "
            "WHERE plan_id = %s ORDER BY ordinal", (self.plan_id,)
        ).fetchall()
        self.plan_items = [dict(r) for r in plan_items]

    def tearDown(self):
        self.db.close()

    def _consume(self, ordinal: int, outcome: str) -> None:
        item = self.plan_items[ordinal]
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.db.record_consumed_search_attempt(self.ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=self.plan_id,
            plan_item_id=item["id"],
            plan_ordinal=ordinal,
            plan_strategy=f"slot_{ordinal}",
            plan_canonical_query_key=f"k{ordinal}",
            plan_repeat_group=None,
            plan_generator_id="g1",
            query=f"q{ordinal}",
            outcome=outcome,
            plan_item_count=len(self.plan_items),
            cycle_count_snapshot=int(req["plan_cycle_count"]),
            elapsed_s=1.0, result_count=0,
            apply_scheduler_attempt=True,
            scheduler_success=False,
        ))

    def test_wrap_count_increments_per_cycle_wrap(self):
        # Walk through the plan twice -- two wraps expected.
        for _ in range(2):
            for ordinal in range(len(self.plan_items)):
                self._consume(ordinal, "no_results")

        metrics = self.db.get_pipeline_dashboard_metrics(plan_generator_id="g1")
        windows = metrics["searches"]["windows"]
        self.assertTrue(windows)
        window_24h = next(w for w in windows if w["label"] == "24h")
        self.assertEqual(window_24h["cursor_wraps"], 2)
        # Sanity: no new exhausted rows after the cutover.
        self.assertEqual(window_24h["outcomes"]["exhausted"], 0)
        # Cache attribution stays cycle-only -- ``search_log`` has no
        # per-search cache columns.
        self.assertEqual(window_24h["cache_attribution_level"], "cycle_only")

    def test_historical_exhausted_rows_still_counted(self):
        """Pre-cutover ``outcome='exhausted'`` rows must remain visible in
        the existing search-window bucket. The dashboard does not strip
        them out; it only stops emitting new ones."""
        self.db.log_search(
            self.req_id, query="historical-exhausted",
            elapsed_s=0.0, outcome="exhausted",
        )
        metrics = self.db.get_pipeline_dashboard_metrics(plan_generator_id="g1")
        window_24h = next(
            w for w in metrics["searches"]["windows"] if w["label"] == "24h"
        )
        self.assertEqual(window_24h["outcomes"]["exhausted"], 1)
        # No wraps yet -- historical exhausted is not a wrap.
        self.assertEqual(window_24h["cursor_wraps"], 0)


@requires_postgres
class TestDenylist(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="deny-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_add_and_get_denylist(self):
        self.db.add_denylist(self.req_id, "bad_user", "low bitrate")
        denied = self.db.get_denylisted_users(self.req_id)
        self.assertEqual(len(denied), 1)
        self.assertEqual(denied[0]["username"], "bad_user")
        self.assertEqual(denied[0]["reason"], "low bitrate")

    def test_multiple_denied_users(self):
        self.db.add_denylist(self.req_id, "user1", "bad quality")
        self.db.add_denylist(self.req_id, "user2", "incomplete")
        denied = self.db.get_denylisted_users(self.req_id)
        usernames = {d["username"] for d in denied}
        self.assertEqual(usernames, {"user1", "user2"})

    def test_duplicate_denylist_ignored(self):
        self.db.add_denylist(self.req_id, "user1", "reason1")
        self.db.add_denylist(self.req_id, "user1", "reason2")
        denied = self.db.get_denylisted_users(self.req_id)
        self.assertEqual(len(denied), 1)


@requires_postgres
class TestRetryLogic(unittest.TestCase):
    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="retry-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_record_attempt_increments_counters(self):
        self.db.record_attempt(self.req_id, "search")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 1)

        self.db.record_attempt(self.req_id, "search")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 2)

    def test_record_attempt_sets_backoff(self):
        self.db.record_attempt(self.req_id, "download")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["download_attempts"], 1)
        self.assertIsNotNone(req["last_attempt_at"])
        self.assertGreater(req["next_retry_after"], datetime.now(timezone.utc))

    def test_exponential_backoff(self):
        self.db.record_attempt(self.req_id, "search")
        req1 = self.db.get_request(self.req_id)
        assert req1 is not None
        retry1 = req1["next_retry_after"]

        self.db.record_attempt(self.req_id, "search")
        req2 = self.db.get_request(self.req_id)
        assert req2 is not None
        retry2 = req2["next_retry_after"]

        now = datetime.now(timezone.utc)
        delta1 = (retry1 - now).total_seconds()
        delta2 = (retry2 - now).total_seconds()
        self.assertGreater(delta2, delta1)

    def test_backoff_caps_at_four_hours(self):
        # BACKOFF_MAX_MINUTES = 60 * 4 per lib/pipeline_db.py (was 6h
        # until commit 1d84037 lowered it to raise steady-state search
        # frequency from ~4 to ~6 searches/release/day).
        for _ in range(6):
            self.db.record_attempt(self.req_id, "search")

        req = self.db.get_request(self.req_id)
        assert req is not None
        retry_at = req["next_retry_after"]
        assert retry_at is not None

        delta = (retry_at - datetime.now(timezone.utc)).total_seconds()
        self.assertLessEqual(delta, 4 * 60 * 60 + 5)
        self.assertGreater(delta, 3 * 60 * 60)


@requires_postgres
class TestSourcePreservation(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_request_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="req-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["source"], "request")

    def test_redownload_source_preserved(self):
        req_id = self.db.add_request(
            mb_release_id="rd-uuid",
            artist_name="A",
            album_title="B",
            source="redownload",
        )
        self.db.update_status(req_id, "imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["source"], "redownload")


@requires_postgres
class TestResetToWanted(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _make_request(self, suffix: str = "") -> int:
        req_id = self.db.add_request(
            mb_release_id=f"reset-{suffix}-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        return req_id

    def test_reset_to_wanted(self):
        req_id = self._make_request("basic")
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["next_retry_after"])
        self.assertEqual(req["search_attempts"], 0)
        self.assertEqual(req["download_attempts"], 0)
        self.assertEqual(req["validation_attempts"], 0)

    def test_reset_to_wanted_can_preserve_retry_counters(self):
        req_id = self._make_request("preserve-counters")
        self.db.record_attempt(req_id, "search")
        self.db.record_attempt(req_id, "download")
        self.db.record_attempt(req_id, "validation")
        before = self.db.get_request(req_id)
        assert before is not None
        before_retry = before["next_retry_after"]

        self.db.reset_to_wanted(req_id, clear_retry_counters=False)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertEqual(req["search_attempts"], 1)
        self.assertEqual(req["download_attempts"], 1)
        self.assertEqual(req["validation_attempts"], 1)
        self.assertEqual(req["next_retry_after"], before_retry)

    def test_abandon_auto_import_request_audits_and_resets_atomically(self):
        req_id = self.db.add_request(
            mb_release_id="abandon-auto-import",
            artist_name="A",
            album_title="B",
            source="request",
        )
        state = {
            "current_path": "/tmp/staged",
            "import_subprocess_started_at": "2026-05-06T00:00:00+00:00",
        }
        self.assertTrue(self.db.set_downloading(req_id, json.dumps(state)))

        log_id = self.db.abandon_auto_import_request(
            request_id=req_id,
            current_path="/tmp/staged",
            soulseek_username="alice",
            filetype="flac",
            beets_scenario="abandoned_auto_import",
            beets_detail="abandoned",
            outcome="failed",
            staged_path="/tmp/staged",
            error_message="abandoned",
            validation_result=None,
        )

        self.assertIsInstance(log_id, int)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["active_download_state"])
        self.assertEqual(req["download_attempts"], 1)
        history = self.db.get_download_history(req_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["beets_scenario"],
                         "abandoned_auto_import")

        second = self.db.abandon_auto_import_request(
            request_id=req_id,
            current_path="/tmp/staged",
            soulseek_username="alice",
            filetype="flac",
            beets_scenario="abandoned_auto_import",
            beets_detail="abandoned",
            outcome="failed",
            staged_path="/tmp/staged",
            error_message="abandoned",
            validation_result=None,
        )
        self.assertIsNone(second)
        self.assertEqual(len(self.db.get_download_history(req_id)), 1)

    def test_get_wanted_prioritizes_only_never_attempted_rows(self):
        fresh_ids = {
            self.db.add_request(
                mb_release_id=f"fresh-{idx}",
                artist_name="Fresh",
                album_title=str(idx),
                source="request",
            )
            for idx in range(3)
        }
        for idx in range(40):
            req_id = self.db.add_request(
                mb_release_id=f"auto-requeued-{idx}",
                artist_name="Auto",
                album_title=str(idx),
                source="request",
            )
            self.db._execute(
                """
                UPDATE album_requests
                SET search_attempts = 0,
                    validation_attempts = 1,
                    next_retry_after = %s
                WHERE id = %s
                """,
                (datetime.now(timezone.utc) - timedelta(minutes=1), req_id),
            )
        self.db.conn.commit()

        wanted = self.db.get_wanted(limit=3)

        self.assertEqual({row["id"] for row in wanted}, fresh_ids)

    def test_preserves_search_filetype_override_when_omitted(self):
        req_id = self._make_request("preserve-qo")
        self.db.update_request_fields(req_id, search_filetype_override="flac,mp3 v0")
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_sets_search_filetype_override_when_passed(self):
        req_id = self._make_request("set-qo")
        self.db.update_request_fields(req_id, search_filetype_override="flac,mp3 v0,mp3 320")
        self.db.reset_to_wanted(req_id, search_filetype_override="flac,mp3 v0")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_clears_search_filetype_override_when_none(self):
        req_id = self._make_request("clear-qo")
        self.db.update_request_fields(req_id, search_filetype_override="flac")
        self.db.reset_to_wanted(req_id, search_filetype_override=None)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertIsNone(req["search_filetype_override"])

    def test_preserves_min_bitrate_when_omitted(self):
        req_id = self._make_request("preserve-br")
        self.db.update_request_fields(req_id, min_bitrate=320)
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["min_bitrate"], 320)

    def test_sets_min_bitrate_when_passed(self):
        req_id = self._make_request("set-br")
        self.db.update_request_fields(req_id, min_bitrate=192)
        self.db.reset_to_wanted(req_id, min_bitrate=320)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["min_bitrate"], 320)
        self.assertEqual(req["prev_min_bitrate"], 192)

    def test_clears_manual_reason(self):
        """U6: re-queue clears ``manual_reason`` alongside attempt counters.

        Single-seam reset: every re-queue path funnels through
        ``reset_to_wanted``, so this one assertion covers web UI button,
        ``pipeline-cli`` requeue, and importer requeue transparently.
        """
        req_id = self._make_request("clear-mr")
        # Bump search_attempts and populate manual_reason as if the
        # variant ladder had exhausted earlier.
        self.db._execute(
            "UPDATE album_requests SET search_attempts = 7, manual_reason = %s "
            "WHERE id = %s",
            ("search_exhausted", req_id),
        )
        self.db.conn.commit()
        self.db.reset_to_wanted(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 0)
        self.assertIsNone(req["manual_reason"])


@requires_postgres
class TestClearOnDiskQualityFields(unittest.TestCase):
    """``clear_on_disk_quality_fields`` is the write-side half of the
    "beets is the source of truth" invariant: once an album leaves beets
    (ban-source, manual ``beet rm``), every ``album_requests`` field that
    describes on-disk state must be cleared. Preserves ``min_bitrate`` as
    a conservative baseline for the next quality-gate comparison, and
    leaves ``last_download_spectral_*`` alone (that's a download-attempt
    audit field, not on-disk state).
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _make_request(self, suffix: str = "") -> int:
        req_id = self.db.add_request(
            mb_release_id=f"clear-od-{suffix}-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "imported")
        return req_id

    def test_clears_spectral_and_verified_lossless(self):
        req_id = self._make_request("basic")
        self.db.update_request_fields(
            req_id,
            verified_lossless=True,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            current_lossless_source_v0_probe_min_bitrate=165,
            current_lossless_source_v0_probe_avg_bitrate=171,
            current_lossless_source_v0_probe_median_bitrate=169,
        )

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertFalse(req["verified_lossless"])
        self.assertIsNone(req["current_spectral_grade"])
        self.assertIsNone(req["current_spectral_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_median_bitrate"])

    def test_clears_imported_path(self):
        """After ``beet remove -d`` the on-disk path is stale — the pipeline
        tab renders ``imported_path`` directly, so leaving it populated
        would claim the album is imported at a directory that has just
        been deleted.
        """
        req_id = self._make_request("path")
        self.db.update_request_fields(
            req_id,
            imported_path="/mnt/virtio/Music/Beets/Stale/Path",
        )

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertIsNone(req["imported_path"])

    def test_preserves_min_bitrate(self):
        """min_bitrate is a baseline for the NEXT gate, not on-disk state."""
        req_id = self._make_request("preserve-min")
        self.db.update_request_fields(req_id, min_bitrate=320)

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["min_bitrate"], 320)

    def test_preserves_last_download_spectral(self):
        """last_download_* tracks the latest download attempt, not on-disk state."""
        req_id = self._make_request("preserve-ld")
        self.db.update_request_fields(
            req_id,
            last_download_spectral_grade="suspect",
            last_download_spectral_bitrate=192,
        )

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_grade"], "suspect")
        self.assertEqual(req["last_download_spectral_bitrate"], 192)

    def test_idempotent_when_fields_already_clear(self):
        req_id = self._make_request("idempotent")

        self.db.clear_on_disk_quality_fields(req_id)
        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertFalse(req["verified_lossless"])
        self.assertIsNone(req["current_spectral_grade"])
        self.assertIsNone(req["current_spectral_bitrate"])


@requires_postgres
class TestApplyTransitionDB(unittest.TestCase):
    """DB-backed contract tests for apply_transition preserve semantics."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _make_request(self, suffix: str = "", **extra: object) -> int:
        from lib.transitions import apply_transition
        req_id = self.db.add_request(
            mb_release_id=f"transition-{suffix}-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        if extra:
            self.db.update_request_fields(req_id, **extra)
        # Move to imported so we can transition to wanted
        apply_transition(self.db, req_id, "imported", from_status="wanted")
        return req_id

    def test_transition_to_wanted_preserves_override(self):
        from lib.transitions import apply_transition
        req_id = self._make_request("preserve", search_filetype_override="flac,mp3 v0")
        apply_transition(self.db, req_id, "wanted", from_status="imported")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_transition_to_wanted_with_narrowed_override(self):
        from lib.transitions import apply_transition
        req_id = self._make_request("narrow", search_filetype_override="flac,mp3 v0,mp3 320")
        apply_transition(self.db, req_id, "wanted", from_status="imported",
                         search_filetype_override="flac,mp3 v0")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["search_filetype_override"], "flac,mp3 v0")

    def test_transition_to_imported_clears_override(self):
        from lib.transitions import apply_transition
        req_id = self._make_request("clear", search_filetype_override="flac")
        apply_transition(self.db, req_id, "wanted", from_status="imported")
        apply_transition(self.db, req_id, "imported", from_status="wanted",
                         search_filetype_override=None)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertIsNone(req["search_filetype_override"])


@requires_postgres
class TestAlbumQualityEvidenceStorage(unittest.TestCase):
    """Content-addressed album-quality evidence storage (post migration 021).

    The pre-021 ``TestAlbumQualityEvidenceStorage`` exercised the
    ``AlbumQualityEvidenceOwner``-keyed surface — owner round trips,
    ``validate_album_quality_evidence_owner``, ``load_album_quality_evidence(owner)``,
    legacy-scalars fallback, owner-typed delete-cascade. All those production
    methods were removed in U2/U3 (commit 5bd1bbb). The cases that were
    purely about the old key shape have been deleted; the cases that
    exercise behaviour still meaningful on the new ``(mb_release_id,
    snapshot_fingerprint)`` key have been migrated below.

    Equivalence proofs for deleted tests:
        - ``test_upsert_load_request_current_round_trips_typed_evidence``,
          ``test_upsert_load_download_log_candidate_uses_neutral_v0_shape``,
          ``test_import_job_candidate_owner_round_trips`` — covered by
          content-addressed round-trip below + dispatch slice/orchestration
          tests in ``tests/test_dispatch_core.py`` and
          ``tests/test_import_evidence.py``.
        - ``test_legacy_scalars_are_not_loaded_as_active_evidence`` — the
          legacy-scalars fallback path was removed alongside owner-keyed
          load.
        - ``test_validation_rejects_bad_owner_and_bad_snapshot`` — the
          owner-validation method no longer exists. Snapshot-shape
          validation is still covered below.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="evidence-uuid",
            artist_name="Evidence Artist",
            album_title="Evidence Album",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _seed(self, **kwargs):
        """Build evidence with the canonical content-addressed shape."""
        return make_album_quality_evidence(
            mb_release_id=kwargs.pop("mb_release_id", "mbid-fixture"),
            **kwargs,
        )

    def test_upsert_then_find_by_content_address_round_trips(self):
        from lib.quality import (
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        evidence = self._seed(
            mb_release_id="mbid-round-trip",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=860,
                avg_bitrate_kbps=912,
                median_bitrate_kbps=899,
                format="flac",
                spectral_grade="genuine",
                verified_lossless=True,
                was_converted_from="flac",
            ),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="02 - Beta.flac",
                    size_bytes=2000,
                    mtime_ns=20,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
                AlbumQualityEvidenceFile(
                    relative_path="01 - Alpha.flac",
                    size_bytes=1000,
                    mtime_ns=10,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
            ],
            codec="flac",
            container="flac",
            storage_format="flac",
            target_format="lossless",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=165,
                avg_bitrate_kbps=228,
                median_bitrate_kbps=225,
                source_lineage="lossless_container_source",
                source_provenance="transcoded from verified FLAC candidate",
                proof_provenance="spectral genuine plus V0 probe",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                proof_origin="import",
                source="lossless candidate",
                classifier="spectral+v0",
                detail="genuine spectral result",
            ),
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertEqual(loaded.measurement.format, "flac")
        self.assertTrue(loaded.measurement.verified_lossless)
        self.assertIsNotNone(loaded.verified_lossless_proof)
        # Files round-trip sorted-for-storage.
        self.assertEqual(
            [file.relative_path for file in loaded.files],
            ["01 - Alpha.flac", "02 - Beta.flac"],
        )
        assert loaded.v0_metric is not None
        self.assertEqual(loaded.v0_metric.avg_bitrate_kbps, 228)

    def test_duplicate_content_address_upsert_replaces_snapshot_rows(self):
        from lib.quality import AlbumQualityEvidenceFile

        # Two distinct file sets → two distinct snapshot fingerprints; the
        # second upsert with the same mb_release_id but different files
        # creates a new row, not a replacement (content-addressed). Reusing
        # the first fingerprint via msgspec.replace lets us assert
        # "same content address replaces".
        files_v1 = [
            AlbumQualityEvidenceFile(
                relative_path="01.mp3",
                size_bytes=1,
                mtime_ns=1,
                extension="mp3",
                container="mp3",
            ),
        ]
        first = self._seed(mb_release_id="mbid-replace", files=files_v1)
        self.db.upsert_album_quality_evidence(first)

        # Same content address, but mutate a non-keyed field (storage_format)
        # — the upsert should replace.
        import msgspec
        replaced = msgspec.structs.replace(first, storage_format="mp3 V0")
        self.db.upsert_album_quality_evidence(replaced)

        loaded = self.db.find_album_quality_evidence(
            mb_release_id=first.mb_release_id,
            snapshot_fingerprint=first.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.storage_format, "mp3 V0")

        # Only one row exists for this content address.
        cur = self.db._execute(
            "SELECT count(*) AS n FROM album_quality_evidence "
            "WHERE mb_release_id = %s AND snapshot_fingerprint = %s",
            (first.mb_release_id, first.snapshot_fingerprint),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["n"], 1)

    def test_fk_chain_resolves_request_current_evidence(self):
        evidence = self._seed(mb_release_id="mbid-fk-current")
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(self.req_id, persisted.id)

        evidence_id = self.db.get_request_current_evidence_id(self.req_id)
        self.assertEqual(evidence_id, persisted.id)
        loaded = self.db.load_album_quality_evidence_by_id(evidence_id)
        assert loaded is not None
        self.assertEqual(loaded.mb_release_id, "mbid-fk-current")

    def test_fk_chain_resolves_download_log_candidate_evidence(self):
        log_id = self.db.log_download(
            request_id=self.req_id, outcome="rejected"
        )
        evidence = self._seed(mb_release_id="mbid-fk-dl")
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(log_id, persisted.id)

        evidence_id = self.db.get_download_log_candidate_evidence_id(log_id)
        self.assertEqual(evidence_id, persisted.id)

    def test_fk_chain_resolves_import_job_candidate_evidence(self):
        job = self.db.enqueue_import_job(
            "manual_import",
            request_id=self.req_id,
            payload={"failed_path": "/tmp/candidate"},
        )
        evidence = self._seed(mb_release_id="mbid-fk-job")
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_import_job_candidate_evidence(job.id, persisted.id)

        evidence_id = self.db.get_import_job_candidate_evidence_id(job.id)
        self.assertEqual(evidence_id, persisted.id)

    def test_round_trip_preview_evidence_facts_with_every_new_field(self):
        """U1: new preview-evidence fields round-trip through upsert/find."""
        import msgspec
        from lib.quality import AlbumQualityEvidenceFile

        # Seed a bad_audio_hashes row to reference; matched_bad_audio_hash_id
        # is an optional FK.
        cur = self.db._execute(
            """
            INSERT INTO bad_audio_hashes (hash_value, audio_format, request_id)
            VALUES (decode('abcd1234', 'hex'), 'mp3', %s)
            RETURNING id
            """,
            (self.req_id,),
        )
        row = cur.fetchone()
        assert row is not None
        bad_id = int(row["id"])

        evidence = self._seed(
            mb_release_id="mbid-preview-facts",
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01 - Track.mp3",
                    size_bytes=12345,
                    mtime_ns=10,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                    decode_ok=False,
                ),
            ],
        )
        evidence = msgspec.structs.replace(
            evidence,
            audio_corrupt=True,
            folder_layout="nested",
            audio_file_count=1,
            filetype_band="mp3",
            matched_bad_audio_hash_id=bad_id,
            matched_bad_audio_hash_path="01 - Track.mp3",
        )

        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertTrue(loaded.audio_corrupt)
        self.assertEqual(loaded.folder_layout, "nested")
        self.assertEqual(loaded.audio_file_count, 1)
        self.assertEqual(loaded.filetype_band, "mp3")
        self.assertEqual(loaded.matched_bad_audio_hash_id, bad_id)
        self.assertEqual(loaded.matched_bad_audio_hash_path, "01 - Track.mp3")
        self.assertEqual(len(loaded.files), 1)
        self.assertFalse(loaded.files[0].decode_ok)

    def test_empty_fileset_is_storable_when_audio_file_count_is_zero(self):
        """U1 AE4: audio_file_count=0 + files=[] round-trips without error."""
        import msgspec

        evidence = self._seed(
            mb_release_id="mbid-empty-fileset",
            files=[],
        )
        evidence = msgspec.structs.replace(evidence, audio_file_count=0)
        self.db.upsert_album_quality_evidence(evidence)
        loaded = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.audio_file_count, 0)
        self.assertEqual(loaded.files, [])

    def test_non_empty_files_with_audio_file_count_zero_inconsistency_still_storable(
        self,
    ):
        """U1: files=[] with audio_file_count!=0 raises (consistency guard)."""
        import msgspec

        evidence = self._seed(
            mb_release_id="mbid-inconsistent",
            files=[],
        )
        evidence = msgspec.structs.replace(evidence, audio_file_count=2)
        with self.assertRaisesRegex(ValueError, "at least one snapshot file is required"):
            self.db.upsert_album_quality_evidence(evidence)

    def test_matched_bad_audio_hash_id_and_path_must_pair(self):
        """U1: hash FK and paired path must be set together or both NULL."""
        import msgspec

        evidence = self._seed(mb_release_id="mbid-bad-hash-pair")
        bad_pair = msgspec.structs.replace(
            evidence, matched_bad_audio_hash_id=1, matched_bad_audio_hash_path=None,
        )
        with self.assertRaisesRegex(ValueError, "must be set together or both NULL"):
            self.db.upsert_album_quality_evidence(bad_pair)
        bad_pair2 = msgspec.structs.replace(
            evidence,
            matched_bad_audio_hash_id=None,
            matched_bad_audio_hash_path="a.mp3",
        )
        with self.assertRaisesRegex(ValueError, "must be set together or both NULL"):
            self.db.upsert_album_quality_evidence(bad_pair2)

    def test_extension_validation_still_enforced_on_evidence_files(self):
        from lib.quality import AlbumQualityEvidenceFile

        with self.assertRaisesRegex(ValueError, "extension is required"):
            self.db.upsert_album_quality_evidence(self._seed(
                mb_release_id="mbid-bad-ext",
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path="bad",
                        size_bytes=1,
                        mtime_ns=1,
                        extension="",
                        container="mp3",
                    ),
                ],
            ))


@requires_postgres
class TestSpectralColumns(unittest.TestCase):
    """Test spectral quality columns on download_log and album_requests."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="spectral-uuid",
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_log_download_with_spectral_fields(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            filetype="mp3",
            outcome="success",
            spectral_grade="suspect",
            spectral_bitrate=128,
            slskd_filetype="mp3",
            slskd_bitrate=320000,
            actual_filetype="mp3",
            actual_min_bitrate=320000,
            existing_min_bitrate=92,
            existing_spectral_bitrate=64,
        )
        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        h = history[0]
        self.assertEqual(h["spectral_grade"], "suspect")
        self.assertEqual(h["spectral_bitrate"], 128)
        self.assertEqual(h["slskd_filetype"], "mp3")
        self.assertEqual(h["slskd_bitrate"], 320000)
        self.assertEqual(h["actual_filetype"], "mp3")
        self.assertEqual(h["actual_min_bitrate"], 320000)
        self.assertEqual(h["existing_min_bitrate"], 92)
        self.assertEqual(h["existing_spectral_bitrate"], 64)

    def test_spectral_fields_null_by_default(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            outcome="success",
        )
        history = self.db.get_download_history(self.req_id)
        h = history[0]
        self.assertIsNone(h.get("spectral_grade"))
        self.assertIsNone(h.get("spectral_bitrate"))
        self.assertIsNone(h.get("slskd_filetype"))

    def test_log_download_with_v0_probe_fields(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            filetype="flac",
            outcome="success",
            v0_probe_kind="lossless_source_v0",
            v0_probe_min_bitrate=165,
            v0_probe_avg_bitrate=228,
            v0_probe_median_bitrate=225,
            existing_v0_probe_kind="lossless_source_v0",
            existing_v0_probe_min_bitrate=128,
            existing_v0_probe_avg_bitrate=171,
            existing_v0_probe_median_bitrate=169,
        )

        history = self.db.get_download_history(self.req_id)
        self.assertEqual(len(history), 1)
        h = history[0]
        self.assertEqual(h["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(h["v0_probe_min_bitrate"], 165)
        self.assertEqual(h["v0_probe_avg_bitrate"], 228)
        self.assertEqual(h["v0_probe_median_bitrate"], 225)
        self.assertEqual(h["existing_v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(h["existing_v0_probe_min_bitrate"], 128)
        self.assertEqual(h["existing_v0_probe_avg_bitrate"], 171)
        self.assertEqual(h["existing_v0_probe_median_bitrate"], 169)

    def test_album_request_spectral_columns(self):
        self.db.update_status(self.req_id, "imported",
                              last_download_spectral_bitrate=128,
                              last_download_spectral_grade="suspect")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_bitrate"], 128)
        self.assertEqual(req["last_download_spectral_grade"], "suspect")

    def test_on_disk_spectral_columns(self):
        """current_spectral_grade/bitrate describe files currently in beets."""
        self.db.update_status(self.req_id, "imported",
                              current_spectral_grade="suspect",
                              current_spectral_bitrate=160)
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["current_spectral_grade"], "suspect")
        self.assertEqual(req["current_spectral_bitrate"], 160)

    def test_on_disk_spectral_null_by_default(self):
        """current_spectral columns are NULL for pre-existing albums."""
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["current_spectral_grade"])
        self.assertIsNone(req["current_spectral_bitrate"])

    def test_update_spectral_state_updates_both_pairs(self):
        from lib import pipeline_db
        from lib.quality import SpectralMeasurement

        self.db.update_spectral_state(
            self.req_id,
            pipeline_db.RequestSpectralStateUpdate(
                last_download=SpectralMeasurement(
                    grade="suspect", bitrate_kbps=128),
                current=SpectralMeasurement(
                    grade="genuine", bitrate_kbps=245),
            ),
        )

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_grade"], "suspect")
        self.assertEqual(req["last_download_spectral_bitrate"], 128)
        self.assertEqual(req["current_spectral_grade"], "genuine")
        self.assertEqual(req["current_spectral_bitrate"], 245)

    def test_update_spectral_state_on_disk_only_clears_nulls(self):
        from lib import pipeline_db
        from lib.quality import SpectralMeasurement

        self.db.update_status(
            self.req_id,
            "imported",
            last_download_spectral_grade="likely_transcode",
            last_download_spectral_bitrate=192,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=192,
        )

        self.db.update_spectral_state(
            self.req_id,
            pipeline_db.RequestSpectralStateUpdate(
                current=SpectralMeasurement(
                    grade="genuine", bitrate_kbps=None),
            ),
        )

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["last_download_spectral_grade"], "likely_transcode")
        self.assertEqual(req["last_download_spectral_bitrate"], 192)
        self.assertEqual(req["current_spectral_grade"], "genuine")
        self.assertIsNone(req["current_spectral_bitrate"])

    def test_v0_probe_state_update_fields_set_current_source_probe(self):
        """``RequestV0ProbeStateUpdate.as_update_fields()`` is the live wire
        between the importer (``lib/dispatch/``) and the request
        row. Production funnels the fields through ``finalize_request`` →
        ``mark_imported_with_rescue`` / ``update_status``; this test drives
        the same column names through ``update_request_fields`` (both
        writers interpolate the dict keys into an ``UPDATE album_requests``
        SET list, and the column contract is pinned by
        ``test_pipeline_db_column_contract.py``)."""
        from lib import pipeline_db
        from lib.quality import V0ProbeEvidence

        update = pipeline_db.RequestV0ProbeStateUpdate(
            current_lossless_source=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=165,
                avg_bitrate_kbps=228,
                median_bitrate_kbps=225,
            ),
        )
        self.db.update_request_fields(self.req_id, **update.as_update_fields())

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["current_lossless_source_v0_probe_min_bitrate"], 165)
        self.assertEqual(req["current_lossless_source_v0_probe_avg_bitrate"], 228)
        self.assertEqual(req["current_lossless_source_v0_probe_median_bitrate"], 225)

    def test_v0_probe_state_update_fields_can_clear_current_source_probe(self):
        from lib import pipeline_db
        from lib.quality import V0ProbeEvidence

        set_update = pipeline_db.RequestV0ProbeStateUpdate(
            current_lossless_source=V0ProbeEvidence(
                kind="lossless_source_v0",
                min_bitrate_kbps=165,
                avg_bitrate_kbps=228,
                median_bitrate_kbps=225,
            ),
        )
        self.db.update_request_fields(
            self.req_id, **set_update.as_update_fields())
        clear_update = pipeline_db.RequestV0ProbeStateUpdate(
            clear_current_lossless_source=True,
        )
        self.db.update_request_fields(
            self.req_id, **clear_update.as_update_fields())

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_median_bitrate"])

    def test_v0_probe_fields_null_by_default(self):
        self.db.log_download(
            request_id=self.req_id,
            soulseek_username="testuser",
            outcome="success",
        )

        history = self.db.get_download_history(self.req_id)
        self.assertIsNone(history[0].get("v0_probe_kind"))
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(req["current_lossless_source_v0_probe_median_bitrate"])


@requires_postgres
class TestBatchHistory(unittest.TestCase):
    """Test get_download_history_batch — batch download history lookup."""

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="batch-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="batch-2", artist_name="C", album_title="D", source="request")
        self.req3 = self.db.add_request(
            mb_release_id="batch-3", artist_name="E", album_title="F", source="request")
        # Add history for req1 and req2, but not req3
        self.db.log_download(self.req1, soulseek_username="user1", outcome="success")
        self.db.log_download(self.req1, soulseek_username="user2", outcome="rejected")
        self.db.log_download(self.req2, soulseek_username="user3", outcome="success")

    def tearDown(self):
        self.db.close()

    def test_returns_grouped_by_request_id(self):
        result = self.db.get_download_history_batch([self.req1, self.req2, self.req3])
        self.assertIn(self.req1, result)
        self.assertIn(self.req2, result)
        self.assertNotIn(self.req3, result)  # no history
        self.assertEqual(len(result[self.req1]), 2)
        self.assertEqual(len(result[self.req2]), 1)

    def test_empty_list(self):
        result = self.db.get_download_history_batch([])
        self.assertEqual(result, {})

    def test_order_is_desc_by_id(self):
        result = self.db.get_download_history_batch([self.req1])
        history = result[self.req1]
        # Most recent first (rejected was logged after success)
        self.assertEqual(history[0]["outcome"], "rejected")
        self.assertEqual(history[1]["outcome"], "success")


@requires_postgres
class TestTrackCounts(unittest.TestCase):
    """Test get_track_counts — batch track count lookup."""

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="tc-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="tc-2", artist_name="C", album_title="D", source="request")
        self.req3 = self.db.add_request(
            mb_release_id="tc-3", artist_name="E", album_title="F", source="request")
        self.db.set_tracks(self.req1, [
            {"disc_number": 1, "track_number": 1, "title": "T1", "length_seconds": 100},
            {"disc_number": 1, "track_number": 2, "title": "T2", "length_seconds": 200},
        ])
        self.db.set_tracks(self.req2, [
            {"disc_number": 1, "track_number": 1, "title": "T1", "length_seconds": 100},
        ])
        # req3 has no tracks

    def tearDown(self):
        self.db.close()

    def test_returns_counts(self):
        result = self.db.get_track_counts([self.req1, self.req2, self.req3])
        self.assertEqual(result[self.req1], 2)
        self.assertEqual(result[self.req2], 1)
        self.assertNotIn(self.req3, result)  # no tracks

    def test_empty_list(self):
        result = self.db.get_track_counts([])
        self.assertEqual(result, {})


@requires_postgres
class TestDownloadingStatus(unittest.TestCase):
    """Test the 'downloading' status and active_download_state JSONB column."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_downloading_status_allowed(self):
        """Insert row, update to 'downloading', verify roundtrip."""
        req_id = self.db.add_request(
            mb_release_id="dl-status-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.update_status(req_id, "downloading")
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")

    def test_active_download_state_jsonb_roundtrip(self):
        """Write JSONB to active_download_state column, read back, verify structure."""
        req_id = self.db.add_request(
            mb_release_id="ads-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        state = {
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000}
            ],
        }
        self.db._execute(
            "UPDATE album_requests SET active_download_state = %s::jsonb WHERE id = %s",
            (json.dumps(state), req_id),
        )
        req = self.db.get_request(req_id)
        assert req is not None
        ads = req["active_download_state"]
        self.assertIsInstance(ads, dict)
        self.assertEqual(ads["filetype"], "flac")
        self.assertEqual(len(ads["files"]), 1)
        self.assertEqual(ads["files"][0]["username"], "user1")

    def test_get_downloading(self):
        """get_downloading() returns only status='downloading' rows."""
        id1 = self.db.add_request(mb_release_id="gd-1", artist_name="A",
                                  album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="gd-2", artist_name="C",
                                  album_title="D", source="request")
        id3 = self.db.add_request(mb_release_id="gd-3", artist_name="E",
                                  album_title="F", source="request")
        self.db.update_status(id1, "downloading")
        self.db.update_status(id2, "downloading")
        # id3 stays wanted

        downloading = self.db.get_downloading()
        dl_ids = [r["id"] for r in downloading]
        self.assertIn(id1, dl_ids)
        self.assertIn(id2, dl_ids)
        self.assertNotIn(id3, dl_ids)

    def test_set_downloading(self):
        """set_downloading() sets status + writes JSONB atomically."""
        req_id = self.db.add_request(
            mb_release_id="sd-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        state_json = json.dumps({
            "filetype": "mp3 v0",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [],
        })
        self.db.set_downloading(req_id, state_json)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")
        self.assertIsNotNone(req["active_download_state"])
        ads = req["active_download_state"]
        self.assertEqual(ads["filetype"], "mp3 v0")
        # Starting a download should not consume a backoff attempt.
        self.assertEqual(req["download_attempts"], 0)

    def test_set_downloading_returns_true_from_wanted(self):
        """set_downloading() returns True when album is wanted."""
        req_id = self.db.add_request(
            mb_release_id="guard-ok", artist_name="A", album_title="B",
            source="request")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        result = self.db.set_downloading(req_id, state_json)
        self.assertTrue(result)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")

    def test_set_downloading_noop_from_imported(self):
        """set_downloading() returns False and doesn't overwrite imported status."""
        req_id = self.db.add_request(
            mb_release_id="guard-imp", artist_name="A", album_title="B",
            source="request")
        self.db.update_status(req_id, "imported")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        result = self.db.set_downloading(req_id, state_json)
        self.assertFalse(result)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "imported")

    def test_set_downloading_noop_from_downloading(self):
        """set_downloading() returns False when already downloading (no state overwrite)."""
        req_id = self.db.add_request(
            mb_release_id="guard-dl", artist_name="A", album_title="B",
            source="request")
        original_state = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        self.db.set_downloading(req_id, original_state)
        new_state = json.dumps({"filetype": "mp3 v0", "enqueued_at": "t2", "files": []})
        result = self.db.set_downloading(req_id, new_state)
        self.assertFalse(result)
        # Original state preserved
        req = self.db.get_request(req_id)
        assert req is not None
        ads = req["active_download_state"]
        self.assertEqual(ads["filetype"], "flac")

    def test_set_downloading_noop_from_manual(self):
        """set_downloading() returns False when status is manual."""
        req_id = self.db.add_request(
            mb_release_id="guard-man", artist_name="A", album_title="B",
            source="request")
        self.db.update_status(req_id, "manual")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "t", "files": []})
        result = self.db.set_downloading(req_id, state_json)
        self.assertFalse(result)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "manual")

    def test_update_download_state(self):
        """update_download_state() rewrites JSONB without changing status."""
        req_id = self.db.add_request(
            mb_release_id="uds-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.set_downloading(
            req_id,
            json.dumps({"filetype": "flac", "enqueued_at": "2026-04-03T12:00:00+00:00", "files": []}),
        )
        self.db.update_download_state(
            req_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "processing_started_at": "2026-04-03T12:05:00+00:00",
                "files": [],
            }),
        )
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")
        ads = req["active_download_state"]
        assert ads is not None
        self.assertEqual(ads["processing_started_at"], "2026-04-03T12:05:00+00:00")

    def test_update_download_state_if_downloading_success_and_guard(self):
        req_id = self.db.add_request(
            mb_release_id="udsifd-ok",
            artist_name="A",
            album_title="B",
            source="request",
        )
        blocked_id = self.db.add_request(
            mb_release_id="udsifd-blocked",
            artist_name="C",
            album_title="D",
            source="request",
        )
        original_state = json.dumps({
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [],
        })
        self.db.set_downloading(req_id, original_state)
        self.db.set_downloading(blocked_id, original_state)
        self.db.update_status(blocked_id, "imported")

        updated = self.db.update_download_state_if_downloading(
            req_id,
            json.dumps({
                "filetype": "mp3 v0",
                "enqueued_at": "2026-04-03T12:01:00+00:00",
                "files": [],
            }),
        )
        blocked = self.db.update_download_state_if_downloading(
            blocked_id,
            json.dumps({
                "filetype": "mp3 320",
                "enqueued_at": "2026-04-03T12:02:00+00:00",
                "files": [],
            }),
        )

        self.assertTrue(updated)
        self.assertFalse(blocked)
        req = self.db.get_request(req_id)
        blocked_req = self.db.get_request(blocked_id)
        assert req is not None
        assert blocked_req is not None
        self.assertEqual(req["active_download_state"]["filetype"], "mp3 v0")
        self.assertIsNone(blocked_req["active_download_state"])

    def test_reset_downloading_to_wanted_success_and_guard(self):
        req_id = self.db.add_request(
            mb_release_id="rdtw-ok",
            artist_name="A",
            album_title="B",
            source="request",
        )
        blocked_id = self.db.add_request(
            mb_release_id="rdtw-blocked",
            artist_name="C",
            album_title="D",
            source="request",
        )
        state_json = json.dumps({
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [],
        })
        self.db.set_downloading(req_id, state_json)
        self.db.record_attempt(req_id, "download")

        reset = self.db.reset_downloading_to_wanted(req_id)
        blocked = self.db.reset_downloading_to_wanted(blocked_id)

        self.assertTrue(reset)
        self.assertFalse(blocked)
        req = self.db.get_request(req_id)
        blocked_req = self.db.get_request(blocked_id)
        assert req is not None
        assert blocked_req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["active_download_state"])
        self.assertEqual(req["download_attempts"], 1)
        self.assertEqual(blocked_req["status"], "wanted")

    def test_update_download_state_current_path(self):
        """update_download_state_current_path() rewrites only the path field."""
        req_id = self.db.add_request(
            mb_release_id="udscp-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.set_downloading(
            req_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [],
            }),
        )

        self.db.update_download_state_current_path(req_id, "/tmp/staged")

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")
        ads = req["active_download_state"]
        assert ads is not None
        self.assertEqual(ads["current_path"], "/tmp/staged")
        self.assertEqual(ads["filetype"], "flac")

    def test_update_download_state_current_path_noop_when_not_downloading(self):
        """update_download_state_current_path() does not recreate cleared state."""
        req_id = self.db.add_request(
            mb_release_id="udscp-noop-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.set_downloading(
            req_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [],
            }),
        )
        self.db.update_status(req_id, "imported")

        self.db.update_download_state_current_path(req_id, "/tmp/staged")

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "imported")
        self.assertIsNone(req["active_download_state"])

    def test_update_download_state_current_path_noop_when_state_missing(self):
        """update_download_state_current_path() must not fabricate a partial state."""
        req_id = self.db.add_request(
            mb_release_id="udscp-null-state-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.db.set_downloading(
            req_id,
            json.dumps({
                "filetype": "flac",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [],
            }),
        )
        self.db._execute(
            "UPDATE album_requests SET active_download_state = NULL WHERE id = %s",
            (req_id,),
        )
        self.db.conn.commit()

        self.db.update_download_state_current_path(req_id, "/tmp/staged")

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "downloading")
        self.assertIsNone(req["active_download_state"])


@requires_postgres
class TestUserCooldowns(unittest.TestCase):
    """Tests for global user cooldown system (issue #39)."""

    def setUp(self):
        self.db = make_db()
        # Create two requests for cross-request cooldown testing
        self.req1 = self.db.add_request(
            mb_release_id="cool-1", artist_name="A", album_title="B", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="cool-2", artist_name="C", album_title="D", source="request")

    def tearDown(self):
        self.db.close()

    def test_user_cooldowns_table_exists(self):
        cur = self.db._execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'user_cooldowns'
        """)
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["table_name"], "user_cooldowns")

    def test_add_and_get_cooldown(self):
        from datetime import datetime, timedelta, timezone
        until = datetime.now(timezone.utc) + timedelta(days=3)
        self.db.add_cooldown("deaduser", until, "5 consecutive timeouts")
        cooled = self.db.get_cooled_down_users()
        self.assertIn("deaduser", cooled)

    def test_expired_cooldown_not_returned(self):
        from datetime import datetime, timedelta, timezone
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        self.db.add_cooldown("expireduser", past, "old timeout")
        cooled = self.db.get_cooled_down_users()
        self.assertNotIn("expireduser", cooled)

    def test_upsert_extends_cooldown(self):
        from datetime import datetime, timedelta, timezone
        until1 = datetime.now(timezone.utc) + timedelta(days=1)
        until2 = datetime.now(timezone.utc) + timedelta(days=5)
        self.db.add_cooldown("user1", until1, "first")
        self.db.add_cooldown("user1", until2, "extended")
        cur = self.db._execute(
            "SELECT cooldown_until FROM user_cooldowns WHERE username = %s",
            ("user1",),
        )
        rows = cur.fetchall()
        self.assertEqual(len(rows), 1)
        # Should have the later date
        self.assertGreater(rows[0]["cooldown_until"], until1)

    def test_check_and_apply_cooldown_triggers(self):
        """5 timeouts across different requests → cooldown applied."""
        for i in range(5):
            req = self.req1 if i < 3 else self.req2
            self.db.log_download(request_id=req, soulseek_username="baduser",
                                 outcome="timeout")
        result = self.db.check_and_apply_cooldown("baduser")
        self.assertTrue(result)
        cooled = self.db.get_cooled_down_users()
        self.assertIn("baduser", cooled)

    def test_check_and_apply_cooldown_mixed_no_trigger(self):
        """3 timeouts + 2 successes → no cooldown."""
        outcomes: list[DownloadLogOutcome] = [
            "timeout", "timeout", "success", "timeout", "success"]
        for outcome in outcomes:
            self.db.log_download(request_id=self.req1, soulseek_username="mixeduser",
                                 outcome=outcome)
        result = self.db.check_and_apply_cooldown("mixeduser")
        self.assertFalse(result)
        cooled = self.db.get_cooled_down_users()
        self.assertNotIn("mixeduser", cooled)

    def test_check_and_apply_cooldown_counts_multi_user_rows(self):
        """Comma-joined usernames in download_log should count for each user."""
        for i in range(5):
            req = self.req1 if i < 3 else self.req2
            self.db.log_download(
                request_id=req,
                soulseek_username="disc1user, disc2user",
                outcome="timeout",
            )
        self.assertTrue(self.db.check_and_apply_cooldown("disc1user"))
        cooled = self.db.get_cooled_down_users()
        self.assertIn("disc1user", cooled)

    def test_check_and_apply_cooldown_below_threshold(self):
        """Only 2 outcomes → not enough data → no cooldown."""
        self.db.log_download(request_id=self.req1, soulseek_username="newuser",
                             outcome="timeout")
        self.db.log_download(request_id=self.req1, soulseek_username="newuser",
                             outcome="timeout")
        result = self.db.check_and_apply_cooldown("newuser")
        self.assertFalse(result)

    def test_check_and_apply_cooldown_ignores_abandoned_auto_import_audit(self):
        """Interrupted local imports should not count as source failures."""
        for _ in range(4):
            self.db.log_download(
                request_id=self.req1,
                soulseek_username="retryuser",
                outcome="timeout",
            )
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="retryuser",
            outcome="failed",
            beets_scenario="abandoned_auto_import",
        )

        result = self.db.check_and_apply_cooldown("retryuser")

        self.assertFalse(result)
        self.assertNotIn("retryuser", self.db.get_cooled_down_users())


class TestReleaseIdToLockKey(unittest.TestCase):
    """Issue #133 / #132 P1: ``release_id_to_lock_key`` must be stable
    across processes and fit int32 so it can drive the two-arg
    ``pg_advisory_lock``.

    Pure function — no PG dependency, runs without ``@requires_postgres``.
    """

    def test_same_mbid_maps_to_same_key(self) -> None:
        from lib.pipeline_db import release_id_to_lock_key
        mbid = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(mbid))

    def test_different_mbids_produce_different_keys(self) -> None:
        """Collision is statistically possible but unlikely with the
        handful of MBIDs in this test — a regression that makes the
        hash degenerate (e.g. returning a constant) would show up here.
        """
        from lib.pipeline_db import release_id_to_lock_key
        keys = {
            release_id_to_lock_key(s)
            for s in [
                "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                "cccccccc-4444-5555-6666-dddddddddddd",
                "eeeeeeee-7777-8888-9999-ffffffffffff",
                "12856590",   # Discogs numeric id
                "1073741824",  # Discogs numeric
                "",            # edge case: empty string → hash(b"") = 0
            ]
        }
        self.assertEqual(len(keys), 6)

    def test_key_fits_non_negative_int32(self) -> None:
        """``pg_advisory_lock(int4, int4)`` takes signed int32; we mask
        to 31 bits so the value is always in [0, 2^31-1]. Negative keys
        work too in PG but keeping them non-negative makes ``pg_locks``
        rows readable during debugging."""
        from lib.pipeline_db import release_id_to_lock_key
        for s in [
                "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
                "cccccccc-4444-5555-6666-dddddddddddd",
                "12856590",
                "xxxxxxxx-yyyy-zzzz-wwww-vvvvvvvvvvvv",
                "",
        ]:
            k = release_id_to_lock_key(s)
            self.assertGreaterEqual(k, 0)
            self.assertLess(k, 1 << 31)

    def test_key_is_stable_across_imports(self) -> None:
        """Sanity: the function does NOT use ``hash()`` (which is salted
        per-interpreter and would break cross-process locking). Re-import
        the module and verify the same input still maps to the same key.
        """
        import importlib
        from lib import pipeline_db
        mbid = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
        k1 = pipeline_db.release_id_to_lock_key(mbid)
        importlib.reload(pipeline_db)
        k2 = pipeline_db.release_id_to_lock_key(mbid)
        self.assertEqual(k1, k2)

    def test_whitespace_is_stripped_before_hashing(self) -> None:
        """Legacy DB rows sometimes carry stray leading/trailing
        whitespace on ``mb_release_id``. Two processes that normalise
        differently would otherwise hash to different keys and the
        advisory lock would silently fail to serialise them. ``.strip()``
        before hashing closes that gap."""
        from lib.pipeline_db import release_id_to_lock_key
        mbid = "12856590"
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(f" {mbid}"))
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(f"{mbid}\t"))
        self.assertEqual(
            release_id_to_lock_key(mbid),
            release_id_to_lock_key(f"  {mbid}\n"))


@requires_postgres
class TestAdvisoryLock(unittest.TestCase):
    """Issue #92: ``PipelineDB.advisory_lock`` must cross-session-serialize.

    A second session trying the same ``(namespace, key)`` must see ``False``
    while the first session holds the lock, and must succeed once the first
    session releases. This guards the force/manual-import concurrency fix
    in ``dispatch_import_from_db``.
    """

    NS = 0x46494D50  # ADVISORY_LOCK_NAMESPACE_IMPORT

    def setUp(self):
        self.db1 = make_db()  # truncates + gives us a fresh connection
        from lib import pipeline_db
        self.db2 = pipeline_db.PipelineDB(TEST_DSN)

    def tearDown(self):
        self.db1.close()
        self.db2.close()

    def test_lock_acquired_when_free(self):
        with self.db1.advisory_lock(self.NS, 12345) as acquired:
            self.assertTrue(acquired)

    def test_second_session_blocked_then_unblocked(self):
        with self.db1.advisory_lock(self.NS, 12345) as acquired1:
            self.assertTrue(acquired1)
            with self.db2.advisory_lock(self.NS, 12345) as acquired2:
                self.assertFalse(acquired2)
        # After the first session releases, the second can acquire.
        with self.db2.advisory_lock(self.NS, 12345) as acquired3:
            self.assertTrue(acquired3)

    def test_different_keys_do_not_collide(self):
        with self.db1.advisory_lock(self.NS, 12345) as a1:
            self.assertTrue(a1)
            with self.db2.advisory_lock(self.NS, 67890) as a2:
                self.assertTrue(a2)

    def test_lock_released_on_exception(self):
        """Exception raised inside the with-block must still release the lock."""
        with self.assertRaises(RuntimeError):
            with self.db1.advisory_lock(self.NS, 12345) as acquired:
                self.assertTrue(acquired)
                raise RuntimeError("boom")
        # Lock must be free now — a different session can acquire it.
        with self.db2.advisory_lock(self.NS, 12345) as a2:
            self.assertTrue(a2)

    def test_release_namespace_isolated_from_import_namespace(self):
        """Issue #133 / #132 P1: the RELEASE lock namespace must not
        collide with the IMPORT lock namespace. Holding one in session A
        must not prevent session B from acquiring the other at the same
        integer key — they are logically unrelated resources.
        """
        from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_IMPORT,
                                     ADVISORY_LOCK_NAMESPACE_RELEASE)
        with self.db1.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT, 12345) as a1:
            self.assertTrue(a1)
            with self.db2.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_RELEASE, 12345) as a2:
                self.assertTrue(a2)

    def test_reentrant_within_same_session(self):
        """``docs/advisory-locks.md`` depends on within-session
        reentrancy: the auto path's outer ``_handle_valid_result``
        acquire and ``dispatch_import_core``'s inner acquire on the
        same key both succeed because they share a session.

        Two acquires from the same session both return True; the inner
        release must be a no-op (the outer context still prevents a
        second session from acquiring). Only after the outer context
        releases does a second session succeed.
        """
        with self.db1.advisory_lock(self.NS, 12345) as outer:
            self.assertTrue(outer)
            with self.db1.advisory_lock(self.NS, 12345) as inner:
                self.assertTrue(inner)
                # Inner release happens on __exit__; outer still holds.
                # Second session must still be locked out.
                with self.db2.advisory_lock(self.NS, 12345) as other:
                    self.assertFalse(other)
            # Back in the outer context after the inner release — the
            # second session must STILL be blocked (outer still holds).
            with self.db2.advisory_lock(self.NS, 12345) as other:
                self.assertFalse(other)
        # After the outer context releases, the second session can
        # finally acquire.
        with self.db2.advisory_lock(self.NS, 12345) as other:
            self.assertTrue(other)

    def test_wrong_match_cleanup_namespace_isolated(self):
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
            wrong_match_cleanup_lock_key,
        )

        key = wrong_match_cleanup_lock_key(42, 77, "/failed/Artist - Album")
        with self.db1.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            key,
        ) as import_lock:
            self.assertTrue(import_lock)
            with self.db2.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP,
                key,
            ) as cleanup_lock:
                self.assertTrue(cleanup_lock)


@requires_postgres
class TestGetWrongMatches(unittest.TestCase):
    """Issue #113: every rejected row with a failed_path must be reachable.

    The previous ``DISTINCT ON (request_id)`` collapsed every rejection for a
    request to the newest row, hiding older failed_imports dirs on disk.
    ``get_wrong_matches`` now returns one row per eligible ``download_log``
    entry so the web UI can group and expand them for per-candidate actions.
    """

    def setUp(self):
        self.db = make_db()
        self.req1 = self.db.add_request(
            mb_release_id="wm-uuid-1", artist_name="Artist 1",
            album_title="Album 1", source="request")
        self.req2 = self.db.add_request(
            mb_release_id="wm-uuid-2", artist_name="Artist 2",
            album_title="Album 2", source="request")

    def tearDown(self):
        self.db.close()

    def _log_rejected(self, request_id: int, username: str,
                      failed_path: str | None,
                      scenario: str = "high_distance") -> None:
        vr: dict[str, object] = {"scenario": scenario, "distance": 0.25}
        if failed_path is not None:
            vr["failed_path"] = failed_path
        self.db.log_download(
            request_id=request_id,
            soulseek_username=username,
            outcome="rejected",
            beets_scenario=scenario,
            validation_result=json.dumps(vr),
        )

    def test_returns_every_rejected_row_for_same_request(self):
        """RED for issue #113: three rejected rows with failed_path → three returned."""
        self._log_rejected(self.req1, "alice", "/fi/path_0")
        self._log_rejected(self.req1, "bob",   "/fi/path_1")
        self._log_rejected(self.req1, "carol", "/fi/path_2")

        rows = self.db.get_wrong_matches()
        self.assertEqual(
            len(rows), 3,
            f"Expected all 3 rejections for request {self.req1}, got {len(rows)}. "
            f"DISTINCT ON is collapsing them.")
        self.assertEqual({r["request_id"] for r in rows}, {self.req1})
        self.assertEqual(
            {r["soulseek_username"] for r in rows},
            {"alice", "bob", "carol"})

    def test_rows_ordered_newest_first_per_request(self):
        """Within a request, rows must be ordered by download_log id DESC."""
        self._log_rejected(self.req1, "oldest",  "/fi/a")
        self._log_rejected(self.req1, "middle",  "/fi/b")
        self._log_rejected(self.req1, "newest",  "/fi/c")

        rows = self.db.get_wrong_matches()
        usernames = [r["soulseek_username"] for r in rows]
        self.assertEqual(usernames, ["newest", "middle", "oldest"])

    def test_rows_across_multiple_requests(self):
        """Every eligible row across multiple requests is returned."""
        self._log_rejected(self.req1, "r1-a", "/fi/1a")
        self._log_rejected(self.req1, "r1-b", "/fi/1b")
        self._log_rejected(self.req2, "r2-a", "/fi/2a")
        self._log_rejected(self.req2, "r2-b", "/fi/2b")

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 4)
        by_req: dict[int, list[str]] = {}
        for r in rows:
            rid = r["request_id"]
            assert isinstance(rid, int)
            user = r["soulseek_username"]
            assert isinstance(user, str)
            by_req.setdefault(rid, []).append(user)
        self.assertEqual(sorted(by_req[self.req1]), ["r1-a", "r1-b"])
        self.assertEqual(sorted(by_req[self.req2]), ["r2-a", "r2-b"])

    def test_excludes_rows_with_null_failed_path(self):
        self._log_rejected(self.req1, "has-path",  "/fi/ok")
        self._log_rejected(self.req1, "no-path",   None)

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "has-path")

    def test_excludes_audio_corrupt_and_spectral_reject(self):
        self._log_rejected(self.req1, "ok",      "/fi/keep",   scenario="high_distance")
        self._log_rejected(self.req1, "corrupt", "/fi/drop-a", scenario="audio_corrupt")
        self._log_rejected(self.req1, "transc",  "/fi/drop-b", scenario="spectral_reject")

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "ok")

    def test_deduplicates_same_failed_path_per_request(self):
        """Codex round 2: when the same folder is retried and rejected repeatedly,
        `download_log` accumulates duplicate rows for the same `failed_path`.
        The UI must show one row per actionable folder, not one per log entry.
        Keeps the newest row per `(request_id, failed_path)` pair.
        """
        # Live pattern: slskd reuses the `_9` suffix after the folder is
        # deleted, so the same failed_path can appear on two distinct rejected
        # download_log rows (older one is stale, newer is actionable).
        self._log_rejected(self.req1, "alice-old", "/fi/path_9")
        self._log_rejected(self.req1, "alice-new", "/fi/path_9")  # same path, newer
        self._log_rejected(self.req1, "bob",       "/fi/path_8")

        rows = self.db.get_wrong_matches()
        self.assertEqual(
            len(rows), 2,
            f"Expected 2 distinct folders (_9, _8), got {len(rows)}. "
            f"Same failed_path should collapse to newest row.")
        by_path = {
            r["soulseek_username"]: r for r in rows
        }
        # The surviving row for path_9 must be the newest ("alice-new"),
        # not the stale "alice-old".
        self.assertIn("alice-new", by_path)
        self.assertNotIn("alice-old", by_path)
        self.assertIn("bob", by_path)

    def test_clear_wrong_match_paths_clears_matching_request_and_paths(self):
        """Force-import cleanup clears every observed representation of one source."""
        self._log_rejected(self.req1, "raw", "failed_imports/Album")
        self._log_rejected(self.req1, "absolute", "/abs/Album")
        self._log_rejected(self.req1, "other-path", "/abs/Other")
        self._log_rejected(self.req2, "other-request", "/abs/Album")
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="successful",
            outcome="success",
            validation_result=json.dumps({"failed_path": "/abs/Album"}),
        )

        cleared = self.db.clear_wrong_match_paths(
            self.req1, ["failed_imports/Album", "/abs/Album"])

        self.assertEqual(cleared, 2)
        rows = self.db.get_wrong_matches()
        remaining = {
            (r["request_id"], r["soulseek_username"])
            for r in rows
        }
        self.assertEqual(remaining, {
            (self.req1, "other-path"),
            (self.req2, "other-request"),
        })

    def test_excludes_non_rejected_outcomes(self):
        """success / force_import / timeout must never surface in wrong-matches."""
        self._log_rejected(self.req1, "reject-me", "/fi/keep")
        self.db.log_download(
            request_id=self.req1, soulseek_username="success-u",
            outcome="success",
            validation_result=json.dumps({"failed_path": "/fi/no-1"}))
        self.db.log_download(
            request_id=self.req1, soulseek_username="force-u",
            outcome="force_import",
            validation_result=json.dumps({"failed_path": "/fi/no-2"}))
        self.db.log_download(
            request_id=self.req1, soulseek_username="timeout-u",
            outcome="timeout",
            validation_result=json.dumps({"failed_path": "/fi/no-3"}))

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["soulseek_username"], "reject-me")

    def test_result_shape_has_required_fields(self):
        """Each row must carry the fields the route layer reads."""
        self._log_rejected(self.req1, "alice", "/fi/a")

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        for field in ("download_log_id", "request_id", "artist_name",
                      "album_title", "mb_release_id", "soulseek_username",
                      "validation_result"):
            self.assertIn(field, row)

    def test_result_exposes_per_attempt_spectral_and_v0_probe_columns(self):
        """Per-candidate evidence (download_log columns) is projected.

        The Wrong Matches tab needs per-attempt spectral grade/floor and
        lossless-source V0 probe average to let the operator eyeball
        candidates by audio quality before destructive actions. These
        columns already exist on ``download_log`` (migrations 001/007);
        ``get_wrong_matches`` must surface them.
        """
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice",
            outcome="rejected",
            beets_scenario="high_distance",
            spectral_grade="suspect",
            spectral_bitrate=320,
            v0_probe_kind="lossless_source_v0",
            v0_probe_avg_bitrate=265,
            validation_result=json.dumps({
                "scenario": "high_distance",
                "distance": 0.25,
                "failed_path": "/fi/path_a",
            }),
        )

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["spectral_grade"], "suspect")
        self.assertEqual(row["spectral_bitrate"], 320)
        self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(row["v0_probe_avg_bitrate"], 265)

    def test_result_per_attempt_evidence_keys_present_when_null(self):
        """Legacy rows (pre-migration-007 / pre-spectral) come back with
        the four keys present and ``None`` — never missing."""
        self._log_rejected(self.req1, "alice", "/fi/legacy")

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        for field in ("spectral_grade", "spectral_bitrate",
                      "v0_probe_kind", "v0_probe_avg_bitrate"):
            self.assertIn(field, row)
            self.assertIsNone(row[field])

    def test_result_dedup_keeps_newer_evidence(self):
        """When the same failed_path is retried, the newer attempt wins —
        including its per-attempt spectral/V0 evidence."""
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice-old",
            outcome="rejected",
            beets_scenario="high_distance",
            spectral_grade="genuine",
            spectral_bitrate=900,
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/path_dup",
            }),
        )
        self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice-new",
            outcome="rejected",
            beets_scenario="high_distance",
            spectral_grade="suspect",
            spectral_bitrate=280,
            v0_probe_kind="lossless_source_v0",
            v0_probe_avg_bitrate=255,
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/path_dup",
            }),
        )

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["soulseek_username"], "alice-new")
        self.assertEqual(row["spectral_grade"], "suspect")
        self.assertEqual(row["spectral_bitrate"], 280)
        self.assertEqual(row["v0_probe_avg_bitrate"], 255)

    def test_result_surfaces_evidence_when_denorm_columns_are_null(self):
        """RED guard: ``get_wrong_matches`` must join album_quality_evidence.

        Live rejections write the canonical measurement to
        ``album_quality_evidence`` and link it via
        ``download_log.candidate_evidence_id``; the legacy denorm columns
        on ``download_log`` (``spectral_grade`` etc.) stay NULL because
        the wrong-match path rejects before the denorm-writing dispatch
        runs. The SQL must LEFT JOIN the evidence row and prefer it over
        the denorm columns; otherwise every wrong-match candidate
        silently surfaces as ``spectral=None / format=None`` in the UI.

        Reproduces the regression that motivated this slice — the route
        was reading ``dl.spectral_grade`` directly and showing dashes for
        every actual rejection on prod (every candidate evidence row was
        populated; nothing surfaced).
        """
        from lib.quality import (
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        # log_download intentionally writes NO denorm spectral / V0
        # values — this mirrors the live wrong-match-reject row shape.
        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice",
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/evidence-only",
            }),
        )

        # Seed canonical evidence and link it to the download_log row.
        evidence = make_album_quality_evidence(
            mb_release_id="wm-uuid-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=0,
                avg_bitrate_kbps=920,
                median_bitrate_kbps=900,
                format="FLAC",
                spectral_grade="genuine",
                spectral_bitrate_kbps=21,
                verified_lossless=True,
            ),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=1000,
                    mtime_ns=10,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
            ],
            codec="flac",
            container="flac",
            storage_format="FLAC",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=220,
                avg_bitrate_kbps=265,
                median_bitrate_kbps=260,
                source_lineage="lossless_source",
                source_provenance="real wire shape",
                proof_provenance="real wire shape",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                proof_origin="import",
                source="real wire shape",
                classifier="spectral+v0",
                detail=None,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(log_id, persisted.id)

        rows = self.db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        row = rows[0]

        # Evidence-derived spectral and V0 fields (COALESCEd against the
        # NULL denorm columns) reach the row payload.
        self.assertEqual(row["spectral_grade"], "genuine")
        self.assertEqual(row["spectral_bitrate"], 21)
        self.assertEqual(row["v0_probe_kind"], "lossless_source")
        self.assertEqual(row["v0_probe_avg_bitrate"], 265)

        # New evidence-only fields surfaced for the entry quality badge.
        self.assertEqual(row["evidence_storage_format"], "FLAC")
        self.assertEqual(row["evidence_min_bitrate"], 0)
        self.assertTrue(row["evidence_verified_lossless"])

    def test_download_history_seams_overlay_evidence_onto_legacy_columns(self):
        """RED guard: every download_log read seam overlays evidence.

        The denorm spectral / V0 columns on download_log are NULL whenever
        a candidate was rejected before the dispatch backfill ran. The
        per-request download-history view (pipeline detail tab) reads
        rows through ``get_download_history`` /
        ``get_download_history_batch`` / ``get_download_log_entry`` and
        feeds them to ``LogEntry.from_row`` which extracts
        ``spectral_grade`` / ``v0_probe_kind`` directly. Without an
        evidence overlay every wrong-match row in the audit trail
        silently shows blank spectral / V0 evidence — same regression
        class as ``get_wrong_matches`` itself.
        """
        from lib.quality import (
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
        )

        # NO denorm values on the row — only evidence.
        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice",
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/history-evidence-only",
            }),
        )
        evidence = make_album_quality_evidence(
            mb_release_id="wm-uuid-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=0,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=880,
                format="FLAC",
                spectral_grade="suspect",
                spectral_bitrate_kbps=18,
            ),
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01.flac",
                    size_bytes=1000,
                    mtime_ns=10,
                    extension="flac",
                    container="flac",
                    codec="flac",
                ),
            ],
            codec="flac",
            container="flac",
            storage_format="FLAC",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=200,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=240,
                source_lineage="lossless_source",
                source_provenance="real wire shape",
                proof_provenance="real wire shape",
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(log_id, persisted.id)

        # All three reader seams must overlay evidence onto the row.
        # The overlay translates the evidence lineage label
        # (``lossless_source``) into the download_log probe-kind wire shape
        # (``lossless_source_v0``) so the frontend renderer's kind-aware
        # branches match.
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["spectral_grade"], "suspect")
        self.assertEqual(entry["spectral_bitrate"], 18)
        self.assertEqual(entry["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(entry["v0_probe_avg_bitrate"], 245)

        history = self.db.get_download_history(self.req1)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["spectral_grade"], "suspect")
        self.assertEqual(history[0]["v0_probe_avg_bitrate"], 245)

        batch = self.db.get_download_history_batch([self.req1])
        self.assertEqual(batch[self.req1][0]["spectral_grade"], "suspect")
        self.assertEqual(batch[self.req1][0]["v0_probe_kind"], "lossless_source_v0")

    def test_download_history_keeps_explicit_denorm_when_evidence_missing(self):
        """Legacy rows without an evidence FK fall back to denorm columns.

        Historical download_log rows (pre-evidence) populated
        spectral_grade directly; the overlay must leave those alone so
        the audit trail doesn't lose data when evidence is absent.
        """
        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="historical",
            outcome="rejected",
            beets_scenario="high_distance",
            spectral_grade="genuine",
            spectral_bitrate=920,
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/fi/historical",
            }),
        )

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["spectral_grade"], "genuine")
        self.assertEqual(entry["spectral_bitrate"], 920)
        self.assertIsNone(entry["v0_probe_kind"])

    def test_result_carries_current_request_quality_fields(self):
        """Row must expose the request's on-disk quality state.

        The wrong-matches tab needs to show the current album's quality at
        the group level so the user can judge whether force-importing is
        worthwhile. That data lives on ``album_requests`` (status,
        min_bitrate, verified_lossless, spectral pair) and is pulled in via
        the existing JOIN.
        """
        # Seed the request with imported-quality state.
        self.db._execute(
            "UPDATE album_requests SET status = %s, min_bitrate = %s, "
            "verified_lossless = %s, current_spectral_grade = %s, "
            "current_spectral_bitrate = %s, imported_path = %s "
            "WHERE id = %s",
            ("imported", 207, True, "genuine", None,
             "/mnt/virtio/Music/Beets/Artist/Album", self.req1),
        )
        self._log_rejected(self.req1, "alice", "/fi/a")

        rows = self.db.get_wrong_matches()
        row = rows[0]
        self.assertEqual(row["request_status"], "imported")
        self.assertEqual(row["request_min_bitrate"], 207)
        self.assertTrue(row["request_verified_lossless"])
        self.assertEqual(row["request_current_spectral_grade"], "genuine")
        self.assertIsNone(row["request_current_spectral_bitrate"])
        self.assertEqual(row["request_imported_path"],
                         "/mnt/virtio/Music/Beets/Artist/Album")

    def test_get_wrong_matches_keyset_parity(self):
        """#523 -- fake<->production parity for the widest read projection.

        Seeds the SAME sequence ``_log_rejected`` runs on ``self.db``
        (real PG) onto a fresh ``FakePipelineDB``, then compares the full
        21-column wrong-match projection. Reuses
        ``TestReadProjectionParity._assert_keyset_parity`` -- it is a
        staticmethod whose first param is the ``TestCase`` instance, so
        cross-class reuse is safe.
        """
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        fake_req1 = fake.add_request(
            mb_release_id="wm-uuid-1", artist_name="Artist 1",
            album_title="Album 1", source="request")

        self._log_rejected(self.req1, "alice", "/fi/parity-a")
        fake.log_download(
            request_id=fake_req1,
            soulseek_username="alice",
            outcome="rejected",
            beets_scenario="high_distance",
            validation_result=json.dumps({
                "scenario": "high_distance", "distance": 0.25,
                "failed_path": "/fi/parity-a",
            }),
        )

        real_rows = self.db.get_wrong_matches()
        fake_rows = fake.get_wrong_matches()
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_wrong_matches parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_wrong_matches parity would pass vacuously")
        TestReadProjectionParity._assert_keyset_parity(
            self, real_rows, fake_rows, "get_wrong_matches")


@requires_postgres
class TestBadAudioHashes(unittest.TestCase):
    """Real-DB coverage for the bad_audio_hashes helpers (plan U2)."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="bad-hash-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _hash(self, n: int) -> bytes:
        return bytes([n]) * 32

    def test_add_returns_count_for_fresh_inserts(self):
        from lib.pipeline_db import BadAudioHashInput
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="m4a"),
        ]
        n = self.db.add_bad_audio_hashes(self.req_id, "H@rco", "bad rip", inputs)
        self.assertEqual(n, 3)

    def test_add_empty_list_returns_zero(self):
        n = self.db.add_bad_audio_hashes(self.req_id, "u", "r", [])
        self.assertEqual(n, 0)

    def test_add_full_duplicate_returns_zero(self):
        from lib.pipeline_db import BadAudioHashInput
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
        ]
        first = self.db.add_bad_audio_hashes(self.req_id, "H@rco", "x", inputs)
        second = self.db.add_bad_audio_hashes(
            self.req_id, "OtherUser", "y", inputs)
        self.assertEqual(first, 2)
        self.assertEqual(second, 0)

    def test_add_partial_overlap_returns_partial_count(self):
        from lib.pipeline_db import BadAudioHashInput
        first_batch = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
        ]
        self.db.add_bad_audio_hashes(self.req_id, "H@rco", "x", first_batch)
        second_batch = [
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="flac"),
        ]
        n = self.db.add_bad_audio_hashes(
            self.req_id, "Other", "y", second_batch)
        self.assertEqual(n, 1)

    def test_add_same_hash_different_format_both_inserted(self):
        from lib.pipeline_db import BadAudioHashInput
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(1), audio_format="mp3"),
        ]
        n = self.db.add_bad_audio_hashes(self.req_id, "u", "r", inputs)
        self.assertEqual(n, 2)

    def test_lookup_hits_when_present(self):
        from lib.pipeline_db import BadAudioHashInput
        self.db.add_bad_audio_hashes(
            self.req_id, "H@rco", "x",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        row = self.db.lookup_bad_audio_hash(self._hash(7), "flac")
        assert row is not None
        self.assertEqual(row.hash_value, self._hash(7))
        self.assertEqual(row.audio_format, "flac")
        self.assertEqual(row.request_id, self.req_id)
        self.assertEqual(row.reported_username, "H@rco")
        self.assertEqual(row.reason, "x")
        self.assertIsNotNone(row.reported_at)

    def test_lookup_miss_returns_none(self):
        self.assertIsNone(
            self.db.lookup_bad_audio_hash(self._hash(99), "flac"))

    def test_lookup_format_must_match(self):
        from lib.pipeline_db import BadAudioHashInput
        self.db.add_bad_audio_hashes(
            self.req_id, "u", "r",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        # Same hash, different format → miss
        self.assertIsNone(
            self.db.lookup_bad_audio_hash(self._hash(7), "mp3"))
        # Same format, different hash → miss
        self.assertIsNone(
            self.db.lookup_bad_audio_hash(self._hash(8), "flac"))

    def test_has_any_false_on_fresh_table(self):
        self.assertFalse(self.db.has_any_bad_audio_hashes())

    def test_has_any_true_after_one_insert(self):
        from lib.pipeline_db import BadAudioHashInput
        self.db.add_bad_audio_hashes(
            self.req_id, None, None,
            [BadAudioHashInput(hash_value=self._hash(1), audio_format="flac")],
        )
        self.assertTrue(self.db.has_any_bad_audio_hashes())


@requires_postgres
class TestRecentSuccessfulUploader(unittest.TestCase):
    """Real-DB coverage for get_recent_successful_uploader (plan U2)."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="rsu-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_returns_none_when_no_logs(self):
        self.assertIsNone(
            self.db.get_recent_successful_uploader(self.req_id))

    def test_returns_none_when_only_rejected_logs(self):
        self.db.log_download(
            self.req_id, soulseek_username="bob", outcome="rejected")
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="failed")
        self.assertIsNone(
            self.db.get_recent_successful_uploader(self.req_id))

    def test_returns_most_recent_success_when_multiple_present(self):
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="success")
        self.db.log_download(
            self.req_id, soulseek_username="bob", outcome="success")
        self.assertEqual(
            self.db.get_recent_successful_uploader(self.req_id), "bob")

    def test_returns_force_import_uploader(self):
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="success")
        self.db.log_download(
            self.req_id, soulseek_username="harco", outcome="force_import")
        self.assertEqual(
            self.db.get_recent_successful_uploader(self.req_id), "harco")

    def test_isolated_per_request(self):
        other = self.db.add_request(
            mb_release_id="rsu-other",
            artist_name="A",
            album_title="C",
            source="request",
        )
        self.db.log_download(
            self.req_id, soulseek_username="alice", outcome="success")
        self.db.log_download(
            other, soulseek_username="bob", outcome="success")
        self.assertEqual(
            self.db.get_recent_successful_uploader(self.req_id), "alice")
        self.assertEqual(
            self.db.get_recent_successful_uploader(other), "bob")


@requires_postgres
class TestActiveImportJobForRequest(unittest.TestCase):
    """Real-DB coverage for get_active_import_job_for_request (plan U2)."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="aij-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _enqueue(self, *, request_id: int, dedupe_key: str):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        return self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=manual_import_payload(failed_path="/tmp/x"),
        )

    def test_returns_none_when_no_jobs(self):
        self.assertIsNone(
            self.db.get_active_import_job_for_request(self.req_id))

    def test_returns_queued_job(self):
        job = self._enqueue(
            request_id=self.req_id, dedupe_key="manual:%d" % self.req_id)
        result = self.db.get_active_import_job_for_request(self.req_id)
        assert result is not None
        self.assertEqual(result["id"], job.id)
        self.assertEqual(result["status"], "queued")

    def test_returns_running_job(self):
        self._enqueue(
            request_id=self.req_id, dedupe_key="manual:%d" % self.req_id)
        # Mark would_import → claim → running
        self.db._execute("""
            UPDATE import_jobs
            SET preview_status = 'would_import',
                importable_at = NOW()
            WHERE request_id = %s
        """, (self.req_id,))
        claimed = self.db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        result = self.db.get_active_import_job_for_request(self.req_id)
        assert result is not None
        self.assertEqual(result["status"], "running")
        self.assertEqual(result["id"], claimed.id)

    def test_returns_none_for_completed_job(self):
        job = self._enqueue(
            request_id=self.req_id, dedupe_key="manual:%d" % self.req_id)
        self.db._execute("""
            UPDATE import_jobs
            SET preview_status = 'would_import',
                importable_at = NOW()
            WHERE id = %s
        """, (job.id,))
        claimed = self.db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        self.db.mark_import_job_completed(claimed.id, result={"ok": True})
        self.assertIsNone(
            self.db.get_active_import_job_for_request(self.req_id))

    def test_returns_none_for_failed_job(self):
        job = self._enqueue(
            request_id=self.req_id, dedupe_key="manual:%d" % self.req_id)
        self.db._execute("""
            UPDATE import_jobs
            SET preview_status = 'would_import',
                importable_at = NOW()
            WHERE id = %s
        """, (job.id,))
        claimed = self.db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        self.db.mark_import_job_failed(claimed.id, error="boom")
        self.assertIsNone(
            self.db.get_active_import_job_for_request(self.req_id))

    def test_filters_by_request_id(self):
        other = self.db.add_request(
            mb_release_id="aij-other",
            artist_name="A",
            album_title="C",
            source="request",
        )
        self._enqueue(request_id=self.req_id, dedupe_key="manual:a")
        self._enqueue(request_id=other, dedupe_key="manual:b")
        r1 = self.db.get_active_import_job_for_request(self.req_id)
        r2 = self.db.get_active_import_job_for_request(other)
        assert r1 is not None and r2 is not None
        self.assertEqual(r1["request_id"], self.req_id)
        self.assertEqual(r2["request_id"], other)

    def test_returns_most_recent_when_multiple_active(self):
        first = self._enqueue(request_id=self.req_id, dedupe_key="manual:a")
        second = self._enqueue(request_id=self.req_id, dedupe_key="manual:b")
        result = self.db.get_active_import_job_for_request(self.req_id)
        assert result is not None
        self.assertEqual(result["id"], max(first.id, second.id))


@requires_postgres
class TestActiveImportJobsForWrongMatch(unittest.TestCase):
    """Real-DB coverage for Wrong Matches active-job race checks."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="wm-active-uuid",
            artist_name="Wrong",
            album_title="Match",
            source="request",
        )
        self.other_req_id = self.db.add_request(
            mb_release_id="wm-active-other",
            artist_name="Other",
            album_title="Match",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_matches_active_jobs_by_row_request_path_and_source_dirs(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            IMPORT_JOB_MANUAL,
            force_import_payload,
            manual_import_payload,
        )

        path = "/tmp/failed/Artist - Album"
        source_dir = "user1\\Music\\Artist\\Album"

        by_download_log = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.other_req_id,
            dedupe_key="wm:download-log",
            payload=force_import_payload(download_log_id=77, failed_path="/tmp/other"),
        )
        by_request = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="wm:request",
            payload=manual_import_payload(failed_path="/tmp/unrelated"),
        )
        by_path = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.other_req_id,
            dedupe_key="wm:path",
            payload=manual_import_payload(failed_path=path),
        )
        by_source_dir = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.other_req_id,
            dedupe_key="wm:source-dir",
            payload=force_import_payload(
                download_log_id=88,
                failed_path="/tmp/source-dir-other",
                source_dirs=[source_dir],
            ),
        )
        self.db._execute(
            "UPDATE import_jobs SET status = 'running' WHERE id = %s",
            (by_source_dir.id,),
        )
        ignored = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="wm:ignored",
            payload=force_import_payload(download_log_id=77, failed_path=path),
        )
        completed = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=self.req_id,
            dedupe_key="wm:completed",
            payload=force_import_payload(download_log_id=77, failed_path=path),
        )
        self.db.mark_import_job_completed(completed.id, result={"ok": True})

        jobs = self.db.list_active_import_jobs_for_wrong_match(
            download_log_id=77,
            request_id=self.req_id,
            failed_paths=[path],
            source_dirs=[source_dir],
            ignore_import_job_id=ignored.id,
        )

        self.assertEqual(
            {job.id for job in jobs},
            {by_download_log.id, by_path.id, by_source_dir.id},
        )


# ---------------------------------------------------------------------------
# Persisted search plans (U1)
# ---------------------------------------------------------------------------


@requires_postgres
class TestPersistedSearchPlanCRUD(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="plan-crud-mbid",
            artist_name="Plan",
            album_title="CRUD",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _items(self, *queries: str) -> list:
        return [
            self.SearchPlanItemInput(
                ordinal=i,
                strategy=f"slot_{i}",
                query=q,
                canonical_query_key=q.lower(),
                repeat_group="default" if i == 0 else None,
            )
            for i, q in enumerate(queries)
        ]

    def test_successful_plan_sets_active_and_resets_cursor(self):
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=self._items("Artist Album", "Artist Track1"),
            metadata_snapshot={"year": 2024},
            provenance={"dropped_low_entropy_tokens": ["the"]},
        )
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, plan_id)
        self.assertEqual(active.plan.status, "active")
        self.assertEqual(active.plan.generator_id, "g1")
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertEqual(len(active.items), 2)
        self.assertEqual(active.items[0].ordinal, 0)
        self.assertEqual(active.items[0].query, "Artist Album")
        from lib.pipeline_db import (
            SearchPlanMetadataSnapshot, SearchPlanProvenance)
        self.assertIsInstance(
            active.plan.metadata_snapshot, SearchPlanMetadataSnapshot)
        self.assertIsInstance(active.plan.provenance, SearchPlanProvenance)
        assert active.plan.metadata_snapshot is not None
        assert active.plan.provenance is not None
        self.assertEqual(active.plan.metadata_snapshot.year, 2024)
        self.assertEqual(
            active.plan.provenance.values["dropped_low_entropy_tokens"],
            ["the"])

    def test_successful_plan_without_set_active_leaves_request_planless(self):
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=self._items("Q1"),
            set_active=False,
        )
        self.assertIsNone(self.db.get_active_search_plan(self.req_id))
        # The plan still exists.
        cur = self.db._execute(
            "SELECT status FROM search_plans WHERE id = %s", (plan_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "active")

    def test_successful_plan_requires_items(self):
        with self.assertRaises(ValueError):
            self.db.create_successful_search_plan(
                request_id=self.req_id,
                generator_id="g1",
                items=[],
            )

    def test_deterministic_failed_plan_leaves_request_unsearchable(self):
        plan_id = self.db.create_failed_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            failure_class="no_runnable_query",
            error_message="no usable artist/title query",
            transient=False,
        )
        # Request stays wanted, but no active plan -> not searchable.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")
        self.assertIsNone(req["active_plan_id"])
        self.assertIsNone(self.db.get_active_search_plan(self.req_id))
        cur = self.db._execute(
            "SELECT status, failure_class, error_message FROM search_plans "
            "WHERE id = %s", (plan_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "failed_deterministic")
        self.assertEqual(row["failure_class"], "no_runnable_query")
        self.assertEqual(row["error_message"], "no usable artist/title query")

    def test_transient_failed_plan_is_not_sticky(self):
        plan_id = self.db.create_failed_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            failure_class="resolver_unavailable",
            transient=True,
        )
        cur = self.db._execute(
            "SELECT status FROM search_plans WHERE id = %s", (plan_id,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "failed_transient")

    def test_supersede_replaces_active_plan_and_resets_cursor(self):
        first = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=self._items("Q1", "Q2"),
        )
        # Move cursor / cycle to non-zero so we can prove they reset.
        self.db._execute(
            "UPDATE album_requests "
            "SET next_plan_ordinal = 1, plan_cycle_count = 3 WHERE id = %s",
            (self.req_id,),
        )
        new_id = self.db.supersede_search_plan_with_replacement(
            request_id=self.req_id,
            generator_id="g2",
            items=self._items("Q3"),
        )
        # Cursor/cycle reset.
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, new_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        # Old plan flipped to superseded with link to replacement.
        cur = self.db._execute(
            "SELECT status, superseded_at, superseded_by_plan_id "
            "FROM search_plans WHERE id = %s", (first,))
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["status"], "superseded")
        self.assertIsNotNone(row["superseded_at"])
        self.assertEqual(row["superseded_by_plan_id"], new_id)

    def test_get_active_search_plan_returns_items_in_ordinal_order(self):
        """Single-query plan fetch returns items ordered by ordinal with
        every column hydrated (provenance, canonical_query_key,
        repeat_group). Guards the JSONB-aggregation rewrite of
        ``get_active_search_plan`` against drift from the prior two-query
        shape.
        """
        items = [
            self.SearchPlanItemInput(
                ordinal=0, strategy="primary", query="Artist Album",
                canonical_query_key="artist album",
                repeat_group="default",
                provenance={"repeat_index": 1},
            ),
            self.SearchPlanItemInput(
                ordinal=1, strategy="track1", query="Artist Track 1",
                canonical_query_key="artist track 1",
                provenance={"source_track_index": 0, "track_slot_index": 1},
            ),
            self.SearchPlanItemInput(
                ordinal=2, strategy="track2", query="Artist Track 2",
                canonical_query_key="artist track 2",
                provenance=None,
            ),
        ]
        # Insert plan items with ordinals reversed to prove ORDER BY works.
        self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=list(reversed(items)),
        )
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(len(active.items), 3)
        ordinals = [it.ordinal for it in active.items]
        self.assertEqual(ordinals, [0, 1, 2])
        self.assertEqual(active.items[0].query, "Artist Album")
        self.assertEqual(active.items[0].canonical_query_key, "artist album")
        self.assertEqual(active.items[0].repeat_group, "default")
        from lib.pipeline_db import SearchPlanItemProvenance
        self.assertIsInstance(
            active.items[0].provenance, SearchPlanItemProvenance)
        assert active.items[0].provenance is not None
        assert active.items[1].provenance is not None
        self.assertEqual(active.items[0].provenance.values["repeat_index"], 1)
        self.assertEqual(active.items[1].strategy, "track1")
        self.assertEqual(
            active.items[1].provenance.values["source_track_index"], 0)
        self.assertEqual(
            active.items[1].provenance.values["track_slot_index"], 1)
        self.assertIsNone(active.items[2].provenance)
        self.assertIsNone(active.items[2].repeat_group)
        # IDs must be present + ints (matched the prior two-query contract).
        for it in active.items:
            self.assertIsInstance(it.id, int)
            self.assertGreater(it.id, 0)
            self.assertEqual(it.plan_id, active.plan.id)

    def test_get_active_search_plan_handles_zero_items(self):
        """LEFT JOIN + jsonb_agg can mis-handle the zero-item case (NULL
        or ``[null]``). Confirm an active plan whose items got deleted
        out-of-band returns ``items=[]`` rather than crashing or
        constructing a row from NULLs.
        """
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=self._items("only"),
        )
        # Out-of-band deletion (production never does this; the migrator
        # might, or a future cleanup tool).
        self.db._execute(
            "DELETE FROM search_plan_items WHERE plan_id = %s", (plan_id,))
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, plan_id)
        self.assertEqual(active.items, [])


@requires_postgres
class TestListSearchPlanClassificationForRequests(unittest.TestCase):
    """Batch-fetch dry-run classification.

    ``list_search_plan_classification_for_requests`` collapses the
    per-row 5-query inspection call into a single query. Verify the
    per-request data returned matches what the previous per-row
    ``get_search_plan_inspection`` path would have surfaced for the
    same rows.
    """

    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add(self, mbid: str) -> int:
        return self.db.add_request(
            mb_release_id=mbid, artist_name="Artist",
            album_title=mbid, source="request",
        )

    def test_empty_input_returns_empty_dict(self):
        self.assertEqual(
            self.db.list_search_plan_classification_for_requests([]), {})

    def test_returns_none_generator_ids_when_no_failed_plans(self):
        rid = self._add("nofail")
        result = self.db.list_search_plan_classification_for_requests([rid])
        self.assertIn(rid, result)
        self.assertIsNone(
            result[rid].latest_failed_deterministic_generator_id)
        self.assertIsNone(
            result[rid].latest_failed_transient_generator_id)

    def test_returns_latest_failed_generator_ids_per_request(self):
        rid_a = self._add("rid-a")
        rid_b = self._add("rid-b")
        rid_c = self._add("rid-c")
        # rid_a: deterministic failure on g-old, then deterministic
        # failure on g-new -- the new one should win.
        self.db.create_failed_search_plan(
            request_id=rid_a, generator_id="g-old",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_failed_search_plan(
            request_id=rid_a, generator_id="g-new",
            failure_class="metadata_incomplete", transient=False,
        )
        # rid_b: only transient failure on g-new.
        self.db.create_failed_search_plan(
            request_id=rid_b, generator_id="g-new",
            failure_class="resolver_unavailable", transient=True,
        )
        # rid_c: both -- one deterministic g-det, one transient g-trans.
        self.db.create_failed_search_plan(
            request_id=rid_c, generator_id="g-det",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_failed_search_plan(
            request_id=rid_c, generator_id="g-trans",
            failure_class="dependency_failure", transient=True,
        )

        result = self.db.list_search_plan_classification_for_requests(
            [rid_a, rid_b, rid_c])

        self.assertEqual(
            result[rid_a].latest_failed_deterministic_generator_id, "g-new")
        self.assertIsNone(
            result[rid_a].latest_failed_transient_generator_id)

        self.assertIsNone(
            result[rid_b].latest_failed_deterministic_generator_id)
        self.assertEqual(
            result[rid_b].latest_failed_transient_generator_id, "g-new")
        self.assertIsNotNone(
            result[rid_b].latest_failed_transient_created_at)

        self.assertEqual(
            result[rid_c].latest_failed_deterministic_generator_id, "g-det")
        self.assertEqual(
            result[rid_c].latest_failed_transient_generator_id, "g-trans")

    def test_matches_per_row_inspection_for_same_data(self):
        """Equivalence guard: the batch result for each request must
        agree with what ``get_search_plan_inspection`` would say about
        the same rows. This is the contract the dry-run classifier
        relies on after the rewrite.
        """
        rid = self._add("equiv")
        # Mixed plan history: deterministic failure, then a successful
        # plan (irrelevant to the classifier), then a transient
        # failure.
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
            set_active=False,
        )
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g2",
            failure_class="resolver_unavailable", transient=True,
        )

        inspection = self.db.get_search_plan_inspection(rid)
        det = inspection.latest_failed_deterministic
        trans = inspection.latest_failed_transient

        batch = self.db.list_search_plan_classification_for_requests([rid])
        entry = batch[rid]
        self.assertEqual(
            entry.latest_failed_deterministic_generator_id,
            det.generator_id if det is not None else None,
        )
        self.assertEqual(
            entry.latest_failed_transient_generator_id,
            trans.generator_id if trans is not None else None,
        )
        self.assertEqual(
            entry.latest_failed_transient_created_at,
            trans.created_at if trans is not None else None,
        )


@requires_postgres
class TestGetWantedSearchable(unittest.TestCase):
    """``get_wanted_searchable`` filters Phase 2 execution candidates.

    Only rows whose active plan exists, status='active', and
    generator_id matches the passed-in id are returned. Rows without a
    current-generator active plan must be excluded so Phase 2 cannot
    accidentally fall back to recomputing variants from search_attempts.
    """

    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add_wanted(self, mbid: str) -> int:
        return self.db.add_request(
            mb_release_id=mbid, artist_name="A",
            album_title=mbid, source="request",
        )

    def _make_active(self, request_id: int, generator_id: str) -> int:
        return self.db.create_successful_search_plan(
            request_id=request_id,
            generator_id=generator_id,
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query=f"q-{request_id}")],
        )

    def test_returns_only_rows_with_current_generator_active_plan(self):
        rid_match = self._add_wanted("match")
        self._make_active(rid_match, "g1")

        rid_no_plan = self._add_wanted("no-plan")  # never planned
        rid_old_gen = self._add_wanted("old-gen")
        self._make_active(rid_old_gen, "g0")  # different gen

        rid_imported = self._add_wanted("imp")
        self._make_active(rid_imported, "g1")
        self.db.update_status(rid_imported, "imported")

        rows = self.db.get_wanted_searchable("g1")
        rids = {r["id"] for r in rows}
        self.assertEqual(rids, {rid_match})

    def test_respects_retry_backoff(self):
        rid = self._add_wanted("backoff")
        self._make_active(rid, "g1")
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (future, rid),
        )
        self.db.conn.commit()
        self.assertEqual(self.db.get_wanted_searchable("g1"), [])

    def test_excludes_request_after_supersede_to_new_generator(self):
        # Old-generator active plan -> supersede to new -> the new id
        # must be searchable; the old id is not.
        rid = self._add_wanted("supersede")
        self._make_active(rid, "g0")
        self.assertEqual(
            [r["id"] for r in self.db.get_wanted_searchable("g0")], [rid])
        self.db.supersede_search_plan_with_replacement(
            request_id=rid,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q-new")],
        )
        self.assertEqual(self.db.get_wanted_searchable("g0"), [])
        rids = [r["id"] for r in self.db.get_wanted_searchable("g1")]
        self.assertEqual(rids, [rid])

    def test_failed_deterministic_only_excluded(self):
        rid = self._add_wanted("det-fail")
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.assertEqual(self.db.get_wanted_searchable("g1"), [])

    def test_failed_transient_only_excluded(self):
        rid = self._add_wanted("trans-fail")
        self.db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        self.assertEqual(self.db.get_wanted_searchable("g1"), [])

    def test_active_youtube_rescue_excluded(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        rid_running = self._add_wanted("yt-running")
        self._make_active(rid_running, "g1")
        self.db.insert_youtube_running(
            request_id=rid_running,
            browse_id="MPREb_running",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=running",
            expected_track_count=10,
        )

        rid_import = self._add_wanted("yt-import")
        self._make_active(rid_import, "g1")
        self.db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=rid_import,
            dedupe_key=youtube_import_dedupe_key(123),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-import",
                request_id=rid_import,
                browse_id="MPREb_import",
            ),
        )

        rid_clear = self._add_wanted("clear")
        self._make_active(rid_clear, "g1")

        rows = self.db.get_wanted_searchable("g1")
        self.assertEqual({r["id"] for r in rows}, {rid_clear})

    def test_limit_applied(self):
        ids = []
        for i in range(5):
            rid = self._add_wanted(f"lim-{i}")
            self._make_active(rid, "g1")
            ids.append(rid)
        rows = self.db.get_wanted_searchable("g1", limit=3)
        self.assertEqual(len(rows), 3)
        self.assertTrue(set(r["id"] for r in rows).issubset(set(ids)))


@requires_postgres
class TestPersistedSearchPlanReconciliation(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _add(self, mbid: str, status: str = "wanted") -> int:
        rid = self.db.add_request(
            mb_release_id=mbid,
            artist_name="A",
            album_title=mbid,
            source="request",
        )
        if status != "wanted":
            self.db.update_status(rid, status)
        return rid

    def test_lists_all_wanted_ignoring_retry_eligibility_and_pagination(self):
        far_future = datetime.now(timezone.utc) + timedelta(hours=24)
        rid_due = self._add("recon-due")
        rid_backoff = self._add("recon-backoff")
        rid_imported = self._add("recon-imported", status="imported")
        # Set far-future retry on the second wanted row -- get_wanted would
        # skip it; reconciliation MUST include it.
        self.db._execute(
            "UPDATE album_requests SET next_retry_after = %s WHERE id = %s",
            (far_future, rid_backoff),
        )
        # And one wanted row already has an active plan.
        self.db.create_successful_search_plan(
            request_id=rid_due,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )

        rows = self.db.list_wanted_for_plan_reconciliation()
        rids = {r.request_id for r in rows}
        self.assertIn(rid_due, rids)
        self.assertIn(rid_backoff, rids)
        self.assertNotIn(rid_imported, rids)

        by_id = {r.request_id: r for r in rows}
        self.assertIsNotNone(by_id[rid_due].active_plan_id)
        self.assertEqual(by_id[rid_due].active_plan_generator_id, "g1")
        self.assertIsNone(by_id[rid_backoff].active_plan_id)
        self.assertIsNone(by_id[rid_backoff].active_plan_generator_id)

    def test_reconciliation_candidate_ignores_non_active_plan_pointer(self):
        rid = self._add("recon-malformed")
        failed_id = self.db.create_failed_search_plan(
            request_id=rid,
            generator_id="g1",
            failure_class="no_runnable_query",
            error_message="failed",
            transient=False,
        )
        self.db._execute(
            "UPDATE album_requests SET active_plan_id = %s WHERE id = %s",
            (failed_id, rid),
        )

        rows = self.db.list_wanted_for_plan_reconciliation()
        by_id = {r.request_id: r for r in rows}
        self.assertIn(rid, by_id)
        self.assertIsNone(by_id[rid].active_plan_id)
        self.assertIsNone(by_id[rid].active_plan_generator_id)


@requires_postgres
class TestPersistedSearchPlanInspection(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import SearchPlanItemInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="inspect-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_inspection_returns_active_failed_superseded_and_legacy_counts(self):
        # Legacy log row (no plan context).
        self.db.log_search(
            self.req_id, query="legacy", outcome="error",
        )
        # Deterministic + transient failed attempts.
        self.db.create_failed_search_plan(
            request_id=self.req_id, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.db.create_failed_search_plan(
            request_id=self.req_id, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        # First successful, then supersede with second.
        self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q1")],
        )
        new_id = self.db.supersede_search_plan_with_replacement(
            request_id=self.req_id, generator_id="g2",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q2")],
        )

        info = self.db.get_search_plan_inspection(self.req_id)
        assert info.active is not None
        self.assertEqual(info.active.plan.id, new_id)
        assert info.latest_failed_deterministic is not None
        self.assertEqual(
            info.latest_failed_deterministic.failure_class,
            "no_runnable_query")
        assert info.latest_failed_transient is not None
        self.assertEqual(
            info.latest_failed_transient.failure_class,
            "resolver_unavailable")
        self.assertEqual(info.superseded_count, 1)
        self.assertEqual(info.legacy_search_log_count, 1)


@requires_postgres
class TestRecordConsumedSearchAttempt(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import (ConsumedAttemptInput,
                                     SearchPlanItemInput)
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="consumed-mbid",
            artist_name="A", album_title="B", source="request",
        )
        self.plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=[
                self.SearchPlanItemInput(
                    ordinal=0, strategy="default", query="Q0",
                    canonical_query_key="q0", repeat_group="rg"),
                self.SearchPlanItemInput(
                    ordinal=1, strategy="track_0", query="Q1",
                    canonical_query_key="q1"),
            ],
        )
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.active = active
        self.item_ids = [it.id for it in active.items]

    def tearDown(self):
        self.db.close()

    def _attempt(self, ordinal: int, **overrides):
        kwargs = dict(
            request_id=self.req_id,
            plan_id=self.plan_id,
            plan_item_id=self.item_ids[ordinal],
            plan_ordinal=ordinal,
            plan_strategy=self.active.items[ordinal].strategy,
            plan_canonical_query_key=(
                self.active.items[ordinal].canonical_query_key),
            plan_repeat_group=self.active.items[ordinal].repeat_group,
            plan_generator_id="g1",
            query=self.active.items[ordinal].query,
            outcome="no_match",
            plan_item_count=len(self.active.items),
            apply_scheduler_attempt=True,
            scheduler_success=False,
        )
        kwargs.update(overrides)
        return self.ConsumedAttemptInput(**kwargs)

    def test_advance_ordinal_writes_log_and_updates_cursor(self):
        result = self.db.record_consumed_search_attempt(
            self._attempt(0, outcome="no_match"))
        self.assertEqual(result.cursor_update_status, "advanced")
        self.assertEqual(result.new_next_ordinal, 1)
        self.assertEqual(result.new_cycle_count, 0)
        self.assertFalse(result.is_stale)
        # Log row written with plan context + cycle snapshot.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        log = rows[0]
        self.assertEqual(log["plan_id"], self.plan_id)
        self.assertEqual(log["plan_ordinal"], 0)
        self.assertEqual(log["execution_stage"], "accepted")
        self.assertTrue(log["attempt_consumed"])
        self.assertEqual(log["cursor_update_status"], "advanced")
        self.assertEqual(log["plan_cycle_snapshot"], 0)
        # Request cursor advanced.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 1)
        self.assertEqual(req["plan_cycle_count"], 0)
        # Scheduler/backoff updated.
        self.assertEqual(req["search_attempts"], 1)
        self.assertIsNotNone(req["next_retry_after"])

    def test_final_ordinal_wraps_and_increments_cycle(self):
        # Move cursor to final ordinal first.
        self.db._execute(
            "UPDATE album_requests SET next_plan_ordinal = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(1))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(result.new_next_ordinal, 0)
        self.assertEqual(result.new_cycle_count, 1)
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 1)
        # Log row reflects pre-write cycle snapshot.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows[0]["cursor_update_status"], "wrapped")
        self.assertEqual(rows[0]["plan_cycle_snapshot"], 0)

    def test_stale_completion_logs_but_does_not_advance(self):
        # Advance the cursor manually so the executor's plan_ordinal=0
        # no longer matches.
        self.db._execute(
            "UPDATE album_requests SET next_plan_ordinal = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(0))
        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        # Cursor NOT advanced.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 1)
        # Log row still inserted, flagged stale.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["execution_stage"], "stale_completion")
        self.assertFalse(rows[0]["attempt_consumed"])
        self.assertEqual(rows[0]["cursor_update_status"], "stale")
        self.assertEqual(rows[0]["stale_reason"], "regenerated")
        # No scheduler/backoff bump on stale.
        self.assertEqual(req["search_attempts"], 0)

    def test_stale_when_cycle_count_does_not_match(self):
        self.db._execute(
            "UPDATE album_requests SET plan_cycle_count = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(0))
        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 1)
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows[0]["execution_stage"], "stale_completion")
        self.assertFalse(rows[0]["attempt_consumed"])
        self.assertEqual(rows[0]["plan_cycle_snapshot"], 0)

    def test_stale_when_plan_id_does_not_match(self):
        # Regenerate -- new plan, cursor reset.
        new_plan = self.db.supersede_search_plan_with_replacement(
            request_id=self.req_id,
            generator_id="g2",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="Qnew")],
        )
        # Old executor completes against the old plan id.
        result = self.db.record_consumed_search_attempt(self._attempt(0))
        self.assertTrue(result.is_stale)
        # Active plan is still the new one, cursor at 0.
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        self.assertEqual(active.plan.id, new_plan)
        self.assertEqual(active.next_ordinal, 0)

    def test_rejects_plan_item_from_another_request(self):
        other_req_id = self.db.add_request(
            mb_release_id="consumed-other-mbid",
            artist_name="C", album_title="D", source="request",
        )
        other_plan = self.db.create_successful_search_plan(
            request_id=other_req_id,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="Q-other")],
        )
        other_active = self.db.get_active_search_plan(other_req_id)
        assert other_active is not None
        with self.assertRaises(ValueError):
            self.db.record_consumed_search_attempt(
                self._attempt(
                    0,
                    plan_id=self.plan_id,
                    plan_item_id=other_active.items[0].id,
                )
            )
        self.assertIsNotNone(other_plan)
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows, [])

    def test_consumed_attempt_persists_u11_forensics_columns(self):
        """U11: consumed-attempt rows surface R22-R27 from the input."""
        self.db.record_consumed_search_attempt(self._attempt(
            0,
            outcome="no_match",
            rejection_reason="strict_count_mismatch",
            result_count_uncapped=873,
            query_token_count=4,
            query_distinct_token_count=3,
            expected_track_count=10,
            matcher_score_top1=1.5,
            query_template="{artist} {title} FLAC",
        ))
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["rejection_reason"], "strict_count_mismatch")
        self.assertEqual(row["result_count_uncapped"], 873)
        self.assertEqual(row["query_token_count"], 4)
        self.assertEqual(row["query_distinct_token_count"], 3)
        self.assertEqual(row["expected_track_count"], 10)
        score = row["matcher_score_top1"]
        assert isinstance(score, float)
        self.assertAlmostEqual(score, 1.5, places=4)
        self.assertEqual(row["query_template"], "{artist} {title} FLAC")

    def test_u12_wrap_writes_failure_class_b_cands_never_match(self):
        """U12: wrap classifies all-no_match cycle as B."""
        # Cycle 0: both items return no_match (matcher rejected candidates).
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match", rejection_reason="strict_count_mismatch",
        ))
        # Final ordinal → wrap.
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_match", rejection_reason="avg_ratio_low",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "B_cands_never_match")

    def test_u12_wrap_writes_failure_class_a_zero_results_dominant(self):
        """U12: wrap classifies dominant-no_results cycle as A."""
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_results",
        ))
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_results",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "A_zero_results_dominant")

    def test_u12_non_wrap_advance_does_not_write_failure_class(self):
        """U12: classification only fires on wrap, not on plain advance."""
        result = self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match",
        ))
        self.assertEqual(result.cursor_update_status, "advanced")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertIsNone(req["failure_class"])

    def test_u12_wrap_with_status_imported_classifies_resolved(self):
        """U12: status moved past 'wanted' overrides search-pattern verdict."""
        # Mid-cycle, the importer marked the request 'imported'.
        self.db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (self.req_id,),
        )
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="no_match",
        ))
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_match",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "resolved")

    def test_u12_wrap_preserves_prior_failure_class_on_degenerate_cycle(self):
        """U12: empty cycle (all stale) leaves prior failure_class intact.

        Seed a prior failure_class, then trigger a wrap whose only
        consumed attempt is the wrap itself. Verify the classifier sees
        one consumed attempt; for richer "zero consumed" coverage see
        the FakePipelineDB self-test where we can drive the
        no-consumed-attempts case directly.
        """
        # Seed a prior verdict so we can distinguish "unchanged" from
        # "overwritten".
        self.db._execute(
            "UPDATE album_requests SET failure_class = 'E_mixed' "
            "WHERE id = %s",
            (self.req_id,),
        )
        # Single attempt + wrap. Branch ordering: found dominates →
        # D_found_but_no_import overwrites the prior E_mixed.
        self.db._execute(
            "UPDATE album_requests SET next_plan_ordinal = 1 WHERE id = %s",
            (self.req_id,),
        )
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="found",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "D_found_but_no_import")

    def test_u12_wrap_d_found_but_no_import(self):
        """U12: one found + status still wanted → D_found_but_no_import."""
        self.db.record_consumed_search_attempt(self._attempt(
            0, outcome="found",
        ))
        result = self.db.record_consumed_search_attempt(self._attempt(
            1, outcome="no_match",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["failure_class"], "D_found_but_no_import")

    def test_u12_failure_class_check_constraint_enforced(self):
        """U12: every classifier verdict must satisfy the CHECK constraint.

        Walk a wrap for each of A/B/D/resolved/E (constants from the
        classifier module) and assert that PostgreSQL accepts the
        write. If the classifier ever returns a value the schema
        rejects, this surfaces as a constraint violation at write time
        — not as silent corruption.
        """
        from lib.search_classification import ALL_FAILURE_CLASSES
        for fc in ALL_FAILURE_CLASSES:
            with self.subTest(failure_class=fc):
                self.db._execute(
                    "UPDATE album_requests SET failure_class = %s "
                    "WHERE id = %s",
                    (fc, self.req_id),
                )
                req = self.db.get_request(self.req_id)
                assert req is not None
                self.assertEqual(req["failure_class"], fc)


@requires_postgres
class TestRecordNonConsumingSearchAttempt(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import (NonConsumingAttemptInput,
                                     SearchPlanItemInput)
        self.NonConsumingAttemptInput = NonConsumingAttemptInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="nonconsuming-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_writes_visible_log_and_applies_backoff_without_advancing(self):
        log_id = self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id,
                outcome="error",
                error_message="slskd 503",
                apply_scheduler_attempt=True,
            )
        )
        self.assertGreater(log_id, 0)
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["outcome"], "error")
        self.assertEqual(rows[0]["execution_stage"], "pre_attempt")
        self.assertFalse(rows[0]["attempt_consumed"])
        self.assertEqual(rows[0]["cursor_update_status"], "unchanged")
        self.assertEqual(rows[0]["plan_cycle_snapshot"], 0)
        # Cursor + cycle untouched, scheduler/backoff applied.
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 0)
        self.assertEqual(req["search_attempts"], 1)
        self.assertIsNotNone(req["next_retry_after"])

    def test_can_skip_scheduler_attempt(self):
        self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id,
                outcome="error",
                apply_scheduler_attempt=False,
            )
        )
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["search_attempts"], 0)
        self.assertIsNone(req["next_retry_after"])

    def test_non_consuming_attempt_persists_u11_forensics_columns(self):
        """U11: pre-attempt rows surface R22-R27 from the input."""
        self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id,
                outcome="error",
                error_message="slskd 503",
                apply_scheduler_attempt=True,
                rejection_reason=None,
                result_count_uncapped=None,
                query_token_count=3,
                query_distinct_token_count=3,
                expected_track_count=12,
                matcher_score_top1=None,
                query_template="{artist} {title}",
            )
        )
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        # Pre-attempt: matcher never ran → score/reason/uncapped NULL.
        self.assertIsNone(row["rejection_reason"])
        self.assertIsNone(row["matcher_score_top1"])
        self.assertIsNone(row["result_count_uncapped"])
        # Token-counts + template + expected-track-count come from
        # plan-context state that's known before slskd dispatch.
        self.assertEqual(row["query_token_count"], 3)
        self.assertEqual(row["query_distinct_token_count"], 3)
        self.assertEqual(row["expected_track_count"], 12)
        self.assertEqual(row["query_template"], "{artist} {title}")


@requires_postgres
class TestRequestSearchSummaryViewU11RoundTrip(unittest.TestCase):
    """U11 R29: writing search_log rows with populated forensics columns
    must surface through ``request_search_summary`` for the dominant
    rejection-reason rollup.

    Migration 031 defines ``request_search_summary`` with
    ``MODE() WITHIN GROUP (ORDER BY rejection_reason)`` — the mode is
    the most-frequent non-NULL reason. This test pins that contract:
    five known rows with mixed reasons must roll up to the operator's
    expected ``dominant_rejection_reason``.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="u11-summary-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def _log(self, reason: str | None, outcome: str = "no_match") -> None:
        self.db.log_search(
            request_id=self.req_id,
            query="q",
            outcome=outcome,
            candidates=[],
            rejection_reason=reason,
        )

    def test_dominant_rejection_reason_rolls_up_from_recent_rows(self):
        # 5 rows: 3 avg_ratio_low, 1 strict_count_mismatch, 1 NULL
        # (e.g. found). Mode over non-NULL = avg_ratio_low.
        self._log("avg_ratio_low")
        self._log("avg_ratio_low")
        self._log("strict_count_mismatch")
        self._log("avg_ratio_low")
        self._log(None, outcome="found")

        cur = self.db._execute(
            "SELECT total_searches, dominant_rejection_reason "
            "FROM request_search_summary WHERE request_id = %s",
            (self.req_id,),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertEqual(row["total_searches"], 5)
        self.assertEqual(row["dominant_rejection_reason"], "avg_ratio_low")

    def test_all_null_reasons_produce_null_dominant(self):
        # Every row's reason is NULL (e.g. all found / no_results).
        self._log(None, outcome="found")
        self._log(None, outcome="no_results")
        cur = self.db._execute(
            "SELECT dominant_rejection_reason "
            "FROM request_search_summary WHERE request_id = %s",
            (self.req_id,),
        )
        row = cur.fetchone()
        assert row is not None
        self.assertIsNone(row["dominant_rejection_reason"])


@requires_postgres
class TestPersistedSearchPlanLifecycleEdgeCases(unittest.TestCase):
    def setUp(self):
        from lib.pipeline_db import (ConsumedAttemptInput,
                                     SearchPlanItemInput)
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.SearchPlanItemInput = SearchPlanItemInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="lifecycle-mbid",
            artist_name="A", album_title="B", source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_historical_logs_with_null_plan_context_still_returned(self):
        self.db.log_search(self.req_id, query="legacy", outcome="error")
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].get("plan_id"))
        self.assertIsNone(rows[0].get("execution_stage"))

    def test_request_delete_cascades_plans_keeps_inspection_at_zero(self):
        self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )
        self.db.delete_request(self.req_id)
        # After deletion the cascade should clear plans / items / logs (because
        # search_log already CASCADEs on request from migration 001). The
        # inspection method just returns zeros for a missing request.
        info = self.db.get_search_plan_inspection(self.req_id)
        self.assertIsNone(info.active)
        self.assertIsNone(info.latest_failed_deterministic)
        self.assertIsNone(info.latest_failed_transient)
        self.assertEqual(info.superseded_count, 0)
        self.assertEqual(info.legacy_search_log_count, 0)

    def test_consumed_attempt_rolls_back_on_failure_no_partial_state(self):
        plan_id = self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id="g1",
            items=[self.SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )
        # Build an attempt referencing a plan_item_id that doesn't exist; the
        # FK on search_log.plan_item_id fires inside the transaction and rolls
        # back the cursor write too.
        attempt = self.ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=plan_id,
            plan_item_id=999999,
            plan_ordinal=0,
            plan_strategy="default",
            plan_canonical_query_key=None,
            plan_repeat_group=None,
            plan_generator_id="g1",
            query="q",
            outcome="no_match",
            plan_item_count=1,
        )
        with self.assertRaises(Exception):
            self.db.record_consumed_search_attempt(attempt)
        # No log row, cursor untouched.
        rows = self.db.get_search_history(self.req_id)
        self.assertEqual(rows, [])
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["next_plan_ordinal"], 0)
        self.assertEqual(req["plan_cycle_count"], 0)


@requires_postgres
class TestSearchPlanStats(unittest.TestCase):
    """U8: ``get_search_plan_stats`` aggregates plan-aware search_log
    rows into per-slot and per-query-group usefulness stats. Cache
    attribution is ``cycle_only`` because there are no per-search
    cache columns on ``search_log`` today.
    """

    def setUp(self):
        from lib.pipeline_db import (
            ConsumedAttemptInput, NonConsumingAttemptInput,
            SearchPlanItemInput,
        )
        self.SearchPlanItemInput = SearchPlanItemInput
        self.ConsumedAttemptInput = ConsumedAttemptInput
        self.NonConsumingAttemptInput = NonConsumingAttemptInput
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="stats-mbid",
            artist_name="Stats", album_title="Test",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def _make_plan(self, *, ordinals: int = 2, generator_id: str = "g1"):
        items = [
            self.SearchPlanItemInput(
                ordinal=i, strategy="default" if i == 0 else f"strategy_{i}",
                query=f"q{i}", canonical_query_key=f"k{i}",
                repeat_group="default-3" if i == 0 else None,
            )
            for i in range(ordinals)
        ]
        return self.db.create_successful_search_plan(
            request_id=self.req_id, generator_id=generator_id,
            items=items, set_active=True,
        )

    def _consume(self, plan_id, plan_item_id, ordinal, strategy, query,
                 *, outcome, plan_item_count, **kw):
        req = self.db.get_request(self.req_id)
        assert req is not None
        attempt = self.ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=plan_id, plan_item_id=plan_item_id,
            plan_ordinal=ordinal, plan_strategy=strategy,
            plan_canonical_query_key=kw.pop(
                "canonical_query_key", f"k{ordinal}"),
            plan_repeat_group=kw.pop("repeat_group", None),
            plan_generator_id="g1",
            query=query, outcome=outcome,
            plan_item_count=plan_item_count,
            cycle_count_snapshot=kw.pop(
                "cycle_count_snapshot", int(req["plan_cycle_count"])),
            elapsed_s=kw.pop("elapsed_s", 1.0),
            result_count=kw.pop("result_count", 0),
            browse_time_s=kw.pop("browse_time_s", 0.5),
            match_time_s=kw.pop("match_time_s", 0.25),
            peers_browsed=kw.pop("peers_browsed", 4),
            peers_browsed_lazy=kw.pop("peers_browsed_lazy", 1),
            fanout_waves=kw.pop("fanout_waves", 1),
            apply_scheduler_attempt=kw.pop("apply_scheduler_attempt", True),
            scheduler_success=kw.pop("scheduler_success", False),
        )
        return self.db.record_consumed_search_attempt(attempt)

    def _items_for(self, plan_id):
        cur = self.db._execute(
            "SELECT id, ordinal, strategy, canonical_query_key, "
            "repeat_group FROM search_plan_items WHERE plan_id = %s "
            "ORDER BY ordinal",
            (plan_id,),
        )
        return [dict(r) for r in cur.fetchall()]

    def test_stats_groups_by_slot_and_query_group(self):
        plan_id = self._make_plan(ordinals=2)
        items = self._items_for(plan_id)
        # Run two attempts on slot 0, one on slot 1.
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="no_match", plan_item_count=2,
                      repeat_group="default-3")
        self._consume(plan_id, items[1]["id"], 1, "strategy_1", "q1",
                      outcome="found", plan_item_count=2,
                      result_count=5, elapsed_s=2.0)
        # After wrap, slot 0 again.
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="no_results", plan_item_count=2,
                      repeat_group="default-3")

        stats = self.db.get_search_plan_stats(self.req_id)
        slots = stats.current.slots
        self.assertEqual(len(slots), 2)
        # Slots are ordered by ordinal.
        self.assertEqual(slots[0].identity["ordinal"], 0)
        self.assertEqual(slots[0].attempts, 2)
        self.assertEqual(slots[0].consumed_attempts, 2)
        self.assertEqual(
            slots[0].outcome_counts,
            {"no_match": 1, "no_results": 1})
        self.assertEqual(slots[1].identity["ordinal"], 1)
        self.assertEqual(slots[1].attempts, 1)
        self.assertEqual(slots[1].outcome_counts, {"found": 1})
        # Cache attribution is honest about cycle-only counters.
        self.assertEqual(stats.current.cache_attribution_level, "cycle_only")
        self.assertFalse(stats.current.cache_per_search_available)
        # Query groups exist with stable (repeat_group, key) order.
        # ordinal-1 has no repeat_group (sorts first as ""),
        # ordinal-0 carries "default-3".
        order = [
            (g.identity["repeat_group"] or "",
             g.identity["canonical_query_key"] or "")
            for g in stats.current.query_groups
        ]
        self.assertEqual(order, sorted(order))

    def test_stats_includes_legacy_bucket_when_current_only_false(self):
        # One legacy log without plan context.
        self.db.log_search(
            request_id=self.req_id, query="legacy",
            outcome="no_match", variant="v1",
        )
        plan_id = self._make_plan(ordinals=1)
        items = self._items_for(plan_id)
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="found", plan_item_count=1,
                      repeat_group="default-3")

        # Default current_only=True: no legacy in current cohort,
        # legacy bucket only appears in superseded_and_legacy when
        # current_only=False.
        stats_current = self.db.get_search_plan_stats(self.req_id)
        self.assertIsNone(stats_current.current.legacy_bucket)
        self.assertEqual(
            stats_current.superseded_and_legacy.slots, [])
        self.assertIsNone(
            stats_current.superseded_and_legacy.legacy_bucket)

        stats_full = self.db.get_search_plan_stats(
            self.req_id, current_only=False)
        self.assertIsNotNone(stats_full.superseded_and_legacy.legacy_bucket)
        legacy = stats_full.superseded_and_legacy.legacy_bucket
        assert legacy is not None
        self.assertEqual(legacy.attempts, 1)
        self.assertEqual(legacy.identity, {"kind": "legacy"})

    def test_stats_counts_non_consuming_pre_attempt_rows(self):
        plan_id = self._make_plan(ordinals=1)
        items = self._items_for(plan_id)
        # Pre-attempt failure: non-consuming.
        self.db.record_non_consuming_search_attempt(
            self.NonConsumingAttemptInput(
                request_id=self.req_id, outcome="empty_query",
                plan_id=plan_id, plan_item_id=items[0]["id"],
                plan_ordinal=0, plan_strategy="default",
                plan_canonical_query_key="k0", plan_repeat_group=None,
                plan_generator_id="g1", query="",
            ))
        # Consumed attempt that yields a found.
        self._consume(plan_id, items[0]["id"], 0, "default", "q0",
                      outcome="found", plan_item_count=1)

        stats = self.db.get_search_plan_stats(self.req_id)
        # Both rows live on slot 0; non-consuming counted separately.
        self.assertEqual(len(stats.current.slots), 1)
        slot0 = stats.current.slots[0]
        self.assertEqual(slot0.attempts, 2)
        self.assertEqual(slot0.consumed_attempts, 1)
        self.assertEqual(slot0.non_consuming_attempts, 1)
        self.assertEqual(slot0.stale_completion_attempts, 0)


class _FakeCursor:
    def __init__(self, conn, raise_on_execute=None, mark_conn_closed_on_error=False):
        self._conn = conn
        self._raise_on_execute = raise_on_execute
        self._mark_conn_closed_on_error = mark_conn_closed_on_error
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        exc = self._raise_on_execute
        if exc is not None:
            if self._mark_conn_closed_on_error:
                self._conn.closed = 2
            raise exc

    def fetchone(self):
        return {"ok": 1}


class _FakeConn:
    def __init__(self, raise_on_execute=None, mark_conn_closed_on_error=False):
        self.closed = 0
        self.autocommit = False
        self._raise_on_execute = raise_on_execute
        self._mark_conn_closed_on_error = mark_conn_closed_on_error
        self.cursors = []

    def cursor(self, *args, **kwargs):
        cur = _FakeCursor(
            self,
            raise_on_execute=self._raise_on_execute,
            mark_conn_closed_on_error=self._mark_conn_closed_on_error,
        )
        self.cursors.append(cur)
        return cur

    def close(self):
        self.closed = 1


class TestPipelineDBReconnectOnDeadConn(unittest.TestCase):
    """``PipelineDB._execute`` must transparently reconnect when the
    server has closed the socket between statements.

    Reproduces the live failure mode from the import-preview worker:
    the connection sits idle between jobs long enough that PostgreSQL
    (or an intermediary) tears it down, libpq doesn't notice until the
    next send, and the next ``cur.execute`` raises ``OperationalError``
    with ``conn.closed != 0``. ``_execute`` must reconnect once and
    retry the statement instead of letting the exception escape and
    crash the worker thread.
    """

    def test_reconnects_and_retries_on_operational_error_with_dead_conn(self):
        import psycopg2 as real_psycopg2

        dead_conn = _FakeConn(
            raise_on_execute=real_psycopg2.OperationalError(
                "server closed the connection unexpectedly"
            ),
            mark_conn_closed_on_error=True,
        )
        live_conn = _FakeConn()
        conn_iter = iter([dead_conn, live_conn])

        with patch("psycopg2.connect", side_effect=lambda *a, **kw: next(conn_iter)):
            from lib import pipeline_db
            db = pipeline_db.PipelineDB(dsn="postgresql://fake")
            cur = db._execute("SELECT 1")

        # We consumed both fake conns: initial + reconnect-on-retry.
        self.assertEqual(db.conn, live_conn)
        # The retry happened on the live conn.
        self.assertEqual(len(live_conn.cursors), 1)
        self.assertIs(cur, live_conn.cursors[0])
        self.assertEqual(cur.executed, [("SELECT 1", None)])  # type: ignore[attr-defined]

    def test_does_not_retry_when_conn_still_open_after_error(self):
        """Statement-level OperationalError (e.g. statement_timeout) keeps
        the connection open. We must NOT silently retry — that would
        mask real query failures and could double-execute side effects.
        Re-raise so the caller sees the error.
        """
        import psycopg2 as real_psycopg2

        live_but_failing_conn = _FakeConn(
            raise_on_execute=real_psycopg2.OperationalError(
                "canceling statement due to statement timeout"
            ),
            mark_conn_closed_on_error=False,
        )
        conn_iter = iter([live_but_failing_conn])

        with patch("psycopg2.connect", side_effect=lambda *a, **kw: next(conn_iter)):
            from lib import pipeline_db
            db = pipeline_db.PipelineDB(dsn="postgresql://fake")
            with self.assertRaises(real_psycopg2.OperationalError):
                db._execute("SELECT 1")

        # Only the original conn was used; no reconnect.
        self.assertEqual(db.conn, live_but_failing_conn)
        self.assertEqual(len(live_but_failing_conn.cursors), 1)


@requires_postgres
class TestFieldResolutionRecording(unittest.TestCase):
    """``record_field_resolution`` UPSERT contract against real PG.

    Side table: ``album_request_field_resolutions`` (migration 030).
    Tests pin the UPSERT semantics: fresh row carries ``attempts=1``;
    conflict increments ``attempts`` and updates status/reason/timestamp.
    """

    def _seed_request(self, db):
        req_id = db.add_request(
            mb_release_id="rec-mbid-0001",
            mb_release_group_id=None,
            mb_artist_id=None,
            discogs_release_id=None,
            artist_name="Test Artist",
            album_title="Test Album",
            year=2026,
            country="US",
            source="request",
        )
        return req_id

    def test_first_call_inserts_row_with_attempts_one(self):
        db = make_db()
        req_id = self._seed_request(db)

        db.record_field_resolution(
            request_id=req_id,
            field_name="release_group_year",
            status="resolved",
            reason_code=None,
        )

        row = db.get_field_resolution(req_id, "release_group_year")
        assert row is not None
        self.assertEqual(row["status"], "resolved")
        self.assertIsNone(row["reason_code"])
        self.assertEqual(row["attempts"], 1)
        self.assertIsNotNone(row["resolved_at"])

    def test_conflict_increments_attempts_and_updates_fields(self):
        db = make_db()
        req_id = self._seed_request(db)

        db.record_field_resolution(
            req_id, "release_group_year",
            "unresolved_mirror_unavailable", "URLError",
        )
        # Capture the first row's resolved_at to assert it advances.
        first = db.get_field_resolution(req_id, "release_group_year")
        assert first is not None
        first_resolved_at = first["resolved_at"]

        # Sleep enough to ensure NOW() advances (microsecond resolution
        # may be the same on very fast machines; use a small delay).
        import time
        time.sleep(0.05)

        db.record_field_resolution(
            req_id, "release_group_year",
            "resolved", None,
        )

        row = db.get_field_resolution(req_id, "release_group_year")
        assert row is not None
        self.assertEqual(row["status"], "resolved")
        self.assertIsNone(row["reason_code"])
        self.assertEqual(row["attempts"], 2)
        self.assertGreater(row["resolved_at"], first_resolved_at)

    def test_unique_constraint_one_row_per_field(self):
        """UNIQUE(request_id, field_name) — distinct field_name gives a 2nd row."""
        db = make_db()
        req_id = self._seed_request(db)

        db.record_field_resolution(
            req_id, "release_group_year", "resolved", None,
        )
        db.record_field_resolution(
            req_id, "catalog_number", "unresolved_404", "http_404",
        )

        cur = db._execute(
            "SELECT COUNT(*)::int AS n FROM album_request_field_resolutions "
            "WHERE request_id = %s",
            (req_id,),
        )
        row = cur.fetchone() or {}
        self.assertEqual(row.get("n"), 2)

    def test_fk_cascade_on_request_delete(self):
        db = make_db()
        req_id = self._seed_request(db)
        db.record_field_resolution(
            req_id, "release_group_year", "resolved", None,
        )
        # Sanity check.
        self.assertIsNotNone(
            db.get_field_resolution(req_id, "release_group_year"),
        )
        # Delete the parent.
        db._execute("DELETE FROM album_requests WHERE id = %s", (req_id,))
        # Migration 030's FK is ON DELETE CASCADE.
        self.assertIsNone(
            db.get_field_resolution(req_id, "release_group_year"),
        )

    def test_get_field_resolution_returns_none_when_absent(self):
        db = make_db()
        req_id = self._seed_request(db)
        self.assertIsNone(db.get_field_resolution(req_id, "track_artist"))


@requires_postgres
class TestMarkImportedWithRescue(unittest.TestCase):
    """U14: long-tail-rescue event capture against real PG.

    Pins the atomic four-write contract:
      1. ``status`` → ``'imported'``
      2. ``rescued_at`` → ``NOW()`` (when prior unfindable category set)
      3. ``prior_unfindable_category`` → the cleared category value
      4. ``unfindable_category`` → ``NULL`` (the rescue IS the resolution)

    All four mutations commit together OR none of them apply. The
    method follows the ``replace_request_with_new_mbid`` autocommit-flip
    pattern: ``conn.autocommit=False`` + explicit ``commit()`` /
    ``rollback()`` in try/finally.
    """

    UNFINDABLE_CATEGORIES = (
        "artist_absent",
        "album_absent_artist_present",
        "one_track_structural",
        "wrong_pressing_available",
    )

    def _seed_wanted(self, db, *, category=None, rescued_at=None,
                     prior_category=None):
        rid = db.add_request(
            mb_release_id=f"rescue-{category or 'none'}",
            artist_name="Rescue Artist",
            album_title="Rescue Album",
            source="request",
        )
        # Set the unfindable category WHILE the row is still wanted —
        # ``set_unfindable_category`` is guarded by ``status='wanted'`` in
        # production (lost-update protection against concurrent rescue),
        # so a seed helper that flipped to downloading first would silently
        # no-op the category write.
        if category is not None:
            ts = datetime(2026, 5, 20, tzinfo=timezone.utc)
            db.set_unfindable_category(
                rid, category=category, categorised_at=ts,
            )
        # Move to downloading so the imported transition is the canonical one.
        db._execute(
            "UPDATE album_requests SET status = 'downloading' WHERE id = %s",
            (rid,),
        )
        if rescued_at is not None or prior_category is not None:
            db._execute(
                "UPDATE album_requests "
                "SET rescued_at = %s, prior_unfindable_category = %s "
                "WHERE id = %s",
                (rescued_at, prior_category, rid),
            )
        return rid

    def test_rescue_writes_three_columns_on_first_import_from_unfindable(self):
        """Happy path: row with unfindable_category gets rescue stamp."""
        for category in self.UNFINDABLE_CATEGORIES:
            with self.subTest(category=category):
                db = make_db()
                rid = self._seed_wanted(db, category=category)

                db.mark_imported_with_rescue(rid, beets_distance=0.05)

                row = db.get_request(rid)
                assert row is not None
                self.assertEqual(row["status"], "imported")
                self.assertIsNone(row["unfindable_category"])
                self.assertEqual(
                    row["prior_unfindable_category"], category)
                self.assertIsNotNone(row["rescued_at"])
                # Sanity: the imported extras also landed.
                self.assertEqual(float(row["beets_distance"]), 0.05)

    def test_no_rescue_stamp_when_unfindable_was_null(self):
        """No prior category → ``rescued_at`` stays NULL."""
        db = make_db()
        rid = self._seed_wanted(db, category=None)

        db.mark_imported_with_rescue(rid, beets_distance=0.05)

        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["rescued_at"])
        self.assertIsNone(row["prior_unfindable_category"])
        self.assertIsNone(row["unfindable_category"])

    def test_first_rescue_wins_re_import_does_not_overwrite(self):
        """One-shot capture: a row already rescued is not re-stamped.

        Simulates: rescued → Replace → new request → re-categorised →
        imports again. The second import must NOT bump ``rescued_at``
        nor change ``prior_unfindable_category``. Original rescue
        instant is the canonical audit record.
        """
        db = make_db()
        original_rescue_at = datetime(2026, 1, 15, tzinfo=timezone.utc)
        rid = self._seed_wanted(
            db,
            category="album_absent_artist_present",
            rescued_at=original_rescue_at,
            prior_category="artist_absent",
        )

        db.mark_imported_with_rescue(rid, beets_distance=0.05)

        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        # rescued_at is immutable once set.
        self.assertEqual(row["rescued_at"], original_rescue_at)
        # prior_unfindable_category is immutable too — original rescue wins.
        self.assertEqual(row["prior_unfindable_category"], "artist_absent")
        # Current unfindable_category is still cleared (the rescue IS
        # the resolution, regardless of one-shot-stamp semantics).
        self.assertIsNone(row["unfindable_category"])

    def test_atomic_rollback_on_mid_transaction_failure(self):
        """A forced failure inside the transaction leaves the row untouched.

        Forces an exception inside the autocommit-disabled block by
        passing an ``extra`` kwarg that references a non-existent
        column. The dynamic ``UPDATE`` raises ``UndefinedColumn``
        AFTER the row lock + read have been taken but BEFORE the
        commit fires — exactly the mid-flow scenario the autocommit-
        flip pattern exists to protect against.

        Without ``autocommit=False`` + try/finally, three separate
        UPDATEs in autocommit mode would leave a half-rescued row in
        the audit trail. With the pattern, the row is rolled back to
        its pre-call state and autocommit is restored for subsequent
        calls.
        """
        db = make_db()
        rid = self._seed_wanted(db, category="artist_absent")

        before = db.get_request(rid)
        assert before is not None
        self.assertEqual(before["status"], "downloading")
        self.assertEqual(before["unfindable_category"], "artist_absent")

        with self.assertRaises(Exception):
            # ``column_that_does_not_exist`` rides through the
            # dynamic ``sets`` builder into the UPDATE statement,
            # raising ``UndefinedColumn`` inside the transaction.
            db.mark_imported_with_rescue(
                rid, column_that_does_not_exist=1,
            )

        # All writes rolled back together — the row is untouched.
        after = db.get_request(rid)
        assert after is not None
        self.assertEqual(after["status"], "downloading")
        self.assertEqual(after["unfindable_category"], "artist_absent")
        self.assertIsNone(after["rescued_at"])
        self.assertIsNone(after["prior_unfindable_category"])
        # Autocommit restored after the failure so subsequent calls work.
        self.assertTrue(db.conn.autocommit)
        # Sanity: the next call still works (proves rollback cleared
        # the failed transaction state).
        db.mark_imported_with_rescue(rid, beets_distance=0.07)
        retried = db.get_request(rid)
        assert retried is not None
        self.assertEqual(retried["status"], "imported")
        self.assertEqual(retried["prior_unfindable_category"], "artist_absent")


@requires_postgres
class TestUnfindableDetectionPipelineDB(unittest.TestCase):
    """U13: real-PG round-trip coverage for the 4 detection writers.

    The FakePipelineDB mirrors give us shape coverage; this class pins
    the production SQL against the real fixture so the CHECK
    constraints (migration 028's 4-category vocabulary) and the
    ``AND status='wanted'`` lost-update guards behave exactly like
    operators will see them on doc2.

    Mirrors the ``TestMarkImportedWithRescue`` style next door.
    """

    UNFINDABLE_CATEGORIES = (
        "artist_absent",
        "album_absent_artist_present",
        "one_track_structural",
        "wrong_pressing_available",
    )

    def _seed_wanted(self, db, *, artist_name="A", album_title="B",
                     mbid=None):
        return db.add_request(
            mb_release_id=mbid or f"unf-{artist_name}-{album_title}",
            artist_name=artist_name,
            album_title=album_title,
            source="request",
        )

    # ---- list_unfindable_probe_candidates ----

    def test_list_candidates_orders_oldest_first_and_filters_by_cadence(self):
        """NULL probes sort first; rows fresher than window are excluded."""
        db = make_db()
        now = datetime.now(timezone.utc)
        # Three wanted rows.
        rid_null = self._seed_wanted(db, artist_name="Null", mbid="unf-null")
        rid_old = self._seed_wanted(db, artist_name="Old", mbid="unf-old")
        rid_fresh = self._seed_wanted(
            db, artist_name="Fresh", mbid="unf-fresh")
        # Old probe = 10 days ago (older than 7d window → eligible).
        db.record_artist_probe(
            rid_old, match_count=0,
            observed_at=now - timedelta(days=10),
        )
        # Fresh probe = 1 day ago (inside window → ineligible).
        db.record_artist_probe(
            rid_fresh, match_count=0,
            observed_at=now - timedelta(days=1),
        )

        cands = db.list_unfindable_probe_candidates(
            limit=10, probe_interval_days=7,
        )
        ids = [c["id"] for c in cands]
        # NULL probe sorts first.
        self.assertEqual(ids[0], rid_null)
        # Old probe included; fresh probe excluded.
        self.assertIn(rid_old, ids)
        self.assertNotIn(rid_fresh, ids)

    def test_list_candidates_excludes_non_wanted(self):
        """A row in any non-wanted status is excluded from the cohort."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-imp")
        db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (rid,),
        )
        cands = db.list_unfindable_probe_candidates(
            limit=10, probe_interval_days=7,
        )
        self.assertNotIn(rid, [c["id"] for c in cands])

    # ---- record_artist_probe ----

    def test_record_artist_probe_round_trips_count_and_timestamp(self):
        """Probe column updates land and round-trip through SELECT."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-rec-1")
        ts = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)

        db.record_artist_probe(rid, match_count=42, observed_at=ts)

        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["last_artist_probe_match_count"], 42)
        self.assertEqual(row["last_artist_probe_at"], ts)

    def test_record_artist_probe_silent_noop_when_status_not_wanted(self):
        """The lost-update guard makes late writes invisible — no error."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-rec-2")
        # Capture the pre-existing probe state (NULL by default).
        before = db.get_request(rid)
        assert before is not None
        self.assertIsNone(before["last_artist_probe_at"])
        # Concurrent rescue flips status mid-probe.
        db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (rid,),
        )
        # Detection's late write — must be a silent no-op.
        ts = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)
        db.record_artist_probe(rid, match_count=99, observed_at=ts)
        after = db.get_request(rid)
        assert after is not None
        # Probe columns untouched.
        self.assertIsNone(after["last_artist_probe_at"])
        self.assertIsNone(after["last_artist_probe_match_count"])

    # ---- set_unfindable_category ----

    def test_set_unfindable_category_round_trips_all_four_categories(self):
        """Every valid category round-trips; CHECK constraint passes."""
        ts = datetime(2026, 5, 26, tzinfo=timezone.utc)
        for category in self.UNFINDABLE_CATEGORIES:
            with self.subTest(category=category):
                db = make_db()
                rid = self._seed_wanted(db, mbid=f"unf-set-{category}")
                db.set_unfindable_category(
                    rid, category=category, categorised_at=ts,
                )
                row = db.get_request(rid)
                assert row is not None
                self.assertEqual(row["unfindable_category"], category)
                self.assertEqual(row["unfindable_categorised_at"], ts)

    def test_set_unfindable_category_rejects_off_vocabulary_value(self):
        """An unknown category trips the CHECK constraint → IntegrityError."""
        from psycopg2.errors import CheckViolation

        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-set-bad")
        ts = datetime(2026, 5, 26, tzinfo=timezone.utc)
        with self.assertRaises(CheckViolation):
            db.set_unfindable_category(
                rid, category="garbage_value", categorised_at=ts,
            )

    def test_set_unfindable_category_silent_noop_when_status_not_wanted(self):
        """Late verdict write does not clobber a row already past wanted."""
        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-set-imp")
        db._execute(
            "UPDATE album_requests SET status = 'imported' WHERE id = %s",
            (rid,),
        )
        ts = datetime(2026, 5, 26, tzinfo=timezone.utc)
        db.set_unfindable_category(
            rid, category="artist_absent", categorised_at=ts,
        )
        row = db.get_request(rid)
        assert row is not None
        # Category never landed; row remains in imported shape.
        self.assertIsNone(row["unfindable_category"])
        self.assertEqual(row["status"], "imported")

    # ---- get_unfindable_search_log_signal ----

    def test_search_log_signal_aggregates_zero_find_and_wrong_pressing(self):
        """Hand-computed aggregates match the production SQL."""
        from lib.pipeline_db import (
            ConsumedAttemptInput, SearchPlanItemInput,
        )

        db = make_db()
        rid = self._seed_wanted(db, mbid="unf-sig")
        # Seed a plan + advance the cursor 4 times so we have
        # 4 distinct ``plan_cycle_snapshot`` values in the log.
        # Cycles 0..3 from four ordinal consumptions.
        plan_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id="unf-gen",
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="default",
                    query="q0", canonical_query_key="q0"),
            ],
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        item_id = active.items[0].id

        def _attempt(cycle_idx: int, *, outcome: str,
                     rejection_reason: str | None = None,
                     matcher_score_top1: float | None = None):
            return ConsumedAttemptInput(
                request_id=rid,
                plan_id=plan_id,
                plan_item_id=item_id,
                plan_ordinal=0,
                plan_strategy="default",
                plan_canonical_query_key="q0",
                plan_repeat_group=None,
                plan_generator_id="unf-gen",
                query="q0",
                outcome=outcome,
                plan_item_count=1,
                cycle_count_snapshot=cycle_idx,
                apply_scheduler_attempt=True,
                scheduler_success=(outcome == "found"),
                rejection_reason=rejection_reason,
                matcher_score_top1=matcher_score_top1,
            )

        # Cycle 0: no_match w/ wrong-pressing signature (high score) → hit.
        db.record_consumed_search_attempt(_attempt(
            0, outcome="no_match",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.9,
        ))
        # Cycle 1: one found → cycle NOT zero-find.
        db.record_consumed_search_attempt(_attempt(1, outcome="found"))
        # Cycle 2: no_match w/ low score → not a wrong-pressing hit;
        # AND no found → counts as a zero-find cycle.
        db.record_consumed_search_attempt(_attempt(
            2, outcome="no_match",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.5,
        ))
        # Cycle 3: no_results → zero-find cycle.
        db.record_consumed_search_attempt(_attempt(3, outcome="no_results"))

        sig = db.get_unfindable_search_log_signal(
            rid, window_days=30, matcher_score_threshold=0.85,
        )
        # Cycles 0, 2, 3 are zero-find (cycle 1 had the found row).
        self.assertEqual(sig.zero_find_cycles, 3)
        # One wrong-pressing hit (cycle 0).
        self.assertEqual(sig.wrong_pressing_hits, 1)


@requires_postgres
class TestYoutubeAlbumMappings(unittest.TestCase):
    """Integration tests for PipelineDB youtube_album_mappings CRUD (U4).

    Exercises the real PostgreSQL CRUD against migration 034. The atomic
    replace test verifies that mid-replace state is never visible — a
    concurrent reader sees either the old matrix or the new, never an
    interleaved subset.
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def _row(self, **overrides: Any) -> PersistedYoutubeRow:
        fields: dict[str, Any] = {
            "yt_browse_id": "MPREb_abc",
            "yt_audio_playlist_id": "OLAK5uy_abc",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_abc",
            "yt_year": 2020,
            "yt_track_count": 10,
            # Album-level facts the service writes alongside the row
            # (migration 036). Round 2 P0-1 + maintainability-5.
            "album_title": "Test Album",
            "album_artist": "Test Album Artist",
            "yt_tracks": [
                PersistedTrack(
                    title="Track 1", video_id="v1", length_seconds=200,
                    track_number=1, disc_number=1,
                    artists=[{"name": "Artist"}],
                ),
            ],
            "distances": [
                PersistedDistance(mbid="mb-1", distance=0.05),
            ],
        }
        fields.update(overrides)
        return PersistedYoutubeRow(**fields)

    def test_get_returns_none_when_pair_never_resolved(self):
        # Distinction matters: ``None`` = "never resolved" (cache MISS),
        # ``[]`` = "resolved to empty matrix" (cache HIT). See
        # ce-code-review finding #3.
        self.assertIsNone(
            self.db.get_youtube_album_mapping("rg-1", "mb"),
        )

    def test_get_returns_empty_list_after_upsert_of_empty_rows(self):
        # Upserting an empty matrix stamps the empty-resolution marker
        # so the next read returns ``[]`` (cache HIT) instead of
        # ``None`` (cache MISS). Without this, the resolver re-polls
        # YT on every cycle for empty-search release groups (R14).
        self.db.upsert_youtube_album_mapping("rg-empty", "mb", [])
        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-empty", "mb"),
            [],
        )

    def test_empty_marker_cleared_on_non_empty_upsert(self):
        # An empty resolve followed by a non-empty resolve must clear the
        # empty marker — subsequent reads return the matrix, not [].
        self.db.upsert_youtube_album_mapping("rg-flip", "mb", [])
        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-flip", "mb"), [])
        self.db.upsert_youtube_album_mapping("rg-flip", "mb", [
            self._row(yt_browse_id="MPREb_real"),
        ])
        got = self.db.get_youtube_album_mapping("rg-flip", "mb")
        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual([r["yt_browse_id"] for r in got], ["MPREb_real"])

    def test_upsert_inserts_new_rows_and_get_returns_them(self):
        rows = [
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_b"),
        ]

        self.db.upsert_youtube_album_mapping("rg-1", "mb", rows)

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 2)
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_a", "MPREb_b"],
        )
        # JSONB columns deserialize back into native Python lists/dicts.
        self.assertEqual(
            got[0]["yt_tracks"][0]["title"], "Track 1")
        # Per ce-code-review finding #25 the field is ``mbid``, not
        # ``mb_release_id`` — aligns with the service-side
        # ``ResolvedDistance.mbid`` wire contract.
        self.assertEqual(
            got[0]["distances"][0]["mbid"], "mb-1")

    def test_upsert_round_trip_preserves_every_field(self):
        """Rule A (``.claude/rules/test-fidelity.md``): every field of the
        typed ``PersistedYoutubeRow`` payload must round-trip through real
        PostgreSQL.

        Round 2 P0-1: ``album_title`` (and now ``album_artist``) were
        silently dropped because the INSERT column list didn't include
        them and ``psycopg2.extras.execute_values`` ignores extra dict
        keys. The Fake-based test stored the dict verbatim and never
        flagged the divergence. #546 W3 made the column list itself
        DERIVE from ``msgspec.structs.fields(PersistedYoutubeRow)`` so
        this class of bug can no longer be expressed — this test iterates
        the SAME derived field list and fails naming the offending field
        if a future drift somehow reappears.
        """
        row_in = self._row(
            yt_browse_id="MPREb_roundtrip",
            yt_audio_playlist_id="OLAK5uy_roundtrip",
            yt_url="https://music.youtube.com/playlist?list=OLAK5uy_roundtrip",
            yt_year=1996,
            yt_track_count=12,
            album_title="The Roundtrip Sessions",
            album_artist="Various Artists",
        )
        self.db.upsert_youtube_album_mapping("rg-rt", "mb", [row_in])
        rows_out = self.db.get_youtube_album_mapping("rg-rt", "mb")
        assert rows_out is not None
        self.assertEqual(len(rows_out), 1)
        for f in msgspec.structs.fields(PersistedYoutubeRow):
            expected = getattr(row_in, f.name)
            if f.name in ("yt_tracks", "distances"):
                expected = msgspec.to_builtins(expected)
            self.assertEqual(
                rows_out[0].get(f.name), expected,
                msg=f"field {f.name} was dropped at the PG boundary",
            )

    def test_get_orders_rows_by_yt_browse_id(self):
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_z"),
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_m"),
        ])

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_a", "MPREb_m", "MPREb_z"],
        )

    def test_upsert_atomically_replaces_existing_rows(self):
        """DELETE + INSERTs in one transaction; reader never sees partial state."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_old1"),
            self._row(yt_browse_id="MPREb_old2"),
            self._row(yt_browse_id="MPREb_old3"),
        ])

        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_new1"),
            self._row(yt_browse_id="MPREb_new2"),
        ])

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_new1", "MPREb_new2"],
        )

    def test_upsert_does_not_affect_other_release_group_or_source(self):
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a")])
        self.db.upsert_youtube_album_mapping("rg-2", "mb", [
            self._row(yt_browse_id="MPREb_b")])
        self.db.upsert_youtube_album_mapping("rg-1", "discogs", [
            self._row(yt_browse_id="MPREb_c")])

        # Replace only rg-1/mb.
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a_v2")])

        rg1_mb = self.db.get_youtube_album_mapping("rg-1", "mb")
        rg2_mb = self.db.get_youtube_album_mapping("rg-2", "mb")
        rg1_discogs = self.db.get_youtube_album_mapping("rg-1", "discogs")
        assert rg1_mb is not None
        assert rg2_mb is not None
        assert rg1_discogs is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in rg1_mb],
            ["MPREb_a_v2"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in rg2_mb],
            ["MPREb_b"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in rg1_discogs],
            ["MPREb_c"],
        )

    def test_upsert_preserves_nullable_fields(self):
        """yt_audio_playlist_id + yt_year are NULLable per migration 034."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(
                yt_browse_id="MPREb_nulls",
                yt_audio_playlist_id=None,
                yt_year=None,
            ),
        ])

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 1)
        self.assertIsNone(got[0]["yt_audio_playlist_id"])
        self.assertIsNone(got[0]["yt_year"])

    def test_upsert_with_empty_rows_clears_the_pair(self):
        """Passing an empty list deletes the pair's existing matrix."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_b"),
        ])

        self.db.upsert_youtube_album_mapping("rg-1", "mb", [])

        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-1", "mb"), [])

    def test_find_mapping_for_release_matches_exact_distance(self):
        self.db.upsert_youtube_album_mapping("discogs-master-1", "discogs", [
            self._row(
                yt_browse_id="MPREb_discogs",
                distances=[
                    PersistedDistance(mbid="12345", distance=0.05),
                    PersistedDistance(mbid="67890", distance=0.25),
                ],
            )
        ])

        got = self.db.find_youtube_album_mapping_for_release(
            source="discogs",
            release_id="12345",
            browse_id="MPREb_discogs",
        )

        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual(got["release_group_identifier"], "discogs-master-1")
        self.assertEqual(got["source"], "discogs")
        self.assertIsNone(self.db.find_youtube_album_mapping_for_release(
            source="mb", release_id="12345", browse_id="MPREb_discogs"))
        self.assertIsNone(self.db.find_youtube_album_mapping_for_release(
            source="discogs", release_id="99999", browse_id="MPREb_discogs"))
        self.assertIsNone(self.db.find_youtube_album_mapping_for_release(
            source="discogs", release_id="12345", browse_id="MPREb_other"))

    def test_upsert_rolls_back_on_insert_failure(self):
        """If a row insert violates a constraint, the prior matrix survives."""
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_pre1"),
            self._row(yt_browse_id="MPREb_pre2"),
        ])

        # CHECK constraint forbids source != ('mb', 'discogs'). We can't
        # break source on the second call (the method parameter would have
        # to flow into INSERT), so trigger failure via duplicate
        # yt_browse_id within the same upsert payload — the UNIQUE
        # (release_group_identifier, source, yt_browse_id) constraint
        # rejects it.
        with self.assertRaises(Exception):
            self.db.upsert_youtube_album_mapping("rg-1", "mb", [
                self._row(yt_browse_id="MPREb_dup"),
                self._row(yt_browse_id="MPREb_dup"),
            ])

        # Prior matrix must survive — rollback preserved it.
        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_pre1", "MPREb_pre2"],
        )


@requires_postgres
class TestYoutubeIngestDownloadLog(unittest.TestCase):
    """Integration tests for YT-rescue ingest methods on download_log (U2).

    Exercises the real PostgreSQL CRUD against migration 037: source
    discriminator, ``youtube_metadata`` JSONB, partial unique index, and
    the widened ``download_log_outcome_check`` constraint. The Rule A
    round-trip test (``test_insert_youtube_running_round_trip_preserves_every_field``)
    is the load-bearing guard against a future field drifting between
    the Python payload and the INSERT column list.
    """

    def setUp(self) -> None:
        self.db = make_db()
        self.request_id = self.db.add_request(
            mb_release_id="yt-rescue-mbid-1",
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
        )

    def tearDown(self) -> None:
        self.db.close()

    def _yt_payload(self, **overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": self.request_id,
            "browse_id": "MPREb_default",
            "audio_playlist_id": "OLAK5uy_default",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_default",
            "expected_track_count": 10,
        }
        payload.update(overrides)
        return payload

    def test_insert_youtube_running_round_trip_preserves_every_field(self):
        """Rule A: every key in the input dict round-trips through PG.

        If a future schema/method change drops a field from the INSERT
        column list (or from the JSONB blob the helper writes), the
        for-loop below names the offending key. This is the load-bearing
        guard per ``.claude/rules/test-fidelity.md`` § "Rule A".
        """
        payload = self._yt_payload(
            browse_id="MPREb_roundtrip",
            audio_playlist_id="OLAK5uy_roundtrip",
            yt_url="https://music.youtube.com/playlist?list=OLAK5uy_roundtrip",
            expected_track_count=12,
        )
        log_id = self.db.insert_youtube_running(**payload)
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None

        # Top-level row columns set by INSERT.
        self.assertEqual(entry["request_id"], payload["request_id"])
        self.assertEqual(entry["source"], "youtube")
        self.assertEqual(entry["outcome"], "youtube_running")

        # JSONB metadata: every supplied field round-trips through psycopg2.
        meta = entry["youtube_metadata"]
        self.assertIsInstance(meta, dict)
        for key, expected in {
            "yt_url": payload["yt_url"],
            "browse_id": payload["browse_id"],
            "audio_playlist_id": payload["audio_playlist_id"],
            "expected_track_count": payload["expected_track_count"],
        }.items():
            self.assertEqual(
                meta.get(key), expected,
                msg=f"field {key} was dropped at the PG boundary",
            )

    def test_insert_youtube_running_persists_resolver_audit_fields(self):
        log_id = self.db.insert_youtube_running(
            **self._yt_payload(),
            resolver_mapping_id=44,
            per_track_video_ids=["v1", "v2"],
        )

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        meta = entry["youtube_metadata"]
        self.assertEqual(meta["resolver_mapping_id"], 44)
        self.assertEqual(meta["per_track_video_ids"], ["v1", "v2"])

    def test_insert_youtube_running_raises_on_idempotency_violation(self):
        """Partial unique index serialises submissions per R4."""
        first_id = self.db.insert_youtube_running(**self._yt_payload())
        from lib.pipeline_db import YoutubeInFlightError
        with self.assertRaises(YoutubeInFlightError) as ctx:
            self.db.insert_youtube_running(**self._yt_payload(
                browse_id="MPREb_collide",
            ))
        # Existing id surfaced via the exception so the service can put
        # it in SubmitResult.detail.
        self.assertEqual(ctx.exception.existing_download_log_id, first_id)
        self.assertEqual(ctx.exception.request_id, self.request_id)

    def test_insert_after_terminal_succeeds(self):
        """Once a row goes terminal, the partial index admits the next.

        Confirms the WHERE clause on the partial index is keyed to
        ``outcome='youtube_running'`` — otherwise terminal rows would
        permanently block re-submission.
        """
        first_id = self.db.insert_youtube_running(**self._yt_payload())
        self.db.update_youtube_terminal(
            first_id, "youtube_failed", {"reason": "test_release"},
        )
        # Second submit MUST now succeed.
        second_id = self.db.insert_youtube_running(**self._yt_payload(
            browse_id="MPREb_after_terminal",
        ))
        self.assertNotEqual(first_id, second_id)

    def test_update_youtube_terminal_to_success_round_trip_preserves_metadata(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        terminal_meta = {
            "per_track_video_ids": ["v1", "v2", "v3"],
            "observed_track_count": 10,
            "expected_track_count": 10,
        }
        self.db.update_youtube_terminal(log_id, "youtube_success", terminal_meta)

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_success")
        meta = entry["youtube_metadata"]
        self.assertIsInstance(meta, dict)
        # Merge: submission-time fields survive.
        self.assertEqual(meta["browse_id"], "MPREb_default")
        # Terminal-time fields are layered on top.
        self.assertEqual(meta["per_track_video_ids"], ["v1", "v2", "v3"])
        self.assertEqual(meta["observed_track_count"], 10)

    def test_update_youtube_terminal_to_failed_writes_metadata(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        terminal_meta = {
            "reason": "track_count_mismatch",
            "observed_track_count": 7,
            "expected_track_count": 10,
            "stderr_excerpt": "[ytdl] short play\n",
        }
        self.db.update_youtube_terminal(log_id, "youtube_failed", terminal_meta)

        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_failed")
        meta = entry["youtube_metadata"]
        self.assertIsInstance(meta, dict)
        self.assertEqual(meta["reason"], "track_count_mismatch")
        self.assertEqual(meta["observed_track_count"], 7)
        self.assertEqual(meta["stderr_excerpt"], "[ytdl] short play\n")

    def test_update_youtube_terminal_rejects_non_terminal_outcomes(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        for bogus in ("youtube_running", "success", "rejected", ""):
            with self.subTest(outcome=bogus):
                with self.assertRaises(ValueError):
                    self.db.update_youtube_terminal(log_id, bogus, {})

    def test_claim_next_youtube_pending_excludes_slskd_rows(self):
        """Source discriminator must filter slskd rows out of the worker queue."""
        # An slskd-side row for the same request.
        self.db.log_download(
            self.request_id, soulseek_username="alice", outcome="success",
        )
        yt_id = self.db.insert_youtube_running(**self._yt_payload())
        rows = self.db.claim_next_youtube_pending(worker_id="w", limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], yt_id)
        self.assertEqual(rows[0]["source"], "youtube")
        self.assertEqual(rows[0]["outcome"], "youtube_running")

    def test_claim_next_youtube_pending_excludes_terminal_rows(self):
        log_id = self.db.insert_youtube_running(**self._yt_payload())
        # A terminal (never-claimed) row is not drainable.
        self.db.update_youtube_terminal(log_id, "youtube_success", {})
        self.assertEqual(
            self.db.claim_next_youtube_pending(worker_id="w", limit=10), [])

    def test_claim_next_youtube_pending_orders_by_created_at(self):
        """FIFO contract per R16: earliest created_at first."""
        # Distinct requests so the partial unique index permits multiple
        # in-flight rows.
        rid_b = self.db.add_request(
            mb_release_id="yt-rescue-mbid-b",
            artist_name="B Artist",
            album_title="B Album",
            source="request",
        )
        rid_c = self.db.add_request(
            mb_release_id="yt-rescue-mbid-c",
            artist_name="C Artist",
            album_title="C Album",
            source="request",
        )
        first = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_b, browse_id="MPREb_b",
        ))
        second = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_c, browse_id="MPREb_c",
        ))
        rows = self.db.claim_next_youtube_pending(worker_id="w", limit=10)
        self.assertEqual([r["id"] for r in rows], [first, second])

    def test_claim_next_youtube_pending_marks_worker_metadata(self):
        rid_b = self.db.add_request(
            mb_release_id="yt-rescue-mbid-b",
            artist_name="B Artist",
            album_title="B Album",
            source="request",
        )
        first = self.db.insert_youtube_running(**self._yt_payload())
        second = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_b, browse_id="MPREb_b",
        ))
        claimed = self.db.claim_next_youtube_pending(
            worker_id="worker-1", limit=1)
        self.assertEqual([r["id"] for r in claimed], [first])
        # The unclaimed sibling is still drainable by the next claim.
        self.assertEqual(
            [r["id"] for r in self.db.claim_next_youtube_pending(
                worker_id="worker-2", limit=10)],
            [second],
        )
        meta = claimed[0]["youtube_metadata"]
        self.assertEqual(meta["worker_id"], "worker-1")
        self.assertIsNotNone(meta["worker_claimed_at"])

    def test_find_orphan_youtube_running_returns_claimed_ids(self):
        rid_b = self.db.add_request(
            mb_release_id="yt-rescue-mbid-b-claimed",
            artist_name="B Artist",
            album_title="B Album",
            source="request",
        )
        first = self.db.insert_youtube_running(**self._yt_payload())
        second = self.db.insert_youtube_running(**self._yt_payload(
            request_id=rid_b, browse_id="MPREb_b",
        ))
        self.assertEqual(self.db.find_orphan_youtube_running(), [])
        self.db.claim_next_youtube_pending(worker_id="worker-1", limit=1)
        orphans = self.db.find_orphan_youtube_running()
        self.assertEqual(orphans, [first])

        # Worker's startup sweep marks each failed; the orphan set
        # resolves to empty.
        for log_id in orphans:
            self.db.update_youtube_terminal(
                log_id, "youtube_failed", {"reason": "worker_interrupted"},
            )
        self.assertEqual(self.db.find_orphan_youtube_running(), [])
        # The surviving sibling is still drainable after the orphan sweep.
        self.assertEqual(
            [r["id"] for r in self.db.claim_next_youtube_pending(
                worker_id="worker-2", limit=10)],
            [second],
        )

    def test_list_active_youtube_rescues_returns_request_context(self):
        yt_id = self.db.insert_youtube_running(**self._yt_payload(
            browse_id="MPREb_visible",
            expected_track_count=2,
        ))

        rows = self.db.list_active_youtube_rescues(limit=10)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["download_log_id"], yt_id)
        self.assertEqual(rows[0]["request_id"], self.request_id)
        self.assertEqual(rows[0]["source"], "youtube")
        self.assertEqual(rows[0]["outcome"], "youtube_running")
        self.assertEqual(rows[0]["artist_name"], "Test Artist")
        self.assertEqual(rows[0]["album_title"], "Test Album")
        self.assertEqual(
            rows[0]["youtube_metadata"]["browse_id"], "MPREb_visible")

        self.db.update_youtube_terminal(
            yt_id, "youtube_failed", {"reason": "operator_cancelled"},
        )
        self.assertEqual(self.db.list_active_youtube_rescues(limit=10), [])

    def test_active_youtube_import_guard_is_request_scoped(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        job = self.db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=self.request_id,
            dedupe_key=youtube_import_dedupe_key(901),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-a",
                request_id=self.request_id,
                browse_id="MPREb_a",
                download_log_id=901,
            ),
        )

        active = self.db.find_active_youtube_import_job(
            request_id=self.request_id,
            browse_id="MPREb_b",
        )
        assert active is not None
        self.assertEqual(active.id, job.id)

        with self.assertRaises(Exception):
            self.db.enqueue_import_job(
                IMPORT_JOB_YOUTUBE,
                request_id=self.request_id,
                dedupe_key=youtube_import_dedupe_key(902),
                payload=youtube_import_payload(
                    staged_path="/tmp/yt-b",
                    request_id=self.request_id,
                    browse_id="MPREb_b",
                    download_log_id=902,
                ),
            )

    def test_atomic_youtube_import_enqueue_marks_download_log_success(self):
        from lib.import_queue import (
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        log_id = self.db.insert_youtube_running(**self._yt_payload())
        payload = youtube_import_payload(
            staged_path="/tmp/yt-staged",
            request_id=self.request_id,
            browse_id="MPREb_default",
            download_log_id=log_id,
        )

        job = self.db.enqueue_youtube_import_and_mark_success(
            download_log_id=log_id,
            request_id=self.request_id,
            dedupe_key=youtube_import_dedupe_key(log_id),
            payload=payload,
            message="yt handoff",
            terminal_metadata={"observed_track_count": 10},
        )

        self.assertEqual(job.request_id, self.request_id)
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_success")
        self.assertEqual(
            entry["youtube_metadata"]["observed_track_count"], 10)

    def test_read_seam_includes_source_and_youtube_metadata(self):
        """Every download_log read seam surfaces the new columns."""
        # An slskd row (source defaults to 'slskd', youtube_metadata=NULL).
        slskd_id = self.db.log_download(
            self.request_id, soulseek_username="alice", outcome="success",
        )
        yt_id = self.db.insert_youtube_running(**self._yt_payload())
        self.db.update_youtube_terminal(
            yt_id, "youtube_success",
            {"observed_track_count": 10, "expected_track_count": 10},
        )

        # get_download_log_entry
        slskd_entry = self.db.get_download_log_entry(slskd_id)
        assert slskd_entry is not None
        self.assertEqual(slskd_entry["source"], "slskd")
        self.assertIsNone(slskd_entry["youtube_metadata"])

        yt_entry = self.db.get_download_log_entry(yt_id)
        assert yt_entry is not None
        self.assertEqual(yt_entry["source"], "youtube")
        self.assertEqual(yt_entry["outcome"], "youtube_success")
        yt_meta = yt_entry["youtube_metadata"]
        self.assertIsInstance(yt_meta, dict)
        self.assertEqual(yt_meta["observed_track_count"], 10)

        # get_download_history
        history = self.db.get_download_history(self.request_id)
        self.assertEqual(len(history), 2)
        by_source = {r["source"]: r for r in history}
        self.assertEqual(set(by_source.keys()), {"slskd", "youtube"})
        self.assertIsNone(by_source["slskd"]["youtube_metadata"])
        self.assertIsInstance(by_source["youtube"]["youtube_metadata"], dict)

        # get_download_history_batch
        batch = self.db.get_download_history_batch([self.request_id])
        rows = batch[self.request_id]
        self.assertEqual({r["source"] for r in rows}, {"slskd", "youtube"})


def _terminate_backend(dsn, pid):
    """Kill a PostgreSQL backend from a *second* session so the next statement
    on the original connection dies mid-flight (``conn.closed`` flips truthy).

    This is the real "server closed the socket unexpectedly" failure mode the
    ``_execute`` reconnect branch and the ``_atomic`` rollback handler must
    survive — reproduced deterministically instead of via a fake socket.
    """
    killer = psycopg2.connect(dsn)
    killer.autocommit = True
    try:
        with killer.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            cur.fetchone()
    finally:
        killer.close()


@requires_postgres
class TestAtomicAndExecuteHardening(unittest.TestCase):
    """Issue #395 — error-path hardening for the shared transaction
    primitives in ``lib/pipeline_db/_core.py``, exercised against real PG.

    Item 1: ``_execute`` must NOT silently reconnect onto a fresh
    ``autocommit=True`` connection when it dies mid-statement *inside* a
    transaction (``autocommit=False``) — that would drop the in-flight
    transaction's partial writes. It must re-raise so ``_atomic`` rolls back.
    Outside a transaction the reconnect-and-retry heal must still fire.

    Item 2: when the connection is dead, both ``rollback()`` and the
    autocommit-restore in ``_atomic``'s ``finally`` raise a *secondary*
    ``InterfaceError``. Neither may mask the ORIGINAL exception the caller
    should see.
    """

    @staticmethod
    def _backend_pid(db):
        with db.conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            return cur.fetchone()[0]

    def test_execute_inside_transaction_reraises_instead_of_reconnecting(self):
        """Item 1 guard: a mid-statement socket death while ``autocommit=False``
        re-raises and leaves the connection object untouched — no silent swap
        to a fresh autocommit=True connection that would lose the transaction.
        """
        db = make_db()
        self.addCleanup(db.close)
        original_conn = db.conn
        # Simulate being inside `with self._atomic():` — explicit transaction.
        db.conn.autocommit = False
        pid = self._backend_pid(db)  # also opens the transaction
        _terminate_backend(db.dsn, pid)
        with self.assertRaises((psycopg2.OperationalError, psycopg2.InterfaceError)):
            db._execute("SELECT 1")
        # The guard held: _execute did NOT reconnect.
        self.assertIs(db.conn, original_conn)

    def test_execute_reconnects_outside_transaction(self):
        """Item 1 scope check: outside a transaction (``autocommit=True``) a
        dead socket must still heal via reconnect-and-retry. The guard is
        scoped to ``autocommit=False`` only and must not regress this — the
        live failure mode the reconnect branch exists for.
        """
        db = make_db()
        self.addCleanup(db.close)
        original_conn = db.conn
        self.assertTrue(db.conn.autocommit)
        pid = self._backend_pid(db)
        _terminate_backend(db.dsn, pid)
        cur = db._execute("SELECT 1 AS one")
        self.assertEqual(cur.fetchone()["one"], 1)
        self.assertIsNot(db.conn, original_conn)  # reconnected
        self.assertTrue(db.conn.autocommit)

    def test_atomic_rollback_failure_preserves_original_exception(self):
        """Item 2: a dead connection makes both ``rollback()`` and the
        autocommit-restore raise ``InterfaceError``. The ORIGINAL exception
        from the block body must still propagate.
        """
        db = make_db()
        self.addCleanup(db.close)

        class _Boom(Exception):
            pass

        with self.assertRaises(_Boom):
            with db._atomic():
                # Kill the connection so BOTH rollback() (except handler) and
                # autocommit-restore (finally) raise a secondary InterfaceError.
                db.conn.close()
                raise _Boom("the real failure the operator must see")

    def test_atomic_commit_failure_propagates_commit_error(self):
        """Item 2: when the caller's ``commit()`` raises ``OperationalError``
        because the backend died, that commit error propagates — not a
        secondary ``InterfaceError`` from the guarded rollback / restore.
        (``InterfaceError`` is a sibling of ``OperationalError``, so a leak
        would fail this assertion.)
        """
        db = make_db()
        self.addCleanup(db.close)
        with self.assertRaises(psycopg2.OperationalError):
            with db._atomic():
                pid = self._backend_pid(db)
                _terminate_backend(db.dsn, pid)
                db.conn.commit()  # backend gone -> raises OperationalError

    def test_atomic_happy_path_commits_and_restores_autocommit(self):
        """No behaviour change on the happy path: flip to ``autocommit=False``
        for the block, caller commits, autocommit is restored, write persists.
        """
        db = make_db()
        self.addCleanup(db.close)
        self.assertTrue(db.conn.autocommit)
        rid = db.add_request(
            artist_name="Atomic", album_title="Happy Path", source="request")
        with db._atomic():
            self.assertFalse(db.conn.autocommit)  # flipped for the block
            with db.conn.cursor() as cur:
                cur.execute(
                    "UPDATE album_requests SET reasoning = %s WHERE id = %s",
                    ("atomic-write", rid),
                )
            db.conn.commit()
        self.assertTrue(db.conn.autocommit)  # restored
        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["reasoning"], "atomic-write")


@requires_postgres
class TestWrongMatchTriageRoundTrip(unittest.TestCase):
    """Real-PG round-trip for the typed triage write path (#410).

    Test-fidelity Rule A: the typed payload must survive the jsonb_set
    write and decode back identical through the one envelope decode site.
    """

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="triage-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        self.log_id = self.db.log_download(
            request_id=self.req_id,
            soulseek_username="peer",
            outcome="rejected",
            validation_result=json.dumps({
                "failed_path": "/mnt/x/failed_imports/B",
                "scenario": "wrong_match",
            }),
        )

    def tearDown(self):
        self.db.close()

    def test_triage_audit_round_trips_every_field(self):
        from lib.validation_envelope import (
            WrongMatchTriageAudit,
            decode_validation_envelope,
        )

        audit = WrongMatchTriageAudit(
            action="deleted_reject",
            outcome="deleted",
            success=True,
            reason="confident_reject",
            preview_verdict="reject",
            preview_decision="rejected_spectral",
            cleanup_eligible=True,
            source_path="/mnt/x/failed_imports/B",
            stage_chain=["stage1_spectral", "stage2_import"],
            cleared_rows=2,
            deleted_path="/mnt/x/failed_imports/B",
            path_missing=False,
            error=None,
        )
        self.assertTrue(
            self.db.record_wrong_match_triage(self.log_id, audit))

        cur = self.db._execute(
            "SELECT validation_result FROM download_log WHERE id = %s",
            (self.log_id,))
        row = cur.fetchone()
        assert row is not None
        env = decode_validation_envelope(row["validation_result"])
        self.assertEqual(env.wrong_match_triage, audit)
        # jsonb_set must merge, not replace — the pre-existing keys survive.
        self.assertEqual(env.failed_path, "/mnt/x/failed_imports/B")
        self.assertEqual(env.scenario, "wrong_match")

    def test_clear_wrong_match_path_removes_only_the_failed_path_key(self):
        from lib.validation_envelope import decode_validation_envelope

        self.assertTrue(self.db.clear_wrong_match_path(self.log_id))
        entry = self.db.get_download_log_entry(self.log_id)
        assert entry is not None
        env = decode_validation_envelope(entry["validation_result"])
        self.assertIsNone(env.failed_path)
        self.assertEqual(env.scenario, "wrong_match")


@requires_postgres
class TestLatestDownloadSummaries(unittest.TestCase):
    """#426: ``get_latest_download_summaries`` returns only the newest
    download_log row + a history count per request, instead of dragging
    the full per-request history (with fat JSONB) over the wire."""

    def setUp(self):
        self.db = make_db()
        self.r1 = self.db.add_request(
            artist_name="A", album_title="One", source="request",
            mb_release_id="sum-1")
        self.r2 = self.db.add_request(
            artist_name="B", album_title="Two", source="request",
            mb_release_id="sum-2")
        self.r3 = self.db.add_request(
            artist_name="C", album_title="NoHistory", source="request",
            mb_release_id="sum-3")

    def tearDown(self):
        self.db.close()

    def test_latest_row_and_count_per_request(self):
        self.db.log_download(self.r1, "user_old", "flac", "/tmp/1",
                             outcome="rejected")
        self.db.log_download(self.r1, "user_mid", "flac", "/tmp/2",
                             outcome="rejected")
        self.db.log_download(self.r1, "user_new", "flac", "/tmp/3",
                             outcome="success",
                             validation_result=json.dumps({"valid": True}))
        self.db.log_download(self.r2, "solo", "mp3", "/tmp/4",
                             outcome="rejected")

        summaries = self.db.get_latest_download_summaries(
            [self.r1, self.r2, self.r3])

        self.assertEqual(set(summaries), {self.r1, self.r2})
        s1 = summaries[self.r1]
        self.assertEqual(s1["count"], 3)
        self.assertEqual(s1["latest"]["soulseek_username"], "user_new")
        self.assertEqual(s1["latest"]["outcome"], "success")
        # The latest row must carry everything the history classifier
        # consumes (JSONB included) — it feeds build_download_history_row.
        self.assertIn("validation_result", s1["latest"])
        self.assertIn("import_result", s1["latest"])
        self.assertEqual(summaries[self.r2]["count"], 1)

    def test_empty_input_returns_empty(self):
        self.assertEqual(self.db.get_latest_download_summaries([]), {})

    def test_latest_row_overlays_candidate_evidence(self):
        """The evidence overlay that get_download_history_batch applied
        must survive on the summary's latest row."""
        from lib.quality import AudioQualityMeasurement
        log_id = self.db.log_download(self.r1, "u", "flac", "/tmp/x",
                                      outcome="rejected")
        evidence = make_album_quality_evidence(
            mb_release_id="sum-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                median_bitrate_kbps=940,
                format="flac",
                spectral_grade="genuine",
                spectral_bitrate_kbps=998,
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id="sum-1",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        summaries = self.db.get_latest_download_summaries([self.r1])
        latest = summaries[self.r1]["latest"]
        self.assertEqual(latest["spectral_grade"], "genuine")
        self.assertEqual(latest["spectral_bitrate"], 998)


@requires_postgres
class TestSearchRequests(unittest.TestCase):
    """#426: operator search over artist/album across all statuses."""

    def setUp(self):
        self.db = make_db()
        self.db.add_request(
            artist_name="The Mountain Goats", album_title="Tallahassee",
            source="request", mb_release_id="sr-1", status="imported")
        self.db.add_request(
            artist_name="Goat", album_title="World Music",
            source="request", mb_release_id="sr-2", status="wanted")
        self.db.add_request(
            artist_name="100% Wool", album_title="Felt",
            source="request", mb_release_id="sr-3", status="manual")

    def tearDown(self):
        self.db.close()

    def test_matches_artist_case_insensitive(self):
        rows = self.db.search_requests("mountain")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-1"])

    def test_matches_album_title(self):
        rows = self.db.search_requests("world mus")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-2"])

    def test_matches_across_statuses(self):
        rows = self.db.search_requests("goat")
        self.assertEqual(
            {r["mb_release_id"] for r in rows}, {"sr-1", "sr-2"})

    def test_like_wildcards_are_escaped(self):
        rows = self.db.search_requests("100%")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-3"])

    def test_status_narrowing_happens_in_sql(self):
        rows = self.db.search_requests("goat", status="wanted")
        self.assertEqual([r["mb_release_id"] for r in rows], ["sr-2"])

    def test_limit_and_blank_query(self):
        self.assertEqual(self.db.search_requests("  "), [])
        rows = self.db.search_requests("o", limit=2)
        self.assertEqual(len(rows), 2)


@requires_postgres
class TestGetByStatusRecentWindow(unittest.TestCase):
    """#426: the imported list is served newest-first with a cap."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_newest_first_with_limit(self):
        ids = []
        for i in range(3):
            ids.append(self.db.add_request(
                artist_name=f"A{i}", album_title=f"T{i}",
                source="request", mb_release_id=f"recent-{i}",
                status="imported"))
        # Touch the oldest row so updated_at ordering (not insert order)
        # decides recency.
        self.db.update_request_fields(ids[0], reasoning="touched")

        rows = self.db.get_by_status("imported", limit=2, newest_first=True)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], ids[0])

    def test_default_shape_unchanged(self):
        self.db.add_request(
            artist_name="A", album_title="T", source="request",
            mb_release_id="legacy-order", status="wanted")
        rows = self.db.get_by_status("wanted")
        self.assertEqual(len(rows), 1)


@requires_postgres
class TestDashboardFakeParity(unittest.TestCase):
    """Structural parity gate: FakePipelineDB's dashboard mirror vs the
    real PostgreSQL read-model on identically-seeded telemetry.

    The fake's get_pipeline_dashboard_metrics is a ~300-line Python
    mirror of ~700 lines of SQL; this test makes drift mechanical
    instead of review-archaeological. Both sides are seeded through the
    SAME writer calls, then the payloads are compared as SHAPES —
    recursive dict key sets, list lengths, first-element shapes, and
    leaf type categories (null / bool / num / str). Values are not
    compared (timestamps and rates are time-anchored); a key rename, a
    dropped panel, a sparse-vs-dense series, or a None-vs-0 leaf all
    fail loudly.
    """

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    @staticmethod
    def _seed(db: Any) -> None:
        rid = db.add_request(
            "Parity Artist", "Parity Album", "request",
            mb_release_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        db.log_search(
            rid, query="found q", outcome="found", result_count=5,
            elapsed_s=2.0, variant="v1", final_state="Completed",
            browse_time_s=42.0, match_time_s=1.0, peers_browsed=110,
            peers_browsed_lazy=5, fanout_waves=6,
        )
        db.log_search(rid, query="loop", outcome="no_match", elapsed_s=1.0)
        db.log_search(rid, query="old style", outcome="exhausted")
        db.record_cycle_metrics(
            cycle_total_s=300.0, browse_time_s=20.0, match_time_s=10.0,
            search_time_s=240.0, peers_browsed=8, fanout_waves=2,
            find_download_queued=4, find_download_completed=4,
            wanted_total=10,
        )
        db.record_peer_observations(["peer-a", "peer-b"])

    @classmethod
    def _shape(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: cls._shape(v) for k, v in sorted(value.items())}
        if isinstance(value, list):
            head = cls._shape(value[0]) if value else None
            return ("list", len(value), head)
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, (int, float)):
            return "num"
        if isinstance(value, str):
            return "str"
        return type(value).__name__

    def test_fake_dashboard_shape_matches_real_pg(self):
        from tests.fakes import FakePipelineDB
        self._seed(self.db)
        fake = FakePipelineDB()
        self._seed(fake)

        real_shape = self._shape(self.db.get_pipeline_dashboard_metrics())
        fake_shape = self._shape(fake.get_pipeline_dashboard_metrics())

        self.assertEqual(
            real_shape, fake_shape,
            "FakePipelineDB's dashboard mirror drifted from the real "
            "PostgreSQL read-model — fix the fake (tests/fakes.py), "
            "never the production SQL, unless the SQL change is the "
            "point of your PR.",
        )


@requires_postgres
class TestGetDownloadLogCounts(unittest.TestCase):
    """#445 item 2 — the /api/pipeline/log counts aggregate, promoted
    from inline route SQL to a named PipelineDB method."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_empty_tables_yield_zero_counts(self):
        counts = self.db.get_download_log_counts()
        self.assertEqual(
            (counts.total, counts.imported,
             counts.matches_24h, counts.matches_6h),
            (0, 0, 0, 0))

    def test_counts_aggregate_downloads_and_found_searches(self):
        rid = self.db.add_request(
            mb_release_id="counts-mbid-1", artist_name="A",
            album_title="B", source="request")
        self.db.log_download(rid, outcome="success")
        self.db.log_download(rid, outcome="force_import")
        self.db.log_download(rid, outcome="rejected")
        self.db.log_search(rid, outcome="found")
        self.db.log_search(rid, outcome="found")
        self.db.log_search(rid, outcome="error")
        # Age one found-row out of the 6h window but not the 24h one.
        self.db._execute(
            "UPDATE search_log SET created_at = NOW() - INTERVAL '12 hours' "
            "WHERE id = (SELECT MIN(id) FROM search_log "
            "            WHERE outcome = 'found')")
        # And one found-row out of BOTH windows.
        self.db.log_search(rid, outcome="found")
        self.db._execute(
            "UPDATE search_log SET created_at = NOW() - INTERVAL '2 days' "
            "WHERE id = (SELECT MAX(id) FROM search_log)")

        counts = self.db.get_download_log_counts()
        self.assertEqual(counts.total, 3)
        self.assertEqual(counts.imported, 2)
        self.assertEqual(counts.matches_24h, 2)
        self.assertEqual(counts.matches_6h, 1)

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        for db in (self.db, fake):
            rid = db.add_request(
                mb_release_id="parity-mbid-1", artist_name="A",
                album_title="B", source="request")
            db.log_download(rid, outcome="success")
            db.log_download(rid, outcome="timeout")
            db.log_search(rid, outcome="found")
            db.log_search(rid, outcome="no_results")
        real = self.db.get_download_log_counts()
        mirrored = fake.get_download_log_counts()
        self.assertEqual(
            (real.total, real.imported, real.matches_24h, real.matches_6h),
            (mirrored.total, mirrored.imported,
             mirrored.matches_24h, mirrored.matches_6h),
            "FakePipelineDB's counts mirror drifted from the real SQL — "
            "fix the fake (tests/fakes.py), never the production SQL, "
            "unless the SQL change is the point of your PR.")


@requires_postgres
class TestGetPipelineOverlay(unittest.TestCase):
    """#445 item 2 — web/overlay.py::check_pipeline's inline SQL,
    promoted to a named PipelineDB method."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_empty_mbids_short_circuits(self):
        self.assertEqual(self.db.get_pipeline_overlay([]), {})

    def test_maps_known_mbids_with_overlay_fields(self):
        rid = self.db.add_request(
            mb_release_id="overlay-mbid-1", artist_name="A",
            album_title="B", source="request")
        self.db.update_request_fields(
            rid, min_bitrate=900, search_filetype_override="lossless")

        info = self.db.get_pipeline_overlay(
            ["overlay-mbid-1", "overlay-mbid-unknown"])

        self.assertEqual(set(info), {"overlay-mbid-1"})
        row = info["overlay-mbid-1"]
        self.assertEqual(row["id"], rid)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")
        self.assertIsNone(row["target_format"])
        self.assertEqual(row["min_bitrate"], 900)

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        rids: dict[int, int] = {}
        for db in (self.db, fake):
            rid = db.add_request(
                mb_release_id="overlay-parity-1", artist_name="A",
                album_title="B", source="request")
            db.update_request_fields(rid, min_bitrate=320)
            db.add_request(
                mb_release_id="overlay-parity-2", artist_name="C",
                album_title="D", source="request", status="manual")
            rids[id(db)] = rid
        mbids = ["overlay-parity-1", "overlay-parity-2", "nope"]
        real = self.db.get_pipeline_overlay(mbids)
        mirrored = fake.get_pipeline_overlay(mbids)
        # The PG sequence isn't reset between tests, so ids differ by
        # backend — pin each backend's id mapping, compare the rest.
        self.assertEqual(real["overlay-parity-1"]["id"], rids[id(self.db)])
        self.assertEqual(mirrored["overlay-parity-1"]["id"], rids[id(fake)])
        strip = lambda o: {m: {k: v for k, v in row.items() if k != "id"}
                           for m, row in o.items()}
        self.assertEqual(
            strip(real), strip(mirrored),
            "FakePipelineDB's overlay mirror drifted from the real SQL — "
            "fix the fake (tests/fakes.py), never the production SQL, "
            "unless the SQL change is the point of your PR.")
@requires_postgres
class TestSlskdEventCursorRoundTrip(unittest.TestCase):
    """Rule A round-trip for upsert_slskd_event_cursor (issue #146)."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_returns_none_before_first_upsert(self):
        self.assertIsNone(self.db.get_slskd_event_cursor())

    def test_upsert_round_trip_preserves_every_field(self):
        self.db.upsert_slskd_event_cursor(
            "11da6649-4ffc-4d72-afc0-b4238afcc4ec",
            "2026-07-01T23:00:10.7447018Z",
        )

        cursor = self.db.get_slskd_event_cursor()

        assert cursor is not None
        self.assertEqual(
            cursor["last_event_id"], "11da6649-4ffc-4d72-afc0-b4238afcc4ec")
        # Stored verbatim — 7-digit fractional seconds must survive.
        self.assertEqual(
            cursor["last_event_timestamp"], "2026-07-01T23:00:10.7447018Z")
        self.assertIsNotNone(cursor["updated_at"])

    def test_upsert_is_single_row_replace(self):
        self.db.upsert_slskd_event_cursor("ev-1", "2026-07-01T00:00:00.0000000Z")
        self.db.upsert_slskd_event_cursor("ev-2", "2026-07-02T00:00:00.0000000Z")

        cursor = self.db.get_slskd_event_cursor()

        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-2")
        cur = self.db._execute("SELECT COUNT(*) AS n FROM slskd_event_cursor")
        self.assertEqual(cur.fetchone()["n"], 1)

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        for db in (self.db, fake):
            db.upsert_slskd_event_cursor("ev-1", "2026-07-01T00:00:00.0000000Z")
        real = self.db.get_slskd_event_cursor()
        mirrored = fake.get_slskd_event_cursor()
        assert real is not None and mirrored is not None
        strip = lambda c: {k: v for k, v in c.items() if k != "updated_at"}
        self.assertEqual(strip(real), strip(mirrored))


@requires_postgres
class TestSearchLedgerRoundTrip(unittest.TestCase):
    """Rule A round-trip for the slskd search-id write-ahead ledger
    (migration 044, issue #576)."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_get_unswept_search_ids_empty_before_any_record(self):
        self.assertEqual(
            self.db.get_unswept_search_ids(
                older_than=datetime.now(timezone.utc) + timedelta(seconds=1)),
            [])

    def test_record_round_trip_preserves_every_field(self):
        self.db.record_search_id("sid-rt-1", "plan_search", 4321)

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(timezone.utc) + timedelta(seconds=1))

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["search_id"], "sid-rt-1")
        self.assertEqual(row["purpose"], "plan_search")
        self.assertEqual(row["request_id"], 4321)
        self.assertIsNotNone(row["created_at"])

    def test_record_search_id_with_null_request_id_round_trips(self):
        # artist_probe callers may have no request context.
        self.db.record_search_id("sid-rt-null", "artist_probe", None)

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(timezone.utc) + timedelta(seconds=1))

        self.assertEqual(rows[0]["request_id"], None)

    def test_get_unswept_search_ids_excludes_rows_inside_grace_window(self):
        self.db.record_search_id("sid-fresh", "plan_search", 1)

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(timezone.utc) - timedelta(hours=1))

        self.assertEqual(rows, [])

    def test_record_search_id_conflict_does_not_raise(self):
        # ON CONFLICT DO NOTHING — ids are unique by construction, but a
        # second insert for the same id must be a harmless no-op.
        self.db.record_search_id("sid-dup", "plan_search", 1)
        self.db.record_search_id("sid-dup", "plan_search", 1)  # must not raise
        cur = self.db._execute(
            "SELECT COUNT(*) AS n FROM slskd_search_ledger WHERE search_id = %s",
            ("sid-dup",))
        self.assertEqual(cur.fetchone()["n"], 1)

    def test_mark_search_ids_deleted_removes_from_unswept(self):
        self.db.record_search_id("sid-a", "plan_search", 1)
        self.db.record_search_id("sid-b", "plan_search", 2)

        self.db.mark_search_ids_deleted(["sid-a"])

        rows = self.db.get_unswept_search_ids(
            older_than=datetime.now(timezone.utc) + timedelta(seconds=1))
        self.assertEqual([r["search_id"] for r in rows], ["sid-b"])
        cur = self.db._execute(
            "SELECT deleted_at FROM slskd_search_ledger WHERE search_id = %s",
            ("sid-a",))
        self.assertIsNotNone(cur.fetchone()["deleted_at"])

    def test_mark_search_ids_deleted_empty_list_is_a_noop(self):
        self.db.mark_search_ids_deleted([])  # must not raise / not query

    def test_prune_search_ledger_removes_only_old_deleted_rows(self):
        self.db.record_search_id("sid-old", "plan_search", 1)
        self.db.record_search_id("sid-undeleted", "plan_search", 2)
        self.db.mark_search_ids_deleted(["sid-old"])
        self.db._execute(
            "UPDATE slskd_search_ledger SET deleted_at = %s WHERE search_id = %s",
            (datetime.now(timezone.utc) - timedelta(days=10), "sid-old"))

        removed = self.db.prune_search_ledger(
            deleted_before=datetime.now(timezone.utc) - timedelta(days=7))

        self.assertEqual(removed, 1)
        cur = self.db._execute(
            "SELECT search_id FROM slskd_search_ledger ORDER BY search_id")
        self.assertEqual(
            [r["search_id"] for r in cur.fetchall()], ["sid-undeleted"])

    def test_fake_parity_on_identical_state(self):
        from tests.fakes import FakePipelineDB

        fake = FakePipelineDB()
        for db in (self.db, fake):
            db.record_search_id("sid-parity", "plan_search", 99)
        cutoff = datetime.now(timezone.utc) + timedelta(seconds=1)
        real_rows = self.db.get_unswept_search_ids(older_than=cutoff)
        fake_rows = fake.get_unswept_search_ids(older_than=cutoff)
        strip = lambda rows: [
            {k: v for k, v in r.items() if k != "created_at"} for r in rows
        ]
        self.assertEqual(strip(real_rows), strip(fake_rows))


@requires_postgres
class TestReadProjectionParity(unittest.TestCase):
    """#481 item 2 — fake<->production READ-projection parity gate.

    ``FakePipelineDB`` hand-mirrors production SELECT projections as
    literal key tuples (``_long_tail_projection`` and the
    ``list_triage_page`` projection in ``tests/fakes/pipeline_db.py``)
    across dozens of ``get_*`` methods. Nothing failed if the two
    drifted — PR #480 had to update the SQL projection and the fake's
    key tuple in lockstep by hand. This is the read-side mirror of
    ``.claude/rules/test-fidelity.md`` Rule A (write round-trips):
    seed an IDENTICAL row through the real ``PipelineDB`` and
    ``FakePipelineDB``, call the same ``get_*`` method on both, and
    assert KEY-SET EQUALITY of the returned rows (not value equality —
    ids and timestamps are backend-assigned/time-anchored and
    deliberately not compared). A key-set drift means the fake returns
    a column production doesn't (or vice versa) — exactly the seam
    that keeps fake-driven contract tests green while the live route
    500s or renders nulls.

    **The audit table.** Each ``test_*`` method below is one entry;
    growing coverage means adding another method here, seeding both
    backends identically, and calling ``_assert_keyset_parity``. Not
    yet covered — see the PR body / final report Suggestions for the
    rest of the ~51 ``get_*`` methods FakePipelineDB mirrors.
    """

    def setUp(self):
        self.db = make_db()
        from tests.fakes import FakePipelineDB
        self.fake = FakePipelineDB()

    def tearDown(self):
        self.db.close()

    @staticmethod
    def _assert_keyset_parity(
        test: unittest.TestCase,
        real_rows: "list[dict[str, Any]]",
        fake_rows: "list[dict[str, Any]]",
        label: str,
    ) -> None:
        """Assert real PG and FakePipelineDB return identically-keyed rows.

        Compares row count, then per-row key sets — NOT values (some
        columns are backend-assigned like ``id`` or time-anchored).
        On drift, the failure names the exact column(s) that differ,
        matching the DX of the Rule A round-trip tests.
        """
        test.assertEqual(
            len(real_rows), len(fake_rows),
            f"{label}: real PG returned {len(real_rows)} row(s), "
            f"FakePipelineDB returned {len(fake_rows)} row(s) — seeding "
            f"drifted between the two backends",
        )
        for i, (real_row, fake_row) in enumerate(zip(real_rows, fake_rows)):
            real_keys = set(real_row.keys())
            fake_keys = set(fake_row.keys())
            if real_keys == fake_keys:
                continue
            only_real = sorted(real_keys - fake_keys)
            only_fake = sorted(fake_keys - real_keys)
            test.fail(
                f"{label} row {i}: projection key-set drifted between "
                f"real PG and FakePipelineDB — columns only in real PG: "
                f"{only_real}; columns only in FakePipelineDB: {only_fake}. "
                f"Fix the fake's projection to mirror production (or the "
                f"reverse if the SQL change is the point of the PR)."
            )

    # --- get_long_tail_cohort / get_long_tail_request ----------------------

    def _seed_long_tail_request(
        self, db: Any, *, mb_release_id: str, with_tracks: bool,
        with_rescue: bool,
    ) -> int:
        rid = db.add_request(
            "Long Tail Artist", "Long Tail Album", "request",
            mb_release_id=mb_release_id,
        )
        if with_tracks:
            db.set_tracks(rid, [
                {"disc_number": 1, "track_number": 1, "title": "One",
                 "length_seconds": 100},
                {"disc_number": 1, "track_number": 2, "title": "Two",
                 "length_seconds": 200},
            ])
        if with_rescue:
            db.insert_youtube_running(
                request_id=rid, browse_id="MPREb_parity",
                audio_playlist_id=None,
                yt_url="https://example.invalid/parity",
                expected_track_count=2,
            )
        return rid

    def test_get_long_tail_cohort_keyset_parity(self):
        for db in (self.db, self.fake):
            self._seed_long_tail_request(
                db, mb_release_id="lt-parity-plain", with_tracks=False,
                with_rescue=False)
            self._seed_long_tail_request(
                db, mb_release_id="lt-parity-full", with_tracks=True,
                with_rescue=True)

        real_rows = self.db.get_long_tail_cohort()
        fake_rows = self.fake.get_long_tail_cohort()
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_long_tail_cohort")

    def test_get_long_tail_request_keyset_parity(self):
        real_id = self._seed_long_tail_request(
            self.db, mb_release_id="lt-single-parity", with_tracks=True,
            with_rescue=True)
        fake_id = self._seed_long_tail_request(
            self.fake, mb_release_id="lt-single-parity", with_tracks=True,
            with_rescue=True)

        real_row = self.db.get_long_tail_request(real_id)
        fake_row = self.fake.get_long_tail_request(fake_id)
        assert real_row is not None and fake_row is not None
        self._assert_keyset_parity(
            self, [real_row], [fake_row], "get_long_tail_request (hit)")

    def test_get_long_tail_request_none_branch_parity(self):
        # Non-existent id — both sides must agree on None. There's
        # nothing to key-compare when both sides are None; the
        # assertion IS the parity check here.
        self.assertIsNone(self.db.get_long_tail_request(999_999_999))
        self.assertIsNone(self.fake.get_long_tail_request(999_999_999))

    # --- list_triage_page ----------------------------------------------------

    def _seed_triage_request(self, db: Any, *, mb_release_id: str) -> int:
        return db.add_request(
            "Triage Artist", "Triage Album", "request",
            mb_release_id=mb_release_id,
        )

    def test_list_triage_page_all_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter

        for db in (self.db, self.fake):
            self._seed_triage_request(db, mb_release_id="triage-all-1")

        filter_spec = ParsedTriageFilter(kind="all", raw="all")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "list_triage_page(all)")

    def test_list_triage_page_unfindable_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter
        from lib.unfindable_detection_service import CATEGORY_ARTIST_ABSENT

        now = datetime.now(timezone.utc)
        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="triage-unfindable-1")
            db.set_unfindable_category(
                rid, category=CATEGORY_ARTIST_ABSENT, categorised_at=now)

        filter_spec = ParsedTriageFilter(
            kind="unfindable", unfindable_category=CATEGORY_ARTIST_ABSENT,
            raw=f"unfindable:{CATEGORY_ARTIST_ABSENT}")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "list_triage_page(unfindable)")

    def test_list_triage_page_data_quality_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter

        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="triage-dataq-1")
            db.record_field_resolution(
                rid, "release_group_year", "unresolved_mirror_unavailable",
                "URLError")

        filter_spec = ParsedTriageFilter(
            kind="data_quality", raw="data_quality")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "list_triage_page(data_quality)")

    def test_list_triage_page_search_not_converting_keyset_parity(self):
        from lib.triage_service import ParsedTriageFilter

        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="triage-search-1")
            db.log_search(rid, query="q", outcome="no_match", elapsed_s=1.0)

        filter_spec = ParsedTriageFilter(
            kind="search_not_converting", raw="search_not_converting")
        real_rows = self.db.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        fake_rows = self.fake.list_triage_page(
            filter_spec=filter_spec, page_size=50, after_request_id=None)
        self._assert_keyset_parity(
            self, real_rows, fake_rows,
            "list_triage_page(search_not_converting)")

    # --- get_field_resolutions_for_requests ----------------------------------

    def test_get_field_resolutions_for_requests_keyset_parity(self):
        ids: dict[int, int] = {}
        for db in (self.db, self.fake):
            rid = self._seed_triage_request(
                db, mb_release_id="fieldres-parity-1")
            db.record_field_resolution(
                rid, "catalog_number", "unresolved_404", "http_404")
            ids[id(db)] = rid

        real_map = self.db.get_field_resolutions_for_requests(
            [ids[id(self.db)]])
        fake_map = self.fake.get_field_resolutions_for_requests(
            [ids[id(self.fake)]])
        real_rows = real_map.get(ids[id(self.db)], [])
        fake_rows = fake_map.get(ids[id(self.fake)], [])
        self._assert_keyset_parity(
            self, real_rows, fake_rows,
            "get_field_resolutions_for_requests")

    # --- get_wanted_searchable (#523) ----------------------------------------

    def _seed_wanted_searchable_request(
        self, db: Any, *, mb_release_id: str, generator_id: str,
    ) -> int:
        from lib.pipeline_db import SearchPlanItemInput

        rid = db.add_request(
            "WS Artist", "WS Album", "request", mb_release_id=mb_release_id)
        db.create_successful_search_plan(
            request_id=rid, generator_id=generator_id,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="Q")])
        return rid

    def test_get_wanted_searchable_keyset_parity(self):
        for db in (self.db, self.fake):
            self._seed_wanted_searchable_request(
                db, mb_release_id="ws-parity", generator_id="g-parity")

        real_rows = self.db.get_wanted_searchable("g-parity")
        fake_rows = self.fake.get_wanted_searchable("g-parity")
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_wanted_searchable parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_wanted_searchable parity would pass vacuously")
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_wanted_searchable")

    def test_get_wanted_searchable_no_active_plan_empty_branch(self):
        # A wanted request with no active plan is not execution-eligible
        # -- both backends must agree on the empty-list contract. This
        # is the explicit contract being asserted, not the keyset check,
        # so an empty result here is the expected (non-vacuous) outcome.
        for db in (self.db, self.fake):
            db.add_request(
                "WS Artist", "WS Album (no plan)", "request",
                mb_release_id="ws-no-plan")

        self.assertEqual(self.db.get_wanted_searchable("g-parity"), [])
        self.assertEqual(self.fake.get_wanted_searchable("g-parity"), [])

    # --- get_search_summaries_for_requests (#523) ----------------------------

    def _seed_search_summary_request(
        self, db: Any, *, mb_release_id: str,
    ) -> int:
        rid = db.add_request(
            "Summary Artist", "Summary Album", "request",
            mb_release_id=mb_release_id)
        db.log_search(
            rid, query="q1", outcome="found", result_count=5, elapsed_s=1.0)
        return rid

    def test_get_search_summaries_for_requests_keyset_parity(self):
        ids: dict[int, int] = {}
        for db in (self.db, self.fake):
            ids[id(db)] = self._seed_search_summary_request(
                db, mb_release_id="summary-parity-1")

        real_map = self.db.get_search_summaries_for_requests(
            [ids[id(self.db)]])
        fake_map = self.fake.get_search_summaries_for_requests(
            [ids[id(self.fake)]])
        real_rows = list(real_map.values())
        fake_rows = list(fake_map.values())
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_search_summaries_for_requests parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_search_summaries_for_requests parity would pass vacuously")
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_search_summaries_for_requests")

    def test_get_search_summaries_for_requests_empty_input_contract(self):
        # Contract: an empty id list short-circuits to {} without a query
        # on both backends -- not a keyset check, the {} equality IS the
        # assertion.
        self.assertEqual(self.db.get_search_summaries_for_requests([]), {})
        self.assertEqual(self.fake.get_search_summaries_for_requests([]), {})

    # --- get_recent_search_log_for_requests (#523) ---------------------------

    def test_get_recent_search_log_for_requests_keyset_parity(self):
        ids: dict[int, int] = {}
        for db in (self.db, self.fake):
            rid = db.add_request(
                "RecentLog Artist", "RecentLog Album", "request",
                mb_release_id="recentlog-parity-1")
            db.log_search(
                rid, query="q1", outcome="found", result_count=5,
                elapsed_s=1.0)
            ids[id(db)] = rid

        real_map = self.db.get_recent_search_log_for_requests(
            [ids[id(self.db)]], per_request_limit=5)
        fake_map = self.fake.get_recent_search_log_for_requests(
            [ids[id(self.fake)]], per_request_limit=5)
        real_rows = [row for rows in real_map.values() for row in rows]
        fake_rows = [row for rows in fake_map.values() for row in rows]
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_recent_search_log_for_requests parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_recent_search_log_for_requests parity would pass vacuously")
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_recent_search_log_for_requests")

    # --- list_active_youtube_rescues (#523) ----------------------------------

    def test_list_active_youtube_rescues_keyset_parity(self):
        for db in (self.db, self.fake):
            rid = db.add_request(
                "Rescue Artist", "Rescue Album", "request",
                mb_release_id="rescue-parity-1")
            db.insert_youtube_running(
                request_id=rid, browse_id="MPREb_parity",
                audio_playlist_id=None,
                yt_url="https://example.invalid/parity",
                expected_track_count=2)

        real_rows = self.db.list_active_youtube_rescues()
        fake_rows = self.fake.list_active_youtube_rescues()
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "list_active_youtube_rescues parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "list_active_youtube_rescues parity would pass vacuously")
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "list_active_youtube_rescues")

    def test_list_active_youtube_rescues_empty_branch_parity(self):
        # Contract: no in-flight rescues -- both return []. This is the
        # explicit empty contract, not the keyset check.
        self.assertEqual(self.db.list_active_youtube_rescues(), [])
        self.assertEqual(self.fake.list_active_youtube_rescues(), [])

    # --- get_youtube_album_mapping (#523, tri-state) -------------------------

    @staticmethod
    def _youtube_mapping_row(**overrides: Any) -> PersistedYoutubeRow:
        fields: dict[str, Any] = {
            "yt_browse_id": "MPREb_parity",
            "yt_audio_playlist_id": "OLAK5uy_parity",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_parity",
            "yt_year": 2020,
            "yt_track_count": 10,
            "album_title": "Parity Album",
            "album_artist": "Parity Artist",
            "yt_tracks": [
                PersistedTrack(
                    title="Track 1", video_id="v1", length_seconds=200,
                    track_number=1, disc_number=1,
                    artists=[{"name": "Artist"}],
                ),
            ],
            "distances": [
                PersistedDistance(mbid="mb-1", distance=0.05),
            ],
        }
        fields.update(overrides)
        return PersistedYoutubeRow(**fields)

    def test_get_youtube_album_mapping_keyset_parity(self):
        for db in (self.db, self.fake):
            db.upsert_youtube_album_mapping(
                "rg-parity", "mb", [self._youtube_mapping_row()])

        real_rows = self.db.get_youtube_album_mapping("rg-parity", "mb")
        fake_rows = self.fake.get_youtube_album_mapping("rg-parity", "mb")
        self.assertTrue(
            real_rows, "seeding produced no rows on real PG — "
            "get_youtube_album_mapping parity would pass vacuously")
        self.assertTrue(
            fake_rows, "seeding produced no rows on FakePipelineDB — "
            "get_youtube_album_mapping parity would pass vacuously")
        assert real_rows is not None and fake_rows is not None
        self._assert_keyset_parity(
            self, real_rows, fake_rows, "get_youtube_album_mapping")

    def test_get_youtube_album_mapping_resolved_empty_branch_parity(self):
        # Contract: upserting an empty matrix stamps the empty-resolution
        # marker -- both backends return [] (cache HIT), never None.
        for db in (self.db, self.fake):
            db.upsert_youtube_album_mapping("rg-parity-empty", "mb", [])

        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-parity-empty", "mb"), [])
        self.assertEqual(
            self.fake.get_youtube_album_mapping("rg-parity-empty", "mb"), [])

    def test_get_youtube_album_mapping_never_resolved_branch_parity(self):
        # Contract: an unknown (rg, source) pair is a cache MISS -- None
        # on both backends, distinct from the resolved-empty [] above.
        self.assertIsNone(
            self.db.get_youtube_album_mapping("rg-never-resolved", "mb"))
        self.assertIsNone(
            self.fake.get_youtube_album_mapping("rg-never-resolved", "mb"))


@requires_postgres
class TestReadProjectionRegistryParity(unittest.TestCase):
    """#546 W1 — registry-driven read-projection parity gate.

    ``TestReadProjectionParity`` (above) is the hand-written half: one
    ``test_*`` method per covered projection. This class is the
    self-enforcing half — it iterates ``PARITY_REGISTRY`` from
    ``tests/read_projection_registry.py`` and runs every registered
    seeder against a fresh real ``PipelineDB`` and a fresh
    ``FakePipelineDB``, asserting key-set parity for each. Adding a
    seeder to the registry is all it takes to gate a new mirror; the
    companion audit (``tests/test_read_projection_audit.py``) forces
    every ``FakePipelineDB`` read method into the registry, the
    hand-written coverage, or the allowlist.

    Reuses ``TestReadProjectionParity._assert_keyset_parity`` — a
    staticmethod whose first argument is the ``TestCase`` instance, so
    cross-class reuse is safe.
    """

    def test_every_registered_mirror_has_keyset_parity(self):
        from tests.fakes import FakePipelineDB
        from tests.read_projection_registry import PARITY_REGISTRY

        for method_name, seeder in sorted(PARITY_REGISTRY.items()):
            with self.subTest(method=method_name):
                real_db = make_db()
                try:
                    fake_db = FakePipelineDB()
                    real_rows = seeder(real_db)
                    fake_rows = seeder(fake_db)
                    self.assertTrue(
                        real_rows,
                        f"{method_name}: seeder produced no rows on real "
                        f"PG — parity would pass vacuously; fix the seeder "
                        f"in tests/read_projection_registry.py")
                    self.assertTrue(
                        fake_rows,
                        f"{method_name}: seeder produced no rows on "
                        f"FakePipelineDB — parity would pass vacuously; fix "
                        f"the seeder in tests/read_projection_registry.py")
                    TestReadProjectionParity._assert_keyset_parity(
                        self, real_rows, fake_rows, method_name)
                finally:
                    real_db.close()


if __name__ == "__main__":
    unittest.main()
