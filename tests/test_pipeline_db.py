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
from unittest.mock import patch

# Bootstrap ephemeral PostgreSQL if available
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

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
    for table in ["import_jobs", "user_cooldowns", "source_denylist", "search_log", "download_log", "album_tracks", "album_requests"]:
        db._execute(f"TRUNCATE {table} CASCADE")
    db.conn.commit()
    return db


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

    def test_claim_complete_and_fail_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:1",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
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

    def test_enqueue_defaults_to_importable_when_preview_env_absent(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        with patch.dict(os.environ, {}, clear=True):
            job = self.db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=self.req_id,
                dedupe_key="manual:preview-disabled",
                payload=manual_import_payload(failed_path="/tmp/manual"),
            )

        self.assertEqual(job.preview_status, "would_import")
        self.assertEqual(job.preview_message, "Preview gate disabled")
        self.assertIsNotNone(job.preview_completed_at)
        self.assertIsNotNone(job.importable_at)
        self.assertIsNone(
            self.db.claim_next_import_preview_job(worker_id="preview-worker")
        )

        claimed = self.db.claim_next_import_job(worker_id="test-worker")
        assert claimed is not None
        self.assertEqual(claimed.id, job.id)
        self.assertEqual(claimed.status, "running")

    def test_two_sessions_cannot_claim_same_job(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        from lib import pipeline_db

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:claim-once",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
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

    def test_stale_running_jobs_are_listed_and_failed_conservatively(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:stale",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
        )
        self.db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = self.db.claim_next_import_job(worker_id="stale-worker")
        assert claimed is not None
        old = datetime.now(timezone.utc) - timedelta(hours=8)
        self.db._execute(
            "UPDATE import_jobs SET heartbeat_at = %s, updated_at = %s WHERE id = %s",
            (old, old, claimed.id),
        )

        stale = self.db.list_stale_running_import_jobs(
            older_than=timedelta(hours=4),
        )
        self.assertEqual([job.id for job in stale], [claimed.id])

        failed = self.db.fail_stale_running_import_jobs(
            older_than=timedelta(hours=4),
            message="stale importer job",
        )
        self.assertEqual([job.id for job in failed], [claimed.id])
        self.assertEqual(failed[0].status, "failed")

    def test_running_jobs_can_be_requeued_immediately_after_worker_restart(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:restart-retry",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
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
            preview_enabled=True,
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
            preview_enabled=True,
        )
        importable = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:timeline-importable",
            payload=manual_import_payload(failed_path="/tmp/importable"),
            preview_enabled=True,
        )
        self.db.mark_import_job_preview_importable(
            importable.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )

        timeline = self.db.list_import_job_timeline(limit=10)

        self.assertEqual([job.id for job in timeline[:2]], [importable.id, waiting.id])
        self.assertEqual(timeline[0].preview_status, "would_import")

    def test_import_job_timeline_orders_recent_terminal_jobs_after_active(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        importable = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:timeline-active",
            payload=manual_import_payload(failed_path="/tmp/active"),
            preview_enabled=True,
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
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        new_time = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.db._execute(
            "UPDATE import_jobs SET updated_at = %s WHERE id = %s",
            (old_time, older.id),
        )
        self.db._execute(
            "UPDATE import_jobs SET updated_at = %s WHERE id = %s",
            (new_time, newer.id),
        )

        timeline = self.db.list_import_job_timeline(limit=10)

        self.assertEqual([job.id for job in timeline[:3]], [
            importable.id,
            newer.id,
            older.id,
        ])

    def test_preview_claim_and_importable_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        queued = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:preview",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
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
        self.assertEqual(marked.status, "queued")
        self.assertEqual(marked.preview_status, "would_import")
        self.assertEqual(marked.preview_result["verdict"], "would_import")
        self.assertEqual(marked.preview_message, "Preview would import")
        self.assertIsNotNone(marked.preview_completed_at)
        self.assertIsNotNone(marked.importable_at)

    def test_preview_rejection_fails_job_with_audit(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        queued = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:preview-reject",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
        )

        failed = self.db.mark_import_job_preview_failed(
            queued.id,
            preview_status="uncertain",
            error="path_missing",
            preview_result={"verdict": "uncertain", "reason": "path_missing"},
            message="Preview failed: path_missing",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.preview_status, "uncertain")
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
            preview_enabled=True,
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

    def test_batch_fetch(self):
        req2 = self.db.add_request(
            mb_release_id="search-log-uuid-2",
            artist_name="C",
            album_title="D",
            source="request",
        )
        self.db.log_search(self.req_id, query="q1", outcome="found")
        self.db.log_search(req2, query="q2", outcome="timeout")
        batch = self.db.get_search_history_batch([self.req_id, req2])
        self.assertIn(self.req_id, batch)
        self.assertIn(req2, batch)
        self.assertEqual(len(batch[self.req_id]), 1)
        self.assertEqual(batch[req2][0]["outcome"], "timeout")

    def test_all_outcomes_valid(self):
        for outcome in ("found", "no_match", "no_results", "timeout", "error", "empty_query"):
            self.db.log_search(self.req_id, query="q", outcome=outcome)
        history = self.db.get_search_history(self.req_id)
        self.assertEqual(len(history), 6)


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

    def test_backoff_caps_at_six_hours(self):
        for _ in range(6):
            self.db.record_attempt(self.req_id, "search")

        req = self.db.get_request(self.req_id)
        assert req is not None
        retry_at = req["next_retry_after"]
        assert retry_at is not None

        delta = (retry_at - datetime.now(timezone.utc)).total_seconds()
        self.assertLessEqual(delta, 6 * 60 * 60 + 5)
        self.assertGreater(delta, 5 * 60 * 60)


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
        )

        self.db.clear_on_disk_quality_fields(req_id)

        req = self.db.get_request(req_id)
        assert req is not None
        self.assertFalse(req["verified_lossless"])
        self.assertIsNone(req["current_spectral_grade"])
        self.assertIsNone(req["current_spectral_bitrate"])

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

    def test_clear_download_state(self):
        """clear_download_state() nulls the JSONB column."""
        req_id = self.db.add_request(
            mb_release_id="cds-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "now", "files": []})
        self.db.set_downloading(req_id, state_json)
        self.db.clear_download_state(req_id)
        req = self.db.get_request(req_id)
        assert req is not None
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
        cooldowns = self.db.get_user_cooldowns()
        user1_rows = [c for c in cooldowns if c["username"] == "user1"]
        self.assertEqual(len(user1_rows), 1)
        # Should have the later date
        self.assertGreater(user1_rows[0]["cooldown_until"], until1)

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
        for outcome in ["timeout", "timeout", "success", "timeout", "success"]:
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

    def test_record_wrong_match_triage_preserves_audit_after_clear(self):
        log_id = self.db.log_download(
            request_id=self.req1,
            soulseek_username="alice",
            outcome="rejected",
            validation_result=json.dumps({
                "scenario": "high_distance",
                "failed_path": "/abs/Album",
            }),
        )

        recorded = self.db.record_wrong_match_triage(log_id, {
            "action": "deleted_reject",
            "success": True,
            "reason": "downgrade",
        })
        cleared = self.db.clear_wrong_match_paths(self.req1, ["/abs/Album"])

        self.assertTrue(recorded)
        self.assertEqual(cleared, 1)
        entry = self.db.get_download_log_entry(log_id)
        assert entry is not None
        raw_vr = entry["validation_result"]
        vr = json.loads(raw_vr) if isinstance(raw_vr, str) else raw_vr
        assert isinstance(vr, dict)
        self.assertNotIn("failed_path", vr)
        self.assertEqual(vr["wrong_match_triage"], {
            "action": "deleted_reject",
            "success": True,
            "reason": "downgrade",
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


if __name__ == "__main__":
    unittest.main()
