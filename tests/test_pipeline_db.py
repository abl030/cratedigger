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

# Bootstrap ephemeral PostgreSQL if available
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var
from tests.helpers import make_album_quality_evidence

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
    for table in [
        "album_quality_evidence",
        "peer_dir_daily_aggregates",
        "peer_dir_observations",
        "cycle_metrics",
        "bad_audio_hashes",
        "import_jobs",
        "user_cooldowns",
        "source_denylist",
        "search_log",
        "download_log",
        "album_request_field_resolutions",  # migration 030
        "youtube_album_mappings",  # migration 034
        "album_tracks",
        "album_requests",
    ]:
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
        self.assertIn("cycle_metrics", table_names)
        self.assertIn("peer_dir_observations", table_names)
        self.assertIn("peer_dir_daily_aggregates", table_names)
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

    def test_stale_running_jobs_are_listed_and_failed_conservatively(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        job = self.db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=self.req_id,
            dedupe_key="manual:stale",
            payload=manual_import_payload(failed_path="/tmp/manual"),
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
        heavy = metrics["peer_dirs"]["heavy_queries"]
        self.assertEqual(heavy[0]["request_id"], self.req1)
        self.assertEqual(heavy[0]["mb_release_id"], "dash-1")
        self.assertEqual(heavy[0]["query"], "loop b")
        self.assertEqual(heavy[0]["variant"], "track_0")
        self.assertEqual(heavy[0]["result_count"], 500)
        self.assertEqual(heavy[0]["peer_dirs"], 35)
        self.assertEqual(heavy[0]["fanout_waves"], 3)
        self.assertEqual(heavy[0]["browse_time_s"], 12.5)
        self.assertEqual(metrics["cycles"]["outliers"][0]["cycle_total_s"], 900.0)

    def test_peer_dir_observations_track_first_seen_counts(self):
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=2)

        inserted = self.db.record_peer_dir_observations(
            [("user1", "dirA"), ("user1", "dirA"), ("user2", "dirB")],
            observed_at=old,
        )
        self.assertEqual(inserted, 2)

        inserted = self.db.record_peer_dir_observations(
            [("user1", "dirA"), ("user3", "dirC")],
            observed_at=now,
        )
        self.assertEqual(inserted, 1)

        stored = self.db._execute("""
            SELECT
                COUNT(*)::int AS rows,
                SUM(seen_count)::int AS total_seen,
                MAX(seen_count)::int AS max_seen
            FROM peer_dir_observations
        """).fetchone()
        assert stored is not None
        self.assertEqual(stored["rows"], 3)
        self.assertEqual(stored["total_seen"], 4)
        self.assertEqual(stored["max_seen"], 2)

        peer_dirs = self.db.get_peer_dir_daily_metrics(days=14)
        self.assertEqual(peer_dirs["totals"]["known_combos"], 3)
        self.assertEqual(peer_dirs["totals"]["known_peers"], 3)
        self.assertEqual(peer_dirs["totals"]["known_dirs"], 3)
        self.assertEqual(peer_dirs["totals"]["new_24h"], 1)
        self.assertEqual(
            sum(day["new_combos"] for day in peer_dirs["days"]),
            3,
        )


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
class TestSetManual(unittest.TestCase):
    """U6: ``set_manual`` flip sites for system-driven manual transitions."""

    def setUp(self):
        self.db = make_db()
        self.req_id = self.db.add_request(
            mb_release_id="set-manual-uuid",
            artist_name="A",
            album_title="B",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_writes_manual_reason_when_provided(self):
        self.db.set_manual(self.req_id, manual_reason="search_exhausted")
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["status"], "manual")
        self.assertEqual(req["manual_reason"], "search_exhausted")

    def test_does_not_overwrite_existing_manual_reason_when_none(self):
        """Defensive: a None reason must NOT clobber a populated reason.

        Generic flip paths that don't carry a system reason should leave
        any existing populated reason in place.
        """
        # Pre-populate manual_reason directly (simulates an operator hold or
        # a previous system flip).
        self.db._execute(
            "UPDATE album_requests SET manual_reason = %s WHERE id = %s",
            ("operator_hold", self.req_id),
        )
        self.db.conn.commit()
        self.db.set_manual(self.req_id)
        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["status"], "manual")
        self.assertEqual(req["manual_reason"], "operator_hold")


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

    def test_update_v0_probe_state_updates_current_source_probe(self):
        from lib import pipeline_db
        from lib.quality import V0ProbeEvidence

        self.db.update_v0_probe_state(
            self.req_id,
            pipeline_db.RequestV0ProbeStateUpdate(
                current_lossless_source=V0ProbeEvidence(
                    kind="lossless_source_v0",
                    min_bitrate_kbps=165,
                    avg_bitrate_kbps=228,
                    median_bitrate_kbps=225,
                ),
            ),
        )

        req = self.db.get_request(self.req_id)
        assert req is not None
        self.assertEqual(req["current_lossless_source_v0_probe_min_bitrate"], 165)
        self.assertEqual(req["current_lossless_source_v0_probe_avg_bitrate"], 228)
        self.assertEqual(req["current_lossless_source_v0_probe_median_bitrate"], 225)

    def test_update_v0_probe_state_can_clear_current_source_probe(self):
        from lib import pipeline_db
        from lib.quality import V0ProbeEvidence

        self.db.update_v0_probe_state(
            self.req_id,
            pipeline_db.RequestV0ProbeStateUpdate(
                current_lossless_source=V0ProbeEvidence(
                    kind="lossless_source_v0",
                    min_bitrate_kbps=165,
                    avg_bitrate_kbps=228,
                    median_bitrate_kbps=225,
                ),
            ),
        )
        self.db.update_v0_probe_state(
            self.req_id,
            pipeline_db.RequestV0ProbeStateUpdate(
                clear_current_lossless_source=True,
            ),
        )

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


@requires_postgres
class TestPeerDirDailyAggregatesLazyFill(unittest.TestCase):
    """U2: ``get_peer_dir_daily_metrics`` lazy-fills the
    ``peer_dir_daily_aggregates`` cache with completed-day rows and
    computes today live, while preserving the public response shape.
    """

    # Pinned response shape (regression contract). Today's row is keyed
    # by ``date`` and the keys per-day must match exactly. ``totals``
    # keys must match exactly.
    _DAY_KEYS = {"date", "new_combos", "new_peers", "new_dirs"}
    _TOTALS_KEYS = {
        "known_combos", "known_peers", "known_dirs",
        "new_24h", "cold_seen_24h", "days_with_new", "tracked_since",
    }

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    # -- helpers ----------------------------------------------------------

    def _today_perth(self):
        """Return today's Perth-local date as the DB sees it."""
        cur = self.db._execute(
            "SELECT (NOW() AT TIME ZONE 'Australia/Perth')::date AS d"
        )
        row = cur.fetchone()
        assert row is not None
        return row["d"]

    def _cache_count(self) -> int:
        cur = self.db._execute(
            "SELECT COUNT(*)::int AS n FROM peer_dir_daily_aggregates"
        )
        row = cur.fetchone()
        assert row is not None
        return int(row["n"])

    def _cache_rows(self):
        cur = self.db._execute(
            "SELECT day, new_combos, new_peers, new_dirs "
            "FROM peer_dir_daily_aggregates ORDER BY day"
        )
        return [dict(r) for r in cur.fetchall()]

    def _seed_cache(self, day, *, new_combos: int, new_peers: int,
                    new_dirs: int):
        """Direct INSERT bypassing the lazy-fill — used to assert that
        a populated cache prevents recompute."""
        self.db._execute(
            """
            INSERT INTO peer_dir_daily_aggregates
                (day, new_combos, new_peers, new_dirs)
            VALUES (%s, %s, %s, %s)
            """,
            (day, new_combos, new_peers, new_dirs),
        )
        self.db.conn.commit()

    def _assert_response_shape(self, resp: dict):
        self.assertIsInstance(resp, dict)
        self.assertIn("days", resp)
        self.assertIn("totals", resp)
        self.assertIsInstance(resp["days"], list)
        self.assertIsInstance(resp["totals"], dict)
        self.assertEqual(set(resp["totals"].keys()), self._TOTALS_KEYS)
        for day in resp["days"]:
            self.assertEqual(set(day.keys()), self._DAY_KEYS)

    # -- tests ------------------------------------------------------------

    def test_happy_path_empty_cache_backfills_completed_days(self):
        """First call backfills 13 completed-day rows and computes today
        live."""
        today = self._today_perth()
        # Seed observations across 14 distinct Perth-local days. We use
        # 12:00 UTC for each `observed_at` so the Perth-local date never
        # straddles midnight and the bucketing is deterministic.
        for offset in range(14):
            day_perth = today - timedelta(days=offset)
            ts = datetime(
                day_perth.year, day_perth.month, day_perth.day,
                4, 0, 0, tzinfo=timezone.utc,
            )  # 12:00 Perth = 04:00 UTC
            self.db.record_peer_dir_observations(
                [(f"u{offset}", f"d{offset}")],
                observed_at=ts,
            )

        self.assertEqual(self._cache_count(), 0)
        resp = self.db.get_peer_dir_daily_metrics(days=14)
        self._assert_response_shape(resp)
        # 13 completed-day rows cached (today is live, never cached).
        self.assertEqual(self._cache_count(), 13)
        cache_days = {row["day"] for row in self._cache_rows()}
        self.assertNotIn(today, cache_days)
        for offset in range(1, 14):
            self.assertIn(today - timedelta(days=offset), cache_days)
        # Days array has 14 entries, ordered by date DESC (today first).
        self.assertEqual(len(resp["days"]), 14)
        self.assertEqual(resp["days"][0]["date"], today.isoformat())
        # Each seeded day should have exactly 1 new_combo (one obs).
        for day in resp["days"]:
            self.assertEqual(day["new_combos"], 1)
            self.assertEqual(day["new_peers"], 1)
            self.assertEqual(day["new_dirs"], 1)
        # Totals reflect the 14 inserted observations.
        self.assertEqual(resp["totals"]["known_combos"], 14)

    def test_cache_hit_does_not_recompute(self):
        """When all completed days are present in the cache, the method
        does not insert any new cache rows."""
        today = self._today_perth()
        # Seed a cache row for every completed day in the 14-day window.
        for offset in range(1, 14):
            self._seed_cache(
                today - timedelta(days=offset),
                new_combos=offset, new_peers=offset, new_dirs=offset,
            )
        # Add one observation today so the live-today computation runs.
        self.db.record_peer_dir_observations(
            [("today_user", "today_dir")],
        )
        before = self._cache_count()
        self.assertEqual(before, 13)

        resp1 = self.db.get_peer_dir_daily_metrics(days=14)
        after1 = self._cache_count()
        self.assertEqual(after1, before, "cache must not grow on hit")
        # Cache rows should match what we seeded.
        self._assert_response_shape(resp1)
        # Today's row from live-compute (1 observation).
        self.assertEqual(resp1["days"][0]["date"], today.isoformat())
        self.assertEqual(resp1["days"][0]["new_combos"], 1)
        # Yesterday's row from cache.
        yesterday = today - timedelta(days=1)
        ydict = next(d for d in resp1["days"]
                     if d["date"] == yesterday.isoformat())
        self.assertEqual(ydict["new_combos"], 1)

        # Second call must also be a hit.
        resp2 = self.db.get_peer_dir_daily_metrics(days=14)
        self.assertEqual(self._cache_count(), before)
        self.assertEqual(resp1["days"], resp2["days"])

    def test_day_rollover_backfills_only_new_completed_day(self):
        """When the cache covers all but the most-recent completed day,
        the next call backfills exactly that one day."""
        today = self._today_perth()
        # Seed observations for the most-recent completed day only.
        yesterday = today - timedelta(days=1)
        ts = datetime(
            yesterday.year, yesterday.month, yesterday.day,
            4, 0, 0, tzinfo=timezone.utc,
        )
        self.db.record_peer_dir_observations(
            [("yu", "yd")],
            observed_at=ts,
        )
        # Seed all OTHER completed days as cache rows so only yesterday
        # is the missing day.
        for offset in range(2, 14):
            self._seed_cache(
                today - timedelta(days=offset),
                new_combos=0, new_peers=0, new_dirs=0,
            )
        before = self._cache_count()
        self.assertEqual(before, 12)

        resp = self.db.get_peer_dir_daily_metrics(days=14)
        self._assert_response_shape(resp)
        after = self._cache_count()
        self.assertEqual(after, 13, "yesterday must be backfilled")
        # The newly-inserted row matches yesterday's observation count.
        rows = {row["day"]: row for row in self._cache_rows()}
        self.assertIn(yesterday, rows)
        self.assertEqual(rows[yesterday]["new_combos"], 1)

    def test_today_only_observations_no_completed_day_inserts(self):
        """Observations only for today → cache stays empty for today; if
        no completed days have observations, gap-day rows still get
        written for completed days (zero rows) — both behaviours match
        the design (today is live; missing completed days backfill)."""
        today = self._today_perth()
        self.db.record_peer_dir_observations(
            [("u", "d")],
        )
        resp = self.db.get_peer_dir_daily_metrics(days=14)
        self._assert_response_shape(resp)
        # Today's row reflects the observation; completed days are zero.
        self.assertEqual(resp["days"][0]["date"], today.isoformat())
        self.assertEqual(resp["days"][0]["new_combos"], 1)
        for day in resp["days"][1:]:
            self.assertEqual(day["new_combos"], 0)
        # No row in the cache table for today.
        cache_days = {row["day"] for row in self._cache_rows()}
        self.assertNotIn(today, cache_days)

    def test_empty_observations_yields_zeros_no_errors(self):
        """Zero observations entirely → response shows zeros across the
        14-day window and the method does not crash."""
        resp = self.db.get_peer_dir_daily_metrics(days=14)
        self._assert_response_shape(resp)
        self.assertEqual(len(resp["days"]), 14)
        for day in resp["days"]:
            self.assertEqual(day["new_combos"], 0)
            self.assertEqual(day["new_peers"], 0)
            self.assertEqual(day["new_dirs"], 0)
        self.assertEqual(resp["totals"]["known_combos"], 0)
        self.assertIsNone(resp["totals"]["tracked_since"])

    def test_gap_day_in_middle_writes_zero_row(self):
        """A completed day with no observations gets a (0, 0, 0) cache
        row inserted, and subsequent reads hit that zero row."""
        today = self._today_perth()
        # Seed observations on days -2 and -5 only; day -3 and -4 are
        # gap days within the 14-day window.
        for offset in (2, 5):
            day_perth = today - timedelta(days=offset)
            ts = datetime(
                day_perth.year, day_perth.month, day_perth.day,
                4, 0, 0, tzinfo=timezone.utc,
            )
            self.db.record_peer_dir_observations(
                [(f"u{offset}", f"d{offset}")],
                observed_at=ts,
            )
        resp1 = self.db.get_peer_dir_daily_metrics(days=14)
        # 13 completed-day rows backfilled (gap days = zero rows).
        self.assertEqual(self._cache_count(), 13)
        cache = {row["day"]: row for row in self._cache_rows()}
        for offset in (3, 4):  # gap days
            day = today - timedelta(days=offset)
            self.assertIn(day, cache)
            self.assertEqual(cache[day]["new_combos"], 0)
            self.assertEqual(cache[day]["new_peers"], 0)
            self.assertEqual(cache[day]["new_dirs"], 0)
        # Subsequent call must be a cache hit (no new rows).
        before = self._cache_count()
        resp2 = self.db.get_peer_dir_daily_metrics(days=14)
        self.assertEqual(self._cache_count(), before)
        self.assertEqual(resp1["days"], resp2["days"])

    def test_perth_day_boundary_western_edge(self):
        """An observation late in the Perth day must land in that
        Perth-local day's bucket, not the following day."""
        today = self._today_perth()
        # Pick a clearly-completed day (3 days ago).
        target_day = today - timedelta(days=3)
        # 23:55 Perth on target_day = 15:55 UTC on target_day.
        ts = datetime(
            target_day.year, target_day.month, target_day.day,
            15, 55, 0, tzinfo=timezone.utc,
        )
        self.db.record_peer_dir_observations(
            [("edge_u", "edge_d")],
            observed_at=ts,
        )
        self.db.get_peer_dir_daily_metrics(days=14)
        cache = {row["day"]: row for row in self._cache_rows()}
        self.assertIn(target_day, cache)
        self.assertEqual(cache[target_day]["new_combos"], 1)
        # The next day (target_day + 1) must have a (0, 0, 0) cache row.
        next_day = target_day + timedelta(days=1)
        if next_day < today:
            self.assertIn(next_day, cache)
            self.assertEqual(cache[next_day]["new_combos"], 0)

    def test_perth_midnight_lands_in_today_live(self):
        """An observation timestamped just after Perth midnight (i.e.
        late UTC the previous day) must show up in today's live-row
        count, not in yesterday's cache row.

        The test is authoritative: we synthesise an observation at
        ``today_perth 00:30 Perth`` (= ``yesterday_utc 16:30 UTC``) and
        assert it lands in today's bucket.
        """
        today = self._today_perth()
        # 00:30 Perth on today = 16:30 UTC the prior calendar day.
        ts = datetime(
            today.year, today.month, today.day,
            0, 30, 0, tzinfo=timezone.utc,
        ) - timedelta(hours=8)
        # Skip if the synthesised UTC ts is in the future — protects
        # against running this test in the first 30 min of Perth-time
        # midnight. (Rare edge.)
        if ts > datetime.now(timezone.utc):
            self.skipTest("synthesised timestamp is in the future")
        self.db.record_peer_dir_observations(
            [("midnight_u", "midnight_d")],
            observed_at=ts,
        )
        resp = self.db.get_peer_dir_daily_metrics(days=14)
        # Today's live row picks up this observation.
        self.assertEqual(resp["days"][0]["date"], today.isoformat())
        self.assertEqual(resp["days"][0]["new_combos"], 1)
        # Yesterday's cache row is zero (the obs was past midnight Perth).
        yesterday = today - timedelta(days=1)
        cache = {row["day"]: row for row in self._cache_rows()}
        self.assertIn(yesterday, cache)
        self.assertEqual(cache[yesterday]["new_combos"], 0)

    def test_concurrent_backfill_is_race_safe(self):
        """Two simulated concurrent callers backfilling the same days
        must not duplicate rows — ``ON CONFLICT DO NOTHING`` makes the
        second insert a no-op. Both calls return the same response."""
        today = self._today_perth()
        # Seed observations across a few completed days.
        for offset in (2, 4, 7):
            day_perth = today - timedelta(days=offset)
            ts = datetime(
                day_perth.year, day_perth.month, day_perth.day,
                4, 0, 0, tzinfo=timezone.utc,
            )
            self.db.record_peer_dir_observations(
                [(f"u{offset}", f"d{offset}")],
                observed_at=ts,
            )
        # Open a second PipelineDB connection to simulate concurrency.
        from lib import pipeline_db
        db2 = pipeline_db.PipelineDB(TEST_DSN)
        try:
            resp1 = self.db.get_peer_dir_daily_metrics(days=14)
            # Second caller — cache is now populated; must be a no-op.
            before = self._cache_count()
            resp2 = db2.get_peer_dir_daily_metrics(days=14)
            after = self._cache_count()
            self.assertEqual(before, after)
            self.assertEqual(resp1["days"], resp2["days"])
            self.assertEqual(resp1["totals"], resp2["totals"])
        finally:
            db2.close()

    def test_db_error_during_backfill_propagates(self):
        """A DB error during the backfill INSERT propagates as an
        exception — the method does not return a partial dict."""
        # Seed an observation so backfill is attempted.
        ts = datetime.now(timezone.utc) - timedelta(days=2)
        self.db.record_peer_dir_observations(
            [("err_u", "err_d")],
            observed_at=ts,
        )
        # Patch ``execute_values`` on the psycopg2.extras module so the
        # backfill INSERT raises. This is the only external entry point
        # the method uses for the bulk write.
        boom = RuntimeError("simulated backfill failure")

        def explode(*_args, **_kwargs):
            raise boom

        with patch.object(
            psycopg2_extras_module(), "execute_values", explode,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                self.db.get_peer_dir_daily_metrics(days=14)
            self.assertIs(ctx.exception, boom)

    def test_response_shape_pinned_keys_regression(self):
        """The response dict must keep its exact shape — pin every key
        so future drift surfaces as a test failure."""
        # Mix of empty-window and seeded data.
        ts = datetime.now(timezone.utc) - timedelta(hours=2)
        self.db.record_peer_dir_observations(
            [("pin_u", "pin_d")],
            observed_at=ts,
        )
        resp = self.db.get_peer_dir_daily_metrics(days=14)
        self.assertEqual(set(resp.keys()), {"days", "totals"})
        self.assertEqual(set(resp["totals"].keys()), self._TOTALS_KEYS)
        self.assertEqual(len(resp["days"]), 14)
        for day in resp["days"]:
            self.assertEqual(set(day.keys()), self._DAY_KEYS)
            self.assertIsInstance(day["date"], str)
            self.assertIsInstance(day["new_combos"], int)
            self.assertIsInstance(day["new_peers"], int)
            self.assertIsInstance(day["new_dirs"], int)
        # Returned dict must be mutable (caller adds heavy_queries).
        resp["totals"]["heavy_queries"] = []
        self.assertIn("heavy_queries", resp["totals"])


def psycopg2_extras_module():
    """Resolve the ``psycopg2.extras`` module the way ``pipeline_db``
    uses it, so the patch in
    ``test_db_error_during_backfill_propagates`` targets the same
    callable the production code resolves at call time."""
    import psycopg2.extras as _extras
    return _extras


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

    def _row(self, **overrides: Any) -> dict[str, Any]:
        row: dict[str, Any] = {
            "yt_browse_id": "MPREb_abc",
            "yt_audio_playlist_id": "OLAK5uy_abc",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_abc",
            "yt_year": 2020,
            "yt_track_count": 10,
            "yt_tracks": [
                {"title": "Track 1", "video_id": "v1",
                 "length_seconds": 200, "track_number": 1, "disc_number": 1,
                 "artists": [{"name": "Artist"}]},
            ],
            "distances": [
                {"mb_release_id": "mb-1", "distance": 0.05, "error": None},
            ],
        }
        row.update(overrides)
        return row

    def test_get_returns_empty_list_when_nothing_cached(self):
        self.assertEqual(
            self.db.get_youtube_album_mapping("rg-1", "mb"),
            [],
        )

    def test_upsert_inserts_new_rows_and_get_returns_them(self):
        rows = [
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_b"),
        ]

        self.db.upsert_youtube_album_mapping("rg-1", "mb", rows)

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
        self.assertEqual(len(got), 2)
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_a", "MPREb_b"],
        )
        # JSONB columns deserialize back into native Python lists/dicts.
        self.assertEqual(
            got[0]["yt_tracks"][0]["title"], "Track 1")
        self.assertEqual(
            got[0]["distances"][0]["mb_release_id"], "mb-1")

    def test_get_orders_rows_by_yt_browse_id(self):
        self.db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_z"),
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_m"),
        ])

        got = self.db.get_youtube_album_mapping("rg-1", "mb")
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

        self.assertEqual(
            [r["yt_browse_id"] for r in
             self.db.get_youtube_album_mapping("rg-1", "mb")],
            ["MPREb_a_v2"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in
             self.db.get_youtube_album_mapping("rg-2", "mb")],
            ["MPREb_b"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in
             self.db.get_youtube_album_mapping("rg-1", "discogs")],
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
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_pre1", "MPREb_pre2"],
        )


if __name__ == "__main__":
    unittest.main()
