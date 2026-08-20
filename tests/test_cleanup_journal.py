"""Real-PostgreSQL contracts for exact-owner processor cleanup journals."""

from __future__ import annotations

import os
import sys
import unittest

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401  (boots and migrates ephemeral PostgreSQL)

from lib.pipeline_db import (
    CleanupJournalConflict,
    CleanupJournalIntent,
    CleanupJournalReceipt,
    PipelineDB,
    ProcessingCleanupJournalRow,
)
from tests.helpers import REQUEST_CASCADE_RESET_TABLES, delete_all_rows

TEST_DSN = os.environ["TEST_DB_DSN"]


def _intent(*, action: str = "move_tree") -> CleanupJournalIntent:
    return CleanupJournalIntent(
        action=action,
        source_path="/processing/source",
        source_manifest=(
            {"path": "01.flac", "size": 12, "hash": "track-1"},
            {"path": "cover.jpg", "size": 7, "hash": "cover"},
        ),
        source_manifest_hash="source-sha256",
        destination_path="/processing/destination",
        destination_manifest=(
            {"path": "01.flac", "size": 12, "hash": "track-1"},
            {"path": "cover.jpg", "size": 7, "hash": "cover"},
        ),
        destination_manifest_hash="destination-sha256",
        selected_destination_path="/processing/destination",
    )


class CleanupJournalPostgresCase(unittest.TestCase):
    db: PipelineDB

    def setUp(self) -> None:
        self.db = PipelineDB(TEST_DSN)
        delete_all_rows(self.db, REQUEST_CASCADE_RESET_TABLES)

    def tearDown(self) -> None:
        self.db.close()

    def _processing_owner(self, suffix: str) -> tuple[int, int]:
        with self.db._atomic():
            request_cur = self.db._execute(
                """
                INSERT INTO album_requests (
                    mb_release_id, artist_name, album_title, source
                )
                VALUES (%s, 'Artist', 'Album', 'request')
                RETURNING id
                """,
                (f"cleanup-{suffix}",),
            )
            request_id = int(request_cur.fetchone()["id"])
            job_cur = self.db._execute(
                """
                INSERT INTO import_jobs (
                    job_type, status, request_id, payload, preview_status
                )
                VALUES (
                    'automation_import', 'queued', %s, '{}'::jsonb, 'waiting'
                )
                RETURNING id
                """,
                (request_id,),
            )
            job_id = int(job_cur.fetchone()["id"])
            self.db._execute(
                """
                UPDATE album_requests
                SET status = 'processing',
                    active_automation_import_job_id = %s
                WHERE id = %s
                """,
                (job_id, request_id),
            )
            self.db.conn.commit()
        return request_id, job_id


class TestCleanupJournalPersistence(CleanupJournalPostgresCase):
    def test_row_projection_matches_migration_exactly(self) -> None:
        cur = self.db._execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'processing_cleanup_journal'
            """
        )
        columns = {str(row["column_name"]) for row in cur.fetchall()}
        self.assertEqual(
            set(ProcessingCleanupJournalRow.__annotations__),
            columns,
        )

    def test_create_round_trips_exact_intent_and_same_replay_is_idempotent(
        self,
    ) -> None:
        request_id, job_id = self._processing_owner("create")
        intent = _intent()

        created = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            intent=intent,
        )
        replayed = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            intent=intent,
        )

        self.assertEqual(created, replayed)
        self.assertEqual(created["revision"], 1)
        self.assertEqual(
            created["source_manifest"],
            [dict(entry) for entry in intent.source_manifest],
        )
        self.assertEqual(
            created["destination_manifest"],
            [dict(entry) for entry in intent.destination_manifest or ()],
        )
        self.assertEqual(created["step_progress"], {})
        self.assertIsNone(created["completed_receipt"])
        self.assertEqual(
            self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
            ),
            created,
        )

    def test_create_rejects_wrong_owner_and_conflicting_intent(self) -> None:
        request_id, job_id = self._processing_owner("owner-a")
        other_request_id, other_job_id = self._processing_owner("owner-b")

        with self.assertRaises(CleanupJournalConflict) as wrong_owner:
            self.db.create_processing_cleanup_journal(
                request_id=request_id,
                job_id=other_job_id,
                intent=_intent(),
            )
        self.assertEqual(wrong_owner.exception.kind, "owner_mismatch")
        self.assertIsNone(
            self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
            )
        )
        self.assertIsNone(
            self.db.get_processing_cleanup_journal(
                request_id=other_request_id,
                job_id=other_job_id,
            )
        )

        original = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            intent=_intent(),
        )
        with self.assertRaises(CleanupJournalConflict) as conflict:
            self.db.create_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
                intent=_intent(action="quarantine_tree"),
            )
        self.assertEqual(conflict.exception.kind, "intent_conflict")
        self.assertEqual(
            self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
            ),
            original,
        )

    def test_checkpoint_is_monotonic_revision_cas_and_replay_safe(
        self,
    ) -> None:
        request_id, job_id = self._processing_owner("checkpoint")
        intent = _intent()
        created = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            intent=intent,
        )

        first = self.db.checkpoint_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            expected_revision=created["revision"],
            step_progress={"source_verified": True},
        )
        replay = self.db.checkpoint_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            expected_revision=created["revision"],
            step_progress={"source_verified": True},
        )
        self.assertEqual(replay, first)
        self.assertEqual(first["revision"], created["revision"] + 1)
        self.assertEqual(
            self.db.create_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
                intent=intent,
            ),
            first,
            "replaying intent must not erase already-checkpointed progress",
        )

        with self.assertRaises(CleanupJournalConflict) as stale:
            self.db.checkpoint_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
                expected_revision=created["revision"],
                step_progress={
                    "source_verified": True,
                    "published": True,
                },
            )
        self.assertEqual(stale.exception.kind, "revision_conflict")

        with self.assertRaises(CleanupJournalConflict) as rewrite:
            self.db.checkpoint_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
                expected_revision=first["revision"],
                step_progress={"source_verified": False},
            )
        self.assertEqual(rewrite.exception.kind, "progress_conflict")
        self.assertEqual(
            self.db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
            ),
            first,
        )

    def test_complete_requires_exact_typed_receipt_and_is_idempotent(
        self,
    ) -> None:
        request_id, job_id = self._processing_owner("complete")
        created = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            intent=_intent(),
        )
        checkpoint = self.db.checkpoint_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            expected_revision=created["revision"],
            step_progress={"source_verified": True, "published": True},
        )
        receipt = CleanupJournalReceipt(
            outcome="completed",
            action=checkpoint["action"],
            source_path=checkpoint["source_path"],
            source_manifest_hash=checkpoint["source_manifest_hash"],
            destination_path=checkpoint["destination_path"],
            destination_manifest_hash=checkpoint[
                "destination_manifest_hash"
            ],
            selected_destination_path=checkpoint[
                "selected_destination_path"
            ],
            step_progress=checkpoint["step_progress"],
            details={"collision_policy": "selected_once"},
        )

        completed = self.db.complete_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            expected_revision=checkpoint["revision"],
            receipt=receipt,
        )
        replay = self.db.complete_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            expected_revision=checkpoint["revision"],
            receipt=receipt,
        )
        self.assertEqual(replay, completed)
        self.assertEqual(completed["completed_receipt"], receipt)
        self.assertIsNotNone(completed["completed_at"])
        self.assertEqual(
            completed["revision"],
            checkpoint["revision"] + 1,
        )

        conflicting_receipt = CleanupJournalReceipt(
            outcome="completed",
            action=checkpoint["action"],
            source_path=checkpoint["source_path"],
            source_manifest_hash=checkpoint["source_manifest_hash"],
            destination_path=checkpoint["destination_path"],
            destination_manifest_hash=checkpoint[
                "destination_manifest_hash"
            ],
            selected_destination_path=checkpoint[
                "selected_destination_path"
            ],
            step_progress=checkpoint["step_progress"],
            details={"collision_policy": "recomputed"},
        )
        with self.assertRaises(CleanupJournalConflict) as conflict:
            self.db.complete_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
                expected_revision=completed["revision"],
                receipt=conflicting_receipt,
            )
        self.assertEqual(conflict.exception.kind, "receipt_conflict")


if __name__ == "__main__":
    unittest.main()
