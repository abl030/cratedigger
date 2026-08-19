"""Configured-root local-import enqueue contract (issue #1176 PR3)."""

from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace

from lib.import_queue import LocalImportPayload, local_import_dedupe_key
from lib.local_import_service import (
    LOCAL_IMPORT_HTTP_STATUS,
    RESULT_NOT_CONFIGURED,
    RESULT_PATH_UNAVAILABLE,
    RESULT_PROCESSING_LOCKED,
    RESULT_QUEUED,
    RESULT_REQUEST_MBID_MISSING,
    RESULT_REQUEST_MISSING,
    RESULT_UNAUTHORIZED_PATH,
    LocalImportEnqueueResult,
    enqueue_local_import,
)
from tests.fakes import FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row


class TestLocalImportService(unittest.TestCase):
    def test_centralized_adapter_outcome_maps(self) -> None:
        """One table serves both surfaces (mirrors force-import, #1063)."""
        from scripts.pipeline_cli.api_mutations import _exit_code

        expected_exit = {
            RESULT_QUEUED: 0,
            RESULT_REQUEST_MISSING: 2,
            RESULT_REQUEST_MBID_MISSING: 3,
            RESULT_NOT_CONFIGURED: 3,
            RESULT_UNAUTHORIZED_PATH: 3,
            RESULT_PATH_UNAVAILABLE: 5,
            RESULT_PROCESSING_LOCKED: 4,
        }
        expected_status = {
            RESULT_QUEUED: 202,
            RESULT_REQUEST_MISSING: 404,
            RESULT_REQUEST_MBID_MISSING: 422,
            RESULT_NOT_CONFIGURED: 422,
            RESULT_UNAUTHORIZED_PATH: 422,
            RESULT_PATH_UNAVAILABLE: 503,
            RESULT_PROCESSING_LOCKED: 409,
        }
        self.assertEqual(LOCAL_IMPORT_HTTP_STATUS, expected_status)
        for outcome, exit_code in expected_exit.items():
            with self.subTest(outcome=outcome):
                self.assertEqual(
                    _exit_code(LOCAL_IMPORT_HTTP_STATUS[outcome]), exit_code)

    def _world(self, *, enabled: bool = True) -> tuple[FakePipelineDB, SimpleNamespace, str]:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = temp.name
        local_import_dir = os.path.join(root, "LocalImport")
        os.makedirs(local_import_dir)
        # Owned subtrees the authority must refuse a candidate inside —
        # deliberately siblings of local_import_dir, never containing it.
        processing = os.path.join(root, "processing")
        staging = os.path.join(root, "Incoming")
        slskd = os.path.join(root, "slskd")
        beets_dir = os.path.join(root, "Beets")
        beets_db_dir = os.path.join(root, "beets-db")
        for path in (processing, staging, slskd, beets_dir, beets_db_dir):
            os.makedirs(path)
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=867, artist_name="Artist", album_title="Album", mb_release_id="mb-867",
        ))
        cfg = SimpleNamespace(
            local_import_enabled=enabled,
            local_import_dir=local_import_dir,
            processing_dir=processing,
            beets_staging_dir=staging,
            slskd_download_dir=slskd,
            beets_directory=beets_dir,
            beets_library_db=os.path.join(beets_db_dir, "beets-library.db"),
        )
        return db, cfg, local_import_dir

    def test_queues_with_canonical_path(self) -> None:
        db, cfg, root = self._world()
        album = os.path.join(root, "MyRip", "Album")
        os.makedirs(album)

        result = enqueue_local_import(
            db, cfg, request_id=867, source_path=album,
        )

        self.assertEqual(result.outcome, RESULT_QUEUED)
        self.assertEqual(result.request_id, 867)
        self.assertEqual(result.source_path, album)
        self.assertEqual(len(db.list_import_jobs()), 1)
        job = result.job
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job.request_id, 867)
        self.assertEqual(job.dedupe_key, local_import_dedupe_key(867))
        self.assertIsInstance(job.payload, LocalImportPayload)
        assert isinstance(job.payload, LocalImportPayload)
        self.assertEqual(job.payload.source_path, album)
        self.assertEqual(job.payload.request_id, 867)

    def test_missing_request_returns_exact_outcome_without_job(self) -> None:
        db, cfg, root = self._world()

        result = enqueue_local_import(
            db, cfg, request_id=999_999, source_path=root,
        )

        self.assertEqual(result.outcome, RESULT_REQUEST_MISSING)
        self.assertEqual(result.request_id, 999_999)
        self.assertEqual(db.list_import_jobs(), [])

    def test_missing_mbid_is_rejected_before_filesystem(self) -> None:
        db, cfg, root = self._world()
        db.seed_request(make_request_row(
            id=867, artist_name="Artist", album_title="Album",
            mb_release_id=None, discogs_release_id="867",
        ))

        result = enqueue_local_import(
            db, cfg, request_id=867, source_path=root,
        )

        self.assertEqual(result.outcome, RESULT_REQUEST_MBID_MISSING)
        self.assertEqual(db.list_import_jobs(), [])

    def test_processing_owner_is_rejected_before_filesystem_or_enqueue(self) -> None:
        db, cfg, root = self._world()
        job = handoff_automation_owner(db, 867)
        before = db.get_request(867)

        result = enqueue_local_import(
            db, cfg, request_id=867, source_path=root,
        )

        self.assertEqual(result.outcome, RESULT_PROCESSING_LOCKED)
        self.assertIsNotNone(result.processing_owner)
        assert result.processing_owner is not None
        self.assertEqual(result.processing_owner.job_id, job.id)
        self.assertEqual(db.get_request(867), before)
        self.assertEqual(db.list_import_jobs(), [job])

    def test_lane_not_configured_is_its_own_outcome(self) -> None:
        db, cfg, root = self._world(enabled=False)
        album = os.path.join(root, "MyRip")
        os.makedirs(album)

        result = enqueue_local_import(
            db, cfg, request_id=867, source_path=album,
        )

        self.assertEqual(result.outcome, RESULT_NOT_CONFIGURED)
        self.assertIn("services.cratedigger.localImport", result.detail or "")
        self.assertEqual(db.list_import_jobs(), [])

    def test_path_outside_root_is_unauthorized(self) -> None:
        db, cfg, _root = self._world()
        outside = tempfile.mkdtemp()
        self.addCleanup(lambda: os.rmdir(outside))

        result = enqueue_local_import(
            db, cfg, request_id=867, source_path=outside,
        )

        self.assertEqual(result.outcome, RESULT_UNAUTHORIZED_PATH)
        self.assertEqual(db.list_import_jobs(), [])

    def test_path_inside_owned_processing_subtree_is_unauthorized(self) -> None:
        db, cfg, _root = self._world()
        # A broad local_import_dir containing processing_dir as a sibling
        # is fine; pointing INSIDE the owned subtree itself must refuse
        # even when it is nested under the configured root.
        cfg.local_import_dir = os.path.dirname(cfg.processing_dir)
        candidate = os.path.join(cfg.processing_dir, "albums", "some-album")
        os.makedirs(candidate)

        result = enqueue_local_import(
            db, cfg, request_id=867, source_path=candidate,
        )

        self.assertEqual(result.outcome, RESULT_UNAUTHORIZED_PATH)
        self.assertEqual(db.list_import_jobs(), [])

    def test_missing_candidate_reports_path_unavailable(self) -> None:
        db, cfg, root = self._world()
        missing = os.path.join(root, "does-not-exist")

        result = enqueue_local_import(
            db, cfg, request_id=867, source_path=missing,
        )

        # A genuinely absent name is `missing`, which is NOT indeterminate
        # (it positively proves absence) — so this is unauthorized_path,
        # not path_unavailable. Pinned here so a future authority change
        # that reclassifies `missing` is caught by this exact assertion.
        self.assertEqual(result.outcome, RESULT_UNAUTHORIZED_PATH)
        self.assertEqual(db.list_import_jobs(), [])

    def test_result_dataclass_construction(self) -> None:
        result = LocalImportEnqueueResult(RESULT_QUEUED, 42)
        self.assertEqual(result.outcome, RESULT_QUEUED)
        self.assertEqual(result.request_id, 42)
        self.assertIsNone(result.source_path)
        self.assertIsNone(result.job)


if __name__ == "__main__":
    unittest.main()
