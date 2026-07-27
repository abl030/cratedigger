"""Configured-root force-import enqueue contract."""

from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace

from lib.force_import_service import (
    FORCE_IMPORT_EXIT_CODE,
    FORCE_IMPORT_HTTP_STATUS,
    RESULT_DOWNLOAD_LOG_MISSING,
    RESULT_FAILED_PATH_MISSING,
    RESULT_QUEUED,
    RESULT_REQUEST_MBID_MISSING,
    RESULT_REQUEST_MISSING,
    RESULT_UNAUTHORIZED_PATH,
    enqueue_force_import,
)
from lib.import_queue import ForceImportPayload, force_import_dedupe_key
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestForceImportService(unittest.TestCase):
    def test_centralized_adapter_outcome_maps(self) -> None:
        self.assertEqual(FORCE_IMPORT_EXIT_CODE[RESULT_QUEUED], 0)
        self.assertEqual(FORCE_IMPORT_HTTP_STATUS[RESULT_QUEUED], 202)
        for outcome in (RESULT_DOWNLOAD_LOG_MISSING, RESULT_REQUEST_MISSING):
            self.assertEqual(FORCE_IMPORT_EXIT_CODE[outcome], 2)
            self.assertEqual(FORCE_IMPORT_HTTP_STATUS[outcome], 404)
        for outcome in (
            RESULT_REQUEST_MBID_MISSING,
            RESULT_FAILED_PATH_MISSING,
            RESULT_UNAUTHORIZED_PATH,
        ):
            self.assertEqual(FORCE_IMPORT_EXIT_CODE[outcome], 3)
            self.assertEqual(FORCE_IMPORT_HTTP_STATUS[outcome], 422)
    def _world(self) -> tuple[FakePipelineDB, SimpleNamespace, str, int]:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = temp.name
        staging = os.path.join(root, "Incoming")
        slskd = os.path.join(root, "slskd")
        processing = os.path.join(root, "processing")
        os.makedirs(staging)
        os.makedirs(slskd)
        os.makedirs(processing)
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=867, artist_name="Artist", album_title="Album", mb_release_id="mb-867",
        ))
        log_id = db.log_download(
            request_id=867,
            outcome="rejected",
            soulseek_username="peer",
            validation_result={},
        )
        cfg = SimpleNamespace(
            beets_staging_dir=staging,
            slskd_download_dir=slskd,
            processing_dir=processing,
        )
        return db, cfg, staging, log_id

    def _set_path(
        self,
        db: FakePipelineDB,
        log_id: int,
        path: str,
        *,
        soulseek_username: str | None = None,
        source_dirs: list[str] | None = None,
    ) -> None:
        db.download_logs[log_id - 1].validation_result = {
            "scenario": "high_distance", "failed_path": path,
            "soulseek_username": soulseek_username,
            "source_dirs": source_dirs or ["peer\\Artist\\Album"],
        }

    def _assert_queued(
        self,
        db: FakePipelineDB,
        result: object,
        *,
        download_log_id: int,
        request_id: int,
        failed_path: str,
        source_username: str | None,
        source_dirs: list[str],
    ) -> None:
        self.assertEqual(result.outcome, RESULT_QUEUED)
        self.assertEqual(result.download_log_id, download_log_id)
        self.assertEqual(result.request_id, request_id)
        self.assertEqual(result.failed_path, failed_path)
        self.assertEqual(len(db.list_import_jobs()), 1)
        job = result.job
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job.request_id, request_id)
        self.assertEqual(job.dedupe_key, force_import_dedupe_key(download_log_id))
        self.assertIsInstance(job.payload, ForceImportPayload)
        assert isinstance(job.payload, ForceImportPayload)
        self.assertEqual(job.payload.download_log_id, download_log_id)
        self.assertEqual(job.payload.failed_path, failed_path)
        self.assertEqual(job.payload.source_username, source_username)
        self.assertEqual(job.payload.source_dirs, source_dirs)

    def test_authorized_staging_quarantine_enqueues_with_canonical_path(self) -> None:
        db, cfg, staging, log_id = self._world()
        album = os.path.join(staging, "failed_imports", "bad_files", "Album")
        os.makedirs(album)
        self._set_path(db, log_id, album)

        result = enqueue_force_import(db, cfg, log_id)

        self._assert_queued(
            db, result,
            download_log_id=log_id,
            request_id=867,
            failed_path=album,
            source_username="peer",
            source_dirs=["peer\\Artist\\Album"],
        )

    def test_missing_download_log_returns_exact_outcome_without_job(self) -> None:
        db, cfg, _staging, _log_id = self._world()

        result = enqueue_force_import(db, cfg, 999_999)

        self.assertEqual(result.outcome, RESULT_DOWNLOAD_LOG_MISSING)
        self.assertEqual(result.download_log_id, 999_999)
        self.assertIsNone(result.request_id)
        self.assertEqual(db.list_import_jobs(), [])

    def test_missing_request_returns_exact_outcome_without_job(self) -> None:
        db, cfg, _staging, _log_id = self._world()
        log_id = db.log_download(
            request_id=999,
            outcome="rejected",
            validation_result={"failed_path": "/irrelevant"},
        )

        result = enqueue_force_import(db, cfg, log_id)

        self.assertEqual(result.outcome, RESULT_REQUEST_MISSING)
        self.assertEqual(result.download_log_id, log_id)
        self.assertEqual(result.request_id, 999)
        self.assertEqual(db.list_import_jobs(), [])

    def test_missing_failed_path_returns_exact_outcome_without_job(self) -> None:
        db, cfg, _staging, log_id = self._world()

        result = enqueue_force_import(db, cfg, log_id)

        self.assertEqual(result.outcome, RESULT_FAILED_PATH_MISSING)
        self.assertEqual(result.download_log_id, log_id)
        self.assertEqual(result.request_id, 867)
        self.assertEqual(db.list_import_jobs(), [])

    def test_legacy_relative_slskd_path_enqueues_with_canonical_path(self) -> None:
        db, cfg, _staging, log_id = self._world()
        relative = os.path.join("failed_imports", "legacy", "Album")
        album = os.path.join(cfg.slskd_download_dir, relative)
        os.makedirs(album)
        self._set_path(
            db, log_id, relative,
            source_dirs=[" peer\\Album ", "peer\\Album", "", "other\\Album"],
        )

        result = enqueue_force_import(db, cfg, log_id)

        self._assert_queued(
            db, result,
            download_log_id=log_id,
            request_id=867,
            failed_path=album,
            source_username="peer",
            source_dirs=["peer\\Album", "other\\Album"],
        )

    def test_discogs_only_request_is_rejected_before_enqueue(self) -> None:
        db, cfg, staging, log_id = self._world()
        db.seed_request(make_request_row(
            id=867, artist_name="Artist", album_title="Album",
            mb_release_id=None, discogs_release_id="867",
        ))
        album = os.path.join(staging, "failed_imports", "discogs", "Album")
        os.makedirs(album)
        self._set_path(db, log_id, album)

        result = enqueue_force_import(db, cfg, log_id)

        self.assertEqual(result.outcome, RESULT_REQUEST_MBID_MISSING)
        self.assertEqual(result.download_log_id, log_id)
        self.assertEqual(result.request_id, 867)
        self.assertIn("MusicBrainz release ID", result.detail or "")
        self.assertEqual(db.list_import_jobs(), [])

    def test_validation_username_fallback_enqueues_with_exact_payload(self) -> None:
        db, cfg, staging, log_id = self._world()
        db.download_logs[log_id - 1].soulseek_username = None
        album = os.path.join(staging, "failed_imports", "fallback", "Album")
        os.makedirs(album)
        self._set_path(
            db, log_id, album,
            soulseek_username="envelope-peer",
            source_dirs=["one", " one ", "", "two"],
        )

        result = enqueue_force_import(db, cfg, log_id)

        self._assert_queued(
            db, result,
            download_log_id=log_id,
            request_id=867,
            failed_path=album,
            source_username="envelope-peer",
            source_dirs=["one", "two"],
        )

    def test_unauthorized_or_lookalike_path_creates_no_job(self) -> None:
        db, cfg, staging, log_id = self._world()
        lookalike = os.path.join(staging, "failed_imports-old", "Album")
        os.makedirs(lookalike)
        self._set_path(db, log_id, lookalike)

        result = enqueue_force_import(db, cfg, log_id)

        self.assertEqual(result.outcome, RESULT_UNAUTHORIZED_PATH)
        self.assertEqual(db.list_import_jobs(), [])

    def test_symlinked_quarantine_candidate_creates_no_job(self) -> None:
        db, cfg, _staging, log_id = self._world()
        outside = os.path.join(os.path.dirname(cfg.slskd_download_dir), "outside")
        os.makedirs(outside)
        marker = os.path.join(cfg.slskd_download_dir, "failed_imports")
        os.makedirs(marker)
        candidate = os.path.join(marker, "Album")
        os.symlink(outside, candidate)
        self._set_path(db, log_id, candidate)

        result = enqueue_force_import(db, cfg, log_id)

        self.assertEqual(result.outcome, RESULT_UNAUTHORIZED_PATH)
        self.assertEqual(db.list_import_jobs(), [])
