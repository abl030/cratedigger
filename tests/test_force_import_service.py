"""Configured-root force-import enqueue contract."""

from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace

from lib.force_import_service import (
    FORCE_IMPORT_HTTP_STATUS,
    RESULT_DOWNLOAD_LOG_MISSING,
    RESULT_FAILED_PATH_MISSING,
    RESULT_PROCESSING_LOCKED,
    RESULT_QUEUED,
    RESULT_REQUEST_MBID_MISSING,
    RESULT_REQUEST_MISSING,
    RESULT_UNAUTHORIZED_PATH,
    ForceImportEnqueueResult,
    enqueue_force_import,
)
from lib.import_queue import ForceImportPayload, force_import_dedupe_key
from tests.fakes import FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row


class TestForceImportService(unittest.TestCase):
    def test_centralized_adapter_outcome_maps(self) -> None:
        """One table now serves both surfaces (issue #1063).

        ``pipeline-cli force-import`` relays the route, so its exit code
        is DERIVED from the status below. Pinning the derivation is what
        proves the historical CLI mapping survived the move.
        """
        from scripts.pipeline_cli.api_mutations import _exit_code

        expected_exit = {
            RESULT_QUEUED: 0,
            RESULT_DOWNLOAD_LOG_MISSING: 2,
            RESULT_REQUEST_MISSING: 2,
            RESULT_REQUEST_MBID_MISSING: 3,
            RESULT_FAILED_PATH_MISSING: 3,
            RESULT_UNAUTHORIZED_PATH: 3,
            RESULT_PROCESSING_LOCKED: 4,
        }
        expected_status = {
            RESULT_QUEUED: 202,
            RESULT_DOWNLOAD_LOG_MISSING: 404,
            RESULT_REQUEST_MISSING: 404,
            RESULT_REQUEST_MBID_MISSING: 422,
            RESULT_FAILED_PATH_MISSING: 422,
            RESULT_UNAUTHORIZED_PATH: 422,
            RESULT_PROCESSING_LOCKED: 409,
        }
        self.assertEqual(FORCE_IMPORT_HTTP_STATUS, expected_status)
        for outcome, exit_code in expected_exit.items():
            with self.subTest(outcome=outcome):
                self.assertEqual(
                    _exit_code(FORCE_IMPORT_HTTP_STATUS[outcome]), exit_code)

    def test_processing_owner_is_rejected_before_filesystem_or_enqueue(self) -> None:
        db, cfg, staging, log_id = self._world()
        album = os.path.join(staging, "failed_imports", "owned", "Album")
        os.makedirs(album)
        self._set_path(db, log_id, album)
        job = handoff_automation_owner(db, 867)
        before = db.get_request(867)

        result = enqueue_force_import(db, cfg, log_id)

        self.assertEqual(result.outcome, RESULT_PROCESSING_LOCKED)
        self.assertIsNotNone(result.processing_owner)
        assert result.processing_owner is not None
        self.assertEqual(result.processing_owner.job_id, job.id)
        self.assertEqual(db.get_request(867), before)
        self.assertEqual(db.list_import_jobs(), [job])
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
            "source_dirs": (
                ["peer\\Artist\\Album"] if source_dirs is None else source_dirs
            ),
        }

    def _assert_queued(
        self,
        db: FakePipelineDB,
        result: ForceImportEnqueueResult,
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

    def test_youtube_wrong_match_under_staging_enqueues(self) -> None:
        db, cfg, staging, log_id = self._world()
        album = os.path.join(
            staging,
            "auto-import",
            "wrong_matches",
            "Loon_Lake-Low_Res-playlist-request-111-log-39310",
        )
        os.makedirs(album)
        db.download_logs[log_id - 1].soulseek_username = None
        self._set_path(db, log_id, album, source_dirs=[])

        result = enqueue_force_import(db, cfg, log_id)

        self._assert_queued(
            db, result,
            download_log_id=log_id,
            request_id=867,
            failed_path=album,
            source_username=None,
            source_dirs=[],
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
