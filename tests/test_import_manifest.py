import os
import tempfile
import unittest
from typing import Any, cast

from lib.grab_list import DownloadFile
from lib.dispatch import (
    DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
    dispatch_import_from_db,
)
from lib.import_manifest import (
    check_audio_manifest,
    move_failed_import_curated,
    tracked_audio_paths_for_downloads,
)
from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestImportManifest(unittest.TestCase):
    def test_check_audio_manifest_reports_untracked_audio(self):
        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.flac"), "wb").close()
            open(os.path.join(root, "bonus.opus"), "wb").close()
            open(os.path.join(root, "cover.jpg"), "wb").close()

            check = check_audio_manifest(root, ["01.flac"])

        self.assertFalse(check.ok)
        self.assertEqual(check.extra_audio, ["bonus.opus"])
        self.assertEqual(check.missing_audio, [])

    def test_curated_failed_import_excludes_extra_audio_and_keeps_sidecars(self):
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "Album")
            os.mkdir(source)
            open(os.path.join(source, "01.flac"), "wb").close()
            open(os.path.join(source, "bonus.opus"), "wb").close()
            open(os.path.join(source, "cover.jpg"), "wb").close()

            failed_path = move_failed_import_curated(
                source,
                allowed_audio=["01.flac"],
                scenario="high_distance",
            )

            self.assertIsNotNone(failed_path)
            assert failed_path is not None
            self.assertTrue(os.path.exists(os.path.join(failed_path, "01.flac")))
            self.assertTrue(os.path.exists(os.path.join(failed_path, "cover.jpg")))
            self.assertFalse(os.path.exists(os.path.join(failed_path, "bonus.opus")))

            quarantined = os.path.join(
                parent,
                "failed_imports",
                "untracked_audio",
                "Album",
                "bonus.opus",
            )
            self.assertTrue(os.path.exists(quarantined))

    def test_download_manifest_uses_staged_filenames(self):
        files = [
            DownloadFile(
                filename=r"remote\01.flac",
                id="",
                file_dir="",
                username="peer",
                size=1,
            ),
            DownloadFile(
                filename=r"remote\02.opus",
                id="",
                file_dir="",
                username="peer",
                size=1,
                disk_no=2,
                disk_count=2,
            ),
        ]

        self.assertEqual(
            tracked_audio_paths_for_downloads(files),
            ["01.flac", "Disk 2 - 02.opus"],
        )

    def test_validation_manifest_recovers_pre_move_absolute_items(self):
        from lib.import_manifest import tracked_audio_paths_from_validation_items

        with tempfile.TemporaryDirectory() as parent:
            staging = os.path.join(parent, "Incoming", "Album")
            failed = os.path.join(parent, "failed_imports", "Album")
            os.makedirs(staging)
            os.makedirs(failed)

            paths = tracked_audio_paths_from_validation_items(
                [{"path": os.path.join(staging, "01 Perth.flac")}],
                root=failed,
            )

        self.assertEqual(paths, ["01 Perth.flac"])


class TestForceImportManifestGuard(unittest.TestCase):
    @staticmethod
    def _queued_job(db: FakePipelineDB, failed_path: str) -> int:
        return db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            payload=manual_import_payload(failed_path=failed_path),
        ).id

    def test_force_import_rejects_audio_not_in_origin_manifest(self):
        import msgspec
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
        from lib.quality import ImportResult, SpectralAnalysisDetail, SpectralDetail

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Bon Iver",
            album_title="Bon Iver",
        ))
        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01 Perth.flac"), "wb").close()
            open(os.path.join(root, "12 Wash.opus"), "wb").close()
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "failed_path": root,
                    "items": [{"path": os.path.join(root, "01 Perth.flac")}],
                },
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=root,
                    source_username="alice",
                ),
            )
            audit = SpectralDetail(
                candidate=SpectralAnalysisDetail(
                    attempted=True, grade="suspect", bitrate_kbps=96),
                existing=SpectralAnalysisDetail(
                    attempted=True, grade="genuine", bitrate_kbps=245),
            )
            preview_import_result = msgspec.to_builtins(
                ImportResult(spectral=audit))
            assert isinstance(preview_import_result, dict)
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"import_result": preview_import_result},
            )

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=True,
                import_job_id=job.id,
                download_log_id=log_id,
            )

        self.assertFalse(outcome.success)
        # Extra/untracked audio: keep the Wrong Matches entry for operator
        # review (the importer skips cleanup on this code).
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("12 Wash.opus", outcome.message)
        # R20: the album is still wanted — only this source is contaminated.
        # The request self-heals to wanted (idempotent here) + an audit row is
        # written, but the WM entry is preserved (code above).
        self.assertEqual(db.request(42)["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "untracked_audio"), outcomes)
        rejection = next(
            log for log in db.download_logs
            if log.outcome == "rejected" and log.beets_scenario == "untracked_audio"
        )
        self.assertIsNotNone(rejection.import_result)
        assert rejection.import_result is not None
        self.assertEqual(
            ImportResult.from_json(rejection.import_result).spectral,
            audit,
        )
        # Operator's folder choice, not the peer's fault — never denylist.
        self.assertEqual(len(db.denylist), 0)

    def test_force_import_rejects_origin_manifest_with_extra_items(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))
        db.set_tracks(42, [{"track_number": 1, "title": "One"}])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.flac"), "wb").close()
            open(os.path.join(root, "bonus.flac"), "wb").close()
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "failed_path": root,
                    "items": [
                        {"path": os.path.join(root, "01.flac")},
                        {"path": os.path.join(root, "bonus.flac")},
                    ],
                },
            )
            job_id = self._queued_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=True,
                import_job_id=job_id,
                download_log_id=log_id,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("manifest has 2 audio files", outcome.message)
        self.assertEqual(db.request(42)["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "untracked_audio"), outcomes)
        self.assertEqual(len(db.denylist), 0)

    def test_force_import_without_origin_manifest_rejects_track_count_mismatch(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))
        db.set_tracks(42, [
            {"track_number": 1, "title": "One"},
            {"track_number": 2, "title": "Two"},
        ])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            open(os.path.join(root, "02.mp3"), "wb").close()
            open(os.path.join(root, "bonus.mp3"), "wb").close()
            job_id = self._queued_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=False,
                import_job_id=job_id,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("3 audio files", outcome.message)
        self.assertEqual(db.request(42)["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "untracked_audio"), outcomes)
        self.assertEqual(len(db.denylist), 0)

    def test_manual_import_without_manifest_or_tracks_keeps_wm_and_self_heals(self):
        """No manifest and no track rows for a non-empty source: we can't
        verify the folder, so it fails closed against beets AND keeps the
        Wrong Matches entry for review — but the request still self-heals to
        ``wanted`` (R20), the album is still wanted."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            job_id = self._queued_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=False,
                import_job_id=job_id,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("requires either an origin audio manifest", outcome.message)
        self.assertEqual(db.request(42)["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "unverifiable_source"), outcomes)
        self.assertEqual(len(db.denylist), 0)

    def test_undercount_without_manifest_self_heals_to_wanted(self):
        """Issue #387: an under-count source (fewer audio files than the
        request expects, no extra files) is a missing-audio integrity fault.
        The guard self-heals the request back to ``wanted`` (R20) rather than
        stall it, but still returns ``IMPORT_MANIFEST_REJECTED`` so the
        importer PRESERVES the operator's partial audio (it is not 'nothing
        to inspect' — there are real files on disk)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))
        db.set_tracks(42, [
            {"track_number": 1, "title": "One"},
            {"track_number": 2, "title": "Two"},
        ])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            job_id = self._queued_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=False,
                import_job_id=job_id,
            )

        self.assertFalse(outcome.success)
        # Preserve-folder code (importer skips deletion) — a non-empty source
        # must never route through the rmtree-ing QUALITY_PIPELINE_REJECTED.
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(db.request(42)["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "incomplete_fileset"), outcomes)
        # Missing audio is not the peer's fault — never denylist.
        self.assertEqual(len(db.denylist), 0)

    def test_manifest_subset_self_heals_to_wanted(self):
        """Issue #387: the on-disk folder is a strict subset of the validated
        origin manifest (some validated tracks went missing, no extra audio).
        Missing audio → self-heal + preserve the folder, not the
        untracked-audio framing."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))
        db.set_tracks(42, [
            {"track_number": 1, "title": "One"},
            {"track_number": 2, "title": "Two"},
        ])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "failed_path": root,
                    "items": [
                        {"path": os.path.join(root, "01.mp3")},
                        {"path": os.path.join(root, "02.mp3")},
                    ],
                },
            )
            job_id = self._queued_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=True,
                import_job_id=job_id,
                download_log_id=log_id,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(db.request(42)["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "incomplete_fileset"), outcomes)
        self.assertEqual(len(db.denylist), 0)
