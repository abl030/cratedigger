import os
import tempfile
import unittest
from typing import Any, cast

from lib.grab_list import DownloadFile
from lib.import_dispatch import (
    DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
    dispatch_import_from_db,
)
from lib.import_manifest import (
    check_audio_manifest,
    move_failed_import_curated,
    tracked_audio_paths_for_downloads,
)
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
    def test_force_import_rejects_audio_not_in_origin_manifest(self):
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

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=True,
                import_job_id=99,
                download_log_id=log_id,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("12 Wash.opus", outcome.message)

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

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=True,
                import_job_id=99,
                download_log_id=log_id,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("manifest has 2 audio files", outcome.message)

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

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=False,
                import_job_id=99,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("3 audio files", outcome.message)

    def test_manual_import_without_manifest_or_tracks_fails_closed(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
        ))

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                force=False,
                import_job_id=99,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIn("requires either an origin audio manifest", outcome.message)
