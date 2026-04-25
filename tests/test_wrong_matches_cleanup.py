"""Tests for shared Wrong Matches cleanup helpers."""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestWrongMatchCleanup(unittest.TestCase):
    def _make_db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="Artist",
            album_title="Album",
            mb_release_id="mbid-1",
            status="manual",
        ))
        return db

    def _log_rejected(
        self,
        db: FakePipelineDB,
        *,
        failed_path: str,
        request_id: int = 1,
        username: str = "alice",
    ) -> int:
        db.log_download(
            request_id,
            soulseek_username=username,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": failed_path,
            },
        )
        return db.download_logs[-1].id

    def test_deletes_directory_and_clears_original_wrong_match_row(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_rejected(db, failed_path=source)

            result = cleanup_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(result.deleted_path, os.path.abspath(source))
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_clears_relative_and_absolute_duplicate_rows(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            raw_path = "failed_imports/Artist - Album"
            original_id = self._log_rejected(
                db, failed_path=raw_path, username="old")
            self._log_rejected(
                db, failed_path=os.path.abspath(source), username="new")

            result = cleanup_wrong_match_source(
                db, original_id, failed_path_hint=source)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 2)
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_missing_directory_still_clears_stale_pointer(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        shutil.rmtree(source)
        log_id = self._log_rejected(db, failed_path=source)

        result = cleanup_wrong_match_source(db, log_id)

        self.assertTrue(result.success)
        self.assertTrue(result.path_missing)
        self.assertIsNone(result.deleted_path)
        self.assertEqual(result.cleared_rows, 1)
        self.assertEqual(db.get_wrong_matches(), [])

    def test_delete_race_still_clears_stale_pointer(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            log_id = self._log_rejected(db, failed_path=source)

            with patch("lib.wrong_matches.shutil.rmtree",
                       side_effect=FileNotFoundError(source)):
                result = cleanup_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertTrue(result.path_missing)
            self.assertIsNone(result.deleted_path)
            self.assertEqual(result.resolved_path, os.path.abspath(source))
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_delete_error_reports_failure_and_keeps_pointer(self):
        from lib.wrong_matches import cleanup_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            log_id = self._log_rejected(db, failed_path=source)

            with patch("lib.wrong_matches.shutil.rmtree",
                       side_effect=OSError("permission denied")):
                result = cleanup_wrong_match_source(db, log_id)

            self.assertFalse(result.success)
            self.assertIn("permission denied", result.error or "")
            self.assertEqual(result.cleared_rows, 0)
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_dismiss_clears_pointer_without_deleting_directory(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_rejected(db, failed_path=source)

            result = dismiss_wrong_match_source(db, log_id)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 1)
            self.assertEqual(result.resolved_path, os.path.abspath(source))
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_dismiss_clears_relative_and_absolute_duplicate_rows(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        try:
            raw_path = "failed_imports/Artist - Album"
            original_id = self._log_rejected(
                db, failed_path=raw_path, username="old")
            self._log_rejected(
                db, failed_path=os.path.abspath(source), username="new")

            result = dismiss_wrong_match_source(
                db, original_id, failed_path_hint=source)

            self.assertTrue(result.success)
            self.assertEqual(result.cleared_rows, 2)
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_dismiss_missing_directory_still_clears_stale_pointer(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()
        source = tempfile.mkdtemp()
        shutil.rmtree(source)
        log_id = self._log_rejected(db, failed_path=source)

        result = dismiss_wrong_match_source(db, log_id)

        self.assertTrue(result.success)
        self.assertIsNone(result.resolved_path)
        self.assertEqual(result.cleared_rows, 1)
        self.assertEqual(db.get_wrong_matches(), [])

    def test_dismiss_missing_entry_reports_failure(self):
        from lib.wrong_matches import dismiss_wrong_match_source

        db = self._make_db()

        result = dismiss_wrong_match_source(db, 99999)

        self.assertFalse(result.success)
        self.assertFalse(result.entry_found)
        self.assertEqual(result.cleared_rows, 0)
        self.assertIn("99999", result.error or "")


if __name__ == "__main__":
    unittest.main()
