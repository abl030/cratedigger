"""Tests for force-import feature — CLI, DB, and import_one --force flag.

Tests cover:
- import_one.py --force flag (sets MAX_DISTANCE=999)
- pipeline_cli.py force-import command
- pipeline_db.py get_download_log_entry() method
- 'force_import' outcome in download_log
"""

import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Bootstrap ephemeral PostgreSQL if available
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "harness"))

TEST_DSN = os.environ.get("TEST_DB_DSN")


def make_db():
    from pipeline_db import PipelineDB
    db = PipelineDB(TEST_DSN)
    for table in ["source_denylist", "download_log", "album_tracks", "album_requests"]:
        db._execute(f"TRUNCATE {table} CASCADE")
    db.conn.commit()
    return db


# ---------------------------------------------------------------------------
# import_one.py --force flag
# ---------------------------------------------------------------------------

class TestImportOneForceFlag(unittest.TestCase):
    """Test that import_one.main() toggles MAX_DISTANCE from the real CLI flag."""

    def setUp(self) -> None:
        # import_one.main() calls reset_umask() (sets umask to 0 for the
        # subprocess chain, GH #84). Restore so later tests keep their default.
        self._saved_umask = os.umask(0o022)
        os.umask(self._saved_umask)
        self.addCleanup(os.umask, self._saved_umask)

    def test_force_flag_sets_max_distance_999(self) -> None:
        """--force must set MAX_DISTANCE=999 in the real main() entry point."""
        import import_one

        class _StopAfterForce(Exception):
            pass

        original = import_one.MAX_DISTANCE
        try:
            with patch.object(
                sys, "argv",
                ["import_one.py", "/tmp/staged-album", "mbid-123", "--force"],
            ), patch("import_one._log"), patch(
                "import_one.BeetsDB", side_effect=_StopAfterForce
            ):
                with self.assertRaises(_StopAfterForce):
                    import_one.main()

            self.assertEqual(import_one.MAX_DISTANCE, 999)
        finally:
            import_one.MAX_DISTANCE = original

    def test_default_main_keeps_max_distance(self) -> None:
        """Without --force, main() must leave MAX_DISTANCE at the default."""
        import import_one

        class _StopBeforeWork(Exception):
            pass

        original = import_one.MAX_DISTANCE
        try:
            with patch.object(
                sys, "argv",
                ["import_one.py", "/tmp/staged-album", "mbid-123"],
            ), patch("import_one._log"), patch(
                "import_one.BeetsDB", side_effect=_StopBeforeWork
            ):
                with self.assertRaises(_StopBeforeWork):
                    import_one.main()

            self.assertEqual(import_one.MAX_DISTANCE, original)
        finally:
            import_one.MAX_DISTANCE = original


# ---------------------------------------------------------------------------
# pipeline_db: get_download_log_entry and force_import outcome
# ---------------------------------------------------------------------------

@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestGetDownloadLogEntry(unittest.TestCase):
    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    def test_get_download_log_entry_returns_row(self) -> None:
        """get_download_log_entry(log_id) should return the row dict."""
        req_id = self.db.add_request(
            mb_release_id="test-mbid-1",
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
        )
        vr_json = json.dumps({
            "valid": False,
            "failed_path": "failed_imports/Test Artist - Test Album",
            "scenario": "distance_too_high",
        })
        self.db.log_download(
            request_id=req_id,
            outcome="rejected",
            validation_result=vr_json,
        )
        # Get the log entry
        history = self.db.get_download_history(req_id)
        log_id = history[0]["id"]

        entry = self.db.get_download_log_entry(log_id)
        self.assertIsNotNone(entry)
        assert entry is not None  # narrow type for pyright
        self.assertEqual(entry["request_id"], req_id)
        self.assertEqual(entry["outcome"], "rejected")

    def test_get_download_log_entry_not_found(self) -> None:
        """get_download_log_entry returns None for non-existent ID."""
        entry = self.db.get_download_log_entry(99999)
        self.assertIsNone(entry)

    def test_force_import_outcome_allowed(self) -> None:
        """'force_import' should be a valid outcome in download_log."""
        req_id = self.db.add_request(
            mb_release_id="test-mbid-2",
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
        )
        # This should NOT raise a constraint violation
        self.db.log_download(
            request_id=req_id,
            outcome="force_import",
        )
        history = self.db.get_download_history(req_id)
        self.assertEqual(history[0]["outcome"], "force_import")


# ---------------------------------------------------------------------------
# pipeline_cli: force-import command
# ---------------------------------------------------------------------------

@unittest.skipUnless(TEST_DSN, "TEST_DB_DSN not set")
class TestCmdForceImport(unittest.TestCase):
    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    def test_force_import_missing_log_entry(self) -> None:
        """force-import with non-existent download_log_id should print error."""
        import pipeline_cli
        args = MagicMock(download_log_id=99999, dsn=TEST_DSN)
        # Should not raise, just print error
        pipeline_cli.cmd_force_import(self.db, args)

    def test_force_import_no_failed_path(self) -> None:
        """force-import on log entry without failed_path should print error."""
        import pipeline_cli
        req_id = self.db.add_request(
            mb_release_id="test-mbid-3",
            artist_name="Test",
            album_title="Album",
            source="request",
        )
        # Log entry with no validation_result
        self.db.log_download(request_id=req_id, outcome="rejected")
        history = self.db.get_download_history(req_id)
        log_id = history[0]["id"]

        args = MagicMock(download_log_id=log_id, dsn=TEST_DSN)
        pipeline_cli.cmd_force_import(self.db, args)
        # Should just print error, not crash

    def test_force_import_files_missing(self) -> None:
        """force-import when failed_path doesn't exist should print error."""
        import pipeline_cli
        req_id = self.db.add_request(
            mb_release_id="test-mbid-4",
            artist_name="Test",
            album_title="Album",
            source="request",
        )
        vr_json = json.dumps({
            "valid": False,
            "failed_path": "/nonexistent/path/that/does/not/exist",
        })
        self.db.log_download(
            request_id=req_id,
            outcome="rejected",
            validation_result=vr_json,
        )
        history = self.db.get_download_history(req_id)
        log_id = history[0]["id"]

        args = MagicMock(download_log_id=log_id, dsn=TEST_DSN)
        pipeline_cli.cmd_force_import(self.db, args)
        # Should print error about missing files, not crash

    def test_force_import_no_mbid(self) -> None:
        """force-import when album_request has no mb_release_id should error."""
        import pipeline_cli
        req_id = self.db.add_request(
            mb_release_id=None,
            discogs_release_id="12345",
            artist_name="Test",
            album_title="Album",
            source="request",
        )
        vr_json = json.dumps({
            "valid": False,
            "failed_path": "failed_imports/Test - Album",
        })
        self.db.log_download(
            request_id=req_id,
            outcome="rejected",
            validation_result=vr_json,
        )
        history = self.db.get_download_history(req_id)
        log_id = history[0]["id"]

        args = MagicMock(download_log_id=log_id, dsn=TEST_DSN)
        pipeline_cli.cmd_force_import(self.db, args)


# ---------------------------------------------------------------------------
# _resolve_failed_path
# ---------------------------------------------------------------------------

class TestResolveFailedPath(unittest.TestCase):
    def test_absolute_path_exists(self) -> None:
        """Absolute path that exists should be returned as-is."""
        import pipeline_cli
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            result = pipeline_cli._resolve_failed_path(d)
            self.assertEqual(result, d)

    def test_nonexistent_path_returns_none(self) -> None:
        """Path that doesn't exist anywhere should return None."""
        import pipeline_cli
        result = pipeline_cli._resolve_failed_path("/nonexistent/path/xyz")
        self.assertIsNone(result)

    def test_relative_path_resolved(self) -> None:
        """Relative path should be resolved against SLSKD_DOWNLOAD_DIRS."""
        import pipeline_cli
        import tempfile
        with tempfile.TemporaryDirectory() as base:
            # Create a subdir to simulate failed_imports/Album
            subdir = os.path.join(base, "failed_imports", "Test Album")
            os.makedirs(subdir)
            # Temporarily override SLSKD_DOWNLOAD_DIRS
            old = pipeline_cli.SLSKD_DOWNLOAD_DIRS
            pipeline_cli.SLSKD_DOWNLOAD_DIRS = [base]
            try:
                result = pipeline_cli._resolve_failed_path("failed_imports/Test Album")
                self.assertEqual(result, subdir)
            finally:
                pipeline_cli.SLSKD_DOWNLOAD_DIRS = old


if __name__ == "__main__":
    unittest.main()
