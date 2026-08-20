"""Tests for force-import feature — CLI, DB, and import_one --force flag.

Tests cover:
- import_one.py --force flag (raises max_distance to the shared override)
- pipeline_cli.py force-import command
- pipeline_db.py get_download_log_entry() method
- 'force_import' outcome in download_log
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Bootstrap ephemeral PostgreSQL if available
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from tests.helpers import REQUEST_CASCADE_RESET_TABLES, delete_all_rows

TEST_DSN = os.environ.get("TEST_DB_DSN")


def make_db():
    from lib.pipeline_db import PipelineDB
    db = PipelineDB(TEST_DSN)
    delete_all_rows(db, REQUEST_CASCADE_RESET_TABLES)
    return db


# ---------------------------------------------------------------------------
# import_one.py --force flag
# ---------------------------------------------------------------------------

class TestImportOneForceFlag(unittest.TestCase):
    """Test that import_one.main() toggles max_distance from the real CLI flag."""

    def setUp(self) -> None:
        # import_one.main() calls reset_umask() (sets umask to 0o002 for the
        # subprocess chain, GH #84). Restore so later tests keep their default.
        self._saved_umask = os.umask(0o022)
        os.umask(self._saved_umask)
        self.addCleanup(os.umask, self._saved_umask)

    def test_force_flag_raises_max_distance_to_the_shared_override(self) -> None:
        """--force must raise ``max_distance`` to the ONE override constant.

        The expected value comes from the producing module, not a literal:
        ``lib.beets.FORCE_IMPORT_DISTANCE_THRESHOLD`` is the same number the
        force lane hands ``beets_validate``, so both comparison sites in a
        force import run under one override (#1080).
        """
        from harness import import_one
        from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD

        class _StopAfterForce(Exception):
            pass

        original = import_one.max_distance
        try:
            with patch.object(
                sys, "argv",
                ["import_one.py", "/tmp/staged-album", "mbid-123", "--force"],
            ), patch("harness.import_one._log"), patch(
                "harness.import_one.BeetsDB", side_effect=_StopAfterForce
            ), self.assertRaises(_StopAfterForce):
                import_one.main()

            self.assertEqual(
                import_one.max_distance, FORCE_IMPORT_DISTANCE_THRESHOLD,
            )
        finally:
            import_one.max_distance = original

    def test_default_main_keeps_max_distance(self) -> None:
        """Without --force, main() must leave max_distance at the default."""
        from harness import import_one

        class _StopBeforeWork(Exception):
            pass

        original = import_one.max_distance
        try:
            with patch.object(
                sys, "argv",
                ["import_one.py", "/tmp/staged-album", "mbid-123"],
            ), patch("harness.import_one._log"), patch(
                "harness.import_one.BeetsDB", side_effect=_StopBeforeWork
            ), self.assertRaises(_StopBeforeWork):
                import_one.main()

            self.assertEqual(import_one.max_distance, original)
        finally:
            import_one.max_distance = original


# ---------------------------------------------------------------------------
# pipeline_db: get_download_log_entry and force_import outcome
# ---------------------------------------------------------------------------

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

class TestCmdForceImport(unittest.TestCase):
    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    def test_force_import_missing_log_entry(self) -> None:
        """force-import with non-existent download_log_id should print error."""
        from scripts import pipeline_cli
        args = MagicMock(download_log_id=99999, dsn=TEST_DSN)
        # Should not raise, just print error
        pipeline_cli.cmd_force_import(self.db, args)

    def test_force_import_no_failed_path(self) -> None:
        """force-import on log entry without failed_path should print error."""
        from scripts import pipeline_cli
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
        from scripts import pipeline_cli
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

if __name__ == "__main__":
    unittest.main()
