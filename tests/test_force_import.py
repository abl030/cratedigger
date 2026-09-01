"""Tests for force-import feature — CLI, DB, and import_one --force flag.

Tests cover:
- import_one.py --force flag (raises the run's apply-time distance ceiling to
  the shared override)
- pipeline_cli.py force-import command
- pipeline_db.py get_download_log_entry() method
- 'force_import' outcome in download_log
"""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock

# Bootstrap ephemeral PostgreSQL if available
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from tests.helpers import REQUEST_CASCADE_RESET_TABLES, delete_all_rows
from tests.test_import_one_stages import run_evidence_authorized_import

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
    """``--force`` raises the ceiling the Beets child actually runs under.

    The ceiling used to be a module attribute ``main()`` mutated, and these
    tests read it back off the module. It is now derived per run and threaded
    into ``run_import``, so they assert the value the child was handed.

    That value is still a proxy, not the decided outcome: ``run_import`` is
    an intermediate, and the outermost real adapter for a distance ceiling
    is the ``dist > max_distance`` comparison inside ``_run_import_once``.
    ``tests/test_disambiguation.py::TestApplyDistanceCeiling`` drives that
    comparison and asserts apply-versus-reject flipping on the ceiling
    alone; these tests pin that the flag reaches it.
    """

    def test_force_flag_raises_the_ceiling_to_the_shared_override(self) -> None:
        """The expected value comes from the producing module, not a literal.

        ``lib.beets.FORCE_IMPORT_DISTANCE_THRESHOLD`` is the same number the
        force lane hands ``beets_validate``, so both comparison sites in a
        force import run under one override (#1080).
        """
        from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD

        with tempfile.TemporaryDirectory() as tmpdir:
            _result, ceilings = run_evidence_authorized_import(
                tmpdir, force=True)

        self.assertEqual(ceilings, [FORCE_IMPORT_DISTANCE_THRESHOLD])

    def test_a_default_run_keeps_the_default_ceiling(self) -> None:
        from harness import import_one

        with tempfile.TemporaryDirectory() as tmpdir:
            _result, ceilings = run_evidence_authorized_import(tmpdir)

        self.assertEqual(ceilings, [import_one.DEFAULT_MAX_DISTANCE])

    def test_argv_is_where_force_enters(self) -> None:
        """``--force`` on the real CLI contract, and nothing else, sets it."""
        from harness.import_one import ImportOneRequest

        forced = ImportOneRequest.from_argv(
            ["/tmp/staged-album", "mbid-123", "--force"])
        plain = ImportOneRequest.from_argv(["/tmp/staged-album", "mbid-123"])

        self.assertTrue(forced.force)
        self.assertFalse(plain.force)

    def test_a_force_run_does_not_raise_a_later_default_run(self) -> None:
        """The override lives on one request, not on the process.

        While the ceiling was a module global, a ``--force`` run left it
        raised for everything after it. One process, two runs, is the whole
        point of the split.
        """
        from harness import import_one
        from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD

        with tempfile.TemporaryDirectory() as tmpdir:
            _forced, forced_ceilings = run_evidence_authorized_import(
                os.path.join(tmpdir, "first"), force=True)
            _plain, plain_ceilings = run_evidence_authorized_import(
                os.path.join(tmpdir, "second"))

        self.assertEqual(forced_ceilings, [FORCE_IMPORT_DISTANCE_THRESHOLD])
        self.assertEqual(plain_ceilings, [import_one.DEFAULT_MAX_DISTANCE])


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
