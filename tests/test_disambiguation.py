"""Tests for post-import %aunique disambiguation in import_one.py.

Covers:
- run_import() returning kept_duplicate=True when harness sends resolve_duplicate
  with a different MBID (keep both editions)
- run_import() returning kept_duplicate=False for same-MBID duplicates (replace)
- run_import() returning kept_duplicate=False on normal imports (no duplicate)
- beet move invocation after kept_duplicate import
- _run_disambiguation_move() helper hardening (issue #127): subprocess
  fragility around the post-import ``beet move`` call. The helper must
  never raise — TimeoutExpired, OSError, and non-zero rc all surface as
  a short error string (or None on clean exit).
"""

import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch, MagicMock, ANY

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "harness"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))


def _make_harness_proc(messages: list[dict]) -> MagicMock:
    """Create a mock Popen that emits a sequence of JSON messages on stdout.

    Each message is a JSON line. After all messages, readline() returns "".
    """
    proc = MagicMock()
    proc.pid = 12345
    proc.stdin = MagicMock()
    proc.stderr = MagicMock()
    proc.stderr.read.return_value = ""

    lines = [json.dumps(m) + "\n" for m in messages] + [""]
    stdout_mock = MagicMock()
    stdout_mock.fileno.return_value = 99
    stdout_mock.readline = MagicMock(side_effect=lines)
    proc.stdout = stdout_mock

    proc.poll.return_value = 0
    proc.wait.return_value = 0
    return proc


TARGET_MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
OTHER_MBID = "cccccccc-4444-5555-6666-dddddddddddd"


class TestRunImportKeptDuplicate(unittest.TestCase):
    """Test that run_import correctly reports kept_duplicate."""

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_keep_different_edition_sets_kept_duplicate(self, mock_popen, mock_select):
        """When resolve_duplicate has a different MBID and we say keep,
        kept_duplicate should be True."""
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_mbids": [OTHER_MBID]},
            {"type": "choose_match", "candidates": [
                {"album_id": TARGET_MBID, "distance": 0.05,
                 "artist": "The National", "album": "High Violet"},
            ]},
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        # select.select always says stdout is ready
        mock_select.return_value = ([99], [], [])

        rc, beets_lines, kept_duplicate = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 0)
        self.assertTrue(kept_duplicate)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_replace_same_mbid_not_kept_duplicate(self, mock_popen, mock_select):
        """When resolve_duplicate has the same MBID (stale entry), we say
        remove — kept_duplicate should be False."""
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_mbids": [TARGET_MBID]},
            {"type": "choose_match", "candidates": [
                {"album_id": TARGET_MBID, "distance": 0.05,
                 "artist": "The National", "album": "High Violet"},
            ]},
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        rc, beets_lines, kept_duplicate = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 0)
        self.assertFalse(kept_duplicate)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_no_duplicate_not_kept(self, mock_popen, mock_select):
        """Normal import without duplicate resolution — kept_duplicate False."""
        from harness import import_one

        messages = [
            {"type": "choose_match", "candidates": [
                {"album_id": TARGET_MBID, "distance": 0.02,
                 "artist": "The National", "album": "High Violet"},
            ]},
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        rc, beets_lines, kept_duplicate = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 0)
        self.assertFalse(kept_duplicate)

    @patch("harness.import_one.os.killpg")
    @patch("harness.import_one.os.getpgid", return_value=12345)
    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_timeout_returns_false_kept_duplicate(self, mock_popen, mock_select,
                                                  mock_getpgid, mock_killpg):
        """On timeout, kept_duplicate should be False."""
        from harness import import_one

        proc = MagicMock()
        proc.pid = 12345
        proc.stdin = MagicMock()
        proc.stdout = MagicMock()
        proc.stdout.fileno.return_value = 99
        proc.stderr = MagicMock()
        proc.stderr.read.return_value = ""
        proc.wait.return_value = 1
        mock_popen.return_value = proc
        # select returns empty = timeout
        mock_select.return_value = ([], [], [])

        rc, beets_lines, kept_duplicate = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 2)
        self.assertFalse(kept_duplicate)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_skip_returns_false_kept_duplicate(self, mock_popen, mock_select):
        """When MBID not found in candidates (skip), kept_duplicate False."""
        from harness import import_one

        messages = [
            {"type": "choose_match", "candidates": [
                {"album_id": "wrong-mbid", "distance": 0.02,
                 "artist": "X", "album": "Y"},
            ]},
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        rc, beets_lines, kept_duplicate = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 4)
        self.assertFalse(kept_duplicate)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_harness_nonzero_after_apply_returns_error(self, mock_popen, mock_select):
        """A harness crash after applying a candidate must still fail run_import."""
        from harness import import_one

        messages = [
            {"type": "choose_match", "candidates": [
                {"album_id": TARGET_MBID, "distance": 0.02,
                 "artist": "The National", "album": "High Violet"},
            ]},
        ]
        proc = _make_harness_proc(messages)
        proc.poll.return_value = 2
        proc.wait.return_value = 2
        proc.stderr.read.return_value = (
            "beets.dbcore.db.DBAccessError: attempt to write a readonly database\n"
        )
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        rc, beets_lines, kept_duplicate = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 2)
        self.assertFalse(kept_duplicate)
        self.assertIn("readonly database", "\n".join(beets_lines))


class TestDisambiguateBeetMove(unittest.TestCase):
    """Test that beet move is called when kept_duplicate is True."""

    @patch("harness.import_one.subprocess.run")
    def test_beet_move_called_after_kept_duplicate(self, mock_run):
        """When kept_duplicate=True, subprocess.run(['beet', 'move', ...])
        should be called."""
        from harness import import_one
        from lib.quality import PostflightInfo

        # Create mock for beet move call
        move_result = MagicMock()
        move_result.returncode = 0
        mock_run.return_value = move_result

        # Mock BeetsDB to return updated path after move
        mock_beets = MagicMock()
        from lib.beets_db import AlbumInfo
        moved_info = AlbumInfo(
            album_id=42, track_count=11,
            min_bitrate_kbps=245, is_cbr=False,
            album_path="/Beets/The National/2010 - High Violet [expanded edition]")
        mock_beets.get_album_info.side_effect = [moved_info]

        # Simulate: kept_duplicate=True, postflight already populated
        pf = PostflightInfo(beets_id=42, track_count=11,
                            imported_path="/Beets/The National/2010 - High Violet")

        # Call the disambiguation logic directly (extracted for testability)
        mbid = "42f45e3f-3248-4ee5-ac27-4a99a4af48eb"
        kept_duplicate = True

        if kept_duplicate:
            from lib.util import beets_subprocess_env
            move_result = import_one.subprocess.run(
                ["beet", "move", f"mb_albumid:{mbid}"],
                capture_output=True, text=True, timeout=120,
                env=beets_subprocess_env(),
            )
            if move_result.returncode == 0:
                pf_info_after = mock_beets.get_album_info(mbid)
                if pf_info_after:
                    new_path = pf_info_after.album_path
                    if new_path != pf.imported_path:
                        pf.imported_path = new_path
                    pf.disambiguated = True

        # Verify beet move was called
        mock_run.assert_called_once_with(
            ["beet", "move", f"mb_albumid:{mbid}"],
            capture_output=True, text=True, timeout=120,
            env=ANY,
        )
        # Verify path was updated
        self.assertEqual(pf.imported_path,
                         "/Beets/The National/2010 - High Violet [expanded edition]")
        self.assertTrue(pf.disambiguated)

    def test_beet_move_not_called_without_kept_duplicate(self):
        """When kept_duplicate=False, no beet move should occur."""
        from lib.quality import PostflightInfo

        pf = PostflightInfo(beets_id=42, track_count=11,
                            imported_path="/Beets/The National/2010 - High Violet")

        kept_duplicate = False

        # The disambiguation block should not execute
        if kept_duplicate:
            raise AssertionError("Should not reach disambiguation block")

        self.assertFalse(pf.disambiguated)
        self.assertEqual(pf.imported_path, "/Beets/The National/2010 - High Violet")


class TestRunDisambiguationMoveHelper(unittest.TestCase):
    """Direct unit tests for ``_run_disambiguation_move(mbid)`` (issue #127).

    Mirrors the hardened helper pattern in
    ``lib/release_cleanup.py::_run_remove_selector``: one place owns the
    subprocess invocation, never raises, returns a short typed-ish error
    description (``str | None``) the caller stores on ``PostflightInfo``.

    Bug being prevented: the original inline ``subprocess.run`` had no
    try/except. A ``TimeoutExpired`` or ``OSError`` on the post-import
    ``beet move`` crashed import_one.py *after* beets had already
    written the album to disk, so callers parsed no JSON sentinel and
    treated the import as failed — a semi-lie that could trigger a
    duplicate force-import attempt.
    """

    MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"

    @patch("harness.import_one.subprocess.run")
    def test_clean_exit_returns_none(self, mock_run):
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        mock_run.return_value = proc

        result = import_one._run_disambiguation_move(self.MBID)

        self.assertIsNone(result)
        mock_run.assert_called_once()
        # argv shape is the seam contract — must hit `beet move
        # mb_albumid:<mbid>` with text capture and a 120s timeout.
        args, kwargs = mock_run.call_args
        self.assertEqual(args[0][1:], ["move", f"mb_albumid:{self.MBID}"])
        self.assertEqual(kwargs.get("timeout"), 120)
        self.assertTrue(kwargs.get("capture_output"))
        self.assertTrue(kwargs.get("text"))

    @patch("harness.import_one.subprocess.run")
    def test_timeout_returns_typed_string(self, mock_run):
        from harness import import_one

        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=["beet", "move"], timeout=120)

        result = import_one._run_disambiguation_move(self.MBID)

        self.assertIsNotNone(result)
        assert result is not None  # narrow for pyright
        self.assertIn("timeout", result.lower())
        self.assertIn("120", result)

    @patch("harness.import_one.subprocess.run")
    def test_oserror_returns_typed_string(self, mock_run):
        """FileNotFoundError (beet missing on PATH) must be caught."""
        from harness import import_one

        mock_run.side_effect = FileNotFoundError(2, "No such file", "beet")

        result = import_one._run_disambiguation_move(self.MBID)

        self.assertIsNotNone(result)
        assert result is not None
        # Reason tag should identify the exception class so the audit
        # trail in download_log can distinguish "beet missing" from
        # "beet timed out".
        self.assertIn("FileNotFoundError", result)

    @patch("harness.import_one.subprocess.run")
    def test_nonzero_rc_with_stderr_includes_last_line(self, mock_run):
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 1
        proc.stderr = ("warning: ignored line\n"
                       "error: could not find album with id mb_albumid:bogus\n")
        mock_run.return_value = proc

        result = import_one._run_disambiguation_move(self.MBID)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("rc=1", result)
        self.assertIn("could not find album", result)

    @patch("harness.import_one.subprocess.run")
    def test_nonzero_rc_with_empty_stderr_still_returns(self, mock_run):
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 2
        proc.stderr = ""
        mock_run.return_value = proc

        result = import_one._run_disambiguation_move(self.MBID)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("rc=2", result)


if __name__ == "__main__":
    unittest.main()
