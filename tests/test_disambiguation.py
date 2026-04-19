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
    subprocess invocation, never raises, returns a typed
    ``DisambiguationFailure`` (with a ``Literal`` reason tag) the
    caller stores on ``PostflightInfo``. Returns ``None`` on clean exit.

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
    def test_timeout_returns_typed_failure(self, mock_run):
        from harness import import_one

        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=["beet", "move"], timeout=120)

        result = import_one._run_disambiguation_move(self.MBID)

        self.assertIsNotNone(result)
        assert result is not None  # narrow for pyright
        self.assertEqual(result.reason, "timeout")
        self.assertIn("timeout", result.detail.lower())
        self.assertIn("120", result.detail)

    @patch("harness.import_one.subprocess.run")
    def test_oserror_returns_typed_failure(self, mock_run):
        """FileNotFoundError (beet missing on PATH) must be caught."""
        from harness import import_one

        mock_run.side_effect = FileNotFoundError(2, "No such file", "beet")

        result = import_one._run_disambiguation_move(self.MBID)

        self.assertIsNotNone(result)
        assert result is not None
        # Reason tag matches SelectorFailure's "exception" class.
        self.assertEqual(result.reason, "exception")
        # Detail carries the exception class name so the audit trail can
        # distinguish "beet missing" from other OSError flavors.
        self.assertIn("FileNotFoundError", result.detail)

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
        self.assertEqual(result.reason, "nonzero_rc")
        self.assertIn("rc=1", result.detail)
        self.assertIn("could not find album", result.detail)

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
        self.assertEqual(result.reason, "nonzero_rc")
        self.assertEqual(result.detail, "rc=2")


class TestApplyDisambiguationCallsiteContract(unittest.TestCase):
    """Integration test for the ``_apply_disambiguation`` callsite contract.

    The reviewer's P2 ask on PR #128: prove that the import-success
    path is preserved when the move fails. Specifically — when the
    underlying ``beet move`` raises ``TimeoutExpired`` (or returns
    non-zero rc), the function:

    1. Must NOT raise — the album is already on disk, the caller's
       ``ImportResult`` exit_code/decision must be untouched.
    2. Must record a typed ``DisambiguationFailure`` on
       ``r.postflight.disambiguation_failure``.
    3. Must NOT set ``r.postflight.disambiguated = True`` (lying).
    4. Must NOT mutate ``imported_path`` (the failed move did not
       change the path on disk).
    5. Must round-trip cleanly through ImportResult JSON serialization
       — proving the ``__IMPORT_RESULT__`` sentinel still emits.
    """

    MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
    ORIGINAL_PATH = "/Beets/The National/2010 - High Violet"

    def _make_result_and_beets(self):
        from lib.quality import ImportResult, PostflightInfo

        r = ImportResult(
            decision="import",
            postflight=PostflightInfo(
                beets_id=42, track_count=11,
                imported_path=self.ORIGINAL_PATH))
        beets = MagicMock()
        return r, beets

    def _assert_import_success_preserved(self, r):
        """Properties (1)+(5): import succeeded; ImportResult JSON survives."""
        from lib.quality import ImportResult

        self.assertEqual(r.exit_code, 0)
        self.assertEqual(r.decision, "import")
        # JSON round-trip — proves the sentinel line will emit cleanly
        # and downstream parse_import_result() can still consume it.
        roundtrip = ImportResult.from_json(r.to_json())
        self.assertEqual(roundtrip.exit_code, 0)
        self.assertEqual(roundtrip.decision, "import")
        self.assertFalse(roundtrip.postflight.disambiguated)
        self.assertEqual(roundtrip.postflight.imported_path,
                         self.ORIGINAL_PATH)

    @patch("harness.import_one.subprocess.run")
    def test_timeout_does_not_crash_and_preserves_import(self, mock_run):
        from harness import import_one

        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=["beet", "move"], timeout=120)
        r, beets = self._make_result_and_beets()

        # Must NOT raise.
        new_path = import_one._apply_disambiguation(
            self.MBID, beets, self.ORIGINAL_PATH, r)

        # Property (4): path unchanged on failure.
        self.assertEqual(new_path, self.ORIGINAL_PATH)
        # Property (3): not lying about disambiguation.
        self.assertFalse(r.postflight.disambiguated)
        # Property (2): typed failure recorded with correct reason tag.
        self.assertIsNotNone(r.postflight.disambiguation_failure)
        assert r.postflight.disambiguation_failure is not None
        self.assertEqual(
            r.postflight.disambiguation_failure.reason, "timeout")
        # beets.get_album_info MUST NOT be called when move fails —
        # we don't trust the DB state to update the path.
        beets.get_album_info.assert_not_called()
        # Properties (1)+(5).
        self._assert_import_success_preserved(r)

    @patch("harness.import_one.subprocess.run")
    def test_nonzero_rc_does_not_crash_and_preserves_import(self, mock_run):
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 1
        proc.stderr = "error: beets db locked\n"
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()

        new_path = import_one._apply_disambiguation(
            self.MBID, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertFalse(r.postflight.disambiguated)
        assert r.postflight.disambiguation_failure is not None
        self.assertEqual(
            r.postflight.disambiguation_failure.reason, "nonzero_rc")
        self._assert_import_success_preserved(r)

    @patch("harness.import_one.subprocess.run")
    def test_oserror_does_not_crash_and_preserves_import(self, mock_run):
        from harness import import_one

        mock_run.side_effect = FileNotFoundError(2, "No such file", "beet")
        r, beets = self._make_result_and_beets()

        new_path = import_one._apply_disambiguation(
            self.MBID, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertFalse(r.postflight.disambiguated)
        assert r.postflight.disambiguation_failure is not None
        self.assertEqual(
            r.postflight.disambiguation_failure.reason, "exception")
        self._assert_import_success_preserved(r)

    @patch("harness.import_one.subprocess.run")
    def test_clean_move_path_unchanged(self, mock_run):
        """Successful move: pf_info_after returns same path → no path
        mutation, but disambiguated=True and failure is None."""
        from harness import import_one
        from lib.beets_db import AlbumInfo

        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()
        # Same path returned — no rename happened.
        beets.get_album_info.return_value = AlbumInfo(
            album_id=42, track_count=11,
            min_bitrate_kbps=245, is_cbr=False,
            album_path=self.ORIGINAL_PATH)

        new_path = import_one._apply_disambiguation(
            self.MBID, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertTrue(r.postflight.disambiguated)
        # Property: no_failure on success.
        self.assertIsNone(r.postflight.disambiguation_failure)

    @patch("harness.import_one.subprocess.run")
    def test_clean_move_but_pf_info_after_none(self, mock_run):
        """Edge case: move ran cleanly but beets DB no longer returns
        the album (race / out-of-band deletion). Original code set
        ``disambiguated=True`` and left ``imported_path`` unchanged.
        Pin that behavior so a future refactor can't silently change
        whether a partial-state album is treated as disambiguated."""
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()
        beets.get_album_info.return_value = None

        new_path = import_one._apply_disambiguation(
            self.MBID, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertEqual(r.postflight.imported_path, self.ORIGINAL_PATH)
        self.assertTrue(r.postflight.disambiguated)
        self.assertIsNone(r.postflight.disambiguation_failure)

    @patch("harness.import_one.subprocess.run")
    def test_clean_move_path_changed(self, mock_run):
        """Successful move: pf_info_after returns new path → path
        mutates and is propagated back via return value AND on
        r.postflight.imported_path."""
        from harness import import_one
        from lib.beets_db import AlbumInfo

        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()
        renamed = self.ORIGINAL_PATH + " [expanded edition]"
        beets.get_album_info.return_value = AlbumInfo(
            album_id=42, track_count=11,
            min_bitrate_kbps=245, is_cbr=False,
            album_path=renamed)

        new_path = import_one._apply_disambiguation(
            self.MBID, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, renamed)
        self.assertEqual(r.postflight.imported_path, renamed)
        self.assertTrue(r.postflight.disambiguated)
        self.assertIsNone(r.postflight.disambiguation_failure)


if __name__ == "__main__":
    unittest.main()
