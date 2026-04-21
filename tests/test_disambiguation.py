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
from unittest.mock import patch, MagicMock

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

        rc, beets_lines, kept_duplicate, sibling_mbids = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 0)
        self.assertTrue(kept_duplicate)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_same_mbid_still_keeps_never_removes(self, mock_popen, mock_select):
        """Same-MBID in dup_mbids must NOT trigger a 'remove' response.

        Regression guard for the Palo Santo data-loss bug (this PR): when
        beets' ``find_duplicates()`` returns both the same-MBID stale
        entry AND a cross-MBID sibling pressing, answering ``"remove"``
        causes beets' ``remove_duplicates()`` to wipe every item in
        every found duplicate — including the cross-MBID sibling's files
        on disk. The harness must never send ``"remove"``; stale same-
        MBID removal happens in pre-flight via targeted
        ``beet remove -d mb_albumid:<mbid>`` before this subprocess
        starts. ``kept_duplicate`` stays True so post-import
        disambiguation re-runs ``beet move`` to fix %aunique paths.
        """
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

        rc, beets_lines, kept_duplicate, sibling_mbids = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 0)
        self.assertTrue(kept_duplicate,
                        "Any resolve_duplicate firing must set kept_duplicate "
                        "True so post-import beet move re-runs.")
        # No stdin payload may carry action:"remove" — the destructive
        # branch is gone entirely.
        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn('"remove"', writes,
                         "Harness must never answer action:'remove' to "
                         "resolve_duplicate — beets' remove_duplicates() has "
                         "cross-MBID blast radius.")

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

        rc, beets_lines, kept_duplicate, sibling_mbids = import_one.run_import(
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

        rc, beets_lines, kept_duplicate, sibling_mbids = import_one.run_import(
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

        rc, beets_lines, kept_duplicate, sibling_mbids = import_one.run_import(
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

        rc, beets_lines, kept_duplicate, sibling_mbids = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 2)
        self.assertFalse(kept_duplicate)
        self.assertIn("readonly database", "\n".join(beets_lines))


class TestHarnessNeverSendsRemoveToBeets(unittest.TestCase):
    """Invariant: ``run_import`` must NEVER answer ``action: "remove"``.

    Beets' ``remove_duplicates()`` (``beets/importer/tasks.py``) deletes
    every item in every album returned by ``find_duplicates()`` — the
    call cannot be scoped to one MBID from the harness side.

    In the live 2026-04-20 Palo Santo data-loss event, ``find_duplicates()``
    returned a cross-MBID sibling because the user's ``duplicate_keys``
    block was at the TOP level of ``~/.config/beets/config.yaml`` instead
    of under ``import:``. Beets reads strictly from
    ``config["import"]["duplicate_keys"]["album"]`` (traced 2026-04-21 via
    ``beets/importer/tasks.py:385``); the top-level key was silently
    ignored and beets used the default ``[albumartist, album]``. Sibling
    pressings matched on title alone. Fixed in ``beets.nix`` + startup
    assertion in ``harness/beets_harness.py::_assert_duplicate_keys_include_mb_albumid``.

    This invariant remains as defense-in-depth: even if the config
    regresses, ``resolve_duplicate`` only ever answers ``"keep"`` (sibling
    preservation). Stale same-MBID removal happens in pre-flight via a
    targeted ``beet remove -d mb_albumid:<mbid>`` that only matches that
    one album.

    Every sub-test asserts the same thing — ``"remove"`` never crosses
    the wire — under different dup_mbids shapes so a future accidental
    re-introduction of the branch fails here first.
    """

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_same_mbid_only(self, mock_popen, mock_select):
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_mbids": [TARGET_MBID]},
            {"type": "choose_match", "candidates": [
                {"album_id": TARGET_MBID, "distance": 0.05,
                 "artist": "X", "album": "Y"}]},
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        import_one.run_import("/tmp/test", TARGET_MBID)

        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn('"remove"', writes)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_different_mbid_only(self, mock_popen, mock_select):
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_mbids": [OTHER_MBID]},
            {"type": "choose_match", "candidates": [
                {"album_id": TARGET_MBID, "distance": 0.05,
                 "artist": "X", "album": "Y"}]},
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        import_one.run_import("/tmp/test", TARGET_MBID)

        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn('"remove"', writes)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_palo_santo_mixed_dup_mbids_preserves_sibling(
            self, mock_popen, mock_select):
        """Live-log reproduction: dup_mbids contains BOTH the target MBID
        AND a sibling pressing's MBID. Before the fix this took the
        ``"remove"`` branch and beets wiped both albums' files on disk.
        After the fix the harness answers ``"keep"`` so beets imports
        the new album alongside; targeted pre-flight removal (run
        separately upstream of ``run_import``) handles the stale
        same-MBID entry without touching the sibling.
        """
        from harness import import_one

        messages = [
            # The 2026-04-20 live event: both the incoming 19-track
            # (target) MBID and the 11-track sibling's MBID showed up
            # as duplicates.
            {"type": "resolve_duplicate",
             "duplicate_mbids": [TARGET_MBID, OTHER_MBID]},
            {"type": "choose_match", "candidates": [
                {"album_id": TARGET_MBID, "distance": 0.05,
                 "artist": "X", "album": "Y"}]},
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        rc, _, kept_duplicate, _siblings = import_one.run_import(
            "/tmp/test", TARGET_MBID)

        self.assertEqual(rc, 0)
        self.assertTrue(
            kept_duplicate,
            "Even in the mixed case, kept_duplicate must be True — "
            "post-import beet move still needs to re-run %aunique.")
        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn(
            '"remove"', writes,
            "The Palo Santo bug: mixed dup_mbids must NEVER trigger "
            "action:'remove' (blast-radius includes the sibling).")


# ``TestDisambiguateBeetMove`` and ``TestRunDisambiguationMoveHelper``
# were removed in issue #133.
#
# - ``_run_disambiguation_move(mbid)`` was the mb_albumid-based helper
#   superseded by ``_run_album_move_by_id(album_id)`` in PR #131, which
#   in turn is now ``lib.beets_album_op.move_album``. The argv-shape +
#   subprocess-failure-classification coverage that ``_run_disambiguation_move``
#   carried lives in ``tests/test_beets_album_op.py::TestMoveAlbum`` for
#   the id-based move shape. NOTE: no test explicitly covers a
#   ``beet move mb_albumid:<uuid>`` argv any more — production doesn't
#   construct that shape (PR #131 moved every caller to the id-based
#   form), and the grep guard in ``TestBeetOpArgvIsCentralised`` now
#   forbids new callsites from reintroducing it outside the op module.
# - ``TestDisambiguateBeetMove`` reconstructed the disambiguation
#   control flow inline inside the test (never called the production
#   helper), so it guarded nothing. Equivalent coverage of the real
#   path lives in ``TestApplyDisambiguationCallsiteContract`` below,
#   which calls ``import_one._apply_disambiguation`` directly.


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

    @patch("lib.beets_album_op.sp.run")
    def test_timeout_does_not_crash_and_preserves_import(self, mock_run):
        from harness import import_one

        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=["beet", "move"], timeout=120)
        r, beets = self._make_result_and_beets()

        # Must NOT raise.
        new_path = import_one._apply_disambiguation(
            42, beets, self.ORIGINAL_PATH, r)

        # Property (4): path unchanged on failure.
        self.assertEqual(new_path, self.ORIGINAL_PATH)
        # Property (3): not lying about disambiguation.
        self.assertFalse(r.postflight.disambiguated)
        # Property (2): typed failure recorded with correct reason tag.
        self.assertIsNotNone(r.postflight.disambiguation_failure)
        assert r.postflight.disambiguation_failure is not None
        self.assertEqual(
            r.postflight.disambiguation_failure.reason, "timeout")
        # ``get_album_path_by_id`` MUST NOT be called when move fails —
        # we don't trust the DB state to update the path.
        beets.get_album_path_by_id.assert_not_called()
        # Properties (1)+(5).
        self._assert_import_success_preserved(r)

    @patch("lib.beets_album_op.sp.run")
    def test_nonzero_rc_does_not_crash_and_preserves_import(self, mock_run):
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 1
        proc.stderr = "error: beets db locked\n"
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()

        new_path = import_one._apply_disambiguation(
            42, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertFalse(r.postflight.disambiguated)
        assert r.postflight.disambiguation_failure is not None
        self.assertEqual(
            r.postflight.disambiguation_failure.reason, "nonzero_rc")
        self._assert_import_success_preserved(r)

    @patch("lib.beets_album_op.sp.run")
    def test_oserror_does_not_crash_and_preserves_import(self, mock_run):
        from harness import import_one

        mock_run.side_effect = FileNotFoundError(2, "No such file", "beet")
        r, beets = self._make_result_and_beets()

        new_path = import_one._apply_disambiguation(
            42, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertFalse(r.postflight.disambiguated)
        assert r.postflight.disambiguation_failure is not None
        self.assertEqual(
            r.postflight.disambiguation_failure.reason, "exception")
        self._assert_import_success_preserved(r)

    @patch("lib.permissions.fix_library_modes")
    @patch("lib.beets_album_op.sp.run")
    def test_clean_move_path_unchanged(self, mock_run, _mock_fix):
        """Successful move: post-move path equals original → no path
        mutation, but disambiguated=True and failure is None.

        Mocks ``fix_library_modes`` since ``move_album`` calls it on
        every successful move (issue #84); the test doesn't care about
        perm repair — only about ``_apply_disambiguation``'s contract.
        """
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()
        # Same path returned — no rename happened.
        beets.get_album_path_by_id.return_value = self.ORIGINAL_PATH

        new_path = import_one._apply_disambiguation(
            42, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertTrue(r.postflight.disambiguated)
        # Property: no_failure on success.
        self.assertIsNone(r.postflight.disambiguation_failure)

    @patch("lib.permissions.fix_library_modes")
    @patch("lib.beets_album_op.sp.run")
    def test_clean_move_but_pf_info_after_none(self, mock_run, _mock_fix):
        """Edge case: move ran cleanly but beets DB no longer returns
        the album's path (race / out-of-band deletion). Original code
        set ``disambiguated=True`` and left ``imported_path`` unchanged.
        Pin that behavior so a future refactor can't silently change
        whether a partial-state album is treated as disambiguated."""
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()
        beets.get_album_path_by_id.return_value = None

        new_path = import_one._apply_disambiguation(
            42, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, self.ORIGINAL_PATH)
        self.assertEqual(r.postflight.imported_path, self.ORIGINAL_PATH)
        self.assertTrue(r.postflight.disambiguated)
        self.assertIsNone(r.postflight.disambiguation_failure)

    @patch("lib.permissions.fix_library_modes")
    @patch("lib.beets_album_op.sp.run")
    def test_clean_move_path_changed(self, mock_run, _mock_fix):
        """Successful move: post-move path differs → path mutates and
        propagates via return value AND on r.postflight.imported_path."""
        from harness import import_one

        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        mock_run.return_value = proc
        r, beets = self._make_result_and_beets()
        renamed = self.ORIGINAL_PATH + " [expanded edition]"
        beets.get_album_path_by_id.return_value = renamed

        new_path = import_one._apply_disambiguation(
            42, beets, self.ORIGINAL_PATH, r)

        self.assertEqual(new_path, renamed)
        self.assertEqual(r.postflight.imported_path, renamed)
        self.assertTrue(r.postflight.disambiguated)
        self.assertIsNone(r.postflight.disambiguation_failure)


if __name__ == "__main__":
    unittest.main()
