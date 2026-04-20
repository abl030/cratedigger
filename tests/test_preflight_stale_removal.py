"""Tests for same-MBID stale-entry handling in import_one.py.

Bug being locked in (live 2026-04-20): when an upgrade re-import runs
against an album already in beets, we must remove the stale same-MBID
row with a selector that CANNOT reach sibling pressings. The narrowest
such selector is the beets numeric primary key (``id:<N>``).

Round 2 design (post PR #131 Codex P1): the stale removal runs AFTER
the new album is successfully in beets, not before. A pre-flight
remove could leave the user with no files at all if the harness times
out / crashes. The capture-then-import-then-remove shape keeps the
existing copy alive until the replacement is confirmed.

Seams pinned here:

- ``_capture_stale_beets_id(mbid, beets)``
  Pre-import, no destruction: returns the stale row's beets id (if
  present) or None. Caller stashes it until post-import cleanup.

- ``_remove_stale_by_id_logged(stale_id)``
  Post-import, destructive: runs ``beet remove -d id:<N>`` via
  ``lib.release_cleanup.remove_album_by_beets_id``. ``id:<N>`` is a
  SQLite primary-key selector — it cannot match any other row, by
  construction. Logs the outcome so the import audit trail shows
  exactly which beets row was removed.

- ``_canonicalize_siblings(sibling_mbids)``
  Post-import, non-destructive: re-runs ``beet move`` on each
  different-edition sibling so ``%aunique`` re-evaluates their paths
  too. Keeps folder layout symmetric when two pressings of the same
  album name co-exist.
"""

from __future__ import annotations

import os
import subprocess as sp
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "harness"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


TARGET_MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"


def _ok() -> MagicMock:
    return MagicMock(returncode=0, stdout="", stderr="")


class TestRemoveStaleByIdLogged(unittest.TestCase):
    """Post-import cleanup: ``beet remove -d id:<N>`` via the release_cleanup
    primitive. The ``id:<N>`` selector is the beets numeric PK — it
    cannot match any other album, so the blast radius is exactly one
    row."""

    @patch("lib.release_cleanup.sp.run")
    def test_clean_exit_returns_none(self, mock_run: MagicMock) -> None:
        from harness import import_one
        mock_run.return_value = _ok()

        result = import_one._remove_stale_by_id_logged(10319)

        self.assertIsNone(result)
        mock_run.assert_called_once()
        argv = mock_run.call_args.args[0]
        # Selector must be ``-a -d id:<int>`` — album mode (``-a``),
        # delete files (``-d``), primary-key scope (``id:<N>`` against
        # ``albums.id``, unique by SQLite auto-increment). Without
        # ``-a`` the selector would be interpreted against ``items``
        # and match a track PK or nothing (Codex PR #131 round 2 P1).
        self.assertEqual(argv[1:5], ["remove", "-a", "-d", "id:10319"])

    @patch("lib.release_cleanup.sp.run")
    def test_timeout_surfaces_typed_failure(self, mock_run: MagicMock) -> None:
        from harness import import_one
        mock_run.side_effect = sp.TimeoutExpired(
            cmd=["beet", "remove", "-d", "id:10319"], timeout=30)

        result = import_one._remove_stale_by_id_logged(10319)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.reason, "timeout")

    @patch("lib.release_cleanup.sp.run")
    def test_nonzero_rc_surfaces_typed_failure(
            self, mock_run: MagicMock) -> None:
        from harness import import_one
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="some error")

        result = import_one._remove_stale_by_id_logged(10319)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.reason, "nonzero_rc")


class TestCanonicalizeSiblings(unittest.TestCase):
    """Re-running ``beet move`` on sibling albums keeps folder layout symmetric.

    When ``%aunique`` disambiguates an incoming album because a
    different-edition sibling already exists, only the new album gets
    its path re-evaluated at import time. The sibling stays at the
    path it had when it was originally imported — often with no
    suffix because it was alone back then. ``beet move -a id:<N>`` on
    each sibling re-evaluates ``%aunique`` for its path too, so both
    editions end up shaped the same way.

    Keyed by beets numeric album id (not MBID) so Discogs-sourced
    siblings are covered: their ``mb_albumid`` is empty but
    ``albums.id`` is always populated (Codex PR #131 round 3 P3).
    """

    @patch("harness.import_one.subprocess.run")
    def test_noop_when_no_siblings(self, mock_run: MagicMock) -> None:
        from harness import import_one
        import_one._canonicalize_siblings(frozenset([]))
        mock_run.assert_not_called()

    @patch("harness.import_one.subprocess.run")
    def test_runs_beet_move_album_mode_id_selector(
            self, mock_run: MagicMock) -> None:
        """Each sibling move MUST use ``beet move -a id:<N>``.

        ``-a`` puts beets in album mode so ``id:<N>`` matches
        ``albums.id`` (unique PK). Without it, ``id:<N>`` would hit
        ``items.id`` (a separate auto-increment namespace) and move
        a single track instead of the sibling album.
        """
        from harness import import_one
        mock_run.return_value = MagicMock(returncode=0, stderr="")

        import_one._canonicalize_siblings(frozenset([10314]))

        mock_run.assert_called_once()
        argv = mock_run.call_args.args[0]
        self.assertEqual(argv[1:], ["move", "-a", "id:10314"])

    @patch("harness.import_one.subprocess.run")
    def test_handles_discogs_sibling_via_album_id(
            self, mock_run: MagicMock) -> None:
        """Regression for Codex PR #131 round 3 P3.

        Pre-fix, siblings were identified by ``mb_albumid`` — empty
        string for Discogs-sourced pressings, so they silently got
        dropped by the ``if dm`` filter in run_import. Switching the
        collection to ``album_ids`` (PK, always populated) covers
        both sources. The set here carries only an integer — no MBID
        needed — to prove the helper no longer requires it.
        """
        from harness import import_one
        mock_run.return_value = MagicMock(returncode=0, stderr="")

        import_one._canonicalize_siblings(frozenset([12856590]))

        mock_run.assert_called_once()
        argv = mock_run.call_args.args[0]
        self.assertEqual(argv[1:], ["move", "-a", "id:12856590"])

    @patch("harness.import_one.subprocess.run")
    def test_continues_past_per_sibling_failure(
            self, mock_run: MagicMock) -> None:
        """Timeout on sibling 1 must not stop sibling 2 moving.

        Import is already on disk — per-sibling failures only affect
        that sibling's cosmetic path. Keep going.
        """
        from harness import import_one
        # Sibling 1 times out, sibling 2 exits clean.
        mock_run.side_effect = [
            sp.TimeoutExpired(cmd=["beet", "move"], timeout=120),
            MagicMock(returncode=0, stderr=""),
        ]

        import_one._canonicalize_siblings(frozenset([10314, 10315]))

        self.assertEqual(mock_run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
