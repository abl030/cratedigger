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
from dataclasses import dataclass
from typing import Literal, Optional
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "harness"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


TARGET_MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
SIBLING_MBID = "cccccccc-4444-5555-6666-dddddddddddd"


@dataclass
class _StubLocation:
    kind: Literal["exact", "absent"]
    album_id: Optional[int]
    selectors: tuple[str, ...]


class _StubBeetsDB:
    """Minimal BeetsDB double for capture-only pre-flight tests.

    ``_capture_stale_beets_id`` calls ``get_album_info`` to extract the
    stale row's beets id. Shape: return an ``AlbumInfo`` with the id we
    want to see captured, or None for the "not in beets" path.
    """

    def __init__(self, album_info: object | None) -> None:
        self._album_info = album_info

    def get_album_info(self, mbid: str, cfg: object) -> object | None:
        return self._album_info


def _ok() -> MagicMock:
    return MagicMock(returncode=0, stdout="", stderr="")


class TestCaptureStaleBeetsId(unittest.TestCase):
    """Pre-import capture is non-destructive — it only reads.

    The PR #131 round-1 regression was that the remove ran before the
    import, so a crashed import left the user with no album at all.
    This helper replaces that path: it just reads the stale id, caller
    stores it, and the destructive step runs post-import.
    """

    def test_returns_beets_id_when_album_present(self) -> None:
        from harness import import_one
        from lib.beets_db import AlbumInfo

        beets = _StubBeetsDB(AlbumInfo(
            album_id=10319, track_count=19,
            min_bitrate_kbps=320, is_cbr=True,
            album_path="/Beets/Shearwater/2007 - Palo Santo"))

        result = import_one._capture_stale_beets_id(
            TARGET_MBID, beets)  # type: ignore[arg-type]

        self.assertEqual(result, 10319)

    def test_returns_none_when_absent(self) -> None:
        from harness import import_one
        beets = _StubBeetsDB(None)

        result = import_one._capture_stale_beets_id(
            TARGET_MBID, beets)  # type: ignore[arg-type]

        self.assertIsNone(result)

    def test_returns_none_for_empty_mbid(self) -> None:
        from harness import import_one
        beets = _StubBeetsDB(None)

        result = import_one._capture_stale_beets_id(
            "", beets)  # type: ignore[arg-type]

        self.assertIsNone(result)


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
        # Selector must be id:<int> — primary-key scope, NEVER mb_albumid
        # (which can match multiple rows and would hit siblings).
        self.assertEqual(argv[1:4], ["remove", "-d", "id:10319"])

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
    """Re-running ``beet move`` on sibling MBIDs keeps folder layout symmetric.

    When ``%aunique`` disambiguates an incoming album because a
    different-edition sibling already exists, only the new album gets
    its path re-evaluated at import time. The sibling stays at the
    path it had when it was originally imported — often with no
    suffix because it was alone back then. ``beet move
    mb_albumid:<sibling>`` re-evaluates ``%aunique`` for the sibling
    too, so both editions end up shaped the same way.
    """

    @patch("harness.import_one.subprocess.run")
    def test_noop_when_no_siblings(self, mock_run: MagicMock) -> None:
        from harness import import_one
        import_one._canonicalize_siblings(frozenset())
        mock_run.assert_not_called()

    @patch("harness.import_one.subprocess.run")
    def test_runs_beet_move_for_each_sibling(
            self, mock_run: MagicMock) -> None:
        from harness import import_one
        mock_run.return_value = MagicMock(returncode=0, stderr="")

        import_one._canonicalize_siblings(frozenset([SIBLING_MBID]))

        mock_run.assert_called_once()
        argv = mock_run.call_args.args[0]
        self.assertEqual(argv[1:], ["move", f"mb_albumid:{SIBLING_MBID}"])

    @patch("harness.import_one.subprocess.run")
    def test_continues_past_per_sibling_failure(
            self, mock_run: MagicMock) -> None:
        """Timeout on sibling 1 must not stop sibling 2 moving.

        Import is already on disk — per-sibling failures only affect
        that sibling's cosmetic path. Keep going.
        """
        from harness import import_one
        other = "dddddddd-7777-8888-9999-eeeeeeeeeeee"
        # Sibling 1 times out, sibling 2 exits clean.
        mock_run.side_effect = [
            sp.TimeoutExpired(cmd=["beet", "move"], timeout=120),
            MagicMock(returncode=0, stderr=""),
        ]

        import_one._canonicalize_siblings(
            frozenset([SIBLING_MBID, other]))

        self.assertEqual(mock_run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
