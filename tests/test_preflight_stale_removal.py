"""Tests for pre-flight same-MBID removal in import_one.py.

Bug being locked in (live 2026-04-20): when an upgrade re-import runs
against an album already in beets, the stale same-MBID entry must be
removed via a *targeted* ``beet remove -d mb_albumid:<mbid>`` BEFORE
the beets import harness starts — not mid-import via beets' own
``remove_duplicates()``, which has cross-MBID blast radius and wiped
the 11-track Palo Santo sibling in production.

The seam these tests pin: ``_preflight_remove_stale_mbid(mbid, beets)``
in ``harness/import_one.py``. It delegates to
``lib.release_cleanup.remove_album_by_selectors`` (the pure-beets
primitive — no PipelineDB coupling, because the harness subprocess has
no PipelineDB on hand).

Contract:
- If the album IS in beets → returns a non-None ReleaseCleanupResult
  with absent_after reflecting whether removal succeeded. Subprocess
  ran at least once.
- If the album is NOT in beets → returns None (no work to do). No
  subprocess runs.
- Empty mbid → returns None (preflight is opt-in; callers that
  already know the album isn't present shouldn't need to gate).
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


@dataclass
class _StubLocation:
    kind: Literal["exact", "absent"]
    album_id: Optional[int]
    selectors: tuple[str, ...]


class _StubBeetsDB:
    """Minimal BeetsDB double. Same shape as tests/test_release_cleanup.py."""

    def __init__(self, sequence: list[_StubLocation]) -> None:
        self._sequence = list(sequence)
        self.locate_calls: list[str] = []

    def locate(self, release_id: str) -> _StubLocation:
        self.locate_calls.append(release_id)
        return self._sequence.pop(0)

    def album_exists(self, mbid: str) -> bool:
        """Harness pre-flight uses album_exists for the initial check.

        The stub mirrors locate() — one entry consumed per call.
        """
        return self._sequence[0].kind == "exact" if self._sequence else False


def _ok() -> MagicMock:
    return MagicMock(returncode=0, stdout="", stderr="")


class TestPreflightRemoveStaleMBID(unittest.TestCase):
    """The pre-flight helper is the seam that makes the Palo Santo fix work.

    By the time ``run_import`` starts, the stale same-MBID album must
    already be gone — otherwise beets' ``find_duplicates()`` drags it
    into the resolve_duplicate callback and we're back to answering
    ``"keep"`` on a dup list that may include cross-MBID siblings.
    (Keep is safe, but cleaner to not even have the stale entry
    present.)
    """

    @patch("lib.release_cleanup.sp.run")
    def test_removes_when_album_in_beets(self, mock_run: MagicMock) -> None:
        """Album present → targeted beet remove runs, result returned."""
        from harness import import_one
        mock_run.return_value = _ok()
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, (f"mb_albumid:{TARGET_MBID}",)),
            _StubLocation("absent", None, ()),
        ])

        result = import_one._preflight_remove_stale_mbid(
            TARGET_MBID, beets)  # type: ignore[arg-type]

        self.assertIsNotNone(result)
        assert result is not None  # narrow for pyright
        self.assertTrue(result.beets_removed)
        self.assertTrue(result.absent_after)
        # The subprocess MUST use an MBID-scoped selector.
        mock_run.assert_called_once()
        argv = mock_run.call_args.args[0]
        self.assertEqual(argv[0:3], ["beet", "remove", "-d"])
        self.assertEqual(argv[3], f"mb_albumid:{TARGET_MBID}")

    @patch("lib.release_cleanup.sp.run")
    def test_no_op_when_album_absent(self, mock_run: MagicMock) -> None:
        """Album not in beets → returns None, no subprocess, no churn."""
        from harness import import_one
        beets = _StubBeetsDB([
            _StubLocation("absent", None, ()),
        ])

        result = import_one._preflight_remove_stale_mbid(
            TARGET_MBID, beets)  # type: ignore[arg-type]

        self.assertIsNone(result)
        mock_run.assert_not_called()

    @patch("lib.release_cleanup.sp.run")
    def test_empty_mbid_returns_none(self, mock_run: MagicMock) -> None:
        """Empty MBID is a no-op (caller guards)."""
        from harness import import_one
        beets = _StubBeetsDB([])

        result = import_one._preflight_remove_stale_mbid(
            "", beets)  # type: ignore[arg-type]

        self.assertIsNone(result)
        mock_run.assert_not_called()

    @patch("lib.release_cleanup.sp.run")
    def test_surfaces_partial_failure(self, mock_run: MagicMock) -> None:
        """Timeout leaves the album on disk → result reflects it.

        Caller (main) must be able to see absent_after=False and decide
        whether to abort the import rather than blunder on into a
        beets ``remove_duplicates`` → data-loss scenario.
        """
        from harness import import_one
        mock_run.side_effect = sp.TimeoutExpired(
            cmd=["beet", "remove", "-d", f"mb_albumid:{TARGET_MBID}"],
            timeout=30)
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, (f"mb_albumid:{TARGET_MBID}",)),
            _StubLocation("exact", 1, (f"mb_albumid:{TARGET_MBID}",)),
        ])

        result = import_one._preflight_remove_stale_mbid(
            TARGET_MBID, beets)  # type: ignore[arg-type]

        self.assertIsNotNone(result)
        assert result is not None
        self.assertFalse(result.absent_after)
        self.assertEqual(len(result.selector_failures), 1)
        self.assertEqual(result.selector_failures[0].reason, "timeout")


if __name__ == "__main__":
    unittest.main()
