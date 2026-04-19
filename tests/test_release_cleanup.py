"""Tests for ``lib.release_cleanup.remove_and_reset_release``.

Issue #123 PR B: the function's subprocess loop had no
``TimeoutExpired`` or ``OSError`` handling — a timeout on one selector
raised out of the loop, leaving the second selector untried *after*
the ban-source caller had already committed the denylist row. The
hardening replaces the raw ``(bool, bool)`` tuple return with a typed
``ReleaseCleanupResult`` dataclass that surfaces per-selector failures
to the caller, and wraps each ``sp.run`` in a try/except so the loop
always attempts every selector.

The pure-function tests here use a lightweight stub ``BeetsDB`` + a
``MagicMock`` ``PipelineDB`` — the pipeline DB surface this function
touches is exactly one method (``clear_on_disk_quality_fields``), and
the assertion we care about is "was it called, with what argument",
which MagicMock makes direct. Subprocess behavior is mocked via
``patch('lib.release_cleanup.sp.run', ...)``.
"""

from __future__ import annotations

import subprocess as sp
import unittest
from dataclasses import dataclass
from typing import Literal, Optional
from unittest.mock import MagicMock, patch

from lib.release_cleanup import (
    ReleaseCleanupResult,
    SelectorFailure,
    remove_and_reset_release,
)


@dataclass
class _StubLocation:
    kind: Literal["exact", "absent"]
    album_id: Optional[int]
    selectors: tuple[str, ...]


class _StubBeetsDB:
    """Minimal BeetsDB double: returns scripted ``locate`` results.

    ``remove_and_reset_release`` calls ``locate`` exactly twice (before
    and after the subprocess loop). Tests enqueue the sequence and
    assert the transitions. Production callers route through the real
    ``BeetsDB`` which is covered elsewhere (tests/test_beets_db.py).
    """

    def __init__(self, sequence: list[_StubLocation]) -> None:
        self._sequence = list(sequence)
        self.calls: list[str] = []

    def locate(self, release_id: str) -> _StubLocation:
        self.calls.append(release_id)
        return self._sequence.pop(0)


RELEASE_UUID = "aaa0bbb0-cccc-dddd-eeee-ffffffffffff"
DISCOGS_ID = "12856590"


def _ok(stdout: str = "", stderr: str = "") -> MagicMock:
    return MagicMock(returncode=0, stdout=stdout, stderr=stderr)


class TestReleaseCleanupResult(unittest.TestCase):
    """The new typed return contract."""

    def test_result_dataclass_fields(self) -> None:
        """``ReleaseCleanupResult`` exposes beets_removed, absent_after,
        selector_failures — no raw tuples allowed across the seam."""
        r = ReleaseCleanupResult(
            beets_removed=True,
            absent_after=True,
            selector_failures=(),
        )
        self.assertTrue(r.beets_removed)
        self.assertTrue(r.absent_after)
        self.assertEqual(r.selector_failures, ())

    def test_result_is_frozen(self) -> None:
        """The result is immutable — callers must construct, not mutate."""
        r = ReleaseCleanupResult(
            beets_removed=True, absent_after=True, selector_failures=())
        with self.assertRaises(Exception):
            # dataclasses.FrozenInstanceError subclasses AttributeError
            r.beets_removed = False  # type: ignore[misc]

    def test_selector_failure_dataclass_fields(self) -> None:
        """``SelectorFailure`` records which selector, why, and detail."""
        f = SelectorFailure(
            selector="mb_albumid:abc",
            reason="timeout",
            detail="beet remove timed out after 30s",
        )
        self.assertEqual(f.selector, "mb_albumid:abc")
        self.assertEqual(f.reason, "timeout")
        self.assertEqual(f.detail, "beet remove timed out after 30s")


class TestAllSelectorsSucceed(unittest.TestCase):
    """Baseline: when every selector exits 0, no failures, album gone."""

    @patch("lib.release_cleanup.sp.run")
    def test_uuid_single_selector_clean_exit(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _ok()
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, (f"mb_albumid:{RELEASE_UUID}",)),
            _StubLocation("absent", None, ()),
        ])
        pdb = MagicMock()

        result = remove_and_reset_release(
            beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
            release_id=RELEASE_UUID, request_id=42)

        self.assertIsInstance(result, ReleaseCleanupResult)
        self.assertTrue(result.beets_removed)
        self.assertTrue(result.absent_after)
        self.assertEqual(result.selector_failures, ())
        self.assertEqual(mock_run.call_count, 1)
        # Pipeline DB clear fires on absent_after=True.
        pdb.clear_on_disk_quality_fields.assert_called_once_with(42)

    @patch("lib.release_cleanup.sp.run")
    def test_discogs_pair_of_selectors_both_run(
            self, mock_run: MagicMock) -> None:
        """Discogs numeric → two selectors; both run on the happy path."""
        mock_run.return_value = _ok()
        selectors = (f"discogs_albumid:{DISCOGS_ID}", f"mb_albumid:{DISCOGS_ID}")
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, selectors),
            _StubLocation("absent", None, ()),
        ])
        pdb = MagicMock()

        result = remove_and_reset_release(
            beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
            release_id=DISCOGS_ID, request_id=42)

        self.assertEqual(mock_run.call_count, 2,
                         "Discogs selectors must both run on happy path.")
        self.assertTrue(result.absent_after)
        self.assertEqual(result.selector_failures, ())


class TestTimeoutOnOneSelector(unittest.TestCase):
    """The bug report: timeout on selector A must not abort the loop."""

    @patch("lib.release_cleanup.sp.run")
    def test_timeout_on_first_selector_still_runs_second(
            self, mock_run: MagicMock) -> None:
        """``TimeoutExpired`` on selector 1 must not prevent selector 2.

        Before the fix, ``TimeoutExpired`` escaped the loop and the
        ban-source caller saw a 500 after the denylist row was already
        committed. The second selector (legacy ``mb_albumid`` for a
        Discogs-layout album) never ran, so the banned copy stayed on
        disk.
        """
        selectors = (f"discogs_albumid:{DISCOGS_ID}", f"mb_albumid:{DISCOGS_ID}")
        # Selector 1: timeout. Selector 2: clean exit.
        mock_run.side_effect = [
            sp.TimeoutExpired(cmd=["beet", "remove", "-d", selectors[0]],
                              timeout=30),
            _ok(),
        ]
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, selectors),
            _StubLocation("absent", None, ()),
        ])
        pdb = MagicMock()

        result = remove_and_reset_release(
            beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
            release_id=DISCOGS_ID, request_id=42)

        self.assertEqual(
            mock_run.call_count, 2,
            "Timeout on selector 1 must not skip selector 2.")
        self.assertTrue(result.absent_after,
                        "Second selector's success still leaves album gone.")
        self.assertEqual(len(result.selector_failures), 1)
        self.assertEqual(result.selector_failures[0].selector, selectors[0])
        self.assertEqual(result.selector_failures[0].reason, "timeout")

    @patch("lib.release_cleanup.sp.run")
    def test_timeout_on_both_selectors_returns_partial_failure(
            self, mock_run: MagicMock) -> None:
        """All selectors time out → two failures recorded, no clear."""
        selectors = (f"discogs_albumid:{DISCOGS_ID}", f"mb_albumid:{DISCOGS_ID}")
        mock_run.side_effect = [
            sp.TimeoutExpired(cmd=["beet", "remove", "-d", selectors[0]],
                              timeout=30),
            sp.TimeoutExpired(cmd=["beet", "remove", "-d", selectors[1]],
                              timeout=30),
        ]
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, selectors),
            _StubLocation("exact", 1, selectors),  # still present after
        ])
        pdb = MagicMock()

        result = remove_and_reset_release(
            beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
            release_id=DISCOGS_ID, request_id=42)

        self.assertEqual(
            mock_run.call_count, 2,
            "Even after selector 1 times out, selector 2 must be attempted.")
        self.assertFalse(result.absent_after)
        self.assertFalse(result.beets_removed)
        self.assertEqual(len(result.selector_failures), 2)
        # No DB clear when album still present — conservative.
        pdb.clear_on_disk_quality_fields.assert_not_called()


class TestNonZeroExitCodeLoopContinues(unittest.TestCase):
    """``beet remove`` exits non-zero → record, keep looping."""

    @patch("lib.release_cleanup.sp.run")
    def test_nonzero_rc_on_first_still_runs_second(
            self, mock_run: MagicMock) -> None:
        selectors = (f"discogs_albumid:{DISCOGS_ID}", f"mb_albumid:{DISCOGS_ID}")
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="permission denied"),
            _ok(),
        ]
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, selectors),
            _StubLocation("absent", None, ()),
        ])
        pdb = MagicMock()

        result = remove_and_reset_release(
            beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
            release_id=DISCOGS_ID, request_id=42)

        self.assertEqual(mock_run.call_count, 2)
        self.assertEqual(len(result.selector_failures), 1)
        self.assertEqual(result.selector_failures[0].reason, "nonzero_rc")
        # Second selector cleared it → absent_after True.
        self.assertTrue(result.absent_after)


class TestMissingBeetBinary(unittest.TestCase):
    """``FileNotFoundError`` (beet not on PATH) is caught gracefully."""

    @patch("lib.release_cleanup.sp.run")
    def test_filenotfounderror_does_not_propagate(
            self, mock_run: MagicMock) -> None:
        """Beet missing from PATH must not crash the ban-source handler.

        The denylist row is already committed by the caller before
        ``remove_and_reset_release`` is invoked — an uncaught exception
        here leaves the caller's state inconsistent with a surfaced 500.
        """
        selectors = (f"mb_albumid:{RELEASE_UUID}",)
        mock_run.side_effect = FileNotFoundError(
            2, "No such file or directory", "beet")
        beets = _StubBeetsDB([
            _StubLocation("exact", 1, selectors),
            _StubLocation("exact", 1, selectors),
        ])
        pdb = MagicMock()

        # Must not raise — the caller expects a result object it can
        # surface to the user, not a 500.
        result = remove_and_reset_release(
            beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
            release_id=RELEASE_UUID, request_id=42)

        self.assertEqual(len(result.selector_failures), 1)
        self.assertEqual(result.selector_failures[0].reason, "exception")
        self.assertFalse(result.absent_after)
        # Conservative: album still present by locate() so no clear.
        pdb.clear_on_disk_quality_fields.assert_not_called()


class TestAlreadyGoneBeforeCall(unittest.TestCase):
    """Pre-gone: no subprocess runs, pipeline DB still cleared."""

    @patch("lib.release_cleanup.sp.run")
    def test_no_sp_run_when_locate_already_absent(
            self, mock_run: MagicMock) -> None:
        beets = _StubBeetsDB([
            _StubLocation("absent", None, ()),
            _StubLocation("absent", None, ()),
        ])
        pdb = MagicMock()

        result = remove_and_reset_release(
            beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
            release_id=RELEASE_UUID, request_id=42)

        mock_run.assert_not_called()
        self.assertFalse(result.beets_removed)
        self.assertTrue(result.absent_after)
        self.assertEqual(result.selector_failures, ())
        pdb.clear_on_disk_quality_fields.assert_called_once_with(42)


class TestEmptyReleaseIdRejected(unittest.TestCase):
    """Empty release_id is a caller bug — keep the ValueError contract."""

    def test_empty_release_id_raises(self) -> None:
        beets = _StubBeetsDB([])
        pdb = MagicMock()
        with self.assertRaises(ValueError):
            remove_and_reset_release(
                beets_db=beets, pipeline_db=pdb,  # type: ignore[arg-type]
                release_id="", request_id=42)


if __name__ == "__main__":
    unittest.main()
