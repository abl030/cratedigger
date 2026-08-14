"""Deterministic tests for the daily retag-divergence census oneshot
(#1142).

Mirrors ``tests/test_run_unfindable_detection.py``'s shape: ``main()``
wires real runtime config / Beets admission — not worth mocking
end-to-end for a process exit-code contract. ``run_retag_divergence_census``
and ``publish_retag_divergence_census`` are the extracted, directly
testable seams driven here against ``FakeBeetsDB`` and a real temp
directory (so the atomic-publish acceptance criterion is proven against
a real filesystem, not a mock).
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from lib.beets_db import BeetsAlbumIdentityRow
from lib.retag_divergence_census_snapshot import (
    read_retag_divergence_census_snapshot,
    write_retag_divergence_census_snapshot,
)
from scripts.run_retag_divergence_census import (
    EXIT_BEETS_UNAVAILABLE,
    publish_retag_divergence_census,
    run_retag_divergence_census,
)
from tests.fakes import FakeBeetsDB
from tests.test_retag_divergence_census_snapshot import (
    RetagDivergenceCensusSnapshot,
    _report,
)


class TestRunRetagDivergenceCensus(unittest.TestCase):
    def test_wraps_the_report_with_timestamp_and_duration(self) -> None:
        # An empty library (no album rows at all) is the one genuinely
        # "clean" shape — a zero-item ALBUM row is real but classifies
        # "empty" and would report "incomplete" instead (see
        # ``scan_retag_divergence``'s own status precedence).
        beets = FakeBeetsDB()
        times = iter([100.0, 100.4])

        snapshot = run_retag_divergence_census(
            lambda: beets,
            time_fn=lambda: next(times),
            now_fn=lambda: "2026-08-14T09:00:00+00:00",
        )

        self.assertEqual(snapshot.generated_at, "2026-08-14T09:00:00+00:00")
        self.assertAlmostEqual(snapshot.duration_seconds, 0.4)
        self.assertEqual(snapshot.report.status, "clean")

    def test_unexpected_scan_failure_propagates(self) -> None:
        class BrokenQueryBeets(FakeBeetsDB):
            def list_album_mb_identities(self) -> list[BeetsAlbumIdentityRow]:
                raise RuntimeError("programmer defect")

        beets = BrokenQueryBeets()

        with self.assertRaisesRegex(RuntimeError, "programmer defect"):
            run_retag_divergence_census(lambda: beets)


class TestPublishRetagDivergenceCensus(unittest.TestCase):
    def test_publishes_a_readable_snapshot(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(album_id=1, mb_albumid="", item_paths=()),
        ])

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "retag-divergence-census.json")

            snapshot = publish_retag_divergence_census(path, lambda: beets)
            read_back = read_retag_divergence_census_snapshot(path)

        self.assertEqual(read_back, snapshot)

    def test_beets_unavailable_does_not_publish_and_preserves_the_prior_snapshot(
        self,
    ) -> None:
        """B1 (#1142 review) — a run that can't even reach Beets must NOT
        overwrite the last real answer with a fabricated all-zero
        "clean"-looking report: the returned ``SingleAlbumRetagCheckResult``
        counts are all zero for ``beets_unavailable`` (the scan never
        actually ran), and publishing that would make the dashboard read
        a stuck/misconfigured Beets authority as "0 albums scanned,
        nothing wrong" instead of showing the operator yesterday's real
        answer."""
        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_LOCKED

        def unavailable_factory() -> FakeBeetsDB:
            raise failure

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "retag-divergence-census.json")
            prior = RetagDivergenceCensusSnapshot(
                generated_at="2026-08-13T09:00:00+00:00",
                duration_seconds=190.0,
                report=_report("clean"),
            )
            write_retag_divergence_census_snapshot(path, prior)

            snapshot = publish_retag_divergence_census(
                path, unavailable_factory,
            )
            read_back = read_retag_divergence_census_snapshot(path)

        self.assertEqual(snapshot.report.status, "beets_unavailable")
        # The RETURNED snapshot still carries the unavailable report (so
        # main() can log it) — but the PUBLISHED file is untouched.
        self.assertEqual(read_back, prior)

    def test_beets_unavailable_with_no_prior_snapshot_publishes_nothing(
        self,
    ) -> None:
        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_LOCKED

        def unavailable_factory() -> FakeBeetsDB:
            raise failure

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "retag-divergence-census.json")

            publish_retag_divergence_census(path, unavailable_factory)

            self.assertIsNone(read_retag_divergence_census_snapshot(path))

    def test_unexpected_failure_never_touches_a_prior_snapshot(self) -> None:
        """Acceptance criterion 1, proven at this module's own boundary
        (the snapshot-level atomicity test lives in
        ``tests/test_retag_divergence_census_snapshot.py``; this pins the
        composed run+publish contract the daily oneshot actually calls)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "retag-divergence-census.json")
            prior = RetagDivergenceCensusSnapshot(
                generated_at="2026-08-13T09:00:00+00:00",
                duration_seconds=190.0,
                report=_report("clean"),
            )
            write_retag_divergence_census_snapshot(path, prior)

            class BrokenQueryBeets(FakeBeetsDB):
                def list_album_mb_identities(
                    self,
                ) -> list[BeetsAlbumIdentityRow]:
                    raise RuntimeError("programmer defect")

            beets = BrokenQueryBeets()
            with self.assertRaisesRegex(RuntimeError, "programmer defect"):
                publish_retag_divergence_census(path, lambda: beets)

            read_back = read_retag_divergence_census_snapshot(path)

        self.assertEqual(read_back, prior)


class TestExitCodeConstants(unittest.TestCase):
    """Known-bad self-test: the exit codes must be pairwise distinct —
    a script that collapsed EXIT_BEETS_UNAVAILABLE onto 0 or
    EXIT_CONFIG_ABORT would silently hide a real operational signal."""

    def test_exit_codes_are_pairwise_distinct(self) -> None:
        from scripts.run_retag_divergence_census import (
            EXIT_CONFIG_ABORT,
            EXIT_RUN_FAILED,
        )

        codes = [0, EXIT_BEETS_UNAVAILABLE, EXIT_CONFIG_ABORT, EXIT_RUN_FAILED]
        self.assertEqual(len(codes), len(set(codes)))


if __name__ == "__main__":
    unittest.main()
