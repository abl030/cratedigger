"""Persisted daily retag-divergence census snapshot (#1142).

The whole-library census (``lib/retag_divergence_audit.py``) scans ~93,700
files / ~200s — far too expensive to run at dashboard render or normal web
API request time. This module is the read-only persistence boundary: a
daily oneshot (``scripts/run_retag_divergence_census.py``) publishes one
snapshot atomically; the dashboard route and CLI read it back without ever
triggering a scan.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from lib.retag_divergence_audit import (
    RetagDivergenceCounts,
    RetagDivergenceReport,
    RetagDivergenceStatus,
)
from lib.retag_divergence_census_snapshot import (
    RETAG_DIVERGENCE_CENSUS_SNAPSHOT_FILENAME,
    RetagDivergenceCensusSnapshot,
    read_retag_divergence_census_snapshot,
    retag_divergence_census_snapshot_path,
    write_retag_divergence_census_snapshot,
)


def _report(status: RetagDivergenceStatus = "clean") -> RetagDivergenceReport:
    return RetagDivergenceReport(
        status=status,
        complete=True,
        counts=RetagDivergenceCounts(0, 0, 0, 0, 0, 0, 0, 0),
        albums=(),
    )


class TestRetagDivergenceCensusSnapshotPath(unittest.TestCase):
    def test_path_is_derived_from_var_dir(self) -> None:
        self.assertEqual(
            retag_divergence_census_snapshot_path("/var/lib/cratedigger"),
            os.path.join(
                "/var/lib/cratedigger",
                RETAG_DIVERGENCE_CENSUS_SNAPSHOT_FILENAME,
            ),
        )


class TestReadRetagDivergenceCensusSnapshot(unittest.TestCase):
    def test_missing_file_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "retag-divergence-census.json")

            self.assertIsNone(read_retag_divergence_census_snapshot(path))

    def test_round_trip_preserves_generated_at_duration_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "retag-divergence-census.json")
            snapshot = RetagDivergenceCensusSnapshot(
                generated_at="2026-08-14T09:00:00+00:00",
                duration_seconds=196.4,
                report=_report("divergence_found"),
            )

            write_retag_divergence_census_snapshot(path, snapshot)
            read_back = read_retag_divergence_census_snapshot(path)

        self.assertEqual(read_back, snapshot)


class TestWriteRetagDivergenceCensusSnapshotAtomicity(unittest.TestCase):
    """#1142 acceptance 1 — a failed run preserves a prior valid snapshot.
    The write path delegates to the shared same-directory-temp-file +
    ``os.replace`` helper (``lib.sidecar_service._atomic_write_bytes``,
    already proven atomic elsewhere); this pins THIS module's own promise:
    a write that fails after the temp file exists but before the rename
    never touches the previously published snapshot."""

    def test_a_failed_replace_never_touches_the_prior_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "retag-divergence-census.json")
            prior = RetagDivergenceCensusSnapshot(
                generated_at="2026-08-13T09:00:00+00:00",
                duration_seconds=190.0,
                report=_report("clean"),
            )
            write_retag_divergence_census_snapshot(path, prior)

            newer = RetagDivergenceCensusSnapshot(
                generated_at="2026-08-14T09:00:00+00:00",
                duration_seconds=200.0,
                report=_report("divergence_found"),
            )
            with (
                patch(
                    "lib.sidecar_service.os.replace",
                    side_effect=OSError("disk full"),
                ),
                self.assertRaises(OSError),
            ):
                write_retag_divergence_census_snapshot(path, newer)

            read_back = read_retag_divergence_census_snapshot(path)

            self.assertEqual(read_back, prior)
            # No leaked temp file survives a failed publish.
            self.assertEqual(
                [
                    n for n in os.listdir(tmpdir)
                    if n != "retag-divergence-census.json"
                ],
                [],
            )


if __name__ == "__main__":
    unittest.main()
