"""Settled swallow-vs-propagate exception contracts for the five slskd
sweeps (issue #1312, the #1297/#1299 series' named residual).

Every sweep is a registered convergence step, so ``lib/convergence.py``'s
runner already isolates a step failure from the cycle. "Propagates" below
therefore means "reaches the runner's per-step isolation, which logs the
step's registered failure message and continues the cycle" — never
"aborts the cycle". The settled contract, per sweep and seam:

============================  ==================  ====================  =======================
sweep                         slskd failure       per-item destructive  pipeline-DB failure
============================  ==================  ====================  =======================
converge_slskd_orphans        skip pass (0)       log, continue         propagates
reap_disk_orphans             (no slskd calls)    log, continue         protected-set read:
                                                                        aborted=True, zero
                                                                        deletions; ledger-owned
                                                                        read: propagates;
                                                                        abandoned read: degrade
                                                                        to the ordinary age
                                                                        threshold
converge_slskd_searches       skip reconcile,     log, continue         unswept read / mark:
                              prune still runs                          propagates; retention
                                                                        prune: swallowed (warn),
                                                                        summary still returned
prune_transfer_ledger_cycle   (no slskd calls)    (none)                propagates
purge_completed_transfers     noop summary        count failed,         propagates
                                                  continue
============================  ==================  ====================  =======================

The slskd and per-item cells were already pinned where each sweep's tests
live: ``tests/test_download.py`` (``TestConvergeSlskdOrphans``'s
snapshot-failure and cancel-error pins; ``TestPurgeCompletedTransfers``'s
snapshot-failure and removal-error pins), ``tests/test_slskd_searches.py``
(fetch-failure and per-id delete-failure pins), and
``tests/test_disk_reaper_generated.py`` (the fail-closed protected-set
abort pins). This module pins the previously-unpinned pipeline-DB cells —
the propagate/degrade column — one deterministic pin per cell, so the
contract each docstring now states is enforced rather than prose.

Deterministic-only on purpose: the cells are a finite, enumerable
contract, not a world space, and the generated step-failure-isolation
property at the runner boundary
(``tests/test_convergence_runner_generated.py``) already patrols that a
propagated failure never escapes the cycle.

Rule B fidelity: the injected failure is ``psycopg2.OperationalError`` —
what the real ``PipelineDB`` raises when the connection drops — never a
synthetic stand-in.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2

from lib.pipeline_db import TransferLedgerRow
from lib.pipeline_db.rows import AlbumRequestRow
from lib.slskd_searches import SearchSweepSummary, converge_slskd_searches
from lib.slskd_transfer_ledger import prune_transfer_ledger_cycle
from lib.slskd_transfers import (
    converge_slskd_orphans,
    purge_completed_transfers,
    reap_disk_orphans,
)
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import make_ctx_with_fake_db, make_request_row

_DB_DOWN = "injected: connection to pipeline DB lost"


class _DownloadingReadRaises(FakePipelineDB):
    def get_downloading(self) -> list[AlbumRequestRow]:
        raise psycopg2.OperationalError(_DB_DOWN)


class _OwnedKeysReadRaises(FakePipelineDB):
    def get_owned_transfer_keys(self) -> set[tuple[str, str]]:
        raise psycopg2.OperationalError(_DB_DOWN)


class _OwnedPathsReadRaises(FakePipelineDB):
    def get_owned_local_paths(self) -> set[str]:
        raise psycopg2.OperationalError(_DB_DOWN)


class _AbandonedReadRaises(FakePipelineDB):
    def get_abandoned_owned_local_paths(self) -> set[str]:
        raise psycopg2.OperationalError(_DB_DOWN)


class _UnsweptReadRaises(FakePipelineDB):
    def get_unswept_search_ids(
        self, older_than: datetime,
    ) -> list[dict[str, object]]:
        raise psycopg2.OperationalError(_DB_DOWN)


class _SearchPruneRaises(FakePipelineDB):
    def prune_search_ledger(self, deleted_before: datetime) -> int:
        raise psycopg2.OperationalError(_DB_DOWN)


class _TransferPruneRaises(FakePipelineDB):
    def prune_transfer_ledger(self, older_than: datetime) -> int:
        raise psycopg2.OperationalError(_DB_DOWN)


class TestOrphanConvergenceDbFailurePropagates(unittest.TestCase):
    """converge_slskd_orphans: a DB failure reaches the runner unswallowed."""

    def test_downloading_read_failure_propagates(self):
        ctx = make_ctx_with_fake_db(
            _DownloadingReadRaises(), slskd=FakeSlskdAPI())
        with self.assertRaises(psycopg2.OperationalError):
            converge_slskd_orphans(ctx)


class TestCompletedPurgeDbFailurePropagates(unittest.TestCase):
    """purge_completed_transfers: a DB failure reaches the runner unswallowed."""

    def test_owned_keys_read_failure_propagates(self):
        ctx = make_ctx_with_fake_db(
            _OwnedKeysReadRaises(), slskd=FakeSlskdAPI())
        with self.assertRaises(psycopg2.OperationalError):
            purge_completed_transfers(ctx)


class TestTransferLedgerPruneDbFailurePropagates(unittest.TestCase):
    """prune_transfer_ledger_cycle: a DB failure reaches the runner
    unswallowed — the docstring's "DB failures deliberately propagate"
    claim, enforced."""

    def test_prune_failure_propagates(self):
        ctx = make_ctx_with_fake_db(_TransferPruneRaises())
        with self.assertRaises(psycopg2.OperationalError):
            prune_transfer_ledger_cycle(ctx)


class TestSearchSweepDbContracts(unittest.TestCase):
    """converge_slskd_searches: the unswept read propagates; the retention
    prune is internally isolated so a prune failure never discards the
    sweep's own reconciliation work."""

    def test_unswept_read_failure_propagates(self):
        ctx = make_ctx_with_fake_db(_UnsweptReadRaises(), slskd=FakeSlskdAPI())
        with self.assertRaises(psycopg2.OperationalError):
            converge_slskd_searches(ctx)

    def test_retention_prune_failure_is_swallowed_and_summary_survives(self):
        ctx = make_ctx_with_fake_db(_SearchPruneRaises(), slskd=FakeSlskdAPI())
        with self.assertLogs("cratedigger", level="WARNING") as logs:
            summary = converge_slskd_searches(ctx)
        self.assertEqual(summary, SearchSweepSummary(
            deleted=0, already_gone=0, foreign_skipped=0))
        self.assertTrue(
            any("prune failed" in line for line in logs.output),
            f"expected the prune-failure warning, got: {logs.output}")


class TestDiskReaperDbFailureSeams(unittest.TestCase):
    """reap_disk_orphans: three DB seams, three deliberately different
    contracts — the protected-set read aborts fail-closed (pinned in
    tests/test_disk_reaper_generated.py), the ledger-owned read
    propagates, and the abandoned-attempt read degrades to the ordinary
    age threshold."""

    def _ctx(self, root: str, fake_db: FakePipelineDB):
        cfg = MagicMock()
        cfg.slskd_download_dir = root
        return make_ctx_with_fake_db(fake_db, cfg=cfg)

    def test_owned_paths_read_failure_propagates(self):
        with tempfile.TemporaryDirectory() as root:
            ctx = self._ctx(root, _OwnedPathsReadRaises())
            with self.assertRaises(psycopg2.OperationalError):
                reap_disk_orphans(ctx)

    def test_abandoned_read_failure_degrades_to_ordinary_age_threshold(self):
        with tempfile.TemporaryDirectory() as root:
            album_dir = os.path.join(root, "Album")
            os.makedirs(album_dir)
            fresh_path = os.path.join(album_dir, "01 - Track.flac")
            with open(fresh_path, "wb") as f:
                f.write(b"flac-bytes")

            db = _AbandonedReadRaises()
            # A wanted request with no active state: its stamped file IS
            # abandoned, so a healthy abandoned read would reap it now
            # (tests/test_disk_reaper_generated.py pins that world green).
            db.seed_request(make_request_row(
                id=1, status="wanted", active_download_state=None))
            db.record_transfer_enqueue([TransferLedgerRow(
                request_id=1, username="p0",
                filename="Music\\Album\\01 - Track.flac")])
            db.confirm_transfer_enqueue(
                "p0", "Music\\Album\\01 - Track.flac", request_id=1)
            db.stamp_transfer_completion(
                "p0", "Music\\Album\\01 - Track.flac", fresh_path)

            with self.assertLogs("cratedigger", level="WARNING") as logs:
                summary = reap_disk_orphans(self._ctx(root, db))

            self.assertFalse(summary.aborted)
            self.assertEqual(summary.removed, 0)
            self.assertEqual(summary.skipped_young, 1)
            self.assertTrue(os.path.exists(fresh_path))
            self.assertTrue(
                any(
                    "could not read abandoned-attempt paths" in line
                    for line in logs.output
                ),
                f"expected the degrade warning, got: {logs.output}")


if __name__ == "__main__":
    unittest.main()
