"""Settled swallow-vs-propagate exception contracts for the five slskd
sweeps (issue #1312, the #1297/#1299 series' named residual).

The five deferred sweeps — spelled by the run-cycle reachability table's
own scope note before this settlement — are ``converge_slskd_orphans``,
``reap_disk_orphans``, ``converge_slskd_searches``,
``harvest_terminal_transfer_evidence``, and
``purge_completed_transfers``. Every one is a registered convergence
step, so ``lib/convergence.py``'s runner isolates a step failure from
the cycle: "propagates" below means "reaches the runner's per-step
isolation, which logs the step's registered failure message and
continues the cycle" — never "aborts the cycle". That composition is
proven per sweep by the reachability table itself
(``tests/test_convergence_runner_generated.py::
TestRegisteredStepFailureMessagesReachable``), which gained one row per
sweep when this settlement landed; this module pins the sweep-internal
seam behavior. The settled contract, per sweep and seam:

==============================  ==================  ====================  =======================
sweep                           slskd failure       per-item destructive  pipeline-DB failure
==============================  ==================  ====================  =======================
converge_slskd_orphans          skip pass (0)       log, continue         propagates
reap_disk_orphans               (no slskd calls)    log, continue         protected-set read:
                                                                          aborted=True, zero
                                                                          deletions; ledger-owned
                                                                          read: propagates;
                                                                          abandoned-set
                                                                          computation: degrade to
                                                                          the ordinary age
                                                                          threshold
converge_slskd_searches         skip reconcile,     log, continue         unswept read / swept
                                prune still runs                          mark: propagates;
                                                                          retention prune:
                                                                          swallowed (warn), the
                                                                          pass's reconciliation
                                                                          work survives
harvest_terminal_transfer_      snapshot skips      per-row skip,         propagates
evidence                        the pass            continue
purge_completed_transfers       noop summary        count failed,         propagates
==============================  ==================  ====================  =======================

Most slskd and per-item cells were already pinned where each sweep's
tests live: ``tests/test_download.py`` (``TestConvergeSlskdOrphans``'s
snapshot-failure and cancel-error pins; ``TestPurgeCompletedTransfers``'s
snapshot-failure and removal-error pins;
``TestHarvestTerminalTransferEvidence``'s snapshot-noop and per-row
isolation pins) and ``tests/test_slskd_searches.py`` (fetch-failure and
per-id delete-failure pins); ``tests/test_disk_reaper_generated.py``
holds the reaper's fail-closed protected-set abort pins (a pipeline-DB
cell, pinned at its home). This module pins the cells those homes did
not: the pipeline-DB propagate/degrade column, plus the reaper's
per-item removal-failure cell, which no home had pinned at all
(``TestDiskReaperDbFailureSeams.
test_removal_failure_is_logged_and_the_sweep_continues``).

One bonus row rides along: ``prune_transfer_ledger_cycle`` was NOT one
of the deferred five — the reachability table has composed its
DB-failure propagation with the real runner since #1297/#1299 — but its
pin here adds Rule-B exception fidelity (``psycopg2.OperationalError``
where the table's generic injection raises ``RuntimeError``) and keeps
the whole slskd-adjacent Phase-0/end-of-cycle sweep family answerable
in one place.

Deterministic-only on purpose: each cell is one enumerable seam of one
sweep — a finite contract, not a world space — and the sweep↔runner
composition each "propagates" cell relies on is proven by the
reachability table's per-sweep rows named above.

Rule B fidelity: the injected failure is ``psycopg2.OperationalError``
— the class the real ``PipelineDB`` lets escape when its single
closed-connection reconnect-and-retry also fails (``lib/pipeline_db/
_core.py``), never a synthetic stand-in.
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2

from lib.download import harvest_terminal_transfer_evidence
from lib.pipeline_db import TransferLedgerRow
from lib.pipeline_db.rows import AlbumRequestRow
from lib.slskd_searches import (
    SEARCH_LEDGER_SWEEP_GRACE_S,
    SearchSweepSummary,
    converge_slskd_searches,
)
from lib.slskd_transfer_ledger import prune_transfer_ledger_cycle
from lib.slskd_transfers import (
    ORPHAN_MIN_AGE_DAYS,
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


class _MarkSweptRaises(FakePipelineDB):
    def mark_search_ids_deleted(self, search_ids: list[str]) -> None:
        raise psycopg2.OperationalError(_DB_DOWN)


class _SearchPruneRaises(FakePipelineDB):
    def prune_search_ledger(self, deleted_before: datetime) -> int:
        raise psycopg2.OperationalError(_DB_DOWN)


class _TransferPruneRaises(FakePipelineDB):
    def prune_transfer_ledger(self, older_than: datetime) -> int:
        raise psycopg2.OperationalError(_DB_DOWN)


def _seed_eligible_completed_search(
    db: FakePipelineDB, slskd: FakeSlskdAPI, search_id: str,
) -> None:
    """One ledgered search past the grace window whose slskd state is
    terminal — the world in which the sweep's reconciliation block
    actually runs (deletes the search, marks the row swept)."""
    db.record_search_id(search_id, "plan_search", 1)
    db._search_ledger[search_id].created_at = (
        datetime.now(UTC)
        - timedelta(seconds=SEARCH_LEDGER_SWEEP_GRACE_S + 60.0)
    )
    slskd.searches.add_search(
        search_id=search_id, state="Completed, TimedOut", responses=[])


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


class TestHarvestDbFailurePropagates(unittest.TestCase):
    """harvest_terminal_transfer_evidence: a DB failure reaches the
    runner unswallowed. Its snapshot-noop and per-row isolation cells
    are pinned in tests/test_download.py."""

    def test_downloading_read_failure_propagates(self):
        ctx = make_ctx_with_fake_db(
            _DownloadingReadRaises(), slskd=FakeSlskdAPI())
        with self.assertRaises(psycopg2.OperationalError):
            harvest_terminal_transfer_evidence(ctx)


class TestTransferLedgerPruneDbFailurePropagates(unittest.TestCase):
    """prune_transfer_ledger_cycle: a DB failure reaches the runner
    unswallowed — the bonus Rule-B-fidelity row (see module docstring);
    the composed propagate-through-the-runner proof has lived in the
    reachability table since #1297/#1299."""

    def test_prune_failure_propagates(self):
        ctx = make_ctx_with_fake_db(_TransferPruneRaises())
        with self.assertRaises(psycopg2.OperationalError):
            prune_transfer_ledger_cycle(ctx)


class TestSearchSweepDbContracts(unittest.TestCase):
    """converge_slskd_searches: the unswept read and the swept mark
    propagate; the retention prune is internally isolated so a prune
    failure never discards the same pass's reconciliation work."""

    def test_unswept_read_failure_propagates(self):
        ctx = make_ctx_with_fake_db(_UnsweptReadRaises(), slskd=FakeSlskdAPI())
        with self.assertRaises(psycopg2.OperationalError):
            converge_slskd_searches(ctx)

    def test_swept_mark_failure_propagates(self):
        db = _MarkSweptRaises()
        slskd = FakeSlskdAPI()
        _seed_eligible_completed_search(db, slskd, "mark-fails-1")
        ctx = make_ctx_with_fake_db(db, slskd=slskd)
        with self.assertRaises(psycopg2.OperationalError):
            converge_slskd_searches(ctx)

    def test_retention_prune_failure_is_swallowed_and_work_survives(self):
        """The world must actually do reconciliation work, or the pin is
        vacuous: with an empty ledger the summary equals the no-op value
        whether or not the prune failure was contained."""
        db = _SearchPruneRaises()
        slskd = FakeSlskdAPI()
        _seed_eligible_completed_search(db, slskd, "prune-fails-1")
        ctx = make_ctx_with_fake_db(db, slskd=slskd)

        with self.assertLogs("cratedigger", level="WARNING") as logs:
            summary = converge_slskd_searches(ctx)

        self.assertEqual(summary, SearchSweepSummary(deleted=1))
        self.assertIn("prune-fails-1", slskd.searches.delete_calls)
        self.assertIsNotNone(db._search_ledger["prune-fails-1"].deleted_at)
        self.assertTrue(
            any("prune failed" in line for line in logs.output),
            f"expected the prune-failure warning, got: {logs.output}")


class TestDiskReaperDbFailureSeams(unittest.TestCase):
    """reap_disk_orphans: three DB seams, three deliberate dispositions.
    The protected-set read aborts internally (``aborted=True``, pinned in
    tests/test_disk_reaper_generated.py) while the ledger-owned read
    propagates — both end in zero deletions, differing in which failure
    line the operator sees (the runner's registered message vs the
    sweep's own ABORTED log). The abandoned-set computation is the
    behaviorally distinct cell: its failure only degrades, narrowing —
    never widening — what may be deleted this cycle."""

    def _ctx(self, root: str, fake_db: FakePipelineDB):
        cfg = MagicMock()
        cfg.slskd_download_dir = root
        return make_ctx_with_fake_db(fake_db, cfg=cfg)

    def test_owned_paths_read_failure_propagates(self):
        with tempfile.TemporaryDirectory() as root:
            ctx = self._ctx(root, _OwnedPathsReadRaises())
            with self.assertRaises(psycopg2.OperationalError):
                reap_disk_orphans(ctx)

    def _seed_stamped_file(
        self, db: FakePipelineDB, root: str, name: str, *, request_id: int,
    ) -> str:
        """One ledger-owned, completion-stamped, age-eligible file on disk."""
        album_dir = os.path.join(root, "Album")
        os.makedirs(album_dir, exist_ok=True)
        path = os.path.join(album_dir, name)
        with open(path, "wb") as f:
            f.write(b"flac-bytes")
        aged = time.time() - (ORPHAN_MIN_AGE_DAYS + 1) * 86400
        os.utime(path, (aged, aged))
        remote = f"Music\\Album\\{name}"
        db.record_transfer_enqueue([TransferLedgerRow(
            request_id=request_id, username="p0", filename=remote)])
        db.confirm_transfer_enqueue("p0", remote, request_id=request_id)
        db.stamp_transfer_completion("p0", remote, path)
        return path

    def test_removal_failure_is_logged_and_the_sweep_continues(self):
        """The reaper's per-item cell: one file's failed unlink is logged
        and files walked AFTER it are still reaped — never an aborted
        sweep, never an exception.

        ``os.walk`` yields ``filenames`` in readdir order, which on this
        tmpfs put the healthy file FIRST — letting a ``continue``→
        ``break`` mutant survive the pin's own headline claim (#1312
        round-3 review, mutant 4). The walk is wrapped to sort each
        directory's filenames (a filesystem leaf seam, like the
        ``os.remove`` injection below) so the poisoned ``01 -`` file is
        provably visited before the healthy ``02 -`` file.
        """
        with tempfile.TemporaryDirectory() as root:
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=1, status="imported"))
            poisoned = self._seed_stamped_file(
                db, root, "01 - Poisoned.flac", request_id=1)
            healthy = self._seed_stamped_file(
                db, root, "02 - Healthy.flac", request_id=1)

            real_remove = os.remove
            real_walk = os.walk

            def _selective_remove(path: str) -> None:
                if path == poisoned:
                    raise OSError("injected: unlink refused")
                real_remove(path)

            def _sorted_walk(top: str, topdown: bool = True):
                for dirpath, dirnames, filenames in real_walk(
                        top, topdown=topdown):
                    yield dirpath, dirnames, sorted(filenames)

            with (
                patch("os.walk", side_effect=_sorted_walk),
                patch("os.remove", side_effect=_selective_remove),
                self.assertLogs("cratedigger", level="WARNING") as logs,
            ):
                summary = reap_disk_orphans(self._ctx(root, db))

            self.assertFalse(summary.aborted)
            self.assertEqual(summary.removed, 1)
            self.assertTrue(os.path.exists(poisoned))
            self.assertFalse(os.path.exists(healthy))
            self.assertTrue(
                any("failed to remove" in line for line in logs.output),
                f"expected the removal-failure warning, got: {logs.output}")

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
