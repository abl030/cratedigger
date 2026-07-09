#!/usr/bin/env python3
"""Generated tests for the completed-transfer purge (issue #571 PR 5).

Three properties over generated worlds of live slskd transfers, each
independently {stamped-owned / unstamped-owned / foreign} x transfer
state (completed or still live):

1. **P1 (good-citizen)** — a completed transfer record whose id is
   absent from cratedigger's write-ahead ``slskd_transfer_ledger`` is
   NEVER removed by the purge, whatever its state or age. This is the
   flip the old bulk ``remove_completed_downloads()`` call never made:
   it purged every completed record slskd reported, including a human's,
   on a shared instance.
2. **P2 (stamp-before-remove)** — a completed transfer record that IS
   ledger-owned but whose ledger row has not yet received its T2
   completion stamp (``completed_at`` still NULL) is left untouched this
   pass — removing slskd's own record before the stamp lands would race
   the events feed, the ONLY source of completed-file locations.
3. **P3 (housekeeping still works)** — a completed, ledger-owned,
   completion-STAMPED transfer record IS removed each pass, so slskd's
   UI keeps clearing the way the old bulk call used to.

Both properties drive the REAL ``purge_completed_transfers`` entry point
over ``FakeSlskdAPI`` + ``FakePipelineDB`` — not the pure
``lib.repair.find_completed_transfers_to_purge`` helper directly — so the
generated harness also exercises the id-set assembly the purge itself
owns (``PipelineDB.get_owned_transfer_id_sets``).

The deterministic pins for these same invariants live in
``tests/test_download.py::TestPurgeCompletedTransfers`` (orchestration)
and ``tests/test_repair.py::TestFindCompletedTransfersToPurge`` (pure
classification).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import configparser
import os
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.pipeline_db import TransferLedgerRow
from lib.slskd_transfers import purge_completed_transfers
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI

_LIVE_STATES = ("InProgress", "Queued, Remotely", "Queued, Locally", "")
_TERMINAL_STATES = (
    "Completed, Succeeded",
    "Completed, Errored",
    "Completed, Cancelled",
    "Completed, TimedOut",
    "Completed, Aborted",
    "Completed, Rejected",
)
_ALL_STATES = _LIVE_STATES + _TERMINAL_STATES

_OWNERSHIPS = ("stamped", "unstamped", "foreign")


@dataclass(frozen=True)
class CompletedTransferWorld:
    key: int
    state: str
    ownership: str  # "stamped" | "unstamped" | "foreign"


@st.composite
def completed_transfer_worlds(draw) -> tuple[CompletedTransferWorld, ...]:
    count = draw(st.integers(min_value=0, max_value=6))
    worlds = []
    for i in range(count):
        worlds.append(CompletedTransferWorld(
            key=i,
            state=draw(st.sampled_from(_ALL_STATES)),
            ownership=draw(st.sampled_from(_OWNERSHIPS)),
        ))
    return tuple(worlds)


def _username(key: int) -> str:
    return f"peer{key}"


def _filename(key: int) -> str:
    return f"Music\\Album{key}\\track.flac"


def _cfg() -> CratediggerConfig:
    return CratediggerConfig.from_ini(configparser.ConfigParser())


def _build_world_fakes(
    worlds: tuple[CompletedTransferWorld, ...],
) -> tuple[FakePipelineDB, FakeSlskdAPI]:
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    for w in worlds:
        username, filename = _username(w.key), _filename(w.key)
        transfer_id = f"t-{w.key}"
        slskd.add_transfer(
            username=username, directory=f"Music\\Album{w.key}",
            filename=filename, id=transfer_id, state=w.state)
        if w.ownership in ("stamped", "unstamped"):
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=w.key + 1, username=username,
                    filename=filename),
            ])
            if w.ownership == "stamped":
                db.stamp_transfer_completion(
                    username, filename, "/downloads/complete/x",
                    datetime.now(timezone.utc),
                    transfer_id=transfer_id)
            else:
                db.stamp_transfer_id(username, filename, transfer_id)
        # "foreign" -- no ledger row at all.
    return db, slskd


def _ctx(db: FakePipelineDB, slskd: FakeSlskdAPI) -> CratediggerContext:
    return CratediggerContext(
        cfg=_cfg(), slskd=slskd, pipeline_db_source=FakePipelineDBSource(db))


# --- Invariant checkers (module-level so the known-bad self-tests can
# call them directly) --------------------------------------------------


def assert_foreign_never_removed(
    worlds: tuple[CompletedTransferWorld, ...], removed_ids: set[str],
) -> None:
    """P1: a completed transfer id absent from the ledger is never
    removed, whatever its state."""
    for w in worlds:
        if w.ownership != "foreign":
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id in removed_ids:
            raise AssertionError(
                f"foreign completed transfer {transfer_id!r} "
                f"(world={w!r}) was removed by the purge")


def assert_unstamped_owned_never_removed(
    worlds: tuple[CompletedTransferWorld, ...], removed_ids: set[str],
) -> None:
    """P2: a ledger-owned but not-yet-completion-stamped transfer is left
    for a later cycle — never removed this pass."""
    for w in worlds:
        if w.ownership != "unstamped":
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id in removed_ids:
            raise AssertionError(
                f"unstamped owned transfer {transfer_id!r} (world={w!r}) "
                "was removed before its completion stamp landed")


def assert_stamped_owned_completed_is_removed(
    worlds: tuple[CompletedTransferWorld, ...], removed_ids: set[str],
) -> None:
    """P3: a stamped, ledger-owned, COMPLETED transfer IS removed."""
    for w in worlds:
        if w.ownership != "stamped":
            continue
        if not w.state.startswith("Completed"):
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id not in removed_ids:
            raise AssertionError(
                f"stamped, owned, completed transfer {transfer_id!r} "
                f"(world={w!r}) was NOT removed by the purge")


class TestGeneratedPurgeCompletedTransfers(unittest.TestCase):
    """P1 + P2 + P3 properties over generated worlds, through the REAL
    ``purge_completed_transfers`` entry point."""

    @given(worlds=completed_transfer_worlds())
    def test_p1_p2_p3_hold_across_worlds(self, worlds):
        db, slskd = _build_world_fakes(worlds)

        purge_completed_transfers(_ctx(db, slskd))

        removed_ids = {
            c.id for c in slskd.transfers.cancel_download_calls if c.remove
        }
        assert_foreign_never_removed(worlds, removed_ids)
        assert_unstamped_owned_never_removed(worlds, removed_ids)
        assert_stamped_owned_completed_is_removed(worlds, removed_ids)

    @given(worlds=completed_transfer_worlds())
    def test_purge_is_idempotent_second_pass_removes_nothing_new(
        self, worlds,
    ):
        """A second pass over the same (now-purged) slskd state finds no
        NEW removable records — the first pass's removals already took
        them out of the live snapshot (FakeSlskdAPI mirrors slskd's own
        remove-on-cancel behavior)."""
        db, slskd = _build_world_fakes(worlds)
        purge_completed_transfers(_ctx(db, slskd))
        first_pass_calls = list(slskd.transfers.cancel_download_calls)

        purge_completed_transfers(_ctx(db, slskd))

        self.assertEqual(slskd.transfers.cancel_download_calls, first_pass_calls)

    @given(worlds=completed_transfer_worlds())
    def test_every_removal_call_passes_remove_true(self, worlds):
        """Seam guard: the purge must always request record removal, not
        a bare cancel — a plain cancel_download(..., remove=False) on an
        already-completed transfer is a no-op against slskd, silently
        leaving the record in place."""
        db, slskd = _build_world_fakes(worlds)

        purge_completed_transfers(_ctx(db, slskd))

        for call in slskd.transfers.cancel_download_calls:
            self.assertTrue(
                call.remove, f"cancel_download call {call!r} must set remove=True")


class TestPurgeCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker must trip on a planted
    violating removal set — an untested checker is unfalsifiable."""

    def test_p1_checker_trips_when_a_foreign_transfer_is_removed(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Succeeded", ownership="foreign"),)
        with self.assertRaises(AssertionError):
            assert_foreign_never_removed(worlds, removed_ids={"t-0"})

    def test_p2_checker_trips_when_an_unstamped_record_is_removed(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Succeeded", ownership="unstamped"),)
        with self.assertRaises(AssertionError):
            assert_unstamped_owned_never_removed(worlds, removed_ids={"t-0"})

    def test_p3_checker_trips_when_a_removable_record_survives(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Succeeded", ownership="stamped"),)
        with self.assertRaises(AssertionError):
            assert_stamped_owned_completed_is_removed(worlds, removed_ids=set())

    def test_p3_checker_passes_live_stamped_transfer_left_alone(self):
        # A stamped-owned transfer that hasn't reached a terminal state
        # yet is NOT a P3 target (nothing to remove) — the checker must
        # not raise here.
        worlds = (CompletedTransferWorld(
            key=0, state="InProgress", ownership="stamped"),)
        assert_stamped_owned_completed_is_removed(worlds, removed_ids=set())


if __name__ == "__main__":
    unittest.main()
