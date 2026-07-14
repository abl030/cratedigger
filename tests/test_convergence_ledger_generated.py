#!/usr/bin/env python3
"""Generated tests for the ledger-positive ownership flip of
``lib.slskd_transfers.converge_slskd_orphans`` (issue #571 PR 3).

Two properties over generated worlds of live slskd transfers, each
independently {foreign/pending/confirmed} x {backed/unbacked by a
``downloading`` row} x transfer state:

1. **C1 (good-citizen)** — a live transfer without a confirmed accepted
   enqueue in ``slskd_transfer_ledger`` is NEVER cancelled by convergence,
   whatever its state or backed status. This includes both foreign keys and
   pending write-ahead intents whose POST failed or had an unknown outcome.
   This is the flip:
   the OLD doctrine cancelled any transfer no ``downloading`` row backed,
   which risked cancelling a human's transfer on a shared slskd instance.
2. **C2 (housekeeping still works)** — a live (non-terminal), CONFIRMED
   transfer that is NOT backed by a currently-``downloading`` row IS
   cancelled — cratedigger's own stray (the classic Replace-abandons-
   transfer case, and a confirmed transfer whose row already self-healed
   back to ``wanted``).

Both properties drive the REAL ``converge_slskd_orphans`` entry point
over ``FakeSlskdAPI`` + ``FakePipelineDB`` — not the pure
``lib.repair.find_slskd_orphans`` helper directly — so the generated
harness also exercises the ledger-set/backed-set assembly convergence
itself owns.

The deterministic pins for these same invariants live in
``tests/test_download.py::TestConvergeSlskdOrphans`` (orchestration) and
``tests/test_repair.py::TestFindSlskdOrphans`` (pure classification).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import configparser
import os
import sys
import unittest
from dataclasses import dataclass
from typing import Literal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.pipeline_db import TransferLedgerRow
from lib.slskd_transfers import converge_slskd_orphans
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
from tests.helpers import make_request_row

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


@dataclass(frozen=True)
class TransferWorld:
    key: int
    state: str
    ownership: Literal["foreign", "pending", "confirmed"]
    backed: bool


@st.composite
def transfer_worlds(draw) -> tuple[TransferWorld, ...]:
    count = draw(st.integers(min_value=0, max_value=6))
    worlds = []
    for i in range(count):
        worlds.append(TransferWorld(
            key=i,
            state=draw(st.sampled_from(_ALL_STATES)),
            ownership=draw(st.sampled_from(
                ("foreign", "pending", "confirmed"))),
            backed=draw(st.booleans()),
        ))
    return tuple(worlds)


def _username(key: int) -> str:
    return f"peer{key}"


def _filename(key: int) -> str:
    return f"Music\\Album{key}\\track.flac"


def _cfg() -> CratediggerConfig:
    return CratediggerConfig.from_ini(configparser.ConfigParser())


def _build_world_fakes(
    worlds: tuple[TransferWorld, ...],
) -> tuple[FakePipelineDB, FakeSlskdAPI]:
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    ledger_rows = []
    for w in worlds:
        username, filename = _username(w.key), _filename(w.key)
        slskd.add_transfer(
            username=username, directory=f"Music\\Album{w.key}",
            filename=filename, id=f"t-{w.key}", state=w.state)
        if w.ownership != "foreign":
            ledger_rows.append(TransferLedgerRow(
                request_id=w.key + 1, username=username, filename=filename))
        if w.backed:
            db.seed_request(make_request_row(
                id=w.key + 1, status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "files": [{"username": username, "filename": filename}],
                }))
    if ledger_rows:
        db.record_transfer_enqueue(ledger_rows)
    for w in worlds:
        if w.ownership == "confirmed":
            db.confirm_transfer_enqueue(_username(w.key), _filename(w.key))
    return db, slskd


def _ctx(db: FakePipelineDB, slskd: FakeSlskdAPI) -> CratediggerContext:
    return CratediggerContext(
        cfg=_cfg(), slskd=slskd, pipeline_db_source=FakePipelineDBSource(db))


# --- Invariant checkers (module-level so the known-bad self-tests can
# call them directly) --------------------------------------------------


def assert_unconfirmed_never_cancelled(
    worlds: tuple[TransferWorld, ...], cancelled_ids: set[str],
) -> None:
    """C1: a foreign or merely pending transfer is never cancelled."""
    for w in worlds:
        if w.ownership == "confirmed":
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id in cancelled_ids:
            raise AssertionError(
                f"unconfirmed transfer {transfer_id!r} (world={w!r}) "
                "was cancelled by convergence")


def assert_confirmed_unbacked_live_is_cancelled(
    worlds: tuple[TransferWorld, ...], cancelled_ids: set[str],
) -> None:
    """C2: a confirmed, unbacked, LIVE (non-terminal) transfer is always
    cancelled — cratedigger's own stray."""
    for w in worlds:
        if w.ownership != "confirmed" or w.backed:
            continue
        if w.state.startswith("Completed"):
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id not in cancelled_ids:
            raise AssertionError(
                f"confirmed, unbacked, live transfer {transfer_id!r} "
                f"(world={w!r}) was NOT cancelled by convergence")


class TestGeneratedConvergeSlskdOrphans(unittest.TestCase):
    """C1 + C2 properties over generated worlds, through the REAL
    ``converge_slskd_orphans`` entry point."""

    @given(worlds=transfer_worlds())
    def test_c1_and_c2_hold_across_worlds(self, worlds):
        db, slskd = _build_world_fakes(worlds)

        converge_slskd_orphans(_ctx(db, slskd))

        cancelled_ids = {c.id for c in slskd.transfers.cancel_download_calls}
        assert_unconfirmed_never_cancelled(worlds, cancelled_ids)
        assert_confirmed_unbacked_live_is_cancelled(worlds, cancelled_ids)

    @given(worlds=transfer_worlds())
    def test_convergence_is_idempotent_second_pass_cancels_nothing_new(
        self, worlds,
    ):
        """A second pass over the same (now-converged) slskd state finds
        no NEW strays — the first pass's cancels already removed them
        from the live snapshot (FakeSlskdAPI mirrors slskd's own
        remove-on-cancel behavior)."""
        db, slskd = _build_world_fakes(worlds)
        converge_slskd_orphans(_ctx(db, slskd))
        first_pass_calls = list(slskd.transfers.cancel_download_calls)

        converge_slskd_orphans(_ctx(db, slskd))

        self.assertEqual(slskd.transfers.cancel_download_calls, first_pass_calls)


class TestConvergeCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker must trip on a planted
    violating cancellation set — an untested checker is unfalsifiable."""

    def test_c1_checker_trips_when_a_foreign_transfer_is_cancelled(self):
        worlds = (TransferWorld(
            key=0, state="InProgress", ownership="foreign", backed=False),)
        with self.assertRaises(AssertionError):
            assert_unconfirmed_never_cancelled(worlds, cancelled_ids={"t-0"})

    def test_c1_checker_trips_when_a_pending_transfer_is_cancelled(self):
        worlds = (TransferWorld(
            key=0, state="InProgress", ownership="pending", backed=False),)
        with self.assertRaises(AssertionError):
            assert_unconfirmed_never_cancelled(worlds, cancelled_ids={"t-0"})

    def test_c2_checker_trips_when_a_stray_survives_uncancelled(self):
        worlds = (TransferWorld(
            key=0, state="InProgress", ownership="confirmed", backed=False),)
        with self.assertRaises(AssertionError):
            assert_confirmed_unbacked_live_is_cancelled(
                worlds, cancelled_ids=set())

    def test_c2_checker_passes_terminal_stray_left_alone(self):
        # A terminal-state confirmed/unbacked transfer is NOT a C2 target
        # (nothing to cancel) — the checker must not raise here.
        worlds = (TransferWorld(
            key=0, state="Completed, Succeeded",
            ownership="confirmed", backed=False),)
        assert_confirmed_unbacked_live_is_cancelled(
            worlds, cancelled_ids=set())


if __name__ == "__main__":
    unittest.main()
