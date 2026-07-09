#!/usr/bin/env python3
"""Generated tests for the slskd search-id write-ahead-ledger sweep
(issue #576, ``lib.slskd_searches.converge_slskd_searches``).

Two properties over generated worlds of ledgered + foreign (unledgered)
slskd searches in varied states/ages:

1. **I1 (no leak)** — after one sweep pass, no ledgered search past the
   GRACE window whose slskd state is ``Completed*`` (or already absent)
   remains resident in slskd. A ledgered row already marked swept by a
   PRIOR pass (``pre_deleted``) is out of scope for this pass — the
   world still generates it to probe the sweep never chokes on it.
2. **I3 (good-citizen)** — a foreign (unledgered) search is NEVER
   deleted/stopped and never disappears from slskd, in ANY state or age.

A third checker (in-flight never touched) rides along on the same worlds:
a ledgered search still ``InProgress``/``Queued`` past GRACE must never
be deleted or stopped, ledgered or not.

The deterministic pins for these same invariants live in
``tests/test_slskd_searches.py``
(``TestConvergeSlskdSearchesI1Pin`` / ``TestConvergeSlskdSearchesI3Pin``).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import configparser
import os
import sys
import unittest
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.slskd_searches import SEARCH_LEDGER_SWEEP_GRACE_S, converge_slskd_searches
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI

_RESIDENT_STATES = (
    "Completed, TimedOut",
    "Completed, Succeeded",
    "Completed, Errored",
    "Completed, ResponseLimitReached",
    "Completed, FileLimitReached",
    "Completed, Cancelled",
    "Completed, Aborted",
    "Completed, Rejected",
    "InProgress",
    "Queued, Locally",
    "Queued, Remotely",
)

_PAST_GRACE_S = SEARCH_LEDGER_SWEEP_GRACE_S + 60.0
_INSIDE_GRACE_S = SEARCH_LEDGER_SWEEP_GRACE_S / 2.0


@dataclass(frozen=True)
class LedgeredSearchWorld:
    search_id: str
    past_grace: bool
    resident: str | None  # None = absent from slskd (already gone)
    pre_deleted: bool = False  # already swept by a PRIOR pass


@dataclass(frozen=True)
class ForeignSearchWorld:
    search_id: str
    state: str


@dataclass(frozen=True)
class SweepWorld:
    ledgered: tuple[LedgeredSearchWorld, ...]
    foreign: tuple[ForeignSearchWorld, ...]


@st.composite
def _ledgered_rows(draw, *, count: int) -> tuple[LedgeredSearchWorld, ...]:
    rows = []
    for i in range(count):
        rows.append(LedgeredSearchWorld(
            search_id=f"ledger-{i}",
            past_grace=draw(st.booleans()),
            resident=draw(st.one_of(st.none(), st.sampled_from(_RESIDENT_STATES))),
            pre_deleted=draw(st.booleans()),
        ))
    return tuple(rows)


@st.composite
def _foreign_rows(draw, *, count: int) -> tuple[ForeignSearchWorld, ...]:
    return tuple(
        ForeignSearchWorld(search_id=f"foreign-{i}", state=draw(st.sampled_from(_RESIDENT_STATES)))
        for i in range(count)
    )


@st.composite
def sweep_worlds(draw) -> SweepWorld:
    ledgered = draw(_ledgered_rows(count=draw(st.integers(min_value=0, max_value=6))))
    foreign = draw(_foreign_rows(count=draw(st.integers(min_value=0, max_value=4))))
    return SweepWorld(ledgered=ledgered, foreign=foreign)


def _cfg() -> CratediggerConfig:
    return CratediggerConfig.from_ini(configparser.ConfigParser())


def _build_world_fakes(world: SweepWorld) -> tuple[FakePipelineDB, FakeSlskdAPI]:
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    now = datetime.now(timezone.utc)
    for row in world.ledgered:
        db.record_search_id(row.search_id, "plan_search", 1)
        age = _PAST_GRACE_S if row.past_grace else _INSIDE_GRACE_S
        db._search_ledger[row.search_id].created_at = now - timedelta(seconds=age)
        if row.pre_deleted:
            db._search_ledger[row.search_id].deleted_at = now - timedelta(seconds=1)
        if row.resident is not None:
            slskd.searches.add_search(
                search_id=row.search_id, state=row.resident, responses=[])
    for f in world.foreign:
        slskd.searches.add_search(search_id=f.search_id, state=f.state, responses=[])
    return db, slskd


def _ctx(db: FakePipelineDB, slskd: FakeSlskdAPI) -> CratediggerContext:
    return CratediggerContext(
        cfg=_cfg(), slskd=slskd, pipeline_db_source=FakePipelineDBSource(db))


# --- Invariant checkers (module-level so the known-bad self-tests can
# call them directly) --------------------------------------------------


def assert_no_ledgered_completed_search_survives(
    world: SweepWorld, live_ids: set,
) -> None:
    """I1: no ledgered, past-grace, Completed-or-absent search remains
    resident in slskd after one sweep pass. Rows already swept by a
    prior pass (``pre_deleted``) are out of this pass's scope."""
    for row in world.ledgered:
        if row.pre_deleted:
            continue
        eligible = row.past_grace and (
            row.resident is None or row.resident.startswith("Completed"))
        if eligible and row.search_id in live_ids:
            raise AssertionError(
                f"ledgered search {row.search_id!r} (state={row.resident!r}) "
                "survived the sweep")


def assert_inflight_never_touched(
    world: SweepWorld, delete_calls: list, stop_calls: list,
) -> None:
    """A ledgered search still InProgress/Queued must never be deleted or
    stopped by the sweep, regardless of GRACE."""
    for row in world.ledgered:
        if row.pre_deleted or row.resident is None:
            continue
        if row.resident.startswith("Completed"):
            continue
        if row.search_id in delete_calls or row.search_id in stop_calls:
            raise AssertionError(
                f"in-flight ledgered search {row.search_id!r} "
                f"(state={row.resident!r}) was touched by the sweep")


def assert_foreign_untouched(
    world: SweepWorld, delete_calls: list, stop_calls: list, live_ids: set,
) -> None:
    """I3: a foreign (unledgered) search is never deleted/stopped and
    never disappears from slskd, in any state or age."""
    for f in world.foreign:
        if f.search_id in delete_calls or f.search_id in stop_calls:
            raise AssertionError(
                f"sweep touched foreign search {f.search_id!r}")
        if f.search_id not in live_ids:
            raise AssertionError(
                f"foreign search {f.search_id!r} vanished from slskd")


class TestGeneratedSearchLedgerSweep(unittest.TestCase):
    """I1 + I3 (+ in-flight) properties over generated sweep worlds."""

    @given(world=sweep_worlds())
    def test_no_leak_no_foreign_touch_no_inflight_touch(self, world):
        db, slskd = _build_world_fakes(world)

        converge_slskd_searches(_ctx(db, slskd))

        live_ids = {s["id"] for s in slskd.searches.get_all()}
        assert_no_ledgered_completed_search_survives(world, live_ids)
        assert_inflight_never_touched(
            world, slskd.searches.delete_calls, slskd.searches.stop_calls)
        assert_foreign_untouched(
            world, slskd.searches.delete_calls, slskd.searches.stop_calls,
            live_ids)

    @given(world=sweep_worlds())
    def test_sweep_is_idempotent_second_pass_is_quiet(self, world):
        """A second sweep pass over the same (now-converged) state finds
        nothing new to do — the ledger's deleted_at stamp makes the
        cleanup exactly-once, matching converge_slskd_orphans's Phase 0
        contract of going quiet once nothing changed."""
        db, slskd = _build_world_fakes(world)
        converge_slskd_searches(_ctx(db, slskd))
        delete_calls_after_first = list(slskd.searches.delete_calls)

        second = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(second.deleted, 0)
        self.assertEqual(second.already_gone, 0)
        self.assertEqual(slskd.searches.delete_calls, delete_calls_after_first)


class TestSearchLedgerCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker must trip on a planted
    violating world/state — an untested checker is unfalsifiable."""

    def test_no_leak_checker_trips_when_survivor_present(self):
        world = SweepWorld(
            ledgered=(LedgeredSearchWorld(
                search_id="ledger-0", past_grace=True,
                resident="Completed, TimedOut"),),
            foreign=(),
        )
        with self.assertRaises(AssertionError):
            assert_no_ledgered_completed_search_survives(
                world, live_ids={"ledger-0"})

    def test_no_leak_checker_passes_when_pre_deleted_row_survives(self):
        # A pre_deleted row surviving isn't a leak of THIS pass — it's
        # explicitly out of scope (handled by an earlier pass).
        world = SweepWorld(
            ledgered=(LedgeredSearchWorld(
                search_id="ledger-0", past_grace=True,
                resident="Completed, TimedOut", pre_deleted=True),),
            foreign=(),
        )
        assert_no_ledgered_completed_search_survives(
            world, live_ids={"ledger-0"})  # must not raise

    def test_inflight_checker_trips_when_inflight_search_deleted(self):
        world = SweepWorld(
            ledgered=(LedgeredSearchWorld(
                search_id="ledger-0", past_grace=True, resident="InProgress"),),
            foreign=(),
        )
        with self.assertRaises(AssertionError):
            assert_inflight_never_touched(
                world, delete_calls=["ledger-0"], stop_calls=[])

    def test_foreign_checker_trips_when_foreign_deleted(self):
        world = SweepWorld(
            ledgered=(),
            foreign=(ForeignSearchWorld(
                search_id="foreign-0", state="Completed, TimedOut"),),
        )
        with self.assertRaises(AssertionError):
            assert_foreign_untouched(
                world, delete_calls=["foreign-0"], stop_calls=[],
                live_ids=set())

    def test_foreign_checker_trips_when_foreign_vanishes_untouched(self):
        # Even if the sweep never recorded a delete/stop call, a foreign
        # search that's simply gone afterward is still a violation —
        # catches a bug where the sweep deletes by some OTHER path.
        world = SweepWorld(
            ledgered=(),
            foreign=(ForeignSearchWorld(
                search_id="foreign-0", state="Completed, TimedOut"),),
        )
        with self.assertRaises(AssertionError):
            assert_foreign_untouched(
                world, delete_calls=[], stop_calls=[], live_ids=set())


if __name__ == "__main__":
    unittest.main()
