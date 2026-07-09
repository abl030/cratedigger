#!/usr/bin/env python3
"""Generated tests for the Jellyfin DateCreated pin lifecycle (issue #574).

Properties over generated worlds of {pin snapshot} x {live Jellyfin state}
(album present/absent/erroring, item ids kept or recreated, arbitrary child
dates, per-item write failures, pin age vs grace/TTL), driving the REAL
``reconcile_jellyfin_date_created_pins`` / ``capture_jellyfin_date_created_pin``
entry points over ``FakePipelineDB`` with the Jellyfin client seams injected:

1. **P1 (only the original is written)** — the reconciler never writes any
   value other than the pin's captured ``original_date_created`` into
   Jellyfin, whatever the live state does.
2. **P2 (never finalize before the rescan lands)** — while the album item id
   and children id-set still match the capture snapshot, the pin is never
   written to and never marked done: it stays pending within the TTL and
   expires (with zero writes) past it. This detector replaces Plex's
   ``addedAt.locked``; closing a pin before the rescan re-stamps the items
   would silently resurrect the bug.
3. **P3 (terminal-state correctness)** — done ⟹ the rescan landed, every
   drifted item (album + audio children) was written, and no write failed;
   skipped ⟺ the album is no longer locatable; a failed write or an
   erroring client always leaves the pin pending.
4. **P4 (capture snapshot fidelity)** — a capture either persists a pin that
   mirrors exactly what the finder returned (original date, album id,
   children ids, path, request id) or persists nothing.

The deterministic pins for these same invariants live in
tests/test_jellyfin_pin_service.py.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.jellyfin_pin_service import (
    capture_jellyfin_date_created_pin,
    reconcile_jellyfin_date_created_pins,
)
from lib.util import JellyfinAlbumRef, JellyfinItemRef
from tests.fakes import FakePipelineDB

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
GRACE_SECONDS = 180
TTL_HOURS = 48

_DATES = ["D-orig", "D-old-1", "D-old-2", "D-bumped"]
_CHILD_POOL = ["c1", "c2", "c3", "n1", "n2"]


def _cfg() -> CratediggerConfig:
    return CratediggerConfig(jellyfin_url="http://jf:8096", jellyfin_token="t")


@dataclass
class World:
    """One generated pin + the live Jellyfin state the reconciler sees."""
    original: str
    snapshot_children: list[str]
    age_minutes: int
    find_outcome: str            # "present" | "absent" | "raises"
    children_raises: bool
    live_album_recreated: bool
    live_album_date: str
    live_children: list[tuple[str, str]]   # (item_id, date)
    set_fail_ids: set[str]

    @property
    def live_album_id(self) -> str:
        return "alb-new" if self.live_album_recreated else "alb-same"

    @property
    def landed(self) -> bool:
        return (self.live_album_recreated
                or {i for i, _ in self.live_children} != set(self.snapshot_children))

    @property
    def settled(self) -> bool:
        """Landed AND not in the mid-scan zero-children window."""
        return self.landed and (bool(self.live_children)
                                or not self.snapshot_children)

    @property
    def past_grace(self) -> bool:
        return self.age_minutes * 60 > GRACE_SECONDS

    @property
    def past_ttl(self) -> bool:
        return self.age_minutes > TTL_HOURS * 60

    @property
    def drifted_ids(self) -> set[str]:
        out = {i for i, d in self.live_children if d != self.original}
        if self.live_album_date != self.original:
            out.add(self.live_album_id)
        return out


@dataclass
class RunResult:
    world: World
    status: str
    set_calls: list[tuple[str, str, bool]] = field(default_factory=list)


worlds = st.builds(
    World,
    original=st.sampled_from(_DATES),
    snapshot_children=st.lists(
        st.sampled_from(_CHILD_POOL), unique=True, max_size=4),
    age_minutes=st.integers(min_value=0, max_value=TTL_HOURS * 60 * 2),
    find_outcome=st.sampled_from(["present", "present", "absent", "raises"]),
    children_raises=st.booleans(),
    live_album_recreated=st.booleans(),
    live_album_date=st.sampled_from(_DATES),
    live_children=st.lists(
        st.tuples(st.sampled_from(_CHILD_POOL), st.sampled_from(_DATES)),
        unique_by=lambda t: t[0], max_size=4),
    set_fail_ids=st.sets(st.sampled_from(_CHILD_POOL + ["alb-same", "alb-new"])),
)


def _run_reconcile(world: World) -> RunResult:
    db = FakePipelineDB()
    pin_id = db.add_jellyfin_date_created_pin(
        imported_path="A/B", original_date_created=world.original,
        album_item_id="alb-same", children_item_ids=list(world.snapshot_children),
        request_id=1)
    db.jellyfin_date_created_pins[-1]["captured_at"] = (
        NOW - timedelta(minutes=world.age_minutes))
    set_calls: list[tuple[str, str, bool]] = []

    def find_fn(cfg, path):
        if world.find_outcome == "raises":
            raise RuntimeError("jellyfin down")
        if world.find_outcome == "absent":
            return None
        return JellyfinAlbumRef(item_id=world.live_album_id,
                                date_created=world.live_album_date)

    def children_fn(cfg, item_id):
        if world.children_raises:
            raise RuntimeError("jellyfin down")
        return [JellyfinItemRef(item_id=i, date_created=d)
                for i, d in world.live_children]

    def set_fn(cfg, item_id, value):
        ok = item_id not in world.set_fail_ids
        set_calls.append((item_id, value, ok))
        return ok

    reconcile_jellyfin_date_created_pins(
        _cfg(), db, now=NOW, grace_seconds=GRACE_SECONDS, ttl_hours=TTL_HOURS,
        find_fn=find_fn, children_fn=children_fn, set_fn=set_fn)
    status = next(p["status"] for p in db.jellyfin_date_created_pins
                  if p["id"] == pin_id)
    return RunResult(world=world, status=status, set_calls=set_calls)


# --- Invariant checkers (module-level so the known-bad self-tests can call
#     them directly on planted violations) ---


def assert_only_original_written(res: RunResult) -> None:
    """P1: every write carries the pin's captured original value."""
    for item_id, value, _ok in res.set_calls:
        assert value == res.world.original, (
            f"wrote {value!r} to {item_id}; only the captured original "
            f"{res.world.original!r} may ever be written")


def assert_unsettled_never_finalized(res: RunResult) -> None:
    """P2: rescan not observably settled ⇒ zero writes, pending-until-TTL /
    expired-after. Covers both the ids-unchanged case and the mid-scan
    zero-children window."""
    w = res.world
    if not w.past_grace:
        assert res.set_calls == [], "pin inside grace window must be untouched"
        assert res.status == "pending"
        return
    if w.find_outcome != "present" or w.children_raises or w.settled:
        return
    assert res.set_calls == [], (
        "rescan not observably settled but the reconciler wrote — "
        "it would pin doomed items and close the pin with nothing left")
    expected = "expired" if w.past_ttl else "pending"
    assert res.status == expected, (
        f"unsettled pin must be {expected}, got {res.status}")


def assert_terminal_state_correct(res: RunResult) -> None:
    """P3: done/skipped/pending each ⟺ the world facts that justify them."""
    w = res.world
    if not w.past_grace:
        return
    if w.find_outcome == "absent":
        assert res.status == "skipped", "album gone must mark the pin skipped"
        return
    assert res.status != "skipped", "skipped is reserved for album-gone"
    if res.status == "done":
        assert w.find_outcome == "present" and not w.children_raises
        assert w.settled, "done before the rescan observably settled"
        assert all(ok for _, _, ok in res.set_calls), (
            "done despite a failed write — that write is lost forever")
        written = {i for i, _, _ in res.set_calls}
        assert written == w.drifted_ids, (
            f"done but wrote {written} while drifted set is {w.drifted_ids}")
    if w.find_outcome == "raises" or (w.find_outcome == "present"
                                      and w.children_raises):
        assert res.status == "pending", "client errors must leave the pin pending"
    if any(not ok for _, _, ok in res.set_calls):
        assert res.status == "pending", "a failed write must leave the pin pending"


class TestReconcileProperties(unittest.TestCase):
    @given(worlds)
    def test_only_original_written(self, world: World):
        assert_only_original_written(_run_reconcile(world))

    @given(worlds)
    def test_unsettled_never_finalized(self, world: World):
        assert_unsettled_never_finalized(_run_reconcile(world))

    @given(worlds)
    def test_terminal_state_correct(self, world: World):
        assert_terminal_state_correct(_run_reconcile(world))


class TestCaptureProperty(unittest.TestCase):
    @given(
        found=st.booleans(),
        date=st.sampled_from(_DATES),
        children=st.lists(st.sampled_from(_CHILD_POOL), unique=True, max_size=4),
    )
    def test_capture_snapshot_mirrors_finder_or_writes_nothing(
            self, found: bool, date: str, children: list[str]):
        db = FakePipelineDB()
        ref = JellyfinAlbumRef(item_id="alb-1", date_created=date) if found else None
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "Artist/2026 - Album", 42,
            find_fn=lambda cfg, path: ref,
            children_fn=lambda cfg, iid: [
                JellyfinItemRef(item_id=i, date_created="D-old-1")
                for i in children])
        if not found:
            self.assertEqual(res.outcome, "no_album")
            self.assertEqual(db.jellyfin_date_created_pins, [])
            return
        self.assertEqual(res.outcome, "captured")
        pin = db.jellyfin_date_created_pins[0]
        self.assertEqual(pin["original_date_created"], date)
        self.assertEqual(pin["album_item_id"], "alb-1")
        self.assertEqual(pin["children_item_ids"], children)
        self.assertEqual(pin["imported_path"], "Artist/2026 - Album")
        self.assertEqual(pin["request_id"], 42)
        self.assertEqual(pin["status"], "pending")


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: prove the harness detects what it claims to."""

    def _world(self, **kw) -> World:
        base = dict(
            original="D-orig", snapshot_children=["c1"], age_minutes=10,
            find_outcome="present", children_raises=False,
            live_album_recreated=False, live_album_date="D-orig",
            live_children=[("c1", "D-old-1")], set_fail_ids=set())
        base.update(kw)
        return World(**base)  # type: ignore[arg-type]

    def test_only_original_checker_trips_on_foreign_value(self):
        res = RunResult(world=self._world(), status="pending",
                        set_calls=[("c1", "D-bumped", True)])
        with self.assertRaises(AssertionError):
            assert_only_original_written(res)

    def test_unsettled_checker_trips_on_write_before_rescan(self):
        # ids match the snapshot (not landed) yet a write happened.
        res = RunResult(world=self._world(), status="pending",
                        set_calls=[("c1", "D-orig", True)])
        with self.assertRaises(AssertionError):
            assert_unsettled_never_finalized(res)

    def test_unsettled_checker_trips_on_premature_done(self):
        res = RunResult(world=self._world(), status="done", set_calls=[])
        with self.assertRaises(AssertionError):
            assert_unsettled_never_finalized(res)

    def test_unsettled_checker_trips_on_write_in_midscan_window(self):
        # Landed (children id-set changed) but zero live children against a
        # non-empty snapshot: the mid-scan window — writing/closing now
        # orphans the about-to-arrive new items.
        res = RunResult(
            world=self._world(live_album_recreated=True, live_children=[]),
            status="done", set_calls=[])
        with self.assertRaises(AssertionError):
            assert_unsettled_never_finalized(res)

    def test_terminal_checker_trips_on_done_with_failed_write(self):
        res = RunResult(
            world=self._world(live_children=[("n1", "D-bumped")],
                              set_fail_ids={"n1"}),
            status="done", set_calls=[("n1", "D-orig", False)])
        with self.assertRaises(AssertionError):
            assert_terminal_state_correct(res)

    def test_terminal_checker_trips_on_skip_of_present_album(self):
        res = RunResult(world=self._world(), status="skipped", set_calls=[])
        with self.assertRaises(AssertionError):
            assert_terminal_state_correct(res)


if __name__ == "__main__":
    unittest.main()
