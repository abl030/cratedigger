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
2. **P2 (never finalize before the rescan lands)** — the pin stays pending
   until an item id changes (a None snapshot — a floor pin — matches any
   album) or an Audio ``DateCreated`` becomes newer than the captured album
   maximum. The date branch is load-bearing because Jellyfin can restamp a
   same-path Audio row without changing its id. An ABSENT album is a wait
   signal, not a terminal one: after a path-changing upgrade the pinned
   (new) path only exists in Jellyfin once the rescan lands.
3. **P3 (terminal-state correctness)** — done ⟺ the rescan landed, every
   drifted item (album + audio children) was written, and no write failed;
   skipped ⟺ the album was still absent at TTL; a failed write or an
   erroring client always leaves the pin pending.
4. **P4 (capture snapshot fidelity)** — a capture persists the earlier of
   Jellyfin's maximum Audio ``DateCreated`` and Plex's preserved historical
   ``addedAt``, plus the item ids, path, and request id; a genuinely new album
   persists nothing.
5. **P5 (an upgrade is never left unpinned)** — when replaced beets albums
   prove the import was an upgrade, capture writes a pin whenever ANY date
   source exists: a pre-upgrade item at the new path, one at a replaced old
   path, or the pipeline floor (min of Plex history and the oldest
   ``created_at`` across the request's replace chain) — and the pinned value
   is the correct composition for that source.

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

from hypothesis import example, given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.jellyfin_pin_service import (
    CaptureResult,
    capture_jellyfin_date_created_pin,
    reconcile_jellyfin_date_created_pins,
)
from lib.util import JellyfinAlbumRef, JellyfinItemRef
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
GRACE_SECONDS = 180
TTL_HOURS = 48

_DATE_INSTANTS = [
    "2025-01-01T00:00:00",
    "2025-06-01T00:00:00",
    "2026-01-01T00:00:00",
    "2026-07-15T00:00:00",
]
# Pipeline floors are seconds-only; Jellyfin emits seven fractional digits.
# The zero-fraction values deliberately spell the same instants two ways.
_SECONDS_DATES = [f"{instant}Z" for instant in _DATE_INSTANTS]
_JELLYFIN_DATES = [
    date
    for instant in _DATE_INSTANTS
    for date in (f"{instant}.0000000Z", f"{instant}.5000000Z")
]
_DATES = _SECONDS_DATES + _JELLYFIN_DATES
_CHILD_POOL = ["c1", "c2", "c3", "n1", "n2"]


def _parse_iso_date(value: str) -> datetime:
    """Parse a generated ISO date independently of production code."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _date_newer(a: str, b: str) -> bool:
    return _parse_iso_date(a) > _parse_iso_date(b)


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
    # A floor pin (path-changing upgrade, no pre-upgrade item findable at
    # capture): the snapshot album id is NULL and any live album counts as
    # the landed rescan.
    snapshot_album_is_none: bool = False

    @property
    def live_album_id(self) -> str:
        return "alb-new" if self.live_album_recreated else "alb-same"

    @property
    def landed(self) -> bool:
        return (self.snapshot_album_is_none
                or self.live_album_recreated
                or {i for i, _ in self.live_children} != set(self.snapshot_children)
                or any(_date_newer(date, self.original)
                       for _, date in self.live_children))

    @property
    def settled(self) -> bool:
        """Landed AND not in the mid-scan zero-children window."""
        return self.landed and bool(self.live_children)

    @property
    def past_grace(self) -> bool:
        return self.age_minutes * 60 > GRACE_SECONDS

    @property
    def past_ttl(self) -> bool:
        return self.age_minutes > TTL_HOURS * 60

    @property
    def drifted_ids(self) -> set[str]:
        out = {i for i, d in self.live_children
               if _date_newer(d, self.original)}
        if _date_newer(self.live_album_date, self.original):
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
    live_album_date=st.sampled_from(_JELLYFIN_DATES),
    live_children=st.lists(
        st.tuples(
            st.sampled_from(_CHILD_POOL),
            st.sampled_from(_JELLYFIN_DATES),
        ),
        unique_by=lambda t: t[0], max_size=4),
    set_fail_ids=st.sets(st.sampled_from(_CHILD_POOL + ["alb-same", "alb-new"])),
    snapshot_album_is_none=st.booleans(),
)


def _run_reconcile(world: World) -> RunResult:
    db = FakePipelineDB()
    pin_id = db.add_jellyfin_date_created_pin(
        imported_path="A/B", original_date_created=world.original,
        album_item_id=(None if world.snapshot_album_is_none else "alb-same"),
        children_item_ids=list(world.snapshot_children),
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
    terminal-after. Covers unchanged ids with no newer dates, the mid-scan
    zero-children window, and the absent album (the pinned path may not
    exist in Jellyfin until the rescan lands)."""
    w = res.world
    if not w.past_grace:
        assert res.set_calls == [], "pin inside grace window must be untouched"
        assert res.status == "pending"
        return
    if w.find_outcome == "absent":
        assert res.set_calls == [], "absent album ⇒ nothing to write to"
        expected = "skipped" if w.past_ttl else "pending"
        assert res.status == expected, (
            f"absent album must wait then skip at TTL: expected {expected}, "
            f"got {res.status}")
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
        expected = "skipped" if w.past_ttl else "pending"
        assert res.status == expected, (
            "absent album must wait (pending), closing as skipped only at TTL")
        return
    assert res.status != "skipped", "skipped is reserved for absent-at-TTL"
    if res.status == "done":
        assert w.find_outcome == "present" and not w.children_raises
        assert w.settled, "done before the rescan observably settled"
        assert all(ok for _, _, ok in res.set_calls), (
            "done despite a failed write — that write is lost forever")
        written = {i for i, _, _ in res.set_calls}
        assert written == w.drifted_ids, (
            f"done but wrote {written} while drifted set is {w.drifted_ids}")
    if (w.find_outcome == "present" and not w.children_raises and w.settled):
        expected = (
            "pending" if any(not ok for _, _, ok in res.set_calls) else "done"
        )
        assert res.status == expected, (
            f"settled rescan must be {expected}, got {res.status}")
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
    @example(World(
        original="2026-07-15T00:00:00Z",
        snapshot_children=["c1"],
        age_minutes=10,
        find_outcome="present",
        children_raises=False,
        live_album_recreated=False,
        live_album_date="2026-07-15T00:00:00.0000000Z",
        live_children=[("c1", "2026-07-15T00:00:00.5000000Z")],
        set_fail_ids=set(),
    ))
    def test_terminal_state_correct(self, world: World):
        assert_terminal_state_correct(_run_reconcile(world))


class TestCaptureProperty(unittest.TestCase):
    @given(
        found=st.booleans(),
        album_date=st.sampled_from(_JELLYFIN_DATES),
        children=st.lists(
            st.tuples(
                st.sampled_from(_CHILD_POOL),
                st.sampled_from(_JELLYFIN_DATES),
            ),
            unique_by=lambda pair: pair[0],
            max_size=4,
        ),
        historical_date=st.one_of(
            st.none(), st.sampled_from(_SECONDS_DATES)),
    )
    def test_capture_snapshot_mirrors_finder_or_writes_nothing(
            self, found: bool, album_date: str,
            children: list[tuple[str, str]], historical_date: str | None):
        db = FakePipelineDB()
        ref = (
            JellyfinAlbumRef(item_id="alb-1", date_created=album_date)
            if found else None
        )
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "Artist/2026 - Album", 42,
            historical_added_at=(
                int(datetime.fromisoformat(
                    historical_date.replace("Z", "+00:00")
                ).timestamp())
                if historical_date is not None else None
            ),
            find_fn=lambda cfg, path: ref,
            children_fn=lambda cfg, iid: [
                JellyfinItemRef(item_id=item_id, date_created=date)
                for item_id, date in children])
        if not found:
            self.assertEqual(res.outcome, "no_album")
            self.assertEqual(db.jellyfin_date_created_pins, [])
            return
        self.assertEqual(res.outcome, "captured")
        pin = db.jellyfin_date_created_pins[0]
        expected_date = max(
            (date for _, date in children),
            default=album_date,
            key=_parse_iso_date,
        )
        if (historical_date is not None
                and _date_newer(expected_date, historical_date)):
            expected_date = historical_date
        self.assertEqual(pin["original_date_created"], expected_date)
        self.assertEqual(pin["album_item_id"], "alb-1")
        self.assertEqual(
            pin["children_item_ids"],
            [item_id for item_id, _ in children],
        )
        self.assertEqual(pin["imported_path"], "Artist/2026 - Album")
        self.assertEqual(pin["request_id"], 42)
        self.assertEqual(pin["status"], "pending")


@dataclass
class CaptureWorld:
    """One generated upgrade-capture scenario: where (if anywhere) the
    pre-upgrade item is findable, and which floor sources exist."""
    album_at_new: bool
    album_at_old: bool
    has_replaced: bool
    chain_created: str | None      # oldest replace-chain created_at, or None
    historical: str | None         # Plex preserved addedAt, or None
    album_date: str
    children: list[tuple[str, str]]


capture_worlds = st.builds(
    CaptureWorld,
    album_at_new=st.booleans(),
    album_at_old=st.booleans(),
    has_replaced=st.booleans(),
    chain_created=st.one_of(st.none(), st.sampled_from(_SECONDS_DATES)),
    historical=st.one_of(st.none(), st.sampled_from(_SECONDS_DATES)),
    album_date=st.sampled_from(_JELLYFIN_DATES),
    children=st.lists(
        st.tuples(
            st.sampled_from(_CHILD_POOL),
            st.sampled_from(_JELLYFIN_DATES),
        ),
        unique_by=lambda t: t[0], max_size=4),
)


def _iso_epoch(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


def _run_capture_fallback(
        w: CaptureWorld) -> tuple[FakePipelineDB, CaptureResult]:
    db = FakePipelineDB()
    if w.chain_created is not None:
        db.seed_request(make_request_row(
            id=42,
            created_at=datetime.fromisoformat(
                w.chain_created.replace("Z", "+00:00"))))

    def find_fn(cfg, path):
        if path == "New/Path" and w.album_at_new:
            return JellyfinAlbumRef(item_id="alb-live",
                                    date_created=w.album_date)
        if path == "Old/Path" and w.album_at_old:
            return JellyfinAlbumRef(item_id="alb-old",
                                    date_created=w.album_date)
        return None

    res = capture_jellyfin_date_created_pin(
        _cfg(), db, "New/Path", 42,
        historical_added_at=(
            _iso_epoch(w.historical) if w.historical is not None else None),
        replaced_album_paths=(["Old/Path"] if w.has_replaced else []),
        find_fn=find_fn,
        children_fn=lambda cfg, iid: [
            JellyfinItemRef(item_id=i, date_created=d)
            for i, d in w.children])
    return db, res


def assert_capture_fallback_correct(
        w: CaptureWorld, db: FakePipelineDB, res: CaptureResult) -> None:
    """P5: replaced albums ⇒ a pin whenever any date source exists, with the
    correct value for the source that won."""
    found = w.album_at_new or (w.has_replaced and w.album_at_old)
    if found:
        assert res.outcome == "captured", (
            f"pre-upgrade item findable but outcome={res.outcome}")
        pin = db.jellyfin_date_created_pins[0]
        expected = max(
            (d for _, d in w.children),
            default=w.album_date,
            key=_parse_iso_date,
        )
        if (w.historical is not None
                and _date_newer(expected, w.historical)):
            expected = w.historical
        assert pin["original_date_created"] == expected
        assert pin["album_item_id"] == (
            "alb-live" if w.album_at_new else "alb-old")
        assert pin["imported_path"] == "New/Path", (
            "the pin must join on the NEW path — that's where the "
            "reconciler finds the post-rescan items")
        return
    floor_sources = [d for d in (w.historical, w.chain_created)
                     if d is not None]
    if w.has_replaced and floor_sources:
        assert res.outcome == "floor_captured", (
            f"upgrade proven with a floor source but outcome={res.outcome} "
            "— the upgrade would surface as newly added")
        pin = db.jellyfin_date_created_pins[0]
        assert pin["album_item_id"] is None
        assert pin["children_item_ids"] == []
        assert pin["original_date_created"] == min(
            floor_sources, key=_parse_iso_date)
        assert pin["imported_path"] == "New/Path"
        return
    assert res.outcome == "no_album"
    assert db.jellyfin_date_created_pins == []


class TestCaptureFallbackProperty(unittest.TestCase):
    @given(capture_worlds)
    def test_upgrades_always_pin_with_best_available_date(
            self, w: CaptureWorld):
        db, res = _run_capture_fallback(w)
        assert_capture_fallback_correct(w, db, res)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: prove the harness detects what it claims to."""

    def _world(self, **kw) -> World:
        base = dict(
            original="2026-01-01T00:00:00Z", snapshot_children=["c1"],
            age_minutes=10,
            find_outcome="present", children_raises=False,
            live_album_recreated=False,
            live_album_date="2026-01-01T00:00:00Z",
            live_children=[("c1", "2025-01-01T00:00:00Z")],
            set_fail_ids=set())
        base.update(kw)
        return World(**base)  # type: ignore[arg-type]

    def test_only_original_checker_trips_on_foreign_value(self):
        res = RunResult(world=self._world(), status="pending",
                        set_calls=[("c1", "2026-07-15T00:00:00Z", True)])
        with self.assertRaises(AssertionError):
            assert_only_original_written(res)

    def test_unsettled_checker_trips_on_write_before_rescan(self):
        # Ids match and no current date is newer (not landed), yet a write happened.
        res = RunResult(world=self._world(), status="pending",
                        set_calls=[("c1", "2026-01-01T00:00:00Z", True)])
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
            world=self._world(
                live_children=[("n1", "2026-07-15T00:00:00Z")],
                set_fail_ids={"n1"}),
            status="done",
            set_calls=[("n1", "2026-01-01T00:00:00Z", False)])
        with self.assertRaises(AssertionError):
            assert_terminal_state_correct(res)

    def test_terminal_checker_trips_on_lexically_missed_subsecond_bump(self):
        # Planted lexical-comparison result: '.5' sorts before 'Z', so the
        # reconciler stayed pending even though the child is newer in time.
        res = RunResult(
            world=self._world(
                original="2026-07-15T00:00:00Z",
                live_album_date="2026-07-15T00:00:00.0000000Z",
                live_children=[("c1", "2026-07-15T00:00:00.5000000Z")],
            ),
            status="pending",
            set_calls=[],
        )
        with self.assertRaises(AssertionError):
            assert_terminal_state_correct(res)

    def test_terminal_checker_trips_on_skip_of_present_album(self):
        res = RunResult(world=self._world(), status="skipped", set_calls=[])
        with self.assertRaises(AssertionError):
            assert_terminal_state_correct(res)

    def test_checkers_trip_on_premature_skip_of_absent_album(self):
        # The pre-fix behavior (absent ⇒ instant skip): after a path-changing
        # upgrade the pinned path is empty until the rescan lands, so an
        # early skip strands the pin. Both P2 and P3 must catch a revert.
        res = RunResult(world=self._world(find_outcome="absent"),
                        status="skipped", set_calls=[])
        with self.assertRaises(AssertionError):
            assert_unsettled_never_finalized(res)
        with self.assertRaises(AssertionError):
            assert_terminal_state_correct(res)

    def test_unsettled_checker_trips_on_floor_pin_closed_before_children(self):
        # Floor pin landed on the album row alone (zero children yet): the
        # mid-scan window. Closing now leaves the children unclamped.
        res = RunResult(
            world=self._world(snapshot_album_is_none=True,
                              snapshot_children=[], live_children=[]),
            status="done", set_calls=[])
        with self.assertRaises(AssertionError):
            assert_unsettled_never_finalized(res)

    def test_capture_fallback_checker_trips_on_missing_floor_pin(self):
        # An upgrade with a derivable floor whose capture wrote nothing —
        # exactly the 2026-07-16 incident shape.
        w = CaptureWorld(
            album_at_new=False, album_at_old=False, has_replaced=True,
            chain_created="2026-01-01T00:00:00Z", historical=None,
            album_date="2026-01-01T00:00:00Z", children=[])
        with self.assertRaises(AssertionError):
            assert_capture_fallback_correct(
                w, FakePipelineDB(), CaptureResult("no_album"))

    def test_capture_fallback_checker_trips_on_wrong_floor_value(self):
        w = CaptureWorld(
            album_at_new=False, album_at_old=False, has_replaced=True,
            chain_created="2025-01-01T00:00:00Z",
            historical="2026-01-01T00:00:00Z",
            album_date="2026-01-01T00:00:00Z", children=[])
        db = FakePipelineDB()
        # Planted violation: pinned the NEWER source, not the min.
        db.add_jellyfin_date_created_pin(
            imported_path="New/Path",
            original_date_created="2026-01-01T00:00:00Z",
            album_item_id=None, children_item_ids=[], request_id=42)
        with self.assertRaises(AssertionError):
            assert_capture_fallback_correct(
                w, db, CaptureResult("floor_captured", 1,
                                     "2026-01-01T00:00:00Z"))


if __name__ == "__main__":
    unittest.main()
