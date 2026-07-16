"""Orchestration tests for lib.jellyfin_pin_service (migration 046, issue #574).

Capture/reconcile are driven against a real FakePipelineDB with the Jellyfin
client (find/children/set) injected as kwarg-DI seams — no network, no
MagicMock db. Every assertion targets persisted pin state, per the
orchestration-test rule.

These are the deterministic pins for the issue #574 invariants; the generated
properties that patrol the same invariants live in
tests/test_jellyfin_pins_generated.py.
"""
import unittest
from datetime import datetime, timedelta, timezone

from lib.config import CratediggerConfig
from lib.util import JellyfinAlbumRef, JellyfinItemRef
from lib.jellyfin_pin_service import (
    capture_jellyfin_date_created_pin,
    reconcile_jellyfin_date_created_pins,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
ORIGINAL = "2026-04-26T18:31:04.4425337Z"
BUMPED = "2026-07-10T11:55:00.0000000Z"


def _cfg(**kw):
    kw.setdefault("jellyfin_url", "http://jellyfin:8096")
    kw.setdefault("jellyfin_token", "tok")
    return CratediggerConfig(**kw)


def _album(item_id="alb-1", date_created=ORIGINAL):
    return JellyfinAlbumRef(item_id=item_id, date_created=date_created,
                            name="The Wow! Signal", artist="Muse")


def _children(*pairs):
    return [JellyfinItemRef(item_id=i, date_created=d) for i, d in pairs]


class TestCapture(unittest.TestCase):
    def test_captured_persists_pin_with_album_and_children_snapshot(self):
        db = FakePipelineDB()
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "Muse/2026 - The Wow! Signal", 8812,
            find_fn=lambda cfg, path: _album(),
            children_fn=lambda cfg, iid: _children(
                ("tr-1", "2026-01-01T00:00:00Z"), ("tr-2", "2026-01-02T00:00:00Z")))
        self.assertEqual(res.outcome, "captured")
        self.assertEqual(
            res.original_date_created, "2026-01-02T00:00:00Z")
        self.assertEqual(len(db.jellyfin_date_created_pins), 1)
        pin = db.jellyfin_date_created_pins[0]
        self.assertEqual(pin["imported_path"], "Muse/2026 - The Wow! Signal")
        self.assertEqual(
            pin["original_date_created"], "2026-01-02T00:00:00Z")
        self.assertEqual(pin["album_item_id"], "alb-1")
        self.assertEqual(pin["children_item_ids"], ["tr-1", "tr-2"])
        self.assertEqual(pin["request_id"], 8812)
        self.assertEqual(pin["status"], "pending")

    def test_plex_history_clamps_a_polluted_jellyfin_baseline(self):
        """A Jellyfin rebuild must not redefine an old album as newly added."""
        db = FakePipelineDB()
        historical = int(datetime(
            2010, 6, 10, 10, 10, 38, tzinfo=timezone.utc
        ).timestamp())
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "Eldar/2010 - Amaterasu Shiroi", 2506,
            historical_added_at=historical,
            find_fn=lambda cfg, path: _album(
                date_created="2026-07-14T02:03:57.0000000Z"),
            children_fn=lambda cfg, iid: _children(
                ("tr-1", "2026-07-14T02:03:57.0000000Z")))
        self.assertEqual(res.outcome, "captured")
        self.assertEqual(res.original_date_created, "2010-06-10T10:10:38Z")
        self.assertEqual(
            db.jellyfin_date_created_pins[0]["original_date_created"],
            "2010-06-10T10:10:38Z",
        )

    def test_plex_history_never_moves_a_jellyfin_baseline_forward(self):
        db = FakePipelineDB()
        later = int(datetime(
            2026, 7, 11, tzinfo=timezone.utc
        ).timestamp())
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "A/B", 1,
            historical_added_at=later,
            find_fn=lambda cfg, path: _album(),
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.original_date_created, ORIGINAL)

    def test_no_album_writes_no_pin(self):
        # Invariant 1: genuinely-new albums self-select out — no pin.
        db = FakePipelineDB()
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "New/Album", 1,
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "no_album")
        self.assertEqual(db.jellyfin_date_created_pins, [])

    def test_disabled_when_jellyfin_unconfigured(self):
        db = FakePipelineDB()
        calls = []
        res = capture_jellyfin_date_created_pin(
            CratediggerConfig(), db, "A/B", 1,
            find_fn=lambda cfg, path: calls.append(path),
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "disabled")
        self.assertEqual(calls, [])  # never looked up Jellyfin
        self.assertEqual(db.jellyfin_date_created_pins, [])

    def test_disabled_when_no_token(self):
        db = FakePipelineDB()
        res = capture_jellyfin_date_created_pin(
            CratediggerConfig(jellyfin_url="http://jf:8096"), db, "A/B", 1,
            find_fn=lambda cfg, path: _album(),
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "disabled")

    def test_disabled_when_no_imported_path(self):
        db = FakePipelineDB()
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, None, 1,
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "disabled")

    def test_error_on_lookup_failure_writes_no_pin(self):
        db = FakePipelineDB()

        def _boom(cfg, path):
            raise RuntimeError("jellyfin down")
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "A/B", 1, find_fn=_boom,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "error")
        self.assertEqual(db.jellyfin_date_created_pins, [])

    def test_error_on_children_failure_writes_no_pin(self):
        # Without the children snapshot the landed-detector can't work, so a
        # children fetch failure must not persist a half-formed pin.
        db = FakePipelineDB()

        def _boom(cfg, iid):
            raise RuntimeError("jellyfin down")
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "A/B", 1,
            find_fn=lambda cfg, path: _album(), children_fn=_boom)
        self.assertEqual(res.outcome, "error")
        self.assertEqual(db.jellyfin_date_created_pins, [])

    def test_error_on_persist_failure(self):
        class FailingDB(FakePipelineDB):
            def add_jellyfin_date_created_pin(self, **kw):
                raise RuntimeError("db down")
        res = capture_jellyfin_date_created_pin(
            _cfg(), FailingDB(), "A/B", 1,
            find_fn=lambda cfg, path: _album(),
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "error")


class TestCapturePathChangedUpgrade(unittest.TestCase):
    """A path-changing upgrade (the 2026-07-16 Arcade Fire incident): the
    pre-upgrade Jellyfin items live only at the replaced beets albums' old
    paths, or nowhere findable at all."""

    OLD = "Arcade Fire/2007 - B-Sides & Rarities"
    NEW = "Arcade Fire/0000 - B-Sides & Rarities"

    def test_capture_falls_back_to_replaced_album_old_path(self):
        db = FakePipelineDB()
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, self.NEW, 8504,
            replaced_album_paths=[self.OLD],
            find_fn=lambda cfg, path: _album() if path == self.OLD else None,
            children_fn=lambda cfg, iid: _children(
                ("tr-1", "2026-01-01T00:00:00Z")))
        self.assertEqual(res.outcome, "captured")
        pin = db.jellyfin_date_created_pins[0]
        # The pin joins on the NEW path — that's where the reconciler must
        # look for the post-rescan items — while the snapshot holds the OLD
        # item's identity and date.
        self.assertEqual(pin["imported_path"], self.NEW)
        self.assertEqual(pin["album_item_id"], "alb-1")
        self.assertEqual(pin["children_item_ids"], ["tr-1"])
        self.assertEqual(pin["original_date_created"], "2026-01-01T00:00:00Z")

    def test_capture_prefers_the_new_path_when_it_resolves(self):
        # Same-path upgrade with replaced albums listed: the new-path lookup
        # already finds the pre-upgrade item; old paths are never consulted.
        db = FakePipelineDB()
        calls: list[str] = []

        def find(cfg, path):
            calls.append(path)
            return _album()
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "A/B", 1, replaced_album_paths=["A/OLD"],
            find_fn=find, children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "captured")
        self.assertEqual(calls, ["A/B"])

    def test_floor_pin_when_upgrade_proven_but_nothing_findable(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=8504,
            created_at=datetime(2026, 6, 4, 4, 45, 50, tzinfo=timezone.utc)))
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, self.NEW, 8504,
            replaced_album_paths=[self.OLD],
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: self.fail(
                "no album found — children must not be fetched"))
        self.assertEqual(res.outcome, "floor_captured")
        pin = db.jellyfin_date_created_pins[0]
        self.assertEqual(pin["imported_path"], self.NEW)
        self.assertIsNone(pin["album_item_id"])
        self.assertEqual(pin["children_item_ids"], [])
        self.assertEqual(pin["original_date_created"], "2026-06-04T04:45:50Z")
        self.assertEqual(pin["request_id"], 8504)

    def test_floor_uses_oldest_created_at_across_replace_chain(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=10, status="replaced",
            created_at=datetime(2026, 2, 1, tzinfo=timezone.utc)))
        db.seed_request(make_request_row(
            id=11, replaces_request_id=10,
            created_at=datetime(2026, 6, 1, tzinfo=timezone.utc)))
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "New/Path", 11,
            replaced_album_paths=["Old/Path"],
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "floor_captured")
        self.assertEqual(
            db.jellyfin_date_created_pins[0]["original_date_created"],
            "2026-02-01T00:00:00Z")

    def test_floor_prefers_an_older_plex_history(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, created_at=datetime(2026, 6, 4, tzinfo=timezone.utc)))
        historical = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "New/Path", 1,
            historical_added_at=historical,
            replaced_album_paths=["Old/Path"],
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "floor_captured")
        self.assertEqual(
            db.jellyfin_date_created_pins[0]["original_date_created"],
            "2026-04-01T00:00:00Z")

    def test_floor_ignores_a_newer_plex_history(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, created_at=datetime(2026, 6, 4, tzinfo=timezone.utc)))
        historical = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp())
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "New/Path", 1,
            historical_added_at=historical,
            replaced_album_paths=["Old/Path"],
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(
            db.jellyfin_date_created_pins[0]["original_date_created"],
            "2026-06-04T00:00:00Z")

    def test_no_floor_source_writes_no_pin(self):
        # Upgrade proven but request unknown and no Plex history: there is
        # no date to pin, so nothing is written (logged, best-effort).
        db = FakePipelineDB()
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "New/Path", None,
            replaced_album_paths=["Old/Path"],
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "no_album")
        self.assertEqual(db.jellyfin_date_created_pins, [])

    def test_floor_db_failure_is_error(self):
        class FailingDB(FakePipelineDB):
            def get_oldest_request_chain_created_at(self, request_id):
                raise RuntimeError("db down")
        db = FailingDB()
        res = capture_jellyfin_date_created_pin(
            _cfg(), db, "New/Path", 1,
            replaced_album_paths=["Old/Path"],
            find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [])
        self.assertEqual(res.outcome, "error")
        self.assertEqual(db.jellyfin_date_created_pins, [])


class TestReconcile(unittest.TestCase):
    def _seed(self, db, *, path="A/B", original=ORIGINAL,
              album_item_id: str | None = "alb-1",
              children=("tr-1", "tr-2"), captured_at=None):
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path=path, original_date_created=original,
            album_item_id=album_item_id, children_item_ids=list(children),
            request_id=1)
        db.jellyfin_date_created_pins[-1]["captured_at"] = (
            captured_at or NOW - timedelta(minutes=10))
        return pin_id

    def _pin(self, db, pin_id):
        return next(p for p in db.jellyfin_date_created_pins if p["id"] == pin_id)

    def _reconcile(self, db, *, find_fn, children_fn, set_fn, cfg=None, **kw):
        return reconcile_jellyfin_date_created_pins(
            cfg or _cfg(), db, now=NOW, find_fn=find_fn,
            children_fn=children_fn, set_fn=set_fn, **kw)

    # --- Invariant 2: never finalize before the rescan is observable ---

    def test_unlanded_pin_waits_with_zero_writes(self):
        # Album item-id and children id-set both match the capture snapshot:
        # the rescan has not landed. The pin must stay pending, untouched.
        db = FakePipelineDB()
        pin_id = self._seed(db, children=("tr-1", "tr-2"))
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(item_id="alb-1"),
            # Same id-set, dates still the pre-upgrade originals (old items).
            children_fn=lambda cfg, iid: _children(
                ("tr-2", "2025-01-01T00:00:00Z"), ("tr-1", "2025-01-02T00:00:00Z")),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.waiting, 1)
        self.assertEqual(set_calls, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_unlanded_pin_past_ttl_expires_with_zero_writes(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, captured_at=NOW - timedelta(hours=49))
        set_calls = []
        res = self._reconcile(
            db, ttl_hours=48,
            find_fn=lambda cfg, path: _album(item_id="alb-1"),
            children_fn=lambda cfg, iid: _children(
                ("tr-1", "2025-01-01T00:00:00Z"), ("tr-2", "2025-01-02T00:00:00Z")),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.expired, 1)
        self.assertEqual(set_calls, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "expired")

    def test_midscan_empty_children_waits(self):
        # Landed by album recreation, but zero audio children against a
        # non-empty snapshot: the mid-scan window (old items deleted, new not
        # yet inserted). Writing/closing now would orphan the new items.
        db = FakePipelineDB()
        pin_id = self._seed(db, album_item_id="alb-old", children=("tr-1",))
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(item_id="alb-new", date_created=BUMPED),
            children_fn=lambda cfg, iid: [],
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.waiting, 1)
        self.assertEqual(set_calls, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    # --- Invariant 3: once landed, every drifted item is restored ---

    def test_landed_via_children_change_restores_album_and_drifted_children(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, children=("tr-1", "tr-2"))
        set_calls = []
        res = self._reconcile(
            db,
            # Album item survived but its tracks were replaced (the BT case):
            # new child ids, ctime-fresh dates; one child already correct.
            find_fn=lambda cfg, path: _album(item_id="alb-1", date_created=ORIGINAL),
            children_fn=lambda cfg, iid: _children(
                ("new-1", BUMPED), ("new-2", BUMPED), ("new-3", ORIGINAL)),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.pinned, 1)
        # Album already at original → not written; the two drifted children
        # are written back to the ORIGINAL value (invariant 4).
        self.assertEqual(set_calls, [("new-1", ORIGINAL), ("new-2", ORIGINAL)])
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    def test_subsecond_newer_child_lands_despite_mixed_iso_formats(self):
        """A seconds-only original ("…44Z", floor/historical format) vs
        Jellyfin's 7-digit fraction ("…44.5000000Z"): naive string
        comparison sorts '.' before 'Z' and would miss the newer date —
        the comparison must be chronological."""
        db = FakePipelineDB()
        original = "2026-07-15T21:46:44Z"
        subsecond_newer = "2026-07-15T21:46:44.5000000Z"
        pin_id = self._seed(db, original=original, children=("tr-1",))
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(
                item_id="alb-1", date_created=original),
            children_fn=lambda cfg, iid: _children(
                ("tr-1", subsecond_newer)),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.pinned, 1)
        self.assertEqual(set_calls, [("tr-1", original)])
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    def test_same_second_mixed_formats_do_not_bump(self):
        # Equal to the second across formats is NOT newer — no landing
        # signal, no write.
        db = FakePipelineDB()
        original = "2026-07-15T21:46:44Z"
        same_second = "2026-07-15T21:46:44.0000000Z"
        pin_id = self._seed(db, original=original, children=("tr-1",))
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(
                item_id="alb-1", date_created=same_second),
            children_fn=lambda cfg, iid: _children(("tr-1", same_second)),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.waiting, 1)
        self.assertEqual(set_calls, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_same_ids_with_newer_dates_are_a_landed_upgrade(self):
        """Jellyfin restamps changed same-path Audio rows without changing ids."""
        db = FakePipelineDB()
        pin_id = self._seed(db, children=("tr-1", "tr-2"))
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(
                item_id="alb-1", date_created=BUMPED),
            children_fn=lambda cfg, iid: _children(
                ("tr-1", BUMPED),
                ("tr-2", "2026-01-01T00:00:00Z"),
            ),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True,
        )
        self.assertEqual(res.pinned, 1)
        self.assertEqual(
            set_calls,
            [("alb-1", ORIGINAL), ("tr-1", ORIGINAL)],
        )
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    def test_landed_via_album_recreation_restores_album_too(self):
        # The Broderick case: the album item itself was deleted + recreated,
        # so its DateCreated is fresh as well.
        db = FakePipelineDB()
        pin_id = self._seed(db, album_item_id="alb-old", children=("tr-1",))
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(item_id="alb-new", date_created=BUMPED),
            children_fn=lambda cfg, iid: _children(("new-1", BUMPED)),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.pinned, 1)
        self.assertEqual(set_calls, [("alb-new", ORIGINAL), ("new-1", ORIGINAL)])
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    def test_landed_but_everything_already_original_is_already_correct(self):
        # A previous cycle's writes stuck; the follow-up pass verifies and
        # closes the pin without writing.
        db = FakePipelineDB()
        pin_id = self._seed(db, children=("tr-1",))
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(item_id="alb-1", date_created=ORIGINAL),
            children_fn=lambda cfg, iid: _children(("new-1", ORIGINAL)),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.already_correct, 1)
        self.assertEqual(set_calls, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    # --- Invariant 5: failed writes leave the pin pending for retry ---

    def test_write_failure_leaves_pin_pending(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, children=("tr-1", "tr-2"))
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(item_id="alb-1", date_created=ORIGINAL),
            children_fn=lambda cfg, iid: _children(
                ("new-1", BUMPED), ("new-2", BUMPED)),
            set_fn=lambda cfg, iid, val: iid != "new-2")  # second write fails
        self.assertEqual(res.errors, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_album_absent_waits(self):
        # Nothing at the pinned path yet: after a path-changing upgrade the
        # new folder only exists in Jellyfin once the rescan lands, so
        # absence is a wait signal — closing now would strand the pin.
        db = FakePipelineDB()
        pin_id = self._seed(db)
        res = self._reconcile(
            db, find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [],
            set_fn=lambda *a: True)
        self.assertEqual(res.waiting, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_album_absent_past_ttl_is_skipped(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, captured_at=NOW - timedelta(hours=49))
        res = self._reconcile(
            db, ttl_hours=48, find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [],
            set_fn=lambda *a: True)
        self.assertEqual(res.skipped, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "skipped")

    # --- Floor pins (path-changing upgrade with no findable pre-upgrade
    #     item): a None snapshot means ANY album at the path is the landed
    #     rescan ---

    def test_floor_pin_lands_when_album_with_children_appears(self):
        db = FakePipelineDB()
        floor = "2026-06-04T04:45:50Z"
        pin_id = self._seed(
            db, original=floor, album_item_id=None, children=())
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(
                item_id="alb-new", date_created=BUMPED),
            children_fn=lambda cfg, iid: _children(
                ("new-1", BUMPED), ("new-2", "2026-01-01T00:00:00Z")),
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.pinned, 1)
        # Only items NEWER than the floor are clamped; new-2 predates it.
        self.assertEqual(set_calls, [("alb-new", floor), ("new-1", floor)])
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    def test_floor_pin_waits_in_the_mid_scan_zero_children_window(self):
        # The album row exists but its Audio rows aren't inserted yet —
        # writing/closing now would miss the children.
        db = FakePipelineDB()
        pin_id = self._seed(db, album_item_id=None, children=())
        set_calls = []
        res = self._reconcile(
            db,
            find_fn=lambda cfg, path: _album(
                item_id="alb-new", date_created=BUMPED),
            children_fn=lambda cfg, iid: [],
            set_fn=lambda cfg, iid, val: set_calls.append((iid, val)) or True)
        self.assertEqual(res.waiting, 1)
        self.assertEqual(set_calls, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_find_exception_leaves_pin_pending(self):
        db = FakePipelineDB()
        pin_id = self._seed(db)

        def _boom(cfg, path):
            raise RuntimeError("jellyfin down")
        res = self._reconcile(
            db, find_fn=_boom, children_fn=lambda cfg, iid: [],
            set_fn=lambda *a: True)
        self.assertEqual(res.errors, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_children_exception_leaves_pin_pending(self):
        db = FakePipelineDB()
        pin_id = self._seed(db)

        def _boom(cfg, iid):
            raise RuntimeError("jellyfin down")
        res = self._reconcile(
            db, find_fn=lambda cfg, path: _album(),
            children_fn=_boom, set_fn=lambda *a: True)
        self.assertEqual(res.errors, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_pin_within_grace_window_is_not_processed(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, captured_at=NOW - timedelta(seconds=10))
        touched = []
        res = self._reconcile(
            db, grace_seconds=180,
            find_fn=lambda cfg, path: touched.append(path),
            children_fn=lambda cfg, iid: [],
            set_fn=lambda *a: True)
        self.assertEqual((res.pinned, res.skipped, res.errors, res.waiting), (0, 0, 0, 0))
        self.assertEqual(touched, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_disabled_when_jellyfin_unconfigured(self):
        db = FakePipelineDB()
        self._seed(db)
        called = []
        res = self._reconcile(
            db, cfg=CratediggerConfig(),
            find_fn=lambda cfg, path: called.append(1),
            children_fn=lambda cfg, iid: [],
            set_fn=lambda *a: True)
        self.assertEqual(
            (res.pinned, res.already_correct, res.skipped, res.waiting,
             res.expired, res.errors), (0, 0, 0, 0, 0, 0))
        self.assertEqual(called, [])

    def test_pending_fetch_failure_returns_empty_result(self):
        # A DB fetch failure aborts the pass with all-zero counters and
        # touches nothing (best-effort, never raises).
        class FailingDB(FakePipelineDB):
            def get_pending_jellyfin_date_created_pins(self, **kw):
                raise RuntimeError("db down")
        db = FailingDB()
        pin_id = self._seed(db)
        res = self._reconcile(
            db, find_fn=lambda cfg, path: self.fail("must not reach Jellyfin"),
            children_fn=lambda cfg, iid: [],
            set_fn=lambda *a: True)
        self.assertEqual(
            (res.pinned, res.already_correct, res.skipped, res.waiting,
             res.expired, res.errors), (0, 0, 0, 0, 0, 0))
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")


if __name__ == "__main__":
    unittest.main()
