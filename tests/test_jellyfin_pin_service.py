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
        self.assertEqual(res.original_date_created, ORIGINAL)
        self.assertEqual(len(db.jellyfin_date_created_pins), 1)
        pin = db.jellyfin_date_created_pins[0]
        self.assertEqual(pin["imported_path"], "Muse/2026 - The Wow! Signal")
        self.assertEqual(pin["original_date_created"], ORIGINAL)
        self.assertEqual(pin["album_item_id"], "alb-1")
        self.assertEqual(pin["children_item_ids"], ["tr-1", "tr-2"])
        self.assertEqual(pin["request_id"], 8812)
        self.assertEqual(pin["status"], "pending")

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


class TestReconcile(unittest.TestCase):
    def _seed(self, db, *, path="A/B", original=ORIGINAL, album_item_id="alb-1",
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

    def test_album_gone_is_skipped(self):
        db = FakePipelineDB()
        pin_id = self._seed(db)
        res = self._reconcile(
            db, find_fn=lambda cfg, path: None,
            children_fn=lambda cfg, iid: [],
            set_fn=lambda *a: True)
        self.assertEqual(res.skipped, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "skipped")

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


if __name__ == "__main__":
    unittest.main()
