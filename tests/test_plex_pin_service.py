"""Orchestration tests for lib.plex_pin_service (migration 040).

Capture/reconcile are driven against a real FakePipelineDB with the Plex
client (find/set) injected as kwarg-DI seams — no network, no MagicMock db.
Every assertion targets persisted pin state, per the orchestration-test rule.
"""
import unittest
from datetime import datetime, timedelta, timezone

from lib.config import CratediggerConfig
from lib.util import PlexAlbumRef
from lib.plex_pin_service import (
    capture_plex_added_at_pin,
    reconcile_plex_added_at_pins,
)
from tests.fakes import FakePipelineDB

NOW = datetime(2026, 6, 28, 12, 0, tzinfo=timezone.utc)


def _cfg(**kw):
    kw.setdefault("plex_url", "http://plex:32400")
    return CratediggerConfig(**kw)


class TestCapture(unittest.TestCase):
    def test_captured_persists_pin_with_album_fields(self):
        db = FakePipelineDB()
        ref = PlexAlbumRef(rating_key="458495", added_at=1782611948,
                           title="The Wow! Signal", artist="Muse")
        res = capture_plex_added_at_pin(
            _cfg(), db, "Muse/2026 - The Wow! Signal", 8812,
            find_fn=lambda cfg, path: ref)
        self.assertEqual(res.outcome, "captured")
        self.assertEqual(res.original_added_at, 1782611948)
        self.assertEqual(len(db.plex_added_at_pins), 1)
        pin = db.plex_added_at_pins[0]
        self.assertEqual(pin["imported_path"], "Muse/2026 - The Wow! Signal")
        self.assertEqual(pin["original_added_at"], 1782611948)
        self.assertEqual(pin["rating_key"], "458495")
        self.assertEqual(pin["request_id"], 8812)
        self.assertEqual(pin["status"], "pending")

    def test_no_album_writes_no_pin(self):
        db = FakePipelineDB()
        res = capture_plex_added_at_pin(
            _cfg(), db, "New/Album", 1, find_fn=lambda cfg, path: None)
        self.assertEqual(res.outcome, "no_album")
        self.assertEqual(db.plex_added_at_pins, [])

    def test_disabled_when_plex_unconfigured(self):
        db = FakePipelineDB()
        calls = []
        res = capture_plex_added_at_pin(
            CratediggerConfig(), db, "A/B", 1,
            find_fn=lambda cfg, path: calls.append(path))
        self.assertEqual(res.outcome, "disabled")
        self.assertEqual(calls, [])  # never looked up Plex
        self.assertEqual(db.plex_added_at_pins, [])

    def test_disabled_when_no_imported_path(self):
        db = FakePipelineDB()
        res = capture_plex_added_at_pin(_cfg(), db, None, 1,
                                        find_fn=lambda cfg, path: None)
        self.assertEqual(res.outcome, "disabled")

    def test_error_on_lookup_failure_writes_no_pin(self):
        db = FakePipelineDB()

        def _boom(cfg, path):
            raise RuntimeError("plex down")
        res = capture_plex_added_at_pin(_cfg(), db, "A/B", 1, find_fn=_boom)
        self.assertEqual(res.outcome, "error")
        self.assertEqual(db.plex_added_at_pins, [])

    def test_error_on_persist_failure(self):
        class FailingDB(FakePipelineDB):
            def add_plex_added_at_pin(self, *, imported_path, original_added_at,
                                      rating_key, request_id):
                raise RuntimeError("db down")
        ref = PlexAlbumRef(rating_key="1", added_at=100)
        res = capture_plex_added_at_pin(
            _cfg(), FailingDB(), "A/B", 1, find_fn=lambda cfg, path: ref)
        self.assertEqual(res.outcome, "error")


class TestReconcile(unittest.TestCase):
    def _seed(self, db, *, path="A/B", original=100, captured_at):
        pin_id = db.add_plex_added_at_pin(
            imported_path=path, original_added_at=original,
            rating_key="rk", request_id=1)
        db.plex_added_at_pins[-1]["captured_at"] = captured_at
        return pin_id

    def _pin(self, db, pin_id):
        return next(p for p in db.plex_added_at_pins if p["id"] == pin_id)

    def test_bumped_date_is_restored_and_pin_marked_done(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, original=100, captured_at=NOW - timedelta(minutes=10))
        set_calls = []
        res = reconcile_plex_added_at_pins(
            _cfg(), db, now=NOW,
            find_fn=lambda cfg, path: PlexAlbumRef(rating_key="rk", added_at=999),
            set_fn=lambda cfg, rk, added: set_calls.append((rk, added)) or True)
        self.assertEqual(res.pinned, 1)
        self.assertEqual(set_calls, [("rk", 100)])  # restored ORIGINAL, not 999
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    def test_already_correct_still_locks_the_value(self):
        # Even when the date already matches, lock it (addedAt.locked=1) so a
        # not-yet-completed Plex rescan can't bump it later. Mark done on
        # success.
        db = FakePipelineDB()
        pin_id = self._seed(db, original=100, captured_at=NOW - timedelta(minutes=10))
        set_calls = []
        res = reconcile_plex_added_at_pins(
            _cfg(), db, now=NOW,
            find_fn=lambda cfg, path: PlexAlbumRef(rating_key="rk", added_at=100),
            set_fn=lambda cfg, rk, added: set_calls.append((rk, added)) or True)
        self.assertEqual(res.already_correct, 1)
        self.assertEqual(set_calls, [("rk", 100)])  # locked at the ORIGINAL value
        self.assertEqual(self._pin(db, pin_id)["status"], "done")

    def test_already_correct_lock_failure_leaves_pin_pending(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, original=100, captured_at=NOW - timedelta(minutes=10))
        res = reconcile_plex_added_at_pins(
            _cfg(), db, now=NOW,
            find_fn=lambda cfg, path: PlexAlbumRef(rating_key="rk", added_at=100),
            set_fn=lambda cfg, rk, added: False)
        self.assertEqual(res.errors, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_album_gone_is_skipped(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, captured_at=NOW - timedelta(minutes=10))
        res = reconcile_plex_added_at_pins(
            _cfg(), db, now=NOW, find_fn=lambda cfg, path: None,
            set_fn=lambda *a, **k: True)
        self.assertEqual(res.skipped, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "skipped")

    def test_put_failure_leaves_pin_pending_for_retry(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, original=100, captured_at=NOW - timedelta(minutes=10))
        res = reconcile_plex_added_at_pins(
            _cfg(), db, now=NOW,
            find_fn=lambda cfg, path: PlexAlbumRef(rating_key="rk", added_at=999),
            set_fn=lambda cfg, rk, added: False)
        self.assertEqual(res.errors, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_find_exception_leaves_pin_pending(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, captured_at=NOW - timedelta(minutes=10))

        def _boom(cfg, path):
            raise RuntimeError("plex down")
        res = reconcile_plex_added_at_pins(
            _cfg(), db, now=NOW, find_fn=_boom, set_fn=lambda *a, **k: True)
        self.assertEqual(res.errors, 1)
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_pin_within_grace_window_is_not_processed(self):
        db = FakePipelineDB()
        pin_id = self._seed(db, captured_at=NOW - timedelta(seconds=10))
        set_calls = []
        res = reconcile_plex_added_at_pins(
            _cfg(), db, now=NOW, grace_seconds=180,
            find_fn=lambda cfg, path: PlexAlbumRef(rating_key="rk", added_at=999),
            set_fn=lambda *a, **k: set_calls.append(1) or True)
        self.assertEqual((res.pinned, res.skipped, res.errors), (0, 0, 0))
        self.assertEqual(set_calls, [])
        self.assertEqual(self._pin(db, pin_id)["status"], "pending")

    def test_disabled_when_plex_unconfigured(self):
        db = FakePipelineDB()
        self._seed(db, captured_at=NOW - timedelta(minutes=10))
        called = []
        res = reconcile_plex_added_at_pins(
            CratediggerConfig(), db, now=NOW,
            find_fn=lambda cfg, path: called.append(1),
            set_fn=lambda *a, **k: True)
        self.assertEqual((res.pinned, res.already_correct, res.skipped, res.errors),
                         (0, 0, 0, 0))
        self.assertEqual(called, [])


if __name__ == "__main__":
    unittest.main()
