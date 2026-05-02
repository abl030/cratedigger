"""Tests for the bounded parallel browse fan-out primitive (issue #198 U2).

The fan-out function `_fanout_browse_users` lives in `lib/browse.py` next to
`_browse_directories`. It accepts a flat list of `(username, file_dir)` work
items, submits each to a bounded `ThreadPoolExecutor`, and bounds wall-clock
via a per-wave deadline. Returns the set of usernames whose futures had not
completed by the deadline.

These tests pin down:
  * happy-path bucket population
  * pre-create-bucket invariant (no `setdefault` race across futures)
  * empty work, all-exceptions, all-failure tolerance
  * deadline trips populate the timed-out set
  * wall-clock bound (manual `shutdown(wait=False, cancel_futures=True)` works)
  * deadline=0 short-circuit
  * concurrency cap honored
  * 1-user × N-dirs race regression (the case Step 1 pre-create fixes)
"""

from __future__ import annotations

import threading
import time
import unittest
from typing import Any
from unittest.mock import MagicMock

from lib.browse import _fanout_browse_users
from lib.context import CratediggerContext
from tests.fakes import FakeSlskdAPI


def _make_ctx(slskd: Any) -> CratediggerContext:
    """Minimal context wired to a slskd fake — only fields the fan-out reads."""
    return CratediggerContext(
        cfg=MagicMock(),
        slskd=slskd,
        pipeline_db_source=MagicMock(),
    )


def _make_directory(dir_path: str) -> dict[str, Any]:
    """Slskd-shaped directory listing (single track)."""
    return {
        "directory": dir_path,
        "files": [{"filename": "01 - Track.flac", "size": 100}],
    }


class TestFanoutBrowseHappyPath(unittest.TestCase):
    def test_populates_cache_for_all_user_dir_pairs(self):
        """5 users × 3 dirs all return immediately → 15 cache entries, no timeouts."""
        slskd = FakeSlskdAPI()
        users = [f"user{i}" for i in range(5)]
        dirs = ["A", "B", "C"]
        work = []
        for u in users:
            for d in dirs:
                slskd.users.set_directory(u, d, [_make_directory(d)])
                work.append((u, d))

        ctx = _make_ctx(slskd)
        timed_out = _fanout_browse_users(
            work, slskd, ctx, max_workers=8, deadline_s=5.0
        )

        self.assertEqual(timed_out, set())
        self.assertEqual(set(ctx.folder_cache.keys()), set(users))
        for u in users:
            self.assertEqual(set(ctx.folder_cache[u].keys()), set(dirs))
            self.assertEqual(set(ctx._folder_cache_ts[u].keys()), set(dirs))

    def test_pre_creates_user_buckets_for_every_work_item(self):
        """Every user in the wave must have an inner dict before any future writes.

        Pin the contract that fixes the `setdefault + nested-write` race: the
        function must pre-create `ctx.folder_cache[user] = {}` for every user
        in the wave before submitting any future. We probe this by checking
        that the inner dict exists for users whose dirs ALL fail — if buckets
        were created lazily on success, those users would be absent from
        `folder_cache` entirely.
        """
        slskd = FakeSlskdAPI()
        # user_ok succeeds, user_fail's dir raises.
        slskd.users.set_directory("user_ok", "A", [_make_directory("A")])
        slskd.users.set_directory_error("user_fail", "B", Exception("peer gone"))
        work = [("user_ok", "A"), ("user_fail", "B")]

        ctx = _make_ctx(slskd)
        _fanout_browse_users(
            work, slskd, ctx, max_workers=4, deadline_s=5.0
        )

        self.assertIn("user_ok", ctx.folder_cache)
        self.assertIn("user_fail", ctx.folder_cache)
        self.assertEqual(ctx.folder_cache["user_fail"], {})


class TestFanoutBrowseEdgeCases(unittest.TestCase):
    def test_empty_work_list_returns_empty_set_no_exception(self):
        slskd = FakeSlskdAPI()
        ctx = _make_ctx(slskd)
        timed_out = _fanout_browse_users(
            [], slskd, ctx, max_workers=4, deadline_s=5.0
        )
        self.assertEqual(timed_out, set())
        self.assertEqual(ctx.folder_cache, {})

    def test_all_peers_fail_with_exceptions_no_writes_no_timeouts(self):
        """Per-task exceptions are not timeouts — folder_cache stays empty."""
        slskd = FakeSlskdAPI()
        slskd.users.set_directory_error("user1", "A", RuntimeError("x"))
        slskd.users.set_directory_error("user2", "B", ConnectionError("y"))
        work = [("user1", "A"), ("user2", "B")]

        ctx = _make_ctx(slskd)
        timed_out = _fanout_browse_users(
            work, slskd, ctx, max_workers=4, deadline_s=5.0
        )

        self.assertEqual(timed_out, set())
        # Buckets pre-created but empty; no per-(user,dir) write succeeded.
        self.assertEqual(ctx.folder_cache["user1"], {})
        self.assertEqual(ctx.folder_cache["user2"], {})


class TestFanoutBrowseDeadline(unittest.TestCase):
    def test_deadline_trips_marks_slow_users_as_timed_out(self):
        slskd = FakeSlskdAPI()
        # user_a: two dirs, both fast.
        slskd.users.set_directory("user_a", "A1", [_make_directory("A1")])
        slskd.users.set_directory("user_a", "A2", [_make_directory("A2")])
        # users B and C: one dir each, delayed past the deadline.
        slskd.users.set_directory("user_b", "B", [_make_directory("B")])
        slskd.users.set_directory("user_c", "C", [_make_directory("C")])
        slskd.users.set_directory_delay("user_b", "B", 0.5)
        slskd.users.set_directory_delay("user_c", "C", 0.5)
        work = [
            ("user_a", "A1"), ("user_a", "A2"),
            ("user_b", "B"), ("user_c", "C"),
        ]

        ctx = _make_ctx(slskd)
        t0 = time.monotonic()
        timed_out = _fanout_browse_users(
            work, slskd, ctx, max_workers=8, deadline_s=0.2
        )
        elapsed = time.monotonic() - t0

        self.assertEqual(timed_out, {"user_b", "user_c"})
        self.assertEqual(set(ctx.folder_cache["user_a"].keys()), {"A1", "A2"})
        self.assertEqual(ctx.folder_cache["user_b"], {})
        self.assertEqual(ctx.folder_cache["user_c"], {})
        self.assertLess(elapsed, 1.0, f"deadline did not bound wall-clock (elapsed={elapsed:.3f}s)")

    def test_wall_clock_bound_by_deadline_not_slowest_task(self):
        """A 5s task should NOT keep the function blocked past `deadline_s`.

        The naive `with ThreadPoolExecutor(...)` exit calls `shutdown(wait=True)`
        which would block on every running future. The implementation must use
        manual lifetime + `shutdown(wait=False, cancel_futures=True)`.
        """
        slskd = FakeSlskdAPI()
        slskd.users.set_directory("user_slow", "X", [_make_directory("X")])
        slskd.users.set_directory_delay("user_slow", "X", 5.0)
        work = [("user_slow", "X")]

        ctx = _make_ctx(slskd)
        t0 = time.monotonic()
        timed_out = _fanout_browse_users(
            work, slskd, ctx, max_workers=4, deadline_s=0.2
        )
        elapsed = time.monotonic() - t0

        self.assertEqual(timed_out, {"user_slow"})
        self.assertLess(
            elapsed, 0.5,
            f"shutdown(wait=False) did not short-circuit (elapsed={elapsed:.3f}s)",
        )

    def test_deadline_zero_short_circuits_all_users(self):
        slskd = FakeSlskdAPI()
        slskd.users.set_directory("u1", "A", [_make_directory("A")])
        slskd.users.set_directory("u2", "B", [_make_directory("B")])
        # Tiny delay so the futures definitely haven't finished by deadline.
        slskd.users.set_directory_delay("u1", "A", 0.05)
        slskd.users.set_directory_delay("u2", "B", 0.05)
        work = [("u1", "A"), ("u2", "B")]

        ctx = _make_ctx(slskd)
        timed_out = _fanout_browse_users(
            work, slskd, ctx, max_workers=4, deadline_s=0.0
        )

        self.assertEqual(timed_out, {"u1", "u2"})
        self.assertEqual(ctx.folder_cache["u1"], {})
        self.assertEqual(ctx.folder_cache["u2"], {})


class TestFanoutBrowseConcurrencyCap(unittest.TestCase):
    def test_max_workers_caps_in_flight_directory_calls(self):
        slskd = FakeSlskdAPI()
        peak = 0
        in_flight = 0
        lock = threading.Lock()

        def probe(delta: int) -> None:
            nonlocal peak, in_flight
            with lock:
                in_flight += delta
                if in_flight > peak:
                    peak = in_flight

        slskd.users.in_flight_probe = probe

        work = []
        for i in range(50):
            u, d = f"u{i}", f"d{i}"
            slskd.users.set_directory(u, d, [_make_directory(d)])
            slskd.users.set_directory_delay(u, d, 0.05)  # hold each call long enough to overlap
            work.append((u, d))

        ctx = _make_ctx(slskd)
        _fanout_browse_users(
            work, slskd, ctx, max_workers=4, deadline_s=10.0
        )

        self.assertLessEqual(peak, 4, f"max_workers=4 cap was violated; peak={peak}")
        # Sanity: most of the 50 work items completed within the 10s deadline.
        self.assertGreaterEqual(len(ctx.folder_cache), 40)


class TestFanoutBrowseRaceRegression(unittest.TestCase):
    def test_one_user_eight_dirs_no_lost_entries_across_iterations(self):
        """Regression for the `setdefault + nested-write` race.

        With one user contributing 8 different dirs, all 8 futures share the
        same inner dict. The pre-create-buckets step removes the race; this
        test pins that no entries are lost across many iterations.
        """
        for iteration in range(50):
            slskd = FakeSlskdAPI()
            user = "user1"
            dirs = [f"d{i}" for i in range(8)]
            for d in dirs:
                slskd.users.set_directory(user, d, [_make_directory(d)])
            work = [(user, d) for d in dirs]

            ctx = _make_ctx(slskd)
            timed_out = _fanout_browse_users(
                work, slskd, ctx, max_workers=8, deadline_s=5.0
            )

            self.assertEqual(timed_out, set(), f"iteration {iteration}: unexpected timeouts")
            self.assertEqual(
                len(ctx.folder_cache[user]), 8,
                f"iteration {iteration}: expected 8 entries, got {len(ctx.folder_cache[user])}",
            )


if __name__ == "__main__":
    unittest.main()
