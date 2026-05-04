"""Per-search progress watchdog inside `_collect_search_results` (issue #212).

A search whose `responseCount` has not advanced for 90 seconds (and is still
`InProgress` / `Queued`) trips the watchdog: cratedigger calls slskd's PUT
stop endpoint best-effort, waits up to 5s for slskd's async response-
persistence cleanup, then runs the existing harvest path unchanged.

The 90s deadline measures *progress*, not wall-time-from-submission — a
search receiving any new peer response within the last 90s keeps going.
The deadline only fires when the response stream has gone silent at the
slskd level, which is the 8h-hang failure mode this guards against.

Pinned invariants:
  * Watchdog never fires while `responseCount` is increasing.
  * On fire, `slskd.searches.stop(id)` is called exactly once, best-effort.
  * After stop(), poll state every 200ms for up to 5s, breaking when
    state shows `Completed` (slskd has flushed `Responses` into the DB).
  * The harvest path runs UNCHANGED on the watchdog-fired branch — same
    `outcome`, same `SearchResult` shape, no `outcome="timeout"`.
  * `SearchResult.watchdog_fired` is `True` iff the watchdog tripped.
  * State-transition check happens BEFORE the deadline check inside the
    poll loop body, so a search that completes naturally at the 90th
    second exits via state-transition and never calls stop().
"""
from __future__ import annotations

import unittest
from dataclasses import replace
from typing import Any

import cratedigger
from lib.config import CratediggerConfig
from tests.fakes import FakeSlskdSearches


class _FakeClock:
    """Deterministic monotonic clock for watchdog tests.

    The poll loop calls `clock_fn()` to read the current time. Tests advance
    the clock by calling `advance(seconds)`. Real `time.sleep` is patched out
    so the test runs in milliseconds.
    """
    def __init__(self, start: float = 0.0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += float(seconds)


class _FakeSlskd:
    """Minimal slskd stand-in — only `searches` is used by `_collect_search_results`."""
    def __init__(self, searches: FakeSlskdSearches) -> None:
        self.searches = searches


def _empty_cfg(**overrides) -> CratediggerConfig:
    import configparser
    cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


def _patch_sleep():
    """No-op `time.sleep` so the loop body runs at full speed in tests."""
    return unittest.mock.patch.object(cratedigger.time, "sleep", lambda _s: None)


import unittest.mock  # noqa: E402  (imported after the helpers for clarity)


# ---------------------------------------------------------------------------
# Happy paths — watchdog must NOT fire when search makes progress
# ---------------------------------------------------------------------------


class TestWatchdogDoesNotFireOnHealthySearch(unittest.TestCase):

    def test_search_completes_normally_no_watchdog(self):
        """State transitions to Completed at iteration 1, watchdog never fires."""
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed",
                            responses=[{"username": "alice", "files": []}],
                            response_count=1)
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertFalse(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [],
                         "stop() must NOT be called when search completes naturally")

    def test_slow_but_legit_search_does_not_trigger_watchdog(self):
        """`responseCount` rises by 1 every 5s for 120s of poll iterations.
        Each new response resets the no-progress timer; watchdog never fires.
        Closes the wall-clock false-positive class.
        """
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="InProgress",
                            responses=[], response_count=0)
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        # Drive: every poll iteration advances clock by 5s and bumps count.
        # After 24 iterations (120 simulated seconds) flip state to Completed.
        iter_count = {"n": 0, "rc": 0}

        original_state = searches.state

        def _state(sid, include):
            iter_count["n"] += 1
            iter_count["rc"] += 1
            searches.set_response_count(sid, iter_count["rc"])
            clock.advance(5.0)
            if iter_count["n"] >= 24:
                searches.set_state(sid, "Completed")
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertFalse(result.watchdog_fired,
                         "slow-but-progressing search must not trip the watchdog")
        self.assertEqual(searches.stop_calls, [])


# ---------------------------------------------------------------------------
# Watchdog firing — the 8h-hang case and partial-harvest variants
# ---------------------------------------------------------------------------


class TestWatchdogFires(unittest.TestCase):

    def test_no_responses_ever_fires_watchdog(self):
        """state="InProgress" indefinitely, responseCount=0 forever.
        At simulated t=90s the watchdog must fire, call stop() once,
        and let the existing harvest path run.
        Covers AE1 (the 8h-hang case)."""
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress",
            responses=[], response_count=0,
            # Post-stop cleanup: state flips to Completed | Cancelled in
            # the first state() call after stop(); responses stay empty.
            post_stop_state="Completed | Cancelled",
            post_stop_responses=[],
        )
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        # Advance clock past the 90s deadline on the second iteration.
        original_state = searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1],
                         "stop() must be called exactly once on watchdog fire")
        self.assertEqual(result.outcome, "no_results",
                         "empty harvest classifies as no_results, NOT 'timeout'")

    def test_responses_then_silence_fires_watchdog_partial_harvest(self):
        """Search lands 47 responses early, then goes silent. Watchdog
        fires after 90s of no further responses; harvest returns the 47
        cached responses. Outcome reflects what those 47 justify (NOT
        'timeout'). Covers AE1 (harvest-partial case).

        Implementation note: the fake seeds 47 responses at iter 1; from
        iter 2 onward the clock advances 31s per poll while
        responseCount stays at 47. The watchdog trips on the third
        post-iter-1 poll (~93s of no progress) and the harvest delivers
        all 47. The shape is "responses already in, then silence" —
        which exercises the same partial-harvest path as a search that
        ramped up over 10s and then stalled.
        """
        responses = [{"username": f"u{i}", "files": []} for i in range(47)]
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress",
            responses=responses, response_count=47,
            post_stop_state="Completed | Cancelled",
            post_stop_responses=responses,
        )
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        # First poll: 47 responses already in. Subsequent polls advance
        # clock by 30s with no further progress. After ~3 ticks the
        # watchdog fires (90s of no progress).
        original_state = searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] >= 2:
                clock.advance(31.0)
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1])
        # Harvest delivered 47 responses — outcome is not 'timeout' and not
        # 'no_results'. The exact outcome depends on caching/match logic,
        # but the result should be a successful harvest.
        self.assertEqual(result.result_count, 47)
        self.assertNotEqual(result.outcome, "timeout",
                            "watchdog must not introduce a 'timeout' outcome")


# ---------------------------------------------------------------------------
# Stop endpoint: best-effort
# ---------------------------------------------------------------------------


class TestStopBestEffort(unittest.TestCase):

    def test_stop_raises_does_not_break_harvest(self):
        """If `searches.stop()` raises, the watchdog still breaks out of the
        poll loop and the harvest path runs. Covers AE2."""
        searches = FakeSlskdSearches()
        # Even when stop() raises on our side, slskd's own
        # cancel-then-cleanup may still land on the next poll — model that
        # so the post-cancel wait loop exits via state-transition.
        searches.add_search(
            search_id=1, state="InProgress",
            responses=[], response_count=0,
            post_stop_state="Completed | Cancelled",
            post_stop_responses=[],
        )
        searches.set_stop_error(1, RuntimeError("network error"))
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        original_state = searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1])
        self.assertEqual(result.outcome, "no_results")

    def test_stop_returns_false_404_logged_and_ignored(self):
        """slskd 404s are not errors — search may have just transitioned
        to Completed in between our last state poll and our cancel call.
        R2: 404 logged and ignored."""
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress",
            responses=[], response_count=0,
            post_stop_state="Completed | Cancelled",
            post_stop_responses=[],
        )
        searches.set_stop_return(1, False)
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        original_state = searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1])


# ---------------------------------------------------------------------------
# Post-cancel state-transition wait
# ---------------------------------------------------------------------------


class TestPostCancelStateTransitionWait(unittest.TestCase):

    def test_responses_persisted_in_time(self):
        """slskd's async cleanup completes within the 5s post-cancel budget:
        state flips to Completed | Cancelled and the 23 pending responses
        become readable. Harvest returns them — NOT 'no_results'.
        Regression guard for the silent-no_results-degrade failure mode.
        """
        responses = [{"username": f"u{i}", "files": []} for i in range(23)]
        searches = FakeSlskdSearches()
        # Seed: visible responses are empty until stop() lands; then state
        # flips to Completed and responses become the 23.
        searches.add_search(
            search_id=1, state="InProgress",
            responses=[], response_count=0,
            post_stop_state="Completed | Cancelled",
            post_stop_responses=responses,
        )
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        original_state = searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertTrue(result.watchdog_fired)
        self.assertEqual(result.result_count, 23,
                         "harvest must wait for slskd's async cleanup before reading")

    def test_slskd_hung_at_cleanup_5s_timeout(self):
        """slskd's cleanup itself is hung: stop() runs but state never
        transitions out of InProgress. The post-cancel wait hits its 5s
        budget, harvest runs anyway with empty responses, no exception.
        """
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress",
            responses=[], response_count=0,
            # post_stop_state=None → state stays InProgress forever
        )
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        # Trip the watchdog on iter 2; thereafter every state() call inside
        # the post-cancel wait advances clock 0.21s, exhausting the 5s budget.
        original_state = searches.state
        n = {"i": 0, "in_post_cancel": False}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
                n["in_post_cancel"] = True
            elif n["in_post_cancel"]:
                clock.advance(0.21)
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertTrue(result.watchdog_fired)
        # Harvest with empty responses — outcome="no_results", but no exception.
        self.assertEqual(result.outcome, "no_results")


# ---------------------------------------------------------------------------
# State-transition vs deadline ordering
# ---------------------------------------------------------------------------


class TestCompletionVsWatchdogOrdering(unittest.TestCase):

    def test_state_completed_at_90s_does_not_fire_watchdog(self):
        """`responseCount=10` for 89s without further increases, then on the
        90s poll state flips to Completed. State-transition exit MUST win;
        stop() must NOT be called. Pins the ordering invariant: the
        state-transition check happens BEFORE the deadline check.
        """
        responses = [{"username": f"u{i}", "files": []} for i in range(10)]
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress",
            responses=responses, response_count=10,
        )
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        # On the second poll, advance clock to 91s AND flip state to
        # Completed. The loop should observe Completed first and break,
        # never reaching the watchdog deadline check.
        original_state = searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
                searches.set_state(sid, "Completed")
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertFalse(result.watchdog_fired,
                         "natural completion at 90s must beat the watchdog")
        self.assertEqual(searches.stop_calls, [],
                         "stop() must NOT be called when state transitions naturally")


# ---------------------------------------------------------------------------
# state() raises mid-poll
# ---------------------------------------------------------------------------


class TestStateRaisesMidPoll(unittest.TestCase):

    def test_state_exception_breaks_loop_unchanged(self):
        """Existing behaviour: if `state()` raises, the loop logs a warning
        and breaks. Harvest still runs. The watchdog must not change this.
        """
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress",
            responses=[], response_count=0,
        )
        slskd = _FakeSlskd(searches)
        cfg = _empty_cfg()
        clock = _FakeClock()

        original_state = searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] >= 2:
                raise RuntimeError("network blip")
            return original_state(sid, include)
        searches.state = _state  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertFalse(result.watchdog_fired,
                         "state() exception is not a watchdog event")
        self.assertEqual(searches.stop_calls, [])


if __name__ == "__main__":
    unittest.main()
