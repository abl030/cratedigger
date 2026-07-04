"""Authoritative suite for the unified slskd search lifecycle
(``lib.search_exec.execute_search`` + ``_fetch_search_responses_settled``).

Issue #466 consolidated three drifted copies of the
submit → poll-state → settle → harvest → delete lifecycle
(``cratedigger.search_for_album``, ``cratedigger._collect_search_results``,
``lib.unfindable_detection_service.run_artist_probe``, plus the bench
script) into ``execute_search``. This file is the single authoritative
coverage for that lifecycle; the call sites keep only thin-wrapper tests.

Pinned invariants (superset of the pre-consolidation watchdog + settle
contracts, which lived in ``test_search_watchdog.py`` /
``test_search_response_settle.py`` and moved here):

  * Submit mode (``search_id=None`` + ``submit_kwargs``) forwards the
    caller's search params to ``searches.search_text`` and derives the
    search id from the response.
  * Submit failure raises ``SearchSubmitError`` (pre-accept, so callers
    can classify it as non-consuming) — the poll/harvest phases never run.
  * Pre-submitted mode (``search_id`` given) skips submit entirely — the
    parallel pipeline submits sequentially under slskd's semaphore and
    hands the id to ``execute_search`` for the parallel collect phase.
  * The #212 progress watchdog fires only on responseCount stagnation
    while still InProgress/Queued; a state transition on the deadline
    poll wins over the deadline check; ``stop()`` is best-effort.
  * The #242 settle-harvest polls ``search_responses`` until two
    consecutive reads agree (or the deadline), so a terminal state with a
    not-yet-committed response store never degrades to an empty harvest.
  * ``delete`` is honoured per the flag, after the harvest.
  * ``response_count_terminal`` carries slskd's uncapped ``responseCount``
    independent of the (possibly truncated/raced) harvested list length.
"""

from __future__ import annotations

import unittest
from typing import Any

from lib import search_exec
from lib.search_exec import (
    SearchExecutionResult,
    SearchSubmitError,
    execute_search,
)
from tests.fakes import FakeSlskdAPI, FakeSlskdSearches


def _noop_sleep(_s: float) -> None:
    return None


class _FakeClock:
    """Deterministic monotonic clock. Tests call ``advance(seconds)``."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += float(seconds)


def _api_with(searches: FakeSlskdSearches) -> FakeSlskdAPI:
    api = FakeSlskdAPI()
    api.searches = searches
    return api


# ---------------------------------------------------------------------------
# Submit / pre-submitted / delete seams
# ---------------------------------------------------------------------------


class TestExecuteSearchSubmitSeam(unittest.TestCase):

    def test_submit_mode_forwards_kwargs_and_returns_terminal_state(self):
        api = FakeSlskdAPI()
        api.searches.search_text_id_sequence = [77]
        api.searches.add_search(
            search_id=77, state="Completed",
            responses=[{"username": "u", "files": []}], response_count=1,
        )
        submit_kwargs = {
            "searchText": "*rtist Album",
            "searchTimeout": 30000,
            "filterResponses": True,
            "maximumPeerQueueLength": 5,
            "minimumPeerUploadSpeed": 0,
            "responseLimit": 1000,
            "fileLimit": 5000,
        }
        result = execute_search(
            api, submit_kwargs=submit_kwargs, delete=False,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertIsInstance(result, SearchExecutionResult)
        call = api.searches.search_text_calls[0]
        self.assertEqual(call.search_text, "*rtist Album")
        self.assertEqual(call.kwargs["responseLimit"], 1000)
        self.assertEqual(call.kwargs["fileLimit"], 5000)
        self.assertEqual(result.final_state, "Completed")
        self.assertEqual(result.response_count_terminal, 1)
        self.assertEqual(len(result.responses), 1)
        self.assertFalse(result.watchdog_fired)

    def test_submit_failure_raises_SearchSubmitError_before_poll(self):
        api = FakeSlskdAPI()
        api.searches.search_text_error = RuntimeError("slskd offline")
        with self.assertRaises(SearchSubmitError):
            execute_search(
                api, submit_kwargs={"searchText": "q", "responseLimit": 1},
                delete=False, clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
            )
        # Poll never ran — no state() calls recorded.
        self.assertEqual(api.searches.state_calls, [])

    def test_pre_submitted_mode_skips_submit(self):
        api = FakeSlskdAPI()
        api.searches.add_search(
            search_id=5, state="Completed",
            responses=[{"username": "u", "files": []}], response_count=1,
        )
        result = execute_search(
            api, search_id=5, delete=False,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(api.searches.search_text_calls, [])
        self.assertEqual(result.response_count_terminal, 1)

    def test_missing_submit_kwargs_and_id_is_error(self):
        api = FakeSlskdAPI()
        with self.assertRaises(ValueError):
            execute_search(
                api, delete=False,
                clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
            )

    def test_delete_true_deletes_after_harvest(self):
        api = FakeSlskdAPI()
        api.searches.add_search(search_id=9, state="Completed", responses=[])
        execute_search(
            api, search_id=9, delete=True,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(api.searches.delete_calls, [9])

    def test_delete_false_does_not_delete(self):
        api = FakeSlskdAPI()
        api.searches.add_search(search_id=9, state="Completed", responses=[])
        execute_search(
            api, search_id=9, delete=False,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(api.searches.delete_calls, [])

    def test_delete_failure_is_best_effort(self):
        """A failed cleanup DELETE must never discard the harvested result.

        Regression guard: the pre-#466 serial pipeline path ran delete inside
        the collection try/except, so a delete failure rolled a good harvest
        into ``collection_crash``. execute_search swallows delete errors.
        """
        api = FakeSlskdAPI()
        api.searches.add_search(
            search_id=9, state="Completed",
            responses=[{"username": "u", "files": []}], response_count=1,
        )

        def _boom(sid: Any) -> None:
            api.searches.delete_calls.append(sid)
            raise RuntimeError("slskd delete failed")

        api.searches.delete = _boom  # type: ignore[method-assign]
        result = execute_search(
            api, search_id=9, delete=True,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(api.searches.delete_calls, [9])
        self.assertEqual(len(result.responses), 1)
        self.assertEqual(result.response_count_terminal, 1)

    def test_harvest_error_still_deletes_search(self):
        """The cleanup DELETE runs even when the harvest raises (try/finally).

        Pre-#466 the probe deleted its search in a ``finally``, so a
        ``search_responses`` transport error never leaked the search on
        slskd's side. The unified lifecycle must preserve that: the harvest
        exception still propagates to the caller, but delete fires first.
        """
        api = FakeSlskdAPI()
        api.searches.add_search(search_id=9, state="Completed", responses=[])

        def _raise(_sid: Any) -> list[dict[str, Any]]:
            raise RuntimeError("harvest transport error")

        api.searches.search_responses = _raise  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            execute_search(
                api, search_id=9, delete=True,
                clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
            )
        self.assertEqual(api.searches.delete_calls, [9],
                         "delete must run in finally even when harvest raises")

    def test_response_count_terminal_independent_of_harvest_length(self):
        """slskd truncates the harvested array at responseLimit/fileLimit but
        still reports the uncapped ``responseCount`` in state. The result
        carries the uncapped count, not ``len(responses)``."""
        api = FakeSlskdAPI()
        api.searches.add_search(
            search_id=3, state="Completed, FileLimitReached",
            responses=[{"username": "u", "files": []}],
            response_count=812,
        )
        result = execute_search(
            api, search_id=3, delete=False,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(result.response_count_terminal, 812)
        self.assertEqual(len(result.responses), 1)


# ---------------------------------------------------------------------------
# Progress watchdog (issue #212) — moved from test_search_watchdog.py
# ---------------------------------------------------------------------------


class TestWatchdogDoesNotFireOnHealthySearch(unittest.TestCase):

    def test_search_completes_normally_no_watchdog(self):
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed",
                            responses=[{"username": "alice", "files": []}],
                            response_count=1)
        api = _api_with(searches)
        result = execute_search(
            api, search_id=1, delete=False,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertFalse(result.watchdog_fired)
        self.assertFalse(result.state_poll_error)
        self.assertEqual(searches.stop_calls, [])

    def test_slow_but_progressing_search_does_not_trigger_watchdog(self):
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="InProgress",
                            responses=[], response_count=0)
        api = _api_with(searches)
        clock = _FakeClock()
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
        result = execute_search(
            api, search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertFalse(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [])


class TestWatchdogFires(unittest.TestCase):

    def _advance_on_second_poll(self, searches: FakeSlskdSearches,
                                clock: _FakeClock) -> None:
        original_state = searches.state
        n = {"i": 0}

        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]

    def test_no_responses_ever_fires_watchdog(self):
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress", responses=[], response_count=0,
            post_stop_state="Completed | Cancelled", post_stop_responses=[],
        )
        clock = _FakeClock()
        self._advance_on_second_poll(searches, clock)
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1])
        self.assertEqual(result.responses, [])

    def test_responses_then_silence_partial_harvest(self):
        responses = [{"username": f"u{i}", "files": []} for i in range(47)]
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress", responses=responses,
            response_count=47,
            post_stop_state="Completed | Cancelled",
            post_stop_responses=responses,
        )
        clock = _FakeClock()
        original_state = searches.state
        n = {"i": 0}

        def _state(sid, include):
            n["i"] += 1
            if n["i"] >= 2:
                clock.advance(31.0)
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1])
        self.assertEqual(len(result.responses), 47)


class TestStopBestEffort(unittest.TestCase):

    def _advance_on_second_poll(self, searches, clock):
        original_state = searches.state
        n = {"i": 0}

        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]

    def test_stop_raises_does_not_break_harvest(self):
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress", responses=[], response_count=0,
            post_stop_state="Completed | Cancelled", post_stop_responses=[],
        )
        searches.set_stop_error(1, RuntimeError("network error"))
        clock = _FakeClock()
        self._advance_on_second_poll(searches, clock)
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1])
        self.assertEqual(result.responses, [])

    def test_stop_returns_false_is_ignored(self):
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress", responses=[], response_count=0,
            post_stop_state="Completed | Cancelled", post_stop_responses=[],
        )
        searches.set_stop_return(1, False)
        clock = _FakeClock()
        self._advance_on_second_poll(searches, clock)
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertTrue(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [1])


class TestPostCancelSettleWait(unittest.TestCase):

    def test_responses_persisted_within_budget(self):
        responses = [{"username": f"u{i}", "files": []} for i in range(23)]
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress", responses=[], response_count=0,
            post_stop_state="Completed | Cancelled",
            post_stop_responses=responses,
        )
        clock = _FakeClock()
        original_state = searches.state
        n = {"i": 0}

        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertTrue(result.watchdog_fired)
        self.assertEqual(len(result.responses), 23)

    def test_slskd_hung_at_cleanup_hits_budget_and_returns_empty(self):
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress", responses=[], response_count=0,
        )
        clock = _FakeClock()
        original_state = searches.state
        n = {"i": 0, "post": False}

        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
                n["post"] = True
            elif n["post"]:
                clock.advance(0.21)
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertTrue(result.watchdog_fired)
        self.assertEqual(result.responses, [])


class TestCompletionVsWatchdogOrdering(unittest.TestCase):

    def test_state_completed_at_deadline_does_not_fire_watchdog(self):
        responses = [{"username": f"u{i}", "files": []} for i in range(10)]
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress", responses=responses,
            response_count=10,
        )
        clock = _FakeClock()
        original_state = searches.state
        n = {"i": 0}

        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock.advance(91.0)
                searches.set_state(sid, "Completed")
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertFalse(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [])


class TestStateRaisesMidPoll(unittest.TestCase):

    def test_state_exception_breaks_loop_then_harvests(self):
        searches = FakeSlskdSearches()
        searches.add_search(
            search_id=1, state="InProgress",
            responses=[{"username": "u", "files": []}], response_count=1,
        )
        original_state = searches.state
        n = {"i": 0}

        def _state(sid, include):
            n["i"] += 1
            if n["i"] >= 2:
                raise RuntimeError("network blip")
            return original_state(sid, include)

        searches.state = _state  # type: ignore[method-assign]
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        # state() exception is not a watchdog event; loop breaks and the
        # harvest still runs against whatever slskd committed. The execution
        # is flagged degraded so trust-sensitive callers (the probe) can bail.
        self.assertFalse(result.watchdog_fired)
        self.assertTrue(result.state_poll_error)
        self.assertEqual(searches.stop_calls, [])
        self.assertEqual(len(result.responses), 1)


# ---------------------------------------------------------------------------
# Settle helper (issue #242) — moved from test_search_response_settle.py
# ---------------------------------------------------------------------------


class TestFetchSearchResponsesSettled(unittest.TestCase):

    def _build(self, response_sequence: list[list[dict[str, Any]]]) -> FakeSlskdAPI:
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed",
                            responses=[], response_count=0)
        idx = {"i": 0}
        original = searches.search_responses

        def _resp(sid: Any) -> list[dict[str, Any]]:
            original(sid)
            i = idx["i"]
            if i < len(response_sequence):
                idx["i"] = i + 1
                return list(response_sequence[i])
            return list(response_sequence[-1])

        searches.search_responses = _resp  # type: ignore[method-assign]
        return _api_with(searches)

    def test_already_settled_returns_after_one_extra_call(self):
        responses = [{"username": f"u{i}", "files": []} for i in range(47)]
        api = self._build([responses, responses])
        result = search_exec._fetch_search_responses_settled(
            api, search_id=1, deadline_s=2.0, poll_s=0.2,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(len(result), 47)
        self.assertEqual(len(api.searches.responses_calls), 2)

    def test_race_resolves_on_second_call(self):
        late = [{"username": f"u{i}", "files": []} for i in range(757)]
        api = self._build([[], late, late])
        result = search_exec._fetch_search_responses_settled(
            api, search_id=1, deadline_s=2.0, poll_s=0.2,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(len(result), 757)
        self.assertGreaterEqual(len(api.searches.responses_calls), 3)

    def test_growing_responses_settle_when_count_stabilises(self):
        seq = [
            [],
            [{"username": f"u{i}", "files": []} for i in range(100)],
            [{"username": f"u{i}", "files": []} for i in range(400)],
            [{"username": f"u{i}", "files": []} for i in range(757)],
            [{"username": f"u{i}", "files": []} for i in range(757)],
        ]
        api = self._build(seq)
        result = search_exec._fetch_search_responses_settled(
            api, search_id=1, deadline_s=2.0, poll_s=0.2,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertEqual(len(result), 757)

    def test_deadline_returns_last_fetched_list_no_exception(self):
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed",
                            responses=[], response_count=0)
        api = _api_with(searches)
        clock = _FakeClock()
        call_n = {"i": 0}

        def _resp(sid: Any) -> list[dict[str, Any]]:
            searches.responses_calls.append(sid)
            k = call_n["i"]
            call_n["i"] = k + 1
            clock.advance(0.5)
            return [{"username": f"u{i}", "files": []} for i in range(k + 1)]

        searches.search_responses = _resp  # type: ignore[method-assign]
        result = search_exec._fetch_search_responses_settled(
            api, search_id=1, deadline_s=2.0, poll_s=0.2,
            clock_fn=clock, sleep_fn=_noop_sleep,
        )
        self.assertGreater(len(result), 0)
        self.assertLess(len(searches.responses_calls), 10)

    def test_search_responses_exception_propagates(self):
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed",
                            responses=[], response_count=0)

        def _raise(_sid: Any) -> list[dict[str, Any]]:
            raise RuntimeError("network blip")

        searches.search_responses = _raise  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            search_exec._fetch_search_responses_settled(
                _api_with(searches), search_id=1, deadline_s=2.0, poll_s=0.2,
                clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
            )


class TestNaturalCompletionSettleRace(unittest.TestCase):
    """The #242 race through the full ``execute_search`` natural-completion
    path: terminal state reported immediately, response store empty on the
    first read then populated. Pre-consolidation this lived on
    ``_collect_search_results``; it now belongs to the unified lifecycle."""

    def test_natural_completion_with_race_recovers_via_settle(self):
        late = [{"username": f"u{i}", "files": []} for i in range(757)]
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed, FileLimitReached",
                            responses=late, response_count=757)
        first = {"hit": False}

        def _resp(sid: Any) -> list[dict[str, Any]]:
            searches.responses_calls.append(sid)
            if not first["hit"]:
                first["hit"] = True
                return []
            return list(late)

        searches.search_responses = _resp  # type: ignore[method-assign]
        result = execute_search(
            _api_with(searches), search_id=1, delete=False,
            clock_fn=_FakeClock(), sleep_fn=_noop_sleep,
        )
        self.assertFalse(result.watchdog_fired)
        self.assertEqual(searches.stop_calls, [])
        self.assertEqual(len(result.responses), 757)
        self.assertEqual(result.response_count_terminal, 757)


if __name__ == "__main__":
    unittest.main()
