"""Settle-and-poll mitigation for the slskd state→responses race (issue #242).

slskd's terminal-state transition (``Completed, FileLimitReached`` /
``ResponseLimitReached`` / ``TimedOut``) and its response-store commit are
written by different threads and have no atomicity guarantee. Reading
``search_responses`` immediately after seeing ``"Completed"`` in state can
return an empty list while the writer is still flushing. Empirical evidence:
``FileLimitReached`` zero-rate of 56% on identical queries that return 700+
results 30 seconds later.

The watchdog-cancel branch already mitigates this on the cancel path with a
state-poll loop bounded at ``SEARCH_CANCEL_WAIT_DEADLINE_S``. This module
generalises that mitigation to all terminal-state transitions by polling
``search_responses`` directly until two consecutive calls return the same
length (the natural stability signal) or a deadline expires.

Pinned invariants:
  * Helper polls ``search_responses`` (NOT state) — the response list is
    the actual data we care about settling.
  * Two consecutive same-length reads is the stability signal. In the
    happy path (already settled), this costs exactly one extra HTTP call.
  * Helper bounded at ``deadline_s``; on deadline expiry it returns the
    most-recently-fetched list rather than retrying forever.
  * Helper deterministic under ``clock_fn`` injection — production omits
    the kwarg (defaults to ``time.monotonic``).
  * Both production sites — ``search_for_album`` (serial pipeline) and
    ``_collect_search_results`` (parallel pipeline natural-completion
    branch) — use the helper so neither drops responses to the race.
  * The watchdog-cancel branch in ``_collect_search_results`` reuses the
    same helper; its semantics (5.0s post-cancel deadline) are preserved.
"""
from __future__ import annotations

import unittest
import unittest.mock
from dataclasses import replace
from typing import Any

import cratedigger
from lib.config import CratediggerConfig
from tests.fakes import FakeSlskdSearches


class _FakeClock:
    """Deterministic monotonic clock — see test_search_watchdog._FakeClock."""
    def __init__(self, start: float = 0.0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += float(seconds)


class _FakeSlskd:
    """Minimal slskd stand-in — only ``searches`` is used."""
    def __init__(self, searches: FakeSlskdSearches) -> None:
        self.searches = searches


def _empty_cfg(**overrides) -> CratediggerConfig:
    import configparser
    cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


def _patch_sleep():
    """No-op ``time.sleep`` — the helper polls inside a sleep loop."""
    return unittest.mock.patch.object(cratedigger.time, "sleep", lambda _s: None)


# ---------------------------------------------------------------------------
# Direct unit tests of the settle helper
# ---------------------------------------------------------------------------


class TestFetchSearchResponsesSettled(unittest.TestCase):
    """Pure-ish unit tests for ``_fetch_search_responses_settled``.

    The helper sits between ``_collect_search_results``'s state-poll exit
    and the existing harvest path. It must:

      1. Return immediately (after one extra confirmatory call) when
         responses are already settled.
      2. Loop on the race window — re-fetch until two consecutive calls
         agree — and return the settled list.
      3. Honour the deadline; never spin forever.
    """

    def _build(self, response_sequence: list[list[dict[str, Any]]]) -> _FakeSlskd:
        """Build a fake slskd whose ``search_responses`` returns the given
        sequence on consecutive calls. Past the end, returns the last list
        (slskd's response store, once committed, is stable)."""
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed",
                            responses=[], response_count=0)

        # Override search_responses to return the sequence in order.
        idx = {"i": 0}
        original = searches.search_responses

        def _resp(sid: Any) -> list[dict[str, Any]]:
            original(sid)  # record the call for assertions
            i = idx["i"]
            if i < len(response_sequence):
                idx["i"] = i + 1
                return list(response_sequence[i])
            return list(response_sequence[-1])

        searches.search_responses = _resp  # type: ignore[method-assign]
        return _FakeSlskd(searches)

    def test_already_settled_returns_after_one_extra_call(self):
        """Happy path: first call returns 47 rows, second confirmatory call
        returns the same 47 rows. Helper returns those 47 — costs exactly
        one extra HTTP call vs. naive ``search_responses`` (the price of
        race protection)."""
        responses = [{"username": f"u{i}", "files": []} for i in range(47)]
        slskd = self._build([responses, responses])
        clock = _FakeClock()

        with _patch_sleep():
            result = cratedigger._fetch_search_responses_settled(
                slskd, search_id=1,
                deadline_s=2.0, poll_s=0.2, clock_fn=clock,
            )

        self.assertEqual(len(result), 47)
        self.assertEqual(len(slskd.searches.responses_calls), 2,
                         "settled-on-first-call costs exactly one extra fetch")

    def test_race_resolves_on_second_call(self):
        """The race scenario from issue #242: first ``search_responses``
        call returns ``[]`` because slskd's response store hasn't committed
        yet; the second call returns 757 rows once the writer flushes.
        Helper must not return the empty list — it has to loop until
        consecutive calls agree.
        """
        late_responses = [{"username": f"u{i}", "files": []} for i in range(757)]
        # Sequence: [], 757, 757 — third call confirms the second.
        slskd = self._build([[], late_responses, late_responses])
        clock = _FakeClock()

        with _patch_sleep():
            result = cratedigger._fetch_search_responses_settled(
                slskd, search_id=1,
                deadline_s=2.0, poll_s=0.2, clock_fn=clock,
            )

        self.assertEqual(len(result), 757,
                         "helper must wait past the race window before returning")
        self.assertGreaterEqual(len(slskd.searches.responses_calls), 3,
                                "race resolution requires at least 3 fetches "
                                "(empty -> 757 -> confirm 757)")

    def test_growing_responses_settle_when_count_stabilises(self):
        """Responses can grow during the race (slskd commits in chunks).
        Helper must wait until the count stops growing before returning.
        Sequence: 0, 100, 400, 757, 757."""
        seq = [
            [],
            [{"username": f"u{i}", "files": []} for i in range(100)],
            [{"username": f"u{i}", "files": []} for i in range(400)],
            [{"username": f"u{i}", "files": []} for i in range(757)],
            [{"username": f"u{i}", "files": []} for i in range(757)],
        ]
        slskd = self._build(seq)
        clock = _FakeClock()

        with _patch_sleep():
            result = cratedigger._fetch_search_responses_settled(
                slskd, search_id=1,
                deadline_s=2.0, poll_s=0.2, clock_fn=clock,
            )

        self.assertEqual(len(result), 757)

    def test_deadline_returns_last_fetched_list_no_exception(self):
        """If responses never settle (slskd's writer is hung), the helper
        must respect its deadline and return the last list it saw — no
        exception, no infinite loop. The fact that we got *something* is
        better than crashing.
        """
        sequence = [
            [{"username": "u0", "files": []}],
            [{"username": "u0", "files": []}, {"username": "u1", "files": []}],
        ]
        slskd = self._build(sequence)
        clock = _FakeClock()

        # Override search_responses so each call returns a NEW length and
        # advances the clock past the deadline.
        call_n = {"i": 0}

        def _resp(sid: Any) -> list[dict[str, Any]]:
            slskd.searches.responses_calls.append(sid)
            n = call_n["i"]
            call_n["i"] = n + 1
            clock.advance(0.5)
            # Return ever-growing lists so two consecutive calls never agree.
            return [{"username": f"u{i}", "files": []} for i in range(n + 1)]

        slskd.searches.search_responses = _resp  # type: ignore[method-assign]

        with _patch_sleep():
            result = cratedigger._fetch_search_responses_settled(
                slskd, search_id=1,
                deadline_s=2.0, poll_s=0.2, clock_fn=clock,
            )

        # Returned the last list seen; did not raise; respected the deadline.
        self.assertGreater(len(result), 0)
        # Should have stopped well within ~5 calls given 0.5s/call vs 2.0s budget.
        self.assertLess(len(slskd.searches.responses_calls), 10)

    def test_search_responses_raises_propagates(self):
        """Caller already wraps the harvest in try/except. The helper does
        not need to swallow exceptions from ``search_responses`` — let them
        propagate so the caller can route to the existing error path.
        """
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed",
                            responses=[], response_count=0)

        def _raise(_sid: Any) -> list[dict[str, Any]]:
            raise RuntimeError("network blip")

        searches.search_responses = _raise  # type: ignore[method-assign]
        slskd = _FakeSlskd(searches)
        clock = _FakeClock()

        with _patch_sleep():
            with self.assertRaises(RuntimeError):
                cratedigger._fetch_search_responses_settled(
                    slskd, search_id=1,
                    deadline_s=2.0, poll_s=0.2, clock_fn=clock,
                )


# ---------------------------------------------------------------------------
# Integration: the race in the natural-completion branch of
# `_collect_search_results` (parallel pipeline). RED test pre-fix.
# ---------------------------------------------------------------------------


class TestRaceInCollectSearchResults(unittest.TestCase):
    """Reproduces the issue #242 scenario in the parallel pipeline.

    Pre-fix: ``_collect_search_results`` reads ``search_responses`` once
    immediately after seeing ``Completed`` in state. If slskd's response
    store hasn't committed yet, the harvest sees ``[]`` and the search is
    classified ``no_results`` — even though hundreds of responses are
    moments away.

    Post-fix: the helper polls until two consecutive calls agree, so the
    race is bridged and the harvest gets the real 757-row list.
    """

    def _setup_race_search(self) -> tuple[_FakeSlskd, FakeSlskdSearches]:
        """Build a slskd fake where state immediately reports
        ``Completed, FileLimitReached`` but ``search_responses`` returns
        ``[]`` on the first call and 757 rows thereafter.
        """
        late_responses = [{"username": f"u{i}", "files": []}
                          for i in range(757)]
        searches = FakeSlskdSearches()
        # Important: state already terminal — the race is between state
        # update and response-store commit on slskd's side.
        searches.add_search(search_id=1,
                            state="Completed, FileLimitReached",
                            responses=late_responses,
                            response_count=757)

        # Override search_responses so the FIRST call returns []
        # (modelling slskd's writer not yet flushed) and subsequent
        # calls return the real 757-row list.
        first_call = {"hit": False}

        def _resp(sid: Any) -> list[dict[str, Any]]:
            searches.responses_calls.append(sid)
            if not first_call["hit"]:
                first_call["hit"] = True
                return []
            return list(late_responses)

        searches.search_responses = _resp  # type: ignore[method-assign]
        return _FakeSlskd(searches), searches

    def test_natural_completion_with_race_recovers_via_settle(self):
        """The race scenario: ``Completed, FileLimitReached`` is reported
        immediately; the first ``search_responses`` call returns ``[]`` but
        the next returns 757 rows. Pre-fix this test asserts ``result_count
        > 0`` and FAILS (gets 0). Post-fix it passes (gets 757).
        """
        slskd, searches = self._setup_race_search()
        cfg = _empty_cfg()
        clock = _FakeClock()

        with _patch_sleep():
            result = cratedigger._collect_search_results(
                1, "q", album_id=42, search_cfg=cfg, slskd_client=slskd,
                clock_fn=clock,
            )

        self.assertFalse(result.watchdog_fired,
                         "natural completion must not fire the watchdog")
        self.assertEqual(searches.stop_calls, [],
                         "stop() must NOT be called on natural completion")
        self.assertGreater(result.result_count or 0, 0,
                           "the race fix must bridge the empty -> populated "
                           "window so we don't drop a 700-row search")
        self.assertEqual(result.result_count, 757)
        self.assertNotEqual(result.outcome, "no_results",
                            "post-fix the harvest sees the real responses")


if __name__ == "__main__":
    unittest.main()
