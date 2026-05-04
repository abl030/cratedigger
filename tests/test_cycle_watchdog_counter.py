"""Cycle watchdog counter (issue #212).

Replaces the rolled-back `cycle_deadline_skipped` counter that fed the
removed `cycle_max_runtime_s` cycle-entry gate.

`ctx.cycle_searches_watchdog_killed` accumulates one increment per
`SearchResult` whose `watchdog_fired=True` and is surfaced in the
cycle-summary log line so operators can spot stuck-search firing rates
(healthy steady-state is 0–1 per cycle; >3 sustained warrants
investigation).
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from lib.context import CratediggerContext
from lib.cycle_summary import format_cycle_summary


class TestContextCounterDefault(unittest.TestCase):

    def test_default_zero(self):
        ctx = CratediggerContext(
            cfg=MagicMock(), slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )
        self.assertEqual(ctx.cycle_searches_watchdog_killed, 0)


class TestCycleSummaryWatchdogField(unittest.TestCase):
    """The cycle-summary line surfaces `cycle_searches_watchdog_killed`
    so operators can grep for cycles that fired the watchdog and how
    many searches were killed. Replaces the removed
    `cycle_deadline_skipped=` field — old field name MUST NOT appear."""

    def _ctx(self, **fields) -> CratediggerContext:
        ctx = CratediggerContext(
            cfg=MagicMock(), slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )
        for k, v in fields.items():
            setattr(ctx, k, v)
        return ctx

    def test_zero_emitted(self):
        line = format_cycle_summary(self._ctx(), elapsed_s=412.3)
        self.assertIn("cycle_searches_watchdog_killed=0", line)
        self.assertNotIn("cycle_deadline_skipped", line)

    def test_three_emitted(self):
        line = format_cycle_summary(
            self._ctx(cycle_searches_watchdog_killed=3), elapsed_s=611.0)
        self.assertIn("cycle_searches_watchdog_killed=3", line)


class TestLogSearchResultIncrementsCounter(unittest.TestCase):
    """`_log_search_result` is the single increment site — it sees every
    SearchResult emitted by both the parallel pipeline and the serial
    fallback. Watchdog-fired results bump the counter; non-fired do not."""

    def _ctx(self) -> CratediggerContext:
        # Pipeline-db source needs `_get_db()` to return a MagicMock for
        # the log_search / record_attempt calls inside _log_search_result.
        # Bare MagicMock() already provides this — every attribute access
        # auto-vivifies — so no explicit setup is needed.
        return CratediggerContext(
            cfg=MagicMock(), slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )

    def _result(self, *, watchdog_fired: bool, outcome: str = "no_results"):
        from lib.search import SearchResult
        return SearchResult(
            album_id=1, success=False, query="q",
            outcome=outcome, variant_tag="default",
            watchdog_fired=watchdog_fired,
        )

    def _album(self, with_request_id: bool = True):
        album = MagicMock()
        album.db_request_id = 42 if with_request_id else None
        return album

    def test_watchdog_fired_bumps_counter(self):
        import cratedigger
        ctx = self._ctx()
        cratedigger._log_search_result(
            self._album(), self._result(watchdog_fired=True), ctx)
        self.assertEqual(ctx.cycle_searches_watchdog_killed, 1)

    def test_normal_result_does_not_bump_counter(self):
        import cratedigger
        ctx = self._ctx()
        cratedigger._log_search_result(
            self._album(), self._result(watchdog_fired=False), ctx)
        self.assertEqual(ctx.cycle_searches_watchdog_killed, 0)

    def test_three_watchdog_fires_in_a_cycle(self):
        """Mixed cycle: 5 results, 3 watchdog'd, 2 normal. Counter is 3."""
        import cratedigger
        ctx = self._ctx()
        for _ in range(3):
            cratedigger._log_search_result(
                self._album(), self._result(watchdog_fired=True), ctx)
        for _ in range(2):
            cratedigger._log_search_result(
                self._album(), self._result(watchdog_fired=False), ctx)
        self.assertEqual(ctx.cycle_searches_watchdog_killed, 3)

    def test_counter_increments_even_without_db_request_id(self):
        """Albums with no db_request_id still bump the counter — the
        early-return for log persistence is for DB writes, not for
        per-cycle telemetry."""
        import cratedigger
        ctx = self._ctx()
        cratedigger._log_search_result(
            self._album(with_request_id=False),
            self._result(watchdog_fired=True), ctx)
        self.assertEqual(ctx.cycle_searches_watchdog_killed, 1)


class TestRemovedFieldsAreGone(unittest.TestCase):
    """Pin that the old gate API is gone — guards against accidental
    reintroduction. R7, R8, R9 from issue #212."""

    def test_config_does_not_have_cycle_max_runtime_s(self):
        import configparser
        from lib.config import CratediggerConfig
        cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
        self.assertFalse(hasattr(cfg, "cycle_max_runtime_s"))

    def test_context_does_not_have_cycle_deadline(self):
        ctx = CratediggerContext(
            cfg=MagicMock(), slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )
        self.assertFalse(hasattr(ctx, "cycle_deadline"))
        self.assertFalse(hasattr(ctx, "cycle_deadline_skipped"))

    def test_compute_cycle_deadline_is_removed(self):
        import lib.context as ctx_mod
        self.assertFalse(hasattr(ctx_mod, "compute_cycle_deadline"))


if __name__ == "__main__":
    unittest.main()
