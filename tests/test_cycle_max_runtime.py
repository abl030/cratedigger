"""Cycle deadline gate (issue #198 follow-up to the 2026-05-02 rollback).

The 2026-05-02 rollback (commit 10d8af3) removed every client-side wall-clock
guard — wave deadlines, cycle browse budget, and the legacy
`slskd_timeout * 2 + 15` per-search poll cap — because they were terminating
productive work mid-call and cascading skips across albums (search outcome
`timeout` jumped from ~1% to 35%, found rate dropped 13.7% → 2.2%).

The cure for the *opposite* failure mode (search 16/16 stalled forever, cycle
ran 8h53m) is **not** to bring back the same kind of guards. It is to add a
single cycle-level entry gate:

  * Past `cfg.cycle_max_runtime_s`, do not submit any new searches.
  * In-flight work continues to completion (no mid-call termination).
  * Albums not yet submitted stay `wanted` for the next cycle (no cascade
    write of "failed" or "skipped" outcomes).

This file pins those four invariants. Default is 600 s; opt-out with `<= 0`
preserves the rollback's "no client-side cap" behaviour for emergencies.
"""
from __future__ import annotations

import configparser
import logging
import time
import unittest
from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock, patch

import cratedigger
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.cycle_summary import format_cycle_summary


def _empty_cfg(**overrides) -> CratediggerConfig:
    cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


# ---------------------------------------------------------------------------
# Plumbing: config + context + cycle summary
# ---------------------------------------------------------------------------


class TestCycleMaxRuntimeConfig(unittest.TestCase):
    """`cfg.cycle_max_runtime_s` exists, defaults to 600, parses from INI."""

    def test_default_is_600(self):
        cfg = _empty_cfg()
        self.assertEqual(cfg.cycle_max_runtime_s, 600)

    def test_ini_override(self):
        ini = configparser.ConfigParser()
        ini["Search Settings"] = {"cycle_max_runtime_s": "120"}
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.cycle_max_runtime_s, 120)

    def test_zero_means_no_cap(self):
        """Opt-out: <= 0 preserves the rollback's "no client-side cap"
        behaviour. The deadline gate must treat this as disabled, not as
        "deadline = now → skip everything"."""
        ini = configparser.ConfigParser()
        ini["Search Settings"] = {"cycle_max_runtime_s": "0"}
        cfg = CratediggerConfig.from_ini(ini)
        self.assertEqual(cfg.cycle_max_runtime_s, 0)


class TestCycleDeadlineContext(unittest.TestCase):
    """`ctx.cycle_deadline` and `ctx.cycle_deadline_skipped` exist with safe
    defaults so consumers can reference them unconditionally."""

    def test_deadline_default_none(self):
        ctx = CratediggerContext(
            cfg=MagicMock(), slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )
        self.assertIsNone(ctx.cycle_deadline)
        self.assertEqual(ctx.cycle_deadline_skipped, 0)


class TestCycleSummaryDeadlineField(unittest.TestCase):
    """The cycle-summary line surfaces `cycle_deadline_skipped` so we can
    grep for cycles that hit the deadline (and how many albums deferred)."""

    def _ctx(self, **fields) -> CratediggerContext:
        ctx = CratediggerContext(
            cfg=MagicMock(), slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )
        for k, v in fields.items():
            setattr(ctx, k, v)
        return ctx

    def test_deadline_skipped_zero_is_emitted(self):
        line = format_cycle_summary(self._ctx(), elapsed_s=412.3)
        self.assertIn("cycle_deadline_skipped=0", line)

    def test_deadline_skipped_nonzero(self):
        line = format_cycle_summary(self._ctx(cycle_deadline_skipped=7), elapsed_s=611.0)
        self.assertIn("cycle_deadline_skipped=7", line)


# ---------------------------------------------------------------------------
# Behaviour: parallel pipeline gate
# ---------------------------------------------------------------------------


class _Album:
    """Minimal stand-in for an AlbumRecord that the parallel pipeline
    reads `id`, `title`, and `artist_name` off."""
    def __init__(self, album_id: int):
        self.id = album_id
        self.title = f"Title{album_id}"
        self.artist_name = f"Artist{album_id}"


def _stub_submit_next_returns_albums(albums: list[_Album]):
    """Build a fake `_submit_search` that always succeeds — returns a tuple
    shaped like the real submit_result so the pipeline keeps marching."""
    from itertools import count
    counter = count()

    def _fake_submit_search(album, variant, search_cfg, slskd_client):
        return (next(counter), variant.query or f"q{album.id}", album.id, variant.tag)

    return _fake_submit_search


class TestParallelPipelineDeadlineGate(unittest.TestCase):
    """When `time.time() > ctx.cycle_deadline`, `_submit_next` must return
    None and log how many albums are deferred. Albums are NOT marked
    `failed_search` — they simply stay on the queue for the next cycle.
    """

    def setUp(self) -> None:
        self._orig_cfg = cratedigger.cfg
        self._orig_slskd = cratedigger.slskd

    def tearDown(self) -> None:
        cratedigger.cfg = self._orig_cfg
        cratedigger.slskd = self._orig_slskd

    def _make_ctx(self, deadline: float | None) -> CratediggerContext:
        cfg = _empty_cfg(cycle_max_runtime_s=120)
        cratedigger.cfg = cfg
        cratedigger.slskd = MagicMock()
        ctx = CratediggerContext(
            cfg=cfg,
            slskd=cratedigger.slskd,
            pipeline_db_source=MagicMock(),
        )
        ctx.cycle_deadline = deadline
        return ctx

    def test_no_deadline_runs_all_albums(self):
        """Sanity baseline: with cycle_deadline=None, every album submitted."""
        ctx = self._make_ctx(deadline=None)
        albums = [_Album(i) for i in range(5)]

        # Patch the variant selector + submitter + collector to no-op happily.
        from lib.search import SearchResult, SearchVariant
        variant = SearchVariant(kind="default", query="q", tag="default", slice_index=None)

        with patch.object(cratedigger, "_select_variant_for_album",
                          return_value=(variant, "q")), \
             patch.object(cratedigger, "_submit_search",
                          side_effect=_stub_submit_next_returns_albums(albums)), \
             patch.object(cratedigger, "_collect_search_results",
                          side_effect=lambda *a, **k: SearchResult(
                              album_id=0, success=False, query="q", outcome="not_found",
                              variant_tag="default",
                          )), \
             patch.object(cratedigger, "_log_search_result"), \
             patch.object(cratedigger, "find_download"):
            cratedigger._search_and_queue_parallel(albums, ctx)

        self.assertEqual(ctx.cycle_deadline_skipped, 0)

    def test_past_deadline_skips_remaining(self):
        """When the deadline has elapsed BEFORE the seed loop runs,
        no albums get submitted and the deferred counter equals the queue
        length."""
        ctx = self._make_ctx(deadline=time.time() - 1.0)  # already past
        albums = [_Album(i) for i in range(8)]

        submit_calls: list[Any] = []

        def _track_submit(album, variant, search_cfg, slskd_client):
            submit_calls.append(album.id)
            return (album.id, "q", album.id, variant.tag)

        from lib.search import SearchVariant
        variant = SearchVariant(kind="default", query="q", tag="default", slice_index=None)

        with patch.object(cratedigger, "_select_variant_for_album",
                          return_value=(variant, "q")), \
             patch.object(cratedigger, "_submit_search", side_effect=_track_submit), \
             patch.object(cratedigger, "_collect_search_results"), \
             patch.object(cratedigger, "_log_search_result"), \
             patch.object(cratedigger, "find_download"), \
             self.assertLogs("cratedigger", level=logging.INFO) as captured:
            cratedigger._search_and_queue_parallel(albums, ctx)

        self.assertEqual(submit_calls, [],
                         "no slskd searches should be submitted past the deadline")
        self.assertEqual(ctx.cycle_deadline_skipped, 8)
        deadline_lines = [r.message for r in captured.records
                          if "cycle deadline" in r.message.lower()]
        self.assertTrue(deadline_lines, "expected a cycle-deadline log line")
        self.assertTrue(any("8" in m for m in deadline_lines),
                        "deferred count should appear in the log line")

    def test_deadline_does_not_mark_failed_search(self):
        """Deferred albums must not appear in any "failed" bucket — they
        remain `wanted` and the next cycle picks them up. The skipped
        counter is the only signal."""
        ctx = self._make_ctx(deadline=time.time() - 1.0)
        albums = [_Album(i) for i in range(4)]

        # If the function ever calls _log_search_result on a deferred album
        # with an outcome != deferred, that would write a forensic row and
        # consume the album. We assert it is never called.
        log_calls: list[Any] = []

        def _track_log(album, result, ctx):
            log_calls.append((album.id, result.outcome))

        from lib.search import SearchVariant
        variant = SearchVariant(kind="default", query="q", tag="default", slice_index=None)

        with patch.object(cratedigger, "_select_variant_for_album",
                          return_value=(variant, "q")), \
             patch.object(cratedigger, "_submit_search"), \
             patch.object(cratedigger, "_collect_search_results"), \
             patch.object(cratedigger, "_log_search_result", side_effect=_track_log), \
             patch.object(cratedigger, "find_download"):
            cratedigger._search_and_queue_parallel(albums, ctx)

        self.assertEqual(log_calls, [],
                         "deferred albums must not be logged as failed/timeout")


# ---------------------------------------------------------------------------
# Behaviour: serial pipeline gate
# ---------------------------------------------------------------------------


class TestSerialPipelineDeadlineGate(unittest.TestCase):
    """The serial path (`search_and_queue` when parallel_searches <= 1 or
    only one album) must honour the same cycle deadline."""

    def setUp(self) -> None:
        self._orig_cfg = cratedigger.cfg

    def tearDown(self) -> None:
        cratedigger.cfg = self._orig_cfg

    def _ctx(self, deadline: float | None) -> CratediggerContext:
        cfg = _empty_cfg(parallel_searches=1, cycle_max_runtime_s=60)
        cratedigger.cfg = cfg
        ctx = CratediggerContext(
            cfg=cfg, slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )
        ctx.cycle_deadline = deadline
        return ctx

    def test_serial_skips_remaining_past_deadline(self):
        ctx = self._ctx(deadline=time.time() - 1.0)
        albums = [_Album(i) for i in range(3)]

        with patch.object(cratedigger, "search_for_album") as mock_search:
            cratedigger.search_and_queue(albums, ctx)

        self.assertEqual(mock_search.call_count, 0,
                         "no album should hit search_for_album past deadline")
        self.assertEqual(ctx.cycle_deadline_skipped, 3)


# ---------------------------------------------------------------------------
# Opt-out: cycle_max_runtime_s <= 0
# ---------------------------------------------------------------------------


class TestZeroMeansDisabled(unittest.TestCase):
    """`cycle_max_runtime_s <= 0` must NOT cause the deadline to be set
    (or, equivalently, the gate must treat None and 0 the same). Pinned
    so the opt-out switch keeps working."""

    def test_zero_disables_deadline_in_ctx(self):
        cfg = _empty_cfg(cycle_max_runtime_s=0)
        ctx = CratediggerContext(
            cfg=cfg, slskd=MagicMock(), pipeline_db_source=MagicMock(),
        )
        # Helper used by main() to compute the deadline. Returns None when
        # the cap is disabled so the gate short-circuits.
        from lib.context import compute_cycle_deadline
        self.assertIsNone(compute_cycle_deadline(cfg, now=time.time()))

    def test_positive_returns_future_timestamp(self):
        from lib.context import compute_cycle_deadline
        cfg = _empty_cfg(cycle_max_runtime_s=120)
        now = time.time()
        deadline = compute_cycle_deadline(cfg, now=now)
        assert deadline is not None
        self.assertAlmostEqual(deadline - now, 120.0, places=1)


if __name__ == "__main__":
    unittest.main()
