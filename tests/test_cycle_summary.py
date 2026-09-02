"""Tests for cycle-summary formatting and per-cycle instrumentation.

The summary line is the operator-facing signal for browse/search/match timing,
fan-out work, and Redis peer-cache behavior.
"""
from __future__ import annotations

import configparser
import time
import unittest
from datetime import UTC, datetime
from typing import ClassVar, cast
from unittest.mock import MagicMock, patch

from cratedigger import SlskdFile, TrackRecord
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.cycle_counters import COUNTER_NAMES, CycleCounters
from lib.cycle_summary import (
    CYCLE_COMPLETE_PREFIX,
    format_cycle_summary,
    log_cycle_summary,
    record_cycle_metrics_cycle,
    record_peer_observations_cycle,
)
from lib.matching import check_for_match
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
from tests.helpers import (
    make_ctx_with_fake_db,
    make_cycle_collaborators,
    rebind_collaborators,
)


def _make_ctx() -> CratediggerContext:
    cfg = MagicMock()
    cfg.var_dir = "/tmp/unused"
    return CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=cfg,
            slskd=FakeSlskdAPI(),
            pipeline_db_source=FakePipelineDBSource(),
        ),
    )


def _make_real_cfg() -> CratediggerConfig:
    ini = configparser.ConfigParser()
    ini["Search Settings"] = {
        "minimum_filename_match_ratio": "0.5",
        "ignored_users": "",
        "allowed_filetypes": "flac, mp3",
        "browse_parallelism": "4",
    }
    return CratediggerConfig.from_ini(ini)


def _make_real_ctx() -> CratediggerContext:
    cfg = _make_real_cfg()
    ctx = CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=cfg,
            slskd=FakeSlskdAPI(),
            pipeline_db_source=FakePipelineDBSource(),
        ),
    )
    album = MagicMock()
    album.title = "Cool Album"
    album.artist_name = "Cool Artist"
    ctx.current_album_cache[1] = album
    return ctx


def _track(title: str) -> TrackRecord:
    return cast(TrackRecord, {"albumId": 1, "title": title, "mediumNumber": 1})


def _file(filename: str) -> SlskdFile:
    return cast(SlskdFile, {"filename": filename})


class TestPhase1ContextForwarding(unittest.TestCase):
    """Phase 1 polls on its own thread with its own DB connection, but
    every OTHER per-cycle collaborator must be forwarded from the owner
    context or Phase 1 silently degrades (issue #1278).

    ``download_ownership`` is the one with teeth: Phase 1 reaches
    ``lib.download._timeout_album`` -> ``cancel_and_delete``, which is
    gated on positively proven ledger ownership and fails CLOSED without
    the collaborator — turning every download-timeout cleanup into a
    logged no-op while the code, CLAUDE.md and the PR all claim it is
    ownership-scoped.
    """

    def _owner_ctx(self) -> CratediggerContext:
        from lib.download_ownership import DownloadOwnershipWriter
        ctx = _make_ctx()
        ledger = FakePipelineDB()
        rebind_collaborators(ctx, download_ownership=DownloadOwnershipWriter(
            db_factory=lambda: ledger, close_after_use=False))
        ctx.cooled_down_users = {"grumpy-peer"}
        return ctx

    def test_forwards_the_ownership_collaborator(self):
        from cratedigger import build_phase1_context

        owner = self._owner_ctx()

        phase1 = build_phase1_context(
            cfg=owner.cfg,
            slskd=owner.slskd,
            pipeline_db_source=FakePipelineDBSource(),
            owner_ctx=owner,
        )

        self.assertIs(phase1.download_ownership, owner.download_ownership)

    def test_forwards_cooldowns_and_keeps_its_own_db_source(self):
        from cratedigger import build_phase1_context

        owner = self._owner_ctx()
        phase1_source = FakePipelineDBSource()

        phase1 = build_phase1_context(
            cfg=owner.cfg,
            slskd=owner.slskd,
            pipeline_db_source=phase1_source,
            owner_ctx=owner,
        )

        # assertIs, not assertEqual: sharing the SET OBJECT is what lets
        # Phase 1's `ctx.cooled_down_users.add(...)` (lib/download.py)
        # reach the owner context and, through it, Phase 2's worker
        # contexts. Copying the set instead would satisfy equality and
        # silently strand every cooldown Phase 1 discovers — a mutant
        # replacing the forward with `set(owner_ctx.cooled_down_users)`
        # survived the equality version of this assertion (#1278 review).
        self.assertIs(phase1.cooled_down_users, owner.cooled_down_users)
        self.assertIs(phase1.pipeline_db_source, phase1_source)
        self.assertIsNot(phase1.pipeline_db_source, owner.pipeline_db_source)


class TestContextAccumulators(unittest.TestCase):
    """A fresh context carries a fresh all-zero counters value."""

    def test_accumulator_fields_default_zero(self):
        ctx = _make_ctx()
        self.assertEqual(ctx.counters, CycleCounters())
        self.assertEqual(ctx.peer_observations, set())

    def test_each_context_gets_its_own_counters(self):
        """Not a shared default: a worker's tally must not reach a sibling.

        ``default_factory`` is what makes this true; a plain
        ``counters: CycleCounters = CycleCounters()`` default would be a
        single instance shared by every context in the process, so one
        cycle's numbers would leak into the next.
        """
        first, second = _make_ctx(), _make_ctx()
        first.counters.peers_browsed += 7
        self.assertEqual(second.counters.peers_browsed, 0)


class TestFormatCycleSummary(unittest.TestCase):
    """``format_cycle_summary(counters, elapsed_s)`` renders the one
    operator-facing line, every counter as a ``key=value`` pair.

    ``EXPECTED_LINE`` is the whole contract, spelled out. Production
    derives the line from ``lib.cycle_counters``, so an expectation
    derived the same way would agree with any renderer by construction
    and could not notice a counter leaving the line — which is exactly
    how ``search_time_s=`` went unasserted for the life of this file
    (issue #1348). This literal is the one place the sentence an operator
    greps for is written down.
    """

    #: The counters used to render ``EXPECTED_LINE``. One distinct value
    #: per counter so a swap between two of them changes the line.
    POPULATED = CycleCounters(
        browse_time_s=12.3,
        match_time_s=4.5,
        search_time_s=6.25,
        cache_pos_hits=8,
        cache_neg_hits=9,
        cache_misses=10,
        cache_errors=11,
        cache_fuse_tripped=12,
        cache_write_errors=13,
        peers_browsed=42,
        peers_browsed_lazy=5,
        fanout_waves=2,
        cycle_searches_watchdog_killed=7,
        find_download_queued=3,
        find_download_completed=1,
        find_download_drain_time_s=8.9,
    )

    EXPECTED_LINE = (
        "Cratedigger cycle complete in 99.9s "
        "browse_time_s=12.3 match_time_s=4.5 search_time_s=6.2 "
        "cache_pos_hits=8 cache_neg_hits=9 cache_misses=10 "
        "cache_errors=11 cache_fuse_tripped=12 cache_write_errors=13 "
        "peers_browsed=42 peers_browsed_lazy=5 fanout_waves=2 "
        "cycle_searches_watchdog_killed=7 find_download_queued=3 "
        "find_download_completed=1 find_download_drain_time_s=8.9 "
        "cycle_total_s=99.9"
    )

    def test_summary_renders_the_exact_operator_line(self):
        self.assertEqual(
            format_cycle_summary(self.POPULATED, elapsed_s=99.9),
            self.EXPECTED_LINE,
        )

    def test_summary_renders_the_exact_zero_cycle_line(self):
        self.assertEqual(
            format_cycle_summary(CycleCounters(), elapsed_s=0.0),
            "Cratedigger cycle complete in 0.0s "
            "browse_time_s=0.0 match_time_s=0.0 search_time_s=0.0 "
            "cache_pos_hits=0 cache_neg_hits=0 cache_misses=0 "
            "cache_errors=0 cache_fuse_tripped=0 cache_write_errors=0 "
            "peers_browsed=0 peers_browsed_lazy=0 fanout_waves=0 "
            "cycle_searches_watchdog_killed=0 find_download_queued=0 "
            "find_download_completed=0 find_download_drain_time_s=0.0 "
            "cycle_total_s=0.0",
        )

    def test_every_declared_counter_reaches_the_line(self):
        """Declaring a counter and forgetting to log it is now impossible.

        This one IS derived, deliberately: it is the half that catches a
        counter ADDED to the value type, where ``EXPECTED_LINE`` cannot
        help because a human wrote it. The two together cover both
        directions — a token vanishing, and a counter never arriving.
        """
        line = format_cycle_summary(self.POPULATED, elapsed_s=99.9)
        for name in COUNTER_NAMES:
            with self.subTest(counter=name):
                self.assertIn(f"{name}=", line)

    def test_summary_renders_floats_by_declaration_not_by_value(self):
        """An int assigned to a float counter still logs one decimal."""
        counters = CycleCounters()
        counters.browse_time_s = 12
        self.assertIn(
            "browse_time_s=12.0", format_cycle_summary(counters, elapsed_s=1.0))

    def test_summary_is_single_line(self):
        line = format_cycle_summary(CycleCounters(), elapsed_s=1.0)
        self.assertNotIn("\n", line, "summary must be one line for grep-ability")

    def test_summary_preserves_human_prefix(self):
        """Log scrapers match 'Cratedigger cycle complete' and nothing else
        (scripts/verify_cratedigger_cycle.sh, docs/nixos-module.md), so the
        prefix is a harder contract than the tokens after it."""
        line = format_cycle_summary(CycleCounters(), elapsed_s=1.0)
        self.assertTrue(
            line.startswith("Cratedigger cycle complete"),
            f"prefix changed: {line!r}",
        )
        self.assertIn(CYCLE_COMPLETE_PREFIX, line)


class TestMatchTimeAccumulator(unittest.TestCase):
    """`ctx.counters.match_time_s` accumulates across check_for_match calls regardless
    of return path, including exceptions raised inside the matching loop.
    Reviewer-flagged gap: pre-fix, the accumulator was a += at two separate
    return sites and would silently drop time on any exception path."""

    USERNAME = "user1"
    TRACKS: ClassVar = [_track("Alpha"), _track("Bravo"), _track("Charlie")]

    def _seed_cache(self, ctx: CratediggerContext, dir_name: str,
                    files: list[SlskdFile]) -> None:
        ctx.folder_cache.setdefault(self.USERNAME, {})[dir_name] = {
            "directory": dir_name,
            "files": files,
        }

    def test_match_time_increments_on_successful_match(self):
        ctx = _make_real_ctx()
        self._seed_cache(ctx, "dirA", [
            _file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac"),
        ])
        result = check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        self.assertTrue(result.matched)
        self.assertGreater(
            ctx.counters.match_time_s, 0.0,
            "matched return path must credit match_time_s",
        )

    def test_match_time_increments_on_no_match(self):
        ctx = _make_real_ctx()
        # Files don't match track titles → no strict accept
        self._seed_cache(ctx, "dirA", [
            _file("zzz1.flac"), _file("zzz2.flac"), _file("zzz3.flac"),
        ])
        result = check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        self.assertFalse(result.matched)
        self.assertGreater(
            ctx.counters.match_time_s, 0.0,
            "no-match return path must credit match_time_s",
        )

    def test_match_time_credited_when_album_match_raises(self):
        """An exception inside the matching loop must still credit
        match_time_s (try/finally contract). Regression guard for the
        pre-fix bug where two += sites silently dropped time on raise.

        Uses the ``album_match_fn`` kwarg DI on ``check_for_match`` to
        inject an exception-raising stub. The production code path
        runs through the same try/finally accumulator as a real
        ``album_match`` call would.
        """
        ctx = _make_real_ctx()
        self._seed_cache(ctx, "dirA", [
            _file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac"),
        ])

        def _boom(*_args, **_kwargs):
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            check_for_match(
                self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx,
                album_match_fn=_boom,
            )
        self.assertGreater(
            ctx.counters.match_time_s, 0.0,
            "exception inside matching loop must still credit match_time_s",
        )

    def test_match_time_accumulates_across_calls(self):
        """Multiple check_for_match calls in one cycle add up — a single
        long-running album doesn't overwrite a short one's contribution."""
        ctx = _make_real_ctx()
        self._seed_cache(ctx, "dirA", [
            _file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac"),
        ])
        check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        first_total = ctx.counters.match_time_s
        self.assertGreater(first_total, 0.0)
        check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        self.assertGreater(
            ctx.counters.match_time_s, first_total,
            "second call must add to match_time_s, not replace it",
        )


class TestBrowseTimeAccumulator(unittest.TestCase):
    """`ctx.counters.browse_time_s` accumulates around `_browse_directories` calls,
    including the exception path. Same try/finally contract as match_time_s."""

    USERNAME = "user1"
    TRACKS: ClassVar = [_track("Alpha"), _track("Bravo"), _track("Charlie")]

    def test_browse_time_credited_when_browse_raises(self):
        ctx = _make_real_ctx()
        # Don't seed cache — forces the browse path to fire
        with patch(
            "lib.matching._browse_directories",
            side_effect=RuntimeError("network broke"),
        ), self.assertRaises(RuntimeError):
            check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        self.assertGreater(
            ctx.counters.browse_time_s, 0.0,
            "exception inside _browse_directories must still credit browse_time_s",
        )
        # The lazy-fallback path bumps peers_browsed_lazy (issue #198 review #5);
        # peers_browsed is reserved for fan-out submissions in lib/enqueue.py.
        self.assertEqual(ctx.counters.peers_browsed_lazy, 1)
        self.assertEqual(ctx.counters.peers_browsed, 0, "fan-out path should not be credited")

    def test_browse_time_zero_when_cache_warm(self):
        ctx = _make_real_ctx()
        ctx.folder_cache.setdefault(self.USERNAME, {})["dirA"] = {
            "directory": "dirA",
            "files": [_file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac")],
        }
        check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        self.assertEqual(
            ctx.counters.browse_time_s, 0.0,
            "cache hit shouldn't count as browse work",
        )
        self.assertEqual(ctx.counters.peers_browsed, 0)
        self.assertEqual(ctx.counters.peers_browsed_lazy, 0)


class TestCloseOutSteps(unittest.TestCase):
    """The three registered end-of-cycle close-out steps persist domain state."""

    def _ctx(self, db: FakePipelineDB):
        ctx = make_ctx_with_fake_db(db, cfg=CratediggerConfig())
        ctx.cycle_started_at = datetime(2026, 8, 31, 8, 0, tzinfo=UTC)
        ctx.cycle_start = time.time() - 12.0
        return ctx

    def test_log_cycle_summary_emits_the_canonical_line(self):
        ctx = self._ctx(FakePipelineDB())
        ctx.counters.peers_browsed = 3

        with self.assertLogs("cratedigger", level="INFO") as captured:
            line = log_cycle_summary(ctx)

        self.assertIn("Cratedigger cycle complete in", line)
        self.assertIn("peers_browsed=3", line)
        self.assertIn(line, captured.output[0])
        # The elapsed figure is derived, so assert its VALUE: the fixture
        # started the cycle 12 seconds ago. Checking only that the line
        # says "complete in <something>s" leaves `time.time() +
        # ctx.cycle_start` alive, which reports every cycle as having
        # taken about 56 years.
        elapsed = float(line.split(" in ")[1].split("s ")[0])
        self.assertGreaterEqual(elapsed, 12.0)
        self.assertLess(elapsed, 60.0)

    def test_record_cycle_metrics_persists_every_context_accumulator(self):
        # A distinct value per counter, derived from the declaration so a
        # newly declared counter is covered the day it lands: a swapped
        # forward fails, not only a dropped one.
        db = FakePipelineDB()
        ctx = self._ctx(db)
        forwarded = {
            name: 2 + offset for offset, name in enumerate(COUNTER_NAMES)}
        for name, value in forwarded.items():
            setattr(ctx.counters, name, value)

        record_cycle_metrics_cycle(ctx)

        self.assertEqual(len(db.cycle_metrics), 1)
        row = db.cycle_metrics[0]
        self.assertEqual(row["started_at"], ctx.cycle_started_at)
        # Bounded, not just a floor: an unbounded assertion is satisfied
        # by `time.time() + ctx.cycle_start`, which persists a duration of
        # roughly 1.8 billion seconds for every cycle.
        self.assertGreaterEqual(row["cycle_total_s"], 12.0)
        self.assertLess(row["cycle_total_s"], 60.0)
        # The completion stamp goes into a TIMESTAMPTZ column; a naive
        # local datetime here is silently off by the host's UTC offset.
        self.assertIsNotNone(row["created_at"].tzinfo)
        for name, value in forwarded.items():
            with self.subTest(counter=name):
                self.assertEqual(row[name], value)

    def test_record_peer_observations_flushes_the_roster(self):
        db = FakePipelineDB()
        ctx = self._ctx(db)
        ctx.peer_observations = {"peer-a", "peer-b"}

        self.assertEqual(record_peer_observations_cycle(ctx), 2)
        self.assertEqual(len(db.peer_observations), 2)

    def test_record_peer_observations_skips_an_empty_roster(self):
        # Assert the guard itself, not the end state: the fake's
        # record_peer_observations is naturally idempotent on empty input,
        # so an empty peer_observations dict would hold even with the
        # production guard deleted (review mutant M17 survived on exactly
        # that). The step's contract is that an empty roster makes NO DB
        # call and logs NO persisted line.
        db = FakePipelineDB()
        ctx = self._ctx(db)
        calls: list[object] = []
        real_record = db.record_peer_observations

        def _recording_record(*args, **kwargs):
            calls.append((args, kwargs))
            return real_record(*args, **kwargs)

        db.record_peer_observations = _recording_record

        self.assertEqual(record_peer_observations_cycle(ctx), 0)
        self.assertEqual(calls, [])
        self.assertEqual(db.peer_observations, {})


if __name__ == "__main__":
    unittest.main()
