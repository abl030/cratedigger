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
from lib.cycle_summary import (
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
    """The new per-cycle accumulator fields exist on CratediggerContext and
    default to zero, so any consumer can reference them safely."""

    def test_accumulator_fields_default_zero(self):
        ctx = _make_ctx()
        self.assertEqual(ctx.browse_time_s, 0.0)
        self.assertEqual(ctx.match_time_s, 0.0)
        self.assertEqual(ctx.cache_pos_hits, 0)
        self.assertEqual(ctx.cache_neg_hits, 0)
        self.assertEqual(ctx.cache_misses, 0)
        self.assertEqual(ctx.cache_errors, 0)
        self.assertEqual(ctx.cache_fuse_tripped, 0)
        self.assertEqual(ctx.cache_write_errors, 0)
        self.assertEqual(ctx.peers_browsed, 0)
        self.assertEqual(ctx.peers_browsed_lazy, 0)
        self.assertEqual(ctx.fanout_waves, 0)
        self.assertEqual(ctx.peer_observations, set())


class TestFormatCycleSummary(unittest.TestCase):
    """`format_cycle_summary(ctx, elapsed_s)` returns a single log line that
    includes every R13/R15 metric as a `key=value` pair, so log scrapers can
    parse browse/search/match/cache_load contributions out of one line."""

    REQUIRED_KEYS = (
        "browse_time_s=",
        "match_time_s=",
        "cache_pos_hits=",
        "cache_neg_hits=",
        "cache_misses=",
        "cache_errors=",
        "cache_fuse_tripped=",
        "cache_write_errors=",
        "peers_browsed=",
        "peers_browsed_lazy=",
        "fanout_waves=",
        "find_download_queued=",
        "find_download_completed=",
        "find_download_drain_time_s=",
        "cycle_total_s=",
    )

    def test_summary_includes_all_required_keys_on_zero_cycle(self):
        ctx = _make_ctx()
        line = format_cycle_summary(ctx, elapsed_s=0.0)
        for key in self.REQUIRED_KEYS:
            self.assertIn(key, line, f"missing key {key!r} in summary line")

    def test_summary_reflects_populated_values(self):
        ctx = _make_ctx()
        ctx.browse_time_s = 12.3
        ctx.match_time_s = 4.5
        ctx.cache_pos_hits = 8
        ctx.cache_neg_hits = 9
        ctx.cache_misses = 10
        ctx.cache_errors = 11
        ctx.cache_fuse_tripped = 12
        ctx.cache_write_errors = 13
        ctx.peers_browsed = 42
        ctx.peers_browsed_lazy = 5
        ctx.fanout_waves = 2
        ctx.find_download_queued = 3
        ctx.find_download_completed = 2
        ctx.find_download_drain_time_s = 8.9
        line = format_cycle_summary(ctx, elapsed_s=99.9)
        self.assertIn("browse_time_s=12.3", line)
        self.assertIn("match_time_s=4.5", line)
        self.assertNotIn("cache_load_s=", line)
        self.assertIn("cache_pos_hits=8", line)
        self.assertIn("cache_neg_hits=9", line)
        self.assertIn("cache_misses=10", line)
        self.assertIn("cache_errors=11", line)
        self.assertIn("cache_fuse_tripped=12", line)
        self.assertIn("cache_write_errors=13", line)
        self.assertIn("peers_browsed=42", line)
        self.assertIn("peers_browsed_lazy=5", line)
        self.assertIn("fanout_waves=2", line)
        self.assertIn("find_download_queued=3", line)
        self.assertIn("find_download_completed=2", line)
        self.assertIn("find_download_drain_time_s=8.9", line)
        self.assertIn("cycle_total_s=99.9", line)

    def test_summary_is_single_line(self):
        ctx = _make_ctx()
        line = format_cycle_summary(ctx, elapsed_s=1.0)
        self.assertNotIn("\n", line, "summary must be one line for grep-ability")

    def test_summary_preserves_human_prefix(self):
        """Existing log scrapers expect 'Cratedigger cycle complete' as the
        prefix; new keys append to it."""
        ctx = _make_ctx()
        line = format_cycle_summary(ctx, elapsed_s=1.0)
        self.assertTrue(
            line.startswith("Cratedigger cycle complete"),
            f"prefix changed: {line!r}",
        )


class TestMatchTimeAccumulator(unittest.TestCase):
    """`ctx.match_time_s` accumulates across check_for_match calls regardless
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
            ctx.match_time_s, 0.0,
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
            ctx.match_time_s, 0.0,
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
            ctx.match_time_s, 0.0,
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
        first_total = ctx.match_time_s
        self.assertGreater(first_total, 0.0)
        check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        self.assertGreater(
            ctx.match_time_s, first_total,
            "second call must add to match_time_s, not replace it",
        )


class TestBrowseTimeAccumulator(unittest.TestCase):
    """`ctx.browse_time_s` accumulates around `_browse_directories` calls,
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
            ctx.browse_time_s, 0.0,
            "exception inside _browse_directories must still credit browse_time_s",
        )
        # The lazy-fallback path bumps peers_browsed_lazy (issue #198 review #5);
        # peers_browsed is reserved for fan-out submissions in lib/enqueue.py.
        self.assertEqual(ctx.peers_browsed_lazy, 1)
        self.assertEqual(ctx.peers_browsed, 0, "fan-out path should not be credited")

    def test_browse_time_zero_when_cache_warm(self):
        ctx = _make_real_ctx()
        ctx.folder_cache.setdefault(self.USERNAME, {})["dirA"] = {
            "directory": "dirA",
            "files": [_file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac")],
        }
        check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
        self.assertEqual(
            ctx.browse_time_s, 0.0,
            "cache hit shouldn't count as browse work",
        )
        self.assertEqual(ctx.peers_browsed, 0)
        self.assertEqual(ctx.peers_browsed_lazy, 0)


class TestCloseOutSteps(unittest.TestCase):
    """The three registered end-of-cycle close-out steps persist domain state."""

    def _ctx(self, db: FakePipelineDB):
        ctx = make_ctx_with_fake_db(db, cfg=CratediggerConfig())
        ctx.cycle_started_at = datetime(2026, 8, 31, 8, 0, tzinfo=UTC)
        ctx.cycle_start = time.time() - 12.0
        return ctx

    def test_log_cycle_summary_emits_the_canonical_line(self):
        ctx = self._ctx(FakePipelineDB())
        ctx.peers_browsed = 3

        with self.assertLogs("cratedigger", level="INFO") as captured:
            line = log_cycle_summary(ctx)

        self.assertIn("Cratedigger cycle complete in", line)
        self.assertIn("peers_browsed=3", line)
        self.assertIn(line, captured.output[0])

    def test_record_cycle_metrics_persists_every_context_accumulator(self):
        # Distinct value per forwarded field so a swapped forward
        # (e.g. browse_time_s=ctx.match_time_s) fails, not only a dropped
        # one — asserting a 3-field sample left the other 13 unpinned.
        db = FakePipelineDB()
        ctx = self._ctx(db)
        forwarded = {
            "browse_time_s": 1.5, "match_time_s": 2.5, "search_time_s": 3.5,
            "cache_pos_hits": 4, "cache_neg_hits": 5, "cache_misses": 6,
            "cache_errors": 7, "cache_fuse_tripped": 8,
            "cache_write_errors": 9, "peers_browsed": 10,
            "peers_browsed_lazy": 11, "fanout_waves": 12,
            "cycle_searches_watchdog_killed": 13,
            "find_download_queued": 14, "find_download_completed": 15,
            "find_download_drain_time_s": 16.5,
        }
        for fieldname, value in forwarded.items():
            setattr(ctx, fieldname, value)

        record_cycle_metrics_cycle(ctx)

        self.assertEqual(len(db.cycle_metrics), 1)
        row = db.cycle_metrics[0]
        self.assertEqual(row["started_at"], ctx.cycle_started_at)
        self.assertGreaterEqual(row["cycle_total_s"], 12.0)
        for fieldname, value in forwarded.items():
            with self.subTest(field=fieldname):
                self.assertEqual(row[fieldname], value)

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
