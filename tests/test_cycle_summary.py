"""Tests for U1 instrumentation: cycle-summary formatting + cache_load_s timing.

Issue #198 — instrumentation lands first as its own commit so we have numerical
"before" data on browse / search / match / cache_load percentages before the
fan-out refactor.
"""
from __future__ import annotations

import configparser
import json
import os
import tempfile
import unittest
from typing import cast
from unittest.mock import MagicMock, patch

from cratedigger import SlskdFile, TrackRecord
from lib.cache import load_caches, save_caches
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.cycle_summary import format_cycle_summary
from lib.matching import check_for_match


def _make_ctx() -> CratediggerContext:
    cfg = MagicMock()
    cfg.var_dir = "/tmp/unused"
    slskd = MagicMock()
    return CratediggerContext(cfg=cfg, slskd=slskd, pipeline_db_source=MagicMock())


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
        cfg=cfg,
        slskd=MagicMock(),
        pipeline_db_source=MagicMock(),
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


class TestContextAccumulators(unittest.TestCase):
    """The new per-cycle accumulator fields exist on CratediggerContext and
    default to zero, so any consumer can reference them safely."""

    def test_accumulator_fields_default_zero(self):
        ctx = _make_ctx()
        self.assertEqual(ctx.browse_time_s, 0.0)
        self.assertEqual(ctx.match_time_s, 0.0)
        self.assertEqual(ctx.cache_load_s, 0.0)
        self.assertEqual(ctx.cycle_browse_time_s, 0.0)
        self.assertEqual(ctx.peers_browsed, 0)
        self.assertEqual(ctx.peers_timed_out, 0)
        self.assertEqual(ctx.fanout_waves, 0)


class TestCacheLoadTiming(unittest.TestCase):
    """`load_caches` records its wall-clock duration on `ctx.cache_load_s` so
    the cycle summary can attribute time to the JSON cache load tax."""

    def test_load_with_existing_file_records_positive_duration(self):
        with tempfile.TemporaryDirectory() as var_dir:
            # Seed a small valid cache file
            payload = {
                "saved_at": "2026-05-01T00:00:00+00:00",
                "folder_cache": {},
                "user_upload_speed": {},
                "search_dir_audio_count": {},
            }
            with open(os.path.join(var_dir, "cratedigger_cache.json"), "w") as f:
                json.dump(payload, f)

            ctx = _make_ctx()
            self.assertEqual(ctx.cache_load_s, 0.0)
            load_caches(ctx, var_dir)
            self.assertGreater(ctx.cache_load_s, 0.0)
            self.assertLess(ctx.cache_load_s, 5.0,
                            "cache load on a tiny file should be near-instant")

    def test_load_with_missing_file_leaves_duration_zero(self):
        with tempfile.TemporaryDirectory() as var_dir:
            ctx = _make_ctx()
            load_caches(ctx, var_dir)
            self.assertEqual(ctx.cache_load_s, 0.0,
                             "no file → no measurable load → stay at 0.0")

    def test_load_with_corrupt_file_records_parse_attempt(self):
        """Corrupt files still credit the (tiny) parse-attempt cost so the
        cycle summary reflects time we actually spent. The try/finally
        guarantees the metric isn't silently dropped on the early-return
        path — that would be the same kind of bug as #2 in matching.py."""
        with tempfile.TemporaryDirectory() as var_dir:
            with open(os.path.join(var_dir, "cratedigger_cache.json"), "w") as f:
                f.write("{ not valid json")
            ctx = _make_ctx()
            load_caches(ctx, var_dir)
            self.assertGreater(ctx.cache_load_s, 0.0)
            self.assertLess(
                ctx.cache_load_s, 1.0,
                "corrupt-file parse cost should be near-instant",
            )

    def test_save_load_roundtrip_preserves_accumulator_default(self):
        """save_caches doesn't read or write accumulator fields — they're
        cycle-scoped, not persistent."""
        with tempfile.TemporaryDirectory() as var_dir:
            ctx = _make_ctx()
            ctx.browse_time_s = 99.0  # would-be live-cycle value
            save_caches(ctx, var_dir)

            ctx2 = _make_ctx()
            load_caches(ctx2, var_dir)
            self.assertEqual(
                ctx2.browse_time_s, 0.0,
                "accumulators must not leak across cycles via the cache file",
            )


class TestFormatCycleSummary(unittest.TestCase):
    """`format_cycle_summary(ctx, elapsed_s)` returns a single log line that
    includes every R13/R15 metric as a `key=value` pair, so log scrapers can
    parse browse/search/match/cache_load contributions out of one line."""

    REQUIRED_KEYS = (
        "browse_time_s=",
        "match_time_s=",
        "cache_load_s=",
        "peers_browsed=",
        "peers_browsed_lazy=",
        "peers_timed_out=",
        "fanout_waves=",
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
        ctx.cache_load_s = 6.7
        ctx.peers_browsed = 42
        ctx.peers_browsed_lazy = 5
        ctx.peers_timed_out = 3
        ctx.fanout_waves = 2
        line = format_cycle_summary(ctx, elapsed_s=99.9)
        self.assertIn("browse_time_s=12.3", line)
        self.assertIn("match_time_s=4.5", line)
        self.assertIn("cache_load_s=6.7", line)
        self.assertIn("peers_browsed=42", line)
        self.assertIn("peers_browsed_lazy=5", line)
        self.assertIn("peers_timed_out=3", line)
        self.assertIn("fanout_waves=2", line)
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
    TRACKS = [_track("Alpha"), _track("Bravo"), _track("Charlie")]

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
        pre-fix bug where two += sites silently dropped time on raise."""
        ctx = _make_real_ctx()
        self._seed_cache(ctx, "dirA", [
            _file("Alpha.flac"), _file("Bravo.flac"), _file("Charlie.flac"),
        ])
        with patch("lib.matching.album_match", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                check_for_match(self.TRACKS, "flac", ["dirA"], self.USERNAME, ctx)
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
    TRACKS = [_track("Alpha"), _track("Bravo"), _track("Charlie")]

    def test_browse_time_credited_when_browse_raises(self):
        ctx = _make_real_ctx()
        # Don't seed cache — forces the browse path to fire
        with patch(
            "lib.matching._browse_directories",
            side_effect=RuntimeError("network broke"),
        ):
            with self.assertRaises(RuntimeError):
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


if __name__ == "__main__":
    unittest.main()
