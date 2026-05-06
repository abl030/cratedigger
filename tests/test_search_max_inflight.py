"""Contract tests for cfg.search_max_inflight (issue #198 U4).

The hard-coded MAX_INFLIGHT=2 in `_search_and_queue_parallel` is replaced
with `cfg.search_max_inflight` (default 4). These tests pin:

  * The configured value is used to size the ThreadPoolExecutor and the
    initial seed loop — verified via the "Pipelined search: N albums,
    K in flight" log line.
  * Default (4) and a non-default override (1) both round-trip correctly.
"""

from __future__ import annotations

import configparser
import json
import logging
import threading
import time
import unittest
from dataclasses import replace
from unittest.mock import MagicMock, patch

import cratedigger
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.enqueue import (
    FindDownloadMetrics,
    FindDownloadOwnerPathError,
    FindDownloadResult,
)
from lib.grab_list import DownloadFile, GrabListEntry
from lib.quality import CandidateScore
from lib.search import SearchResult, SearchVariant
from tests.fakes import FakePipelineDB, FakeSlskdAPI


def _empty_cfg(**overrides) -> CratediggerConfig:
    """A real CratediggerConfig built from an empty INI (all defaults), then
    optionally overridden via dataclasses.replace."""
    cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


class TestSearchMaxInflightPipelineLog(unittest.TestCase):
    """The pipeline log line must report the configured value, not the
    legacy hard-coded 2."""

    def setUp(self) -> None:
        self._orig_cfg = cratedigger.cfg
        self._orig_slskd = cratedigger.slskd

    def tearDown(self) -> None:
        cratedigger.cfg = self._orig_cfg
        cratedigger.slskd = self._orig_slskd

    def _run_with(self, cfg: CratediggerConfig) -> str:
        """Run _search_and_queue_parallel with an empty album list, return
        the captured "Pipelined search" log line."""
        cratedigger.cfg = cfg
        cratedigger.slskd = MagicMock()
        ctx = MagicMock()
        ctx.cfg = cfg
        with self.assertLogs("cratedigger", level=logging.INFO) as captured:
            cratedigger._search_and_queue_parallel([], ctx)
        for record in captured.records:
            if "Pipelined search" in record.message:
                return record.message
        self.fail("Expected 'Pipelined search' log line not emitted")

    def test_default_search_max_inflight_is_four(self):
        cfg = _empty_cfg()
        self.assertEqual(
            cfg.search_max_inflight, 4,
            "default raised from legacy 2 to 4 (issue #198 U4)",
        )
        line = self._run_with(cfg)
        self.assertIn("4 in flight", line)

    def test_configured_value_is_used(self):
        cfg = _empty_cfg(search_max_inflight=1)
        line = self._run_with(cfg)
        self.assertIn("1 in flight", line)


class TestFindDownloadDoesNotBlockSearchRefill(unittest.TestCase):
    def test_blocked_find_download_does_not_prevent_next_search_submit(self):
        cfg = _empty_cfg(search_max_inflight=1)
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [101, 102]
        source = MagicMock()
        source._get_db.return_value = MagicMock()
        ctx = CratediggerContext(cfg=cfg, slskd=slskd, pipeline_db_source=source)
        albums = [
            MagicMock(id=1, artist_name="Artist", title="One"),
            MagicMock(id=2, artist_name="Artist", title="Two"),
        ]
        first_find_started = threading.Event()
        release_first_find = threading.Event()

        def variant_for(album, _cfg, _db):
            return (
                SearchVariant(
                    kind="default",
                    query=f"Artist {album.title}",
                    tag="default",
                    slice_index=None,
                ),
                f"Artist {album.title}",
            )

        def collect(_search_id, query, album_id, _cfg, _slskd, variant_tag):
            return SearchResult(
                album_id=album_id,
                success=True,
                cache_entries={},
                upload_speeds={},
                dir_audio_counts={},
                query=query,
                result_count=1,
                elapsed_s=0.01,
                variant_tag=variant_tag,
            )

        def find(album, _worker_ctx):
            if album.id == 1:
                first_find_started.set()
                self.assertTrue(release_first_find.wait(timeout=2))
            return FindDownloadResult(outcome="no_match")

        def run_pipeline():
            with patch.object(
                cratedigger, "_select_variant_for_album", side_effect=variant_for,
            ), patch.object(
                cratedigger, "_collect_search_results", side_effect=collect,
            ), patch.object(
                cratedigger,
                "prepare_find_download_context",
                side_effect=lambda album, ctx, result=None: ctx,
            ), patch.object(
                cratedigger, "find_download", side_effect=find,
            ):
                return cratedigger._search_and_queue_parallel(albums, ctx)

        thread = threading.Thread(target=run_pipeline)
        thread.start()
        try:
            self.assertTrue(first_find_started.wait(timeout=2))
            deadline = time.monotonic() + 2
            while (
                len(slskd.searches.search_text_calls) < 2
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            self.assertEqual(
                len(slskd.searches.search_text_calls),
                2,
                "second search should be submitted while first find_download is blocked",
            )
        finally:
            release_first_find.set()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())

    def test_parallel_find_download_results_merge_and_log_on_owner_thread(self):
        cfg = _empty_cfg(search_max_inflight=1)
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [201, 202]
        db = FakePipelineDB()
        rid_found = db.add_request(
            artist_name="Artist",
            album_title="Found",
            source="request",
            mb_release_id="mbid-found",
        )
        rid_miss = db.add_request(
            artist_name="Artist",
            album_title="Miss",
            source="request",
            mb_release_id="mbid-miss",
        )
        source = MagicMock()
        source._get_db.return_value = db
        ctx = CratediggerContext(cfg=cfg, slskd=slskd, pipeline_db_source=source)
        albums = [
            MagicMock(
                id=-rid_found,
                db_request_id=rid_found,
                artist_name="Artist",
                title="Found",
            ),
            MagicMock(
                id=-rid_miss,
                db_request_id=rid_miss,
                artist_name="Artist",
                title="Miss",
            ),
        ]
        score_found = CandidateScore(
            username="peer1",
            dir="Music\\Found",
            filetype="flac",
            matched_tracks=2,
            total_tracks=2,
            avg_ratio=1.0,
            missing_titles=[],
            file_count=2,
        )
        score_miss = CandidateScore(
            username="peer2",
            dir="Music\\Miss",
            filetype="flac",
            matched_tracks=0,
            total_tracks=2,
            avg_ratio=0.0,
            missing_titles=[],
            file_count=1,
        )
        grab_entry = GrabListEntry(
            album_id=-rid_found,
            files=[
                DownloadFile(
                    filename="Music\\Found\\01.flac",
                    id="transfer-1",
                    file_dir="Music\\Found",
                    username="peer1",
                    size=123,
                ),
            ],
            filetype="flac",
            title="Found",
            artist="Artist",
            year="1991",
            mb_release_id="mbid-found",
            db_request_id=rid_found,
            db_source="request",
        )

        def variant_for(album, _cfg, _db):
            return (
                SearchVariant(
                    kind="default",
                    query=f"Artist {album.title}",
                    tag="default",
                    slice_index=None,
                ),
                f"Artist {album.title}",
            )

        def collect(_search_id, query, album_id, _cfg, _slskd, variant_tag):
            return SearchResult(
                album_id=album_id,
                success=True,
                cache_entries={},
                upload_speeds={},
                dir_audio_counts={},
                query=query,
                result_count=1,
                elapsed_s=0.01,
                variant_tag=variant_tag,
                final_state="Completed",
            )

        def find(album, _worker_ctx):
            if album.id == -rid_found:
                return FindDownloadResult(
                    outcome="found",
                    grab_entry=grab_entry,
                    candidates=(score_found,),
                    metrics=FindDownloadMetrics(
                        browse_time_s=1.5,
                        match_time_s=0.25,
                        peers_browsed=3,
                        peers_browsed_lazy=1,
                        fanout_waves=2,
                        cache_pos_hits=5,
                        cache_neg_hits=1,
                        cache_misses=8,
                    ),
                )
            return FindDownloadResult(
                outcome="no_match",
                candidates=(score_miss,),
                metrics=FindDownloadMetrics(
                    browse_time_s=0.5,
                        match_time_s=0.75,
                        peers_browsed=4,
                        peers_browsed_lazy=0,
                        fanout_waves=1,
                        cache_pos_hits=2,
                        cache_neg_hits=3,
                        cache_misses=4,
                    ),
                )

        with patch.object(
            cratedigger, "_select_variant_for_album", side_effect=variant_for,
        ), patch.object(
            cratedigger, "_collect_search_results", side_effect=collect,
        ), patch.object(
            cratedigger,
            "prepare_find_download_context",
            side_effect=lambda album, ctx, result=None: ctx,
        ), patch.object(
            cratedigger, "find_download", side_effect=find,
        ):
            grab_list, failed_search, failed_grab = cratedigger._search_and_queue_parallel(
                albums,
                ctx,
            )

        self.assertEqual(list(grab_list), [-rid_found])
        self.assertIs(grab_list[-rid_found], grab_entry)
        self.assertEqual(failed_search, [])
        self.assertEqual(failed_grab, [albums[1]])
        self.assertEqual(ctx.find_download_queued, 2)
        self.assertEqual(ctx.find_download_completed, 2)
        self.assertEqual(ctx.browse_time_s, 2.0)
        self.assertEqual(ctx.match_time_s, 1.0)
        self.assertEqual(ctx.peers_browsed, 7)
        self.assertEqual(ctx.peers_browsed_lazy, 1)
        self.assertEqual(ctx.fanout_waves, 3)
        self.assertEqual(ctx.cache_pos_hits, 7)
        self.assertEqual(ctx.cache_neg_hits, 4)
        self.assertEqual(ctx.cache_misses, 12)

        logs_by_request = {row.request_id: row for row in db.search_logs}
        self.assertEqual(logs_by_request[rid_found].outcome, "found")
        self.assertEqual(logs_by_request[rid_miss].outcome, "no_match")
        self.assertEqual(logs_by_request[rid_found].peers_browsed, 3)
        self.assertEqual(logs_by_request[rid_found].peers_browsed_lazy, 1)
        self.assertEqual(logs_by_request[rid_found].fanout_waves, 2)
        self.assertEqual(logs_by_request[rid_found].browse_time_s, 1.5)
        self.assertEqual(logs_by_request[rid_miss].peers_browsed, 4)
        self.assertEqual(logs_by_request[rid_miss].fanout_waves, 1)
        found_candidates = json.loads(logs_by_request[rid_found].candidates or "[]")
        miss_candidates = json.loads(logs_by_request[rid_miss].candidates or "[]")
        self.assertEqual(found_candidates[0]["username"], "peer1")
        self.assertEqual(miss_candidates[0]["username"], "peer2")

    def test_owner_exception_after_find_submit_returns_partial_grab(self):
        cfg = _empty_cfg(search_max_inflight=1)
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [301]
        db = FakePipelineDB()
        rid_found = db.add_request(
            artist_name="Artist",
            album_title="Found",
            source="request",
            mb_release_id="mbid-found",
        )
        rid_crash = db.add_request(
            artist_name="Artist",
            album_title="Crash",
            source="request",
            mb_release_id="mbid-crash",
        )
        source = MagicMock()
        source._get_db.return_value = db
        ctx = CratediggerContext(cfg=cfg, slskd=slskd, pipeline_db_source=source)
        albums = [
            MagicMock(
                id=-rid_found,
                db_request_id=rid_found,
                artist_name="Artist",
                title="Found",
            ),
            MagicMock(
                id=-rid_crash,
                db_request_id=rid_crash,
                artist_name="Artist",
                title="Crash",
            ),
        ]
        grab_entry = GrabListEntry(
            album_id=-rid_found,
            files=[
                DownloadFile(
                    filename="Music\\Found\\01.flac",
                    id="transfer-1",
                    file_dir="Music\\Found",
                    username="peer1",
                    size=123,
                ),
            ],
            filetype="flac",
            title="Found",
            artist="Artist",
            year="1991",
            mb_release_id="mbid-found",
            db_request_id=rid_found,
            db_source="request",
        )

        def variant_for(album, _cfg, _db):
            if album.id == -rid_crash:
                raise RuntimeError("owner path failed after worker submit")
            return (
                SearchVariant(
                    kind="default",
                    query=f"Artist {album.title}",
                    tag="default",
                    slice_index=None,
                ),
                f"Artist {album.title}",
            )

        def collect(_search_id, query, album_id, _cfg, _slskd, variant_tag):
            return SearchResult(
                album_id=album_id,
                success=True,
                query=query,
                result_count=1,
                elapsed_s=0.01,
                variant_tag=variant_tag,
            )

        def find(_album, _worker_ctx):
            return FindDownloadResult(
                outcome="found",
                grab_entry=grab_entry,
                metrics=FindDownloadMetrics(),
            )

        with patch.object(
            cratedigger, "_select_variant_for_album", side_effect=variant_for,
        ), patch.object(
            cratedigger, "_collect_search_results", side_effect=collect,
        ), patch.object(
            cratedigger,
            "prepare_find_download_context",
            side_effect=lambda album, ctx, result=None: ctx,
        ), patch.object(
            cratedigger, "find_download", side_effect=find,
        ), self.assertRaises(FindDownloadOwnerPathError):
            cratedigger._search_and_queue_parallel(albums, ctx)


if __name__ == "__main__":
    unittest.main()
