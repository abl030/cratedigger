#!/usr/bin/env python3
"""Server-level endpoint tests: static routes, error handling, client disconnects.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import json
import logging
import os
import sys
import unittest
from unittest.mock import patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _FakeDbWebServerCase

from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestServerEndpoints(_FakeDbWebServerCase):
    """Test HTTP endpoints return expected status and structure."""

    def setUp(self) -> None:
        super().setUp()
        self.db.seed_request(make_request_row(
            id=100, status="imported", min_bitrate=320,
        ))
        self.db.set_tracks(100, [
            {"disc_number": 1, "track_number": 1, "title": "Track",
             "length_seconds": 180},
        ])
        self.db.log_download(
            100, outcome="success", beets_scenario="strong_match",
            beets_distance=0.012, soulseek_username="testuser",
            filetype="mp3", bitrate=320000, actual_filetype="mp3",
            actual_min_bitrate=320, valid=True,
        )

    def _seed_log_counts_state(self) -> None:
        """Real rows behind /api/pipeline/log counts: setUp's one
        success row + these make total=7, imported=2; three recent
        found-searches, one aged past the 6h window (#445 item 2 —
        the counts flow through PipelineDB.get_download_log_counts
        and the fake's state-derived mirror, no queued cursor)."""
        from datetime import timedelta

        self.db.log_download(100, outcome="force_import")
        for _ in range(5):
            self.db.log_download(100, outcome="rejected")
        for _ in range(3):
            self.db.log_search(100, outcome="found")
        self.db.search_logs[0].created_at -= timedelta(hours=12)

    # --- GET endpoints ---

    def test_index_returns_html(self):
        with urlopen(f"{self.base}/") as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/html", resp.headers.get("Content-Type", ""))

    def test_pipeline_log_returns_entries(self):
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        self.assertIn("log", data)
        self.assertIn("counts", data)
        self.assertIsInstance(data["log"], list)
        if data["log"]:
            entry = data["log"][0]
            for key in ("badge", "verdict", "summary", "album_title",
                        "artist_name", "outcome"):
                self.assertIn(key, entry, f"Missing key '{key}' in log entry")

    def test_pipeline_log_filter_imported(self):
        self.db.log_download(100, outcome="rejected",
                             soulseek_username="rejuser")
        status, data = self._get("/api/pipeline/log?outcome=imported")
        self.assertEqual(status, 200)
        self.assertTrue(data["log"])
        self.assertEqual(
            {e["outcome"] for e in data["log"]}, {"success"},
            "imported filter must drop the rejected row")

    def test_pipeline_log_filter_rejected(self):
        self.db.log_download(100, outcome="rejected",
                             soulseek_username="rejuser")
        status, data = self._get("/api/pipeline/log?outcome=rejected")
        self.assertEqual(status, 200)
        self.assertEqual(
            {e["outcome"] for e in data["log"]}, {"rejected"},
            "rejected filter must drop the success row")

    def test_pipeline_log_limit_param(self):
        self.db.log_download(100, outcome="rejected")
        self.db.log_download(100, outcome="rejected")
        status, data = self._get("/api/pipeline/log?outcome=rejected&limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["log"]), 1)

    def test_pipeline_log_limit_param_is_capped(self):
        # 501 extra rows + the setUp row = 502 total; the route must
        # cap ?limit=5000 to 500 rows.
        for _ in range(501):
            self.db.log_download(100, outcome="success")
        status, data = self._get("/api/pipeline/log?limit=5000")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["log"]), 500)
        # And the no-param default is 50 — same seeded rows.
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["log"]), 50)

    def test_pipeline_log_filter_invalid_ignored(self):
        self.db.log_download(100, outcome="rejected",
                             soulseek_username="rejuser")
        status, data = self._get("/api/pipeline/log?outcome=badvalue")
        self.assertEqual(status, 200)
        # Invalid filter is ignored — BOTH outcomes are returned.
        self.assertEqual(
            {e["outcome"] for e in data["log"]}, {"success", "rejected"})

    def test_pipeline_log_counts_structure(self):
        self._seed_log_counts_state()
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        counts = data["counts"]
        for key in ("all", "imported", "rejected", "matches_24h", "matches_6h"):
            self.assertIn(key, counts)
            self.assertIsInstance(counts[key], int)
        for key in ("matches_per_hour_24h", "matches_per_hour_6h"):
            self.assertIn(key, counts)
            self.assertIsInstance(counts[key], (int, float))
        # The seeded rows actually flowed into the payload.
        self.assertEqual(counts["all"], 7)
        self.assertEqual(counts["imported"], 2)
        # rejected (5) is coprime with matches_24h (3) so a wiring swap
        # cannot pass by numeric coincidence; 24h (3) ≠ 6h (2) so a
        # window transposition cannot either.
        self.assertEqual(counts["rejected"], 5)
        self.assertEqual(counts["matches_24h"], 3)
        self.assertEqual(counts["matches_6h"], 2)
        self.assertAlmostEqual(counts["matches_per_hour_24h"], 3 / 24)
        self.assertAlmostEqual(counts["matches_per_hour_6h"], 2 / 6)

    def test_pipeline_status(self):
        status, data = self._get("/api/pipeline/status")
        self.assertEqual(status, 200)
        self.assertIn("counts", data)
        self.assertIn("wanted", data)

    def test_pipeline_all(self):
        status, data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertIn("counts", data)
        for key in ("wanted", "downloading", "imported", "unsearchable"):
            self.assertIn(key, data)

    def test_pipeline_status_includes_downloading(self):
        """count_by_status includes downloading when albums are downloading."""
        for rid in (201, 202):
            self.db.seed_request(make_request_row(
                id=rid, status="downloading", mb_release_id=f"dl-{rid}",
            ))
        status, data = self._get("/api/pipeline/status")
        self.assertEqual(status, 200)
        self.assertEqual(data["counts"]["downloading"], 2)

    def test_pipeline_all_includes_downloading(self):
        """get_pipeline_all returns downloading albums in the response."""
        self.db.seed_request(make_request_row(
            id=200, album_title="Downloading Album", artist_name="DL Artist",
            mb_release_id="dl-uuid", status="downloading",
            active_download_state={"filetype": "flac", "enqueued_at": "now", "files": []},
        ))
        status, data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertIn("downloading", data)
        self.assertEqual(len(data["downloading"]), 1)
        self.assertEqual(data["downloading"][0]["album_title"], "Downloading Album")

    def test_pipeline_downloading_returns_current_downloads_only(self):
        self.db.seed_request(make_request_row(
            id=201, album_title="Active Download", artist_name="DL Artist",
            mb_release_id="dl-uuid", status="downloading",
            active_download_state={
                "filetype": "mp3 320",
                "enqueued_at": "2026-05-05T12:00:00+00:00",
                "files": [{"username": "peer", "bytes_transferred": 1, "size": 2}],
            },
        ))
        # A prior rejected attempt — its summary must be stamped onto the
        # downloading row (the real get_latest_download_summaries path).
        self.db.log_download(
            201, outcome="rejected", soulseek_username="peer",
            beets_scenario="high_distance",
        )

        status, data = self._get("/api/pipeline/downloading")

        self.assertEqual(status, 200)
        self.assertEqual(data["counts"]["downloading"], 1)
        self.assertEqual(len(data["downloading"]), 1)
        row = data["downloading"][0]
        self.assertEqual(row["album_title"], "Active Download")
        # Request 100 (imported, from setUp) must NOT appear here.
        self.assertNotIn(100, [r["id"] for r in data["downloading"]])
        self.assertEqual(row["last_outcome"], "rejected")
        self.assertEqual(row["last_username"], "peer")

    def test_pipeline_downloading_includes_active_youtube_ingest(self):
        self.db.seed_request(make_request_row(
            id=202, status="wanted", artist_name="YT Artist",
            album_title="YT Album", mb_release_id="yt-mbid",
        ))
        log_id = self.db.insert_youtube_running(
            request_id=202,
            browse_id="yt-browse",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=abc",
            expected_track_count=2,
        )

        status, data = self._get("/api/pipeline/downloading")

        self.assertEqual(status, 200)
        self.assertEqual(data["downloading"], [])
        self.assertEqual(len(data["youtube_ingest"]), 1)
        rescue = data["youtube_ingest"][0]
        self.assertEqual(rescue["download_log_id"], log_id)
        self.assertEqual(rescue["album_title"], "YT Album")
        self.assertEqual(rescue["youtube_metadata"]["browse_id"], "yt-browse")
        self.assertEqual(rescue["request_status"], "wanted")

    def test_pipeline_downloading_caps_youtube_ingest_at_50(self):
        """The route hardcodes limit=50 on list_active_youtube_rescues —
        one running rescue per request (partial unique index), so 51
        requests with running ingests page down to 50."""
        for i in range(51):
            rid = 300 + i
            self.db.seed_request(make_request_row(
                id=rid, status="wanted", mb_release_id=f"yt-{rid}"))
            self.db.insert_youtube_running(
                request_id=rid, browse_id=f"b{rid}",
                audio_playlist_id=None, yt_url=f"https://yt/{rid}",
                expected_track_count=2,
            )
        status, data = self._get("/api/pipeline/downloading")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["youtube_ingest"]), 50)

    def test_pipeline_detail(self):
        status, data = self._get("/api/pipeline/100")
        self.assertEqual(status, 200)
        self.assertIn("request", data)
        self.assertIn("history", data)
        self.assertIn("tracks", data)
        # History items should have verdict
        if data["history"]:
            self.assertIn("verdict", data["history"][0])
            self.assertIn("downloaded_label", data["history"][0])

    def test_pipeline_detail_not_found(self):
        status, data = self._get("/api/pipeline/999")
        self.assertEqual(status, 404)

    def test_unknown_get_returns_404(self):
        status, data = self._get("/api/nonexistent")
        self.assertEqual(status, 404)

    # --- POST endpoints ---

    def test_post_pipeline_add_missing_mbid(self):
        status, data = self._post("/api/pipeline/add", {})
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        # Pydantic adapter populates ``errors`` with structured field-path
        # entries — the frontend uses ``loc`` + ``msg`` + ``type`` to
        # render real validation messages instead of a single string.
        self.assertIn("errors", data)
        self.assertIsInstance(data["errors"], list)
        self.assertTrue(data["errors"])
        first = data["errors"][0]
        self.assertIn("loc", first)
        self.assertIn("msg", first)
        self.assertIn("type", first)

    def test_post_pipeline_delete_missing_id(self):
        status, data = self._post("/api/pipeline/delete", {})
        self.assertEqual(status, 400)

    def test_post_set_intent_success(self):
        """POST /api/pipeline/set-intent returns ok with required fields."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "lossless"})
        self.assertEqual(status, 200)
        for key in ("status", "id", "intent", "target_format", "requeued"):
            self.assertIn(key, data, f"Missing key '{key}' in set-intent response")
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["intent"], "lossless")

    def test_post_set_intent_backward_compat(self):
        """Old 'flac_only' intent is aliased to 'lossless'."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "flac_only"})
        self.assertEqual(status, 200)
        self.assertEqual(data["intent"], "lossless")

    @patch("web.routes.pipeline_mutations.resolve_failed_path", return_value="/tmp/Test Album")
    def test_post_force_import_passes_source_username(self, _mock_resolve):
        from lib.import_queue import (
            ForceImportPayload,
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
        )

        log_id = self.db.log_download(
            100, outcome="rejected", soulseek_username="baduser",
            validation_result={
                "failed_path": "/tmp/Test Album",
                "scenario": "high_distance",
                "source_dirs": ["baduser\\Artist\\Album"],
            },
        )

        status, data = self._post(
            "/api/pipeline/force-import", {"download_log_id": log_id})

        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "queued")
        req = self.db.request(100)
        self.assertEqual(data["artist"], req["artist_name"])
        self.assertEqual(data["album"], req["album_title"])
        # The job landed in the real import queue with the full payload.
        jobs = self.db.list_import_jobs()
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job.job_type, IMPORT_JOB_FORCE)
        self.assertEqual(job.request_id, 100)
        self.assertEqual(job.dedupe_key, force_import_dedupe_key(log_id))
        assert isinstance(job.payload, ForceImportPayload)
        self.assertEqual(job.payload.failed_path, "/tmp/Test Album")
        self.assertEqual(job.payload.source_username, "baduser")
        self.assertEqual(job.payload.source_dirs, ["baduser\\Artist\\Album"])

    def test_post_set_intent_default_clears_stale_lossless_override(self):
        self.db.seed_request(make_request_row(
            id=100, status="wanted", artist_name="Test Artist",
            album_title="Test Album", target_format="lossless",
            search_filetype_override="lossless",
        ))
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "default"})
        self.assertEqual(status, 200)
        self.assertFalse(data["requeued"])
        # Both stale fields cleared on the row itself.
        row = self.db.request(100)
        self.assertIsNone(row["target_format"])
        self.assertIsNone(row["search_filetype_override"])
        self.assertEqual(
            self.db.update_request_fields_calls,
            [(100, {"target_format": None,
                    "search_filetype_override": None})])

    def test_post_set_intent_invalid(self):
        """POST /api/pipeline/set-intent with bad intent returns 400."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "garbage"})
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_post_set_intent_missing_id(self):
        """POST /api/pipeline/set-intent without id returns 400."""
        status, data = self._post("/api/pipeline/set-intent",
                                  {"intent": "lossless"})
        self.assertEqual(status, 400)

    def test_unknown_post_returns_404(self):
        status, data = self._post("/api/nonexistent", {})
        self.assertEqual(status, 404)

    # --- datetime serialization ---

    def test_log_entries_have_string_dates(self):
        """Datetime fields should be serialized to strings, not objects."""
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        if data["log"]:
            created = data["log"][0].get("created_at")
            self.assertIsInstance(created, str)
            # ISO-8601 round-trip — no fixed-year assertion (the seeded
            # row carries the fake's "now", which outlives 2026).
            from datetime import datetime as _dt
            _dt.fromisoformat(created)


    def test_disambiguate_endpoint(self):
        """Disambiguate endpoint returns releases with unique track info."""
        fake_releases = [
            {
                "id": "rel-1",
                "title": "Album",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-1",
                    "title": "Album",
                    "primary-type": "Album",
                    "secondary-types": [],
                },
                "media": [{
                    "position": 1,
                    "format": "CD",
                    "track-count": 2,
                    "tracks": [
                        {"position": 1, "number": "1", "title": "Track A",
                         "recording": {"id": "rec-1", "title": "Track A"}},
                        {"position": 2, "number": "2", "title": "Track B",
                         "recording": {"id": "rec-2", "title": "Track B"}},
                    ],
                }],
            },
            {
                "id": "rel-2",
                "title": "Single",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-2",
                    "title": "Single",
                    "primary-type": "Single",
                    "secondary-types": [],
                },
                "media": [{
                    "position": 1,
                    "format": "CD",
                    "track-count": 2,
                    "tracks": [
                        {"position": 1, "number": "1", "title": "Track A",
                         "recording": {"id": "rec-1", "title": "Track A"}},
                        {"position": 2, "number": "2", "title": "B-side",
                         "recording": {"id": "rec-3", "title": "B-side"}},
                    ],
                }],
            },
        ]
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_artist_releases_with_recordings.return_value = fake_releases
            mock_mb.get_artist_name.return_value = "Test Artist"
            status, data = self._get("/api/artist/664c3e0e-42d8-48c1-b209-1efca19c0325/disambiguate")

        self.assertEqual(status, 200)
        self.assertEqual(data["artist_name"], "Test Artist")
        rgs = data["release_groups"]
        self.assertEqual(len(rgs), 2)

        # Album (tier 1) has 2 unique, Single's Track A is covered by Album
        album_rg = [rg for rg in rgs if rg["release_group_id"] == "rg-1"][0]
        single_rg = [rg for rg in rgs if rg["release_group_id"] == "rg-2"][0]
        self.assertEqual(album_rg["unique_track_count"], 2)
        self.assertEqual(single_rg["unique_track_count"], 1)

        # B-side is unique, Track A on single is covered by album
        bside = [t for t in single_rg["tracks"] if t["title"] == "B-side"][0]
        self.assertTrue(bside["unique"])
        track_a = [t for t in single_rg["tracks"] if t["title"] == "Track A"][0]
        self.assertFalse(track_a["unique"])

        # Pressings should be present with recording_ids
        self.assertEqual(len(album_rg["pressings"]), 1)
        self.assertEqual(album_rg["pressings"][0]["release_id"], "rel-1")
        self.assertIn("rec-1", album_rg["pressings"][0]["recording_ids"])

    def test_disambiguate_filters_live(self):
        """Disambiguate endpoint filters out live releases."""
        fake_releases = [
            {
                "id": "rel-1",
                "title": "Studio",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-1",
                    "title": "Studio",
                    "primary-type": "Album",
                    "secondary-types": [],
                },
                "media": [{"position": 1, "format": "CD", "track-count": 1,
                           "tracks": [{"position": 1, "number": "1", "title": "Song",
                                       "recording": {"id": "rec-1", "title": "Song"}}]}],
            },
            {
                "id": "rel-2",
                "title": "Live Album",
                "date": "2020",
                "status": "Official",
                "release-group": {
                    "id": "rg-2",
                    "title": "Live",
                    "primary-type": "Album",
                    "secondary-types": ["Live"],
                },
                "media": [{"position": 1, "format": "CD", "track-count": 1,
                           "tracks": [{"position": 1, "number": "1", "title": "Song Live",
                                       "recording": {"id": "rec-2", "title": "Song Live"}}]}],
            },
        ]
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_artist_releases_with_recordings.return_value = fake_releases
            mock_mb.get_artist_name.return_value = "Test Artist"
            status, data = self._get("/api/artist/664c3e0e-42d8-48c1-b209-1efca19c0325/disambiguate")

        self.assertEqual(status, 200)
        self.assertEqual(len(data["release_groups"]), 1)
        self.assertEqual(data["release_groups"][0]["title"], "Studio")


class TestFuzzyShimRemoved(unittest.TestCase):
    """Issue #123: ``web.server.check_beets_by_artist_album`` deleted.

    Guard against accidental reintroduction — the shim was the only
    path from the web layer into the fuzzy fallback, so deleting it
    completes the closure.
    """

    def test_check_beets_by_artist_album_no_longer_exposed(self) -> None:
        from web import server
        self.assertFalse(
            hasattr(server, "check_beets_by_artist_album"),
            "check_beets_by_artist_album was deleted in issue #123 "
            "— the fuzzy artist+album shim must not return.",
        )


class _RaisingUpdateFieldsDB(FakePipelineDB):
    """update_request_fields raises ``update_error`` when set —
    deterministic stand-in for a client disconnect (or DB error)
    surfacing mid-handler."""

    def __init__(self) -> None:
        super().__init__()
        self.update_error: Exception | None = None

    def update_request_fields(self, request_id: int, **fields: object) -> bool:
        if self.update_error is not None:
            raise self.update_error
        return super().update_request_fields(request_id, **fields)


class TestClientDisconnectHandling(_FakeDbWebServerCase):
    """Issue #233 wedge regression: client closes mid-response.

    Before this fix, a BrokenPipeError raised inside do_GET/do_POST
    (typically from _json's wfile.write to a closed socket) reached
    the bare ``except Exception`` catch-all, which unconditionally
    called _try_reconnect_db() and tried to write a 500 body back to
    the dead socket — causing a second BrokenPipeError, a 30-line
    chained traceback in journald, and a costly DB reconnect.
    Sustained client-disconnect traffic (cratedigger.py's end-of-cycle
    POST that closed mid-body, see U1) compounded reconnects until the
    single-threaded server wedged in poll().

    U2's fix: a typed ``except (BrokenPipeError, ConnectionResetError,
    ConnectionAbortedError)`` clause sits before the existing catch-all
    in both do_GET and do_POST. Disconnect errors get a single WARNING
    log line and a clean return — no DB reconnect, no second body-write.

    These tests inject the exception via a typed FakePipelineDB
    subclass whose ``update_request_fields`` raises on demand, rather
    than via raw-socket mid-body close. The mechanism is the same code
    path (typed except clause inside do_POST's try block); the
    injection approach is deterministic where raw-socket timing is
    flaky on localhost. R3 regression-guard tests confirm the existing
    catch-all still fires for real handler errors.
    """

    DB_FACTORY = _RaisingUpdateFieldsDB

    @property
    def raising_db(self) -> _RaisingUpdateFieldsDB:
        assert isinstance(self.db, _RaisingUpdateFieldsDB)
        return self.db

    def setUp(self):
        super().setUp()
        # set-intent "default" on a wanted row reaches the
        # update_request_fields write — the injection point.
        self.db.seed_request(make_request_row(
            id=100, status="wanted", target_format="lossless",
            search_filetype_override="lossless",
        ))

    def _post_may_disconnect(self, path, body):
        """Like ``_post`` but tolerates connection-level failures —
        exactly what the wedge produces server-side. Tests assert on
        server-side observable state, not on the client response."""
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=2) as resp:
                return resp.status, json.loads(resp.read())
        except HTTPError as e:
            with e:
                return e.code, json.loads(e.read())
        except Exception:
            return None, None

    def _assert_no_reconnect_no_traceback(self, mock_reconnect, log_records, kind):
        """Shared assertions for the disconnect-class tests."""
        # The narrowed clause caught the disconnect — no reconnect attempted.
        self.assertEqual(
            mock_reconnect.call_count, 0,
            f"{kind} must not trigger _try_reconnect_db",
        )
        # A single WARNING-level record about the disconnect.
        warnings = [r for r in log_records if r.levelname == "WARNING"]
        self.assertGreaterEqual(
            len(warnings), 1,
            f"Expected at least one WARNING log line for {kind}, "
            f"got records: {[(r.levelname, r.getMessage()) for r in log_records]}",
        )
        self.assertTrue(
            any("Client disconnect" in r.getMessage() for r in warnings),
            f"Expected 'Client disconnect' in WARNING for {kind}, got: "
            f"{[r.getMessage() for r in warnings]}",
        )
        # No log.exception (ERROR with exc_info) — that's the 30-line
        # traceback pattern the wedge produced.
        errors_with_exc = [
            r for r in log_records
            if r.levelname == "ERROR" and r.exc_info is not None
        ]
        self.assertEqual(
            len(errors_with_exc), 0,
            f"Disconnect ({kind}) must not emit a traceback "
            f"(would be the journald-flood signature). Found: "
            f"{[r.getMessage() for r in errors_with_exc]}",
        )

    @patch("web.server._try_reconnect_db")
    def test_brokenpipe_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Wedge regression: BrokenPipeError reaches the typed except
        clause first, never the catch-all that reconnects."""
        self.raising_db.update_error = BrokenPipeError(32, "Broken pipe")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post_may_disconnect("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "BrokenPipeError")

    @patch("web.server._try_reconnect_db")
    def test_connection_reset_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Sibling disconnect class — same handling expected."""
        self.raising_db.update_error = ConnectionResetError(104, "Connection reset by peer")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post_may_disconnect("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "ConnectionResetError")

    @patch("web.server._try_reconnect_db")
    def test_connection_aborted_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Sibling disconnect class — same handling expected."""
        self.raising_db.update_error = ConnectionAbortedError(103, "Software caused connection abort")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post_may_disconnect("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "ConnectionAbortedError")

    @patch("web.server._try_reconnect_db")
    def test_real_db_error_still_triggers_reconnect(self, mock_reconnect):
        """R3 regression guard: psycopg2.OperationalError must still hit
        the catch-all and trigger _try_reconnect_db. The narrowing must
        not change behaviour for real DB errors."""
        import psycopg2
        self.raising_db.update_error = psycopg2.OperationalError("simulated PG outage")
        with self.assertLogs("cratedigger-web", level="ERROR") as cm:
            status, _ = self._post_may_disconnect("/api/pipeline/set-intent",
                                   {"id": 100, "intent": "default"})
        # Real DB error → catch-all fires → reconnect attempted.
        self.assertEqual(
            mock_reconnect.call_count, 1,
            "Real psycopg2.OperationalError must still trigger _try_reconnect_db",
        )
        # log.exception emits ERROR with exc_info — that's the existing behaviour.
        errors_with_exc = [
            r for r in cm.records
            if r.levelname == "ERROR" and r.exc_info is not None
        ]
        self.assertGreaterEqual(
            len(errors_with_exc), 1,
            "Real DB error must produce a log.exception traceback for diagnosis",
        )
        # Server returns 500.
        self.assertEqual(status, 500)

    @patch("web.server._try_reconnect_db")
    def test_other_exception_in_handler_still_triggers_reconnect(self, mock_reconnect):
        """Unchanged-behaviour regression guard: a real handler bug
        (e.g. ValueError) still hits the catch-all. Narrowing this
        further (so non-DB exceptions skip the reconnect) is explicitly
        out of scope per the plan — see follow-up #234."""
        self.raising_db.update_error = ValueError("simulated handler bug")
        with self.assertLogs("cratedigger-web", level="ERROR") as cm:
            status, _ = self._post_may_disconnect("/api/pipeline/set-intent",
                                   {"id": 100, "intent": "default"})
        self.assertEqual(
            mock_reconnect.call_count, 1,
            "Existing broad catch-all behaviour preserved: any non-disconnect "
            "exception still triggers _try_reconnect_db. Narrowing is deferred to #234.",
        )
        errors_with_exc = [
            r for r in cm.records
            if r.levelname == "ERROR" and r.exc_info is not None
        ]
        self.assertGreaterEqual(len(errors_with_exc), 1)
        self.assertEqual(status, 500)

    @patch("web.server._try_reconnect_db")
    def test_normal_post_no_reconnect_no_warning(self, mock_reconnect):
        """Happy path regression guard: a successful POST does not
        trigger any reconnect or disconnect-warning side effects."""
        # No update_error set — the fake's real write path runs.
        # Use assertNoLogs (Python 3.10+) to assert no WARNING/ERROR records.
        # Some logger setups still emit INFO; we only care that no WARNING
        # for "Client disconnect" appears and no ERROR is emitted.
        with self.assertLogs("cratedigger-web", level="DEBUG") as cm:
            # assertLogs requires at least one record; a trivial DEBUG log
            # ensures the context manager is satisfied even on a quiet path.
            logging.getLogger("cratedigger-web").debug("test marker: normal POST path")
            status, _ = self._post_may_disconnect("/api/pipeline/set-intent",
                                   {"id": 100, "intent": "default"})
        self.assertEqual(mock_reconnect.call_count, 0)
        disconnect_warnings = [
            r for r in cm.records
            if r.levelname == "WARNING" and "Client disconnect" in r.getMessage()
        ]
        self.assertEqual(len(disconnect_warnings), 0,
            "Normal POST must not emit a disconnect WARNING")
        errors = [r for r in cm.records if r.levelname == "ERROR"]
        self.assertEqual(len(errors), 0,
            f"Normal POST must not produce ERROR logs, got: "
            f"{[r.getMessage() for r in errors]}")

if __name__ == "__main__":
    unittest.main()
