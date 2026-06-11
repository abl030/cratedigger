#!/usr/bin/env python3
"""Server-level endpoint tests: static routes, error handling, client disconnects.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

from datetime import datetime, timezone
import json
import logging
import os
import sys
import unittest
from unittest.mock import patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _MOCK_PIPELINE_REQUEST, _make_server

from tests.helpers import make_request_row


class TestServerEndpoints(unittest.TestCase):
    """Test HTTP endpoints return expected status and structure."""

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port, cls.mock_db = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _get(self, path: str) -> tuple[int, dict]:
        """GET a path and return (status, json)."""
        url = f"{self.base}{path}"
        try:
            resp = urlopen(url)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    def _post(self, path: str, body: dict) -> tuple[int, dict]:
        """POST JSON and return (status, json)."""
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            resp = urlopen(req)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    # --- GET endpoints ---

    def test_index_returns_html(self):
        resp = urlopen(f"{self.base}/")
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
        status, data = self._get("/api/pipeline/log?outcome=imported")
        self.assertEqual(status, 200)
        self.assertIn("log", data)
        # Verify the DB was called with the filter
        self.mock_db.get_log.assert_called_with(limit=50, outcome_filter="imported")

    def test_pipeline_log_filter_rejected(self):
        status, data = self._get("/api/pipeline/log?outcome=rejected")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=50, outcome_filter="rejected")

    def test_pipeline_log_limit_param(self):
        status, data = self._get("/api/pipeline/log?outcome=rejected&limit=300")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=300, outcome_filter="rejected")

    def test_pipeline_log_limit_param_is_capped(self):
        status, data = self._get("/api/pipeline/log?limit=5000")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=500, outcome_filter=None)

    def test_pipeline_log_filter_invalid_ignored(self):
        status, data = self._get("/api/pipeline/log?outcome=badvalue")
        self.assertEqual(status, 200)
        self.mock_db.get_log.assert_called_with(limit=50, outcome_filter=None)

    def test_pipeline_log_counts_structure(self):
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        counts = data["counts"]
        for key in ("all", "imported", "rejected", "matches_24h", "matches_6h"):
            self.assertIn(key, counts)
            self.assertIsInstance(counts[key], int)
        for key in ("matches_per_hour_24h", "matches_per_hour_6h"):
            self.assertIn(key, counts)
            self.assertIsInstance(counts[key], (int, float))

    def test_pipeline_status(self):
        status, data = self._get("/api/pipeline/status")
        self.assertEqual(status, 200)
        self.assertIn("counts", data)
        self.assertIn("wanted", data)

    def test_pipeline_all(self):
        status, data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertIn("counts", data)
        for key in ("wanted", "downloading", "imported", "manual"):
            self.assertIn(key, data)

    def test_pipeline_status_includes_downloading(self):
        """count_by_status includes downloading when albums are downloading."""
        self.mock_db.count_by_status.return_value = {
            "wanted": 3, "downloading": 2, "imported": 10, "manual": 1}
        status, data = self._get("/api/pipeline/status")
        self.assertEqual(status, 200)
        self.assertEqual(data["counts"]["downloading"], 2)
        # Restore
        self.mock_db.count_by_status.return_value = {"wanted": 0, "imported": 1, "manual": 0}

    def test_pipeline_all_includes_downloading(self):
        """get_pipeline_all returns downloading albums in the response."""
        downloading_row = make_request_row(
            id=200, album_title="Downloading Album", artist_name="DL Artist",
            mb_release_id="dl-uuid", status="downloading",
            active_download_state={"filetype": "flac", "enqueued_at": "now", "files": []},
        )
        self.mock_db.get_by_status.side_effect = lambda s: [downloading_row] if s == "downloading" else []
        self.mock_db.count_by_status.return_value = {"downloading": 1}
        self.mock_db.get_download_history_batch.return_value = {}
        status, data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertIn("downloading", data)
        self.assertEqual(len(data["downloading"]), 1)
        self.assertEqual(data["downloading"][0]["album_title"], "Downloading Album")
        # Restore
        self.mock_db.get_by_status.side_effect = None
        self.mock_db.get_by_status.return_value = []
        self.mock_db.count_by_status.return_value = {"wanted": 0, "imported": 1, "manual": 0}

    def test_pipeline_downloading_returns_current_downloads_only(self):
        downloading_row = make_request_row(
            id=201, album_title="Active Download", artist_name="DL Artist",
            mb_release_id="dl-uuid", status="downloading",
            active_download_state={
                "filetype": "mp3 320",
                "enqueued_at": "2026-05-05T12:00:00+00:00",
                "files": [{"username": "peer", "bytes_transferred": 1, "size": 2}],
            },
        )
        self.mock_db.get_by_status.side_effect = (
            lambda s: [downloading_row] if s == "downloading" else []
        )
        self.mock_db.count_by_status.return_value = {"downloading": 1}
        self.mock_db.get_download_history_batch.reset_mock()
        self.mock_db.get_download_history_batch.return_value = {}

        status, data = self._get("/api/pipeline/downloading")

        self.assertEqual(status, 200)
        self.assertEqual(data["counts"]["downloading"], 1)
        self.assertEqual(len(data["downloading"]), 1)
        self.assertEqual(data["downloading"][0]["album_title"], "Active Download")
        self.mock_db.get_by_status.assert_called_with("downloading")
        self.mock_db.get_download_history_batch.assert_any_call([201])

        self.mock_db.get_by_status.side_effect = None
        self.mock_db.get_by_status.return_value = []
        self.mock_db.get_download_history_batch.reset_mock()
        self.mock_db.count_by_status.return_value = {
            "wanted": 0, "imported": 1, "manual": 0}

    def test_pipeline_downloading_includes_active_youtube_ingest(self):
        self.mock_db.get_by_status.side_effect = None
        self.mock_db.get_by_status.return_value = []
        self.mock_db.count_by_status.return_value = {"downloading": 0}
        self.mock_db.get_download_history_batch.return_value = {}
        self.mock_db.list_active_youtube_rescues.return_value = [{
            "download_log_id": 301,
            "request_id": 202,
            "source": "youtube",
            "outcome": "youtube_running",
            "youtube_metadata": {
                "browse_id": "yt-browse",
                "expected_track_count": 2,
                "yt_url": "https://music.youtube.com/playlist?list=abc",
            },
            "created_at": datetime(2026, 5, 28, tzinfo=timezone.utc),
            "artist_name": "YT Artist",
            "album_title": "YT Album",
            "mb_release_id": "yt-mbid",
            "request_status": "wanted",
        }]

        status, data = self._get("/api/pipeline/downloading")

        self.assertEqual(status, 200)
        self.assertEqual(data["downloading"], [])
        self.assertEqual(len(data["youtube_ingest"]), 1)
        rescue = data["youtube_ingest"][0]
        self.assertEqual(rescue["download_log_id"], 301)
        self.assertEqual(rescue["album_title"], "YT Album")
        self.assertEqual(rescue["youtube_metadata"]["browse_id"], "yt-browse")
        self.assertTrue(rescue["created_at"].startswith("2026-05-28T00:00:00"))
        self.mock_db.list_active_youtube_rescues.assert_called_with(limit=50)

        self.mock_db.get_by_status.return_value = []
        self.mock_db.list_active_youtube_rescues.return_value = []
        self.mock_db.get_download_history_batch.reset_mock()
        self.mock_db.count_by_status.return_value = {
            "wanted": 0, "imported": 1, "manual": 0}

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
        self.mock_db.get_request.return_value = None
        status, data = self._get("/api/pipeline/999")
        self.assertEqual(status, 404)
        # Restore
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST

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

    @patch("web.routes.pipeline.resolve_failed_path", return_value="/tmp/Test Album")
    def test_post_force_import_passes_source_username(self, _mock_resolve):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_dedupe_key

        self.mock_db.get_download_log_entry.return_value = {
            "id": 42,
            "request_id": 100,
            "soulseek_username": "baduser",
            "validation_result": {
                "failed_path": "/tmp/Test Album",
                "scenario": "high_distance",
                "source_dirs": ["baduser\\Artist\\Album"],
            },
        }

        status, data = self._post("/api/pipeline/force-import", {"download_log_id": 42})

        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "queued")
        self.assertEqual(data["artist"], _MOCK_PIPELINE_REQUEST["artist_name"])
        self.assertEqual(data["album"], _MOCK_PIPELINE_REQUEST["album_title"])
        self.mock_db.enqueue_import_job.assert_called_once()
        args, kwargs = self.mock_db.enqueue_import_job.call_args
        self.assertEqual(args, (IMPORT_JOB_FORCE,))
        self.assertEqual(kwargs["request_id"], 100)
        self.assertEqual(kwargs["dedupe_key"], force_import_dedupe_key(42))
        self.assertEqual(kwargs["payload"]["failed_path"], "/tmp/Test Album")
        self.assertEqual(kwargs["payload"]["source_username"], "baduser")
        self.assertEqual(kwargs["payload"]["source_dirs"], ["baduser\\Artist\\Album"])

    def test_post_set_intent_default_clears_stale_lossless_override(self):
        self.mock_db.get_request.return_value = make_request_row(
            id=100, status="wanted", artist_name="Test Artist",
            album_title="Test Album", target_format="lossless",
            search_filetype_override="lossless",
        )
        self.mock_db.update_request_fields.reset_mock()
        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "default"})
        self.assertEqual(status, 200)
        self.assertFalse(data["requeued"])
        self.mock_db.update_request_fields.assert_called_once_with(
            100, target_format=None, search_filetype_override=None)
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST

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
            self.assertIn("2026", created)


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


class TestClientDisconnectHandling(unittest.TestCase):
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

    These tests inject the exception via ``mock_db.<method>.side_effect``
    rather than via raw-socket mid-body close. The mechanism is the same
    code path (typed except clause inside do_POST's try block); the
    side_effect approach is deterministic where raw-socket timing is
    flaky on localhost. R3 regression-guard tests confirm the existing
    catch-all still fires for real handler errors.
    """

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port, cls.mock_db = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        # Ensure each test starts with a clean side_effect.
        self.mock_db.update_request_fields.side_effect = None
        self.mock_db.update_request_fields.return_value = None

    def _post(self, path, body):
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            resp = urlopen(req, timeout=2)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())
        except Exception:
            # Connection-level failures (which is exactly what the wedge
            # produces server-side) surface client-side as URLError or
            # similar. Tests assert on server-side observable state, not
            # on the client response.
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
        self.mock_db.update_request_fields.side_effect = BrokenPipeError(32, "Broken pipe")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "BrokenPipeError")

    @patch("web.server._try_reconnect_db")
    def test_connection_reset_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Sibling disconnect class — same handling expected."""
        self.mock_db.update_request_fields.side_effect = ConnectionResetError(104, "Connection reset by peer")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "ConnectionResetError")

    @patch("web.server._try_reconnect_db")
    def test_connection_aborted_during_post_does_not_trigger_reconnect(self, mock_reconnect):
        """Sibling disconnect class — same handling expected."""
        self.mock_db.update_request_fields.side_effect = ConnectionAbortedError(103, "Software caused connection abort")
        with self.assertLogs("cratedigger-web", level="WARNING") as cm:
            self._post("/api/pipeline/set-intent",
                       {"id": 100, "intent": "default"})
        self._assert_no_reconnect_no_traceback(mock_reconnect, cm.records, "ConnectionAbortedError")

    @patch("web.server._try_reconnect_db")
    def test_real_db_error_still_triggers_reconnect(self, mock_reconnect):
        """R3 regression guard: psycopg2.OperationalError must still hit
        the catch-all and trigger _try_reconnect_db. The narrowing must
        not change behaviour for real DB errors."""
        import psycopg2
        self.mock_db.update_request_fields.side_effect = psycopg2.OperationalError("simulated PG outage")
        with self.assertLogs("cratedigger-web", level="ERROR") as cm:
            status, _ = self._post("/api/pipeline/set-intent",
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
        self.mock_db.update_request_fields.side_effect = ValueError("simulated handler bug")
        with self.assertLogs("cratedigger-web", level="ERROR") as cm:
            status, _ = self._post("/api/pipeline/set-intent",
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
        # mock_db.set_intent_for_request returns True by default per setUp.
        # Use assertNoLogs (Python 3.10+) to assert no WARNING/ERROR records.
        # Some logger setups still emit INFO; we only care that no WARNING
        # for "Client disconnect" appears and no ERROR is emitted.
        with self.assertLogs("cratedigger-web", level="DEBUG") as cm:
            # assertLogs requires at least one record; a trivial DEBUG log
            # ensures the context manager is satisfied even on a quiet path.
            logging.getLogger("cratedigger-web").debug("test marker: normal POST path")
            status, _ = self._post("/api/pipeline/set-intent",
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
