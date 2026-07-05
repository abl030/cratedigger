"""Contract tests for the in-repo typed slskd client (issue #146).

Fixtures below are captured from the live slskd 0.24.5 instance on doc2
(2026-07-02): the events envelope carries `data` as a JSON *string* that
must be decoded a second time, events are returned newest-first, and the
`X-Total-Count` header carries the retained-event total.

The tests run against a real in-process HTTP server so the request path,
query params, JSON bodies, auth header, and error semantics are all
exercised over a real socket — no mocked session objects.
"""

from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import cast
from urllib.parse import parse_qs, urlparse

import msgspec
import requests

from lib.slskd_client import (
    SlskdClient,
    SlskdDownloadDirectoryCompleteEvent,
    SlskdDownloadFileCompleteEvent,
    SlskdRawEvent,
    TransferSnapshot,
    decode_download_directory_complete,
    decode_download_file_complete,
    parse_transfer_snapshot,
)


EVENT_FILE_COMPLETE_DATA = {
    "version": 0,
    "localFilename": "/mnt/virtio/music/slskd/Dungeon Vision/1-03 Flashlight.flac",
    "remoteFilename": "@@illfv\\Music\\Earth Tongue\\Dungeon Vision\\1-03 Flashlight.flac",
    "transfer": {
        "id": "977c6c28-39e0-4cb1-a57b-fbfa07f027ae",
        "username": "neuro31",
        "direction": "Download",
        "filename": "@@illfv\\Music\\Earth Tongue\\Dungeon Vision\\1-03 Flashlight.flac",
        "size": 30095492,
        "startOffset": 0,
        "state": "Completed, Succeeded",
        "requestedAt": "2026-07-01T22:55:28.116046",
        "enqueuedAt": "2026-07-01T22:55:29.1111336Z",
        "startedAt": "2026-07-01T22:57:40.7358334Z",
        "endedAt": "2026-07-01T23:00:10.7447018Z",
        "bytesTransferred": 30095492,
        "averageSpeed": 200624.75186300382,
        "placeInQueue": None,
        "exception": None,
        "bytesRemaining": 0,
        "percentComplete": 100,
    },
}

EVENT_DIR_COMPLETE_DATA = {
    "version": 0,
    "localDirectoryName": "/mnt/virtio/music/slskd/The Twilight Sad - Acoustic EP",
    "remoteDirectoryName": "@@gjuwn\\Alphabetti Spagehetti 5\\The Twilight Sad\\The Twilight Sad - Acoustic EP",
    "username": "Triple Sun",
}

EVENTS_FIXTURE = [
    {
        "timestamp": "2026-07-01T23:00:10.7447018Z",
        "type": "DownloadFileComplete",
        "data": json.dumps(EVENT_FILE_COMPLETE_DATA),
        "id": "11da6649-4ffc-4d72-afc0-b4238afcc4ec",
    },
    {
        "timestamp": "2026-07-01T22:50:12.4899393Z",
        "type": "DownloadDirectoryComplete",
        "data": json.dumps(EVENT_DIR_COMPLETE_DATA),
        "id": "b9dce74f-c790-4768-8a6f-14897df4121e",
    },
]

DOWNLOADS_FIXTURE = [
    {
        "username": "FourTwenty",
        "directories": [
            {
                "directory": "Music\\Rock\\Led Zeppelin\\1969 - Led Zeppelin II",
                "fileCount": 1,
                "files": [
                    {
                        "id": "1839fe97-46dd-4351-9ada-4422a57f9f7a",
                        "username": "FourTwenty",
                        "direction": "Download",
                        "filename": "Music\\Rock\\Led Zeppelin\\1969 - Led Zeppelin II\\06 - Living Loving Maid.flac",
                        "size": 113325058,
                        "state": "Queued, Remotely",
                        "bytesTransferred": 0,
                        "percentComplete": 0,
                    }
                ],
            }
        ],
    }
]

SEARCH_STATE_FIXTURE = {
    "id": "ba10680e-2f65-11f1-8e15-bc24117f4304",
    "isComplete": True,
    "state": "Completed, TimedOut",
    "responseCount": 4,
    "fileCount": 22,
    "searchText": "*eta *adio Ancient Transition",
}

SEARCH_RESPONSES_FIXTURE = [
    {
        "username": "peer1",
        "uploadSpeed": 100,
        "files": [{"filename": "a\\b.flac", "size": 123, "bitRate": None}],
    }
]

USER_STATUS_FIXTURE = {"presence": "Online", "isPrivileged": False}

USER_DIRECTORY_FIXTURE = [
    {
        "directory": "music\\Artist\\Album",
        "files": [{"filename": "01 track.flac", "size": 999}],
    }
]


class _RecordingHandler(BaseHTTPRequestHandler):
    """Serves canned fixtures keyed on (method, path) and records requests."""

    def _respond(self) -> None:
        server = cast("_FixtureServer", self.server)
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else None
        record = {
            "method": self.command,
            "path": parsed.path,
            "query": parse_qs(parsed.query),
            "body": body,
            "api_key": self.headers.get("X-API-Key"),
        }
        server.requests.append(record)

        key = (self.command, parsed.path)
        status, headers, payload = server.fixtures.get(
            key, (404, {}, {"error": "no fixture"}))
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    do_GET = _respond
    do_POST = _respond
    do_PUT = _respond
    do_DELETE = _respond

    def log_message(self, format: str, *args: object) -> None:
        pass


class _FixtureServer(HTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _RecordingHandler)
        self.requests: list[dict] = []
        self.fixtures: dict[tuple[str, str], tuple[int, dict, object]] = {}


class SlskdClientTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _FixtureServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        host = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.client = SlskdClient(host=host, api_key="test-key", pool_size=1)

    def set_fixture(self, method: str, path: str, payload: object,
                    *, status: int = 200, headers: dict | None = None) -> None:
        self.server.fixtures[(method, f"/api/v0{path}")] = (
            status, headers or {}, payload)

    def last_request(self) -> dict:
        return self.server.requests[-1]


class TestEventsEndpoint(SlskdClientTestCase):
    def test_list_decodes_envelope_and_total_count(self):
        self.set_fixture("GET", "/events", EVENTS_FIXTURE,
                         headers={"X-Total-Count": "389110"})

        page = self.client.events.list(limit=5, offset=0)

        self.assertEqual(page.total_count, 389110)
        self.assertEqual(len(page.events), 2)
        self.assertIsInstance(page.events[0], SlskdRawEvent)
        self.assertEqual(page.events[0].type, "DownloadFileComplete")
        self.assertEqual(page.events[0].id, "11da6649-4ffc-4d72-afc0-b4238afcc4ec")
        req = self.last_request()
        self.assertEqual(req["query"], {"limit": ["5"], "offset": ["0"]})
        self.assertEqual(req["api_key"], "test-key")

    def test_download_file_complete_payload_double_decodes(self):
        self.set_fixture("GET", "/events", EVENTS_FIXTURE,
                         headers={"X-Total-Count": "2"})
        page = self.client.events.list()

        payload = decode_download_file_complete(page.events[0])

        self.assertIsInstance(payload, SlskdDownloadFileCompleteEvent)
        self.assertEqual(
            payload.local_filename,
            "/mnt/virtio/music/slskd/Dungeon Vision/1-03 Flashlight.flac")
        self.assertEqual(payload.transfer.username, "neuro31")
        self.assertEqual(payload.transfer.size, 30095492)
        self.assertEqual(
            payload.transfer.filename,
            "@@illfv\\Music\\Earth Tongue\\Dungeon Vision\\1-03 Flashlight.flac")

    def test_download_directory_complete_payload_double_decodes(self):
        self.set_fixture("GET", "/events", EVENTS_FIXTURE,
                         headers={"X-Total-Count": "2"})
        page = self.client.events.list()

        payload = decode_download_directory_complete(page.events[1])

        self.assertIsInstance(payload, SlskdDownloadDirectoryCompleteEvent)
        self.assertEqual(
            payload.local_directory_name,
            "/mnt/virtio/music/slskd/The Twilight Sad - Acoustic EP")
        self.assertEqual(payload.username, "Triple Sun")

    def test_malformed_envelope_row_is_skipped_not_fatal(self):
        # One poison event (data: null) must not brick the whole page —
        # a page-level strict decode would wedge the ingest cursor
        # behind it forever.
        malformed = {
            "timestamp": "2026-07-01T23:59:00.0000000Z",
            "type": "SomeFutureEvent",
            "data": None,
            "id": "bad-row",
        }
        self.set_fixture("GET", "/events", [malformed, *EVENTS_FIXTURE],
                         headers={"X-Total-Count": "3"})

        page = self.client.events.list()

        self.assertEqual(
            [e.id for e in page.events],
            [e["id"] for e in EVENTS_FIXTURE])

    def test_missing_total_count_header_reports_none(self):
        # Callers must treat an absent X-Total-Count as "unknown", not 0 —
        # a 0 default would silently truncate multi-page scans.
        self.set_fixture("GET", "/events", EVENTS_FIXTURE)

        page = self.client.events.list()

        self.assertIsNone(page.total_count)

    def test_wire_type_drift_raises_validation_error(self):
        # RED-boundary guard per code-quality rules: an int-typed field
        # arriving as a string must fail loudly at the decode site.
        bad = dict(EVENT_FILE_COMPLETE_DATA)
        bad_transfer = dict(bad["transfer"])
        bad_transfer["size"] = "30095492"
        bad["transfer"] = bad_transfer
        event = SlskdRawEvent(
            id="x", timestamp="2026-07-01T00:00:00Z",
            type="DownloadFileComplete", data=json.dumps(bad))

        with self.assertRaises(msgspec.ValidationError):
            decode_download_file_complete(event)


class TestTransfersEndpoints(SlskdClientTestCase):
    def test_enqueue_posts_files_and_returns_true(self):
        self.set_fixture("POST", "/transfers/downloads/some%20user", {}, status=201)
        files = [{"filename": "a\\b.flac", "size": 1}]

        self.assertTrue(self.client.transfers.enqueue(
            username="some user", files=files))
        req = self.last_request()
        self.assertEqual(req["method"], "POST")
        self.assertEqual(req["body"], files)

    def test_enqueue_error_preserves_response_body_for_offline_detection(self):
        self.set_fixture(
            "POST", "/transfers/downloads/peer",
            {"detail": "User peer appears to be offline"}, status=500)

        with self.assertRaises(requests.HTTPError) as caught:
            self.client.transfers.enqueue(username="peer", files=[])
        response = caught.exception.response
        assert response is not None
        self.assertIn("appears to be offline", response.text)

    def test_error_responses_do_not_leak_pool_slots(self):
        # pool_size=1 with pool_block=True: if error bodies were not
        # consumed, the second call would block forever on the pool.
        self.set_fixture("GET", "/transfers/downloads/", {}, status=500)
        for _ in range(3):
            with self.assertRaises(requests.HTTPError):
                self.client.transfers.get_all_downloads()

    def test_get_all_downloads_passes_include_removed(self):
        self.set_fixture("GET", "/transfers/downloads/", DOWNLOADS_FIXTURE)

        downloads = self.client.transfers.get_all_downloads(includeRemoved=True)

        self.assertEqual(downloads, DOWNLOADS_FIXTURE)
        self.assertEqual(
            self.last_request()["query"], {"includeRemoved": ["True"]})

    def test_cancel_download_deletes_with_remove_param(self):
        self.set_fixture("DELETE", "/transfers/downloads/peer/abc", {}, status=204)

        self.assertTrue(self.client.transfers.cancel_download(
            username="peer", id="abc"))
        req = self.last_request()
        self.assertEqual(req["method"], "DELETE")
        self.assertEqual(req["query"], {"remove": ["False"]})

    def test_remove_completed_downloads(self):
        self.set_fixture("DELETE", "/transfers/downloads/all/completed", {}, status=204)

        self.assertTrue(self.client.transfers.remove_completed_downloads())


class TestUsersEndpoints(SlskdClientTestCase):
    def test_status_gets_quoted_username(self):
        self.set_fixture("GET", "/users/DJ%20Raygun/status", USER_STATUS_FIXTURE)

        status = self.client.users.status("DJ Raygun")

        self.assertEqual(status, USER_STATUS_FIXTURE)

    def test_username_with_slash_is_path_safe(self):
        self.set_fixture("GET", "/users/a%2Fb/status", USER_STATUS_FIXTURE)

        status = self.client.users.status("a/b")

        self.assertEqual(status, USER_STATUS_FIXTURE)

    def test_directory_posts_directory_body(self):
        self.set_fixture("POST", "/users/peer/directory", USER_DIRECTORY_FIXTURE)

        result = self.client.users.directory("peer", "music\\Artist\\Album")

        self.assertEqual(result, USER_DIRECTORY_FIXTURE)
        self.assertEqual(
            self.last_request()["body"], {"directory": "music\\Artist\\Album"})


class TestSearchesEndpoints(SlskdClientTestCase):
    def test_search_text_posts_full_payload(self):
        self.set_fixture("POST", "/searches", SEARCH_STATE_FIXTURE)

        search = self.client.searches.search_text(
            searchText="*adiohead Kid A",
            searchTimeout=15000,
            filterResponses=True,
            maximumPeerQueueLength=1000000,
            minimumPeerUploadSpeed=0,
            responseLimit=100,
            fileLimit=50000,
        )

        self.assertEqual(search["id"], SEARCH_STATE_FIXTURE["id"])
        body = self.last_request()["body"]
        self.assertEqual(body["searchText"], "*adiohead Kid A")
        self.assertEqual(body["searchTimeout"], 15000)
        self.assertEqual(body["fileLimit"], 50000)
        self.assertEqual(body["responseLimit"], 100)
        self.assertTrue(body["filterResponses"])
        # slskd requires a client-generated uuid for the search id.
        self.assertIn("id", body)

    def test_state_passes_include_responses(self):
        self.set_fixture("GET", "/searches/abc", SEARCH_STATE_FIXTURE)

        state = self.client.searches.state("abc", False)

        self.assertEqual(state["state"], "Completed, TimedOut")
        self.assertEqual(state["responseCount"], 4)
        self.assertEqual(
            self.last_request()["query"], {"includeResponses": ["False"]})

    def test_search_responses(self):
        self.set_fixture("GET", "/searches/abc/responses", SEARCH_RESPONSES_FIXTURE)

        responses = self.client.searches.search_responses("abc")

        self.assertEqual(responses, SEARCH_RESPONSES_FIXTURE)

    def test_stop_puts_and_delete_deletes(self):
        self.set_fixture("PUT", "/searches/abc", {}, status=200)
        self.set_fixture("DELETE", "/searches/abc", {}, status=204)

        self.assertTrue(self.client.searches.stop("abc"))
        self.assertEqual(self.last_request()["method"], "PUT")
        self.assertTrue(self.client.searches.delete("abc"))
        self.assertEqual(self.last_request()["method"], "DELETE")

    def test_search_submit_http_error_carries_status_code(self):
        # cratedigger._submit_plan_search retries on 429/409 via
        # e.response.status_code — the raised error must be a real
        # requests.HTTPError with the response attached.
        self.set_fixture("POST", "/searches", {}, status=429)

        with self.assertRaises(requests.HTTPError) as caught:
            self.client.searches.search_text(searchText="x")
        response = caught.exception.response
        assert response is not None
        self.assertEqual(response.status_code, 429)


class TestApplicationEndpoint(SlskdClientTestCase):
    def test_version(self):
        self.set_fixture("GET", "/application/version", "0.24.5")

        self.assertEqual(self.client.application.version(), "0.24.5")


class TestPoolSizing(unittest.TestCase):
    def test_pool_size_is_derived_from_concurrency_values(self):
        from dataclasses import replace

        from lib.config import CratediggerConfig
        from lib.slskd_client import (
            SLSKD_HTTP_POOL_ADMIN_SLACK,
            derive_slskd_http_pool_size,
        )

        cfg = replace(
            CratediggerConfig(),
            browse_global_max_workers=32,
            search_max_inflight=4,
            page_size=10,
        )
        self.assertEqual(
            derive_slskd_http_pool_size(cfg),
            32 + 4 + 10 + SLSKD_HTTP_POOL_ADMIN_SLACK,
        )

    def test_cratedigger_factory_builds_client_with_derived_pool(self):
        from dataclasses import replace

        import cratedigger
        from lib.config import CratediggerConfig
        from lib.slskd_client import derive_slskd_http_pool_size

        cfg = replace(
            CratediggerConfig(),
            slskd_host_url="http://slskd.example",
            slskd_api_key="secret",
            slskd_url_base="/base",
        )
        client = cratedigger._create_slskd_client(cfg)

        self.assertIsInstance(client, SlskdClient)
        self.assertEqual(client.api_url, "http://slskd.example/base/api/v0")
        self.assertEqual(client._session.headers["X-API-Key"], "secret")
        adapter = client._session.adapters["http://"]
        self.assertEqual(
            adapter._pool_maxsize,  # type: ignore[attr-defined]
            derive_slskd_http_pool_size(cfg),
        )
        self.assertTrue(adapter._pool_block)  # type: ignore[attr-defined]


class TestClientConstruction(unittest.TestCase):
    def test_pool_adapters_configured_blocking_at_derived_size(self):
        client = SlskdClient(
            host="http://localhost:5030", api_key="k", pool_size=46)

        for prefix in ("http://", "https://"):
            adapter = client._session.adapters[prefix]
            self.assertEqual(adapter._pool_connections, 46)  # type: ignore[attr-defined]
            self.assertEqual(adapter._pool_maxsize, 46)  # type: ignore[attr-defined]
            self.assertTrue(adapter._pool_block)  # type: ignore[attr-defined]

    def test_url_base_is_joined_into_api_url(self):
        client = SlskdClient(
            host="http://localhost:5030", api_key="k", url_base="/base")

        self.assertEqual(client.api_url, "http://localhost:5030/base/api/v0")

    def test_default_url_base_is_root(self):
        client = SlskdClient(host="http://localhost:5030", api_key="k")

        self.assertEqual(client.api_url, "http://localhost:5030/api/v0")


class TestTransferSnapshot(unittest.TestCase):
    """#468: TransferSnapshot is the typed replacement for DownloadFile.status.

    Fields/keys mirror the live-captured DOWNLOADS_FIXTURE entry above.
    """

    def test_decodes_full_fixture_entry(self):
        raw = DOWNLOADS_FIXTURE[0]["directories"][0]["files"][0]

        snap = parse_transfer_snapshot(raw)

        assert snap is not None
        self.assertEqual(snap.id, "1839fe97-46dd-4351-9ada-4422a57f9f7a")
        self.assertEqual(snap.username, "FourTwenty")
        self.assertEqual(
            snap.filename,
            "Music\\Rock\\Led Zeppelin\\1969 - Led Zeppelin II\\06 - Living Loving Maid.flac")
        self.assertEqual(snap.state, "Queued, Remotely")
        self.assertEqual(snap.size, 113325058)
        self.assertEqual(snap.bytes_transferred, 0)
        self.assertEqual(snap.percent_complete, 0)
        # This fixture entry (a freshly-queued transfer) carries no
        # lifecycle timestamps yet — every one must default to None.
        self.assertIsNone(snap.requested_at)
        self.assertIsNone(snap.enqueued_at)
        self.assertIsNone(snap.started_at)
        self.assertIsNone(snap.ended_at)

    def test_unknown_fields_are_ignored(self):
        # slskd's real Transfer DTO carries fields we don't model
        # (direction, averageSpeed, placeInQueue, exception, ...) — these
        # must not trip decoding.
        raw = {
            "id": "t1", "username": "u", "filename": "f.flac",
            "state": "InProgress", "direction": "Download",
            "averageSpeed": 1234.5, "placeInQueue": None, "exception": None,
        }

        snap = parse_transfer_snapshot(raw)

        assert snap is not None
        self.assertEqual(snap.id, "t1")

    def test_missing_fields_default(self):
        # A bare match-lookup style entry (only filename/id) still decodes —
        # every other field defaults rather than raising.
        raw = {"filename": "f.flac", "id": "t1"}

        snap = parse_transfer_snapshot(raw)

        assert snap is not None
        self.assertEqual(snap.state, "")
        self.assertEqual(snap.bytes_transferred, 0)
        self.assertEqual(snap.size, 0)
        self.assertIsNone(snap.ended_at)

    def test_lifecycle_timestamps_round_trip(self):
        raw = {
            "id": "t1", "filename": "f.flac", "state": "Completed, Succeeded",
            "requestedAt": "2026-04-03T20:00:00+00:00",
            "enqueuedAt": "2026-04-03T20:00:01+00:00",
            "startedAt": "2026-04-03T20:00:02+00:00",
            "endedAt": "2026-04-03T21:00:00+00:00",
        }

        snap = parse_transfer_snapshot(raw)

        assert snap is not None
        self.assertEqual(snap.requested_at, "2026-04-03T20:00:00+00:00")
        self.assertEqual(snap.enqueued_at, "2026-04-03T20:00:01+00:00")
        self.assertEqual(snap.started_at, "2026-04-03T20:00:02+00:00")
        self.assertEqual(snap.ended_at, "2026-04-03T21:00:00+00:00")

    def test_malformed_entry_returns_none_not_raise(self):
        # RED-boundary guard, tolerant side: this runs against a snapshot
        # shared by every in-flight album in the 5-min poll loop — one
        # malformed row must degrade to "no status observed", never crash
        # or void matching for every other transfer in the cycle.
        raw = {"id": "t1", "filename": "f.flac", "state": "InProgress",
               "bytesTransferred": "not-a-number"}

        snap = parse_transfer_snapshot(raw)

        self.assertIsNone(snap)

    def test_construction_with_only_state_for_synthetic_statuses(self):
        # Mirrors _restored_terminal_status / the vanished-transfer
        # fallback in lib/download.py — both build a TransferSnapshot
        # directly (not via decode) with only state (+ bytes_transferred).
        snap = TransferSnapshot(state="Completed, Errored")

        self.assertEqual(snap.state, "Completed, Errored")
        self.assertEqual(snap.id, "")
        self.assertEqual(snap.bytes_transferred, 0)


if __name__ == "__main__":
    unittest.main()
