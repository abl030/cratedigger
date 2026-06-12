#!/usr/bin/env python3
"""Shared HTTP test harness for the web route contract tests (#408, #430).

Starts a real HTTP server on a random port; the ``tests/web/test_*.py``
modules verify response codes, JSON structure, and error handling
against it. One harness, no per-class copies, no MagicMock DB: every
test runs against a fresh, bare :class:`FakePipelineDB` installed as
``web.server.db`` (the same module-global swap production uses for
DSN-less handles), so assertions hit the fake's real query semantics.
"""

import json
import os
import sys
import threading
import unittest
from http.server import HTTPServer, ThreadingHTTPServer
from unittest.mock import patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Eager import: lib.beets_distance pins the real ``beets`` package
# (see lib/beets_distance.py:49-55) — we must trigger it *before* the
# next two ``sys.path.insert`` calls add ``lib/`` ahead of site-
# packages, otherwise downstream imports of lib.youtube_album_service
# (which imports lib.beets_distance lazily inside the route handler)
# fail with "cannot import name 'library' from 'beets'" because
# ``beets`` would resolve to ``lib/beets.py``.
import lib.beets_distance  # noqa: F401,E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "web"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "lib"))

from tests.fakes import FakePipelineDB

# Production-shaped ``validation_result`` template for wrong-match
# seeding (the JSONB blob a rejected download_log row carries). Tests
# deepcopy and override; see test_routes_imports._seed_wrong_match.
_DEFAULT_WRONG_MATCH_VALIDATION = {
    "distance": 0.25,
    "scenario": "high_distance",
    "detail": "distance too high",
    "failed_path": "/mnt/virtio/music/slskd/failed_imports/Test",
    "soulseek_username": "testuser",
    "candidates": [{
        "is_target": True,
        "artist": "Test Artist",
        "album": "Test Album",
        "distance": 0.25,
        "distance_breakdown": {"tracks": 0.15, "album": 0.10},
        "track_count": 10,
        "mapping": [],
        "extra_items": [],
        "extra_tracks": [],
    }],
    "items": [{"path": "01 Track.mp3", "title": "Track"}],
}


def _assert_required_fields(
    case: unittest.TestCase,
    payload: dict,
    required_fields: set[str],
    label: str,
) -> None:
    missing = required_fields - set(payload.keys())
    case.assertFalse(missing, f"{label} missing fields: {missing}")


class _WebServerCase(unittest.TestCase):
    """Shared HTTP server harness (no DB opinions — see the subclass)."""

    server: HTTPServer
    port: int
    base: str

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        # shutdown() stops serve_forever but leaves the listening
        # socket open — close it or every test class leaks one
        # ResourceWarning into the suite output (#445 item 5).
        cls.server.server_close()

    def _get(self, path: str) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        try:
            with urlopen(url) as resp:
                return resp.status, json.loads(resp.read())
        except HTTPError as e:
            with e:
                return e.code, json.loads(e.read())

    def _post(self, path: str, body: dict) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except HTTPError as e:
            with e:
                return e.code, json.loads(e.read())


class _FakeDbWebServerCase(_WebServerCase):
    """Contract-test base with a bare per-test :class:`FakePipelineDB`.

    ``setUp`` installs a fresh fake as ``web.server.db`` (the same
    module-global swap production uses for DSN-less handles — ``_db()``
    returns it directly), so every test starts from empty typed state.
    Tests seed what they need (``self.db.seed_request(...)``,
    ``self.db.log_download(...)``, ``self.db.update_status(...)``) and
    assertions hit the fake's real query semantics.
    """

    db: FakePipelineDB

    #: Override in subclasses that need a typed failure-injecting
    #: FakePipelineDB subclass (e.g. raising connection errors from a
    #: specific method) — still a stateful fake, never a MagicMock.
    DB_FACTORY: type[FakePipelineDB] = FakePipelineDB

    def setUp(self) -> None:
        super().setUp()
        import web.server as srv
        self.db = self.DB_FACTORY()
        patcher = patch.object(srv, "db", self.db)
        patcher.start()
        self.addCleanup(patcher.stop)


def _fresh_triage_runner(case: unittest.TestCase):
    """Swap in a fresh runner so triage tests don't share sweep state."""
    from web import triage_runner as triage_runner_module
    from web.routes import imports as imports_module
    previous = imports_module._triage_runner
    runner = triage_runner_module.TriageRunner()
    imports_module._triage_runner = runner
    case.addCleanup(
        setattr, imports_module, "_triage_runner", previous,
    )
    return runner


def _make_server():
    """Create a test server on a random port.

    The boot-time ``web.server.db`` is an empty :class:`FakePipelineDB`
    — :class:`_FakeDbWebServerCase` shadows it with a fresh fake per
    test, so the boot fake only serves requests issued outside a test
    body (there are none in practice).
    """
    import web.server as srv

    srv.db = FakePipelineDB()
    srv.beets_db_path = None  # No beets DB in tests

    # Mirror production: ThreadingHTTPServer + the same Handler.
    server = ThreadingHTTPServer(("127.0.0.1", 0), srv.Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port
