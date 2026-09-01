"""Shared HTTP test harness for the web route contract tests (#408, #430).

Starts a real HTTP server on a random port; the ``tests/web/test_*.py``
modules verify response codes, JSON structure, and error handling
against it. One harness, no per-class copies, no MagicMock DB: every
test runs against a fresh, bare :class:`FakePipelineDB` injected as the
runtime's ``shared_db`` (the same seam production uses for DSN-less
handles), so assertions hit the fake's real query semantics.
"""

import json
import os
import sys
import threading
import unittest
from http.server import HTTPServer, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_web_runtime
from web.request_security import (
    BROWSER_CHANNEL,
    CHANNEL_HEADER,
)
from web.runtime import WebRuntime, install_runtime, runtime

CANONICAL_ORIGIN = "https://music.ablz.au"

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
        # One runtime for the class, uninstalled by enterClassContext on
        # tearDownClass — the six hand-set module globals this replaced
        # had no teardown at all and leaked into whatever ran next.
        cls.enterClassContext(install_runtime(make_web_runtime(
            WebRuntime(canonical_origin=CANONICAL_ORIGIN),
            db=FakePipelineDB(),
            beets=FakeBeetsDB(),
        )))
        cls.server, cls.port = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        # shutdown() stops serve_forever but leaves the listening
        # socket open — close it or every test class leaks one
        # ResourceWarning into the suite output (#445 item 5).
        cls.server.server_close()

    def _get(
        self,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        include_security: bool = True,
    ) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        request_headers = (
            {CHANNEL_HEADER: BROWSER_CHANNEL} if include_security else {}
        )
        request_headers.update(headers or {})
        request = Request(url, headers=request_headers)
        try:
            with urlopen(request) as resp:
                return resp.status, json.loads(resp.read())
        except HTTPError as e:
            with e:
                return e.code, json.loads(e.read())

    def _post(
        self,
        path: str,
        body: dict,
        *,
        headers: dict[str, str] | None = None,
        include_security: bool = True,
    ) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        request_headers = {"Content-Type": "application/json"}
        if include_security:
            request_headers.update({
                CHANNEL_HEADER: BROWSER_CHANNEL,
                "Origin": CANONICAL_ORIGIN,
            })
        request_headers.update(headers or {})
        req = Request(url, data=data, headers=request_headers)
        try:
            with urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except HTTPError as e:
            with e:
                return e.code, json.loads(e.read())


class _FakeDbWebServerCase(_WebServerCase):
    """Contract-test base with a bare per-test :class:`FakePipelineDB`.

    ``setUp`` derives a runtime carrying a fresh fake as ``shared_db``
    (the same seam production uses for DSN-less handles — ``db()``
    returns it directly), so every test starts from empty typed state.
    Tests seed what they need (``self.db.seed_request(...)``,
    ``self.db.log_download(...)``, ``self.db.update_status(...)``) and
    assertions hit the fake's real query semantics.

    A test that needs to vary one more collaborator derives again from
    the installed one, e.g.::

        with install_runtime(replace(runtime(), shared_beets=seeded)):
            ...
    """

    db: FakePipelineDB

    #: Override in subclasses that need a typed failure-injecting
    #: FakePipelineDB subclass (e.g. raising connection errors from a
    #: specific method) — still a stateful fake, never a MagicMock.
    DB_FACTORY: type[FakePipelineDB] = FakePipelineDB

    def setUp(self) -> None:
        super().setUp()
        self.db = self.DB_FACTORY()
        self.enterContext(install_runtime(make_web_runtime(
            runtime(), db=self.db, beets=FakeBeetsDB(),
        )))


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

    The runtime is already installed by the caller
    (:meth:`_WebServerCase.setUpClass`); its class-scoped
    :class:`FakePipelineDB` only serves requests issued outside a test
    body, since :class:`_FakeDbWebServerCase` derives a fresh one per
    test. Everything this used to hand-set — a beets path, two snapshot
    paths — is simply absent from that runtime, which is what "not
    configured in tests" now means.
    """
    import web.server as srv

    # Mirror production: ThreadingHTTPServer + the same Handler.
    server = ThreadingHTTPServer(("127.0.0.1", 0), srv.Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port
