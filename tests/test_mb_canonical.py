"""Deterministic pins for MusicBrainz canonical-release resolution (#1059).

Invariants under test — the module answers exactly one question, "what does
MusicBrainz call this release NOW?", and every failure mode is fail-open:

I1  A merged id resolves to the survivor MusicBrainz names.
I2  A current id resolves to None — "no different canonical known".
I3  Non-MusicBrainz identities never reach the network at all.
I4  Every transport/protocol/shape failure returns None and never raises.
I5  A 4xx is never read as "this release was deleted".
I6  An unconfigured process is inert, so an entry point that forgets to
    wire the base degrades to today's literal behaviour rather than to
    public MusicBrainz.
I7  A merge is proven by the observed 301, never by a response body field.
I8  The ``{"payload": …, "redirected": …}`` envelope every other test hands
    to the seam is the envelope the REAL ``_fetch_json`` produces, and its
    ``redirected`` flag really is set by a real HTTP redirect.
I9  ``canonical_release_status`` (#1089) keeps "MusicBrainz answered and
    names no different id" (``CanonicalReleaseCurrent``) distinct from "no
    answer was obtained at all" (``CanonicalReleaseUnavailable``) — every
    world I3/I4/I6 collapse to ``None`` for ``canonical_release_id`` splits
    into exactly one of those two tagged answers, and ``canonical_release_id``
    itself is unchanged: it is defined in terms of the tagged function.

The fetch seam is the external HTTP edge (leaf-seam mocking is sanctioned
there). Per test-fidelity Rule B the failure fakes raise the exception
classes ``urllib`` really raises — verified live against the mirror on
2026-08-06, where a bogus UUID returns **400**, not 404.

Per test-fidelity Rule C, the envelope those seam fakes return is not left
as a hand-written literal: ``TestRealFetchProducesTheEnvelope`` drives the
real ``_fetch_json`` (and the whole uninjected ``canonical_release_id``)
against a stdlib ``http.server`` serving a real 301, a real direct 200, and
a real oversized body — so the producer of ``redirected`` and the enforcer
of the byte cap are exercised, not assumed.
"""

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar

from lib.mb_canonical import (
    _MAX_RESPONSE_BYTES,
    CanonicalReleaseCurrent,
    CanonicalReleaseRedirected,
    CanonicalReleaseUnavailable,
    _fetch_json,
    canonical_release_id,
    canonical_release_status,
)

# The live merge probed on 2026-08-06: request 316's frozen acquisition id
# and the survivor MusicBrainz redirects it to.
MERGED = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
CURRENT = "d990b8af-01db-46f1-a2cb-d9ca19f57e94"

BASE = "http://mirror.test/ws/2"


def _redirected(release_id: str) -> dict[str, object]:
    """The envelope the real fetch returns for an observed 301."""
    return {"payload": {"id": release_id}, "redirected": True}


def _not_redirected(release_id: str) -> dict[str, object]:
    """A 200 served straight from the requested URL — no merge declared."""
    return {"payload": {"id": release_id}, "redirected": False}


def _fetch_returning(payload: object):
    """A fetch seam that records its URL and returns one decoded body."""
    calls: list[str] = []

    def fetch(url: str) -> object:
        calls.append(url)
        return payload

    return fetch, calls


def _fetch_raising(exc: BaseException):
    calls: list[str] = []

    def fetch(url: str) -> object:
        calls.append(url)
        raise exc

    return fetch, calls


class TestCanonicalResolution(unittest.TestCase):
    def test_merged_id_resolves_to_survivor(self) -> None:
        """I1 — urllib follows the 301; the body's top-level id is canonical."""
        fetch, calls = _fetch_returning(_redirected(SURVIVOR))
        self.assertEqual(
            canonical_release_id(MERGED, ws2_base=BASE, fetch=fetch),
            SURVIVOR,
        )
        self.assertEqual(len(calls), 1)
        self.assertIn(MERGED, calls[0])
        self.assertTrue(calls[0].startswith(BASE))

    def test_current_id_resolves_to_none(self) -> None:
        """I2 — same id back means nothing was merged; caller keeps literal."""
        fetch, _calls = _fetch_returning(_not_redirected(CURRENT))
        self.assertIsNone(canonical_release_id(CURRENT, ws2_base=BASE, fetch=fetch))

    def test_canonical_comparison_ignores_case(self) -> None:
        """I2 — an uppercased echo of the same id is not a merge."""
        fetch, _calls = _fetch_returning(_redirected(CURRENT.upper()))
        self.assertIsNone(canonical_release_id(CURRENT, ws2_base=BASE, fetch=fetch))

    def test_a_body_without_a_redirect_never_declares_a_successor(self) -> None:
        """I7 — a merge is proven by the 301, never by a body field.

        The mirror has served wrong bodies for adversarially-selected
        MBIDs from a TTL-less cache, and this lookup authorizes a RETAG of
        installed files plus a rekey of the request. A 200 answered
        straight from the requested URL must fail closed to the stored id
        however plausible its payload.
        """
        fetch, calls = _fetch_returning(_not_redirected(SURVIVOR))
        self.assertIsNone(
            canonical_release_id(MERGED, ws2_base=BASE, fetch=fetch)
        )
        self.assertEqual(len(calls), 1, "the lookup still happened")


class TestNonMusicBrainzIdentitiesNeverFetch(unittest.TestCase):
    """I3 — no adapter code between MB and Discogs; this is MB-only."""

    def test_discogs_numeric_and_malformed_never_reach_the_network(self) -> None:
        for release_id in ("1870", "0", "", "   ", "not-a-uuid", "12345678"):
            with self.subTest(release_id=release_id):
                fetch, calls = _fetch_returning(_redirected(SURVIVOR))
                self.assertIsNone(
                    canonical_release_id(release_id, ws2_base=BASE, fetch=fetch)
                )
                self.assertEqual(calls, [], "non-MB identity must not fetch")


class TestFailOpen(unittest.TestCase):
    """I4/I5 — every failure returns None; a 4xx never means 'deleted'."""

    def _http_error(self, code: int) -> urllib.error.HTTPError:
        return urllib.error.HTTPError(
            url=f"{BASE}/release/{MERGED}",
            code=code,
            msg="boom",
            hdrs=None,  # pyright: ignore[reportArgumentType]
            fp=None,
        )

    def test_http_errors_return_none(self) -> None:
        # 400 is what the live mirror returns for a bogus UUID; 404 is the
        # shape the poisoned-cache incident produced. Neither is deletion.
        for code in (400, 404, 500, 503):
            with self.subTest(code=code):
                fetch, _calls = _fetch_raising(self._http_error(code))
                self.assertIsNone(
                    canonical_release_id(MERGED, ws2_base=BASE, fetch=fetch)
                )

    def test_transport_failures_return_none(self) -> None:
        failures: list[BaseException] = [
            urllib.error.URLError("connection refused"),
            TimeoutError("timed out"),
            OSError("network unreachable"),
            json.JSONDecodeError("bad", "", 0),
            ValueError("nonsense"),
        ]
        for exc in failures:
            with self.subTest(exc=type(exc).__name__):
                fetch, _calls = _fetch_raising(exc)
                self.assertIsNone(
                    canonical_release_id(MERGED, ws2_base=BASE, fetch=fetch)
                )

    def test_unusable_response_shapes_return_none(self) -> None:
        """Even behind an observed redirect, an unusable body is a non-answer."""
        bodies: list[object] = [
            {},                       # no id at all
            {"id": None},
            {"id": 12345},            # wrong wire type
            {"id": "not-a-uuid"},     # not an MB release id
            {"id": ""},
            [],                       # not an object
            "a string",
            None,
        ]
        for body in bodies:
            with self.subTest(body=body):
                fetch, _calls = _fetch_returning(
                    {"payload": body, "redirected": True},
                )
                self.assertIsNone(
                    canonical_release_id(MERGED, ws2_base=BASE, fetch=fetch)
                )


class TestInertWithoutConfiguredBase(unittest.TestCase):
    """I6 — unconfigured means literal, never a silent public-MB fallback."""

    def test_no_base_configured_never_fetches(self) -> None:
        for base in (None, ""):
            with self.subTest(base=base):
                fetch, calls = _fetch_returning(_redirected(SURVIVOR))
                self.assertIsNone(
                    canonical_release_id(MERGED, ws2_base=base, fetch=fetch)
                )
                self.assertEqual(calls, [], "unconfigured base must not fetch")


class TestCanonicalReleaseStatus(unittest.TestCase):
    """I9 — the tagged variant keeps "answered, current" apart from "no
    answer at all", the distinction #1089's operator merge-rekey action
    needs and ``canonical_release_id`` cannot give it.
    """

    def test_a_redirect_reports_the_survivor(self) -> None:
        fetch, _calls = _fetch_returning(_redirected(SURVIVOR))
        status = canonical_release_status(MERGED, ws2_base=BASE, fetch=fetch)
        self.assertEqual(status, CanonicalReleaseRedirected(SURVIVOR))

    def test_an_answered_current_id_is_current_not_unavailable(self) -> None:
        """The #8792 world: MusicBrainz answered — genuinely not merged."""
        fetch, _calls = _fetch_returning(_not_redirected(CURRENT))
        status = canonical_release_status(CURRENT, ws2_base=BASE, fetch=fetch)
        self.assertEqual(status, CanonicalReleaseCurrent())

    def test_a_cosmetic_self_redirect_is_current(self) -> None:
        fetch, _calls = _fetch_returning(_redirected(CURRENT.upper()))
        status = canonical_release_status(CURRENT, ws2_base=BASE, fetch=fetch)
        self.assertEqual(status, CanonicalReleaseCurrent())

    def test_a_body_without_a_redirect_is_current(self) -> None:
        fetch, _calls = _fetch_returning(_not_redirected(SURVIVOR))
        status = canonical_release_status(MERGED, ws2_base=BASE, fetch=fetch)
        self.assertEqual(status, CanonicalReleaseCurrent())

    def test_a_non_musicbrainz_identity_is_unavailable_not_current(self) -> None:
        """No adapter between MusicBrainz and Discogs — structurally cannot
        answer, so this is "unavailable", never a false "current"."""
        for release_id in ("1870", "0", "", "   ", "not-a-uuid"):
            with self.subTest(release_id=release_id):
                fetch, calls = _fetch_returning(_redirected(SURVIVOR))
                status = canonical_release_status(
                    release_id, ws2_base=BASE, fetch=fetch,
                )
                self.assertEqual(status, CanonicalReleaseUnavailable())
                self.assertEqual(calls, [])

    def test_an_unconfigured_base_is_unavailable(self) -> None:
        for base in (None, ""):
            with self.subTest(base=base):
                fetch, calls = _fetch_returning(_redirected(SURVIVOR))
                status = canonical_release_status(
                    MERGED, ws2_base=base, fetch=fetch,
                )
                self.assertEqual(status, CanonicalReleaseUnavailable())
                self.assertEqual(calls, [])

    def test_http_errors_are_unavailable(self) -> None:
        """BLOCKING-1 (#1089) — the exact bug: a configured-but-down mirror
        must never read as ``CanonicalReleaseCurrent``."""
        for code in (400, 404, 500, 503):
            with self.subTest(code=code):
                exc = urllib.error.HTTPError(
                    url=f"{BASE}/release/{MERGED}", code=code, msg="boom",
                    hdrs=None,  # pyright: ignore[reportArgumentType]
                    fp=None,
                )
                fetch, _calls = _fetch_raising(exc)
                status = canonical_release_status(
                    MERGED, ws2_base=BASE, fetch=fetch,
                )
                self.assertEqual(status, CanonicalReleaseUnavailable())
                self.assertNotEqual(status, CanonicalReleaseCurrent())

    def test_transport_failures_are_unavailable(self) -> None:
        failures: list[BaseException] = [
            urllib.error.URLError("connection refused"),
            TimeoutError("timed out"),
            OSError("network unreachable"),
            json.JSONDecodeError("bad", "", 0),
            ValueError("nonsense"),
        ]
        for exc in failures:
            with self.subTest(exc=type(exc).__name__):
                fetch, _calls = _fetch_raising(exc)
                status = canonical_release_status(
                    MERGED, ws2_base=BASE, fetch=fetch,
                )
                self.assertEqual(status, CanonicalReleaseUnavailable())

    def test_unusable_response_shapes_are_unavailable(self) -> None:
        bodies: list[object] = [
            {}, {"id": None}, {"id": 12345}, {"id": "not-a-uuid"},
            {"id": ""}, [], "a string", None,
        ]
        for body in bodies:
            with self.subTest(body=body):
                fetch, _calls = _fetch_returning(
                    {"payload": body, "redirected": True},
                )
                status = canonical_release_status(
                    MERGED, ws2_base=BASE, fetch=fetch,
                )
                self.assertEqual(status, CanonicalReleaseUnavailable())

    def test_malformed_top_level_envelopes_are_unavailable_not_current(
        self,
    ) -> None:
        """#1089 MINOR-1 (review round 2): a top-level shape that isn't even
        the ``{"payload":..., "redirected":...}`` contract is exactly as
        much "no answer" as a raised exception — never a confirmed
        "current" the operator merge-rekey action could act on.

        Distinct from ``test_unusable_response_shapes_are_unavailable``
        above, which probes a garbage/absent ``payload.id`` behind an
        OBSERVED ``redirected: True`` — these probe the envelope itself:
        not a dict at all, or a dict missing/mistyping the ``redirected``
        key entirely.
        """
        envelopes: list[object] = [
            None, [], "a bare string", {},
            {"payload": {"id": SURVIVOR}},              # missing "redirected"
            {"payload": {"id": SURVIVOR}, "redirected": "yes"},  # wrong type
        ]
        for envelope in envelopes:
            with self.subTest(envelope=envelope):
                fetch, _calls = _fetch_returning(envelope)
                status = canonical_release_status(
                    MERGED, ws2_base=BASE, fetch=fetch,
                )
                self.assertEqual(status, CanonicalReleaseUnavailable())
                self.assertIsNone(
                    canonical_release_id(MERGED, ws2_base=BASE, fetch=fetch),
                )

    def test_canonical_release_id_agrees_with_the_tagged_variant(self) -> None:
        """``canonical_release_id`` is DEFINED IN TERMS OF the tagged
        function — this pins that relationship rather than assuming it."""
        cases: list[tuple[object, str | None]] = [
            (_redirected(SURVIVOR), SURVIVOR),
            (_not_redirected(CURRENT), None),
        ]
        for payload, expected in cases:
            with self.subTest(payload=payload):
                fetch, _calls = _fetch_returning(payload)
                self.assertEqual(
                    canonical_release_id(MERGED, ws2_base=BASE, fetch=fetch),
                    expected,
                )


#: The release id whose document is deliberately larger than the byte cap.
OVERSIZED = "cafecafe-0000-4000-8000-cafecafecafe"
#: A release whose WS/2 URL redirects to itself — a cosmetic redirect, the
#: shape a scheme/host normalisation or trailing-slash rewrite produces.
SELF_REDIRECT = "beefbeef-0000-4000-8000-beefbeefbeef"


class _MirrorHandler(BaseHTTPRequestHandler):
    """A real WS/2-shaped mirror: one merge 301, direct 200s, one huge body."""

    protocol_version = "HTTP/1.0"
    received_user_agents: ClassVar[list[str]] = []

    def log_message(self, format: str, *args: object) -> None:
        del format, args  # a test server must not narrate onto stderr

    def _send(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        type(self).received_user_agents.append(self.headers.get("User-Agent", ""))
        if MERGED in self.path:
            # Exactly what MusicBrainz does for a merged-away release.
            self.send_response(301)
            self.send_header(
                "Location", f"/ws/2/release/{SURVIVOR}?fmt=json",
            )
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if SELF_REDIRECT in self.path and "redirected=1" not in self.path:
            self.send_response(301)
            self.send_header(
                "Location",
                f"/ws/2/release/{SELF_REDIRECT}?fmt=json&redirected=1",
            )
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if SELF_REDIRECT in self.path:
            self._send(200, json.dumps({"id": SELF_REDIRECT}).encode())
            return
        if OVERSIZED in self.path:
            filler = "x" * (_MAX_RESPONSE_BYTES + 1)
            self._send(
                200, json.dumps({"id": SURVIVOR, "filler": filler}).encode(),
            )
            return
        release_id = self.path.split("/release/")[1].split("?")[0]
        self._send(200, json.dumps({"id": release_id}).encode())


@contextmanager
def _mirror() -> Iterator[str]:
    """Serve the handler above on a loopback port; yield its WS/2 base."""
    _MirrorHandler.received_user_agents = []
    server = HTTPServer(("127.0.0.1", 0), _MirrorHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address[:2]
        yield f"http://{host}:{port}/ws/2"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class TestRealFetchProducesTheEnvelope(unittest.TestCase):
    """I8 — the producer of ``redirected`` and the byte cap, driven for real.

    Every other test in this file injects ``fetch`` and hands the module an
    envelope. ``_fetch_json`` is the only thing that can build one, and it
    also enforces ``_MAX_RESPONSE_BYTES``; without this class neither line
    would ever execute (test-fidelity Rule C).
    """

    def _url(self, base: str, release_id: str) -> str:
        return f"{base}/release/{release_id}?fmt=json"

    def test_a_real_301_sets_redirected_and_carries_the_survivor(self) -> None:
        with _mirror() as base:
            envelope = _fetch_json(self._url(base, MERGED))

        self.assertEqual(
            envelope, {"payload": {"id": SURVIVOR}, "redirected": True},
        )
        # The seam fakes' ``_redirected`` helper claims exactly this shape.
        self.assertEqual(envelope, _redirected(SURVIVOR))
        self.assertTrue(
            all(
                agent.startswith("cratedigger-canonical/")
                for agent in _MirrorHandler.received_user_agents
            ),
            _MirrorHandler.received_user_agents,
        )

    def test_a_real_direct_200_leaves_redirected_false(self) -> None:
        with _mirror() as base:
            envelope = _fetch_json(self._url(base, CURRENT))

        self.assertEqual(envelope, _not_redirected(CURRENT))

    def test_an_oversized_body_is_refused_by_the_byte_cap(self) -> None:
        with _mirror() as base, self.assertRaises(ValueError) as caught:
            _fetch_json(self._url(base, OVERSIZED))

        self.assertIn(str(_MAX_RESPONSE_BYTES), str(caught.exception))

    def test_the_uninjected_resolver_follows_a_real_merge(self) -> None:
        """The whole production path: no ``fetch`` argument anywhere."""
        with _mirror() as base:
            self.assertEqual(
                canonical_release_id(MERGED, ws2_base=base), SURVIVOR,
            )
            self.assertIsNone(canonical_release_id(CURRENT, ws2_base=base))
            # I5/I4 fail-open still holds over a real socket.
            self.assertIsNone(canonical_release_id(OVERSIZED, ws2_base=base))

    def test_the_uninjected_tagged_resolver_follows_a_real_merge(self) -> None:
        """I9 — the tagged production path, no ``fetch`` argument, over a
        real socket: redirected / current / unavailable, each for real."""
        with _mirror() as base:
            self.assertEqual(
                canonical_release_status(MERGED, ws2_base=base),
                CanonicalReleaseRedirected(SURVIVOR),
            )
            self.assertEqual(
                canonical_release_status(CURRENT, ws2_base=base),
                CanonicalReleaseCurrent(),
            )
            # A real oversized-body failure is "unavailable", never "current".
            self.assertEqual(
                canonical_release_status(OVERSIZED, ws2_base=base),
                CanonicalReleaseUnavailable(),
            )

    def test_a_cosmetic_redirect_to_the_same_id_declares_no_successor(
        self,
    ) -> None:
        """``redirected`` is necessary, not sufficient: the id must differ."""
        with _mirror() as base:
            envelope = _fetch_json(self._url(base, SELF_REDIRECT))
            self.assertEqual(envelope, _redirected(SELF_REDIRECT))
            self.assertIsNone(
                canonical_release_id(SELF_REDIRECT, ws2_base=base),
            )


if __name__ == "__main__":
    unittest.main()
