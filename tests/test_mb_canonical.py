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

The fetch seam is the external HTTP edge (leaf-seam mocking is sanctioned
there). Per test-fidelity Rule B the failure fakes raise the exception
classes ``urllib`` really raises — verified live against the mirror on
2026-08-06, where a bogus UUID returns **400**, not 404.
"""

from __future__ import annotations

import json
import unittest
import urllib.error

from lib.mb_canonical import canonical_release_id

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
        MBIDs from a TTL-less cache, and this lookup authorizes a
        duplicate REMOVAL. A 200 answered straight from the requested URL
        must fail closed to the stored id however plausible its payload.
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


if __name__ == "__main__":
    unittest.main()
