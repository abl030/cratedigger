"""Deterministic pins for MusicBrainz canonical-release resolution (#1049).

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

import configparser
import json
import unittest
import urllib.error
from pathlib import Path

from lib.beets_db import (
    CurrentBeetsMissing,
    CurrentBeetsUnique,
    open_beets_db,
)
from lib.config import CratediggerConfig
from lib.mb_canonical import CanonicalReleaseFn, canonical_release_id
from lib.release_identity import ReleaseIdentity
from tests.beets_world import BeetsWorld, BeetsWorldRelease

REPO = Path(__file__).resolve().parent.parent

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


class TestJoinFollowsMerges(unittest.TestCase):
    """The Beets join resolves through the 301 instead of missing.

    Driven against a REAL ``BeetsDB`` over a real temp SQLite library — the
    guard and the authority composed over one resource, never a mock of our
    own resolver.

    J1  A release Beets holds only under the survivor id resolves.
    J2  The dict stays keyed by the STORED identity, while the resolution
        reports the CANONICAL identity and selectors — what Beets actually
        holds, which is what a destructive path must act on.
    J3  Miss-triggered: a hit never asks MusicBrainz, and a Discogs
        identity never asks at all.
    J4  A sibling retag (no redirect) still reports missing — one code, one
        meaning.
    J5  Unresolvable (mirror down / 4xx) is byte-identical to today.
    """

    def setUp(self) -> None:
        self.world = BeetsWorld(REPO)
        self.addCleanup(self.world.close)

    def _hold(self, release_id: str, *, tracks: int = 2) -> str:
        """Really import one release into the real scratch library."""
        return self.world.import_release(BeetsWorldRelease(
            release_id=release_id,
            artist="Archivist",
            album="Exact pressing",
            year=2001,
            track_count=tracks,
        )).album_path

    def _open(self, canonical_fn: CanonicalReleaseFn | None):
        ini = configparser.RawConfigParser()
        ini["Beets"] = {
            "directory": str(self.world.library_root),
            "library": str(self.world.library_db),
        }
        return open_beets_db(
            CratediggerConfig.from_ini(ini),
            canonical_release_fn=canonical_fn,
        )

    def _resolve(self, stored: str, canonical_map: dict[str, str]):
        calls: list[str] = []

        def canonical_fn(release_id: str) -> str | None:
            calls.append(release_id)
            return canonical_map.get(release_id)

        identity = ReleaseIdentity.from_id(stored)
        assert identity is not None
        with self._open(canonical_fn) as beets:
            return beets.resolve_current_release(identity), calls, identity

    def test_release_held_under_survivor_id_resolves(self) -> None:
        """J1/J2 — the live 316 shape: Beets holds only the survivor."""
        album_path = self._hold(SURVIVOR)
        resolution, calls, stored_identity = self._resolve(
            MERGED, {MERGED: SURVIVOR},
        )
        self.assertIsInstance(resolution, CurrentBeetsUnique)
        assert isinstance(resolution, CurrentBeetsUnique)
        self.assertEqual(resolution.album_path, album_path)
        self.assertEqual(calls, [MERGED], "exactly one miss-triggered lookup")
        # J2 — the resolution ANSWERS with the identity we asked for; what
        # Beets really holds travels separately. Every consumer that
        # compares a resolution back to its request depends on the first
        # half, and the delete/post-import consumers on the second.
        self.assertEqual(resolution.identity, stored_identity)
        self.assertEqual(resolution.identity.release_id, MERGED)
        self.assertEqual(resolution.effective_identity.release_id, SURVIVOR)
        self.assertEqual(resolution.held_identity, ReleaseIdentity.from_id(SURVIVOR))
        self.assertEqual(resolution.selectors, (f"mb_albumid:{SURVIVOR}",))

    def test_hit_never_asks_musicbrainz(self) -> None:
        """J3 — the trigger is the miss, not a scan."""
        self._hold(CURRENT, tracks=1)
        resolution, calls, _identity = self._resolve(CURRENT, {})
        self.assertIsInstance(resolution, CurrentBeetsUnique)
        self.assertEqual(calls, [], "a hit must never pay for a lookup")

    def test_discogs_identity_never_asks_musicbrainz(self) -> None:
        """J3 — no adapter code between MB and Discogs."""
        resolution, calls, _identity = self._resolve("1870", {})
        self.assertIsInstance(resolution, CurrentBeetsMissing)
        self.assertEqual(calls, [])

    def test_sibling_retag_without_redirect_still_missing(self) -> None:
        """J4 — a remaining miss keeps exactly one meaning."""
        self._hold(SURVIVOR, tracks=1)
        resolution, calls, _identity = self._resolve(MERGED, {})
        self.assertIsInstance(resolution, CurrentBeetsMissing)
        self.assertEqual(calls, [MERGED])

    def test_unresolvable_canonical_is_identical_to_today(self) -> None:
        """J5 — mirror down: never worse than the status quo."""
        self._hold(SURVIVOR, tracks=1)
        identity = ReleaseIdentity.from_id(MERGED)
        assert identity is not None
        with self._open(None) as beets:
            without = beets.resolve_current_release(identity)
        resolution, _calls, _identity = self._resolve(MERGED, {})
        self.assertEqual(resolution, without)
        self.assertIsInstance(without, CurrentBeetsMissing)

    def test_canonical_also_missing_stays_missing(self) -> None:
        """J5/J6 — a survivor Beets does not hold is still a miss, and the
        miss still names the identity the caller ASKED for.

        Reporting the canonical identity on an unresolved miss would be a
        substituted identity leaking out of a failed lookup: downstream
        compares a resolution's identity against the request's stored id,
        and ``assert_current_resolution`` calls that "resolver substituted
        another release identity". Asserting only ``CurrentBeetsMissing``
        here let a planted mutant survive (fault injection, 2026-08-06).
        """
        resolution, calls, stored_identity = self._resolve(
            MERGED, {MERGED: SURVIVOR},
        )
        self.assertIsInstance(resolution, CurrentBeetsMissing)
        self.assertEqual(calls, [MERGED])
        self.assertEqual(resolution.identity, stored_identity)
        self.assertEqual(resolution.identity.release_id, MERGED)


if __name__ == "__main__":
    unittest.main()
