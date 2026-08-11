"""Generated properties for MusicBrainz merge-redirect resolution (#1059).

The pins in ``tests/test_mb_canonical.py`` prove the exact live shapes; these
properties patrol the world space around them, driving the REAL
``canonical_release_id`` over every fetch outcome a broken, hostile, or
merely unlucky mirror can produce.

This module authorizes a RETAG of installed files plus a rekey of the
request downstream, so its failure posture matters more than its success
case. The mirror has served wrong bodies for adversarially-selected MBIDs
from a TTL-less cache, and a bogus UUID answers ``400`` rather than ``404``
at all.

Invariants patrolled — each is a module-level checker so the known-bad
self-tests below can call it directly:

R1  Never raises. Whatever the fetch does — any exception, any shape — the
    caller gets an answer, because every consumer of this module is
    fail-open by contract (#1059 invariant 8).
R2  Redirect-proof: a successor is declared only by an OBSERVED redirect.
    A body ``id`` on a non-redirected response never becomes an answer,
    however confident the document looks (#1059 invariant 2).
R3  A returned survivor is never the requested id. A self-redirect is not a
    merge, and acting on one would make the sweep retag and rekey a request
    onto the id it already holds.
R4  A returned survivor is always a MusicBrainz UUID. A Discogs numeric or
    a garbage token never escapes as a release identity.
R5  Non-MusicBrainz identities and unconfigured processes never touch the
    network at all — inertness is observable, not merely intended.
"""

from __future__ import annotations

import email.message
import unittest
import urllib.error

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.mb_canonical import canonical_release_id
from lib.release_identity import detect_release_source, normalize_release_id

# The live merge probed on 2026-08-06 (request 316).
STORED = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
BASE = "http://mirror.test/ws/2"


# ---------------------------------------------------------------------------
# Invariant checkers — module level so the known-bad self-tests can call them
# ---------------------------------------------------------------------------


def check_redirect_proof(
    answer: str | None,
    *,
    redirected: bool,
) -> None:
    """R2 — only an observed redirect may declare a successor."""
    if answer is not None and not redirected:
        raise AssertionError(
            "a successor was declared without an observed redirect: "
            f"{answer!r} — a response body alone is not proof of a merge"
        )


def check_never_the_requested_id(answer: str | None, requested: str) -> None:
    """R3 — a self-redirect is not a merge."""
    if answer is not None and answer == normalize_release_id(requested):
        raise AssertionError(
            f"resolver returned the requested id {answer!r} as its own "
            "successor; a request can never be rekeyed onto the id it holds"
        )


def check_answer_is_a_musicbrainz_id(answer: str | None) -> None:
    """R4 — nothing but a MusicBrainz UUID escapes as an identity."""
    if answer is None:
        return
    if detect_release_source(answer) != "musicbrainz":
        raise AssertionError(
            f"resolver returned a non-MusicBrainz identity: {answer!r}"
        )


def check_no_fetch_attempted(calls: list[str]) -> None:
    """R5 — inert paths are observably inert."""
    if calls:
        raise AssertionError(
            f"a lookup that must be inert reached the network: {calls!r}"
        )


# ---------------------------------------------------------------------------
# Strategies over the world space — no plausibility filters
# ---------------------------------------------------------------------------

def _http_error(code: int, msg: str) -> urllib.error.HTTPError:
    """A real ``HTTPError``, headers included, per test-fidelity Rule B."""
    return urllib.error.HTTPError(
        url=f"{BASE}/release/{STORED}",
        code=code,
        msg=msg,
        hdrs=email.message.Message(),
        fp=None,
    )


#: Every failure a real fetch can raise. Rule B: the real classes, never a
#: synthetic stand-in, and 4xx is present precisely because it must NOT be
#: read as "this release is gone". 400 is what the live mirror answers for a
#: bogus UUID; 404 is the shape the poisoned-cache incident produced.
FETCH_EXCEPTIONS = st.sampled_from([
    _http_error(400, "Bad Request"),
    _http_error(404, "Not Found"),
    _http_error(500, "Server Error"),
    _http_error(503, "Unavailable"),
    urllib.error.URLError("connection refused"),
    TimeoutError("read timed out"),
    ValueError("response exceeded the byte cap"),
])

#: What the decoded envelope's ``payload.id`` can be.
PAYLOAD_IDS = st.sampled_from([
    SURVIVOR,
    STORED,
    "12856590",                              # a Discogs numeric
    "not-a-uuid",
    "",
    "7AABF975-9A06-4B2E-854C-2C700380EBD5",  # the survivor, upper case
])

#: Envelope shapes, including the ones a broken mirror or a future refactor
#: could produce. ``redirected`` is the only field that may declare a merge.
ENVELOPES = st.one_of(
    st.builds(
        lambda rid, red: {"payload": {"id": rid}, "redirected": red},
        PAYLOAD_IDS,
        st.booleans(),
    ),
    st.just({"payload": {}, "redirected": True}),
    st.just({"payload": None, "redirected": True}),
    st.just({"redirected": True}),
    st.just({"payload": {"id": SURVIVOR}}),
    st.just({"payload": {"id": SURVIVOR}, "redirected": "yes"}),
    st.just({}),
    st.just([]),
    st.just("a bare string"),
    st.just(None),
)


def _recording_fetch(outcome: object, *, raises: BaseException | None = None):
    calls: list[str] = []

    def fetch(url: str) -> object:
        calls.append(url)
        if raises is not None:
            raise raises
        return outcome

    return fetch, calls


class TestResolverProperties(unittest.TestCase):
    """R1–R4 over every envelope shape, driving the real resolver."""

    @settings(deadline=None)
    @given(envelope=ENVELOPES)
    # The two decisive worlds: a real merge, and the poisoned-cache shape
    # where the body names a successor the transport never redirected to.
    @example(envelope={"payload": {"id": SURVIVOR}, "redirected": True})
    @example(envelope={"payload": {"id": SURVIVOR}, "redirected": False})
    def test_every_envelope_upholds_the_resolver_invariants(
        self, envelope: object,
    ) -> None:
        fetch, calls = _recording_fetch(envelope)

        # R1: never raises.
        answer = canonical_release_id(STORED, ws2_base=BASE, fetch=fetch)

        redirected = (
            isinstance(envelope, dict) and envelope.get("redirected") is True
        )
        check_redirect_proof(answer, redirected=redirected)
        check_never_the_requested_id(answer, STORED)
        check_answer_is_a_musicbrainz_id(answer)
        self.assertEqual(len(calls), 1, "a configured MB lookup must fetch once")

    @settings(deadline=None)
    @given(exc=FETCH_EXCEPTIONS)
    def test_every_fetch_failure_is_fail_open(
        self, exc: BaseException,
    ) -> None:
        """R1 + #1059 invariant 8 — a non-answer is never an answer.

        A 4xx is in the strategy on purpose: reading one as "this release
        was deleted" is the documented mirror-trust bug, and the only way
        this module can avoid it is by treating every failure identically.
        """
        fetch, calls = _recording_fetch(None, raises=exc)

        answer = canonical_release_id(STORED, ws2_base=BASE, fetch=fetch)

        self.assertIsNone(answer)
        self.assertEqual(len(calls), 1)

    @settings(deadline=None)
    @given(
        release_id=st.sampled_from([
            "12856590", "not-a-uuid", "", "   ", "0",
        ]),
    )
    def test_non_musicbrainz_identities_never_reach_the_network(
        self, release_id: str,
    ) -> None:
        """R5 — Discogs has no redirect concept; this is not an adapter."""
        fetch, calls = _recording_fetch(
            {"payload": {"id": SURVIVOR}, "redirected": True},
        )

        answer = canonical_release_id(release_id, ws2_base=BASE, fetch=fetch)

        self.assertIsNone(answer)
        check_no_fetch_attempted(calls)

    @settings(deadline=None)
    @given(base=st.sampled_from(["", "   ", "/", None]))
    def test_an_unconfigured_process_is_inert(self, base: str | None) -> None:
        """R5 — a forgotten wiring degrades to the stored id, never to
        public MusicBrainz."""
        fetch, calls = _recording_fetch(
            {"payload": {"id": SURVIVOR}, "redirected": True},
        )

        answer = canonical_release_id(STORED, ws2_base=base or "", fetch=fetch)

        self.assertIsNone(answer)
        check_no_fetch_attempted(calls)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every checker owes a planted violation proving it can fail.

    A property that has never rejected anything is unfalsifiable until
    proven otherwise.
    """

    def test_body_declared_successor_without_a_redirect_is_rejected(
        self,
    ) -> None:
        with self.assertRaises(AssertionError):
            check_redirect_proof(SURVIVOR, redirected=False)

    def test_self_redirect_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_never_the_requested_id(STORED, STORED)

    def test_discogs_numeric_answer_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_answer_is_a_musicbrainz_id("12856590")

    def test_garbage_answer_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_answer_is_a_musicbrainz_id("not-a-uuid")

    def test_a_fetch_on_an_inert_path_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_no_fetch_attempted([f"{BASE}/release/x"])

    def test_checkers_accept_the_legitimate_answer(self) -> None:
        """Must-still-work: the real merge passes every checker."""
        check_redirect_proof(SURVIVOR, redirected=True)
        check_never_the_requested_id(SURVIVOR, STORED)
        check_answer_is_a_musicbrainz_id(SURVIVOR)
        check_no_fetch_attempted([])


if __name__ == "__main__":
    unittest.main()
