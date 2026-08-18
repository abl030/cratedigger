"""Generated proof of the real-API cover-art fallback contract (#1200).

The invariant this patrols, driven through the REAL patched
``DiscogsPlugin.select_cover_art`` (installed by
``configure_discogs_cover_art_fallback``) against a REAL
``discogs_client.Release`` object, with only the ``requests.get`` network
leaf faked:

* A stock (unpatched) lookup that already yields a URL is returned
  UNCHANGED, and the real Discogs API is never called (no-op for stock
  behaviour).
* With no configured user token, the real API is never called and the
  result is ``None``.
* With a token, the real API is called AT MOST ONCE per invocation, for the
  EXACT release id the ``Release`` object carries (strict pressing identity
  — a fallback that fetches a different release's artwork is the same
  defect this fallback exists to eliminate), with the fail-soft timeout
  pinned (never blocks an import on a black-holed api.discogs.com). If it
  returns a syntactically valid, non-empty ``images`` list, the fallback
  returns the first entry's ``uri`` exactly. Any other outcome — a raised
  network exception, a non-2xx response, a malformed JSON body, a
  type-mismatched payload, or an empty ``images`` list — degrades to
  ``None``, never an exception.

Deterministic pins for the same contract (incl. the canonical David Bowie
1969/1967 pressing-collision regression) live in
``tests/test_discogs_cover_art_fallback.py``.
"""

from __future__ import annotations

import json
import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Literal
from unittest.mock import patch

import confuse
import discogs_client
import requests
from beetsplug.discogs import DiscogsPlugin
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from harness.beets_compat import (
    _DISCOGS_COVER_ART_TIMEOUT_SECONDS,
    DISCOGS_REAL_API_BASE,
    configure_discogs_cover_art_fallback,
)

_MIRROR_CLIENT = SimpleNamespace(_base_url="http://discogs-mirror.example")

NetworkMode = Literal[
    "connection_error",
    "timeout",
    "http_error",
    "malformed_json",
    "wrong_type_images",
    "valid_images",
]

_NETWORK_MODES: tuple[NetworkMode, ...] = (
    "connection_error",
    "timeout",
    "http_error",
    "malformed_json",
    "wrong_type_images",
    "valid_images",
)


@dataclass(frozen=True)
class CoverArtWorld:
    release_id: int
    stock_url: str | None
    token: str
    mode: NetworkMode
    http_status: int
    images: tuple[str, ...]

    @property
    def token_present(self) -> bool:
        return bool(self.token)

    @property
    def network_images(self) -> list[str] | None:
        """The exact image-uri list a valid response carried, else ``None``.

        ``None`` means the network edge never produced a usable list — it
        raised, returned a non-2xx status, or the body was malformed/
        type-mismatched.
        """
        if self.mode == "valid_images":
            return list(self.images)
        return None


@st.composite
def cover_art_worlds(draw: st.DrawFn) -> CoverArtWorld:
    release_id = draw(st.integers(min_value=1, max_value=99_999_999))
    stock_has_url = draw(st.booleans())
    stock_url = (
        draw(st.text(min_size=1, max_size=24)) if stock_has_url else None
    )
    token = draw(st.one_of(st.just(""), st.text(min_size=1, max_size=24)))
    mode = draw(st.sampled_from(_NETWORK_MODES))
    http_status = (
        draw(st.sampled_from((400, 401, 404, 500, 503)))
        if mode == "http_error"
        else 200
    )
    images: tuple[str, ...] = ()
    if mode == "valid_images":
        count = draw(st.integers(min_value=0, max_value=4))
        images = tuple(
            draw(st.text(min_size=1, max_size=24)) for _ in range(count)
        )
    return CoverArtWorld(
        release_id=release_id,
        stock_url=stock_url,
        token=token,
        mode=mode,
        http_status=http_status,
        images=images,
    )


_RELEASE_URL_PREFIX = f"{DISCOGS_REAL_API_BASE}/releases/"


def _requested_release_id(requested_urls: list[str]) -> int | None:
    """Parse the release id the LAST real-API call actually requested.

    ``None`` means either no call happened, or the requested URL does not
    match the documented ``{DISCOGS_REAL_API_BASE}/releases/{release_id}``
    shape at all -- both are real findings the checker must be able to see,
    never silently swallowed.
    """
    if not requested_urls:
        return None
    url = requested_urls[-1]
    if not url.startswith(_RELEASE_URL_PREFIX):
        return None
    suffix = url[len(_RELEASE_URL_PREFIX):]
    try:
        return int(suffix)
    except ValueError:
        return None


def _fake_response(status_code: int, body: bytes) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body
    return response


def _network_responder(world: CoverArtWorld):
    """Build the ``requests.get`` replacement for one generated world."""

    if world.mode == "connection_error":
        def responder(*_a: object, **_kw: object) -> requests.Response:
            raise requests.exceptions.ConnectionError("no route to host")
        return responder
    if world.mode == "timeout":
        def responder(*_a: object, **_kw: object) -> requests.Response:
            raise requests.exceptions.Timeout("timed out")
        return responder
    if world.mode == "http_error":
        def responder(*_a: object, **_kw: object) -> requests.Response:
            return _fake_response(world.http_status, b"error body")
        return responder
    if world.mode == "malformed_json":
        def responder(*_a: object, **_kw: object) -> requests.Response:
            return _fake_response(200, b"{not valid json")
        return responder
    if world.mode == "wrong_type_images":
        def responder(*_a: object, **_kw: object) -> requests.Response:
            return _fake_response(
                200, json.dumps({"images": "not-a-list"}).encode()
            )
        return responder

    def responder(*_a: object, **_kw: object) -> requests.Response:
        body = json.dumps(
            {"images": [{"uri": uri} for uri in world.images]}
        ).encode()
        return _fake_response(200, body)
    return responder


# Invariant checker (module-level so the known-bad self-tests can call it
# directly). Accumulates every violation rather than short-circuiting, per
# .claude/rules/code-quality.md "prefer an accumulating list[str]".
def cover_art_fallback_violations(
    *,
    stock_url: str | None,
    token_present: bool,
    network_images: list[str] | None,
    wrapped_result: str | None,
    real_api_call_count: int,
    requested_release_id: int | None,
    expected_release_id: int,
    requested_timeout: object,
) -> list[str]:
    violations: list[str] = []

    if real_api_call_count > 1:
        violations.append(
            "the real Discogs API was called more than once for one "
            f"select_cover_art invocation (calls={real_api_call_count})"
        )

    if real_api_call_count > 0:
        # Strict pressing identity (#1200): a fallback lookup that fetches
        # a DIFFERENT release's artwork is exactly the wrong-pressing
        # defect this fallback exists to eliminate -- not a lesser bug.
        if requested_release_id != expected_release_id:
            violations.append(
                "the real Discogs API was called for the WRONG release id "
                f"-- strict pressing identity violation: requested="
                f"{requested_release_id!r} expected={expected_release_id!r}"
            )
        if requested_timeout != _DISCOGS_COVER_ART_TIMEOUT_SECONDS:
            violations.append(
                "the real Discogs API call did not pin the fail-soft "
                f"timeout: requested={requested_timeout!r} expected="
                f"{_DISCOGS_COVER_ART_TIMEOUT_SECONDS!r}"
            )

    if stock_url:
        if wrapped_result != stock_url:
            violations.append(
                "stock select_cover_art result was not returned unchanged: "
                f"stock={stock_url!r} wrapped={wrapped_result!r}"
            )
        if real_api_call_count != 0:
            violations.append(
                "the real Discogs API was called even though the stock "
                f"lookup already returned a URL (calls={real_api_call_count})"
            )
        return violations

    if not token_present:
        if real_api_call_count != 0:
            violations.append(
                "the real Discogs API was called with no configured user "
                f"token (calls={real_api_call_count})"
            )
        if wrapped_result is not None:
            violations.append(
                "fallback returned a URL with no configured token: "
                f"{wrapped_result!r}"
            )
        return violations

    if network_images:
        if wrapped_result != network_images[0]:
            violations.append(
                "fallback did not return the real API's primary image "
                f"uri: expected={network_images[0]!r} got={wrapped_result!r}"
            )
        return violations

    if wrapped_result is not None:
        violations.append(
            "fallback returned a URL from an unreachable/malformed/empty "
            f"real API lookup: {wrapped_result!r}"
        )
    return violations


@dataclass(frozen=True)
class FallbackDrive:
    wrapped_result: str | None
    call_count: int
    requested_release_id: int | None
    requested_timeout: object


def _drive_real_fallback(world: CoverArtWorld) -> FallbackDrive:
    """Run the REAL patched ``select_cover_art`` over one generated world.

    Only ``requests.get`` — the documented network leaf — is faked; every
    call it actually received (URL and ``timeout=`` kwarg) is recorded, not
    merely counted, so the checker can prove WHICH release was fetched and
    that the fail-soft timeout was really pinned (#1200 review F2/F3).
    """
    configure_discogs_cover_art_fallback()
    plugin = object.__new__(DiscogsPlugin)
    root = confuse.RootView(
        [confuse.ConfigSource.of({"discogs": {"user_token": world.token}})]
    )
    plugin.config = root["discogs"]
    stock_images = [{"uri": world.stock_url}] if world.stock_url else []
    release = discogs_client.Release(
        _MIRROR_CLIENT, {"id": world.release_id, "images": stock_images}
    )

    call_count = 0
    requested_urls: list[str] = []
    requested_timeouts: list[object] = []
    responder = _network_responder(world)

    def counting_responder(*args: object, **kwargs: object) -> requests.Response:
        nonlocal call_count
        call_count += 1
        if args:
            requested_urls.append(str(args[0]))
        requested_timeouts.append(kwargs.get("timeout"))
        return responder(*args, **kwargs)

    with patch("requests.get", side_effect=counting_responder):
        result = plugin.select_cover_art(release)
    return FallbackDrive(
        wrapped_result=result,
        call_count=call_count,
        requested_release_id=_requested_release_id(requested_urls),
        requested_timeout=requested_timeouts[-1] if requested_timeouts else None,
    )


class TestCoverArtFallbackGenerated(unittest.TestCase):
    @given(world=cover_art_worlds())
    def test_real_api_fallback_contract_holds(self, world: CoverArtWorld) -> None:
        drive = _drive_real_fallback(world)
        violations = cover_art_fallback_violations(
            stock_url=world.stock_url,
            token_present=world.token_present,
            network_images=world.network_images,
            wrapped_result=drive.wrapped_result,
            real_api_call_count=drive.call_count,
            requested_release_id=drive.requested_release_id,
            expected_release_id=world.release_id,
            requested_timeout=drive.requested_timeout,
        )
        self.assertEqual(violations, [], (world, violations))


#: Matching id/timeout, used by self-tests that don't care about F2/F3's
#: clauses so that clause stays silent and only the intended one fires.
_NEUTRAL_RELEASE_ID = 4242424
_NEUTRAL_TIMEOUT = _DISCOGS_COVER_ART_TIMEOUT_SECONDS


# Known-bad self-tests: each checker CLAUSE must trip on a planted
# violation, proven with the checker's own message.
class TestCoverArtFallbackCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_real_api_called_more_than_once(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=True,
            network_images=["https://real/a.jpg"],
            wrapped_result="https://real/a.jpg",
            real_api_call_count=2,
            requested_release_id=_NEUTRAL_RELEASE_ID,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=_NEUTRAL_TIMEOUT,
        )
        self.assertTrue(
            any("called more than once" in v for v in violations), violations
        )

    def test_trips_when_stock_result_is_altered(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url="https://mirror/stock.jpg",
            token_present=True,
            network_images=None,
            wrapped_result="https://mirror/DIFFERENT.jpg",
            real_api_call_count=0,
            requested_release_id=None,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=None,
        )
        self.assertTrue(
            any("not returned unchanged" in v for v in violations), violations
        )

    def test_trips_when_real_api_called_despite_stock_hit(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url="https://mirror/stock.jpg",
            token_present=True,
            network_images=None,
            wrapped_result="https://mirror/stock.jpg",
            real_api_call_count=1,
            requested_release_id=_NEUTRAL_RELEASE_ID,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=_NEUTRAL_TIMEOUT,
        )
        self.assertTrue(
            any(
                "called even though the stock" in v for v in violations
            ),
            violations,
        )

    def test_trips_when_real_api_called_with_no_token(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=False,
            network_images=None,
            wrapped_result=None,
            real_api_call_count=1,
            requested_release_id=_NEUTRAL_RELEASE_ID,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=_NEUTRAL_TIMEOUT,
        )
        self.assertTrue(
            any(
                "called with no configured user token" in v
                for v in violations
            ),
            violations,
        )

    def test_trips_when_result_returned_with_no_token(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=False,
            network_images=None,
            wrapped_result="https://real/should-not-exist.jpg",
            real_api_call_count=0,
            requested_release_id=None,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=None,
        )
        self.assertTrue(
            any(
                "returned a URL with no configured token" in v
                for v in violations
            ),
            violations,
        )

    def test_trips_when_primary_image_is_not_returned(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=True,
            network_images=["https://real/first.jpg", "https://real/second.jpg"],
            wrapped_result="https://real/second.jpg",
            real_api_call_count=1,
            requested_release_id=_NEUTRAL_RELEASE_ID,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=_NEUTRAL_TIMEOUT,
        )
        self.assertTrue(
            any(
                "did not return the real API's primary image uri" in v
                for v in violations
            ),
            violations,
        )

    def test_trips_when_result_returned_from_unreachable_lookup(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=True,
            network_images=None,
            wrapped_result="https://real/should-not-exist.jpg",
            real_api_call_count=1,
            requested_release_id=_NEUTRAL_RELEASE_ID,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=_NEUTRAL_TIMEOUT,
        )
        self.assertTrue(
            any(
                "unreachable/malformed/empty" in v for v in violations
            ),
            violations,
        )

    def test_trips_when_real_api_requests_the_wrong_release_id(self) -> None:
        """#1200 review F2 -- a fallback that fetches a DIFFERENT release's
        artwork (a foreign/hardcoded id) is the exact strict-pressing-
        identity violation this fallback exists to eliminate."""
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=True,
            network_images=["https://real/a.jpg"],
            wrapped_result="https://real/a.jpg",
            real_api_call_count=1,
            requested_release_id=999999,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=_NEUTRAL_TIMEOUT,
        )
        self.assertTrue(
            any(
                "WRONG release id" in v for v in violations
            ),
            violations,
        )

    def test_trips_when_real_api_call_does_not_pin_the_timeout(self) -> None:
        """#1200 review F3 -- an unpinned request has no default timeout,
        so a black-holed api.discogs.com would hang the harness past the
        "never blocks an import" guarantee instead of failing soft."""
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=True,
            network_images=["https://real/a.jpg"],
            wrapped_result="https://real/a.jpg",
            real_api_call_count=1,
            requested_release_id=_NEUTRAL_RELEASE_ID,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=None,
        )
        self.assertTrue(
            any(
                "did not pin the fail-soft timeout" in v for v in violations
            ),
            violations,
        )

    def test_passes_on_a_clean_fallback_success(self) -> None:
        violations = cover_art_fallback_violations(
            stock_url=None,
            token_present=True,
            network_images=["https://real/first.jpg"],
            wrapped_result="https://real/first.jpg",
            real_api_call_count=1,
            requested_release_id=_NEUTRAL_RELEASE_ID,
            expected_release_id=_NEUTRAL_RELEASE_ID,
            requested_timeout=_NEUTRAL_TIMEOUT,
        )
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
