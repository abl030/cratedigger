"""Generated application request-security boundary proof."""

from __future__ import annotations

import json
import unittest
from dataclasses import dataclass, replace
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.web._harness import _WebServerCase
from web.request_security import (
    BROWSER_CHANNEL,
    CHANNEL_HEADER,
    CLI_CHANNEL,
    RequestSecurityError,
    authorize_request,
)
from web.runtime import install_runtime, runtime


@dataclass(frozen=True)
class RequestWorld:
    method: str
    canonical_origin: str
    channels: tuple[str, ...]
    origins: tuple[str, ...]
    referers: tuple[str, ...]
    expected_allowed: bool


def assert_request_authorization(
    world: RequestWorld,
    actual_allowed: bool,
) -> None:
    """Checker kept separate so planted allow mutants prove it trips."""
    if actual_allowed != world.expected_allowed:
        raise AssertionError(
            "request authorization drifted: "
            f"{world=!r}, {actual_allowed=!r}"
        )


def assert_youtube_dispatch_blocked(
    *,
    status: int,
    resolver_calls: int,
    db_touches: int,
) -> None:
    """Reject worlds before either cache-writing resolver dependency."""
    if status != 403 or resolver_calls != 0 or db_touches != 0:
        raise AssertionError(
            "browser provenance rejection crossed the resolver boundary: "
            f"{status=}, {resolver_calls=}, {db_touches=}"
        )


_SAFE_METHODS = st.sampled_from(("GET", "HEAD", "OPTIONS"))
_UNSAFE_METHODS = st.sampled_from(
    ("POST", "PUT", "PATCH", "DELETE", "PURGE", "FUTURE")
)
_HOSTS = st.sampled_from(("music.example", "crate.example", "archive.example"))


@st.composite
def request_worlds(draw: st.DrawFn) -> RequestWorld:
    scheme = draw(st.sampled_from(("http", "https")))
    host = draw(_HOSTS)
    default_port = 80 if scheme == "http" else 443
    canonical_port = draw(
        st.sampled_from((None, default_port, 8443))
    )
    canonical_authority = (
        host
        if canonical_port is None
        else f"{host}:{canonical_port}"
    )
    canonical = f"{scheme}://{canonical_authority}"

    channel_case = draw(
        st.sampled_from(
            (
                "browser",
                "cli",
                "missing",
                "unknown",
                "duplicate",
                "case",
            )
        )
    )
    channels = {
        "browser": (BROWSER_CHANNEL,),
        "cli": (CLI_CHANNEL,),
        "missing": (),
        "unknown": ("trusted",),
        "duplicate": (BROWSER_CHANNEL, BROWSER_CHANNEL),
        "case": ("Browser",),
    }[channel_case]

    method = draw(st.one_of(_SAFE_METHODS, _UNSAFE_METHODS))
    signal_case = draw(
        st.sampled_from(
            (
                "missing",
                "origin",
                "referer",
                "both",
                "wrong_host",
                "wrong_scheme",
                "wrong_port",
                "userinfo",
                "null",
                "duplicate",
                "serialized_multiple",
                "serialized_referers",
                "conflict",
                "malformed",
                "empty_query_delimiter",
                "empty_fragment_delimiter",
            )
        )
    )

    # Equivalent spellings deliberately vary host case and default ports.
    exact_host = draw(st.sampled_from((host, host.upper())))
    if canonical_port in (None, default_port):
        exact_port = draw(st.sampled_from((None, default_port)))
    else:
        exact_port = canonical_port
    exact_authority = (
        exact_host if exact_port is None else f"{exact_host}:{exact_port}"
    )
    exact_origin = f"{scheme}://{exact_authority}"
    exact_referer = exact_origin + draw(
        st.sampled_from(("/", "/library", "/x?q=1", "/#tab"))
    )

    origins: tuple[str, ...] = ()
    referers: tuple[str, ...] = ()
    valid_signal = False
    if signal_case == "origin":
        origins = (exact_origin,)
        valid_signal = True
    elif signal_case == "referer":
        referers = (exact_referer,)
        valid_signal = True
    elif signal_case == "both":
        origins = (exact_origin,)
        referers = (exact_referer,)
        valid_signal = True
    elif signal_case == "wrong_host":
        origins = (f"{scheme}://evil.example",)
    elif signal_case == "wrong_scheme":
        other = "http" if scheme == "https" else "https"
        origins = (f"{other}://{canonical_authority}",)
    elif signal_case == "wrong_port":
        origins = (f"{scheme}://{host}:6553",)
    elif signal_case == "userinfo":
        origins = (f"{scheme}://operator@{canonical_authority}",)
    elif signal_case == "null":
        origins = ("null",)
    elif signal_case == "duplicate":
        origins = (exact_origin, exact_origin)
    elif signal_case == "serialized_multiple":
        origins = (f"{exact_origin} {scheme}://evil.example",)
    elif signal_case == "serialized_referers":
        referers = (
            f"{exact_origin}/path,{scheme}://evil.example/path",
        )
    elif signal_case == "conflict":
        origins = (exact_origin,)
        referers = (f"{scheme}://evil.example/path",)
    elif signal_case == "malformed":
        origins = (f"{scheme}://",)
    elif signal_case == "empty_query_delimiter":
        origins = (f"{exact_origin}?",)
    elif signal_case == "empty_fragment_delimiter":
        origins = (f"{exact_origin}#",)

    valid_channel = channel_case in ("browser", "cli")
    expected = (
        valid_channel
        and (
            channel_case == "cli"
            or method in ("GET", "HEAD", "OPTIONS")
            or valid_signal
        )
    )
    return RequestWorld(
        method=method,
        canonical_origin=canonical,
        channels=channels,
        origins=origins,
        referers=referers,
        expected_allowed=expected,
    )


class TestWebRequestSecurityGenerated(unittest.TestCase):
    @given(world=request_worlds())
    @example(
        world=RequestWorld(
            method="FUTURE",
            canonical_origin="https://music.example",
            channels=(BROWSER_CHANNEL,),
            origins=(),
            referers=(),
            expected_allowed=False,
        )
    )
    def test_real_authorizer_matches_every_generated_world(
        self,
        world: RequestWorld,
    ) -> None:
        try:
            authorize_request(
                method=world.method,
                channel_values=world.channels,
                origin_values=world.origins,
                referer_values=world.referers,
                canonical_origin=world.canonical_origin,
            )
        except RequestSecurityError:
            actual = False
        else:
            actual = True
        assert_request_authorization(world, actual)

    def test_checker_rejects_mismatched_origin_allow_mutant(self) -> None:
        world = RequestWorld(
            method="POST",
            canonical_origin="https://music.example",
            channels=(BROWSER_CHANNEL,),
            origins=("https://evil.example",),
            referers=(),
            expected_allowed=False,
        )
        with self.assertRaises(AssertionError):
            assert_request_authorization(world, True)

    def test_checker_rejects_missing_provenance_allow_mutant(self) -> None:
        world = RequestWorld(
            method="POST",
            canonical_origin="https://music.example",
            channels=(BROWSER_CHANNEL,),
            origins=(),
            referers=(),
            expected_allowed=False,
        )
        with self.assertRaises(AssertionError):
            assert_request_authorization(world, True)

    def test_checker_rejects_future_unsafe_method_allow_mutant(self) -> None:
        world = RequestWorld(
            method="FUTURE",
            canonical_origin="https://music.example",
            channels=(BROWSER_CHANNEL,),
            origins=(),
            referers=(),
            expected_allowed=False,
        )
        with self.assertRaises(AssertionError):
            assert_request_authorization(world, True)


_INVALID_BROWSER_PROVENANCE = st.sampled_from((
    (),
    (("Origin", "https://evil.example"),),
    (("Origin", "http://music.ablz.au"),),
    (("Origin", "https://music.ablz.au:444"),),
    (("Referer", "https://evil.example/path"),),
    (
        ("Origin", "https://music.ablz.au"),
        ("Referer", "https://evil.example/path"),
    ),
))


class TestYoutubeResolverProvenanceGenerated(_WebServerCase):
    @given(provenance=_INVALID_BROWSER_PROVENANCE)
    @example(provenance=())
    @example(provenance=(("Origin", "https://evil.example"),))
    def test_invalid_browser_provenance_never_reaches_resolver_or_db(
        self,
        provenance: tuple[tuple[str, str], ...],
    ) -> None:
        class _ForbiddenDB:
            touches = 0

            def __getattribute__(self, name: str) -> object:
                if name == "touches":
                    return object.__getattribute__(self, name)
                object.__setattr__(
                    self,
                    "touches",
                    object.__getattribute__(self, "touches") + 1,
                )
                raise AssertionError(
                    f"rejected resolver request touched DB attribute {name}"
                )

        forbidden_db = _ForbiddenDB()
        headers = {
            CHANNEL_HEADER: BROWSER_CHANNEL,
            "Content-Type": "application/json",
            **dict(provenance),
        }
        request = Request(
            f"{self.base}/api/youtube-album",
            data=json.dumps({
                "identifier": "release-id",
                "refresh": False,
            }).encode(),
            headers=headers,
            method="POST",
        )
        with install_runtime(
            replace(runtime(), shared_db=forbidden_db),
        ), patch(
            "web.routes.youtube.resolve_youtube_album",
        ) as resolver:
            try:
                with urlopen(request, timeout=5) as response:
                    status = response.status
                    response.read()
            except HTTPError as exc:
                with exc:
                    status = exc.code
                    exc.read()
        assert_youtube_dispatch_blocked(
            status=status,
            resolver_calls=resolver.call_count,
            db_touches=forbidden_db.touches,
        )

    def test_dispatch_checker_rejects_resolver_call_mutant(self) -> None:
        with self.assertRaises(AssertionError):
            assert_youtube_dispatch_blocked(
                status=403,
                resolver_calls=1,
                db_touches=0,
            )

    def test_dispatch_checker_rejects_db_touch_mutant(self) -> None:
        with self.assertRaises(AssertionError):
            assert_youtube_dispatch_blocked(
                status=403,
                resolver_calls=0,
                db_touches=1,
            )


if __name__ == "__main__":
    unittest.main()
