"""Generated application request-security boundary proof."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from web.request_security import (
    BROWSER_CHANNEL,
    CLI_CHANNEL,
    RequestSecurityError,
    authorize_request,
)


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


if __name__ == "__main__":
    unittest.main()
