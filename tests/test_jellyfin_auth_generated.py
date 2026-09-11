"""Generated contract: every Jellyfin request authenticates with the
``MediaBrowser`` scheme in ``Authorization`` (issue #1409).

Jellyfin 12 ships ``EnableLegacyAuthorization=false``: the ``X-Emby-Token``,
``X-MediaBrowser-Token`` and ``X-Emby-Authorization`` headers answer 401,
while ``Authorization: MediaBrowser …, Token="…"`` is accepted by every
supported line (10.11 and 12). The deterministic pins live in
``tests/test_util.py``; this property drives the three real urllib leaves
over token variation and judges the captured request with a port of the
server's own header parser (``AuthorizationContext.GetParts`` at v12.0:
split on unescaped commas, ``Trim('"')``, ``WebUtility.UrlDecode`` — which
also turns a raw ``+`` into a space).
"""

from __future__ import annotations

import re
import unittest
import urllib.request
from collections.abc import Mapping
from unittest.mock import MagicMock, patch
from urllib.parse import unquote_plus

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib import util
from lib.config import CratediggerConfig

LEGACY_HEADER_NAMES = frozenset(
    {"x-emby-token", "x-mediabrowser-token", "x-emby-authorization"})

# What the gist the server documents itself against requires: alphanumeric
# keys, every value double-quoted, no raw quote or comma inside a value
# (those are percent-encoded), pairs joined by a comma and a space.
_PARAMETER_GRAMMAR = re.compile(
    r'[A-Za-z0-9]+="[^",]*"(, [A-Za-z0-9]+="[^",]*")*')


def parse_media_browser_header(value: str) -> tuple[str, dict[str, str]]:
    """Scheme plus parameters, parsed the way Jellyfin 12.0 parses them.

    A port of ``AuthorizationContext.GetParts``: a quote toggles the escaped
    state, an unescaped comma ends a pair, ``=`` outside quotes ends a key,
    and each value is quote-trimmed then URL-decoded (``WebUtility.UrlDecode``
    decodes ``+`` as a space, hence ``unquote_plus``).
    """
    scheme, _, rest = value.partition(" ")
    parts: dict[str, str] = {}
    escaped = False
    start = 0
    key = ""
    for i, ch in enumerate(rest):
        if ch in ('"', ","):
            escaped = (not escaped) == (ch == '"')
            if ch == "," and not escaped:
                if start < i:
                    parts[key] = unquote_plus(rest[start:i].strip('"'))
                    key = ""
                start = i + 1
        elif not escaped and ch == "=":
            key = rest[start:i].strip()
            start = i + 1
    end = len(rest)
    if start < end:
        parts[key] = unquote_plus(rest[start:end].strip('"'))
    return scheme, parts


def jellyfin_authorization_violations(
    headers: Mapping[str, str], token: str,
) -> list[str]:
    """Every clause a captured request's headers can violate, accumulated.

    Clause 1: no legacy header name is sent. Clause 2: an ``Authorization``
    header is present (the remaining clauses read its value). Clause 3: its
    scheme is ``MediaBrowser``. Clause 4: the parameters follow the quoted,
    encoded ``key="value"`` grammar. Clause 5: the server-side parse of
    ``Token`` yields exactly the configured token.
    """
    violations: list[str] = []
    lowered = {name.lower(): value for name, value in headers.items()}
    legacy = sorted(LEGACY_HEADER_NAMES & lowered.keys())
    if legacy:
        violations.append(f"legacy authorization header sent: {legacy}")
    value = lowered.get("authorization")
    if value is None:
        violations.append("no Authorization header")
        return violations
    scheme, parameters = parse_media_browser_header(value)
    if scheme != "MediaBrowser":
        violations.append(f"scheme is {scheme!r}, not MediaBrowser")
    _, _, parameter_text = value.partition(" ")
    if not _PARAMETER_GRAMMAR.fullmatch(parameter_text):
        violations.append(
            "parameters are not quoted, encoded key=value pairs: "
            f"{parameter_text!r}")
    if parameters.get("Token") != token:
        violations.append(
            f"Token decodes to {parameters.get('Token')!r}, "
            "not the configured token")
    return violations


# Real API keys are 32 hex characters; the alphabet deliberately reaches past
# them into every byte the grammar reserves (quote, comma, equals, plus,
# percent, space) and a non-ASCII character, so the encoding is what keeps
# the parse honest, not the token's tameness.
_TOKEN = st.text(
    alphabet='abcdef0123456789-_ ",=+%~/:é',
    min_size=1,
    max_size=48,
).filter(lambda token: token == token.strip())
_LEAVES = st.sampled_from(("scan", "get", "post"))


def _cfg(token: str) -> CratediggerConfig:
    return CratediggerConfig(
        beets_directory="/music",
        jellyfin_url="https://jellyfin.example.test",
        jellyfin_token=token,
        jellyfin_path_map="/music:/jellyfin/music",
    )


def _captured_request(leaf: str, token: str) -> urllib.request.Request:
    """Drive one real leaf and return the ``Request`` it handed urllib."""
    response = MagicMock()
    response.__enter__ = lambda self: self
    response.__exit__ = MagicMock(return_value=False)
    response.read.return_value = b'{"Items": []}'
    response.status = 204
    cfg = _cfg(token)
    with patch("lib.util.urllib.request.urlopen", return_value=response) as urlopen:
        if leaf == "scan":
            util.trigger_jellyfin_scan(cfg, "/music/Artist/Album")
        elif leaf == "get":
            util._jellyfin_get_json(cfg, "/Items", searchTerm="x")
        else:
            util._jellyfin_post_json(cfg, "/Items/x", {"Id": "x"})
    (request,) = urlopen.call_args.args
    return request


class TestGeneratedJellyfinAuthorization(unittest.TestCase):
    @given(leaf=_LEAVES, token=_TOKEN)
    @example(leaf="scan", token='a b,"c=d+e')
    @example(leaf="get", token="deadbeefdeadbeefdeadbeefdeadbeef")
    @example(leaf="post", token="%2C,%22")
    def test_every_leaf_authenticates_with_the_media_browser_scheme(
        self, leaf: str, token: str,
    ) -> None:
        request = _captured_request(leaf, token)
        headers = dict(request.header_items())
        self.assertEqual(jellyfin_authorization_violations(headers, token), [])


_GOOD = (
    'MediaBrowser Client="Cratedigger", Device="cratedigger", '
    'DeviceId="cratedigger", Version="1", Token="abc"'
)


class TestInvariantCheckerTripsOnViolations(unittest.TestCase):
    """One minimal world per clause, each violating that clause alone."""

    def test_quiet_on_a_correct_request(self) -> None:
        self.assertEqual(
            jellyfin_authorization_violations({"Authorization": _GOOD}, "abc"), [])

    def test_legacy_header_clause(self) -> None:
        self.assertEqual(
            jellyfin_authorization_violations(
                {"Authorization": _GOOD, "X-emby-token": "abc"}, "abc"),
            ["legacy authorization header sent: ['x-emby-token']"])

    def test_missing_header_clause(self) -> None:
        self.assertEqual(
            jellyfin_authorization_violations(
                {"Content-type": "application/json"}, "abc"),
            ["no Authorization header"])

    def test_scheme_clause(self) -> None:
        self.assertEqual(
            jellyfin_authorization_violations(
                {"Authorization": _GOOD.replace("MediaBrowser", "Emby")}, "abc"),
            ["scheme is 'Emby', not MediaBrowser"])

    def test_grammar_clause(self) -> None:
        # The server tolerates an unquoted value; the documented contract
        # does not, so this trips the grammar clause and nothing else.
        self.assertEqual(
            jellyfin_authorization_violations(
                {"Authorization": "MediaBrowser Token=abc"}, "abc"),
            ["parameters are not quoted, encoded key=value pairs: 'Token=abc'"])

    def test_token_clause(self) -> None:
        # A raw '+' decodes to a space server-side: the token the server
        # would check is not the one configured.
        self.assertEqual(
            jellyfin_authorization_violations(
                {"Authorization": 'MediaBrowser Token="a+b"'}, "a+b"),
            ["Token decodes to 'a b', not the configured token"])


class TestServerParserPort(unittest.TestCase):
    """The port reproduces the parses the server is known to make."""

    def test_decodes_values_and_keeps_escaped_commas(self) -> None:
        self.assertEqual(
            parse_media_browser_header(
                'MediaBrowser Client="Jellyfin Web", Device="a,b", Token="x%2Cy+z"'),
            ("MediaBrowser",
             {"Client": "Jellyfin Web", "Device": "a,b", "Token": "x,y z"}))

    def test_unquoted_values_still_parse(self) -> None:
        self.assertEqual(
            parse_media_browser_header("Emby Token=abc, Client=x"),
            ("Emby", {"Token": "abc", "Client": "x"}))


if __name__ == "__main__":
    unittest.main()
