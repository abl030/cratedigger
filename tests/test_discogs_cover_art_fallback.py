"""Real-Beets pins for the Discogs real-API cover-art fallback (issue #1200).

The Discogs mirror (``nix/beets.nix``) is built from CC0 XML dumps that carry
zero artwork, so upstream ``DiscogsPlugin.select_cover_art`` always returns
``None`` for a mirror-backed install and ``cover_art_url`` is never set.
Two same-titled David Bowie pressings (Discogs release 2823685, the 1969 UK
Philips ``SBL 7912``, vs. the 1967 Deram debut) then collided onto the same
iTunes title-guessed art.

Invariant under test (see ``tests/test_discogs_cover_art_fallback_generated.py``
for the generated property over this same contract):

1. **No-op for stock behaviour.** If the original (unpatched)
   ``select_cover_art`` returns a URL, that URL is returned unchanged and the
   real Discogs API is never called.
2. **One fallback lookup, exact release id.** Only when the original yields
   nothing does the patch perform exactly one authenticated lookup against
   the real API for the release id the ``Release`` object itself carries.
3. **Fails soft.** A missing token, a network error, a timeout, a non-2xx
   response, or a malformed/type-mismatched payload all yield ``None`` —
   never an exception into the import path.
"""

from __future__ import annotations

import ast
import inspect
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import confuse
import discogs_client
import msgspec
import requests
from beetsplug.discogs import DiscogsPlugin

from harness import beets_harness
from harness.beets_compat import (
    _DISCOGS_COVER_ART_TIMEOUT_SECONDS,
    DISCOGS_REAL_API_BASE,
    BeetsCapabilityError,
    _discogs_select_cover_art_method,
    _DiscogsApiCoverArtResponse,
    _real_discogs_cover_art_url,
    configure_discogs_cover_art_fallback,
)

_BOWIE_1969_RELEASE_ID = 2823685
_MIRROR_CLIENT = SimpleNamespace(_base_url="http://discogs-mirror.example")


def _fake_response(status_code: int, body: bytes) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body
    return response


def _config(token: str) -> confuse.Subview:
    """An isolated real confuse Subview shaped exactly like
    ``DiscogsPlugin.config`` (``beets.config["discogs"]``) — never the
    shared global config, so tests can't leak `user_token` across each
    other."""
    root = confuse.RootView(
        [confuse.ConfigSource.of({"discogs": {"user_token": token}})]
    )
    return root["discogs"]


def _release(release_id: int, images: list[dict[str, object]]) -> discogs_client.Release:
    return discogs_client.Release(
        _MIRROR_CLIENT, {"id": release_id, "images": images}
    )


class TestDiscogsCoverArtFallback(unittest.TestCase):
    def setUp(self) -> None:
        configure_discogs_cover_art_fallback()

    def test_older_discogs_plugin_without_select_cover_art_fails_closed(self) -> None:
        class LegacyDiscogsPlugin:
            pass

        with self.assertRaisesRegex(
            BeetsCapabilityError,
            "lacks callable select_cover_art",
        ):
            _discogs_select_cover_art_method(LegacyDiscogsPlugin)

    def test_stock_lookup_wins_and_real_api_is_never_called(self) -> None:
        plugin = object.__new__(DiscogsPlugin)
        plugin.config = _config("irrelevant-token")
        release = _release(
            111, [{"uri": "http://discogs-mirror.example/already-have-it.jpg"}]
        )

        with patch("requests.get") as mock_get:
            result = plugin.select_cover_art(release)

        self.assertEqual(
            result, "http://discogs-mirror.example/already-have-it.jpg"
        )
        mock_get.assert_not_called()

    def test_mirror_empty_images_falls_back_to_real_api_primary_image(self) -> None:
        """The exact live regression: mirror returns ``images: []`` for the
        1969 Bowie pressing; the real API's first image is the correct
        sleeve and must win instead of an iTunes title guess."""
        plugin = object.__new__(DiscogsPlugin)
        plugin.config = _config("real-token-123")
        release = _release(_BOWIE_1969_RELEASE_ID, [])

        payload = _fake_response(
            200,
            msgspec.json.encode(
                {
                    "images": [
                        {"uri": "https://api.discogs.com/philips-sbl-7912.jpg"},
                        {"uri": "https://api.discogs.com/other-angle.jpg"},
                    ]
                }
            ),
        )
        with patch("requests.get", return_value=payload) as mock_get:
            result = plugin.select_cover_art(release)

        self.assertEqual(
            result, "https://api.discogs.com/philips-sbl-7912.jpg"
        )
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertEqual(
            args[0], f"{DISCOGS_REAL_API_BASE}/releases/{_BOWIE_1969_RELEASE_ID}"
        )
        self.assertEqual(
            kwargs["headers"]["Authorization"], "Discogs token=real-token-123"
        )
        self.assertIn("User-Agent", kwargs["headers"])
        self.assertEqual(
            kwargs["timeout"], _DISCOGS_COVER_ART_TIMEOUT_SECONDS
        )

    def test_missing_token_never_calls_the_real_api(self) -> None:
        plugin = object.__new__(DiscogsPlugin)
        plugin.config = _config("")
        release = _release(_BOWIE_1969_RELEASE_ID, [])

        with patch("requests.get") as mock_get:
            result = plugin.select_cover_art(release)

        self.assertIsNone(result)
        mock_get.assert_not_called()

    def test_network_and_payload_failure_modes_fail_soft(self) -> None:
        """Every documented real-adapter failure mode returns ``None``,
        using the real exception classes ``requests``/``msgspec`` raise
        (test-fidelity Rule B) — never a synthetic stand-in."""
        release = _release(_BOWIE_1969_RELEASE_ID, [])
        cases = (
            (
                "connection error",
                requests.exceptions.ConnectionError("no route to host"),
            ),
            ("timeout", requests.exceptions.Timeout("timed out")),
        )
        for desc, exc in cases:
            with self.subTest(desc=desc):
                plugin = object.__new__(DiscogsPlugin)
                plugin.config = _config("tok")
                with patch("requests.get", side_effect=exc):
                    self.assertIsNone(plugin.select_cover_art(release))

        response_cases = (
            ("http 404", _fake_response(404, b"not found")),
            ("http 500", _fake_response(500, b"server error")),
            ("malformed json", _fake_response(200, b"{not json")),
            (
                "wrong type at msgspec boundary",
                _fake_response(200, b'{"images": [{"uri": 123}]}'),
            ),
            (
                "images is not a list",
                _fake_response(200, b'{"images": "nope"}'),
            ),
        )
        for desc, response in response_cases:
            with self.subTest(desc=desc):
                plugin = object.__new__(DiscogsPlugin)
                plugin.config = _config("tok")
                with patch("requests.get", return_value=response):
                    self.assertIsNone(plugin.select_cover_art(release))

    def test_crlf_token_never_leaks_into_the_fail_soft_log(self) -> None:
        """#1200 review N1 -- a confirmed secret-disclosure defect.

        A CR/LF inside the configured Discogs token makes ``requests``'
        own header validator raise ``InvalidHeader`` -- a
        ``RequestException``, so it IS caught by the fail-soft path --
        with the offending header VALUE (the token) embedded in its
        message. ``check_beets_config`` only tests ``token.strip()``
        nonempty, so a token carrying a CR/LF passes startup and would
        leak on every art miss if the log line ever interpolated the
        exception object. Drives the REAL, fully unmocked path (header
        validation raises client-side, before any network I/O -- no
        ``requests.get`` patch needed) rather than a synthetic stand-in.
        """
        token = "sUpErSeCrEtT0k3n\nX"
        release = _release(_BOWIE_1969_RELEASE_ID, [])
        plugin = object.__new__(DiscogsPlugin)
        plugin.config = _config(token)
        with self.assertLogs("harness.beets_compat", level="WARNING") as logs:
            result = plugin.select_cover_art(release)
        self.assertIsNone(result)
        self.assertEqual(len(logs.output), 1)
        self.assertNotIn(token, logs.output[0])
        self.assertNotIn("sUpErSeCrEtT0k3n", logs.output[0])
        self.assertIn(str(_BOWIE_1969_RELEASE_ID), logs.output[0])
        self.assertIn("InvalidHeader", logs.output[0])

    def test_429_fails_soft_but_logs_the_release_id_and_reason(self) -> None:
        """Residual from #1200 review: a rate-limited window otherwise
        fails soft to "no art" with nothing to diagnose from -- the WARNING
        log line is the only signal an operator gets."""
        release = _release(_BOWIE_1969_RELEASE_ID, [])
        plugin = object.__new__(DiscogsPlugin)
        plugin.config = _config("tok")
        with (
            patch(
                "requests.get",
                return_value=_fake_response(429, b"too many requests"),
            ),
            self.assertLogs("harness.beets_compat", level="WARNING") as logs,
        ):
            result = plugin.select_cover_art(release)
        self.assertIsNone(result)
        self.assertEqual(len(logs.output), 1)
        self.assertIn(str(_BOWIE_1969_RELEASE_ID), logs.output[0])
        self.assertIn("429", logs.output[0])

    def test_empty_real_api_images_list_returns_none(self) -> None:
        plugin = object.__new__(DiscogsPlugin)
        plugin.config = _config("tok")
        release = _release(_BOWIE_1969_RELEASE_ID, [])
        response = _fake_response(200, b'{"images": []}')

        with patch("requests.get", return_value=response):
            self.assertIsNone(plugin.select_cover_art(release))

    def test_missing_config_attribute_fails_soft_without_network_call(self) -> None:
        plugin = object.__new__(DiscogsPlugin)
        release = _release(_BOWIE_1969_RELEASE_ID, [])

        with patch("requests.get") as mock_get:
            self.assertIsNone(plugin.select_cover_art(release))
        mock_get.assert_not_called()

    def test_non_int_str_release_id_fails_soft_without_network_call(self) -> None:
        """Direct unit coverage on the leaf lookup: a ``Release`` always
        carries an ``id`` (the discogs_client constructor requires it), but
        the fallback's own release-id guard is exercised directly here since
        no real ``Release`` can be built without one."""
        with patch("requests.get") as mock_get:
            result = _real_discogs_cover_art_url(None, "tok")
        self.assertIsNone(result)
        mock_get.assert_not_called()

    def test_malformed_image_uri_type_raises_validation_error_at_boundary(
        self,
    ) -> None:
        """RED: the wrong type at the msgspec wire boundary must be caught
        as ``msgspec.ValidationError``, not silently coerced."""
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                {"images": [{"uri": 123}]}, type=_DiscogsApiCoverArtResponse
            )


class TestDiscogsCoverArtFallbackWiring(unittest.TestCase):
    """The patch is dead unless the harness installs it before plugin load."""

    def test_installed_in_run_protocol_before_plugins_load(self) -> None:
        source = inspect.getsource(beets_harness._run_protocol)
        tree = ast.parse(source)
        func = tree.body[0]
        self.assertIsInstance(func, ast.FunctionDef)

        # ast.unparse never fails here: every node walked came from
        # ast.parse'ing this function's own already-valid source.
        calls: list[tuple[int, str]] = sorted(
            (node.lineno, ast.unparse(node.func))
            for node in ast.walk(func)
            if isinstance(node, ast.Call)
        )

        fallback_lines = [
            line for line, name in calls
            if name.endswith("configure_discogs_cover_art_fallback")
        ]
        load_plugins_lines = [
            line for line, name in calls if name == "plugins.load_plugins"
        ]
        self.assertEqual(
            len(fallback_lines), 1,
            "configure_discogs_cover_art_fallback must be called exactly "
            f"once in _run_protocol, found calls: {calls}",
        )
        self.assertEqual(
            len(load_plugins_lines), 1,
            f"plugins.load_plugins call not found as expected: {calls}",
        )
        self.assertLess(
            fallback_lines[0], load_plugins_lines[0],
            "configure_discogs_cover_art_fallback must run before "
            "plugins.load_plugins -- a house convention keeping every "
            "harness class patch (this and its sibling "
            "configure_discogs_subtracks) grouped at the same one "
            "process-startup point. NOT a correctness requirement: "
            "self.select_cover_art is resolved on the class at CALL time "
            "(beetsplug/discogs/__init__.py's plain attribute access), not "
            "at DiscogsPlugin instantiation, so a patch installed on "
            "either side of load_plugins is already live before any real "
            "import request reaches it (issue #1200 review F6).",
        )

    def test_timeout_constant_stays_well_under_the_validation_watchdogs(
        self,
    ) -> None:
        """#1200 review N2 -- the F3 pins only proved the recorded
        ``timeout=`` kwarg EQUALS ``_DISCOGS_COVER_ART_TIMEOUT_SECONDS``
        itself; changing that constant (e.g. 10 -> 600) would leave every
        one of them green while genuinely breaking the "never blocks an
        import" guarantee. This is the missing absolute bound: the
        fallback's own timeout must stay well under BOTH the harness's
        120s validation watchdog (``lib/beets.py::_beets_validate_once``'s
        ``threading.Timer(120.0, _timeout_kill)``) and
        harness/import_one.py's 300s ``HARNESS_TIMEOUT`` -- otherwise a
        single black-holed api.discogs.com lookup can outlast the shorter
        watchdog and turn a soft art-miss into a hard validation
        failure."""
        self.assertLess(
            _DISCOGS_COVER_ART_TIMEOUT_SECONDS,
            120,
            "_DISCOGS_COVER_ART_TIMEOUT_SECONDS must stay well under "
            "lib/beets.py's 120s _beets_validate_once watchdog (and "
            "also harness/import_one.py's 300s HARNESS_TIMEOUT) or a "
            "black-holed Discogs lookup converts a soft art-miss into a "
            "hard validation failure",
        )


if __name__ == "__main__":
    unittest.main()
