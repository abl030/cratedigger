"""The static-file rule: the resolver, and what the real server serves."""
import unittest
from typing import ClassVar
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from tests.web._harness import _FakeDbWebServerCase
from web.request_security import BROWSER_CHANNEL, CHANNEL_HEADER
from web.static_assets import (
    ICON_ASSETS,
    JS_CONTENT_TYPE,
    WEB_ROOT,
    resolve_static_file,
)


class TestResolveStaticFile(unittest.TestCase):
    """The rule both servers ask, in isolation (#1390 residual 8)."""

    def test_a_js_module_resolves_into_web_js(self):
        resolved = resolve_static_file("/js/main.js")
        assert resolved is not None
        self.assertEqual(resolved.path, WEB_ROOT / "js" / "main.js")
        self.assertEqual(resolved.content_type, JS_CONTENT_TYPE)
        self.assertEqual(resolved.cache_control, "no-cache")

    def test_every_icon_resolves_into_web_assets_with_its_type(self):
        for url_path, (filename, content_type) in ICON_ASSETS.items():
            with self.subTest(url_path=url_path):
                resolved = resolve_static_file(url_path)
                assert resolved is not None
                self.assertEqual(
                    resolved.path, WEB_ROOT / "assets" / filename)
                self.assertEqual(resolved.content_type, content_type)
                self.assertEqual(
                    resolved.cache_control, "public, max-age=86400")
                self.assertTrue(resolved.path.is_file(), url_path)

    def test_nothing_outside_the_two_directories_resolves(self):
        # The first six are real files under web/ that the dev server used
        # to serve; the rest are near-misses on each half of the rule.
        for url_path in (
            "/server.py",
            "/classify.py",
            "/routes/pipeline.py",
            "/index.html",
            "/js/jsconfig.json",
            "/js/globals.d.ts",
            "/",
            "/js/",
            "/js",
            "/JS/main.js",
            "/api/pipeline/all",
            "/assets/favicon.ico",
            "/favicon.png",
        ):
            with self.subTest(url_path=url_path):
                self.assertIsNone(resolve_static_file(url_path))

    def test_a_traversing_js_path_stays_inside_web_js(self):
        # `..` survives the suffix test only when the whole path still ends
        # `.js`; basename then flattens it to a sibling that does not exist.
        resolved = resolve_static_file("/js/../server.js")
        assert resolved is not None
        self.assertEqual(resolved.path, WEB_ROOT / "js" / "server.js")
        self.assertFalse(resolved.path.exists())
        self.assertIsNone(resolve_static_file("/js/../server.py"))

    def test_resolution_does_not_imply_the_file_exists(self):
        resolved = resolve_static_file("/js/no-such-module.js")
        assert resolved is not None
        self.assertFalse(resolved.path.exists())


class TestStaticIconAssets(_FakeDbWebServerCase):
    """Browser icon assets are served, not 404 noise (#161)."""

    CASES: ClassVar = [
        ("/favicon.ico", "image/x-icon", b"\x00\x00\x01\x00"),
        ("/favicon-16x16.png", "image/png", b"\x89PNG"),
        ("/favicon-32x32.png", "image/png", b"\x89PNG"),
        ("/apple-touch-icon.png", "image/png", b"\x89PNG"),
    ]

    def test_icon_assets_serve_with_correct_type(self):
        for path, content_type, magic in self.CASES:
            with self.subTest(path=path):
                request = Request(
                    f"{self.base}{path}",
                    headers={CHANNEL_HEADER: BROWSER_CHANNEL},
                )
                with urlopen(request) as resp:
                    body = resp.read()
                    self.assertEqual(resp.status, 200)
                    self.assertEqual(
                        resp.headers["Content-Type"], content_type)
                    self.assertEqual(
                        int(resp.headers["Content-Length"]), len(body))
                self.assertTrue(body.startswith(magic),
                                f"{path} bytes don't match {magic!r}")


class TestStaticJsModules(_FakeDbWebServerCase):
    """The `/js/` half of the rule, through the real production handler."""

    def _status(self, path: str) -> int:
        request = Request(
            f"{self.base}{path}",
            headers={CHANNEL_HEADER: BROWSER_CHANNEL},
        )
        try:
            with urlopen(request) as resp:
                return resp.status
        except HTTPError as exc:
            with exc:
                exc.read()
            return exc.code

    def test_a_js_module_serves_its_bytes(self):
        request = Request(
            f"{self.base}/js/main.js",
            headers={CHANNEL_HEADER: BROWSER_CHANNEL},
        )
        with urlopen(request) as resp:
            body = resp.read()
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.headers["Content-Type"], JS_CONTENT_TYPE)
            self.assertEqual(resp.headers["Cache-Control"], "no-cache")
        self.assertEqual(body, (WEB_ROOT / "js" / "main.js").read_bytes())

    def test_repository_files_outside_the_rule_are_404(self):
        # Each of these names a real file under web/ (#1390 residual 8: the
        # dev server served all of them), so the 404 is the rule refusing a
        # readable file rather than a missing one.
        for path in (
            "/server.py",
            "/classify.py",
            "/routes/pipeline.py",
            "/index.html",
            "/js/../server.py",
            "/js/jsconfig.json",
            "/js/globals.d.ts",
        ):
            with self.subTest(path=path):
                self.assertTrue(
                    (WEB_ROOT / path.lstrip("/")).resolve().is_file(),
                    "this path must name a real file, or the 404 below "
                    "proves nothing about the rule",
                )
                self.assertEqual(self._status(path), 404)

    def test_a_js_path_with_no_file_behind_it_is_404(self):
        self.assertEqual(self._status("/js/no-such-module.js"), 404)


if __name__ == "__main__":
    unittest.main()
