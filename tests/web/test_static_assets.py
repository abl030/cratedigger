"""Browser icon assets are served, not 404 noise (#161)."""
import unittest
from typing import ClassVar
from urllib.request import Request, urlopen

from tests.web._harness import _FakeDbWebServerCase
from web.request_security import BROWSER_CHANNEL, CHANNEL_HEADER


class TestStaticIconAssets(_FakeDbWebServerCase):
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


if __name__ == "__main__":
    unittest.main()
