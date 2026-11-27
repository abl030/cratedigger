"""``pipeline-cli other-pressings`` relays the one route (issue #1445).

Runs the real command against the real route and service over the fake
pipeline DB; the two MusicBrainz reads serve the Razorlight capture.
"""

from __future__ import annotations

import argparse
import io
import json
import unittest
import urllib.error
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import pipeline_cli
from scripts.pipeline_cli.api_mutations import TcpApiEndpoint
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase
from tests.web.test_routes_other_pressings import (
    _fixture,
    _Mirror,
    _validation_result,
)


class TestCmdOtherPressings(_FakeDbWebServerCase):
    def _seed(self) -> int:
        self.db.seed_request(make_request_row(id=970, status="imported"))
        return self.db.log_download(
            970, outcome="success",
            validation_result=_validation_result(_fixture(28366)))

    def _run(self, log_id: int, *, json_out: bool = False) -> tuple[int, str]:
        mirror = _Mirror(_fixture(28366))
        args = argparse.Namespace(
            download_log_id=log_id, json=json_out,
            api_endpoint=TcpApiEndpoint(self.base))
        out = io.StringIO()
        with redirect_stdout(out), \
                patch("lib.mb_api.get_release_with_discids",
                      side_effect=mirror.release), \
                patch("lib.mb_api.browse_release_group_with_discids",
                      side_effect=mirror.page):
            rc = pipeline_cli.cmd_other_pressings(None, args)
        return rc, out.getvalue()

    def test_prints_the_sentences_and_links(self) -> None:
        rc, out = self._run(self._seed())
        self.assertEqual(rc, 0)
        lines = out.splitlines()
        self.assertTrue(lines[0].startswith(
            "This copy matches 2 other pressings"))
        self.assertIn(
            "Both have 'To the Sea' and 'Fall, Fall, Fall' where this "
            "release lists 'Hang By, Hang By' and 'To the Sea'.", lines)
        self.assertIn(
            "  matching pressing: UK CD, 2004 https://musicbrainz.org/"
            "release/d63463aa-2636-3c34-a919-f13a6f7a5e2d", lines)

    def test_json_prints_the_typed_result(self) -> None:
        rc, out = self._run(self._seed(), json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "checked")
        self.assertEqual(len(payload["matching_pressings"]), 2)

    def test_unknown_row_exits_2(self) -> None:
        rc, out = self._run(424242)
        self.assertEqual(rc, 2)
        self.assertIn("This attempt could not be found.", out)

    def test_mirror_failure_exits_5_with_the_sentence(self) -> None:
        log_id = self._seed()
        args = argparse.Namespace(
            download_log_id=log_id, json=False,
            api_endpoint=TcpApiEndpoint(self.base))
        out = io.StringIO()
        with redirect_stdout(out), patch(
                "lib.mb_api.get_release_with_discids",
                side_effect=urllib.error.URLError("refused")):
            rc = pipeline_cli.cmd_other_pressings(None, args)
        self.assertEqual(rc, 5)
        self.assertIn("Could not reach MusicBrainz", out.getvalue())


if __name__ == "__main__":
    unittest.main()
