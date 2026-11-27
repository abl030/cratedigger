"""``pipeline-cli other-pressings`` relays the one route (issue #1445).

Runs the real command against the real route, service and MusicBrainz
client over the fake pipeline DB; only the HTTP leaf ``lib.mb_api._get`` is
replaced, answering from the real Razorlight capture.
"""

from __future__ import annotations

import argparse
import io
import json
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from scripts import pipeline_cli
from scripts.pipeline_cli.api_mutations import TcpApiEndpoint
from tests.helpers import make_request_row
from tests.other_pressings_fixtures import (
    RAZORLIGHT,
    load_fixture,
    mirror_get,
    validation_result,
)
from tests.web._harness import _FakeDbWebServerCase


class TestCmdOtherPressings(_FakeDbWebServerCase):
    def _seed(self) -> int:
        self.db.seed_request(make_request_row(id=970, status="imported"))
        return self.db.log_download(
            970, outcome="success",
            validation_result=validation_result(load_fixture(RAZORLIGHT)))

    def _run(
        self, log_id: int, *, json_out: bool = False,
        failure: BaseException | None = None,
    ) -> tuple[int, str]:
        rc, out, _ = self._run_with_stderr(
            log_id, json_out=json_out, failure=failure)
        return rc, out

    def _run_with_stderr(
        self, log_id: int, *, json_out: bool = False,
        failure: BaseException | None = None,
    ) -> tuple[int, str, str]:
        args = argparse.Namespace(
            download_log_id=log_id, json=json_out,
            api_endpoint=TcpApiEndpoint(self.base))
        out = io.StringIO()
        err = io.StringIO()
        side_effect = (
            failure if failure is not None
            else mirror_get(load_fixture(RAZORLIGHT)))
        with redirect_stdout(out), redirect_stderr(err), patch(
                "lib.mb_api._get", side_effect=side_effect):
            rc = pipeline_cli.cmd_other_pressings(None, args)
        return rc, out.getvalue(), err.getvalue()

    def test_prints_the_sentences_and_links(self) -> None:
        rc, out = self._run(self._seed())
        self.assertEqual(rc, 0)
        lines = out.splitlines()
        self.assertTrue(lines[1].startswith(
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

    def test_mirror_failure_exits_5_and_names_musicbrainz_on_stderr(
        self,
    ) -> None:
        rc, out, err = self._run_with_stderr(
            self._seed(), failure=urllib.error.URLError("refused"))
        self.assertEqual(rc, 5)
        self.assertIn("Could not reach MusicBrainz", out)
        self.assertEqual(json.loads(err)["error"], "musicbrainz_unavailable")

    def test_web_api_down_exits_5_and_names_the_api_on_stderr(self) -> None:
        args = argparse.Namespace(
            download_log_id=1, json=False,
            api_endpoint=TcpApiEndpoint("http://127.0.0.1:9"))
        out = io.StringIO()
        err = io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            rc = pipeline_cli.cmd_other_pressings(None, args)
        self.assertEqual(rc, 5)
        self.assertEqual(out.getvalue(), "")
        self.assertEqual(json.loads(err.getvalue())["error"], "api_unavailable")


if __name__ == "__main__":
    unittest.main()
