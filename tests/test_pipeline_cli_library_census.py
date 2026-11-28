"""``pipeline-cli library-census`` relays the one route (issue #1472).

Runs the real command against the real route over the fake pipeline DB and
a real published snapshot.
"""

from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace

from lib.library_completeness import (
    CompletenessAlbum,
    CompletenessCounts,
    CompletenessFinding,
    CompletenessReport,
)
from lib.library_completeness_snapshot import (
    LibraryCompletenessSnapshot,
    library_completeness_snapshot_path,
    write_library_completeness_snapshot,
)
from scripts import pipeline_cli
from scripts.pipeline_cli.api_mutations import TcpApiEndpoint
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase
from web.runtime import install_runtime, runtime

SNAPSHOT = LibraryCompletenessSnapshot(
    "2026-11-28T00:00:00+00:00", 2.0,
    CompletenessReport("complete", CompletenessCounts(
        3, 3, 0, 0, 0, track_length=1, track_length_harmless=1,
        track_length_measured=2, track_length_pending=1, track_length_unlisted=0,
    ), (
        CompletenessAlbum(
            501, "Booka Shade", "Movements", "rel-booka",
            (CompletenessFinding(
                "track_length", "ending missing? (track 14)",
                sentence="Last track runs 4:23 but the release lists 6:18."),),
            14, 14, 14),
        CompletenessAlbum(
            502, "Artist", "Hidden", "rel-hidden",
            (CompletenessFinding(
                "track_length", "hidden track? (track 9)",
                sentence="Probably fine.", probably_harmless=True),),
            9, 9, 9),
    )),
)


class TestCmdLibraryCensus(_FakeDbWebServerCase):
    def _run(self, *, category: str | None, json_out: bool = False) -> tuple[int, str]:
        self.db.seed_request(make_request_row(
            id=3264, status="imported", mb_release_id="rel-booka"))
        args = argparse.Namespace(
            category=category, json=json_out, api_endpoint=TcpApiEndpoint(self.base))
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = library_completeness_snapshot_path(tmpdir)
            write_library_completeness_snapshot(path, SNAPSHOT)
            with install_runtime(replace(runtime(), library_completeness_snapshot_path=path)), \
                    redirect_stdout(out):
                rc = pipeline_cli.cmd_library_census(None, args)
        return rc, out.getvalue()

    def test_track_length_worklist_names_the_request_and_sentence(self) -> None:
        rc, out = self._run(category="track_length")
        self.assertEqual(rc, 0)
        self.assertIn("1 waiting", out)
        self.assertIn("#501 Booka Shade — Movements (request 3264)", out)
        self.assertIn("track_length: Last track runs 4:23 but the release lists 6:18.", out)
        self.assertNotIn("Hidden", out)

    def test_json_carries_the_route_payload(self) -> None:
        rc, out = self._run(category=None, json_out=True)
        self.assertEqual(rc, 0)
        albums = json.loads(out)["snapshot"]["report"]["albums"]
        self.assertEqual([album["album_id"] for album in albums], [501, 502])


if __name__ == "__main__":
    unittest.main()
