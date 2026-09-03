"""Tests for scripts/pipeline_cli.py — Pipeline CLI commands."""

import argparse
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import msgspec

# Bootstrap ephemeral PostgreSQL if available
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from typing import TYPE_CHECKING, Any, NoReturn, cast

if TYPE_CHECKING:
    from lib.pipeline_db import DownloadLogWithEvidenceRow

import scripts.pipeline_cli.album_requests as pipeline_cli_album_requests
import scripts.pipeline_cli.long_tail as pipeline_cli_long_tail
import scripts.pipeline_cli.wrong_match as pipeline_cli_wrong_match
from scripts import pipeline_cli
from scripts.pipeline_cli import api_mutations
from scripts.pipeline_cli.api_mutations import TcpApiEndpoint
from tests.dispatch_helpers import handoff_automation_owner
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    REQUEST_CASCADE_RESET_TABLES,
    delete_all_rows,
    make_request_row,
    seed_visible_wrong_match,
)
from tests.test_beets_db import _create_test_db, _insert_album
from tests.web._harness import _FakeDbWebServerCase, _fresh_triage_runner

TEST_DSN = os.environ.get("TEST_DB_DSN")

RELEASE_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
RELEASE_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
RELEASE_C = "cccccccc-cccc-cccc-cccc-cccccccccccc"
DISCOGS_RELEASE = "12856590"
UNAVAILABLE_ERROR = {
    "category": "unavailable",
    "error": "long_tail_authority_unavailable",
    "message": "Current Beets authority is unavailable; retry later.",
}
CONFLICT_ERROR = {
    "category": "conflict",
    "error": "long_tail_authority_conflict",
    "message": "Long-tail exact release authority is ambiguous or invalid.",
}

SAMPLE_MB_RELEASE = {
    "id": "44438bf9-26d9-4460-9b4f-1a1b015e37a1",
    "title": "Riposte",
    "date": "2014-05-06",
    "country": "US",
    "release-group": {"id": "rg-uuid"},
    "artist-credit": [{
        "name": "Buke and Gase",
        "artist": {"id": "artist-uuid", "name": "Buke and Gase"},
    }],
    "media": [{
        "position": 1,
        "tracks": [
            {"position": 1, "title": "Houdini Crush", "length": 200000},
            {"position": 2, "title": "Hiccup", "length": 180000},
            {"position": 3, "title": "Metazoa", "length": 220000},
        ],
    }],
}


def make_db():
    from lib.pipeline_db import PipelineDB
    db = PipelineDB(TEST_DSN)
    delete_all_rows(db, REQUEST_CASCADE_RESET_TABLES)
    return db


class TestMbApiBase(unittest.TestCase):
    """KTD6: pipeline-cli's MB lookups read [MusicBrainz] api_base from the
    runtime config instead of carrying a second hardcoded mirror URL."""

    def test_mb_api_reads_runtime_config(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "config.ini")
            with open(path, "w", encoding="utf-8") as f:
                f.write("[MusicBrainz]\napi_base = http://mb-mirror.test:5200\n")
            with patch.dict(os.environ,
                            {"CRATEDIGGER_RUNTIME_CONFIG": path},
                            clear=False):
                self.assertEqual(pipeline_cli_album_requests._mb_api(),
                                 "http://mb-mirror.test:5200/ws/2")

    def test_mb_api_defaults_to_public(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "config.ini")
            with open(path, "w", encoding="utf-8") as f:
                f.write("[Slskd]\nhost_url = http://x\n")
            with patch.dict(os.environ,
                            {"CRATEDIGGER_RUNTIME_CONFIG": path},
                            clear=False):
                self.assertEqual(pipeline_cli_album_requests._mb_api(),
                                 "https://musicbrainz.org/ws/2")


class TestCmdAdd(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    @patch("web.mb.get_release", return_value={
        "release_group_id": "rg-uuid", "tracks": [], "labels": [],
    })
    @patch("web.mb.get_release_group_year", return_value=2014)
    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_add_with_mbid(self, mock_fetch, _mock_rgy, _mock_get_release):
        mock_fetch.return_value = SAMPLE_MB_RELEASE
        args = MagicMock(mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request")
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id("44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        assert req is not None
        self.assertEqual(req["artist_name"], "Buke and Gase")
        self.assertEqual(req["album_title"], "Riposte")
        self.assertEqual(req["year"], 2014)
        self.assertEqual(req["source"], "request")

        tracks = self.db.get_tracks(req["id"])
        self.assertEqual(len(tracks), 3)

    @patch("web.mb.get_release", return_value={
        "release_group_id": "rg-uuid", "tracks": [], "labels": [],
    })
    @patch("web.mb.get_release_group_year", return_value=2014)
    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_add_with_mbid_creates_active_search_plan(
        self, mock_fetch, _mock_rgy, _mock_get_release,
    ):
        """Plan generation runs after `set_tracks()` on the CLI add path."""
        mock_fetch.return_value = SAMPLE_MB_RELEASE
        args = MagicMock(mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request")
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id("44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        assert req is not None
        active = self.db.get_active_search_plan(req["id"])
        assert active is not None
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        self.assertEqual(active.plan.generator_id, SEARCH_PLAN_GENERATOR_ID)
        self.assertEqual(active.next_ordinal, 0)
        self.assertGreater(len(active.items), 0)

    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_add_duplicate_skipped(self, mock_fetch):
        self.db.add_request(
            mb_release_id="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
            artist_name="A", album_title="B", source="request",
        )
        args = MagicMock(mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request")
        pipeline_cli.cmd_add(self.db, args)
        mock_fetch.assert_not_called()

    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_mb_preflight_race_reports_authoritative_existing_status(
        self, mock_fetch,
    ):
        mock_fetch.return_value = SAMPLE_MB_RELEASE
        db = FakePipelineDB()
        db.arm_request_creation_race(
            SAMPLE_MB_RELEASE["id"], status="imported",
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = pipeline_cli.cmd_add(
                db, MagicMock(mbid=SAMPLE_MB_RELEASE["id"], source="request"),
            )

        self.assertIsNone(rc)
        self.assertIn("Already in DB:", out.getvalue())
        self.assertIn("status=imported", out.getvalue())

    @patch("web.discogs.get_release")
    def test_discogs_preflight_race_reports_authoritative_existing_status(
        self, mock_release,
    ):
        mock_release.return_value = {
            "artist_id": "1", "artist_name": "Race", "title": "Discogs",
            "tracks": [],
        }

        db = FakePipelineDB()
        db.arm_request_creation_race(
            "7919", status="unsearchable", discogs=True,
        )
        out = io.StringIO()
        with redirect_stdout(out):
            rc = pipeline_cli.cmd_add(db, MagicMock(mbid="7919", source="request"))

        self.assertIsNone(rc)
        self.assertIn("status=unsearchable", out.getvalue())

    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_in_lock_exists_with_disappeared_row_is_retryable_exit_4(
        self, mock_fetch,
    ):
        mock_fetch.return_value = SAMPLE_MB_RELEASE
        db = FakePipelineDB()
        db.arm_request_creation_race(
            SAMPLE_MB_RELEASE["id"],
            status="imported",
            disappear_after_in_lock_lookup=True,
        )
        err = io.StringIO()
        with redirect_stderr(err):
            rc = pipeline_cli.cmd_add(
                db, MagicMock(mbid=SAMPLE_MB_RELEASE["id"], source="request"),
            )

        self.assertEqual(rc, 4)
        self.assertIn("disappeared", err.getvalue())

    @patch("web.mb.get_release")
    @patch("web.mb.get_release_group_year")
    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_add_with_mbid_persists_release_group_year_reissue(
        self, mock_fetch, mock_get_rgy, mock_get_release,
    ):
        """U4: reissue MB release → release_group_year populated and
        differs from the per-release year. The CLI add path now routes
        through ``field_resolver_service.resolve_all``, which by default
        dispatches to ``web.mb.get_release_group_year`` for MB UUIDs."""
        mock_fetch.return_value = SAMPLE_MB_RELEASE  # date=2014, rg=rg-uuid
        mock_get_rgy.return_value = 2008
        mock_get_release.return_value = {
            "release_group_id": "rg-uuid",
            "tracks": [],
            "labels": [],
        }
        args = MagicMock(
            mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request",
        )
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id(
            "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        assert req is not None
        self.assertEqual(req["year"], 2014)
        self.assertEqual(req["release_group_year"], 2008)
        mock_get_rgy.assert_called_once_with("rg-uuid")

    @patch("web.mb.get_release")
    @patch("web.mb.get_release_group_year")
    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_add_with_mbid_persists_release_group_year_original(
        self, mock_fetch, mock_get_rgy, mock_get_release,
    ):
        """U4: original release MB release → release_group_year matches
        the per-release year."""
        mock_fetch.return_value = SAMPLE_MB_RELEASE  # date=2014
        mock_get_rgy.return_value = 2014
        mock_get_release.return_value = {
            "release_group_id": "rg-uuid",
            "tracks": [],
            "labels": [],
        }
        args = MagicMock(
            mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request",
        )
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id(
            "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        assert req is not None
        self.assertEqual(req["year"], 2014)
        self.assertEqual(req["release_group_year"], 2014)

    @patch("web.mb.get_release")
    @patch("web.mb.get_release_group_year")
    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_add_with_mbid_release_group_404_leaves_column_null(
        self, mock_fetch, mock_get_rgy, mock_get_release,
    ):
        """U4: 404 / missing release-group → ``release_group_year`` is
        NULL on the new row, no error raised. ``web.mb.get_release_group_year``
        returns None for both 404 and unparseable dates; the resolver
        maps that to ``unresolved_field_missing_upstream``."""
        mock_fetch.return_value = SAMPLE_MB_RELEASE
        mock_get_rgy.return_value = None
        mock_get_release.return_value = {
            "release_group_id": "rg-uuid",
            "tracks": [],
            "labels": [],
        }
        args = MagicMock(
            mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request",
        )
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id(
            "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        assert req is not None
        self.assertEqual(req["year"], 2014)
        self.assertIsNone(req["release_group_year"])

    @patch("web.mb.get_release", return_value={
        "release_group_id": "rg-uuid",
        "tracks": [], "labels": [],
        # Rule 2 (tightened post-#373): release-group is typed as
        # Compilation AND per-track artist credits diverge from the
        # album-level credit (a real VA shape, not a greatest-hits).
        "release-group": {"primary-type": "Compilation"},
    })
    @patch("web.mb.get_release_group_year", return_value=2010)
    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_add_with_mbid_va_compilation_flag_set(
        self, mock_fetch, _mock_rgy, _mock_release,
    ):
        """U4 CLI happy path for VA: a release-group typed as
        Compilation with diverging per-track artist credits flips
        ``is_va_compilation=True`` at enqueue. The diverging credits are
        required post-#373 — a Compilation rg whose tracks all share the
        album artist is a greatest-hits / single-artist comp and stays
        False (so the VA strategy mix doesn't replace its
        default/literal queries)."""
        sample = dict(SAMPLE_MB_RELEASE)
        sample["release-group"] = {
            "id": "rg-uuid", "primary-type": "Compilation",
        }
        # Diverging per-track credits: each track is by a different
        # artist. This is the real-VA shape Rule 2 was designed to
        # catch.
        sample["media"] = [{
            "position": 1,
            "tracks": [
                {"position": 1, "title": "Houdini Crush",
                 "length": 200000,
                 "artist-credit": [{"name": "Artist A"}]},
                {"position": 2, "title": "Hiccup",
                 "length": 180000,
                 "artist-credit": [{"name": "Artist B"}]},
                {"position": 3, "title": "Metazoa",
                 "length": 220000,
                 "artist-credit": [{"name": "Artist C"}]},
            ],
        }]
        mock_fetch.return_value = sample
        args = MagicMock(
            mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request",
        )
        pipeline_cli.cmd_add(self.db, args)

        req = self.db.get_request_by_mb_release_id(
            "44438bf9-26d9-4460-9b4f-1a1b015e37a1")
        assert req is not None
        self.assertTrue(req["is_va_compilation"])
        # PR2 Apply #2: the add path must thread the resolver's VA
        # verdict into ``generate_for_new_request``, so the freshly-
        # added VA request lands with a plan generated by
        # ``_generate_va_plan`` — not ``_generate_normal_plan``. The
        # discriminator is the presence of ``va_track_artist_*`` slots
        # (the heart of the VA mix).
        active = self.db.get_active_search_plan(req["id"])
        assert active is not None
        strategies = [item.strategy for item in active.items]
        self.assertTrue(
            any(s.startswith("va_track_artist_") for s in strategies),
            f"VA add path must emit va_track_artist_* slot; got "
            f"{strategies}",
        )


class TestCmdAddPlanGenerationFakeDB(unittest.TestCase):
    """Fake-backed tests for the plan-generation seam on the CLI add path.

    These run without TEST_DB_DSN so the CLI/web parity contract is
    enforced even on environments where the ephemeral PG isn't bootstrapped.
    """

    @patch("web.mb.get_release", return_value={
        "release_group_id": "rg-uuid", "tracks": [], "labels": [],
    })
    @patch("web.mb.get_release_group_year", return_value=2014)
    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_cli_add_calls_search_plan_service(
        self, mock_fetch, _mock_rgy, _mock_get_release,
    ):
        from tests.fakes import FakePipelineDB
        mock_fetch.return_value = SAMPLE_MB_RELEASE

        db = FakePipelineDB()
        args = MagicMock(
            mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request",
        )
        pipeline_cli.cmd_add(db, args)
        # FakePipelineDB.add_request increments id; first add → id=1.
        active = db.get_active_search_plan(1)
        self.assertIsNotNone(active)
        assert active is not None
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        self.assertEqual(active.plan.generator_id, SEARCH_PLAN_GENERATOR_ID)

    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_cli_add_duplicate_does_not_regenerate(self, mock_fetch):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        # Pre-seed a duplicate request with the same release id.
        db.add_request(
            mb_release_id="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
            artist_name="A", album_title="B", source="request",
        )
        before_plan_count = len(db.search_plans)
        args = MagicMock(
            mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request",
        )
        pipeline_cli.cmd_add(db, args)
        mock_fetch.assert_not_called()
        # No new plan rows for the duplicate path.
        self.assertEqual(len(db.search_plans), before_plan_count)

    @patch("scripts.pipeline_cli.album_requests.fetch_mb_release")
    def test_cli_add_replace_during_resolution_skips_plan(self, mock_fetch):
        class RacingDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.raced = False

            def update_request_fields(
                self,
                request_id: int,
                *,
                expected_status: str | None = None,
                **fields: object,
            ) -> bool:
                if not self.raced:
                    self.raced = True
                    self.supersede_request_mbid(
                        request_id,
                        new_mb_release_id="cli-add-race-descendant",
                        new_mb_release_group_id=None,
                        new_mb_artist_id=None,
                        new_artist_name="Buke and Gase",
                        new_album_title="Riposte (correct pressing)",
                        new_year=None,
                        new_country=None,
                        new_tracks=[],
                    )
                return super().update_request_fields(
                    request_id,
                    expected_status=expected_status,
                    **fields,
                )

        release = json.loads(json.dumps(SAMPLE_MB_RELEASE))
        for track in release["media"][0]["tracks"]:
            track["artist-credit"] = [{"name": "Late Artist"}]
        mock_fetch.return_value = release
        db = RacingDB()
        stderr = io.StringIO()
        with patch(
            "web.mb.get_release_group_year",
            return_value=2014,
        ), redirect_stderr(stderr):
            exit_code = pipeline_cli.cmd_add(db, MagicMock(
                mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
                source="request",
            ))

        source = db.get_request_by_release_id(
            "44438bf9-26d9-4460-9b4f-1a1b015e37a1",
        )
        assert source is not None
        self.assertEqual(source["status"], "replaced")
        self.assertIsNone(db.get_tracks(source["id"])[0]["track_artist"])
        self.assertIsNone(db.get_active_search_plan(source["id"]))
        self.assertIn("Initialization failed", stderr.getvalue())
        self.assertEqual(exit_code, 4)


class TestCmdList(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_list_by_status(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="b", artist_name="C", album_title="D", source="request")
        self.db.update_status(id2, "imported")

        args = MagicMock(filter_status="wanted", search=None)
        pipeline_cli.cmd_list(self.db, args)

    def test_list_all(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        args = MagicMock(filter_status=None, search=None)
        pipeline_cli.cmd_list(self.db, args)

    def test_list_search_mirrors_api_search(self):
        """CLI ⇄ API symmetry (#426): ``list --search`` wraps the same
        ``search_requests`` the web search endpoint uses."""
        import contextlib
        import io

        self.db.add_request(
            mb_release_id="s-1", artist_name="The Mountain Goats",
            album_title="Tallahassee", source="request")
        self.db.add_request(
            mb_release_id="s-2", artist_name="Other",
            album_title="Album", source="request")

        out = io.StringIO()
        args = MagicMock(filter_status=None, search="mountain")
        with contextlib.redirect_stdout(out):
            pipeline_cli.cmd_list(self.db, args)
        text = out.getvalue()
        self.assertIn("The Mountain Goats", text)
        self.assertNotIn("Other", text)

    def test_list_search_with_status_filter(self):
        rid = self.db.add_request(
            mb_release_id="s-3", artist_name="Goat", album_title="One",
            source="request")
        self.db.add_request(
            mb_release_id="s-4", artist_name="Goat", album_title="Two",
            source="request")
        self.db.update_status(rid, "imported")

        import contextlib
        import io
        out = io.StringIO()
        args = MagicMock(filter_status="imported", search="goat")
        with contextlib.redirect_stdout(out):
            pipeline_cli.cmd_list(self.db, args)
        text = out.getvalue()
        self.assertIn("One", text)
        self.assertNotIn("Two", text)


class TestCmdSet(unittest.TestCase):
    def test_initializing_request_cannot_be_published_by_generic_set(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=791, status="initializing"))

        rc = pipeline_cli.cmd_set(db, MagicMock(id=791, status="wanted"))

        self.assertEqual(rc, 4)
        self.assertEqual(db.request(791)["status"], "initializing")
    def test_same_status_is_idempotent_for_operator_statuses(self):
        for index, status in enumerate(("wanted", "imported", "unsearchable"), 1):
            with self.subTest(status=status):
                db = FakePipelineDB()
                db.seed_request(make_request_row(id=index, status=status))
                before = db.get_request(index)

                rc = pipeline_cli.cmd_set(
                    db,
                    MagicMock(id=index, status=status),
                )

                self.assertEqual(rc, 0)
                self.assertEqual(db.get_request(index), before)

    def test_imported_to_unsearchable_is_rejected(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=8, status="imported"))

        rc = pipeline_cli.cmd_set(
            db,
            MagicMock(id=8, status="unsearchable"),
        )

        self.assertEqual(rc, 4)
        self.assertEqual(db.request(8)["status"], "imported")

    def test_vanished_row_conflict_exits_2(self):
        """A row deleted mid-command classifies not_found: the route twin
        answers 404, so cmd_set exits 2 (#1278 review F2 — it used to
        blanket-exit 4 for every conflict kind)."""
        class VanishingDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self._reads = 0

            def get_request(self, request_id: int):
                row = super().get_request(request_id)
                self._reads += 1
                if self._reads == 1:
                    self._requests.pop(request_id, None)
                return row

        db = VanishingDB()
        db.seed_request(make_request_row(id=12, status="unsearchable"))
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_set(db, MagicMock(id=12, status="wanted"))
        self.assertEqual(rc, 2)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["reason"], "not_found")

    def test_processing_status_change_reports_exact_owner_exit_4(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=10,
            status="wanted",
            artist_name="A",
            album_title="B",
        ))
        owner = handoff_automation_owner(db, 10)
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_set(
                db,
                MagicMock(id=10, status="unsearchable"),
            )

        self.assertEqual(rc, 4)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["processing_owner"], {
            "job_id": owner.id,
            "status": owner.status,
            "preview_status": owner.preview_status,
        })
        self.assertEqual(db.request(10)["status"], "processing")

    @patch("builtins.print")
    @patch("scripts.pipeline_cli.album_requests.finalize_request")
    def test_set_routes_dynamic_status_through_shared_finalizer(
        self,
        mock_finalize,
        _mock_print,
    ):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=9,
            status="unsearchable",
            artist_name="A",
            album_title="B",
        ))

        args = MagicMock(id=9, status="imported")
        pipeline_cli.cmd_set(db, args)

        called_db, request_id, transition = mock_finalize.call_args.args
        self.assertIs(called_db, db)
        self.assertEqual(request_id, 9)
        self.assertEqual(transition.target_status, "imported")
        self.assertEqual(transition.from_status, "unsearchable")


class TestTracksFromMbRelease(unittest.TestCase):
    def test_extract_tracks(self):
        tracks = pipeline_cli.tracks_from_mb_release(SAMPLE_MB_RELEASE)
        self.assertEqual(len(tracks), 3)
        self.assertEqual(tracks[0]["title"], "Houdini Crush")
        self.assertEqual(tracks[0]["disc_number"], 1)
        self.assertAlmostEqual(cast(float, tracks[0]["length_seconds"]), 200.0)


class TestCmdImportJobRecovery(unittest.TestCase):
    def _recovery_job(self) -> tuple[FakePipelineDB, int]:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="release-42",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            "force_import",
            request_id=42,
            dedupe_key="force:cli-recovery",
            payload={"download_log_id": 1, "failed_path": "/tmp/cli-recovery"},
        )
        evidence = make_album_quality_evidence(
            mb_release_id="release-42",
            source_path="/tmp/cli-recovery",
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        row = next(row for row in db._import_jobs if row["id"] == job.id)
        row.update({
            "status": "recovery_required",
            "beets_launch_authorized_at": datetime.now(UTC),
            "beets_launch_release_id": "release-42",
            "beets_launch_source_path": "/tmp/cli-recovery",
            "beets_launch_request_status": "wanted",
            "beets_launch_snapshot_fingerprint": evidence.snapshot_fingerprint,
        })
        return db, job.id

    def test_recovery_listing_exposes_launch_authority(self) -> None:
        db, _job_id = self._recovery_job()
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            pipeline_cli.cmd_import_jobs(
                db,
                argparse.Namespace(status="recovery_required", limit=20),
            )

        output = stdout.getvalue()
        self.assertIn("release=release-42", output)
        self.assertIn("source=/tmp/cli-recovery", output)
        self.assertIn("snapshot=", output)
        self.assertIn("authorized=", output)

    def test_show_prints_same_typed_recovery_result(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=43,
            status="wanted",
            mb_release_id="75dbf62e-7dd2-4ddc-b57b-9bad1758b6b0",
        ))
        job = handoff_automation_owner(
            db,
            43,
            canonical_path="/processing/cli-recovery",
        )
        args = argparse.Namespace(
            recovery_action="show",
            job_id=job.id,
            beets_db=None,
            beets_directory=None,
        )
        stdout = io.StringIO()

        with (
            patch.object(
                db,
                "get_processing_cleanup_journal",
                lambda *, request_id, job_id: None,
                create=True,
            ),
            patch(
                "scripts.pipeline_cli.imports._open_recovery_beets",
                side_effect=FileNotFoundError("beets unavailable"),
            ),
            redirect_stdout(stdout),
        ):
            rc = pipeline_cli.cmd_import_job_recovery(
                db,
                args,
            )

        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["outcome"], "ok")
        self.assertEqual(payload["detail"]["owner_stage"]["job_id"], job.id)
        self.assertEqual(
            payload["detail"]["exact_library"]["status"],
            "unavailable",
        )
        self.assertEqual(
            payload["detail"]["cleanup_journal"]["status"],
            "missing",
        )

    def test_show_not_found_uses_canonical_not_found_exit(self) -> None:
        db = FakePipelineDB()
        args = argparse.Namespace(
            recovery_action="show",
            job_id=999999,
            beets_db=None,
            beets_directory=None,
        )

        with (
            patch(
                "scripts.pipeline_cli.imports._open_recovery_beets",
                side_effect=FileNotFoundError("beets unavailable"),
            ),
            redirect_stdout(io.StringIO()) as stdout,
        ):
            rc = pipeline_cli.cmd_import_job_recovery(db, args)

        self.assertEqual(rc, 2)
        self.assertEqual(json.loads(stdout.getvalue())["outcome"], "not_found")

    def test_parser_exposes_only_read_only_show(self) -> None:
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        shown = parser.parse_args(["import-job-recovery", "show", "41"])

        self.assertEqual((shown.recovery_action, shown.job_id), ("show", 41))
        for removed_verb in ("retry", "close"):
            with (
                self.subTest(removed_verb=removed_verb),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parser.parse_args([
                    "import-job-recovery",
                    removed_verb,
                    "41",
                ])


class TestCmdForceImport(_FakeDbWebServerCase):
    """``force-import`` is a thin adapter over ``POST
    /api/pipeline/force-import`` (issue #1063): the quarantine authority
    preflight must run under the service identity, so these pins drive
    the real route through the real dispatcher and assert the CLI's
    historical exit codes come back unchanged."""

    def _run(self, log_id: int) -> tuple[int, str]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_force_import(
                None,
                argparse.Namespace(
                    download_log_id=log_id,
                    api_endpoint=TcpApiEndpoint(self.base),
                ),
            )
        return rc, stdout.getvalue()

    def test_processing_owner_conflict_is_typed_and_exit_four(self) -> None:
        self.db.seed_request(make_request_row(
            id=123,
            status="wanted",
            mb_release_id="mbid-123",
            artist_name="Artist",
            album_title="Album",
        ))
        log_id = self.db.log_download(
            request_id=123,
            outcome="rejected",
            validation_result={},
        )
        owner = handoff_automation_owner(self.db, 123)

        with patch(
            "web.routes.pipeline_mutations.read_runtime_config",
            return_value=MagicMock(),
        ):
            rc, out = self._run(log_id)

        self.assertEqual(rc, 4)
        self.assertEqual(json.loads(out), {
            "error": "processing_locked",
            "reason": "processing_locked",
            "request_id": 123,
            "processing_owner": {
                "job_id": owner.id,
                "status": owner.status,
                "preview_status": owner.preview_status,
            },
            "detail": (
                f"request 123 is owned by automation import job {owner.id}"
            ),
        })
        self.assertEqual(
            [job.id for job in self.db.list_import_jobs()],
            [owner.id],
        )

    def test_force_import_passes_source_username_to_queue(self):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_dedupe_key

        self.db.seed_request(make_request_row(
            id=123, status="unsearchable", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        ))
        with tempfile.TemporaryDirectory() as root:
            staging = os.path.join(root, "Incoming")
            album = os.path.join(staging, "failed_imports", "Test Album")
            os.makedirs(album)
            cfg = SimpleNamespace(
                beets_staging_dir=staging,
                slskd_download_dir=os.path.join(root, "slskd"),
                processing_dir=os.path.join(root, "processing"),
            )
            os.makedirs(cfg.slskd_download_dir)
            os.makedirs(cfg.processing_dir)
            self.db.log_download(
                request_id=123, soulseek_username="baduser", outcome="rejected",
                validation_result={"failed_path": album, "source_dirs": ["peer\\Album"]},
            )
            log_id = self.db.download_logs[0].id
            with patch(
                "web.routes.pipeline_mutations.read_runtime_config",
                return_value=cfg,
            ):
                rc, out = self._run(log_id)

        self.assertEqual(rc, 0)
        self.assertIn("[OK] Queued", out)
        self.assertEqual(len(self.db._import_jobs), 1)
        job_row = self.db._import_jobs[0]
        self.assertEqual(job_row["job_type"], IMPORT_JOB_FORCE)
        self.assertEqual(job_row["request_id"], 123)
        self.assertEqual(job_row["dedupe_key"], force_import_dedupe_key(log_id))
        self.assertEqual(job_row["payload"]["failed_path"], album)
        self.assertEqual(job_row["payload"]["source_username"], "baduser")
        self.assertEqual(job_row["payload"]["source_dirs"], ["peer\\Album"])

    def test_force_import_failure_exit_codes_enqueue_nothing(self):
        self.db.seed_request(make_request_row(
            id=123, mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        ))
        self.db.seed_request(make_request_row(
            id=124, mb_release_id=None, discogs_release_id="124",
            artist_name="Discogs Artist", album_title="Discogs Album",
        ))
        with tempfile.TemporaryDirectory() as root:
            staging = os.path.join(root, "Incoming")
            slskd = os.path.join(root, "slskd")
            processing = os.path.join(root, "processing")
            os.makedirs(staging)
            os.makedirs(slskd)
            os.makedirs(processing)
            cfg = SimpleNamespace(
                beets_staging_dir=staging,
                slskd_download_dir=slskd,
                processing_dir=processing,
            )
            missing_request_log_id = self.db.log_download(
                request_id=999, outcome="rejected", validation_result={},
            )
            missing_path_log_id = self.db.log_download(
                request_id=123, outcome="rejected", validation_result={},
            )
            unauthorized = os.path.join(staging, "failed_imports-old", "Album")
            os.makedirs(unauthorized)
            unauthorized_log_id = self.db.log_download(
                request_id=123,
                outcome="rejected",
                validation_result={"failed_path": unauthorized},
            )
            discogs_album = os.path.join(staging, "failed_imports", "Discogs Album")
            os.makedirs(discogs_album)
            missing_mbid_log_id = self.db.log_download(
                request_id=124,
                outcome="rejected",
                validation_result={"failed_path": discogs_album},
            )

            with patch(
                "web.routes.pipeline_mutations.read_runtime_config",
                return_value=cfg,
            ):
                for name, log_id, expected_rc in (
                    ("missing log", 999_999, 2),
                    ("missing request", missing_request_log_id, 2),
                    ("missing path", missing_path_log_id, 3),
                    ("unauthorized", unauthorized_log_id, 3),
                    ("missing MusicBrainz ID", missing_mbid_log_id, 3),
                ):
                    with self.subTest(name=name):
                        rc, _out = self._run(log_id)
                        self.assertEqual(rc, expected_rc)
                        self.assertEqual(self.db.list_import_jobs(), [])


class TestCmdImportLocal(_FakeDbWebServerCase):
    """``import-local`` is a thin adapter over ``POST
    /api/pipeline/import-local`` (issue #1176 PR3) — mirrors
    ``TestCmdForceImport``'s shape exactly: the configured-authority
    preflight must run under the service identity, so these pins drive
    the real route through the real service and assert the CLI's exit
    codes (derived from ``LOCAL_IMPORT_HTTP_STATUS``, per
    ``lib.local_import_service.enqueue_local_import``'s outcome table)."""

    def _run(self, request_id: int, source_path: str) -> tuple[int, str]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_import_local(
                None,
                argparse.Namespace(
                    request_id=request_id,
                    source_path=source_path,
                    api_endpoint=TcpApiEndpoint(self.base),
                ),
            )
        return rc, stdout.getvalue()

    def test_processing_owner_conflict_is_typed_and_exit_four(self) -> None:
        self.db.seed_request(make_request_row(
            id=123, status="wanted", mb_release_id="mbid-123",
            artist_name="Artist", album_title="Album",
        ))
        owner = handoff_automation_owner(self.db, 123)

        with patch(
            "web.routes.pipeline_mutations.read_runtime_config",
            return_value=MagicMock(),
        ):
            rc, out = self._run(123, "/operator/Album")

        self.assertEqual(rc, 4)
        self.assertEqual(json.loads(out), {
            "error": "processing_locked",
            "reason": "processing_locked",
            "request_id": 123,
            "processing_owner": {
                "job_id": owner.id,
                "status": owner.status,
                "preview_status": owner.preview_status,
            },
            "detail": (
                f"request 123 is owned by automation import job {owner.id}"
            ),
        })
        self.assertEqual(
            [job.id for job in self.db.list_import_jobs()],
            [owner.id],
        )

    def test_import_local_enqueues_with_authorized_path(self) -> None:
        from lib.import_queue import IMPORT_JOB_LOCAL, local_import_dedupe_key

        self.db.seed_request(make_request_row(
            id=123, status="wanted", mb_release_id="mbid-123",
            artist_name="Artist", album_title="Album",
        ))
        with tempfile.TemporaryDirectory() as root:
            local_dir = os.path.join(root, "LocalImport")
            album = os.path.join(local_dir, "MyRip", "Album")
            os.makedirs(album)
            cfg = SimpleNamespace(
                local_import_enabled=True,
                local_import_dir=local_dir,
                processing_dir=os.path.join(root, "processing"),
                beets_staging_dir=os.path.join(root, "Incoming"),
                slskd_download_dir=os.path.join(root, "slskd"),
                beets_directory=os.path.join(root, "Beets"),
                beets_library_db=os.path.join(root, "beets-db", "beets-library.db"),
            )
            with patch(
                "web.routes.pipeline_mutations.read_runtime_config",
                return_value=cfg,
            ):
                rc, out = self._run(123, album)

        self.assertEqual(rc, 0)
        self.assertIn("[OK] Queued", out)
        self.assertEqual(len(self.db._import_jobs), 1)
        job_row = self.db._import_jobs[0]
        self.assertEqual(job_row["job_type"], IMPORT_JOB_LOCAL)
        self.assertEqual(job_row["request_id"], 123)
        self.assertEqual(
            job_row["dedupe_key"], local_import_dedupe_key(123))
        self.assertEqual(job_row["payload"]["source_path"], album)
        self.assertEqual(job_row["payload"]["request_id"], 123)

    def test_import_local_failure_exit_codes_enqueue_nothing(self) -> None:
        self.db.seed_request(make_request_row(
            id=123, mb_release_id="mbid-123",
            artist_name="Artist", album_title="Album",
        ))
        self.db.seed_request(make_request_row(
            id=124, mb_release_id=None, discogs_release_id="124",
            artist_name="Discogs Artist", album_title="Discogs Album",
        ))
        with tempfile.TemporaryDirectory() as root:
            local_dir = os.path.join(root, "LocalImport")
            os.makedirs(local_dir)
            cfg = SimpleNamespace(
                local_import_enabled=True,
                local_import_dir=local_dir,
                processing_dir=os.path.join(root, "processing"),
                beets_staging_dir=os.path.join(root, "Incoming"),
                slskd_download_dir=os.path.join(root, "slskd"),
                beets_directory=os.path.join(root, "Beets"),
                beets_library_db=os.path.join(root, "beets-db", "beets-library.db"),
            )
            outside = os.path.join(root, "outside")
            os.makedirs(outside)

            with patch(
                "web.routes.pipeline_mutations.read_runtime_config",
                return_value=cfg,
            ):
                for name, request_id, source_path, expected_rc in (
                    ("missing request", 999_999, local_dir, 2),
                    ("outside root", 123, outside, 3),
                    ("missing MusicBrainz ID", 124, local_dir, 3),
                ):
                    with self.subTest(name=name):
                        rc, _out = self._run(request_id, source_path)
                        self.assertEqual(rc, expected_rc)
                        self.assertEqual(self.db.list_import_jobs(), [])


class TestCmdImportPreview(unittest.TestCase):
    def test_values_json_outputs_common_preview_json(self):
        """Values-mode JSON output round-trips a real preview verdict.

        Drives the real ``preview_import_from_values`` (no stub) with a
        FLAC scenario that classifies as ``would_import``. The pure
        decision's own coverage lives in ``tests/test_import_preview.py``;
        this CLI test just verifies the wire shape of the JSON output.
        """
        db = FakePipelineDB()
        args = argparse.Namespace(
            download_log_id=None,
            request_id=None,
            path=None,
            no_force=False,
            values=True,
            values_json='{"is_flac": true, "min_bitrate": 900, "spectral_grade": "genuine"}',
            json=True,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_import_preview(db, args)

        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["verdict"], "would_import")
        # The CLI threaded min_bitrate=900 into the simulator — the resulting
        # simulation must reflect it. (target_final_format defaults to mp3 v0
        # when no verified_lossless_target is configured.)
        self.assertEqual(payload["mode"], "values")
        self.assertTrue(payload["would_import"])

    def test_values_args_thread_existing_spectral_grade(self):
        """argparse-style values-mode threads existing_* spectral fields
        through to the real preview engine.

        Observable proof: with a likely_transcode candidate vs a higher-rank
        existing album, the real classifier returns ``confident_reject``.
        Replacing existing_spectral_bitrate with a higher value would flip
        the decision — so the JSON output reflects threading.
        """
        db = FakePipelineDB()
        args = argparse.Namespace(
            download_log_id=None,
            request_id=None,
            path=None,
            no_force=False,
            values=True,
            values_json=None,
            json=True,
            is_flac=False,
            min_bitrate=171,
            is_cbr=False,
            is_vbr=True,
            avg_bitrate=196,
            spectral_grade="likely_transcode",
            spectral_bitrate=160,
            existing_min_bitrate=246,
            existing_avg_bitrate=261,
            existing_spectral_bitrate=128,
            existing_spectral_grade="genuine",
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_import_preview(db, args)

        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        # The simulation dict carries enough state to prove threading: the
        # final_status reflects the existing-side state the CLI passed in.
        self.assertIn("simulation", payload)
        sim = payload["simulation"]
        self.assertIsNotNone(sim)
        # downgrade vs upgrade depends on existing_* being threaded in; any
        # non-import final_status proves the existing-side beat the candidate.
        self.assertFalse(payload["would_import"])

    def test_values_json_rejects_invalid_spectral_grade(self):
        """Validation rejects before reaching the preview engine."""
        db = FakePipelineDB()
        args = argparse.Namespace(
            download_log_id=None,
            request_id=None,
            path=None,
            no_force=False,
            values=True,
            values_json='{"spectral_grade": "likely-transcode"}',
            json=False,
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_import_preview(db, args)

        # rc=2 + the expected stderr message is sufficient evidence that
        # validation rejected before the preview engine was invoked.
        self.assertEqual(rc, 2)
        self.assertIn("spectral_grade must be one of", stderr.getvalue())

    def test_values_json_rejects_invalid_existing_spectral_grade(self):
        db = FakePipelineDB()
        args = argparse.Namespace(
            download_log_id=None,
            request_id=None,
            path=None,
            no_force=False,
            values=True,
            values_json='{"existing_spectral_grade": "likely-transcode"}',
            json=False,
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_import_preview(db, args)

        self.assertEqual(rc, 2)
        self.assertIn(
            "existing_spectral_grade must be one of",
            stderr.getvalue(),
        )

    def test_explicit_path_mode_reports_unavailable_not_missing(self):
        """The CLI-only explicit-path inspector owes the same distinction.

        An unreadable parent used to answer "Path not found" — a
        definitive negative fact it had no evidence for (issue #1063).
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=7, mb_release_id=RELEASE_A))
        root = self.enterContext(tempfile.TemporaryDirectory())
        parent = os.path.join(root, "private")
        album = os.path.join(parent, "Album")
        os.makedirs(album)
        os.chmod(parent, 0o000)
        self.addCleanup(os.chmod, parent, 0o700)
        args = argparse.Namespace(
            download_log_id=None,
            request_id=7,
            path=album,
            no_force=False,
            values=False,
            values_json=None,
            json=True,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_import_preview(db, args)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(payload["decision"], "path_unavailable")
        self.assertIn("could not be observed", payload["reason"])

    def test_explicit_path_mode_still_reports_a_genuinely_missing_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=7, mb_release_id=RELEASE_A))
        root = self.enterContext(tempfile.TemporaryDirectory())
        args = argparse.Namespace(
            download_log_id=None,
            request_id=7,
            path=os.path.join(root, "gone"),
            no_force=False,
            values=False,
            values_json=None,
            json=True,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_import_preview(db, args)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(payload["decision"], "path_missing")


class TestCmdImportPreviewDownloadLogMode(_FakeDbWebServerCase):
    """``--download-log-id`` resolves a DB-owned protected path, so it is
    the one preview mode that runs through the canonical route under the
    service identity (issue #1063)."""

    def test_download_log_mode_routes_through_the_preview_endpoint(self):
        from lib.import_preview import ImportPreviewResult

        args = argparse.Namespace(
            download_log_id=99,
            request_id=None,
            path=None,
            no_force=False,
            values=False,
            values_json=None,
            json=False,
            api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with patch(
            "web.routes.imports.preview_import_from_download_log",
            return_value=ImportPreviewResult(
                mode="download_log",
                verdict="confident_reject",
                decision="downgrade",
                confident_reject=True,
                cleanup_eligible=True,
            ),
        ) as mock_preview, redirect_stdout(stdout):
            rc = pipeline_cli.cmd_import_preview_from_download_log(
                None, args)

        self.assertEqual(rc, 0)
        mock_preview.assert_called_once_with(self.db, 99)
        self.assertIn("cleanup_eligible: yes", stdout.getvalue())

    def test_other_modes_are_refused_by_the_routed_adapter(self):
        args = argparse.Namespace(
            download_log_id=99,
            request_id=None,
            path="/tmp/album",
            no_force=False,
            values=False,
            values_json=None,
            json=False,
            api_endpoint=TcpApiEndpoint(self.base),
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_import_preview_from_download_log(
                None, args)

        self.assertEqual(rc, 2)
        self.assertIn("exactly one mode", stderr.getvalue())


class TestCmdWrongMatchTriage(_FakeDbWebServerCase):
    """``wrong-match-triage`` starts the canonical background sweep and
    follows it to completion over the same route the browser uses
    (issue #1063)."""

    def setUp(self) -> None:
        super().setUp()
        _fresh_triage_runner(self)

    def test_triage_requires_apply(self):
        args = argparse.Namespace(
            apply=False, json=False, api_endpoint=TcpApiEndpoint(self.base),
        )
        stderr = io.StringIO()
        with patch(
            "lib.wrong_match_cleanup_service.cleanup_all_wrong_matches"
        ) as cleanup, redirect_stderr(stderr):
            rc = pipeline_cli.cmd_wrong_match_triage(None, args)

        self.assertEqual(rc, 2)
        cleanup.assert_not_called()
        self.assertIn("--apply", stderr.getvalue())

    def test_triage_apply_runs_the_canonical_sweep_and_prints_its_summary(self):
        self.db.seed_request(make_request_row(id=1))
        args = argparse.Namespace(
            apply=True, json=True, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None,
                args,
                sleep=lambda _seconds: None,
            )

        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["state"], "completed")
        self.assertIsNone(payload["error"])
        self.assertEqual(payload["summary"]["processed"], 0)

    def test_crashed_sweep_is_reported_as_a_failure_not_an_api_refusal(self):
        """A sweep that raised must say so, and exit non-zero (#1063 T3.5)."""
        import web.routes.imports as imports_routes

        def _boom(_db, **_kwargs):
            raise OSError("[Errno 5] Input/output error: /mnt/virtio")

        args = argparse.Namespace(
            apply=True, json=False, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with patch.object(
            imports_routes, "cleanup_all_wrong_matches", _boom,
        ), redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=lambda _seconds: None,
            )
            imports_routes._triage_runner.join(timeout=5)

        self.assertEqual(rc, 5)
        output = stdout.getvalue()
        self.assertIn("sweep FAILED", output)
        self.assertIn("Input/output error", output)
        self.assertNotIn("API refused", output)

    def test_lost_sweep_status_is_reported_as_lost_not_an_api_refusal(self):
        """The web service restarted mid-sweep: idle with no summary."""
        import web.routes.imports as imports_routes

        args = argparse.Namespace(
            apply=True, json=False, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with patch.object(
            imports_routes._triage_runner, "status",
            return_value={
                "state": "idle", "started_at": None, "finished_at": None,
                "summary": None, "error": None,
            },
        ), redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=lambda _seconds: None,
            )
            imports_routes._triage_runner.join(timeout=5)

        self.assertEqual(rc, 5)
        output = stdout.getvalue()
        self.assertIn("No Wrong Matches sweep result is available", output)
        self.assertNotIn("API refused", output)

    def test_transient_poll_failures_do_not_abandon_the_sweep(self):
        """One blip must not detach the operator mid-delete (#1063 T3.6)."""
        self.db.seed_request(make_request_row(id=1))
        real_post = api_mutations._post
        calls = {"n": 0}

        def flaky_post(endpoint, mutation, *, timeout_seconds=15.0,
                       report_failure=True):
            if mutation.method == "GET":
                calls["n"] += 1
                if calls["n"] == 1:
                    return None
            return real_post(
                endpoint, mutation, timeout_seconds=timeout_seconds)

        args = argparse.Namespace(
            apply=True, json=True, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with patch.object(api_mutations, "_post", flaky_post), \
                redirect_stdout(stdout), redirect_stderr(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=lambda _seconds: None,
            )

        self.assertEqual(rc, 0)
        self.assertGreaterEqual(calls["n"], 2)
        self.assertEqual(json.loads(stdout.getvalue())["state"], "completed")

    def test_persistent_poll_failure_still_gives_up(self):
        args = argparse.Namespace(
            apply=True, json=True, api_endpoint=TcpApiEndpoint(self.base),
        )
        real_post = api_mutations._post

        def dead_polls(endpoint, mutation, *, timeout_seconds=15.0,
                       report_failure=True):
            if mutation.method == "GET":
                return None
            return real_post(
                endpoint, mutation, timeout_seconds=timeout_seconds)

        with patch.object(api_mutations, "_post", dead_polls), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=lambda _seconds: None,
            )

        self.assertEqual(rc, 5)

    def test_triage_conflict_when_a_sweep_is_already_running(self):
        import web.routes.imports as imports_routes

        args = argparse.Namespace(
            apply=True, json=True, api_endpoint=TcpApiEndpoint(self.base),
        )
        with patch.object(
            imports_routes._triage_runner, "start", return_value=False,
        ), redirect_stdout(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None,
                args,
                sleep=lambda _seconds: None,
            )

        self.assertEqual(rc, 4)

    def test_sigint_during_poll_cancels_through_the_socket_not_directly(self):
        """Issue #1083: Ctrl-C during the poll must route cancellation
        through the exact same canonical route the web UI's Stop button
        posts to — no direct call, no direct-DB fallback. Simulated the
        same way the youtube-ingest worker's SIGINT test does: the
        collaborator raises ``KeyboardInterrupt`` directly rather than
        racing a real OS signal delivery."""
        import web.routes.imports as imports_routes
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        self.db.seed_request(make_request_row(id=1))
        entered = threading.Event()

        def slow_cleanup(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            assert cancellation_token is not None
            deadline = time.monotonic() + 5
            while (
                not cancellation_token.cancelled
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            return WrongMatchCleanupSummary(
                processed=0, cancelled=cancellation_token.cancelled,
            )

        args = argparse.Namespace(
            apply=True, json=True, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        interrupted = {"done": False}

        def sleep_then_interrupt(_seconds):
            if not interrupted["done"]:
                if entered.wait(timeout=5):
                    interrupted["done"] = True
                    raise KeyboardInterrupt
                return
            time.sleep(0.01)

        real_post = pipeline_cli_wrong_match._post
        captured_bodies: list[object] = []

        def recording_post(endpoint, mutation, *, timeout_seconds=15.0,
                            report_failure=True):
            if mutation.path == "/api/wrong-matches/triage/cancel":
                captured_bodies.append(mutation.body)
            return real_post(
                endpoint, mutation, timeout_seconds=timeout_seconds,
                report_failure=report_failure,
            )

        with patch.object(
            imports_routes, "cleanup_all_wrong_matches", slow_cleanup,
        ), patch.object(
            pipeline_cli_wrong_match, "_post", recording_post,
        ), redirect_stdout(stdout), redirect_stderr(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=sleep_then_interrupt,
            )
            imports_routes._triage_runner.join(timeout=5)

        self.assertEqual(rc, 5)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["state"], "cancelled")
        self.assertIsNone(payload["error"])
        self.assertTrue(payload["summary"]["cancelled"])
        # #1106 N1: the Ctrl-C handler is specifically racing its OWN
        # start POST, so it is the one caller that must arm the sticky
        # pending cancel -- stripping this must turn the test red.
        self.assertEqual(captured_bodies, [{"arm_pending": True}])

    def test_sigint_cancel_post_retries_once_before_giving_up(self):
        """Issue #1083 review: a failed cancel POST must not be silently
        swallowed. The first attempt fails (simulated dropped socket);
        the retry succeeds -- the sweep still gets cancelled and nothing
        is printed claiming the stop failed."""
        import web.routes.imports as imports_routes
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        self.db.seed_request(make_request_row(id=1))
        entered = threading.Event()

        def slow_cleanup(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            assert cancellation_token is not None
            deadline = time.monotonic() + 5
            while (
                not cancellation_token.cancelled
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            return WrongMatchCleanupSummary(
                processed=0, cancelled=cancellation_token.cancelled,
            )

        real_post = pipeline_cli_wrong_match._post
        calls = {"n": 0}
        captured_bodies: list[object] = []

        def flaky_cancel_post(endpoint, mutation, *, timeout_seconds=15.0,
                              report_failure=True):
            if mutation.path == "/api/wrong-matches/triage/cancel":
                calls["n"] += 1
                captured_bodies.append(mutation.body)
                if calls["n"] == 1:
                    return None
            return real_post(
                endpoint, mutation, timeout_seconds=timeout_seconds,
                report_failure=report_failure,
            )

        args = argparse.Namespace(
            apply=True, json=True, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        interrupted = {"done": False}

        def sleep_then_interrupt(_seconds):
            if not interrupted["done"]:
                if entered.wait(timeout=5):
                    interrupted["done"] = True
                    raise KeyboardInterrupt
                return
            time.sleep(0.01)

        with patch.object(
            imports_routes, "cleanup_all_wrong_matches", slow_cleanup,
        ), patch.object(
            pipeline_cli_wrong_match, "_post", flaky_cancel_post,
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=sleep_then_interrupt,
            )
            imports_routes._triage_runner.join(timeout=5)

        self.assertEqual(rc, 5)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["state"], "cancelled")
        self.assertTrue(payload["summary"]["cancelled"])
        self.assertEqual(calls["n"], 2)
        self.assertNotIn("Stop request failed", stderr.getvalue())
        # #1106 N1: both the failed first attempt and the successful
        # retry must arm the pending slot -- this is the CLI's own
        # start-POST race, not a generic Stop click.
        self.assertEqual(
            captured_bodies, [{"arm_pending": True}, {"arm_pending": True}],
        )

    def test_sigint_cancel_post_failure_is_reported_when_retry_also_fails(self):
        """Both cancel attempts fail (dropped socket both times): the CLI
        must say so on stderr instead of silently claiming success -- the
        sweep, never actually told to stop, keeps running to its normal
        completion instead of the CLI falsely implying it was cut off."""
        import web.routes.imports as imports_routes
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        self.db.seed_request(make_request_row(id=1))
        entered = threading.Event()
        release = threading.Event()

        def quick_cleanup(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            release.wait(timeout=5)
            return WrongMatchCleanupSummary(
                processed=1, deleted=1, cancelled=False,
            )

        captured_bodies: list[object] = []

        def always_fail_cancel_post(endpoint, mutation, *, timeout_seconds=15.0,
                                    report_failure=True):
            assert mutation.path == "/api/wrong-matches/triage/cancel"
            captured_bodies.append(mutation.body)

        args = argparse.Namespace(
            apply=True, json=False, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        interrupted = {"done": False}

        def sleep_then_interrupt(_seconds):
            if not interrupted["done"]:
                if entered.wait(timeout=5):
                    interrupted["done"] = True
                    release.set()
                    raise KeyboardInterrupt
                return
            time.sleep(0.01)

        with patch.object(
            imports_routes, "cleanup_all_wrong_matches", quick_cleanup,
        ), patch.object(
            pipeline_cli_wrong_match, "_post", always_fail_cancel_post,
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=sleep_then_interrupt,
            )
            imports_routes._triage_runner.join(timeout=5)

        self.assertIn("Stop request failed", stderr.getvalue())
        self.assertIn("total: 1", stdout.getvalue())
        self.assertEqual(rc, 0)
        # #1106 N1: both attempts (initial + retry) must arm.
        self.assertEqual(
            captured_bodies, [{"arm_pending": True}, {"arm_pending": True}],
        )

    def test_cancelled_sweep_reports_distinctly_from_completed_in_text(self):
        """The human-readable renderer says CANCELLED, never FAILED, and
        still prints exactly what the summary says ran."""
        import web.routes.imports as imports_routes
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        self.db.seed_request(make_request_row(id=1))

        def cancelled_cleanup(db, *, confirm_all_wrong_matches, cancellation_token=None):
            return WrongMatchCleanupSummary(
                processed=1, deleted=1, cancelled=True,
            )

        args = argparse.Namespace(
            apply=True, json=False, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with patch.object(
            imports_routes, "cleanup_all_wrong_matches", cancelled_cleanup,
        ), redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_triage(
                None, args, sleep=lambda _seconds: None,
            )
            imports_routes._triage_runner.join(timeout=5)

        self.assertEqual(rc, 5)
        output = stdout.getvalue()
        self.assertIn("CANCELLED", output)
        self.assertIn("deleted: 1", output)
        self.assertNotIn("FAILED", output)


class TestCmdWrongMatchTriageCancel(_FakeDbWebServerCase):
    """``wrong-match-triage-cancel`` reaches the exact canonical cancel
    route the CLI's own Ctrl-C handler and the web UI's Stop button use
    (issue #1083) — the only way to stop a sweep left running after a
    dropped connection, with no interactive terminal left to catch
    Ctrl-C."""

    def setUp(self) -> None:
        super().setUp()
        _fresh_triage_runner(self)

    def test_cancel_with_nothing_running_still_exits_zero(self) -> None:
        args = argparse.Namespace(api_endpoint=TcpApiEndpoint(self.base))
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_triage_cancel(None, args)

        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(stdout.getvalue())["state"], "idle")

    def test_cancel_body_stays_unarmed(self) -> None:
        """#1106 N1: this command has no start POST of its own to race,
        so it must never send `arm_pending` — arming here would risk
        pre-cancelling a LATER, unrelated sweep the operator has not
        even started. Body-level pin: adding `arm_pending` here must
        turn this test red."""
        real_post = api_mutations._post
        captured_bodies: list[object] = []

        def recording_post(endpoint, mutation, *, timeout_seconds=15.0,
                            report_failure=True):
            if mutation.path == "/api/wrong-matches/triage/cancel":
                captured_bodies.append(mutation.body)
            return real_post(
                endpoint, mutation, timeout_seconds=timeout_seconds,
                report_failure=report_failure,
            )

        args = argparse.Namespace(api_endpoint=TcpApiEndpoint(self.base))
        with patch.object(api_mutations, "_post", recording_post), \
                redirect_stdout(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_triage_cancel(None, args)

        self.assertEqual(rc, 0)
        self.assertEqual(captured_bodies, [{}])

    def test_cancel_stops_a_running_sweep_through_the_real_route(self) -> None:
        import web.routes.imports as imports_routes
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        self.db.seed_request(make_request_row(id=1))
        entered = threading.Event()

        def slow_cleanup(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            assert cancellation_token is not None
            deadline = time.monotonic() + 5
            while (
                not cancellation_token.cancelled
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            return WrongMatchCleanupSummary(
                processed=0, cancelled=cancellation_token.cancelled,
            )

        with patch.object(
            imports_routes, "cleanup_all_wrong_matches", slow_cleanup,
        ):
            status, _body = self._post(
                "/api/wrong-matches/triage",
                {"confirm_all_wrong_matches": True},
            )
            self.assertEqual(status, 202)
            self.assertTrue(entered.wait(timeout=5))

            args = argparse.Namespace(api_endpoint=TcpApiEndpoint(self.base))
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                rc = pipeline_cli.cmd_wrong_match_triage_cancel(None, args)
            imports_routes._triage_runner.join(timeout=5)

        self.assertEqual(rc, 0)
        # ``cancel()`` answers with whatever the sweep thread has already
        # observed at that instant -- honestly async, same as the web
        # UI's Stop button -- so it may still read "running" the moment
        # the request lands. The join() above proves the sweep actually
        # settles into "cancelled" once it next checks the token.
        payload = json.loads(stdout.getvalue())
        self.assertIn(payload["state"], ("running", "cancelled"))
        final = imports_routes._triage_runner.status()
        self.assertEqual(final["state"], "cancelled")
        final_summary = final["summary"]
        assert final_summary is not None
        self.assertTrue(final_summary["cancelled"])


class TestCmdWrongMatchDelete(_FakeDbWebServerCase):
    """``wrong-match-delete`` exit codes, proven end to end: CLI adapter →
    real HTTP route → real delete service → real filesystem. Every
    outcome below is produced by an actual world, not a stubbed result
    (issue #1063)."""

    def _run(self, log_id: int, *, apply: bool = True) -> tuple[int, str]:
        args = argparse.Namespace(
            download_log_id=log_id,
            apply=apply,
            json=True,
            api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_delete(None, args)
        return rc, stdout.getvalue()

    def test_delete_requires_apply(self):
        stderr = io.StringIO()
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match"
        ) as delete, redirect_stderr(stderr):
            rc, _out = self._run(42, apply=False)

        self.assertEqual(rc, 2)
        delete.assert_not_called()
        self.assertIn("--apply", stderr.getvalue())

    def test_delete_removes_the_folder_and_clears_the_pointer(self):
        source = seed_visible_wrong_match(self.db, self.enterContext(
            tempfile.TemporaryDirectory()))
        rc, out = self._run(source.download_log_id)

        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "deleted")
        self.assertEqual(payload["deleted_path"], source.path)
        self.assertFalse(payload["path_missing"])
        self.assertEqual(payload["cleared_rows"], 1)
        self.assertFalse(os.path.exists(source.path))
        self.assertEqual(self.db.get_wrong_matches(), [])

    def test_delete_missing_row_returns_not_found_exit_code(self):
        rc, _out = self._run(999_999)
        self.assertEqual(rc, 2)

    def test_delete_not_visible_row_returns_not_found_exit_code(self):
        self.db.seed_request(make_request_row(id=1))
        log_id = self.db.log_download(
            request_id=1,
            outcome="rejected",
            validation_result={
                "scenario": "spectral_reject",
                "failed_path": "/nowhere/wrong_matches/A",
            },
        )
        rc, _out = self._run(log_id)
        self.assertEqual(rc, 2)

    def test_genuinely_missing_folder_still_clears_the_pointer(self):
        """Must-still-work: PROVEN absence is allowed to clear (issue #1063)."""
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)
        shutil.rmtree(source.path)

        rc, out = self._run(source.download_log_id)

        payload = json.loads(out)
        self.assertEqual(rc, 0)
        # Successful and clearing, but never headlined "deleted" — the
        # folder was proven absent, not removed by us (#1063 invariant 1).
        self.assertEqual(payload["outcome"], "path_missing")
        self.assertTrue(payload["success"])
        self.assertTrue(payload["path_missing"])
        self.assertIsNone(payload["deleted_path"])
        self.assertEqual(payload["cleared_rows"], 1)
        self.assertEqual(self.db.get_wrong_matches(), [])

    def test_unreadable_parent_refuses_and_keeps_both_folder_and_pointer(self):
        """The exact #1063 world: EACCES is not evidence of absence.

        The operator identity cannot traverse the private parent, so the
        probe is refused. The command must fail, delete nothing, and keep
        the row actionable — never report ``deleted``/``path_missing``.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)
        os.chmod(source.parent, 0o000)
        self.addCleanup(os.chmod, source.parent, 0o700)

        rc, out = self._run(source.download_log_id)

        payload = json.loads(out)
        self.assertEqual(rc, 5)
        # The refusal keeps the whole typed result, so --json can still
        # express "neither deleted nor missing" (#1063 review T3.1).
        self.assertEqual(payload["outcome"], "skipped_path_unavailable")
        self.assertFalse(payload["success"])
        self.assertIn("path_unavailable", payload["error"])
        self.assertIsNone(payload["deleted_path"])
        self.assertFalse(payload["path_missing"])
        self.assertEqual(payload["cleared_rows"], 0)
        os.chmod(source.parent, 0o700)
        self.assertTrue(os.path.isdir(source.path))
        self.assertEqual(
            [row["download_log_id"] for row in self.db.get_wrong_matches()],
            [source.download_log_id],
        )

    def test_delete_active_job_returns_conflict_exit_code(self):
        source = seed_visible_wrong_match(self.db, self.enterContext(
            tempfile.TemporaryDirectory()))
        self.db.enqueue_import_job(
            "force_import",
            request_id=1,
            payload={
                "download_log_id": source.download_log_id,
                "failed_path": source.path,
            },
        )
        rc, _out = self._run(source.download_log_id)

        self.assertEqual(rc, 4)
        self.assertTrue(os.path.isdir(source.path))

    def test_delete_unsafe_path_returns_semantic_violation_exit_code(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root, quarantine="elsewhere")
        rc, _out = self._run(source.download_log_id)

        self.assertEqual(rc, 3)
        self.assertTrue(os.path.isdir(source.path))

    def test_delete_locked_returns_transient_exit_code(self):
        source = seed_visible_wrong_match(self.db, self.enterContext(
            tempfile.TemporaryDirectory()))
        self.db.set_advisory_lock_result(False)
        rc, _out = self._run(source.download_log_id)

        self.assertEqual(rc, 5)
        self.assertTrue(os.path.isdir(source.path))

    def test_delete_failure_returns_generic_failure_exit_code(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)
        # The album directory itself is readable but not writable, so the
        # real rmtree fails partway — a genuine delete failure, not an
        # unobservable path.
        os.chmod(source.path, 0o500)
        self.addCleanup(os.chmod, source.path, 0o700)
        rc, _out = self._run(source.download_log_id)

        self.assertEqual(rc, 1)
        self.assertTrue(os.path.isdir(source.path))
        self.assertNotEqual(self.db.get_wrong_matches(), [])


class TestCmdWrongMatchDeleteGroup(_FakeDbWebServerCase):
    """``wrong-match-delete-group`` exit codes through the real route."""

    def _run(self, request_id: int, *, apply: bool = True) -> tuple[int, str]:
        args = argparse.Namespace(
            request_id=request_id,
            apply=apply,
            json=True,
            api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_delete_group(
                None, args)
        return rc, stdout.getvalue()

    def test_delete_group_requires_apply(self):
        stderr = io.StringIO()
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match_group"
        ) as delete, redirect_stderr(stderr):
            rc, _out = self._run(42, apply=False)

        self.assertEqual(rc, 2)
        delete.assert_not_called()
        self.assertIn("--apply", stderr.getvalue())

    def test_delete_group_removes_every_visible_source(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        first = seed_visible_wrong_match(self.db, root, name="Album One")
        second = seed_visible_wrong_match(self.db, root, name="Album Two")

        rc, out = self._run(first.request_id)

        payload = json.loads(out)
        self.assertEqual(rc, 0)
        self.assertEqual(payload["deleted"], 2)
        self.assertEqual(payload["deleted_paths"], 2)
        self.assertEqual(payload["remaining"], 0)
        self.assertFalse(os.path.exists(first.path))
        self.assertFalse(os.path.exists(second.path))

    def test_delete_group_active_job_returns_conflict_exit_code(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)
        self.db.enqueue_import_job(
            "force_import",
            request_id=source.request_id,
            payload={
                "download_log_id": source.download_log_id,
                "failed_path": source.path,
            },
        )

        rc, out = self._run(source.request_id)

        payload = json.loads(out)
        self.assertEqual(rc, 4)
        self.assertEqual(payload["remaining"], 1)
        self.assertTrue(os.path.isdir(source.path))

    def test_delete_group_unreadable_parent_keeps_every_pointer(self):
        """Replace's cleanup lane: unreadable sources must not clear."""
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)
        os.chmod(source.parent, 0o000)
        self.addCleanup(os.chmod, source.parent, 0o700)

        rc, out = self._run(source.request_id)

        payload = json.loads(out)
        self.assertEqual(rc, 5)
        self.assertEqual(payload["remaining"], 1)
        self.assertEqual(payload["deleted"], 0)
        self.assertEqual(payload["deleted_paths"], 0)
        self.assertEqual(payload["cleared"], 0)
        # Issue #1086 item 3: an unreadable (never proven gone) parent is
        # NOT a delete error — it lands in its own ``unavailable`` bucket,
        # not double-counted into ``errors`` alongside ``skipped``.
        self.assertEqual(payload["errors"], 0)
        self.assertGreaterEqual(payload["unavailable"], 1)
        os.chmod(source.parent, 0o700)
        self.assertTrue(os.path.isdir(source.path))
        self.assertEqual(
            [row["download_log_id"] for row in self.db.get_wrong_matches()],
            [source.download_log_id],
        )

class TestMainExitCodes(unittest.TestCase):
    def test_convergence_stop_constructor_outage_maps_to_exit_five(self):
        import psycopg2

        import web.mb

        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "triage",
            "stop",
            "41",
            "--signal-token",
            "a" * 64,
            "--confirm",
            "STOP",
            "--json",
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()
        old_mb_base = web.mb.MB_API_BASE
        self.addCleanup(setattr, web.mb, "MB_API_BASE", old_mb_base)
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.ini")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "[MusicBrainz]\n"
                    "api_base = http://constructor-outage.test:5200\n"
                )
            with patch.object(sys, "argv", argv), patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
                clear=False,
            ), patch(
                "scripts.pipeline_cli.cli.PipelineDB",
                side_effect=psycopg2.OperationalError("database unavailable"),
            ), redirect_stdout(stdout), redirect_stderr(stderr), self.assertRaises(
                SystemExit,
            ) as raised:
                pipeline_cli.main()

        self.assertEqual(raised.exception.code, 5)
        self.assertEqual(stderr.getvalue(), "")
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["outcome"], "unavailable")
        self.assertEqual(payload["request_id"], 41)

    def test_malformed_convergence_token_fails_before_db_construction(self):
        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "triage",
            "stop",
            "41",
            "--signal-token",
            "NOT-HEX",
            "--confirm",
            "STOP",
        ]
        with patch.object(sys, "argv", argv), patch(
            "scripts.pipeline_cli.cli.PipelineDB",
        ) as constructor, redirect_stderr(io.StringIO()), self.assertRaises(
            SystemExit,
        ) as raised:
            pipeline_cli.main()

        self.assertEqual(raised.exception.code, 2)
        constructor.assert_not_called()

    def test_non_quarantine_main_still_configures_mirror_api_bases(self):
        import web.mb

        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "status",
        ]
        db = FakePipelineDB()
        old_mb_base = web.mb.MB_API_BASE
        self.addCleanup(setattr, web.mb, "MB_API_BASE", old_mb_base)
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.ini")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "[MusicBrainz]\n"
                    "api_base = http://main-entrypoint-mirror.test:5200\n"
                )
            with patch.object(sys, "argv", argv), patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
                clear=False,
            ), patch(
                "scripts.pipeline_cli.cli.PipelineDB",
                return_value=db,
            ), redirect_stdout(io.StringIO()):
                pipeline_cli.main()

        self.assertEqual(
            web.mb.MB_API_BASE,
            "http://main-entrypoint-mirror.test:5200/ws/2",
        )
        self.assertEqual(db.close_calls, 1)

    # #1122 F1 removed the three ``test_quarantine_main_maps_*`` tests that
    # lived here: ``triage quarantine`` no longer constructs a PipelineDB in
    # main() at all (it exits through the API-relay branch first, alongside
    # every other #1063 command), so "PipelineDB construction fails for
    # quarantine" is not a reachable scenario any more. What they covered is
    # covered elsewhere now: PipelineDB is never constructed for quarantine
    # (``TestMainProtectedPathDispatch.test_main_routes_every_protected_path_command_without_a_db``,
    # which now includes ``["triage", "quarantine"]``); the SERVICE's own
    # runtime-config-read failure maps to ``QuarantineScanError`` in
    # ``tests/test_quarantine_triage_service.py::test_unreadable_runtime_config_fails_closed``
    # (#1122 review NEW-1); the ROUTE's generic ``QuarantineScanError`` -> 503
    # mapping is pinned in ``tests/web/test_routes_triage.py``
    # (``test_quarantine_filesystem_failure_returns_503``, exercised via a
    # filesystem failure rather than a config failure, plus the separate DB
    # acquisition failure in ``test_quarantine_db_acquisition_failure_returns_stable_503``);
    # and the CLI's relay of any such 503 to exit 5, in both JSON and human
    # form, is pinned in ``TestCmdTriageQuarantine`` below.

    def test_main_propagates_command_return_code(self):
        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "wrong-match-triage",
        ]
        with patch.object(sys, "argv", argv), patch(
            "scripts.pipeline_cli.cli.PipelineDB",
        ) as constructor, self.assertRaises(SystemExit) as raised:
            pipeline_cli.main()

        self.assertEqual(raised.exception.code, 2)
        constructor.assert_not_called()


class TestMainProtectedPathDispatch(_FakeDbWebServerCase):
    """Protected-path commands run through the API and never open a DB.

    ``main()`` must reach the canonical route before it constructs a
    ``PipelineDB`` or configures mirrors, otherwise the command could
    still execute in the operator's own process against a tree that
    identity cannot read (issue #1063).
    """

    def _main(self, *argv_tail: str) -> tuple[int, str]:
        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "--api-base",
            self.base,
            *argv_tail,
        ]
        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), patch(
            "scripts.pipeline_cli.cli.PipelineDB",
        ) as constructor, redirect_stdout(stdout), self.assertRaises(
            SystemExit,
        ) as raised:
            pipeline_cli.main()
        constructor.assert_not_called()
        return cast(int, raised.exception.code), stdout.getvalue()

    def test_main_routes_triage_quarantine(self):
        """#1122 F1: ``triage quarantine`` reaches the real route, and
        function end-to-end through ``main()`` — not just when
        ``cmd_triage_quarantine`` is called directly."""
        root = self.enterContext(tempfile.TemporaryDirectory())
        os.makedirs(os.path.join(root, "failed_imports", "Main Wiring Orphan"))
        config_path = os.path.join(root, "config.ini")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(f"[Slskd]\ndownload_dir = {root}\n")
        self.enterContext(patch.dict(
            os.environ, {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
            clear=False,
        ))

        code, out = self._main("triage", "quarantine", "--json")

        self.assertEqual(code, 0)
        self.assertEqual(
            [f["name"] for f in json.loads(out)["folders"]],
            ["Main Wiring Orphan"],
        )

    def test_main_routes_wrong_match_delete(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)

        code, out = self._main(
            "wrong-match-delete", str(source.download_log_id),
            "--apply", "--json",
        )

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["outcome"], "deleted")
        self.assertFalse(os.path.exists(source.path))

    def test_main_routes_wrong_match_delete_group(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)

        code, out = self._main(
            "wrong-match-delete-group", str(source.request_id),
            "--apply", "--json",
        )

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["deleted"], 1)
        self.assertFalse(os.path.exists(source.path))

    def test_main_routes_every_protected_path_command_without_a_db(self):
        """Each routed command reaches HTTP; none constructs a DB handle."""
        root = self.enterContext(tempfile.TemporaryDirectory())
        source = seed_visible_wrong_match(self.db, root)
        for argv_tail in (
            ["wrong-match-delete", str(source.download_log_id), "--apply"],
            ["wrong-match-delete-group", str(source.request_id), "--apply"],
            ["replace", str(source.request_id), "--to", RELEASE_B],
            ["force-import", str(source.download_log_id)],
            ["beets-distance", str(source.download_log_id), RELEASE_B],
            ["import-preview", "--download-log-id",
             str(source.download_log_id)],
            ["triage", "quarantine"],
        ):
            with self.subTest(command=argv_tail[0]):
                # Only the dispatch boundary is under test here (``_main``
                # asserts no DB handle was constructed); each route's own
                # outcome has its pins above.
                code, _out = self._main(*argv_tail)
                self.assertIsInstance(code, int)


class TestCmdQuery(unittest.TestCase):
    def test_parser_accepts_the_documented_write_escape_hatch_shape(self):
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        args = parser.parse_args(
            ["query", "--write", "--confirm", "WRITE", "-"],
        )

        self.assertTrue(args.write)
        self.assertEqual(args.confirm, "WRITE")
        self.assertEqual(args.sql, "-")

    def test_query_renders_table_output_in_read_only_mode(self):
        db = FakePipelineDB()
        query_cur = MagicMock()
        query_cur.description = [("id",), ("artist_name",), ("details",)]
        query_cur.fetchall.return_value = [
            {"id": 7, "artist_name": "Buke and Gase", "details": {"tracks": 3}},
        ]
        db.queue_execute_results(
            MagicMock(), MagicMock(), query_cur, MagicMock())

        args = MagicMock(sql="SELECT id, artist_name, details FROM album_requests", json=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(db, args)

        # Behavior: query succeeds, output is formatted, read-only mode was used
        self.assertIsNone(rc)
        # Begin and standard-string setup bracket the query with rollback.
        self.assertEqual(len(db.execute_calls), 4)
        output = stdout.getvalue()
        self.assertIn("id | artist_name", output)
        self.assertIn('{"tracks": 3}', output)
        self.assertIn("(1 row)", output)

    def test_query_reads_sql_from_stdin_when_dash_is_passed(self):
        db = FakePipelineDB()
        query_cur = MagicMock()
        query_cur.description = [("value",)]
        query_cur.fetchall.return_value = [{"value": 1}]
        db.queue_execute_results(
            MagicMock(), MagicMock(), query_cur, MagicMock())

        args = MagicMock(sql="-", json=False)
        stdout = io.StringIO()
        with patch("sys.stdin", io.StringIO("SELECT 1 AS value")), redirect_stdout(stdout):
            pipeline_cli.cmd_query(db, args)

        # The query follows begin plus transaction-local string-mode setup.
        self.assertEqual(db.execute_calls[2][0], "SELECT 1 AS value")
        self.assertIn("value", stdout.getvalue())

    def test_query_can_emit_json(self):
        db = FakePipelineDB()
        query_cur = MagicMock()
        query_cur.description = [("id",), ("status",)]
        query_cur.fetchall.return_value = [{"id": 3, "status": "wanted"}]
        db.queue_execute_results(
            MagicMock(), MagicMock(), query_cur, MagicMock())

        args = MagicMock(sql="SELECT id, status FROM album_requests", json=True)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            pipeline_cli.cmd_query(db, args)

        self.assertEqual(
            stdout.getvalue().strip(),
            '[\n  {\n    "id": 3,\n    "status": "wanted"\n  }\n]',
        )

    def test_query_reports_sql_errors_and_cleans_up(self):
        import psycopg2

        db = FakePipelineDB()
        db.queue_execute_results(
            MagicMock(),
            MagicMock(),
            psycopg2.ProgrammingError('syntax error at or near "BOOM"'),
            MagicMock(),
        )

        args = MagicMock(sql="BOOM", json=False)
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(db, args)

        # Behavior: error reported, non-zero exit, cleanup still runs
        self.assertEqual(rc, 1)
        self.assertIn("syntax error", stderr.getvalue())
        # Cleanup call happened after begin/setup/query.
        self.assertEqual(len(db.execute_calls), 4)

    def test_query_rejects_write_mode_without_exact_confirmation_before_sql(self):
        db = FakePipelineDB()
        args = argparse.Namespace(
            sql="DELETE FROM album_requests", json=False, write=True, confirm=None,
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(db, args)

        self.assertEqual(rc, 1)
        self.assertIn("--write --confirm WRITE", stderr.getvalue())
        self.assertEqual(db.execute_calls, [])

    def test_query_executes_write_only_with_exact_confirmation(self):
        db = FakePipelineDB()
        write_cur = MagicMock()
        write_cur.description = None
        db.queue_execute_results(write_cur)
        args = argparse.Namespace(
            sql="UPDATE album_requests SET status = 'wanted'", json=False,
            write=True, confirm="WRITE",
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(db, args)

        self.assertIsNone(rc)
        self.assertEqual(
            db.execute_calls,
            [("UPDATE album_requests SET status = 'wanted'", ())],
        )
        self.assertIn("Query executed successfully.", stdout.getvalue())

    def test_query_rejects_multiple_read_only_statements_before_sql(self):
        db = FakePipelineDB()
        args = argparse.Namespace(
            sql="SET TRANSACTION READ WRITE; DELETE FROM album_requests",
            json=False, write=False, confirm=None,
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(db, args)

        self.assertEqual(rc, 1)
        self.assertIn("one statement only", stderr.getvalue())
        self.assertEqual(db.execute_calls, [])

    def test_read_only_query_lexer_allows_delimiters_in_sql_literals_and_comments(self):
        from scripts.pipeline_cli.query import _read_only_sql

        cases = (
            "SELECT ';' AS value;",
            "SELECT 'it''s; fine' AS value;",
            "SELECT 'ordinary \\; string';",
            "SELECT E'escaped \\' quote; string';",
            "SELECT e'escaped \\' quote; string';",
            'SELECT "semi;identifier" FROM "a;b";',
            "SELECT $$dollar; quoted$$;",
            "SELECT $tag$dollar; quoted$tag$;",
            "SELECT 1 -- semicolon; in a line comment\n;",
            "SELECT /* semicolon; in /* nested */ block */ 1;",
            "SELECT 1; /* trailing comment; is fine */ -- and this one\n",
        )

        for sql in cases:
            with self.subTest(sql=sql):
                self.assertEqual(_read_only_sql(sql), sql)

    def test_read_only_query_lexer_rejects_second_statement_after_comment(self):
        from scripts.pipeline_cli.query import _read_only_sql

        with self.assertRaisesRegex(ValueError, "one statement only"):
            _read_only_sql("SELECT 1; /* complete first statement */ DELETE FROM x")

    def test_standard_string_backslash_cannot_conceal_a_second_statement(self):
        from scripts.pipeline_cli.query import _read_only_sql

        # With standard_conforming_strings=on, the quote after the backslash
        # ends the ordinary string. The following COMMIT is a second statement
        # and must fail lexical validation before it reaches PostgreSQL.
        with self.assertRaisesRegex(ValueError, "one statement only"):
            _read_only_sql("SELECT 'ordinary\\'; COMMIT")

    def test_e_prefix_requires_a_sql_token_boundary(self):
        from scripts.pipeline_cli.query import _read_only_sql

        with self.assertRaisesRegex(ValueError, "one statement only"):
            _read_only_sql("SELECT nameE'ordinary\\'; COMMIT")

    def test_dollar_quote_requires_a_sql_token_boundary(self):
        from scripts.pipeline_cli.query import _read_only_sql

        for sql in (
            "SELECT before$$; COMMIT",
            "SELECT before$tag$; COMMIT",
            (
                "SELECT 1 AS before$tag$; COMMIT; DELETE FROM lexer_probe; "
                "SELECT 1 AS after$tag$"
            ),
        ):
            with self.subTest(sql=sql), self.assertRaisesRegex(
                ValueError, "one statement only",
            ):
                _read_only_sql(sql)

    def test_read_only_scope_never_retries_caller_sql_on_replacement_connection(self):
        """A post-BEGIN socket death must not replay SQL on writable B."""
        import psycopg2

        from lib.pipeline_db import PipelineDB

        class DeadConnection:
            closed = 0
            autocommit = True

            def __init__(self) -> None:
                self.executed: list[str] = []

            def cursor(self, **_kwargs):
                return DeadCursor(self)

            def rollback(self) -> None:
                raise psycopg2.InterfaceError("connection already closed")

        class DeadCursor:
            description = None

            def __init__(self, connection: DeadConnection) -> None:
                self.connection = connection

            def execute(self, sql: str) -> None:
                self.connection.executed.append(sql)
                if sql == "DELETE FROM album_requests":
                    self.connection.closed = 1
                    raise psycopg2.InterfaceError("connection lost during SQL")

            def close(self) -> None:
                return None

        connection_a = DeadConnection()
        db = PipelineDB.__new__(PipelineDB)
        db.conn = connection_a
        db._owner_session_pin = None
        args = argparse.Namespace(
            sql="DELETE FROM album_requests", json=False, write=False, confirm=None,
        )
        stderr = io.StringIO()

        with (
            patch.object(
                db,
                "_connect",
                side_effect=AssertionError("query must not request replacement connection"),
            ),
            redirect_stderr(stderr),
        ):
            rc = pipeline_cli.cmd_query(db, args)

        self.assertEqual(rc, 1)
        self.assertIn("connection lost", stderr.getvalue())
        self.assertEqual(
            connection_a.executed,
            [
                "BEGIN TRANSACTION READ ONLY",
                "SET LOCAL standard_conforming_strings = on",
                "DELETE FROM album_requests",
            ],
        )
        self.assertIs(db.conn, connection_a)


class TestCmdQueryIntegration(unittest.TestCase):
    """Integration test: read-only session rejects writes against real DB."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_query_rejects_writes(self):
        args = argparse.Namespace(
            sql="DELETE FROM album_requests", json=False, write=False, confirm=None,
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertEqual(rc, 1)
        self.assertIn("read-only", stderr.getvalue().lower())

    def test_query_allows_reads(self):
        args = argparse.Namespace(
            sql="SELECT count(*) AS n FROM album_requests", json=False, write=False, confirm=None,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertIsNone(rc)
        self.assertIn("n", stdout.getvalue())

    def test_query_executes_confirmed_write_and_reports_no_result_success(self):
        request_id = self.db.add_request(
            mb_release_id="query-write-confirmed",
            artist_name="Before",
            album_title="Before",
            source="request",
        )
        args = argparse.Namespace(
            sql=(
                "UPDATE album_requests SET artist_name = 'After' "
                f"WHERE id = {request_id}"
            ),
            json=False, write=True, confirm="WRITE",
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(self.db, args)

        self.assertIsNone(rc)
        row = self.db.get_request(request_id)
        assert row is not None
        self.assertEqual(row["artist_name"], "After")
        self.assertIn("Query executed successfully.", stdout.getvalue())

    def test_query_accepts_like_patterns_with_percent(self):
        """Issue #97: SQL containing % (e.g. ILIKE '%foo%') must not be
        interpreted as psycopg2 printf-style placeholders."""
        args = argparse.Namespace(
            sql="SELECT id FROM album_requests WHERE artist_name ILIKE '%nonexistent%'",
            json=False, write=False, confirm=None,
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertIsNone(rc, f"expected success, got stderr={stderr.getvalue()!r}")
        self.assertNotIn("IndexError", stderr.getvalue())

    def test_query_cannot_bypass_read_only_transaction_with_set_transaction(self):
        args = argparse.Namespace(
            sql="SET TRANSACTION READ WRITE; DELETE FROM album_requests",
            json=False, write=False, confirm=None,
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(self.db, args)

        self.assertEqual(rc, 1)
        self.assertIn("one statement only", stderr.getvalue())
        remaining = self.db._execute(
            "SELECT count(*)::int AS n FROM album_requests",
        ).fetchone()
        assert remaining is not None
        self.assertEqual(remaining["n"], 0)

    def test_query_rejects_identifier_adjacent_dollar_quote_before_execution(self):
        """``before$tag$`` is an identifier, not a dollar-string opener."""
        self.db._execute("CREATE TEMP TABLE lexer_probe (id INTEGER PRIMARY KEY)")
        self.db._execute("INSERT INTO lexer_probe (id) VALUES (1)")
        args = argparse.Namespace(
            sql=(
                "SELECT 1 AS before$tag$; COMMIT; DELETE FROM lexer_probe; "
                "SELECT 1 AS after$tag$"
            ),
            json=False,
            write=False,
            confirm=None,
        )
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(self.db, args)

        self.assertEqual(rc, 1)
        self.assertIn("one statement only", stderr.getvalue())
        row = self.db._execute(
            "SELECT count(*)::int AS n FROM lexer_probe",
        ).fetchone()
        assert row is not None
        self.assertEqual(row["n"], 1)


class TestCmdStatusShowsDownloading(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_status_shows_downloading_count(self):
        """pipeline-cli status includes downloading in the count display."""
        import json
        id1 = self.db.add_request(mb_release_id="dl-1", artist_name="A",
                                  album_title="B", source="request")
        state_json = json.dumps({"filetype": "flac", "enqueued_at": "now", "files": []})
        self.db.set_downloading(id1, state_json)

        counts = self.db.count_by_status()
        self.assertIn("downloading", counts)
        self.assertEqual(counts["downloading"], 1)

    def test_status_prints_processing_count(self):
        db = FakePipelineDB()
        for request_id in (2135, 2136):
            db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
            ))
        for request_id in (2137, 2138, 2139):
            db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
            ))
            handoff_automation_owner(db, request_id)
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            pipeline_cli_album_requests.cmd_status(
                db,
                argparse.Namespace(),
            )

        self.assertIn("processing", stdout.getvalue())
        self.assertIn("3", stdout.getvalue())

    def test_show_displays_active_download_state(self):
        """pipeline-cli show renders active_download_state for downloading albums."""
        import json
        id1 = self.db.add_request(mb_release_id="show-dl", artist_name="A",
                                  album_title="B", source="request")
        state = {"filetype": "flac", "enqueued_at": "2026-04-03T12:00:00+00:00",
                 "files": [{"username": "user1", "filename": "f.flac",
                            "file_dir": "d", "size": 1000}]}
        self.db.set_downloading(id1, json.dumps(state))

        req = self.db.get_request(id1)
        assert req is not None
        ads: Any = req.get("active_download_state")
        assert ads is not None
        self.assertEqual(ads["filetype"], "flac")
        self.assertEqual(len(ads["files"]), 1)


class TestCmdMarkIncomplete(unittest.TestCase):
    """Exit-code mapping for cmd_mark_incomplete (issue #1241).

    The service (tests/test_incomplete_mark_service.py) owns branch
    coverage; this checks only the CLI wrapper's outcome → exit-code map.
    """

    @patch("builtins.print")
    def test_mark_and_clear_exit_zero(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="imported", artist_name="A", album_title="B",
        ))
        self.assertEqual(
            pipeline_cli.cmd_mark_incomplete(
                db, MagicMock(id=1, clear=False)),
            0,
        )
        row = db.get_request(1)
        assert row is not None
        self.assertIsNotNone(row["marked_incomplete_at"])
        self.assertEqual(
            pipeline_cli.cmd_mark_incomplete(
                db, MagicMock(id=1, clear=True)),
            0,
        )
        row = db.get_request(1)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])

    @patch("builtins.print")
    def test_idempotent_no_ops_exit_zero(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))
        self.assertEqual(
            pipeline_cli.cmd_mark_incomplete(
                db, MagicMock(id=1, clear=True)),
            0,
        )
        pipeline_cli.cmd_mark_incomplete(
            db, MagicMock(id=1, clear=False))
        self.assertEqual(
            pipeline_cli.cmd_mark_incomplete(
                db, MagicMock(id=1, clear=False)),
            0,
        )

    @patch("builtins.print")
    def test_not_found_exits_two(self, _mock_print):
        db = FakePipelineDB()
        self.assertEqual(
            pipeline_cli.cmd_mark_incomplete(
                db, MagicMock(id=99, clear=False)),
            2,
        )

    @patch("builtins.print")
    def test_replaced_exits_four(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=2, status="replaced"))
        self.assertEqual(
            pipeline_cli.cmd_mark_incomplete(
                db, MagicMock(id=2, clear=False)),
            4,
        )


class TestQualityReplayShowsTheMark(unittest.TestCase):
    """#1257 review F7/M20: pipeline-cli quality's live-candidate replay
    prints a second, beets-whole decision when the request carries the
    operator's incomplete mark — and only then."""

    def _seed(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="mbid-1", status="imported",
        ))
        log_id = db.log_download(request_id=1, outcome="rejected")
        evidence = make_album_quality_evidence(mb_release_id="mbid-1")
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_download_log_candidate_evidence(log_id, persisted.id)
        return db

    def _replay(self, db: FakePipelineDB, *, marked: bool) -> str:
        from lib.quality import QualityRankConfig
        from scripts.pipeline_cli.quality import _print_live_candidate_replay

        with patch("builtins.print") as mock_print:
            _print_live_candidate_replay(
                db, 1,
                expected_release_id="mbid-1",
                rank_cfg=QualityRankConfig.defaults(),
                target_format=None,
                verified_lossless_target=None,
                runtime_audio_check="normal",
                q_override=None,
                gate_unavailable_reason=None,
                marked_incomplete=marked,
            )
        return "\n".join(str(call) for call in mock_print.call_args_list)

    def test_marked_replay_prints_the_beets_whole_decision(self):
        db = self._seed()
        out = self._replay(db, marked=True)
        self.assertIn("beets-whole attempt", out)
        self.assertIn("#1241 mark disregards", out)

    def test_unmarked_replay_prints_no_second_decision(self):
        db = self._seed()
        out = self._replay(db, marked=False)
        self.assertIn("Candidate evidence", out)
        self.assertNotIn("beets-whole attempt", out)


class TestCmdSetIntent(unittest.TestCase):
    def test_set_intent_rejects_initializing_request(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=791, status="initializing"))

        result = pipeline_cli.cmd_set_intent(
            db, MagicMock(id=791, intent="lossless"),
        )

        self.assertEqual(result, 4)
        self.assertEqual(db.request(791)["status"], "initializing")

    def test_processing_intent_reports_exact_owner_exit_4(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=792,
            status="wanted",
            artist_name="A",
            album_title="B",
            target_format=None,
        ))
        owner = handoff_automation_owner(db, 792)
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            result = pipeline_cli.cmd_set_intent(
                db,
                MagicMock(id=792, intent="lossless"),
            )

        self.assertEqual(result, 4)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["processing_owner"], {
            "job_id": owner.id,
            "status": owner.status,
            "preview_status": owner.preview_status,
        })
        self.assertIsNone(db.request(792)["target_format"])
    """Tests for cmd_set_intent — lossless-on-disk toggle."""

    @patch("builtins.print")
    def test_set_lossless_on_wanted(self, mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        ))
        args = MagicMock(id=1, intent="lossless")
        pipeline_cli.cmd_set_intent(db, args)
        self.assertEqual(db.update_request_fields_calls, [(1, {"target_format": "lossless"})])
        rendered = mock_print.call_args.args[0]
        self.assertIn("A - B", rendered)
        self.assertIn("lossless on disk", rendered)
        self.assertIn("target_format: None → lossless", rendered)

    @patch("builtins.print")
    def test_set_default_clears_target(self, mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        ))
        args = MagicMock(id=1, intent="default")
        pipeline_cli.cmd_set_intent(db, args)
        self.assertEqual(db.update_request_fields_calls, [(1, {"target_format": None})])
        self.assertIn(
            "default (pipeline decides)", mock_print.call_args.args[0])

    @patch("builtins.print")
    def test_set_lossless_on_imported_requeues(self, mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=2, status="imported", artist_name="A", album_title="B",
            min_bitrate=245,
        ))
        args = MagicMock(id=2, intent="lossless")
        result = pipeline_cli.cmd_set_intent(db, args)
        self.assertEqual(result, 0)
        # The transition seam itself is proven in
        # tests/test_set_intent_service.py; the wrapper test asserts the
        # domain outcome the real path produced. prev_min_bitrate is the
        # observable that distinguishes "min_bitrate passed through" from
        # "never touched" (PR2 mutant-runner survivor M5).
        row = db.request(2)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "lossless")
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(row["prev_min_bitrate"], 245)
        self.assertEqual(db.update_request_fields_calls, [(2, {"target_format": "lossless"})])
        self.assertIn("re-queued for search", mock_print.call_args.args[0])

    @patch("builtins.print")
    def test_set_intent_reports_replace_race_instead_of_success(
        self,
        mock_print,
    ):
        class RacingDB(FakePipelineDB):
            def update_request_fields(
                self,
                request_id: int,
                *,
                expected_status: str | None = None,
                **fields: Any,
            ) -> bool:
                self.supersede_request_mbid(
                    request_id,
                    new_mb_release_id="set-intent-race-new",
                    new_mb_release_group_id=None,
                    new_mb_artist_id=None,
                    new_artist_name="A",
                    new_album_title="B (correct pressing)",
                    new_year=None,
                    new_country=None,
                    new_tracks=[],
                )
                return super().update_request_fields(
                    request_id,
                    expected_status=expected_status,
                    **fields,
                )

        db = RacingDB()
        db.seed_request(make_request_row(
            id=7,
            status="wanted",
            artist_name="A",
            album_title="B",
            target_format=None,
        ))

        result = pipeline_cli.cmd_set_intent(
            db,
            MagicMock(id=7, intent="lossless"),
        )

        self.assertEqual(result, 4)
        row = db.get_request(7)
        assert row is not None
        self.assertEqual(row["status"], "replaced")
        self.assertIsNone(row["target_format"])
        rendered = "\n".join(str(call.args[0]) for call in mock_print.call_args_list)
        self.assertIn('"error": "transition_conflict"', rendered)
        self.assertNotIn("lossless on disk", rendered)

    @patch("builtins.print")
    def test_set_default_clears_stale_lossless_override(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4, status="wanted", artist_name="A", album_title="B",
            target_format="lossless", search_filetype_override="lossless",
        ))
        args = MagicMock(id=4, intent="default")
        pipeline_cli.cmd_set_intent(db, args)
        self.assertEqual(db.update_request_fields_calls, [(
            4, {"target_format": None, "search_filetype_override": None})])

    @patch("builtins.print")
    def test_set_intent_refuses_downloading_exit_4(self, mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=3, status="downloading", artist_name="A", album_title="B",
        ))
        args = MagicMock(id=3, intent="lossless")
        # Wrong-state refusal: 409/exit 4 by the repository convention
        # (issue #1278 — this used to exit 1 while the route answered 400).
        self.assertEqual(pipeline_cli.cmd_set_intent(db, args), 4)
        self.assertEqual(db.update_request_fields_calls, [])
        # Operator copy is contract too (PR2 mutant-runner survivor M12:
        # swapping this and the not-found message survived every test).
        self.assertIn("downloading", mock_print.call_args.args[0])

    @patch("builtins.print")
    def test_set_intent_not_found_exit_2(self, mock_print):
        db = FakePipelineDB()
        # no rows seeded → get_request returns None
        args = MagicMock(id=99, intent="lossless")
        self.assertEqual(pipeline_cli.cmd_set_intent(db, args), 2)
        self.assertEqual(db.update_request_fields_calls, [])
        self.assertIn("Request 99 not found", mock_print.call_args.args[0])

    def test_set_intent_vanished_row_cas_miss_exits_2(self):
        """A row deleted mid-CAS classifies not_found: the route answers
        404, so the CLI exits 2 (PR2 review — the derived-conflict-exit
        change had no adapter-level pin for its one new exit value)."""
        class VanishingDB(FakePipelineDB):
            def update_request_fields(
                self,
                request_id: int,
                *,
                expected_status: str | None = None,
                **fields: object,
            ) -> bool:
                del expected_status, fields
                self._requests.pop(request_id, None)
                return False

        db = VanishingDB()
        db.seed_request(make_request_row(
            id=8, status="wanted", artist_name="A", album_title="B",
        ))
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            result = pipeline_cli.cmd_set_intent(
                db, MagicMock(id=8, intent="lossless"),
            )
        self.assertEqual(result, 2)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["reason"], "not_found")

    def test_set_intent_replaced_reports_frozen_conflict_exit_4(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=11, status="replaced", artist_name="A", album_title="B",
        ))
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            result = pipeline_cli.cmd_set_intent(
                db, MagicMock(id=11, intent="lossless"),
            )
        self.assertEqual(result, 4)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["error"], "transition_conflict")
        self.assertEqual(payload["reason"], "invalid_edge")
        self.assertEqual(db.request(11)["status"], "replaced")


class TestCmdRepairSpectral(unittest.TestCase):
    """Regression tests for the rank-model repair flow."""

    def test_repair_spectral_reloads_full_request_metadata(self):
        """Repair selects by audit scalars but decides from linked evidence."""
        from lib.quality import AudioQualityMeasurement, VerifiedLosslessProof

        cfg_fd, cfg_path = tempfile.mkstemp(prefix="quality-ranks-", suffix=".ini")
        os.close(cfg_fd)
        try:
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write("[Quality Ranks]\n")

            # Mirror the real repair query shape: it does NOT include
            # mb_release_id/final_format, so the command must re-load the
            # full request row instead of depending on the partial result.
            candidate_cur = MagicMock()
            candidate_cur.fetchall.return_value = [{
                "id": 42,
                "artist_name": "Artist",
                "album_title": "Album",
                "min_bitrate": 207,
                "current_spectral_grade": "genuine",
                "current_spectral_bitrate": 96,
                "last_download_spectral_bitrate": None,
                "last_download_spectral_grade": None,
                "verified_lossless": True,
            }]
            delete_cur = MagicMock()
            delete_cur.fetchall.return_value = []
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                mb_release_id="mbid-123",
                artist_name="Artist",
                album_title="Album",
                min_bitrate=207,
                current_spectral_grade="genuine",
                current_spectral_bitrate=96,
                verified_lossless=True,
                final_format="mp3 v0",
            ))
            evidence = make_album_quality_evidence(
                mb_release_id="mbid-123",
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=245,
                    median_bitrate_kbps=245,
                    format="MP3",
                    is_cbr=False,
                    spectral_grade="genuine",
                    spectral_subject="source",
                    spectral_provenance="carried",
                ),
                verified_lossless_proof=VerifiedLosslessProof(
                    provenance="carried",
                    source="flac",
                    classifier="spectral_verified_lossless",
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db.set_request_current_evidence(42, persisted.id)
            db.queue_execute_results(candidate_cur, delete_cur)

            args = MagicMock(dry_run=False)
            stdout = io.StringIO()
            with patch.dict(os.environ, {"CRATEDIGGER_RUNTIME_CONFIG": cfg_path}), \
                 redirect_stdout(stdout):
                pipeline_cli.cmd_repair_spectral(db, args)

            output = stdout.getvalue()
            self.assertIn("quality_gate_decision → accept", output)
            self.assertIn("→ transitioned to imported", output)
            self.assertEqual(len(db.execute_calls), 2)
            repaired = db.request(42)
            self.assertEqual(repaired["status"], "imported")
            self.assertEqual(repaired["min_bitrate"], 245)
            self.assertIsNone(repaired["last_download_spectral_bitrate"])
            self.assertIsNone(repaired["current_spectral_bitrate"])
        finally:
            os.unlink(cfg_path)

    def test_repair_spectral_rechecks_processing_owner_under_import_lock(self):
        candidate_cur = MagicMock()
        candidate_cur.fetchall.return_value = [{
            "id": 42,
            "artist_name": "Artist",
            "album_title": "Album",
            "min_bitrate": 207,
            "current_spectral_grade": "genuine",
            "current_spectral_bitrate": 96,
            "last_download_spectral_bitrate": None,
            "last_download_spectral_grade": None,
            "verified_lossless": False,
        }]
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="repair-processing-owner",
            artist_name="Artist",
            album_title="Album",
            current_spectral_grade="genuine",
            current_spectral_bitrate=96,
        ))
        db.queue_execute_results(candidate_cur)
        owner_id: int | None = None
        injected = False

        def acquire(_namespace: int, _key: int) -> bool:
            nonlocal injected, owner_id
            if not injected:
                injected = True
                owner_id = handoff_automation_owner(db, 42).id
            return True

        db.set_advisory_lock_result(acquire)
        stdout = io.StringIO()
        with patch(
            "scripts.pipeline_cli.quality._load_runtime_rank_config",
            return_value=MagicMock(),
        ), redirect_stdout(stdout):
            result = pipeline_cli.cmd_repair_spectral(
                db,
                MagicMock(dry_run=False),
            )

        self.assertEqual(result, 4)
        payload = json.loads(stdout.getvalue().strip().splitlines()[-1])
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["processing_owner"], {
            "job_id": owner_id,
            "status": "queued",
            "preview_status": "waiting",
        })
        self.assertEqual(db.request(42)["status"], "processing")
        self.assertEqual(len(db.execute_calls), 1)


def _invoke_cmd_quality(
    db: FakePipelineDB, request_id: int, *, runtime_target: str | None,
) -> str:
    """Shared ``cmd_quality`` invocation seam.

    One ``cast(Any, db)`` call site for every test in this module that
    drives ``cmd_quality`` (``TestCmdQuality`` and
    ``TestCmdQualityLiveCandidateReplay``) — the tests escape-hatch freeze
    (issue #784) counts lexical occurrences, so a second inline copy of this
    same bridge would be a new escape hatch, not a reused one.
    """
    from lib.quality import QualityRankConfig

    stdout = io.StringIO()
    with patch("scripts.pipeline_cli.quality._load_runtime_rank_config",
               return_value=QualityRankConfig.defaults()), \
         patch("scripts.pipeline_cli.quality._load_runtime_verified_lossless_target",
               return_value=runtime_target or ""), \
         redirect_stdout(stdout):
        pipeline_cli.cmd_quality(db, MagicMock(id=request_id))
    return stdout.getvalue()


class TestCmdQuality(unittest.TestCase):
    """Regression tests for pipeline-cli quality simulator parity.

    These tests drive the real :func:`lib.quality.full_pipeline_decision`
    (no stub on the pure simulator) and assert against the printed output.
    The simulator's own coverage lives in
    ``tests/test_quality_classification.py``; this test class is about the
    CLI wrapper: that ``cmd_quality`` threads runtime config and request
    fields into the scenarios it prints, and that the displayed quality
    gate label agrees with the gate verdict.
    """

    def _run_quality(
        self,
        request_row,
        *,
        runtime_target: str | None,
        beets_info: Any = ...,
        beets_error: Exception | None = None,
        current_evidence: Any | None = None,
        auto_link: bool = True,
    ):
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        db.seed_request(request_row)
        if current_evidence is not None:
            db.upsert_album_quality_evidence(current_evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=current_evidence.mb_release_id,
                snapshot_fingerprint=current_evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db.set_request_current_evidence(request_row["id"], persisted.id)

        if beets_info is ...:
            beets_info = SimpleNamespace(
                is_cbr=False,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                format="MP3",
            )
        if (
            auto_link
            and current_evidence is None
            and beets_info is not None
            and beets_error is None
        ):
            current_evidence = make_album_quality_evidence(
                mb_release_id=request_row["mb_release_id"],
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=request_row.get("min_bitrate"),
                    avg_bitrate_kbps=beets_info.avg_bitrate_kbps,
                    median_bitrate_kbps=beets_info.median_bitrate_kbps,
                    format=beets_info.format,
                    is_cbr=beets_info.is_cbr,
                    spectral_grade=request_row.get("current_spectral_grade"),
                    spectral_bitrate_kbps=request_row.get(
                        "current_spectral_bitrate"
                    ),
                ),
            )
            db.upsert_album_quality_evidence(current_evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=current_evidence.mb_release_id,
                snapshot_fingerprint=current_evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db.set_request_current_evidence(request_row["id"], persisted.id)

        return _invoke_cmd_quality(
            db, request_row["id"], runtime_target=runtime_target)

    def _bare_mp3_request(self, *, request_id: int):
        return make_request_row(
            id=request_id,
            status="imported",
            mb_release_id=f"mbid-missing-{request_id}",
            artist_name="Missing Beets Artist",
            album_title="Missing Beets Album",
            min_bitrate=256,
            current_spectral_grade="genuine",
            verified_lossless=False,
            final_format="MP3",
        )

    def test_quality_bare_mp3_missing_beets_mode_is_unavailable(self):
        output = self._run_quality(
            self._bare_mp3_request(request_id=4136),
            runtime_target=None,
            beets_info=None,
        )

        self.assertIn("Quality gate:  UNAVAILABLE", output)
        self.assertIn("linked current evidence unavailable", output)
        self.assertIn("What would happen if we downloaded", output)
        self.assertNotIn("Quality gate:  DONE", output)
        self.assertNotIn("Quality gate:  NEEDS", output)
        self.assertNotIn("is_cbr=False", output)

    def test_quality_bare_mp3_beets_exception_is_unavailable(self):
        output = self._run_quality(
            self._bare_mp3_request(request_id=4137),
            runtime_target=None,
            beets_error=RuntimeError("beets unavailable"),
        )

        self.assertIn("Quality gate:  UNAVAILABLE", output)
        self.assertIn("linked current evidence unavailable", output)
        self.assertIn("What would happen if we downloaded", output)
        self.assertNotIn("Traceback", output)
        self.assertNotIn("is_cbr=False", output)

    def test_quality_threads_runtime_verified_lossless_target(self):
        request_row = make_request_row(
            id=7,
            status="imported",
            mb_release_id="mbid-123",
            artist_name="Artist",
            album_title="Album",
            min_bitrate=245,
            current_spectral_grade="genuine",
            verified_lossless=True,
            final_format="mp3 v0",
            target_format=None,
        )

        output = self._run_quality(request_row, runtime_target="opus 128")

        self.assertIn("  Rank config: metric=avg\n", output)
        # Header line confirms the runtime target was read.
        self.assertIn("Verified-lossless output: opus 128", output)
        # Scenario labels weave the same target through `_quality_preview_target_label`.
        self.assertIn("Genuine FLAC → opus 128 (high bitrate):", output)

    def test_quality_threads_request_target_format(self):
        request_row = make_request_row(
            id=8,
            status="imported",
            mb_release_id="mbid-123",
            artist_name="Artist",
            album_title="Album",
            min_bitrate=245,
            verified_lossless=True,
            final_format="mp3 v0",
            target_format="flac",
        )

        output = self._run_quality(request_row, runtime_target="opus 128")

        # Request's target_format=flac wins over the runtime opus 128 target.
        self.assertIn("Verified-lossless output: flac", output)
        self.assertIn("Genuine FLAC → flac (high bitrate):", output)

    def test_suspect_flac_preview_scenarios_stay_provisional(self):
        """The real CLI displays both source-probe scenarios as provisional."""
        request_row = make_request_row(
            id=81,
            status="imported",
            mb_release_id="mbid-quality-suspect-fixtures",
            artist_name="Artist",
            album_title="Album",
            min_bitrate=320,
            verified_lossless=False,
            final_format="MP3",
        )
        output = self._run_quality(request_row, runtime_target="opus 128")

        lines = output.splitlines()
        for scenario in (
            "Suspect FLAC (transcode, 190kbps)",
            "Suspect FLAC (transcode, 245kbps)",
        ):
            with self.subTest(scenario=scenario):
                index = lines.index(f"    {scenario}:")
                self.assertIn(
                    "IMPORT, denylist, keep searching (final: wanted)",
                    lines[index + 1],
                )
                self.assertIn(
                    "stage2_import=provisional_lossless_upgrade",
                    lines[index + 2],
                )

    def test_quality_bare_mp3_cbr_genuine_below_transparent_needs_upgrade(self):
        """Live request 4135: genuine CBR 256 stays on full tiers."""
        from lib.beets_db import AlbumInfo

        request_row = make_request_row(
            id=4135,
            status="imported",
            mb_release_id="mbid-johnny-x",
            artist_name="The Bouncing Souls",
            album_title="Johnny X",
            min_bitrate=256,
            current_spectral_grade="genuine",
            verified_lossless=False,
            final_format="MP3",
        )
        cbr_info = AlbumInfo(
            album_id=4135,
            track_count=4,
            min_bitrate_kbps=256,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=256,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/The Bouncing Souls/Johnny X",
        )

        output = self._run_quality(
            request_row,
            runtime_target=None,
            beets_info=cbr_info,
        )

        self.assertIn("NEEDS UPGRADE", output)
        self.assertIn("(rank=EXCELLENT)", output)
        self.assertIn("is_cbr=True", output)

    def test_quality_bare_mp3_at_319_needs_upgrade_not_lossless(self):
        """Live request 8499, and the collapse's operator-visible consequence.

        A genuine-graded 319 kbps MP3 with no explicit label reached
        TRANSPARENT on the retired VBR ladder (transparent >= 245) and the
        gate narrowed the search to lossless-only. One ladder puts the same
        measurement in EXCELLENT (transparent = 320), so the gate now asks
        for an upgrade instead — the pipeline resumes hunting a record it
        previously treated as finished, which is the intended direction.
        """
        from lib.beets_db import AlbumInfo

        request_row = make_request_row(
            id=8499,
            status="imported",
            mb_release_id="mbid-vbr-control",
            artist_name="VBR Artist",
            album_title="VBR Album",
            min_bitrate=319,
            current_spectral_grade="genuine",
            verified_lossless=False,
            final_format="MP3",
        )
        vbr_info = AlbumInfo(
            album_id=8499,
            track_count=10,
            min_bitrate_kbps=319,
            avg_bitrate_kbps=319,
            median_bitrate_kbps=319,
            format="MP3",
            is_cbr=False,
            album_path="/Beets/VBR Artist/VBR Album",
        )

        output = self._run_quality(
            request_row,
            runtime_target=None,
            beets_info=vbr_info,
        )

        self.assertIn("Quality gate:  NEEDS UPGRADE", output)
        self.assertIn("(rank=EXCELLENT)", output)
        self.assertIn("is_cbr=False", output)

    def test_backfill_uses_linked_current_evidence_not_request_scalar(self):
        from lib.beets_db import AlbumInfo
        from lib.quality import AudioQualityMeasurement

        request_row = make_request_row(
            id=8500,
            status="imported",
            mb_release_id="mbid-linked-transparent",
            artist_name="Linked Artist",
            album_title="Linked Album",
            min_bitrate=320,
            # Deliberately stale; the linked row below is authoritative.
            current_spectral_grade="suspect",
            verified_lossless=False,
            final_format="MP3",
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-linked-transparent",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )
        beets_info = AlbumInfo(
            album_id=8500,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Linked Artist/Linked Album",
        )

        output = self._run_quality(
            request_row,
            runtime_target=None,
            beets_info=beets_info,
            current_evidence=evidence,
        )

        self.assertIn(
            "Backfill:      would set search_filetype_override='lossless'",
            output,
        )

    def test_backfill_linked_evidence_is_independent_of_beets_display(self):
        """Complete linked evidence remains actionable when Beets is absent."""
        from lib.quality import AudioQualityMeasurement

        request_row = make_request_row(
            id=8502,
            status="imported",
            mb_release_id="mbid-linked-without-beets",
            artist_name="Linked Artist",
            album_title="Linked Without Beets",
            min_bitrate=320,
            current_spectral_grade=None,
            verified_lossless=False,
            final_format="MP3",
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-linked-without-beets",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

        output = self._run_quality(
            request_row,
            runtime_target=None,
            beets_info=None,
            current_evidence=evidence,
        )

        self.assertIn("Quality gate:  NEEDS LOSSLESS", output)
        self.assertIn(
            "Backfill:      would set search_filetype_override='lossless'",
            output,
        )

    def test_backfill_replaces_full_upgrade_ladder_from_linked_evidence(self):
        """Only an already-lossless override makes positive backfill a no-op."""
        from lib.quality import QUALITY_UPGRADE_TIERS, AudioQualityMeasurement

        request_row = make_request_row(
            id=8503,
            status="imported",
            mb_release_id="mbid-linked-upgrade-ladder",
            artist_name="Linked Artist",
            album_title="Linked Upgrade Ladder",
            min_bitrate=320,
            current_spectral_grade=None,
            verified_lossless=False,
            final_format="MP3",
            search_filetype_override=QUALITY_UPGRADE_TIERS,
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-linked-upgrade-ladder",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

        output = self._run_quality(
            request_row,
            runtime_target=None,
            current_evidence=evidence,
        )

        self.assertIn(
            "Backfill:      would set search_filetype_override='lossless'",
            output,
        )
        self.assertNotIn("Backfill:      not needed", output)

        already_lossless = dict(request_row)
        already_lossless["id"] = 8504
        already_lossless["search_filetype_override"] = "lossless"
        output = self._run_quality(
            already_lossless,
            runtime_target=None,
            current_evidence=evidence,
        )
        self.assertIn("Backfill:      not needed", output)

    def test_backfill_does_not_use_unlinked_request_scalar(self):
        from lib.beets_db import AlbumInfo

        request_row = make_request_row(
            id=8501,
            status="imported",
            mb_release_id="mbid-unlinked-transparent",
            artist_name="Unlinked Artist",
            album_title="Unlinked Album",
            min_bitrate=320,
            current_spectral_grade="genuine",
            verified_lossless=False,
            final_format="MP3",
        )
        beets_info = AlbumInfo(
            album_id=8501,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Unlinked Artist/Unlinked Album",
        )

        output = self._run_quality(
            request_row,
            runtime_target=None,
            beets_info=beets_info,
            auto_link=False,
        )

        self.assertIn("Backfill:      won't fire", output)

    def test_quality_label_matches_gate_after_spectral_clamp(self):
        """AFX Analord 09 regression: displayed rank label must match the gate verdict.

        Reproduces the exact post-deploy scenario: VBR ~245kbps + spectral=160
        likely_transcode. Without the spectral clamp, the displayed label
        showed `rank=EXCELLENT` next to `NEEDS UPGRADE` — self-contradictory.
        After the fix, the displayed rank is the post-clamp rank that the
        gate actually used.
        """
        from lib.quality import AudioQualityMeasurement

        request_row = make_request_row(
            id=9,
            status="imported",
            mb_release_id="mbid-afx",
            artist_name="AFX",
            album_title="Analord 09",
            min_bitrate=213,
            current_spectral_bitrate=160,
            current_spectral_grade="likely_transcode",
            verified_lossless=False,
            final_format=None,
        )

        evidence = make_album_quality_evidence(
            mb_release_id="mbid-afx",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=213,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                format="MP3",
                is_cbr=False,
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=160,
            ),
        )
        output = self._run_quality(
            request_row,
            runtime_target=None,
            beets_info=None,
            current_evidence=evidence,
        )
        # The gate must say NEEDS UPGRADE (not DONE) — real quality_gate_decision
        # called by cmd_quality classifies the album below EXCELLENT.
        self.assertIn("NEEDS UPGRADE", output)
        # And the displayed rank must agree — post-clamp 160kbps lands ACCEPTABLE.
        # Use the parenthesized form so this matches the decision rank itself.
        self.assertIn("(rank=ACCEPTABLE)", output)
        self.assertNotIn("(rank=TRANSPARENT)", output)
        self.assertNotIn("(rank=EXCELLENT)", output)

    def test_quality_prints_cd_rip_algorithm_offset_and_provider_confidence(self):
        from lib.quality import (
            AccurateRipBitMatch,
            AudioQualityMeasurement,
            CdRipBitVerification,
            CdTocIdentity,
            CtdbWholeDiscMatch,
        )
        from scripts.pipeline_cli.quality import _print_proof_gate_verdict

        cd_rip = CdRipBitVerification(
            toc=CdTocIdentity(
                track_offsets_sectors=[0, 470],
                leadout_sector=950,
                accuraterip_id="0000058c-00000b18-02000c02",
                musicbrainz_disc_id="exact-disc-id",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/example.bin",
                checksum_version="arv2",
                read_offset_samples=108,
                track_confidences=[38, 37],
                track_checksums=[0x12345678, 0x90ABCDEF],
                response_sha256="a" * 64,
            ),
            ctdb=CtdbWholeDiscMatch(
                provider="ctdb",
                url="https://db.cue.tools/example",
                entry_id="ctdb-123",
                confidence=8026,
                crc32=0xA1B2C3D4,
                stride_samples=5880,
                response_toc_sectors=[0, 470, 950],
                response_toc_shift_sectors=0,
                response_sha256="b" * 64,
            ),
        )
        evidence = make_album_quality_evidence(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                format="FLAC",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            _print_proof_gate_verdict("IN", evidence)

        output = stdout.getvalue()
        self.assertIn("proved by exact CD rip bit match", output)
        self.assertIn("algorithm=cd-rip-bit-verifier-v1", output)
        self.assertIn("ARV2 offset=+108", output)
        self.assertIn("track confidences=[38,37]", output)
        self.assertIn("track checksums=[12345678,90abcdef]", output)
        self.assertIn(f"response-sha256={'a' * 64}", output)
        self.assertIn("confidence=8026", output)
        self.assertIn("whole-disc crc32=a1b2c3d4", output)
        self.assertIn("response-toc=[0, 470, 950]", output)
        self.assertIn("toc-shift=0", output)
        self.assertIn(f"response-sha256={'b' * 64}", output)


class TestCmdQualityLiveCandidateReplay(unittest.TestCase):
    """pipeline-cli quality <id>'s live-candidate replay tier (issue #813
    tooling tier). Every synthetic scenario in TestCmdQuality is a canned
    grade/bitrate combo; this tier replays the request's actual last
    download_log candidate evidence through the real production decider
    (full_pipeline_decision_from_evidence) — PR #812's Mark DeNardo
    verification needed an offline decider run because no tier reproduced
    the exact live candidate; this one does.
    """

    def _run(self, db: FakePipelineDB, request_id: int) -> str:
        return _invoke_cmd_quality(db, request_id, runtime_target=None)

    def test_no_candidate_evidence_shows_diagnostic(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5001, mb_release_id="mbid-no-candidate", status="wanted",
        ))

        output = self._run(db, 5001)

        self.assertIn("What the last real candidate actually decided", output)
        self.assertIn(
            "no download attempt has left measured candidate evidence yet",
            output,
        )

    def test_replays_the_actual_last_candidate_decision(self):
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        request_row = make_request_row(
            id=5002, mb_release_id="mbid-replay", status="wanted",
            min_bitrate=320, current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
        )
        db.seed_request(request_row)

        # The installed ("current") copy: MP3 320 CBR, likely_transcode/160.
        current = make_album_quality_evidence(
            mb_release_id="mbid-replay",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320,
                format="MP3", is_cbr=True,
                spectral_grade="likely_transcode", spectral_bitrate_kbps=160,
            ),
        )
        db.upsert_album_quality_evidence(current)
        current_persisted = db.find_album_quality_evidence(
            mb_release_id=current.mb_release_id,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert current_persisted is not None and current_persisted.id is not None
        db.set_request_current_evidence(5002, current_persisted.id)

        # The actual last candidate: an identical-quality MP3 320 CBR
        # transcode — a real downgrade (Tyler Lamberts / Deerhunter shape).
        log_id = db.log_download(request_id=5002, outcome="rejected")
        candidate = make_album_quality_evidence(
            mb_release_id="mbid-replay",
            source_path="/tmp/candidate-source",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320,
                format="MP3", is_cbr=True,
                spectral_grade="likely_transcode", spectral_bitrate_kbps=160,
            ),
        )
        db.upsert_album_quality_evidence(candidate)
        candidate_persisted = db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert (
            candidate_persisted is not None
            and candidate_persisted.id is not None
        )
        db.set_download_log_candidate_evidence(log_id, candidate_persisted.id)

        output = self._run(db, 5002)

        self.assertIn("What the last real candidate actually decided", output)
        self.assertIn(f"Candidate evidence #{candidate_persisted.id}", output)
        # Real decision: an identical-rank transcode is a downgrade, and
        # issue #813 Finding 2 requires the display to show the denylist
        # production actually applies for that decision.
        self.assertIn("REJECT, denylist", output)
        self.assertIn("stage2_import=downgrade", output)

    def test_shared_current_candidate_projects_source_lineage(self):
        """CLI replay must use the same source semantics as the importer."""
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5013,
            mb_release_id="mbid-cli-shared",
            status="wanted",
        ))
        shared = make_album_quality_evidence(
            mb_release_id="mbid-cli-shared",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="Opus",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
                was_converted_from="flac",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(shared)
        stored = db.find_album_quality_evidence(
            mb_release_id=shared.mb_release_id,
            snapshot_fingerprint=shared.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(db.set_request_current_evidence(5013, stored.id))
        log_id = db.log_download(request_id=5013, outcome="rejected")
        db.set_download_log_candidate_evidence(log_id, stored.id)

        output = self._run(db, 5013)
        replay = output.split(
            "What the last real candidate actually decided:", 1
        )[1]

        self.assertIn("stage2_import=downgrade", replay)
        self.assertIn("audit-only for this codec", replay)
        self.assertNotIn("proof gate IN: tier 1", replay)

    def test_poisoned_candidate_evidence_is_not_replayed_or_printed(self):
        from lib.quality import (
            AccurateRipBitMatch,
            AudioQualityMeasurement,
            CdRipBitVerification,
            CdTocIdentity,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5011, mb_release_id="mbid-cli-exact", status="wanted",
        ))
        log_id = db.log_download(request_id=5011, outcome="rejected")
        cd_rip = CdRipBitVerification(
            toc=CdTocIdentity(
                track_offsets_sectors=[0],
                leadout_sector=470,
                accuraterip_id="000001d6-000003ac-02000601",
                musicbrainz_disc_id="sibling-disc-id",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/sibling.bin",
                checksum_version="arv1",
                read_offset_samples=0,
                track_confidences=[99],
                track_checksums=[0xDEADBEEF],
                response_sha256="c" * 64,
            ),
        )
        sibling = make_album_quality_evidence(
            mb_release_id="mbid-cli-sibling",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                avg_bitrate_kbps=900,
                format="FLAC",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )
        db.upsert_album_quality_evidence(sibling)
        stored = db.find_album_quality_evidence(
            mb_release_id=sibling.mb_release_id,
            snapshot_fingerprint=sibling.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        db.set_download_log_candidate_evidence(log_id, stored.id)

        output = self._run(db, 5011)
        replay = output.split(
            "What the last real candidate actually decided:", 1
        )[1]

        self.assertIn("exact release identity does not match", replay)
        self.assertNotIn("measured FLAC", replay)
        self.assertNotIn("proof gate IN", replay)
        self.assertNotIn("CD rip IN", replay)
        self.assertNotIn("deadbeef", replay)

    def test_poisoned_current_evidence_is_excluded_from_candidate_replay(self):
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5012, mb_release_id="mbid-cli-current-exact", status="wanted",
        ))
        sibling_current = make_album_quality_evidence(
            mb_release_id="mbid-cli-current-sibling",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=64,
                avg_bitrate_kbps=64,
                format="AAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=32,
            ),
            codec="aac",
            container="m4a",
            storage_format="AAC",
        )
        db.upsert_album_quality_evidence(sibling_current)
        stored_current = db.find_album_quality_evidence(
            mb_release_id=sibling_current.mb_release_id,
            snapshot_fingerprint=sibling_current.snapshot_fingerprint,
        )
        assert stored_current is not None and stored_current.id is not None
        db.set_request_current_evidence(5012, stored_current.id)

        log_id = db.log_download(request_id=5012, outcome="success")
        candidate = make_album_quality_evidence(
            mb_release_id="mbid-cli-current-exact",
            source_path="/tmp/exact-candidate-source",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
            ),
        )
        db.upsert_album_quality_evidence(candidate)
        stored_candidate = db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert stored_candidate is not None and stored_candidate.id is not None
        db.set_download_log_candidate_evidence(log_id, stored_candidate.id)

        output = self._run(db, 5012)
        replay = output.split(
            "What the last real candidate actually decided:", 1
        )[1]

        self.assertIn(
            "Quality gate:  UNAVAILABLE (linked current evidence unavailable)",
            output,
        )
        self.assertNotIn("min_bitrate=64kbps", output)
        self.assertIn(
            f"Candidate evidence #{stored_candidate.id}", replay
        )
        self.assertIn("different exact release identity", replay)
        self.assertNotIn("proof gate HAVE", replay)
        self.assertNotIn("spectral grade 'likely_transcode'", replay)

    def test_a_stage1_reject_shows_what_stage2_would_have_decided(self):
        """Issue #829 Phase 5 PR2d: a Stage-1 spectral reject short-circuits
        before Stage 2 runs, so the printed chain stops at
        ``stage1_spectral=reject`` and says nothing about whether the
        candidate was actually an upgrade. The decider now reports that
        counterfactual and the operator surface shows it."""
        from lib.quality import AlbumQualityEvidenceFile, AudioQualityMeasurement

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5010, mb_release_id="mbid-stage1-reject", status="wanted",
            min_bitrate=320, current_spectral_grade="likely_transcode",
            current_spectral_bitrate=192,
        ))

        current = make_album_quality_evidence(
            mb_release_id="mbid-stage1-reject",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320,
                format="MP3", is_cbr=True,
                spectral_grade="likely_transcode", spectral_bitrate_kbps=192,
            ),
        )
        db.upsert_album_quality_evidence(current)
        current_persisted = db.find_album_quality_evidence(
            mb_release_id=current.mb_release_id,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert current_persisted is not None and current_persisted.id is not None
        db.set_request_current_evidence(5010, current_persisted.id)

        log_id = db.log_download(request_id=5010, outcome="rejected")
        candidate = make_album_quality_evidence(
            mb_release_id="mbid-stage1-reject",
            source_path="/tmp/stage1-reject-source",
            # Evidence is content-addressed by (mb_release_id,
            # snapshot_fingerprint), and the fingerprint comes from ``files``
            # — a shared default file list would collapse the candidate and
            # the installed copy into ONE row.
            files=[AlbumQualityEvidenceFile(
                relative_path="01 - candidate.mp3", size_bytes=222,
                mtime_ns=2, extension="mp3", container="mp3", codec="mp3",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256, avg_bitrate_kbps=256,
                format="MP3", is_cbr=True,
                spectral_grade="likely_transcode", spectral_bitrate_kbps=128,
            ),
        )
        db.upsert_album_quality_evidence(candidate)
        candidate_persisted = db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert (
            candidate_persisted is not None
            and candidate_persisted.id is not None
        )
        db.set_download_log_candidate_evidence(log_id, candidate_persisted.id)

        output = self._run(db, 5010)

        self.assertIn("stage1_spectral=reject", output)
        self.assertIn(
            "if stage 1 had deferred: stage2=downgrade, scoring the candidate "
            "worse",
            output,
        )

    def test_uses_the_newest_candidate_when_several_exist(self):
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5003, mb_release_id="mbid-latest", status="wanted",
        ))

        # Distinct file snapshots (evidence is content-addressed by
        # (mb_release_id, snapshot_fingerprint) — a shared default file
        # list would collide the two rows into one).
        from lib.quality import AlbumQualityEvidenceFile

        older_log_id = db.log_download(request_id=5003, outcome="rejected")
        older = make_album_quality_evidence(
            mb_release_id="mbid-latest",
            source_path="/tmp/older-source",
            files=[AlbumQualityEvidenceFile(
                relative_path="01 - older.mp3", size_bytes=111,
                mtime_ns=1, extension="mp3", container="mp3", codec="mp3",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128, format="MP3", is_cbr=True,
            ),
        )
        db.upsert_album_quality_evidence(older)
        older_persisted = db.find_album_quality_evidence(
            mb_release_id=older.mb_release_id,
            snapshot_fingerprint=older.snapshot_fingerprint,
        )
        assert older_persisted is not None and older_persisted.id is not None
        db.set_download_log_candidate_evidence(
            older_log_id, older_persisted.id)

        newer_log_id = db.log_download(request_id=5003, outcome="success")
        newer = make_album_quality_evidence(
            mb_release_id="mbid-latest",
            source_path="/tmp/newer-source",
            files=[AlbumQualityEvidenceFile(
                relative_path="01 - newer.mp3", size_bytes=222,
                mtime_ns=2, extension="mp3", container="mp3", codec="mp3",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, format="MP3", is_cbr=True,
            ),
        )
        db.upsert_album_quality_evidence(newer)
        newer_persisted = db.find_album_quality_evidence(
            mb_release_id=newer.mb_release_id,
            snapshot_fingerprint=newer.snapshot_fingerprint,
        )
        assert newer_persisted is not None and newer_persisted.id is not None
        db.set_download_log_candidate_evidence(
            newer_log_id, newer_persisted.id)

        output = self._run(db, 5003)

        self.assertIn(f"Candidate evidence #{newer_persisted.id}", output)
        self.assertNotIn(f"Candidate evidence #{older_persisted.id}", output)

    def test_policy_incomplete_candidate_shows_diagnostic_not_a_crash(self):
        """A legacy/partial evidence row (e.g. missing bitrate + format)
        must not crash the CLI — full_pipeline_decision_from_evidence's
        _require_evidence_ready raises ValueError; this is a read-only
        diagnostic and must degrade gracefully."""
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5004, mb_release_id="mbid-incomplete", status="wanted",
        ))
        log_id = db.log_download(request_id=5004, outcome="rejected")
        candidate = make_album_quality_evidence(
            mb_release_id="mbid-incomplete",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=None, avg_bitrate_kbps=None,
                median_bitrate_kbps=None, format=None,
            ),
        )
        db.upsert_album_quality_evidence(candidate)
        persisted = db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_download_log_candidate_evidence(log_id, persisted.id)

        output = self._run(db, 5004)

        self.assertIn(f"Candidate evidence #{persisted.id}", output)
        self.assertIn("could not decide", output)
        self.assertNotIn("Traceback", output)


class _ForensicsDB(FakePipelineDB):
    """Minimal FakePipelineDB subclass that lets each test return a
    fixed list from ``get_search_history`` without forcing tests to
    encode/decode the ``candidates`` JSONB blob.

    The four tests below historically used MagicMock to inject the raw
    list-of-dicts shape that ``cmd_show`` consumes. ``log_search`` +
    ``get_search_history`` decode round-trips a JSON-encoded
    ``list[CandidateScore]`` — fine for live code, but here the test
    payload IS the shape ``cmd_show`` reads. Overriding the read
    method lets the original dict fixtures stay readable while still
    satisfying #290's "no MagicMock as a stateful collaborator" rule.
    """

    def __init__(self) -> None:
        super().__init__()
        self._stub_search_history: list[dict[str, object]] = []
        self._stub_download_history: list[dict[str, object]] | None = None

    def set_stub_search_history(self, rows: list[dict[str, object]]) -> None:
        self._stub_search_history = list(rows)

    def get_search_history(self, request_id: int) -> list[dict[str, object]]:
        return [row for row in self._stub_search_history
                if row.get("request_id") == request_id]

    def set_stub_download_history(self, rows: list[dict[str, object]]) -> None:
        self._stub_download_history = list(rows)

    def get_download_history(
        self, request_id: int,
    ) -> "list[DownloadLogWithEvidenceRow]":
        if self._stub_download_history is None:
            return super().get_download_history(request_id)
        return cast("list[DownloadLogWithEvidenceRow]", [
            row for row in self._stub_download_history
            if row.get("request_id") == request_id
        ])


class TestCmdShowSearchForensics(unittest.TestCase):
    """Unit cover for U7 forensic surfacing in `pipeline-cli show`.

    No TEST_DB_DSN required — drives ``cmd_show`` against a
    ``_ForensicsDB`` (typed FakePipelineDB subclass) seeded with the
    forensic blob the test author cared about, and verifies the printed
    text contains variant + final_state + the top-3
    candidate table from the JSONB blob.
    """

    def _row(self, **overrides):
        row = make_request_row(**overrides)
        return row

    def _capture(self, db, request_id, beets=None):
        beets = beets or FakeBeetsDB()
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            pipeline_cli.cmd_show(
                db,
                argparse.Namespace(
                    id=request_id, beets_db=None, beets_directory=None,
                ),
                open_beets_fn=lambda **_kwargs: beets,
            )
        return stdout.getvalue()

    def test_show_renders_fresh_current_path_not_request_cache(self):
        db = _ForensicsDB()
        db.seed_request(self._row(
            id=1844,
            status="imported",
            mb_release_id=RELEASE_A,
        ))
        beets = FakeBeetsDB(library_root="/current/library")
        beets.set_album_ids_for_release(RELEASE_A, [44])
        beets.set_item_paths(
            RELEASE_A, [(4401, "/current/library/Moved/01.flac")],
        )

        out = self._capture(db, 1844, beets)

        self.assertIn("Current Library: unique", out)
        self.assertIn("Current Path:    /current/library/Moved", out)
        self.assertNotIn("/stale/request/cache", out)

    def test_show_renders_typed_missing_and_ambiguous_states(self):
        db = _ForensicsDB()
        db.seed_request(self._row(
            id=1845, status="imported", mb_release_id=RELEASE_A,
        ))
        missing = self._capture(db, 1845, FakeBeetsDB())
        self.assertIn("Current Library: missing", missing)

        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(RELEASE_A, [11, 12])
        ambiguous = self._capture(db, 1845, beets)
        self.assertIn("Current Library: ambiguous", ambiguous)
        self.assertIn("Reason:          multiple_matches", ambiguous)
        self.assertIn("Album IDs:       11, 12", ambiguous)

    def test_show_renders_variant_final_state_and_top_3(self):
        db = _ForensicsDB()
        db.seed_request(self._row(id=1843, status="wanted"))
        # JSONB candidates blob — psycopg2 returns parsed Python list.
        candidates_blob = [
            {"username": "alice", "dir": "A\\Album", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26, "avg_ratio": 0.95,
             "missing_titles": [], "file_count": 26},
            {"username": "bob", "dir": "B\\Album", "filetype": "mp3",
             "matched_tracks": 22, "total_tracks": 26, "avg_ratio": 0.80,
             "missing_titles": ["x"], "file_count": 22},
            {"username": "carol", "dir": "C\\Album", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26, "avg_ratio": 0.85,
             "missing_titles": [], "file_count": 26},
            {"username": "dave", "dir": "D\\Album", "filetype": "flac",
             "matched_tracks": 20, "total_tracks": 26, "avg_ratio": 0.99,
             "missing_titles": ["a", "b"], "file_count": 20},
        ]
        db.set_stub_search_history([{
            "id": 99, "request_id": 1843, "query": "*lice Album",
            "result_count": 100, "elapsed_s": 1.2, "outcome": "no_match",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": candidates_blob,
            "variant": "v3_artist_only", "final_state": "Completed",
        }])

        out = self._capture(db, 1843)

        self.assertIn("Search Forensics:", out)
        self.assertIn("variant:        v3_artist_only", out)
        self.assertIn("final_state:    Completed", out)
        # Per-row variant column appears in Search History.
        self.assertIn("v3_artist_only", out)
        # Top-3 ordering: alice (26, 0.95) > carol (26, 0.85) > bob (22, 0.80).
        # dave (20, 0.99) is excluded because matched_tracks dominates avg_ratio.
        alice_idx = out.find("alice")
        carol_idx = out.find("carol")
        bob_idx = out.find("bob")
        dave_idx = out.find("dave")
        self.assertGreater(alice_idx, 0)
        self.assertGreater(carol_idx, alice_idx)
        self.assertGreater(bob_idx, carol_idx)
        self.assertEqual(dave_idx, -1, "4th candidate must be truncated")

    def test_show_handles_null_candidates_gracefully(self):
        """Historical row with NULL candidates → no crash, no top list."""
        db = _ForensicsDB()
        db.seed_request(self._row(id=3))
        db.set_stub_search_history([{
            "id": 1, "request_id": 3, "query": "q",
            "result_count": None, "elapsed_s": None, "outcome": "timeout",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": None, "variant": None, "final_state": None,
        }])

        out = self._capture(db, 3)

        # Pre-U1 / NULL row prints the "no forensic data" sentinel because
        # variant + final_state are both NULL.
        self.assertIn("(no forensic data yet)", out)
        # And the per-row table renders the variant column as a dash.
        self.assertIn("-", out)

    def test_show_handles_empty_candidates_list(self):
        db = _ForensicsDB()
        db.seed_request(self._row(id=4))
        db.set_stub_search_history([{
            "id": 1, "request_id": 4, "query": "q",
            "result_count": 0, "elapsed_s": 0.1, "outcome": "no_results",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": [], "variant": "v2_artist_album_no_year",
            "final_state": "Completed",
        }])

        out = self._capture(db, 4)

        self.assertIn("variant:        v2_artist_album_no_year", out)
        self.assertIn("(empty list)", out)

    def test_show_renders_youtube_history_source_and_metadata(self):
        db = _ForensicsDB()
        db.seed_request(self._row(id=5))
        log_id = db.insert_youtube_running(
            request_id=5,
            browse_id="MPREb_cli_show",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=cli-show",
            expected_track_count=10,
        )
        db.update_youtube_terminal(
            log_id,
            "youtube_failed",
            {
                "reason": "track_count_mismatch",
                "observed_track_count": 9,
                "stderr_excerpt": "line 1\nline 2",
            },
        )

        out = self._capture(db, 5)

        self.assertIn("youtube_failed via youtube", out)
        self.assertIn("browse_id=MPREb_cli_show", out)
        self.assertIn("tracks=9/10", out)
        self.assertIn("reason=track_count_mismatch", out)
        self.assertIn("yt_url:", out)
        self.assertIn("stderr:    line 2", out)
        self.assertNotIn("from None", out)

    def test_show_renders_have_analysis_failure_diagnostics(self):
        db = _ForensicsDB()
        db.seed_request(self._row(id=6))
        db.set_stub_download_history([{
            "id": 711,
            "request_id": 6,
            "created_at": "2026-07-16T00:00:00+00:00",
            "outcome": "have_analysis_error",
            "source": "slskd",
            "soulseek_username": "archive-peer",
            "beets_distance": None,
            "validation_result": {
                "failure_category": "permission_denied",
                "error": "Permission denied while reading installed album",
                "installed_path": "/music/Artist/Album",
                "candidate_reference": "/incoming/candidate",
            },
            "import_result": None,
        }])

        out = self._capture(db, 6)

        self.assertIn("have_analysis_error", out)
        self.assertIn("log_id:            711", out)
        self.assertIn("failure_category:  permission_denied", out)
        self.assertIn("analysis_error:    Permission denied", out)
        self.assertIn("installed_path:     /music/Artist/Album", out)
        self.assertIn("candidate_reference: /incoming/candidate", out)

    def test_show_renders_the_same_failure_copy_as_the_web_ui(self):
        """Issue #868 CLI ⇄ API symmetry: ``pipeline-cli show`` and Recents
        wrap ONE presenter, so they cannot tell two stories about one row."""
        db = _ForensicsDB()
        db.seed_request(self._row(id=7))
        db.set_stub_download_history([{
            "id": 38272,
            "request_id": 7,
            "created_at": "2026-07-25T02:10:00+00:00",
            "outcome": "timeout",
            "source": "slskd",
            "soulseek_username": "Tymemage",
            "beets_distance": None,
            "error_message": "all 29 files errored — 29× 'Verification required'",
            "transfer_detail": [
                {
                    "username": "Tymemage",
                    "filename": f"@@share\\Beefeater\\{index:02d} - Track.flac",
                    "last_state": "Completed, Rejected",
                    "last_exception": "Verification required",
                    "bytes_transferred": 0,
                    "retry_count": 0,
                }
                for index in range(1, 30)
            ],
            "import_result": None,
        }])

        out = self._capture(db, 7)

        self.assertIn(
            'verdict:   Peer Tymemage rejected all 29 files before transfer '
            '— "Verification required"',
            out,
        )
        self.assertIn('Peer message: 29× "Verification required"', out)

    def test_show_does_not_blame_the_peer_for_local_storage(self):
        db = _ForensicsDB()
        db.seed_request(self._row(id=8))
        db.set_stub_download_history([{
            "id": 38273,
            "request_id": 8,
            "created_at": "2026-07-25T02:10:00+00:00",
            "outcome": "timeout",
            "source": "slskd",
            "soulseek_username": "Tymemage",
            "beets_distance": None,
            "error_message": "all 2 files errored",
            "transfer_detail": [
                {
                    "username": "Tymemage",
                    "filename": f"{index:02d} - Track.flac",
                    "last_state": "Completed, Errored",
                    "last_exception": (
                        "Failed to create file: Stale file handle : "
                        "'/mnt/virtio/music/slskd/incomplete/x'"
                    ),
                    "bytes_transferred": 0,
                    "retry_count": 0,
                }
                for index in range(1, 3)
            ],
            "import_result": None,
        }])

        out = self._capture(db, 8)

        self.assertIn("verdict:   Local storage error writing 2 files", out)
        self.assertIn("Storage error: ", out)
        self.assertNotIn("Peer message", out)


class TestCmdShowProcessingOwner(unittest.TestCase):
    """Finding #4 (PR #933 review): ``pipeline-cli show`` must surface the
    processing owner already carried on ``PipelineDB.get_request``'s
    presentation row (``req['processing_owner']``) — the web twin
    (``GET /api/pipeline/<id>``) exposes the same projection, so the CLI
    leaving it out is a real CLI <-> API asymmetry for this PR's own new
    ``processing`` status.
    """

    def _capture(self, db: FakePipelineDB, request_id: int) -> str:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            pipeline_cli.cmd_show(
                db,
                argparse.Namespace(
                    id=request_id, beets_db=None, beets_directory=None,
                ),
                open_beets_fn=lambda **_kwargs: FakeBeetsDB(),
            )
        return stdout.getvalue()

    def test_show_renders_processing_owner_for_processing_request(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=9001,
            status="wanted",
            mb_release_id=RELEASE_A,
        ))
        # Real lifecycle edge (wanted -> downloading -> processing) — never
        # hand-seed the owner pointer directly.
        owner = handoff_automation_owner(db, 9001)

        out = self._capture(db, 9001)

        self.assertIn("Status:       processing", out)
        self.assertIn(
            f"Owner:        job {owner.id} "
            f"({owner.status}/{owner.preview_status})",
            out,
        )
        self.assertIn(
            f"Owner Detail: pipeline-cli import-job-recovery show {owner.id}",
            out,
        )

    def test_show_omits_owner_block_for_non_processing_request(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=9002,
            status="wanted",
            mb_release_id=RELEASE_B,
        ))

        out = self._capture(db, 9002)

        self.assertIn("Status:       wanted", out)
        self.assertNotIn("Owner:", out)
        self.assertNotIn("Owner Detail:", out)
        self.assertNotIn("import-job-recovery", out)


class TestCmdSearchPlanShow(unittest.TestCase):
    """U6 read-only inspection CLI: ``pipeline-cli search-plan show``.

    Uses ``FakePipelineDB`` so the renderer is exercised against the
    same code path the real DB uses.
    """

    def _seed_request(self, *, status: str = "wanted"):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Test Artist", album_title="Test Album",
            source="request", year=2024, status=status,
        )
        return db, rid

    def _create_active_plan(self, db, rid):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        return db.create_successful_search_plan(
            request_id=rid,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="default", query="Test Artist Test Album",
                    canonical_query_key="k0", repeat_group="default-3",
                    provenance={"src": "gen"},
                ),
                SearchPlanItemInput(
                    ordinal=1, strategy="unwild", query="Test Artist - Album",
                    canonical_query_key="k1",
                ),
            ],
            metadata_snapshot={"artist_name": "Test Artist"},
            provenance={"omitted_candidates": []},
            set_active=True,
        )

    def _create_failed_plan(self, db, rid, *, status, failure_class):
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        return db.create_failed_search_plan(
            request_id=rid,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            failure_class=failure_class,
            error_message="boom",
            transient=(status == "failed_transient"),
        )

    def _run(self, db, rid, *, json_out: bool = False):
        args = argparse.Namespace(id=rid, json=json_out)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_search_plan_show(db, args)
        return rc, stdout.getvalue()

    def test_search_plan_show_human_renders_active_plan(self):
        db, rid = self._seed_request()
        self._create_active_plan(db, rid)
        rc, out = self._run(db, rid)
        self.assertEqual(rc, 0)
        self.assertIn("Active successful plan:", out)
        self.assertIn("Currentness:", out)
        self.assertIn("current_generator_searchable: yes", out)
        self.assertIn("strategy=default", out)
        self.assertIn("strategy=unwild", out)
        self.assertIn("Legacy search log", out)

    def test_search_plan_show_json_returns_full_payload(self):
        db, rid = self._seed_request()
        self._create_active_plan(db, rid)
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        for key in ("request_id", "request", "current_generator_id",
                    "currentness", "active_plan",
                    "latest_failed_deterministic",
                    "latest_failed_transient", "superseded_count",
                    "legacy_logs"):
            self.assertIn(key, payload)
        self.assertEqual(payload["request_id"], rid)
        self.assertIsNotNone(payload["active_plan"])
        self.assertEqual(
            len(payload["active_plan"]["items"]), 2,
            "all items emitted in JSON")
        self.assertTrue(
            payload["currentness"]["current_generator_searchable"])

    def test_search_plan_show_human_marks_failures_and_retryable(self):
        db, rid = self._seed_request()
        # No active plan; one deterministic and one transient failure.
        self._create_failed_plan(
            db, rid, status="failed_deterministic",
            failure_class="no_runnable_query")
        self._create_failed_plan(
            db, rid, status="failed_transient",
            failure_class="resolver_unavailable")
        rc, out = self._run(db, rid)
        self.assertEqual(rc, 0)
        self.assertIn("Deterministic (sticky)", out)
        self.assertIn("no_runnable_query", out)
        self.assertIn("Transient (retryable)", out)
        self.assertIn("resolver_unavailable", out)
        self.assertIn("retry_eligible: yes", out)
        self.assertIn("(no active successful plan)", out)

    def test_search_plan_show_missing_request_returns_nonzero(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rc, out = self._run(db, 9999)
        self.assertNotEqual(rc, 0)
        self.assertIn("9999", out)

    def test_search_plan_show_missing_request_json_is_structured(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rc, out = self._run(db, 9999, json_out=True)
        self.assertEqual(rc, 2)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "Not found")
        self.assertEqual(payload["request_id"], 9999)

    def test_search_plan_show_no_plan_at_all_human_output_visible(self):
        db, rid = self._seed_request()
        rc, out = self._run(db, rid)
        self.assertEqual(rc, 0)
        self.assertIn("(no active successful plan)", out)
        self.assertIn("current_generator_searchable: no", out)
        self.assertIn("Deterministic (sticky): (none)", out)
        self.assertIn("Transient (retryable): (none)", out)

    def test_search_plan_show_legacy_logs_visible_when_no_plan_context(self):
        db, rid = self._seed_request()
        # Use log_search to write a row without plan context (legacy).
        db.log_search(
            request_id=rid, query="legacy q", result_count=0,
            elapsed_s=1.0, outcome="no_match", variant="v1",
            final_state="Completed",
        )
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["legacy_logs"]["count"], 1)
        self.assertEqual(len(payload["legacy_logs"]["head"]), 1)
        head = payload["legacy_logs"]["head"][0]
        self.assertEqual(head["outcome"], "no_match")
        self.assertEqual(head["variant"], "v1")

    def test_search_plan_show_flags_generator_id_drift(self):
        from lib.pipeline_db import SearchPlanItemInput
        db, rid = self._seed_request()
        # Seed an active plan on a stale generator id — this can happen
        # if a request was reconciled before the generator id was bumped
        # and U4 hasn't re-reconciled yet.
        db.create_successful_search_plan(
            request_id=rid,
            generator_id="search-plan/2026-01-01-old",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="q",
                canonical_query_key="k0")],
            set_active=True,
        )
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        cu = payload["currentness"]
        self.assertTrue(cu["has_active_plan"])
        self.assertTrue(cu["generator_id_mismatch"])
        self.assertFalse(cu["current_generator_searchable"])

    def test_search_plan_show_integration_covers_ae16_prerequisites(self):
        """One scenario covering AE16 prerequisites: active plan + det-failed
        + transient-failed + superseded + legacy logs all visible in both
        CLI text and CLI --json on the same request."""
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        db, rid = self._seed_request()
        # Active successful plan on the current generator id.
        self._create_active_plan(db, rid)
        # Supersede with a second successful plan to grow superseded_count.
        db.supersede_search_plan_with_replacement(
            request_id=rid,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="new q",
                canonical_query_key="k0")],
        )
        # And a transient failure attempt for the same generator id.
        self._create_failed_plan(
            db, rid, status="failed_transient",
            failure_class="resolver_unavailable")
        # And a deterministic failure attempt for the same generator.
        self._create_failed_plan(
            db, rid, status="failed_deterministic",
            failure_class="no_runnable_query")
        # And a few legacy logs.
        for i in range(5):
            db.log_search(
                request_id=rid, query=f"legacy {i}", result_count=0,
                elapsed_s=0.1, outcome="no_match", variant="v1",
                final_state="Completed")

        # Human output covers every section.
        rc, text = self._run(db, rid)
        self.assertEqual(rc, 0)
        self.assertIn("Active successful plan:", text)
        self.assertIn("Deterministic (sticky)", text)
        self.assertIn("no_runnable_query", text)
        self.assertIn("Transient (retryable)", text)
        self.assertIn("resolver_unavailable", text)
        self.assertIn("Superseded plans:", text)
        self.assertIn("count: 1", text)  # one superseded
        self.assertIn("Legacy search log", text)

        # JSON output mirrors human output bucket-for-bucket.
        rc_json, payload_text = self._run(db, rid, json_out=True)
        self.assertEqual(rc_json, 0)
        payload = json.loads(payload_text)
        self.assertIsNotNone(payload["active_plan"])
        self.assertIsNotNone(payload["latest_failed_deterministic"])
        self.assertIsNotNone(payload["latest_failed_transient"])
        self.assertEqual(payload["superseded_count"], 1)
        self.assertEqual(payload["legacy_logs"]["count"], 5)
        # head is bounded.
        self.assertLessEqual(len(payload["legacy_logs"]["head"]), 5)


class TestCmdSearchPlanShowStats(unittest.TestCase):
    """U8: ``pipeline-cli search-plan show`` includes a Stats section by
    default. ``--no-stats`` suppresses it. JSON output exposes the
    ``stats`` block with cache attribution honesty.
    """

    def _seed_with_plan(self):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B",
            source="request", year=2024, status="wanted",
        )
        db.create_successful_search_plan(
            request_id=rid,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="default", query="A B",
                    canonical_query_key="k0", repeat_group="default-3"),
                SearchPlanItemInput(
                    ordinal=1, strategy="unwild", query="A B unwild",
                    canonical_query_key="k1"),
            ],
            set_active=True,
        )
        return db, rid

    def test_show_emits_stats_section_by_default(self):
        db, rid = self._seed_with_plan()
        args = argparse.Namespace(id=rid, json=False, no_stats=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_search_plan_show(db, args)
        self.assertEqual(rc, 0)
        out = stdout.getvalue()
        self.assertIn("Stats:", out)
        self.assertIn("cache_attribution_level: cycle_only", out)

    def test_show_suppresses_stats_when_no_stats_flag(self):
        db, rid = self._seed_with_plan()
        args = argparse.Namespace(id=rid, json=False, no_stats=True)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_search_plan_show(db, args)
        self.assertEqual(rc, 0)
        out = stdout.getvalue()
        self.assertNotIn("Stats:", out)

    def test_show_json_contains_stats_block(self):
        db, rid = self._seed_with_plan()
        args = argparse.Namespace(id=rid, json=True, no_stats=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_search_plan_show(db, args)
        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        self.assertIn("stats", payload)
        self.assertIn("current", payload["stats"])
        self.assertIn("superseded_and_legacy", payload["stats"])
        self.assertEqual(
            payload["stats"]["current"]["cache_attribution_level"],
            "cycle_only")
        self.assertFalse(
            payload["stats"]["current"]["cache_per_search_available"])

    def test_show_legacy_only_request_still_emits_stats_with_legacy_bucket(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B",
            source="request", year=2024, status="wanted",
        )
        # Pre-plan rows only. Legacy bucket lives in superseded_and_legacy
        # when current_only=False (which the renderer always uses).
        db.log_search(
            request_id=rid, query="legacy 1", outcome="no_match")
        db.log_search(
            request_id=rid, query="legacy 2", outcome="no_results")
        args = argparse.Namespace(id=rid, json=True, no_stats=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_search_plan_show(db, args)
        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        legacy = payload["stats"]["superseded_and_legacy"]["legacy_bucket"]
        self.assertIsNotNone(legacy)
        self.assertEqual(legacy["attempts"], 2)


class TestCmdSearchPlanRegenerate(unittest.TestCase):
    """U8: ``pipeline-cli search-plan regenerate`` wraps
    ``SearchPlanService.generate_for_request(regenerate=True)``.
    """

    def _seed_with_plan(self, *, status: str = "wanted"):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B",
            source="request", year=2024, status=status,
        )
        db.set_tracks(rid, [
            {"track_number": 1, "title": "Track One"},
            {"track_number": 2, "title": "Track Two"},
            {"track_number": 3, "title": "Track Three"},
            {"track_number": 4, "title": "Track Four"},
        ])
        plan_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="A B",
                canonical_query_key="k0")],
            set_active=True,
        )
        return db, rid, plan_id

    def _run(self, db, rid, *, json_out=False, prepend=False):
        args = argparse.Namespace(
            id=rid, json=json_out, prepend_artist=prepend)
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch("lib.config.read_runtime_config") as mock_cfg:
            # Build a minimal real config from defaults so the service
            # can read escalation_threshold etc.
            import configparser

            from lib.config import CratediggerConfig
            cp = configparser.RawConfigParser()
            cp.read_string("[General]\n")
            mock_cfg.return_value = CratediggerConfig.from_ini(cp)
            rc = pipeline_cli.cmd_search_plan_regenerate(db, args)
        return rc, stdout.getvalue()

    def test_regenerate_succeeds_creates_new_active_plan_and_resets_cursor(self):
        db, rid, old_plan_id = self._seed_with_plan()
        # Bump cursor / cycle so we can prove they reset to 0/0.
        db._requests[rid]["next_plan_ordinal"] = 1
        db._requests[rid]["plan_cycle_count"] = 5

        rc, out = self._run(db, rid)
        self.assertEqual(rc, 0)
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertNotEqual(active.plan.id, old_plan_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertIn("Outcome:", out)
        self.assertIn("success", out)

    def test_regenerate_twice_does_not_drift_cursor(self):
        db, rid, _ = self._seed_with_plan()
        rc1, _ = self._run(db, rid)
        self.assertEqual(rc1, 0)
        rc2, _ = self._run(db, rid)
        self.assertEqual(rc2, 0)
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)

    def test_regenerate_returns_2_when_request_not_found(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rc, out = self._run(db, 9999)
        self.assertEqual(rc, 2)
        self.assertIn("request_not_found", out)

    def test_regenerate_imported_request_succeeds_but_not_executable(self):
        db, rid, _ = self._seed_with_plan(status="imported")
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "success")
        self.assertEqual(payload["request_status"], "imported")
        self.assertFalse(payload["executable"])

    def test_regenerate_replaced_request_returns_4_without_mutation(self):
        db, rid, _ = self._seed_with_plan()
        db._requests[rid]["status"] = "replaced"
        before = db.request(rid)
        plans_before = dict(db.search_plans)

        rc, out = self._run(db, rid, json_out=True)

        self.assertEqual(rc, 4)
        self.assertEqual(json.loads(out)["outcome"], "request_replaced")
        self.assertEqual(db.request(rid), before)
        self.assertEqual(db.search_plans, plans_before)

    def test_regenerate_transient_failure_returns_5(self):
        """Adapter pin for this series' deliberate change (#1278): a
        transient regenerate failure exits 5 per the convention table —
        historically 4 — driven through the real command via advisory
        lock contention, a genuine transient producer."""
        from contextlib import contextmanager

        from tests.fakes import FakePipelineDB

        class ContendedDB(FakePipelineDB):
            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                del namespace, key
                yield False

        db = ContendedDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            status="wanted",
        )
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 5)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "failed_transient")

    def test_regenerate_deterministic_failure_returns_3_preserves_old_plan(self):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        # Empty artist/title would normally fail generation; seed a request
        # with no usable identity and an existing successful plan to prove
        # preservation.
        rid = db.add_request(
            artist_name="", album_title="", source="request", status="wanted",
        )
        old_plan_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="placeholder",
                canonical_query_key="k0")],
            set_active=True,
        )
        # Bump cursor; failed regen must not reset it.
        db._requests[rid]["next_plan_ordinal"] = 0
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 3)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "failed_deterministic")
        # Old active plan still present.
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.plan.id, old_plan_id)


class TestCmdSearchPlanDryRun(unittest.TestCase):
    """U6: ``pipeline-cli search-plan dry-run`` wraps
    ``SearchPlanService.dry_run_for_request`` — read-only generator
    simulator. Mirrors the same exit-code convention as ``search-plan
    show``: success = 0, request_not_found = 2.
    """

    def _seed_request(
        self, *, status: str = "wanted",
        artist: str = "Radiohead", title: str = "Kid A",
        year: int = 2008, release_group_year: int | None = 2000,
        tracks: list[dict] | None = None,
    ):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name=artist, album_title=title,
            source="request", year=year, status=status,
        )
        if release_group_year is not None:
            db._requests[rid]["release_group_year"] = release_group_year
        if tracks is None:
            tracks = [
                {"track_number": 1, "title": "Everything In Its Right Place"},
                {"track_number": 2, "title": "Kid A"},
                {"track_number": 3, "title": "The National Anthem"},
            ]
        if tracks:
            db.set_tracks(rid, tracks)
        return db, rid

    def _run(self, db, rid, *, json_out: bool = False, prepend: bool = False):
        args = argparse.Namespace(
            id=rid, json=json_out, prepend_artist=prepend)
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch("lib.config.read_runtime_config") as mock_cfg:
            import configparser

            from lib.config import CratediggerConfig
            cp = configparser.RawConfigParser()
            cp.read_string("[General]\n")
            mock_cfg.return_value = CratediggerConfig.from_ini(cp)
            rc = pipeline_cli.cmd_search_plan_dry_run(db, args)
        return rc, stdout.getvalue()

    def test_dry_run_generation_failure_is_informational_exit_0(self):
        """`generation_failed` is a success exit through the real command
        (PR3 mutant-runner M14: the dry-run table's one distinguishing
        key had no adapter coverage on either surface)."""
        db, rid = self._seed_request(artist="", title="", tracks=[])
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "generation_failed")

    def test_dry_run_happy_path_prints_plan_items_without_persisting(self):
        db, rid = self._seed_request()
        plans_before = len(db.search_plans)
        items_before = len(db.search_plan_items)
        rc, out = self._run(db, rid)
        self.assertEqual(rc, 0)
        self.assertIn("Outcome:", out)
        self.assertIn("success", out)
        self.assertIn("Plan items", out)
        # Persistence invariant: dry-run never writes plan rows.
        self.assertEqual(len(db.search_plans), plans_before)
        self.assertEqual(len(db.search_plan_items), items_before)
        # Request row's active_plan_id is untouched.
        self.assertIsNone(db._requests[rid]["active_plan_id"])

    def test_dry_run_json_returns_full_payload(self):
        db, rid = self._seed_request()
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        for key in ("request_id", "outcome", "current_generator_id",
                    "request", "plan", "would_supersede_active",
                    "error_message"):
            self.assertIn(key, payload)
        self.assertEqual(payload["request_id"], rid)
        self.assertEqual(payload["outcome"], "success")
        self.assertIsNotNone(payload["plan"])
        # release_group_year reflected in request payload (U5 input).
        self.assertEqual(
            payload["request"]["release_group_year"], 2000)
        # Plan items shape is the documented contract.
        self.assertGreater(len(payload["plan"]["items"]), 0)
        item = payload["plan"]["items"][0]
        for key in ("ordinal", "strategy", "query",
                    "canonical_query_key", "repeat_group", "provenance"):
            self.assertIn(key, item)

    def test_dry_run_missing_request_returns_2(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rc, out = self._run(db, 9999)
        self.assertEqual(rc, 2)
        self.assertIn("request_not_found", out)

    def test_dry_run_missing_request_json_is_structured(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rc, out = self._run(db, 9999, json_out=True)
        self.assertEqual(rc, 2)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "request_not_found")
        self.assertEqual(payload["request_id"], 9999)
        self.assertIsNone(payload["plan"])
        self.assertIsNone(payload["request"])

    def test_dry_run_request_with_no_tracks_succeeds_no_track_slots(self):
        db, rid = self._seed_request(tracks=[])
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "success")
        # No track-fallback slots when the request has zero tracks.
        strategies = [
            it["strategy"] for it in payload["plan"]["items"]
        ]
        self.assertFalse(
            any(s.startswith("track_") for s in strategies),
            f"unexpected track slots: {strategies}")

    def test_dry_run_flags_active_plan_would_be_superseded(self):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        db, rid = self._seed_request()
        db.create_successful_search_plan(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="prior",
                canonical_query_key="k0")],
            set_active=True,
        )
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertTrue(payload["would_supersede_active"])

    def test_dry_run_uses_current_generator_id(self):
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        db, rid = self._seed_request()
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(
            payload["current_generator_id"], SEARCH_PLAN_GENERATOR_ID)
        self.assertEqual(
            payload["plan"]["generator_id"], SEARCH_PLAN_GENERATOR_ID)


class TestCmdSearchPlanSaturation(unittest.TestCase):
    """U7: ``pipeline-cli search-plan saturation`` wraps
    ``SearchPlanService.saturation_for_request``. Exit-code convention:
    success = 0 (even when window is empty — found-but-quiet is still
    success), request_not_found = 2, input_invalid = 3.
    """

    def _seed(self, *, rid: int = 1):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=rid, artist_name="Radiohead", album_title="Kid A",
            source="request",
        ))
        return db, rid

    def _run(self, db, rid, *, json_out: bool = False,
             window_days: int | None = None):
        args = argparse.Namespace(
            id=rid, json=json_out, window_days=window_days)
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch("lib.config.read_runtime_config") as mock_cfg:
            import configparser

            from lib.config import CratediggerConfig
            cp = configparser.RawConfigParser()
            cp.read_string("[General]\n")
            mock_cfg.return_value = CratediggerConfig.from_ini(cp)
            rc = pipeline_cli.cmd_search_plan_saturation(db, args)
        return rc, stdout.getvalue()

    def test_happy_path_prints_human_summary(self):
        db, rid = self._seed()
        for i in range(10):
            final_state = (
                "Completed, ResponseLimitReached" if i < 3
                else "Completed")
            db.log_search(request_id=rid, query=f"q{i}",
                          outcome="found", final_state=final_state,
                          pre_filter_skip_count=4)
        rc, out = self._run(db, rid)
        self.assertEqual(rc, 0)
        self.assertIn("Outcome:", out)
        self.assertIn("success", out)
        self.assertIn("Total searches:", out)
        self.assertIn("10", out)
        self.assertIn("Saturated searches:", out)
        self.assertIn("Saturation rate:", out)
        # Pre-filter skip total surfaces in human view.
        self.assertIn("Pre-filter skips total:", out)
        self.assertIn("40", out)

    def test_json_returns_full_payload(self):
        db, rid = self._seed()
        db.log_search(request_id=rid, query="q",
                      outcome="found",
                      final_state="Completed, FileLimitReached",
                      pre_filter_skip_count=3)
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        for key in ("request_id", "outcome", "total_searches",
                    "saturated_searches", "saturation_rate",
                    "total_pre_filter_skips", "window_days",
                    "error_message"):
            self.assertIn(key, payload)
        self.assertEqual(payload["request_id"], rid)
        self.assertEqual(payload["outcome"], "success")
        self.assertEqual(payload["total_searches"], 1)
        self.assertEqual(payload["saturated_searches"], 1)
        self.assertEqual(payload["saturation_rate"], 1.0)
        self.assertEqual(payload["total_pre_filter_skips"], 3)
        self.assertEqual(payload["window_days"], 14)

    def test_empty_window_exits_0_with_zeros(self):
        # Found-but-quiet — exit 0, all zeros, NOT 404.
        db, rid = self._seed()
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "success")
        self.assertEqual(payload["total_searches"], 0)
        self.assertEqual(payload["saturation_rate"], 0.0)

    def test_missing_request_returns_2(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rc, out = self._run(db, 9999)
        self.assertEqual(rc, 2)
        self.assertIn("request_not_found", out)

    def test_missing_request_json_is_structured(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rc, out = self._run(db, 9999, json_out=True)
        self.assertEqual(rc, 2)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "request_not_found")
        # All summary fields zero-filled so clients can read without
        # branching on outcome.
        self.assertEqual(payload["total_searches"], 0)
        self.assertEqual(payload["saturation_rate"], 0.0)

    def test_invalid_window_days_returns_3(self):
        db, rid = self._seed()
        rc, out = self._run(db, rid, window_days=0)
        self.assertEqual(rc, 3)
        self.assertIn("input_invalid", out)

    def test_window_days_default_is_14(self):
        db, rid = self._seed()
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["window_days"], 14)


class TestStaleCompletionRacingRegeneration(unittest.TestCase):
    """U8: a stale plan-A completion arriving after regeneration must
    log against plan A but never advance plan B's cursor. Integration
    style — uses real SearchPlanService over FakePipelineDB.
    """

    def test_stale_completion_logs_does_not_advance_new_cursor(self):
        import configparser

        from lib.config import CratediggerConfig
        from lib.pipeline_db import (
            ConsumedAttemptInput,
            SearchPlanItemInput,
        )
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from lib.search_plan_service import SearchPlanService
        from tests.fakes import FakePipelineDB
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        cfg = CratediggerConfig.from_ini(cp)

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Stale", album_title="Race",
            source="request", year=2024, status="wanted",
        )
        db.set_tracks(rid, [
            {"track_number": 1, "title": "T1"},
            {"track_number": 2, "title": "T2"},
            {"track_number": 3, "title": "T3"},
            {"track_number": 4, "title": "T4"},
        ])
        # Plan A active.
        plan_a_id = db.create_successful_search_plan(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="Stale Race",
                canonical_query_key="k0")],
            set_active=True,
        )
        # Snapshot the plan A item id the executor would have read.
        item_a = next(
            it for it in db.search_plan_items.values()
            if it.plan_id == plan_a_id and it.ordinal == 0
        )

        # Regenerate -> plan B is now active; cursor reset to 0/0.
        svc = SearchPlanService(db, cfg)
        result = svc.generate_for_request(rid, regenerate=True)
        self.assertEqual(result.outcome, "success")
        active_after = db.get_active_search_plan(rid)
        assert active_after is not None
        self.assertNotEqual(active_after.plan.id, plan_a_id)
        self.assertEqual(active_after.next_ordinal, 0)
        self.assertEqual(active_after.cycle_count, 0)

        # An in-flight plan-A completion lands now.
        attempt = ConsumedAttemptInput(
            request_id=rid, plan_id=plan_a_id, plan_item_id=item_a.id,
            plan_ordinal=0, plan_strategy="default",
            plan_canonical_query_key="k0", plan_repeat_group=None,
            plan_generator_id=SEARCH_PLAN_GENERATOR_ID,
            query="Stale Race", outcome="found",
            plan_item_count=1,
        )
        consumed_result = db.record_consumed_search_attempt(attempt)
        self.assertEqual(consumed_result.cursor_update_status, "stale")
        self.assertTrue(consumed_result.is_stale)

        # Plan B's cursor untouched.
        active_after_stale = db.get_active_search_plan(rid)
        assert active_after_stale is not None
        self.assertEqual(active_after_stale.next_ordinal, 0)
        self.assertEqual(active_after_stale.cycle_count, 0)
        # The log row exists with stale flag.
        history = db.get_search_history(rid)
        stale_rows = [r for r in history
                      if r.get("cursor_update_status") == "stale"]
        self.assertEqual(len(stale_rows), 1)
        self.assertEqual(stale_rows[0]["plan_id"], plan_a_id)


class TestCmdSearchPlanAdvance(unittest.TestCase):
    """``pipeline-cli search-plan advance`` wraps
    ``SearchPlanService.advance_for_request``. Counterpart of the API
    endpoint ``POST /api/pipeline/<id>/search-plan/advance`` — both must
    stay in sync; see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"."""

    def _seed_plan(self):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="David Bowie", album_title="David Bowie",
            source="request", year=1967, status="wanted",
        )
        items = [
            SearchPlanItemInput(
                ordinal=i, strategy="default",
                query="*avid *owie", canonical_query_key="*avid *owie")
            for i in range(5)
        ]
        items.append(SearchPlanItemInput(
            ordinal=5, strategy="track_0", query="Love Till Tuesday",
            canonical_query_key="love till tuesday"))
        db.create_successful_search_plan(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=items, set_active=True,
        )
        return db, rid

    def _run(self, db, rid, *, to_ordinal=None, to_strategy=None,
             json_out=False):
        args = argparse.Namespace(
            id=rid, to_ordinal=to_ordinal, to_strategy=to_strategy,
            json=json_out,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch("lib.config.read_runtime_config") as mock_cfg:
            import configparser

            from lib.config import CratediggerConfig
            cp = configparser.RawConfigParser()
            cp.read_string("[General]\n")
            mock_cfg.return_value = CratediggerConfig.from_ini(cp)
            rc = pipeline_cli.cmd_search_plan_advance(db, args)
        return rc, stdout.getvalue()

    def test_advance_to_ordinal_succeeds_and_moves_cursor(self):
        db, rid = self._seed_plan()
        rc, out = self._run(db, rid, to_ordinal=5)
        self.assertEqual(rc, 0)
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.next_ordinal, 5)
        self.assertIn("track_0", out)

    def test_advance_to_strategy_jumps_past_collapsed_default_slots(self):
        """The motivating use case: self-titled releases collapse into 5
        identical default-strategy slots; --to-strategy track skips them."""
        db, rid = self._seed_plan()
        rc, out = self._run(db, rid, to_strategy="track")
        self.assertEqual(rc, 0)
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.next_ordinal, 5)
        self.assertIn("Cursor:", out)
        self.assertIn("0 → 5", out)

    def test_advance_returns_2_when_request_not_found(self):
        from tests.fakes import FakePipelineDB
        rc, out = self._run(FakePipelineDB(), 9999, to_ordinal=1)
        self.assertEqual(rc, 2)
        self.assertIn("request_not_found", out)

    def test_advance_returns_3_on_invalid_target(self):
        db, rid = self._seed_plan()
        rc, out = self._run(db, rid, to_ordinal=99)
        self.assertEqual(rc, 3)
        self.assertIn("invalid_target", out)

    def test_advance_returns_4_when_no_active_plan(self):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="Y",
            source="request", year=2020, status="wanted",
        )
        rc, out = self._run(db, rid, to_ordinal=1)
        self.assertEqual(rc, 4)
        self.assertIn("no_active_plan", out)

    def test_advance_returns_4_for_replaced_request_without_mutation(self):
        db, rid = self._seed_plan()
        db._requests[rid]["status"] = "replaced"
        before = db.request(rid)

        rc, out = self._run(db, rid, to_ordinal=5, json_out=True)

        self.assertEqual(rc, 4)
        self.assertEqual(json.loads(out)["outcome"], "request_replaced")
        self.assertEqual(db.request(rid), before)

    def test_advance_json_output_carries_full_payload(self):
        db, rid = self._seed_plan()
        rc, out = self._run(db, rid, to_ordinal=5, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "advanced")
        self.assertEqual(payload["new_ordinal"], 5)
        self.assertEqual(payload["new_strategy"], "track_0")
        self.assertEqual(payload["new_query"], "Love Till Tuesday")


class TestCmdReplace(_FakeDbWebServerCase):
    """``pipeline-cli replace`` is a thin adapter over ``POST
    /api/pipeline/<id>/replace``, which owns the one execution path
    (issue #1063 — Replace's cleanup deletes protected Wrong Matches
    folders). These pins drive the real route and assert the CLI's
    historical exit codes and text/JSON output survive the move."""

    def _run(self, *, mock_outcome, mock_kwargs=None, json_out=False,
             req_id=42, target_mbid="new-mbid"):
        from lib.mbid_replace_service import ReplaceResult

        result = ReplaceResult(
            outcome=mock_outcome,
            request_id=req_id,
            **(mock_kwargs or {}),
        )
        args = argparse.Namespace(
            id=req_id, target_mb_release_id=target_mbid, json=json_out,
            api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch("lib.config.read_runtime_config") as mock_cfg, \
                 patch("lib.mbid_replace_service.MbidReplaceService") as MS:
            import configparser

            from lib.config import CratediggerConfig
            cp = configparser.RawConfigParser()
            cp.read_string("[General]\n")
            mock_cfg.return_value = CratediggerConfig.from_ini(cp)
            MS.return_value.replace_request_mbid.return_value = result
            rc = pipeline_cli.cmd_replace(None, args)
        return rc, stdout.getvalue()

    def test_exit_0_on_replaced(self):
        rc, out = self._run(
            mock_outcome="replaced",
            mock_kwargs={"new_request_id": 99},
        )
        self.assertEqual(rc, 0)
        self.assertIn("replaced", out)
        self.assertIn("99", out)

    def test_exit_2_on_not_found(self):
        rc, _ = self._run(mock_outcome="not_found")
        self.assertEqual(rc, 2)

    def test_exit_3_on_semantic_violations(self):
        for outcome in (
            "target_invalid",
            "target_release_group_mismatch",
            "target_same_as_current",
        ):
            with self.subTest(outcome=outcome):
                rc, _ = self._run(mock_outcome=outcome)
                self.assertEqual(rc, 3)

    def test_exit_4_on_wrong_state_and_collision(self):
        for outcome in (
            "wrong_state",
            "target_collision_request",
        ):
            with self.subTest(outcome=outcome):
                rc, _ = self._run(mock_outcome=outcome)
                self.assertEqual(rc, 4)

    def test_processing_locked_json_carries_exact_owner(self):
        from lib.pipeline_db._shared import ProcessingOwnerProjection

        rc, out = self._run(
            mock_outcome="wrong_state",
            mock_kwargs={
                "reason": "processing_locked",
                "processing_owner": ProcessingOwnerProjection(
                    job_id=73,
                    status="running",
                    preview_status="evidence_ready",
                ),
            },
            json_out=True,
        )

        self.assertEqual(rc, 4)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "processing_locked")
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["processing_owner"], {
            "job_id": 73,
            "status": "running",
            "preview_status": "evidence_ready",
        })

    def test_exit_5_on_transient_and_mirror_unconfigured(self):
        # mirror_unconfigured (Discogs mirror not set up) shares exit 5
        # with transient — both are service-unavailable/retryable.
        for outcome in ("transient", "mirror_unconfigured"):
            with self.subTest(outcome=outcome):
                rc, _ = self._run(mock_outcome=outcome)
                self.assertEqual(rc, 5)

    def test_json_output_includes_reason(self):
        """#501 item 2: the CLI's --json payload surfaces the new typed
        ``reason`` field (a REPLACE_REASON_* code) so operators/tooling
        can assert on the stable code instead of parsing error_message."""
        _rc, out = self._run(
            mock_outcome="target_invalid",
            mock_kwargs={
                "reason": "cross_pathway_target",
                "error_message": "target ... is not a valid same-pathway target",
            },
            json_out=True,
        )
        payload = json.loads(out)
        self.assertEqual(payload["reason"], "cross_pathway_target")

    def test_text_output_includes_reason_line(self):
        """#501 item 2: the human-readable output also surfaces the
        reason code when set (target_invalid outcomes only)."""
        _rc, out = self._run(
            mock_outcome="target_invalid",
            mock_kwargs={"reason": "cross_pathway_target"},
        )
        self.assertIn("cross_pathway_target", out)

    def test_numeric_discogs_target_accepted(self):
        """A numeric Discogs id is a valid target on the CLI surface —
        argparse takes ``--to`` as an opaque string; the service
        dispatches on id shape, not the wire param name."""
        rc, out = self._run(
            mock_outcome="replaced",
            mock_kwargs={"new_request_id": 99},
            target_mbid="1002",
        )
        self.assertEqual(rc, 0)
        self.assertIn("99", out)

    def test_json_output_carries_full_payload(self):
        rc, out = self._run(
            mock_outcome="replaced",
            mock_kwargs={"new_request_id": 99, "warnings": ("w1",)},
            json_out=True,
        )
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "replaced")
        self.assertEqual(payload["new_request_id"], 99)
        self.assertEqual(payload["warnings"], ["w1"])

    def test_argparse_rejects_missing_to(self):
        parser_test_argv = ["replace", "42"]
        with patch.object(sys, "argv", ["pipeline-cli"] + parser_test_argv), \
             redirect_stderr(io.StringIO()), \
             self.assertRaises(SystemExit) as cm:
            pipeline_cli.main()
        # argparse exits with code 2 for missing required args.
        self.assertEqual(cm.exception.code, 2)


class TestCmdBeetsDistance(_FakeDbWebServerCase):
    """``pipeline-cli beets-distance`` is a thin adapter over ``GET
    /api/beets-distance/<download_log_id>/<mbid>`` (issue #1063 — the
    folder it reads lives under the private processing tree).
    Service-layer correctness lives in ``tests.test_beets_distance``;
    here we pin the status-code ⇄ exit-code mapping."""

    UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def _run(self, *, outcome, json_out=False, **result_kwargs):
        from lib.beets_distance import BeetsDistanceResult
        result = BeetsDistanceResult(outcome=outcome, **result_kwargs)
        args = argparse.Namespace(
            download_log_id=100, mbid=self.UUID, json=json_out,
            api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch(
            "lib.beets_distance.compute_beets_distance",
            return_value=result,
        ):
            rc = pipeline_cli.cmd_beets_distance(None, args)
        return rc, stdout.getvalue()

    def test_exit_0_on_ok(self):
        rc, out = self._run(
            outcome="ok",
            distance=0.07,
            matched_tracks=12,
            total_local_tracks=12,
            total_mb_tracks=12,
            duration_ms=8,
        )
        self.assertEqual(rc, 0)
        self.assertIn("0.0700", out)
        self.assertIn("12 / 12", out)

    def test_exit_2_on_not_found_branches(self):
        for outcome in ("download_log_not_found", "request_not_found"):
            with self.subTest(outcome=outcome):
                rc, _ = self._run(outcome=outcome,
                                  error_message="not found")
                self.assertEqual(rc, 2)

    def test_exit_3_on_semantic_violations(self):
        """Cross-RG guardrail + missing-RG both surface as exit 3."""
        for outcome in ("wrong_release_group", "mb_no_release_group"):
            with self.subTest(outcome=outcome):
                rc, _ = self._run(outcome=outcome,
                                  error_message="bad MBID")
                self.assertEqual(rc, 3)

    def test_exit_4_on_missing_artifacts(self):
        for outcome in ("folder_missing", "no_audio"):
            with self.subTest(outcome=outcome):
                rc, _ = self._run(outcome=outcome,
                                  error_message="gone")
                self.assertEqual(rc, 4)

    def test_exit_5_on_transient(self):
        rc, _ = self._run(outcome="mb_lookup_failed",
                          error_message="MB mirror down")
        self.assertEqual(rc, 5)

    def test_exit_1_on_distance_failed(self):
        rc, _ = self._run(outcome="distance_failed",
                          error_message="beets blew up")
        self.assertEqual(rc, 1)

    def test_json_output_carries_full_payload(self):
        rc, out = self._run(
            outcome="ok",
            distance=0.07,
            matched_tracks=12,
            total_local_tracks=12,
            total_mb_tracks=12,
            components={"album": 0.0, "artist": 0.05},
            json_out=True,
        )
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "ok")
        self.assertAlmostEqual(payload["distance"], 0.07, places=4)
        self.assertEqual(payload["components"]["album"], 0.0)

    def test_discogs_numeric_id_routes_through_discogs_lookup(self):
        """A numeric mbid (Discogs sibling) must route through
        ``discogs_api.get_release``, not ``web.mb.get_release`` — CLI
        counterpart of the same dispatch fixed in the API route (#530).
        No new MB<->Discogs adapter: ``compute_beets_distance`` already
        treats ``release_group_id`` as optional and ``discogs_api.get_release``
        mirrors ``mb_api.get_release``'s dict shape exactly.
        """
        from lib.beets_distance import BeetsDistanceResult

        captured = {}

        def _fake_compute(download_log_id, mbid, *, pdb, mb_get_release,
                           cache=None, **_kw):
            captured["mb_get_release"] = mb_get_release
            return BeetsDistanceResult(
                outcome="ok", distance=0.05,
                download_log_id=download_log_id, candidate_mbid=mbid,
            )

        discogs_release = {
            "id": "2048516",
            "title": "Fake Album",
            "artist_name": "Fake Artist",
            "artist_id": "999",
            "release_group_id": None,
            "tracks": [],
        }
        args = argparse.Namespace(
            download_log_id=100, mbid="2048516", json=False,
            api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch(
            "lib.beets_distance.compute_beets_distance",
            side_effect=_fake_compute,
        ), patch(
            "web.discogs.get_release",
            return_value=discogs_release,
        ) as discogs_get:
            rc = pipeline_cli.cmd_beets_distance(None, args)
            self.assertIn("mb_get_release", captured)
            resolved = captured["mb_get_release"]("2048516")
            discogs_get.assert_called_once_with(2048516, fresh=False)
        self.assertEqual(rc, 0)
        self.assertEqual(resolved, discogs_release)


class TestCmdTriageQuarantine(_FakeDbWebServerCase):
    """``pipeline-cli triage quarantine`` is a thin adapter over ``GET
    /api/triage/quarantine`` (issue #1122 F1). The processing tree it
    scans is a private ``0700 cratedigger:users`` directory readable only
    by the web service identity — run directly in the invoking operator's
    own CLI process, the scan raised ``EACCES`` and killed the WHOLE view
    (proved empirically against the real root during review), taking down
    the download-dir-rooted roots the operator could otherwise have read
    too. Routing through the canonical web route, exactly like
    ``force-import``/``replace``/``beets-distance``, makes ONE identity own
    those filesystem facts. Service-layer correctness (which folders are
    orphans, special-bucket exclusion, config resolution) lives in
    ``tests.test_quarantine_triage_service`` /
    ``tests.test_quarantine_triage_generated`` and the route's own contract
    in ``tests.web.test_routes_triage``; here we pin the CLI ⇄ route wiring
    and the status-code ⇄ exit-code mapping.
    """

    def _run_quarantine(self, root, *, json_out=False):
        config_path = os.path.join(root, "config.ini")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(f"[Slskd]\ndownload_dir = {root}\n")
        args = argparse.Namespace(
            json=json_out, api_endpoint=TcpApiEndpoint(self.base),
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
            clear=False,
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_triage_quarantine(None, args)
        return rc, stdout.getvalue(), stderr.getvalue()

    def test_quarantine_json_matches_typed_service_shape(self):
        from lib.quarantine_triage_service import QuarantineTriageResult

        with tempfile.TemporaryDirectory() as root:
            quarantine = os.path.join(root, "failed_imports")
            referenced = os.path.join(quarantine, "Referenced")
            orphan = os.path.join(quarantine, "Orphan")
            os.makedirs(referenced)
            os.makedirs(orphan)
            _seed_id = self.db.add_request("Artist", "Album", "request")
            self.db.log_download(
                _seed_id,
                outcome="rejected",
                validation_result={
                    "failed_path": "failed_imports/Referenced",
                    "scenario": "high_distance",
                },
            )

            rc, out, err = self._run_quarantine(root, json_out=True)

        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        result = msgspec.convert(json.loads(out), type=QuarantineTriageResult)
        self.assertEqual([folder.name for folder in result.folders], ["Orphan"])

    def test_quarantine_human_output_names_every_root(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "failed_imports", "Visible Orphan"))
            os.makedirs(os.path.join(root, "wrong_matches", "Wrong Orphan"))
            # The default processing_dir (no [Paths] override in
            # _run_quarantine's config) resolves to ``<root>/processing``.
            os.makedirs(os.path.join(
                root, "processing", "albums", "failed_imports",
                "Processing Failed Orphan",
            ))
            os.makedirs(os.path.join(
                root, "processing", "albums", "wrong_matches",
                "Processing Orphan",
            ))
            rc, out, err = self._run_quarantine(root)
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        self.assertIn("Visible Orphan", out)
        self.assertIn("Wrong Orphan", out)
        self.assertIn("Processing Failed Orphan", out)
        self.assertIn("Processing Orphan", out)
        self.assertIn("Processing wrong-matches root:", out)
        self.assertIn("Processing failed-import root:", out)
        self.assertIn(
            os.path.join(root, "processing", "albums", "wrong_matches"), out,
        )
        self.assertIn(
            os.path.join(root, "processing", "albums", "failed_imports"), out,
        )

    def test_quarantine_scan_error_returns_5_with_json_error(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "failed_imports"), "w", encoding="utf-8") as f:
                f.write("not a directory")
            rc, out, err = self._run_quarantine(root, json_out=True)
        self.assertEqual(rc, 5)
        self.assertEqual(err, "")
        self.assertIn("error", json.loads(out))

    def test_quarantine_scan_error_human_output_reports_the_api_refusal(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "failed_imports"), "w", encoding="utf-8") as f:
                f.write("not a directory")
            rc, out, err = self._run_quarantine(root)
        self.assertEqual(rc, 5)
        self.assertEqual(err, "")
        self.assertIn("API refused", out)

    def test_quarantine_transport_failure_returns_exit_5_with_no_direct_scan(self):
        """#1122 F1: no direct-filesystem fallback. When the API/socket is
        unreachable the command exits 5 and reads nothing — it must NEVER
        fall back to scanning the private processing tree directly as the
        invoking operator, which is exactly the bug this fix closes. That
        guarantee is structural, not just behavioral: ``scripts.pipeline_cli
        .triage`` no longer imports ``lib.quarantine_triage_service`` at
        all (grep confirms it), so there is no owned function left to mock
        here — mocking one just to assert non-invocation would be exactly
        the kind of owned-function patch code-quality.md's leaf-seam-only
        mock rule forbids, for a guarantee the missing import already
        proves.
        """
        args = argparse.Namespace(
            json=False,
            # Nothing listens on this port.
            api_endpoint=TcpApiEndpoint("http://127.0.0.1:1"),
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_triage_quarantine(None, args)
        self.assertEqual(rc, 5)


class TestCmdYoutubeAlbum(unittest.TestCase):
    """``pipeline-cli youtube-album`` wraps
    ``lib.youtube_album_service.resolve_youtube_album``. Counterpart of
    ``POST /api/youtube-album`` (U8). Service-layer correctness lives in
    ``tests.test_youtube_album_service``; here we pin the exit-code
    mapping and the matrix-text output shape per the CLI ⇄ API
    symmetry rule.

    Outcome → exit code MUST come from
    ``lib.youtube_album_service.OUTCOME_EXIT_CODE`` (single source of
    truth shared with the U8 route)."""

    IDENT = "44438bf9-26d9-4460-9b4f-1a1b015e37a1"

    def _make_result(
        self, *,
        outcome: str,
        youtube_releases: Any = None,
        error_message: Any = None,
        from_cache: bool = False,
        release_group_identifier: Any = "rg-uuid",
        source: Any = "mb",
        duration_ms: Any = 42,
    ):
        from lib.youtube_album_service import YoutubeAlbumResolverResult
        return YoutubeAlbumResolverResult(
            outcome=outcome,
            release_group_identifier=release_group_identifier,
            source=source,
            from_cache=from_cache,
            youtube_releases=youtube_releases or [],
            error_message=error_message,
            duration_ms=duration_ms,
        )

    def _make_ok_matrix(self):
        from lib.beets_distance import SyntheticItem
        from lib.youtube_album_service import (
            ResolvedDistance,
            ResolvedYoutubeRelease,
        )
        synth = [
            SyntheticItem(
                title="Track A", artist="Artist", album="Album",
                albumartist="Artist", track=1, tracktotal=1, disc=1,
                disctotal=1, length=180.0,
            ),
        ]
        return [
            ResolvedYoutubeRelease(
                yt_browse_id="MPREb_xxx",
                yt_audio_playlist_id="OLAK5uy_yyy",
                yt_url="https://music.youtube.com/playlist?list=OLAK5uy_yyy",
                year=2014, track_count=1, tracks=synth,
                distances=[
                    ResolvedDistance(
                        mbid=self.IDENT, outcome="ok", distance=0.05,
                        components={"album": 0.0, "artist": 0.05},
                        matched_tracks=1, total_local_tracks=1,
                        total_mb_tracks=1, extra_local_tracks=0,
                        extra_mb_tracks=0,
                    ),
                ],
            ),
        ]

    def _run(self, *, outcome: Any = None, result: Any = None,
             refresh: bool = False, watch_url: str | None = None, json_out: bool = False,
             resolver_side_effect: Exception | None = None):
        if result is None:
            assert outcome is not None, "must pass outcome= or result="
            result = self._make_result(outcome=outcome)
        args = argparse.Namespace(
            identifier=self.IDENT, refresh=refresh, watch_url=watch_url, json=json_out,
        )
        stdout = io.StringIO()
        # cmd_youtube_album's first arg is the PipelineDB instance; the
        # resolve_youtube_album call is mocked out so the DB is never
        # touched, but per the project mock-audit rule (CLAUDE.md §
        # "MOCKS: leaf-seam only") we still use FakePipelineDB instead
        # of MagicMock so the wrapper test stays consistent with how
        # production passes ``db`` through (finding #28).
        #
        # ``_build_youtube_client`` is patched too so the test never
        # constructs a real ``YTMusic`` (which would try to hit the
        # network). The patch returns a (yt, session) tuple where the
        # session is a class with a counting ``close()`` — round 2
        # P2-2 asserts the CLI closes the session in its ``finally``.
        from tests.fakes import FakePipelineDB

        class _FakeSession:
            close_calls = 0

            def close(self) -> None:
                type(self).close_calls += 1

        _FakeSession.close_calls = 0
        self._last_session_cls = _FakeSession

        with redirect_stdout(stdout), patch(
            "scripts.pipeline_cli.youtube._build_youtube_client",
            return_value=(object(), _FakeSession()),
        ), patch(
            "scripts.pipeline_cli.youtube._RedisYoutubeCache",
            return_value=object(),
        ), patch(
            "scripts.pipeline_cli.youtube.resolve_youtube_album",
            return_value=result,
            side_effect=resolver_side_effect,
        ) as mock_resolve:
            rc = pipeline_cli.cmd_youtube_album(
                FakePipelineDB(), args)
        return rc, stdout.getvalue(), mock_resolve

    def test_exit_code_mapping_uses_service_module_dict(self):
        """The CLI must import ``OUTCOME_EXIT_CODE`` from the service
        module — not redefine its own copy. PR #381 lesson: outcome
        vocabulary from one source. We assert the mapping is sourced
        from the service module by checking the attribute lookup."""
        from lib import youtube_album_service as svc
        # The CLI module must reference the service's exit-code dict.
        # Verifying the import alias keeps the contract.
        self.assertIs(
            pipeline_cli.OUTCOME_EXIT_CODE,
            svc.OUTCOME_EXIT_CODE,
        )

    def test_watch_url_is_forwarded_to_shared_resolver(self):
        url = "https://www.youtube.com/watch?v=video&list=playlist"
        _rc, _out, resolve = self._run(outcome="ok", watch_url=url)
        self.assertEqual(resolve.call_args.kwargs["watch_url"], url)

    def test_exit_0_on_ok_text_mode_shows_matrix(self):
        result = self._make_result(
            outcome="ok", youtube_releases=self._make_ok_matrix())
        rc, out, _ = self._run(result=result)
        self.assertEqual(rc, 0)
        # Matrix view: one line per YT release, indented sub-lines per
        # MBID with the distance.
        self.assertIn("MPREb_xxx", out)
        self.assertIn(self.IDENT, out)
        # Distance is rendered.
        self.assertIn("0.05", out)

    def test_exit_0_on_ok_json_mode(self):
        result = self._make_result(
            outcome="ok", youtube_releases=self._make_ok_matrix())
        rc, out, _ = self._run(result=result, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "ok")
        self.assertEqual(payload["source"], "mb")
        self.assertEqual(payload["release_group_identifier"], "rg-uuid")
        self.assertFalse(payload["from_cache"])
        self.assertEqual(payload["duration_ms"], 42)
        self.assertEqual(len(payload["youtube_releases"]), 1)
        yt_rel = payload["youtube_releases"][0]
        self.assertEqual(yt_rel["yt_browse_id"], "MPREb_xxx")
        self.assertEqual(len(yt_rel["distances"]), 1)
        self.assertAlmostEqual(
            yt_rel["distances"][0]["distance"], 0.05, places=4)

    def test_exit_2_on_not_found(self):
        rc, _, _ = self._run(outcome="not_found")
        self.assertEqual(rc, 2)

    def test_exit_5_on_unresolved_4xx_client_mentions_throttle(self):
        result = self._make_result(
            outcome="unresolved_4xx_client",
            error_message="YT user error: rate limited (429)",
        )
        rc, out, _ = self._run(result=result)
        self.assertEqual(rc, 5)
        # Operator should see why: throttling / 4xx in the output.
        self.assertIn("unresolved_4xx_client", out)

    def test_exit_5_on_unresolved_timeout(self):
        rc, _, _ = self._run(outcome="unresolved_timeout")
        self.assertEqual(rc, 5)

    def test_exit_5_on_youtube_parse_failed(self):
        result = self._make_result(
            outcome="youtube_parse_failed",
            error_message="YT parse failed: 'tracks'",
        )
        rc, out, _ = self._run(result=result)
        self.assertEqual(rc, 5)
        # Parse failure mention so operator may want to bump ytmusicapi.
        self.assertIn("youtube_parse_failed", out)

    def test_refresh_flag_forwarded_to_service(self):
        rc, _, mock_resolve = self._run(outcome="ok", refresh=True)
        self.assertEqual(rc, 0)
        # The resolve call took refresh=True.
        _, kwargs = mock_resolve.call_args
        self.assertIs(kwargs.get("refresh"), True)

    def test_refresh_default_false(self):
        rc, _, mock_resolve = self._run(outcome="ok")
        self.assertEqual(rc, 0)
        _, kwargs = mock_resolve.call_args
        self.assertIs(kwargs.get("refresh"), False)

    def test_json_mode_emits_all_result_fields(self):
        result = self._make_result(
            outcome="ok", youtube_releases=self._make_ok_matrix())
        rc, out, _ = self._run(result=result, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        # All YoutubeAlbumResolverResult fields must round-trip.
        for field in ("outcome", "release_group_identifier", "source",
                      "from_cache", "youtube_releases", "error_message",
                      "duration_ms"):
            self.assertIn(field, payload)

    def test_identifier_passed_through_positional(self):
        rc, _, mock_resolve = self._run(outcome="not_found")
        self.assertEqual(rc, 2)
        # First positional arg to resolve_youtube_album is the identifier.
        args, _ = mock_resolve.call_args
        self.assertEqual(args[0], self.IDENT)

    def test_session_close_called_on_happy_path(self):
        """Round 2 P2-2: the CLI's ``finally`` block must call
        ``session.close()`` so the requests connection pool is
        released even on success. Mirror of the web-route test —
        closes the CLI ⇄ API symmetry gap (maintainability-7).
        """
        rc, _, _ = self._run(outcome="ok")
        self.assertEqual(rc, 0)
        self.assertEqual(
            self._last_session_cls.close_calls, 1,
            msg="CLI must call session.close() exactly once on "
                "happy-path resolves (round 2 P2-2 / CLI symmetry)",
        )

    def test_session_close_called_when_service_raises(self):
        """If ``resolve_youtube_album`` raises mid-CLI, the
        ``finally`` clause still releases the session so the
        connection pool isn't leaked.
        """
        with self.assertRaises(RuntimeError):
            self._run(
                outcome="ok",
                resolver_side_effect=RuntimeError(
                    "simulated mid-CLI failure",
                ),
            )

        self.assertEqual(
            self._last_session_cls.close_calls, 1,
            msg="CLI must close the session even when the resolver "
                "raises mid-call (round 2 P2-2)",
        )


class TestCmdSearchPlanHistory(unittest.TestCase):
    """``pipeline-cli search-plan history`` wraps
    ``SearchPlanService.history_for_request``. Counterpart of the API
    endpoint ``GET /api/pipeline/<id>/search-plan/history`` — both must
    stay in sync; see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"."""

    def _seed(self, n: int = 5):
        from tests.fakes import FakePipelineDB
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            year=2020, status="wanted",
        )
        for i in range(n):
            db.log_search(rid, query=f"q{i}", outcome="no_match")
        return db, rid

    def _run(self, db, rid, *, limit=None, before_id=None, json_out=False):
        args = argparse.Namespace(
            id=rid,
            limit=limit,
            before_id=before_id,
            json=json_out,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch("lib.config.read_runtime_config") as mock_cfg:
            import configparser

            from lib.config import CratediggerConfig
            cp = configparser.RawConfigParser()
            cp.read_string("[General]\n")
            mock_cfg.return_value = CratediggerConfig.from_ini(cp)
            rc = pipeline_cli.cmd_search_plan_history(db, args)
        return rc, stdout.getvalue()

    def test_history_success_default_limit_human_output(self):
        db, rid = self._seed(n=3)
        rc, out = self._run(db, rid)
        self.assertEqual(rc, 0)
        self.assertIn("q2", out)
        self.assertIn("q1", out)
        self.assertIn("q0", out)

    def test_history_success_json_output_carries_payload(self):
        db, rid = self._seed(n=3)
        rc, out = self._run(db, rid, limit=2, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["request_id"], rid)
        self.assertEqual(len(payload["rows"]), 2)
        # Newest first.
        self.assertEqual(payload["rows"][0]["query"], "q2")
        self.assertEqual(payload["rows"][1]["query"], "q1")
        self.assertIsNotNone(payload["next_before_id"])

    def test_history_returns_2_when_request_not_found(self):
        from tests.fakes import FakePipelineDB
        rc, out = self._run(FakePipelineDB(), 9999)
        self.assertEqual(rc, 2)
        self.assertIn("request_not_found", out)

    def test_history_returns_3_on_invalid_limit(self):
        db, rid = self._seed(n=2)
        rc, out = self._run(db, rid, limit=500)
        self.assertEqual(rc, 3)
        self.assertIn("[1, 200]", out)

    def test_history_returns_3_on_zero_limit(self):
        db, rid = self._seed(n=2)
        rc, _out = self._run(db, rid, limit=0)
        self.assertEqual(rc, 3)

    def test_history_returns_3_on_negative_before_id(self):
        db, rid = self._seed(n=2)
        rc, _out = self._run(db, rid, limit=10, before_id=0)
        self.assertEqual(rc, 3)

    def test_history_paginates_via_before_id(self):
        db, rid = self._seed(n=5)
        rc1, out1 = self._run(db, rid, limit=3, json_out=True)
        first = json.loads(out1)
        self.assertEqual(rc1, 0)
        self.assertIsNotNone(first["next_before_id"])
        rc2, out2 = self._run(
            db, rid, limit=3, before_id=first["next_before_id"],
            json_out=True,
        )
        second = json.loads(out2)
        self.assertEqual(rc2, 0)
        self.assertEqual(len(second["rows"]), 2)
        self.assertIsNone(second["next_before_id"])
        # No row appears in both pages.
        first_ids = {r["id"] for r in first["rows"]}
        second_ids = {r["id"] for r in second["rows"]}
        self.assertFalse(first_ids.intersection(second_ids))

    def test_history_human_output_shows_next_page_hint(self):
        db, rid = self._seed(n=5)
        rc, out = self._run(db, rid, limit=3)
        self.assertEqual(rc, 0)
        # Hint surfaces the next-page cursor so operators can re-run.
        self.assertIn("--before-id", out)

    def test_history_json_success_omits_outcome_and_error_message(self):
        """F7: --json on success must match the API 200 shape — no
        ``outcome`` or ``error_message`` keys that the API omits."""
        db, rid = self._seed(n=2)
        rc, out = self._run(db, rid, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertNotIn("outcome", payload,
                         "--json success must not include outcome key")
        self.assertNotIn("error_message", payload,
                         "--json success must not include error_message key")
        # Core API fields must still be present.
        self.assertIn("request_id", payload)
        self.assertIn("rows", payload)
        self.assertIn("next_before_id", payload)


class TestPipelineCliTriage(unittest.TestCase):
    """``pipeline-cli triage`` (U16) wraps ``lib.triage_service``.

    Counterpart of the U17 HTTP routes — both wrap the same service and
    must stay in sync (CLAUDE.md § "CLI ⇄ API surface symmetry"). Tests
    drive the real service against ``FakePipelineDB`` rather than
    mocking ``compose_triage_for_request`` / ``list_triage`` — those are
    our own logic, not leaf seams. See `MOCKS: LEAF-SEAM ONLY`.
    """

    def test_list_help_advertises_every_filter_form(self) -> None:
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            parser.parse_args(["triage", "list", "--help"])

        self.assertEqual(raised.exception.code, 0)
        normalized_help = " ".join(output.getvalue().split())
        self.assertIn(
            "Filter spec: all | unfindable | unfindable:<category> | "
            "data_quality | data_quality:<field> | "
            "data_quality:status=<status> | data_quality:reason=<code> | "
            "search_not_converting | converged",
            normalized_help,
        )

    def _seed_healthy(self, db, rid: int) -> None:
        from tests.helpers import make_request_row
        db.seed_request(make_request_row(
            id=rid, artist_name="Healthy", album_title="Imported Album",
            status="imported", failure_class="resolved",
        ))

    def _seed_unfindable(self, db, rid: int, category: str = "artist_absent") -> None:
        from datetime import datetime

        from tests.helpers import make_request_row
        now = datetime(2026, 5, 26, 12, 0, 0, tzinfo=UTC)
        db.seed_request(make_request_row(
            id=rid, artist_name=f"Vanished {rid}",
            album_title=f"Unfindable Album {rid}",
            status="wanted",
            unfindable_category=category,
            unfindable_categorised_at=now,
        ))

    def _seed_data_quality(
        self, db, rid: int, *,
        field_name: str = "release_group_year",
        status: str = "unresolved_404",
        reason_code: str = "http_404",
    ) -> None:
        """Seed a request with one unresolved field-resolution row.

        Production shape: ``status`` is the resolver-status bucket
        (``unresolved_4xx_client`` / ``unresolved_404`` / ...) and
        ``reason_code`` is the per-occurrence specifier (``http_400`` /
        ``http_404`` / ...). See ``lib/field_resolver_service.py``.
        """
        from tests.helpers import make_request_row
        db.seed_request(make_request_row(
            id=rid, artist_name=f"DataOnly {rid}",
            album_title=f"Field Album {rid}", status="wanted",
        ))
        db.record_field_resolution(
            request_id=rid, field_name=field_name,
            status=status, reason_code=reason_code,
        )

    # --- triage show ----------------------------------------------------

    def _run_show(self, db, rid, *, json_out=False):
        args = argparse.Namespace(id=rid, json=json_out)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_triage_show(db, args)
        return rc, stdout.getvalue(), stderr.getvalue()

    def _run_stop(
        self, db, rid, *, signal_token="a" * 64,
        json_out=False,
    ):
        args = argparse.Namespace(
            id=rid,
            signal_token=signal_token,
            confirm="STOP",
            json=json_out,
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_triage_stop(db, args)
        return rc, stdout.getvalue(), stderr.getvalue()

    def test_convergence_filter_show_and_stop_share_signal_identity(self):
        from lib.convergence_service import ConvergenceSignal

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="wanted"))
        now = datetime(2026, 8, 3, tzinfo=UTC)
        db.convergence_signals[41] = ConvergenceSignal(
            request_id=41,
            observation_count=7,
            distinct_peer_count=6,
            distinct_candidate_snapshot_count=5,
            distinct_codec_count=2,
            cliff_hz=15_000,
            raw_cliff_min_hz=14_900,
            raw_cliff_max_hz=15_100,
            cliff_spread_hz=200,
            latest_qualifying_log_id=99,
            first_observed_at=now,
            latest_observed_at=now,
            signal_token="a" * 64,
        )

        rc, shown, err = self._run_show(db, 41)
        self.assertEqual((rc, err), (0, ""))
        self.assertIn("6", shown)
        self.assertIn("--signal-token " + "a" * 64, shown)
        rc, listed, err = self._run_list(db, filter_spec="converged")
        self.assertEqual((rc, err), (0, ""))
        self.assertIn("converged 15kHz/6 peers", listed)

        rc, _out, err = self._run_stop(db, 41, signal_token="b" * 64)
        self.assertEqual(rc, 4)
        self.assertIn("stale", err)
        rc, out, err = self._run_stop(db, 41, json_out=True)
        self.assertEqual((rc, err), (0, ""))
        self.assertEqual(json.loads(out)["outcome"], "stopped")
        self.assertEqual(db.request(41)["status"], "unsearchable")

        rc, stopped_show, err = self._run_show(db, 41)
        self.assertEqual((rc, err), (0, ""))
        self.assertNotIn("pipeline-cli triage stop", stopped_show)
        self.assertIn("pipeline-cli set 41 wanted", stopped_show)

        rc, _out, err = self._run_stop(db, 999)
        self.assertEqual(rc, 2)
        self.assertIn("not_found", err)

        db.seed_request(make_request_row(id=42, status="wanted"))
        rc, _out, err = self._run_stop(db, 42)
        self.assertEqual(rc, 3)
        self.assertIn("not_converged", err)

    def test_convergence_stop_database_outage_returns_exit_5(self):
        import psycopg2

        class UnavailableDB(FakePipelineDB):
            def stop_search_for_convergence(
                self, request_id: int, *, signal_token: str,
            ) -> NoReturn:
                del request_id, signal_token
                raise psycopg2.OperationalError("database unavailable")

        rc, _out, err = self._run_stop(UnavailableDB(), 41)
        self.assertEqual(rc, 5)
        self.assertIn("unavailable", err)

    # #1122 F1 moved the quarantine CLI tests that lived here
    # (test_quarantine_json_matches_typed_service_shape,
    # test_quarantine_human_output_names_folder,
    # test_quarantine_scan_error_returns_5_with_json_error, and their
    # ``_run_quarantine`` helper) into ``TestCmdTriageQuarantine`` below,
    # which extends ``_FakeDbWebServerCase``: the command now relays
    # through a real ``GET /api/triage/quarantine`` instead of calling
    # ``list_unreferenced_quarantine_folders`` directly, so its tests need
    # the same real-HTTP-server harness every other #1063 CLI adapter uses.

    def test_show_human_renders_request_meta_and_search_log(self):
        from lib.triage_service import TriageResult  # noqa: F401
        db = FakePipelineDB()
        self._seed_unfindable(db, 42)
        # One search_log row so the recent_entries renderer is exercised.
        db.log_search(
            request_id=42, query="vanished album", result_count=0,
            outcome="exhausted", rejection_reason=None,
        )
        rc, out, err = self._run_show(db, 42)
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        self.assertIn("Vanished 42", out)
        self.assertIn("Unfindable Album 42", out)
        self.assertIn("wanted", out)
        self.assertIn("artist_absent", out)
        # At least one rendered search log row.
        self.assertIn("exhausted", out)
        self.assertIn("recent_entries", out)

    def test_show_human_renders_cross_request_conflict_marker(self):
        """#1196 item 2: the cross-request enqueue-guard skip marker is
        visible on `pipeline-cli triage show` -- an operator reading a
        plain no_match row can tell a deliberate decline from genuine
        network absence."""
        from lib.pipeline_db import ConsumedAttemptInput, SearchPlanItemInput

        db = FakePipelineDB()
        self._seed_unfindable(db, 42)
        plan_id = db.create_successful_search_plan(
            request_id=42, generator_id="g1",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="q0",
                canonical_query_key="q0")],
        )
        active = db.get_active_search_plan(42)
        assert active is not None
        item_id = active.items[0].id
        db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=42, plan_id=plan_id, plan_item_id=item_id,
            plan_ordinal=0, plan_strategy="default",
            plan_canonical_query_key="q0", plan_repeat_group=None,
            plan_generator_id="g1", query="q0", outcome="no_match",
            plan_item_count=1, cycle_count_snapshot=0,
            cross_request_conflict_request_ids=(8781, 8846),
        ))

        rc, out, err = self._run_show(db, 42)

        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        self.assertIn("conflict=8781,8846", out)

    def test_show_human_renders_dash_when_no_conflict_marker(self):
        """Must-still-work control: a plain no_match row with no guard
        skip renders ``conflict=-``, not a stray marker."""
        from lib.pipeline_db import ConsumedAttemptInput, SearchPlanItemInput

        db = FakePipelineDB()
        self._seed_unfindable(db, 42)
        plan_id = db.create_successful_search_plan(
            request_id=42, generator_id="g1",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="q0",
                canonical_query_key="q0")],
        )
        active = db.get_active_search_plan(42)
        assert active is not None
        item_id = active.items[0].id
        db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=42, plan_id=plan_id, plan_item_id=item_id,
            plan_ordinal=0, plan_strategy="default",
            plan_canonical_query_key="q0", plan_repeat_group=None,
            plan_generator_id="g1", query="q0", outcome="no_match",
            plan_item_count=1, cycle_count_snapshot=0,
        ))

        rc, out, err = self._run_show(db, 42)

        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        self.assertIn("conflict=-", out)

    def test_show_json_round_trips_through_msgspec(self):
        """`--json` payload must decode back into a ``TriageResult`` so
        the API consumer gets the same wire shape on both surfaces."""
        from lib.triage_service import TriageResult
        db = FakePipelineDB()
        self._seed_unfindable(db, 42)
        db.log_search(
            request_id=42, query="q", result_count=0, outcome="exhausted",
        )
        rc, out, err = self._run_show(db, 42, json_out=True)
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        # Valid JSON parseable as TriageResult via msgspec convert.
        payload = json.loads(out)
        result = msgspec.convert(payload, type=TriageResult)
        self.assertEqual(result.request_meta.id, 42)
        self.assertEqual(result.request_meta.artist_name, "Vanished 42")
        assert result.unfindable is not None
        self.assertEqual(result.unfindable.category, "artist_absent")

    def test_show_unknown_id_returns_2_with_stderr_message(self):
        db = FakePipelineDB()
        rc, _out, err = self._run_show(db, 9999)
        self.assertEqual(rc, 2)
        # Human path writes to stderr; the operator running `triage show`
        # should see the error there, not on stdout.
        self.assertIn("9999", err)
        self.assertIn("not found", err.lower())

    def test_show_unknown_id_json_returns_2_with_structured_payload(self):
        db = FakePipelineDB()
        rc, out, _err = self._run_show(db, 9999, json_out=True)
        self.assertEqual(rc, 2)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "Not found")
        self.assertEqual(payload["request_id"], 9999)

    def test_show_healthy_request_renders_no_unfindable_signal(self):
        db = FakePipelineDB()
        self._seed_healthy(db, 1)
        rc, out, _err = self._run_show(db, 1)
        self.assertEqual(rc, 0)
        self.assertIn("Healthy", out)
        self.assertIn("Imported Album", out)
        self.assertIn("(no signals)", out)

    # --- triage list ----------------------------------------------------

    def _run_list(self, db, *, filter_spec="all", limit=50, after=None,
                  json_out=False):
        args = argparse.Namespace(
            filter=filter_spec,
            limit=limit,
            after=after,
            json=json_out,
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_triage_list(db, args)
        return rc, stdout.getvalue(), stderr.getvalue()

    def _seed_cohort(self) -> FakePipelineDB:
        db = FakePipelineDB()
        self._seed_healthy(db, 1)
        self._seed_unfindable(db, 2, category="artist_absent")
        self._seed_unfindable(db, 3, category="wrong_pressing_available")
        # Production shape: status='unresolved_4xx_client' (the sticky
        # bucket #374 surfaces on), reason_code='http_400' (the specific
        # HTTP code the resolver hit).
        self._seed_data_quality(
            db, 4, field_name="release_group_year",
            status="unresolved_4xx_client", reason_code="http_400",
        )
        return db

    def test_list_unfindable_returns_only_unfindable_rows(self):
        db = self._seed_cohort()
        rc, out, err = self._run_list(db, filter_spec="unfindable")
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        # Rows 2 and 3 are unfindable. Row 1 (healthy) and row 4 (data
        # quality only) must be absent from the rendered table.
        self.assertIn("Vanished 2", out)
        self.assertIn("Vanished 3", out)
        self.assertNotIn("Healthy", out)
        self.assertNotIn("DataOnly 4", out)

    def test_list_data_quality_returns_data_quality_rows(self):
        db = self._seed_cohort()
        rc, out, _err = self._run_list(db, filter_spec="data_quality")
        self.assertEqual(rc, 0)
        self.assertIn("DataOnly 4", out)
        # Healthy + unfindable rows without resolutions must not appear.
        self.assertNotIn("Healthy", out)
        self.assertNotIn("Vanished 2", out)

    def test_list_data_quality_status_filter_374(self):
        """#374 canonical form — ``data_quality:status=<resolver_status>``
        filters on the resolver-status column (what
        ``lib/field_resolver_service.py`` actually writes)."""
        db = self._seed_cohort()
        rc, out, _err = self._run_list(
            db, filter_spec="data_quality:status=unresolved_4xx_client",
        )
        self.assertEqual(rc, 0)
        self.assertIn("DataOnly 4", out)

    def test_list_data_quality_reason_code_filter(self):
        """``data_quality:reason=<code>`` complementary filter on the
        ``reason_code`` column (HTTP code-specific)."""
        db = self._seed_cohort()
        rc, out, _err = self._run_list(
            db, filter_spec="data_quality:reason=http_400",
        )
        self.assertEqual(rc, 0)
        self.assertIn("DataOnly 4", out)

    def test_list_invalid_filter_returns_3_and_emits_valid_forms(self):
        db = self._seed_cohort()
        rc, _out, err = self._run_list(db, filter_spec="garbage_value")
        self.assertEqual(rc, 3)
        # Operator sees the valid forms on stderr.
        self.assertIn("Invalid filter spec", err)
        self.assertIn("all", err)
        self.assertIn("unfindable", err)
        self.assertIn("data_quality", err)
        self.assertIn("search_not_converting", err)

    def test_list_json_emits_envelope_matching_api_shape(self):
        """CLI ``--json`` wraps results in the same envelope the API
        emits: ``{results, next_after, page_size, filter}``. Without
        the envelope, agents piping ``--json | jq '.next_after'``
        cannot extract the pagination cursor."""
        from lib.triage_service import TriageResult
        db = self._seed_cohort()
        rc, out, err = self._run_list(
            db, filter_spec="unfindable", json_out=True,
        )
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        payload = json.loads(out)
        # Envelope-shape contract.
        self.assertIsInstance(payload, dict)
        self.assertIn("results", payload)
        self.assertIn("next_after", payload)
        self.assertIn("page_size", payload)
        self.assertIn("filter", payload)
        self.assertEqual(payload["filter"], "unfindable")
        self.assertEqual(payload["page_size"], 50)
        # Partial page (2 of 50) → next_after is None.
        self.assertIsNone(payload["next_after"])
        self.assertIsInstance(payload["results"], list)
        self.assertEqual(len(payload["results"]), 2)
        # Each element must round-trip back to TriageResult.
        triage_rows = [
            msgspec.convert(entry, type=TriageResult)
            for entry in payload["results"]
        ]
        ids = sorted(r.request_meta.id for r in triage_rows)
        self.assertEqual(ids, [2, 3])

    def test_list_json_invalid_filter_emits_json_error_envelope(self):
        """``--json`` + invalid filter must emit a JSON-parseable
        payload on stdout (mirrors cmd_triage_show's 404 path and the
        API 400 envelope). Without this, agents piping ``--json | jq``
        break on the text-stderr fallback."""
        db = self._seed_cohort()
        rc, out, err = self._run_list(
            db, filter_spec="garbage_value", json_out=True,
        )
        self.assertEqual(rc, 3)
        self.assertEqual(err, "")  # Nothing on stderr in JSON mode.
        payload = json.loads(out)
        self.assertIn("error", payload)
        self.assertIn("valid_filters", payload)
        self.assertIn("valid_unfindable_categories", payload)
        self.assertIn("valid_data_quality_fields", payload)
        self.assertIsInstance(payload["valid_filters"], list)
        self.assertIn("all", payload["valid_filters"])

    def test_list_limit_out_of_range_returns_3(self):
        """API-parity bounds check: limit must be in [1, 200]."""
        db = self._seed_cohort()
        rc, _out, err = self._run_list(db, filter_spec="all", limit=500)
        self.assertEqual(rc, 3)
        self.assertIn("--limit", err)

    def test_list_after_below_one_returns_3(self):
        """API-parity bounds check: after must be >= 1."""
        db = self._seed_cohort()
        rc, _out, err = self._run_list(db, filter_spec="all", after=0)
        self.assertEqual(rc, 3)
        self.assertIn("--after", err)

    def test_list_empty_result_is_exit_0(self):
        db = FakePipelineDB()
        # No rows seeded — empty cohort under any filter.
        rc, out, _err = self._run_list(db, filter_spec="unfindable")
        self.assertEqual(rc, 0)
        self.assertIn("No results", out)

    def test_list_limit_returns_page_with_next_after_footer(self):
        db = self._seed_cohort()
        # 2 unfindable rows seeded (ids 2 and 3); limit=2 means full page
        # and the footer should print the next --after cursor.
        rc, out, _err = self._run_list(
            db, filter_spec="unfindable", limit=2,
        )
        self.assertEqual(rc, 0)
        self.assertIn("Vanished 2", out)
        self.assertIn("Vanished 3", out)
        self.assertIn("--after=3", out)
        self.assertIn("--limit=2", out)

    def test_list_partial_page_omits_next_after_footer(self):
        db = self._seed_cohort()
        # Only 2 unfindable rows; limit=10 returns a partial page with no
        # follow-on cursor.
        rc, out, _err = self._run_list(
            db, filter_spec="unfindable", limit=10,
        )
        self.assertEqual(rc, 0)
        self.assertNotIn("--after=", out)


class TestPipelineCliLongTail(unittest.TestCase):
    """``pipeline-cli long-tail`` — the worklist read counterpart of
    ``GET /api/pipeline/long-tail`` (CLI ⇄ API symmetry, U1).

    Both surfaces wrap ``lib.long_tail_service.list_long_tail``. Tests
    inject a deterministic ``band_fn`` via the kwarg-DI seam so they
    don't need a live beets library.
    """

    @staticmethod
    def _band_fn(mapping):
        def _fn(release_ids):
            return {rid: mapping.get(rid, "missing") for rid in release_ids}
        return _fn

    def _seed(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=RELEASE_A,
            artist_name="Missing Artist", album_title="Gone"))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id=RELEASE_B,
            artist_name="On Disk", album_title="Have It"))
        db.seed_request(make_request_row(
            id=3, status="imported", mb_release_id=RELEASE_C))
        return db

    def _run(self, db, *, band=None, request_id=None, json_out=False,
             band_fn=None):
        args = argparse.Namespace(band=band, id=request_id, json=json_out)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_long_tail(
                db, args, band_fn=band_fn)
        return rc, stdout.getvalue(), stderr.getvalue()

    def test_band_missing_filter_returns_only_missing_rows(self):
        db = self._seed()
        band_fn = self._band_fn({RELEASE_B: "transparent"})
        rc, out, err = self._run(db, band="missing", band_fn=band_fn)
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        self.assertIn("Missing Artist", out)
        self.assertNotIn("On Disk", out)  # transparent, filtered out

    def test_json_emits_typed_envelope(self):
        db = self._seed()
        band_fn = self._band_fn({RELEASE_B: "transparent"})
        rc, out, _err = self._run(db, json_out=True, band_fn=band_fn)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(set(payload), {"results", "band", "count"})
        self.assertIsNone(payload["band"])
        self.assertEqual(payload["count"], 2)
        # Both wanted rows present; imported row absent. Round-trips into
        # the Struct (wire shape == Struct shape).
        from lib.long_tail_service import LongTailRow
        rows = [msgspec.convert(r, type=LongTailRow) for r in payload["results"]]
        by_id = {r.id: r for r in rows}
        self.assertEqual(set(by_id), {1, 2})
        self.assertEqual(by_id[1].band, "missing")
        self.assertEqual(by_id[2].band, "transparent")

    def test_json_band_filter_echoed(self):
        db = self._seed()
        band_fn = self._band_fn({RELEASE_A: "missing", RELEASE_B: "missing"})
        rc, out, _ = self._run(
            db, band="missing", json_out=True, band_fn=band_fn)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["band"], "missing")
        self.assertEqual(payload["count"], 2)

    def test_cli_band_fn_propagates_real_beets_open_failure(self):
        """A missing SQLite authority is not evidence of Beets absence."""
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.ini")
            missing_db = os.path.join(root, "missing.db")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "[Beets]\n"
                    f"library = {missing_db}\n"
                    f"directory = {root}\n"
                )
            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
                clear=False,
            ), self.assertRaisesRegex(FileNotFoundError, "Beets DB not found"):
                pipeline_cli_long_tail._cli_band_fn([DISCOGS_RELEASE])

    def test_cli_band_fn_propagates_real_beets_query_failure(self):
        """A real SQLite query error remains distinguishable from absence."""
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.ini")
            empty_db = os.path.join(root, "empty.db")
            sqlite3.connect(empty_db).close()
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "[Beets]\n"
                    f"library = {empty_db}\n"
                    f"directory = {root}\n"
                )
            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
                clear=False,
            ), self.assertRaises(sqlite3.OperationalError) as raised:
                pipeline_cli_long_tail._cli_band_fn([DISCOGS_RELEASE])

        self.assertEqual(raised.exception.sqlite_errorcode, sqlite3.SQLITE_ERROR)

    def test_beets_outage_maps_to_exit_five_without_actionable_rows(self):
        """The CLI maps unavailable authority to the shared retryable error."""
        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "long-tail",
            "--band=missing",
            "--json",
        ]
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=None,
            discogs_release_id=DISCOGS_RELEASE,
            artist_name="Not Known Missing",
            album_title="Discogs Authority Failed",
        ))
        stdout = io.StringIO()
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.ini")
            missing_db = os.path.join(root, "missing.db")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "[Beets]\n"
                    f"library = {missing_db}\n"
                    f"directory = {root}\n"
                )
            with patch.object(sys, "argv", argv), patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
                clear=False,
            ), patch(
                "scripts.pipeline_cli.cli.PipelineDB", return_value=db,
            ), redirect_stdout(stdout), redirect_stderr(stderr), self.assertRaises(
                SystemExit,
            ) as raised:
                pipeline_cli.main()

        self.assertEqual(raised.exception.code, 5)
        self.assertEqual(json.loads(stdout.getvalue()), UNAVAILABLE_ERROR)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(db.close_calls, 1)

    def test_ambiguous_beets_maps_to_exit_four_with_shared_payload(self):
        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "long-tail",
            "--json",
        ]
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=RELEASE_A,
        ))
        stdout = io.StringIO()
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.ini")
            beets_db = os.path.join(root, "beets.db")
            _create_test_db(beets_db)
            _insert_album(
                beets_db,
                1,
                RELEASE_A,
                [(256_000, "/music/one/01.mp3")],
            )
            _insert_album(
                beets_db,
                2,
                RELEASE_A,
                [(256_000, "/music/two/01.mp3")],
            )
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "[Beets]\n"
                    f"library = {beets_db}\n"
                    f"directory = {root}\n"
                )
            with patch.object(sys, "argv", argv), patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
                clear=False,
            ), patch(
                "scripts.pipeline_cli.cli.PipelineDB", return_value=db,
            ), redirect_stdout(stdout), redirect_stderr(stderr), self.assertRaises(
                SystemExit,
            ) as raised:
                pipeline_cli.main()

        self.assertEqual(raised.exception.code, 4)
        self.assertEqual(json.loads(stdout.getvalue()), CONFLICT_ERROR)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(db.close_calls, 1)

    def test_invalid_request_identity_maps_to_exit_four(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id="not-a-release-id",
            discogs_release_id=None,
        ))

        rc, out, err = self._run(
            db,
            json_out=True,
            band_fn=lambda _release_ids: {},
        )

        self.assertEqual(rc, 4)
        self.assertEqual(json.loads(out), CONFLICT_ERROR)
        self.assertEqual(err, "")

    def test_unavailable_text_error_uses_stderr_and_exit_five(self):
        db = self._seed()

        def unavailable(_release_ids):
            raise FileNotFoundError("Beets DB not found")

        rc, out, err = self._run(db, band_fn=unavailable)

        self.assertEqual(rc, 5)
        self.assertEqual(out, "")
        self.assertEqual(
            err,
            "long_tail_authority_unavailable: "
            "Current Beets authority is unavailable; retry later.\n",
        )

    def test_unexpected_schema_failure_still_propagates(self):
        db = self._seed()
        failure = sqlite3.OperationalError("no such table: albums")
        failure.sqlite_errorcode = sqlite3.SQLITE_ERROR

        def broken_schema(_release_ids):
            raise failure

        with self.assertRaises(sqlite3.OperationalError) as raised:
            self._run(db, band_fn=broken_schema)

        self.assertIs(raised.exception, failure)

    def test_empty_cohort_exit_zero(self):
        db = FakePipelineDB()
        rc, out, _err = self._run(db, band_fn=self._band_fn({}))
        self.assertEqual(rc, 0)
        self.assertIn("No wanted rows", out)

    def test_single_id_exit_zero_with_band(self):
        db = self._seed()
        band_fn = self._band_fn({RELEASE_B: "transparent"})
        rc, out, _err = self._run(
            db, request_id=2, json_out=True, band_fn=band_fn)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(set(payload), {"result", "id"})
        self.assertEqual(payload["id"], 2)
        self.assertEqual(payload["result"]["band"], "transparent")

    def test_single_id_not_wanted_exit_two(self):
        db = self._seed()
        rc, _out, err = self._run(
            db, request_id=3, band_fn=self._band_fn({}))  # id 3 is imported
        self.assertEqual(rc, 2)
        self.assertIn("not found or not wanted", err)


class TestPipelineCliRoutes(unittest.TestCase):
    """U18 step 3: ``pipeline-cli routes`` self-documents the CLI surface."""

    def _run_routes(
        self, json_mode: bool = False,
    ) -> tuple[int, str]:
        argv = ["pipeline_cli.py", "routes"]
        if json_mode:
            argv.append("--json")
        # ``cmd_routes`` doesn't need a DB; ``main()`` short-circuits the
        # PipelineDB construction for this subcommand. The patch is still
        # in place defensively in case a future caller flips that wiring.
        db = FakePipelineDB()
        with patch.object(sys, "argv", argv), patch(
            "scripts.pipeline_cli.cli.PipelineDB", return_value=db,
        ), redirect_stdout(io.StringIO()) as out, self.assertRaises(SystemExit) as raised:
            pipeline_cli.main()
        code = raised.exception.code
        return (code if isinstance(code, int) else 0), out.getvalue()

    def test_routes_text_lists_known_subcommands(self):
        rc, output = self._run_routes()
        self.assertEqual(rc, 0)
        # Top-level subcommands that exist regardless of nested routing.
        self.assertIn("list", output)
        self.assertIn("status", output)
        # Nested commands are emitted as space-separated leaves.
        self.assertIn("search-plan show", output)
        self.assertIn("triage list", output)
        self.assertIn("triage quarantine", output)
        # The ``routes`` command must self-describe.
        self.assertIn("routes", output)

    def test_routes_json_emits_shape_matching_help_metadata(self):
        rc, output = self._run_routes(json_mode=True)
        self.assertEqual(rc, 0)
        data = json.loads(output)
        self.assertIsInstance(data, list)
        for entry in data:
            self.assertIn("subcommand", entry)
            self.assertIn("args", entry)
            self.assertIn("description", entry)
            self.assertIsInstance(entry["subcommand"], str)
            self.assertIsInstance(entry["args"], list)
            self.assertIsInstance(entry["description"], str)
        names_list = [entry["subcommand"] for entry in data]
        names_set = set(names_list)
        for expected in (
            "list", "search-plan show", "triage list",
            "triage quarantine", "routes",
        ):
            self.assertIn(expected, names_set)

        # Sort invariant — operators consume this as a stable index.
        # Compare the raw list against ``sorted(names_list)`` (not
        # ``sorted(names_set)``) so a duplicate subcommand surfaces as
        # the inequality it is, rather than being silently deduped.
        self.assertEqual(names_list, sorted(names_list))


class TestPipelineCliDiskCoverage(unittest.TestCase):
    def test_disk_coverage_prints_json_from_shared_service(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted",
            mb_release_id="00000000-0000-4000-8000-000000000001",
        ))
        # This valid strict identity is not seeded as a Beets album.
        beets = FakeBeetsDB()

        args = MagicMock(
            beets_db="/tmp/beets.db",
            beets_directory="/tmp/library",
            counts_only=False,
            include_inverse=False,
        )
        with patch("lib.beets_db.BeetsDB", return_value=beets), \
                redirect_stdout(io.StringIO()) as out:
            pipeline_cli.cmd_disk_coverage(db, args)

        payload = json.loads(out.getvalue())
        self.assertEqual(payload["counts"]["off_disk_total"], 1)
        self.assertEqual(payload["off_disk"][0]["id"], 1)
        self.assertEqual(payload["off_disk"][0]["resolution"], {
            "kind": "missing",
        })

class TestCmdYoutubeRescue(unittest.TestCase):
    """``pipeline-cli youtube-rescue`` wraps
    ``YoutubeIngestService.submit``. Counterpart of the API endpoint
    ``POST /api/pipeline/<id>/youtube-rescue`` (U5) — both must stay in
    sync; see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry".

    Service-layer correctness lives in ``tests.test_youtube_ingest_service``;
    here we pin the outcome → exit-code mapping and the stdout/stderr/JSON
    output discipline.
    """

    def _run(self, *, outcome, download_log_id=None, detail=None,
             json_out=False, request_id=42, browse_id="MPREb_test"):
        from lib.youtube_ingest_service import (
            OUTCOME_EXIT_CODE as INGEST_EXIT_CODE,
        )
        from lib.youtube_ingest_service import (
            SubmitResult,
        )

        # Sanity: the outcome the test asks for must actually be a
        # SubmitOutcome — keeps the subTest table honest as the literal
        # evolves.
        self.assertIn(outcome, INGEST_EXIT_CODE,
                      f"unknown SubmitOutcome {outcome!r}")

        result = SubmitResult(
            outcome=outcome,
            download_log_id=download_log_id,
            detail=detail,
        )

        class _StubService:
            def submit(self, _rid, _browse):
                return result

        def _factory(_db):
            return _StubService()

        args = argparse.Namespace(
            request_id=request_id, browse_id=browse_id, json=json_out,
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_youtube_rescue(
                MagicMock(), args, service_factory=_factory)
        return rc, stdout.getvalue(), stderr.getvalue()

    # ----- outcome → exit code subTest table -----

    def test_exit_codes_match_service_table_for_every_outcome(self):
        """Every ``SubmitOutcome`` maps to its
        ``OUTCOME_EXIT_CODE`` entry — single-source-of-truth contract."""
        from lib.youtube_ingest_service import OUTCOME_EXIT_CODE as TABLE
        cases = [
            ("accepted",                    0),
            ("request_not_found",           2),
            ("wrong_state",                 4),
            ("in_flight",                   4),
            ("no_resolver_mapping",         3),
            ("track_count_precheck_failed", 3),
            ("transient",                   5),
        ]
        # The table itself must match the literal coverage above —
        # forces future contributors who add a new outcome to update
        # both the service map and this subTest table together.
        self.assertEqual({o for o, _ in cases}, set(TABLE),
                         "subTest cases drifted from OUTCOME_EXIT_CODE")
        for outcome, expected_rc in cases:
            with self.subTest(outcome=outcome):
                # in_flight / accepted have a populated download_log_id
                # to make sure the path that reads it doesn't crash.
                dl_id = 7 if outcome in ("accepted", "in_flight") else None
                rc, _, _ = self._run(
                    outcome=outcome, download_log_id=dl_id,
                    detail="example reason",
                )
                self.assertEqual(rc, expected_rc)
                self.assertEqual(rc, TABLE[outcome])

    # ----- plain-text output -----

    def test_accepted_prints_download_log_id_to_stdout(self):
        rc, out, err = self._run(outcome="accepted", download_log_id=99)
        self.assertEqual(rc, 0)
        self.assertIn("download_log_id=99", out)
        self.assertEqual(err, "")

    def test_failure_prints_classified_outcome_to_stderr(self):
        rc, out, err = self._run(
            outcome="wrong_state",
            detail="request 42 is in status 'imported'",
        )
        self.assertEqual(rc, 4)
        # Stdout stays clean for failure paths so JSON-piping callers
        # never see noise mixed in.
        self.assertEqual(out, "")
        self.assertIn("wrong_state", err)
        self.assertIn("imported", err)

    def test_in_flight_surfaces_existing_download_log_id_on_stderr(self):
        rc, _, err = self._run(
            outcome="in_flight", download_log_id=55,
            detail="existing youtube_running row",
        )
        self.assertEqual(rc, 4)
        self.assertIn("in_flight", err)
        self.assertIn("existing download_log_id=55", err)

    # ----- --json output -----

    def test_json_accepted_carries_full_payload(self):
        rc, out, _ = self._run(
            outcome="accepted", download_log_id=99, json_out=True,
        )
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "accepted")
        self.assertEqual(payload["download_log_id"], 99)
        self.assertIsNone(payload["detail"])

    def test_json_failure_carries_null_download_log_id(self):
        rc, out, _ = self._run(
            outcome="no_resolver_mapping",
            detail="run the resolver first",
            json_out=True,
        )
        self.assertEqual(rc, 3)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "no_resolver_mapping")
        self.assertIsNone(payload["download_log_id"])
        self.assertEqual(payload["detail"], "run the resolver first")

    # ----- argparse plumbing -----

    def test_argparse_rejects_missing_browse_id(self):
        parser_test_argv = ["youtube-rescue", "42"]
        with patch.object(sys, "argv", ["pipeline-cli"] + parser_test_argv), \
             redirect_stderr(io.StringIO()), \
             redirect_stdout(io.StringIO()), \
             self.assertRaises(SystemExit) as cm:
            pipeline_cli.main()
        # argparse exits with code 2 for missing required positionals.
        self.assertEqual(cm.exception.code, 2)

    def test_argparse_rejects_missing_request_id(self):
        parser_test_argv = ["youtube-rescue"]
        with patch.object(sys, "argv", ["pipeline-cli"] + parser_test_argv), \
             redirect_stderr(io.StringIO()), \
             redirect_stdout(io.StringIO()), \
             self.assertRaises(SystemExit) as cm:
            pipeline_cli.main()
        self.assertEqual(cm.exception.code, 2)


class TestDestructiveCliAdapters(unittest.TestCase):
    """CLI exit mappings mirror the destructive HTTP adapters."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.beets_path = os.path.join(self.tmpdir.name, "beets.db")
        _create_test_db(self.beets_path)
        self.track_path = os.path.join(
            self.tmpdir.name, "Artist A", "Album A", "01 Track.flac",
        )
        _insert_album(
            self.beets_path,
            7,
            RELEASE_A,
            [(320000, self.track_path)],
            album="Album A",
            albumartist="Artist A",
        )
        self.config_path = os.path.join(self.tmpdir.name, "config.ini")
        with open(self.config_path, "w", encoding="utf-8") as handle:
            handle.write(f"[Beets]\ndirectory = {self.tmpdir.name}\n")

    def _env(self):
        return patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": self.config_path},
            clear=False,
        )

    def test_ban_source_release_mismatch_returns_semantic_exit_3(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41, status="imported", mb_release_id=RELEASE_A,
        ))
        args = argparse.Namespace(
            request_id=41,
            release_id=RELEASE_B,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        output = io.StringIO()
        with self._env(), redirect_stdout(output):
            rc = pipeline_cli.cmd_ban_source(db, args)

        self.assertEqual(rc, 3)
        self.assertEqual(json.loads(output.getvalue())["error"], "release_mismatch")
        self.assertEqual(db.denylist, [])

    def test_ban_source_ambiguous_current_identity_returns_state_exit_4(
        self,
    ) -> None:
        _insert_album(
            self.beets_path,
            8,
            RELEASE_A,
            [(320000, os.path.join(self.tmpdir.name, "Duplicate", "01.flac"))],
            album="Album A duplicate",
            albumartist="Artist A",
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id=RELEASE_A,
        ))
        args = argparse.Namespace(
            request_id=41,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        output = io.StringIO()

        with self._env(), redirect_stdout(output):
            rc = pipeline_cli.cmd_ban_source(db, args)

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 4)
        self.assertEqual(payload["error"], "current_beets_ambiguous")
        self.assertEqual(payload["album_ids"], [7, 8])
        self.assertEqual(db.denylist, [])

    def test_ban_source_processing_returns_exact_owner_exit_4(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status="wanted",
            mb_release_id=RELEASE_A,
        ))
        owner = handoff_automation_owner(db, 41)
        args = argparse.Namespace(
            request_id=41,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        output = io.StringIO()

        with self._env(), redirect_stdout(output):
            rc = pipeline_cli.cmd_ban_source(db, args)

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 4)
        self.assertEqual(payload["error"], "transition_conflict")
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["request_id"], 41)
        self.assertEqual(payload["processing_owner"], {
            "job_id": owner.id,
            "status": owner.status,
            "preview_status": owner.preview_status,
        })
        self.assertEqual(db.denylist, [])

    def test_ban_source_incomplete_reports_resulting_searchability(self) -> None:
        from lib.beets_delete import BeetsDeleteFailed

        args = argparse.Namespace(
            request_id=41,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        delete_requests = []

        def failed_delete(request):
            delete_requests.append(request)
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason="filesystem_error",
                detail="isolated test: album retained",
                album_still_present=True,
            )

        for request_status in ("wanted", "unsearchable"):
            with self.subTest(request_status=request_status):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=41,
                    status=request_status,
                    mb_release_id=RELEASE_A,
                ))
                output = io.StringIO()
                with self._env(), redirect_stdout(output):
                    rc = pipeline_cli.cmd_ban_source(
                        db, args, beets_delete_fn=failed_delete,
                    )

                self.assertEqual(rc, 4)
                payload = json.loads(output.getvalue())
                self.assertEqual(payload["error"], "cleanup_incomplete")
                self.assertEqual(payload["status"], "partial")
                self.assertEqual(payload["request_status"], request_status)
        self.assertEqual(len(delete_requests), 2)
        self.assertEqual({request.album_id for request in delete_requests}, {7})

    def test_library_delete_lock_contention_returns_state_exit_4(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41, status="imported", mb_release_id=RELEASE_A,
        ))
        db.set_advisory_lock_result(False)
        args = argparse.Namespace(
            album_id=7,
            purge_pipeline=True,
            pipeline_id=41,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        output = io.StringIO()
        with self._env(), redirect_stdout(output):
            rc = pipeline_cli.cmd_library_delete(db, args)

        self.assertEqual(rc, 4)
        self.assertEqual(
            json.loads(output.getvalue())["error"],
            "destructive_operation_busy",
        )
        self.assertIsNotNone(db.get_request(41))

    def test_library_delete_processing_returns_exact_owner_exit_4(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status="wanted",
            mb_release_id=RELEASE_A,
        ))
        owner = handoff_automation_owner(db, 41)
        args = argparse.Namespace(
            album_id=7,
            purge_pipeline=True,
            pipeline_id=41,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        output = io.StringIO()

        with self._env(), redirect_stdout(output):
            rc = pipeline_cli.cmd_library_delete(db, args)

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 4)
        self.assertEqual(payload["error"], "transition_conflict")
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["request_id"], 41)
        self.assertEqual(payload["processing_owner"], {
            "job_id": owner.id,
            "status": owner.status,
            "preview_status": owner.preview_status,
        })
        self.assertIsNotNone(db.get_request(41))

    def test_library_delete_ambiguous_identity_returns_state_exit_4(self) -> None:
        _insert_album(
            self.beets_path,
            8,
            RELEASE_A,
            [(320000, os.path.join(self.tmpdir.name, "Duplicate", "01.flac"))],
            album="Album A duplicate",
            albumartist="Artist A",
        )
        db = FakePipelineDB()
        args = argparse.Namespace(
            album_id=7,
            purge_pipeline=False,
            pipeline_id=None,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        output = io.StringIO()

        with self._env(), redirect_stdout(output):
            rc = pipeline_cli.cmd_library_delete(db, args)

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 4)
        self.assertEqual(payload["error"], "current_beets_ambiguous")
        self.assertEqual(payload["album_ids"], [7, 8])

    def test_library_delete_success_exposes_artifacts_and_notifier_warnings(self) -> None:
        from lib.beets_delete import BeetsDeleteCompleted
        from lib.library_delete_notifiers import DeleteNotification

        db = FakePipelineDB()
        args = argparse.Namespace(
            album_id=7,
            purge_pipeline=False,
            pipeline_id=None,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        outcome = BeetsDeleteCompleted(
            album_id=7,
            album_name="Album A",
            artist_name="Artist A",
            former_album_path=self.tmpdir.name,
            deleted_tracks=1,
            deleted_artifacts=3,
            preserved_paths=(os.path.join(self.tmpdir.name, "booklet.pdf"),),
        )
        output = io.StringIO()

        delete_requests = []

        def completed_delete(request):
            delete_requests.append(request)
            with closing(sqlite3.connect(self.beets_path)) as conn:
                conn.execute("DELETE FROM items WHERE album_id = 7")
                conn.execute("DELETE FROM albums WHERE id = 7")
                conn.commit()
            return outcome

        with (
            self._env(),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_library_delete(
                db,
                args,
                beets_delete_fn=completed_delete,
                notify_fn=lambda _path: (DeleteNotification(
                    "jellyfin", "warning", "connection refused"),),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(payload["deleted_artifacts"], 3)
        self.assertEqual(payload["preserved_paths"], [
            os.path.join(self.tmpdir.name, "booklet.pdf")])
        self.assertEqual(payload["notifications"][0]["status"], "warning")
        self.assertEqual(len(delete_requests), 1)
        self.assertEqual(delete_requests[0].library_db_path, self.beets_path)
        self.assertEqual(delete_requests[0].library_root, self.tmpdir.name)

    def test_library_delete_incomplete_matches_http_semantics(self) -> None:
        from lib.beets_delete import BeetsDeleteFailed

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41, status="imported", mb_release_id=RELEASE_A,
        ))
        args = argparse.Namespace(
            album_id=7,
            purge_pipeline=True,
            pipeline_id=41,
            release_id=RELEASE_A,
            beets_db=self.beets_path,
            beets_directory=self.tmpdir.name,
        )
        output = io.StringIO()
        failure = BeetsDeleteFailed(
            album_id=7,
            reason="protocol_error",
            detail="truncated child result",
            album_still_present=False,
        )

        def lose_ack_after_metadata(_request):
            with closing(sqlite3.connect(self.beets_path)) as conn:
                conn.execute("DELETE FROM items WHERE album_id = 7")
                conn.execute("DELETE FROM albums WHERE id = 7")
                conn.commit()
            return failure

        with (
            self._env(),
            redirect_stderr(output),
        ):
            rc = pipeline_cli.cmd_library_delete(
                db,
                args,
                beets_delete_fn=lose_ack_after_metadata,
                notify_fn=lambda _path: (),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 4)
        self.assertEqual(payload["error"], "delete_incomplete")
        self.assertFalse(payload["album_still_present"])
        self.assertTrue(payload["acknowledgement_lost"])
        self.assertEqual(payload["album"], "Album A")
        self.assertEqual(payload["artist"], "Artist A")
        self.assertEqual(
            payload["former_album_path"], os.path.dirname(self.track_path),
        )
        self.assertEqual(payload["pipeline_id"], 41)
        self.assertEqual(payload["pipeline_status"], "imported")
        self.assertIsNone(payload["deleted_files"])
        self.assertIsNone(payload["deleted_artifacts"])
        self.assertIn("Beets acknowledgement was lost", payload["detail"])
        self.assertIn("metadata may be gone", payload["detail"])
        self.assertIsNotNone(db.get_request(41))

    def test_argparse_requires_server_validated_ban_confirmation(self) -> None:
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
            parser.parse_args(["ban-source", "41"])
        self.assertEqual(cm.exception.code, 2)


class _BrokenPipeStdout:
    """A stdout stand-in whose every write raises ``BrokenPipeError`` —
    models a downstream reader (e.g. ``| head``) that closed its end
    early (#1093 review round 4, finding 8)."""

    def write(self, _data: str) -> int:
        raise BrokenPipeError(32, "Broken pipe")

    def flush(self) -> None:
        pass


class TestWorldAuditCLI(unittest.TestCase):
    def test_json_keeps_bucket_b_visible_without_failing(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=743,
            mb_release_id=RELEASE_A,
            status="imported",
        ))
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=FakeBeetsDB()),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_world(
                db,
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(payload["status"], "observations_only")
        self.assertTrue(payload["complete"])
        self.assertIn(
            "current_beets_missing",
            {row["code"] for row in payload["groups"]["b"]["members"]},
        )

    def test_expected_beets_unavailability_is_incomplete_and_exits_five(
        self,
    ) -> None:
        """Issue #1355 item 4: an incomplete report is a non-successful
        exit — this used to exit 0, a pre-existing deviation from the
        sibling retag-divergence audit's own convention."""
        import scripts.pipeline_cli.audit as audit_cli

        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", side_effect=failure),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_world(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 5)
        self.assertEqual(payload["status"], "observations_only")
        self.assertFalse(payload["complete"])
        self.assertEqual(
            [row["code"] for row in payload["groups"]["b"]["members"]],
            ["current_beets_authority_unavailable"],
        )

    def test_integrity_bucket_a_returns_one(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=744,
            mb_release_id=None,
            discogs_release_id=None,
            status="imported",
        ))
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=FakeBeetsDB()),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_world(
                db,
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 1)
        self.assertEqual(payload["status"], "integrity_failed")
        self.assertEqual(
            [row["code"] for row in payload["groups"]["a"]["members"]],
            ["request_identity_missing"],
        )

    def test_unexpected_failure_remains_exit_five(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        output = io.StringIO()
        with (
            patch.object(
                audit_cli,
                "_open_beets",
                side_effect=RuntimeError("programmer defect"),
            ),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_world(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        self.assertEqual(rc, 5)
        self.assertEqual(
            json.loads(output.getvalue())["error"],
            "world_audit_failed",
        )

    def test_render_failure_after_a_successful_scan_still_exits_five(
        self,
    ) -> None:
        """#1093 review round 3, finding 2 — the same defect class as
        `cmd_audit_retag_divergence`'s finding-5 fix: the previous shape
        called `msgspec.convert`/the render+encode steps OUTSIDE the try,
        so a defect there tracebacked out uncaught (Python's default exit
        1), not the documented exit 5. Fail the RENDER step specifically,
        after `report` is already computed, to prove the fix covers the
        whole body, not just the audit call."""
        import scripts.pipeline_cli.audit as audit_cli

        db = FakePipelineDB()
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=FakeBeetsDB()),
            patch.object(
                audit_cli.msgspec,
                "to_builtins",
                side_effect=RuntimeError("render programmer defect"),
            ),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_world(
                db,
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        self.assertEqual(rc, 5)
        self.assertEqual(
            json.loads(output.getvalue())["error"],
            "world_audit_failed",
        )

    def test_broken_pipe_during_render_exits_cleanly(self) -> None:
        """#1093 review round 4, finding 8 — moving the render inside the
        try (the fix above) made a plain, benign ``BrokenPipeError`` from a
        downstream reader (e.g. ``| head``) closing early get caught by the
        broad ``except Exception``, which then tried to print the error
        JSON and raised a SECOND ``BrokenPipeError`` that nothing caught —
        turning a benign `| head` into a traceback. A dedicated
        ``except BrokenPipeError`` ahead of that generic handler must catch
        it and return without attempting the doomed second write."""
        import scripts.pipeline_cli.audit as audit_cli

        db = FakePipelineDB()
        with (
            patch.object(audit_cli, "_open_beets", return_value=FakeBeetsDB()),
            redirect_stdout(_BrokenPipeStdout()),
        ):
            rc = pipeline_cli.cmd_audit_world(
                db,
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        self.assertEqual(rc, 0)

    def test_parser_exposes_nested_audit_world_command(self) -> None:
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        args = parser.parse_args(["audit", "world", "--json"])

        self.assertEqual(args.command, "audit")
        self.assertEqual(args.audit_command, "world")
        self.assertTrue(args.json)


class TestRetagDivergenceAuditCLI(unittest.TestCase):
    """`pipeline-cli audit retag-divergence` (#1093 item 1)."""

    def test_clean_report_exits_zero(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        beets = FakeBeetsDB()
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=beets),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(payload["status"], "clean")
        self.assertEqual(payload["albums"], [])

    def test_incomplete_from_unreadable_file_exits_four(self) -> None:
        """Drives the REAL default leaf reader — the seeded path does not
        exist, so it fails closed to ``unreadable`` (never ``agrees``). An
        unreadable-only finding is ``incomplete``, never a genuine
        divergence — but ``incomplete`` is still NOT success: the world
        blocked a complete answer, so it exits 4 (`.claude/rules/
        code-quality.md` § CLI ⇄ API Surface Symmetry's "wrong state"
        slot), not 0 (#1093 review round 4, finding 5 — an earlier version
        of this test asserted exit 0 with the exact "a cron reads a silent
        0 as clean" argument quoted against itself; that reasoning applies
        to ``incomplete`` just as much as to ``beets_unavailable``)."""
        import scripts.pipeline_cli.audit as audit_cli
        from lib.beets_db import BeetsAlbumIdentityRow

        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=1,
                mb_albumid="7aabf975-9a06-4b2e-854c-2c700380ebd5",
                item_paths=("/nonexistent/library/Album/01.flac",),
            ),
        ])
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=beets),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 4)
        self.assertEqual(payload["status"], "incomplete")
        self.assertEqual(len(payload["albums"]), 1)
        self.assertEqual(payload["albums"][0]["album_class"], "unreadable")

    def test_divergence_found_exits_one(self) -> None:
        """A genuine identity mismatch, driven through the REAL default
        leaf reader over a real, taggable file (mirrors
        ``TestRealRetagDivergenceScan``)."""
        from pathlib import Path

        from mediafile import MediaFile

        import scripts.pipeline_cli.audit as audit_cli
        from lib.beets_db import BeetsAlbumIdentityRow
        from tests.test_beets_retag import MERGED, SURVIVOR, _make_real_mp3

        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = Path(tmpdir) / "01.mp3"
            _make_real_mp3(track_path)
            media = MediaFile(track_path)
            media.mb_albumid = MERGED
            media.save()

            beets = FakeBeetsDB()
            beets.set_album_mb_identities([
                BeetsAlbumIdentityRow(
                    album_id=1, mb_albumid=SURVIVOR,
                    item_paths=(str(track_path),),
                ),
            ])
            output = io.StringIO()
            with (
                patch.object(audit_cli, "_open_beets", return_value=beets),
                redirect_stdout(output),
            ):
                rc = pipeline_cli.cmd_audit_retag_divergence(
                    FakePipelineDB(),
                    argparse.Namespace(
                        beets_db="unused.db",
                        beets_directory="/unused/library",
                        json=True,
                    ),
                )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 1)
        self.assertEqual(payload["status"], "divergence_found")
        self.assertEqual(len(payload["albums"]), 1)
        self.assertEqual(payload["albums"][0]["album_class"], "diverges")

    def test_expected_beets_unavailability_exits_five(self) -> None:
        """#1093 review round 3, finding 1 — the audit never actually ran,
        so exit 0 would let a cron read "no divergence" from a report that
        answered nothing. `database is locked` is exactly the transient/
        retryable class `.claude/rules/code-quality.md` maps to 5/503."""
        import scripts.pipeline_cli.audit as audit_cli

        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", side_effect=failure),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 5)
        self.assertEqual(payload["status"], "beets_unavailable")
        self.assertFalse(payload["complete"])

    def test_unexpected_failure_remains_exit_five(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        output = io.StringIO()
        with (
            patch.object(
                audit_cli,
                "_open_beets",
                side_effect=RuntimeError("programmer defect"),
            ),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        self.assertEqual(rc, 5)
        self.assertEqual(
            json.loads(output.getvalue())["error"],
            "retag_divergence_audit_failed",
        )

    def test_render_failure_after_a_successful_scan_still_exits_five(
        self,
    ) -> None:
        """#1093 review finding 5 — the previous shape called
        ``msgspec.convert``/the render+encode steps OUTSIDE the try, so a
        defect there tracebacked out uncaught (Python's default exit 1),
        not the documented exit 5. Fail the RENDER step specifically,
        after ``report`` is already computed, to prove the fix covers the
        whole body, not just the scan call."""
        import scripts.pipeline_cli.audit as audit_cli

        beets = FakeBeetsDB()
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=beets),
            patch.object(
                audit_cli.msgspec,
                "to_builtins",
                side_effect=RuntimeError("render programmer defect"),
            ),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        self.assertEqual(rc, 5)
        self.assertEqual(
            json.loads(output.getvalue())["error"],
            "retag_divergence_audit_failed",
        )

    def test_broken_pipe_during_render_exits_cleanly(self) -> None:
        """#1093 review round 4, finding 8 — same fix as
        ``TestWorldAuditCLI``'s own test: a downstream reader (e.g.
        ``| head``) closing early must not double-fault through the
        error-JSON print."""
        import scripts.pipeline_cli.audit as audit_cli

        with (
            patch.object(audit_cli, "_open_beets", return_value=FakeBeetsDB()),
            redirect_stdout(_BrokenPipeStdout()),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                ),
            )

        self.assertEqual(rc, 0)

    def test_after_album_id_is_forwarded_to_the_scan(self) -> None:
        """#1093 review round 4, finding 4 — the CLI's ``--after-album-id``
        must reach the service, not just parse."""
        import scripts.pipeline_cli.audit as audit_cli
        from lib.beets_db import BeetsAlbumIdentityRow

        release_id = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=1, mb_albumid=release_id, item_paths=("/a/01.mp3",),
            ),
            BeetsAlbumIdentityRow(
                album_id=2, mb_albumid=release_id, item_paths=("/b/01.mp3",),
            ),
        ])
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=beets),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                    after_album_id=1,
                ),
            )

        payload = json.loads(output.getvalue())
        # Album 1 is filtered out by after_album_id=1 — only album 2 (a
        # nonexistent path, so it fails closed to unreadable/incomplete;
        # the leaf-reader outcome is not this test's point) is scanned.
        self.assertEqual(rc, 4)
        self.assertEqual(payload["status"], "incomplete")
        self.assertEqual(payload["counts"]["albums_scanned"], 1)

    def test_parser_exposes_nested_audit_retag_divergence_command(self) -> None:
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        args = parser.parse_args([
            "audit", "retag-divergence", "--json", "--after-album-id", "7",
        ])

        self.assertEqual(args.command, "audit")
        self.assertEqual(args.audit_command, "retag-divergence")
        self.assertTrue(args.json)
        self.assertEqual(args.after_album_id, 7)

    def test_after_album_id_rejects_the_strict_grammar_at_the_parser(
        self,
    ) -> None:
        """#1093 review round 6, finding 1 — the CLI half of the strict
        cursor grammar (round 5, finding 5) was unpinned: reverting
        ``--after-album-id``'s ``type=`` back to bare ``int`` survived
        every existing suite, because nothing drove a malformed value
        through the actual parser. ``"1_0"`` is the exact reproduction —
        bare ``int()`` silently resolves it to ``10`` via underscore
        digit-grouping; the strict grammar
        (``lib.retag_divergence_audit.parse_after_album_id_cursor``) must
        reject it at argparse's own boundary, matching the API's 400 on
        the identical input (`tests/web/test_routes_retag_divergence_audit.py
        ::test_after_album_id_underscore_grouping_is_a_400_not_silently_reinterpreted`)."""
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args([
                "audit", "retag-divergence", "--after-album-id", "1_0",
            ])


class TestRetagDivergenceAuditAlbumCLI(unittest.TestCase):
    """`pipeline-cli audit retag-divergence-album <id>` (#1142) — the CLI
    counterpart of `GET /api/audit/retag-divergence/album/<id>`, wrapping
    the exact same `scan_retag_divergence_single_album_from_factory`
    service call (CLI ⇄ API surface symmetry)."""

    def test_found_album_exits_zero(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli
        from lib.beets_db import BeetsAlbumIdentityRow

        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(album_id=1, mb_albumid="", item_paths=()),
        ])
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=beets),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence_album(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                    album_id=1,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(payload["status"], "found")
        self.assertEqual(payload["album"]["album_id"], 1)
        self.assertEqual(payload["album"]["album_class"], "empty")

    def test_diverging_album_still_exits_zero(self) -> None:
        """Unlike the whole-library command, exit reflects whether the
        check ANSWERED — not whether the answer was a divergence — since
        this is an explicit interactive lookup, not a health-check gate.
        Mirrors the route's own 200-for-any-found-class contract."""
        from pathlib import Path

        from mediafile import MediaFile

        import scripts.pipeline_cli.audit as audit_cli
        from lib.beets_db import BeetsAlbumIdentityRow
        from tests.test_beets_retag import MERGED, SURVIVOR, _make_real_mp3

        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = Path(tmpdir) / "01.mp3"
            _make_real_mp3(track_path)
            media = MediaFile(track_path)
            media.mb_albumid = MERGED
            media.save()

            beets = FakeBeetsDB()
            beets.set_album_mb_identities([
                BeetsAlbumIdentityRow(
                    album_id=5, mb_albumid=SURVIVOR,
                    item_paths=(str(track_path),),
                ),
            ])
            output = io.StringIO()
            with (
                patch.object(audit_cli, "_open_beets", return_value=beets),
                redirect_stdout(output),
            ):
                rc = pipeline_cli.cmd_audit_retag_divergence_album(
                    FakePipelineDB(),
                    argparse.Namespace(
                        beets_db="unused.db",
                        beets_directory="/unused/library",
                        json=True,
                        album_id=5,
                    ),
                )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(payload["album"]["album_class"], "diverges")

    def test_not_found_exits_two(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        beets = FakeBeetsDB()
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", return_value=beets),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence_album(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                    album_id=999,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 2)
        self.assertEqual(payload["status"], "not_found")

    def test_expected_beets_unavailability_exits_five(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        output = io.StringIO()
        with (
            patch.object(audit_cli, "_open_beets", side_effect=failure),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence_album(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                    album_id=1,
                ),
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(rc, 5)
        self.assertEqual(payload["status"], "beets_unavailable")

    def test_unexpected_failure_exits_five(self) -> None:
        import scripts.pipeline_cli.audit as audit_cli

        output = io.StringIO()
        with (
            patch.object(
                audit_cli,
                "_open_beets",
                side_effect=RuntimeError("programmer defect"),
            ),
            redirect_stdout(output),
        ):
            rc = pipeline_cli.cmd_audit_retag_divergence_album(
                FakePipelineDB(),
                argparse.Namespace(
                    beets_db="unused.db",
                    beets_directory="/unused/library",
                    json=True,
                    album_id=1,
                ),
            )

        self.assertEqual(rc, 5)
        self.assertEqual(
            json.loads(output.getvalue())["error"],
            "retag_divergence_album_check_failed",
        )

    def test_parser_exposes_nested_audit_retag_divergence_album_command(
        self,
    ) -> None:
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        args = parser.parse_args([
            "audit", "retag-divergence-album", "42", "--json",
        ])

        self.assertEqual(args.command, "audit")
        self.assertEqual(args.audit_command, "retag-divergence-album")
        self.assertEqual(args.album_id, 42)
        self.assertTrue(args.json)

    def test_oversized_album_id_is_rejected_at_the_parser(self) -> None:
        """N10 (#1142 review) — same rejection as the HTTP route's 400,
        at the CLI's own input boundary (CLI ⇄ API surface symmetry):
        an id past SQLite's signed-64-bit INTEGER range can never be
        bound as a query parameter, so it must never even reach Beets."""
        from scripts.pipeline_cli.routes_meta import _build_parser

        parser, _, _ = _build_parser()
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args([
                "audit", "retag-divergence-album",
                "99999999999999999999999999999",
            ])


class TestRealBrokenPipeHandling(unittest.TestCase):
    """#1093 review round 5, finding 4 — the synthetic ``_BrokenPipeStdout``
    fake above raises on EVERY write, which no real pipe does
    (test-fidelity.md Rule C: the fake's trigger must be something a real
    producer can actually produce). Empirically (this class's own
    reproduction), stdout to a pipe is block-buffered enough that a single
    ``print()`` of a modest JSON payload does NOT itself raise, even once
    the reader has already closed with nothing read at all — the write
    only surfaces as ``BrokenPipeError`` later, at Python's own automatic
    interpreter-shutdown flush, OUTSIDE any ``except`` clause in this
    module, printing "Exception ignored while flushing sys.stdout" and
    exiting the whole process 120 regardless of any ``sys.exit(rc)``
    already requested. A downstream reader that closes before consuming
    anything (e.g. a consumer that crashes immediately, or ``| true``) is
    exactly this shape. This class spawns a REAL child process, has it
    print a real report to a REAL OS pipe, and closes the read end
    without reading any bytes — checking the CHILD's own exit code."""

    def _run_with_reader_that_closes_immediately(
        self, child_script: str,
    ) -> int:
        import subprocess
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[1]
        child = subprocess.Popen(
            [sys.executable, "-c", child_script],
            stdout=subprocess.PIPE,
            cwd=repo_root,
        )
        assert child.stdout is not None
        # Read NOTHING, then close — the exact shape that forces the
        # write to stay buffered inside Python (rather than reaching the
        # OS during `print()` itself) and defers the failure to shutdown.
        child.stdout.read(0)
        child.stdout.close()
        child.wait(timeout=15)
        return child.returncode

    def test_retag_divergence_broken_pipe_through_a_real_pipe_exits_cleanly(
        self,
    ) -> None:
        """``cmd_audit_world`` shares the exact same
        ``_handle_broken_pipe_and_exit_cleanly`` handler and the same
        ``sys.stdout.flush()``-before-``except BrokenPipeError`` shape
        (round 4, finding 8; round 5, finding 4) — its own coverage is the
        synthetic-fake test in ``TestWorldAuditCLI`` above (proves the
        catch-and-return-0 wiring) plus this real-pipe proof for the one
        shared handler, reached through its sibling caller: both callers
        route through the same function, so a real-pipe proof at either
        call site is evidence for both."""
        script = """
import sys
from lib.beets_db import BeetsAlbumIdentityRow
from tests.fakes import FakeBeetsDB, FakePipelineDB
from scripts.pipeline_cli.audit import cmd_audit_retag_divergence
import argparse
from unittest.mock import patch
import scripts.pipeline_cli.audit as audit_cli

beets = FakeBeetsDB()
beets.set_album_mb_identities([
    BeetsAlbumIdentityRow(
        album_id=i, mb_albumid="7aabf975-9a06-4b2e-854c-2c700380ebd5",
        item_paths=(f"/nonexistent/album-{i}/01.mp3",),
    )
    for i in range(1, 3)
])
with patch.object(audit_cli, "_open_beets", return_value=beets):
    rc = cmd_audit_retag_divergence(
        FakePipelineDB(),
        argparse.Namespace(
            beets_db="unused.db", beets_directory="/unused/library",
            json=True, after_album_id=None,
        ),
    )
sys.exit(rc)
"""
        returncode = self._run_with_reader_that_closes_immediately(script)
        self.assertEqual(returncode, 0)


if __name__ == "__main__":
    unittest.main()
