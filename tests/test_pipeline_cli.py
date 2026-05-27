"""Tests for scripts/pipeline_cli.py — Pipeline CLI commands."""

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import msgspec

# Bootstrap ephemeral PostgreSQL if available
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from typing import Any, cast

from scripts import pipeline_cli
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

TEST_DSN = os.environ.get("TEST_DB_DSN")

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
    for table in ["import_jobs", "source_denylist", "download_log", "album_tracks", "album_requests"]:
        db._execute(f"TRUNCATE {table} CASCADE")
    db.conn.commit()
    return db


class TestCmdAdd(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    @patch("web.mb.get_release", return_value={
        "release_group_id": "rg-uuid", "tracks": [], "labels": [],
    })
    @patch("web.mb.get_release_group_year", return_value=2014)
    @patch("scripts.pipeline_cli.fetch_mb_release")
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
    @patch("scripts.pipeline_cli.fetch_mb_release")
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

    @patch("scripts.pipeline_cli.fetch_mb_release")
    def test_add_duplicate_skipped(self, mock_fetch):
        self.db.add_request(
            mb_release_id="44438bf9-26d9-4460-9b4f-1a1b015e37a1",
            artist_name="A", album_title="B", source="request",
        )
        args = MagicMock(mbid="44438bf9-26d9-4460-9b4f-1a1b015e37a1", source="request")
        pipeline_cli.cmd_add(self.db, args)
        mock_fetch.assert_not_called()

    @patch("web.mb.get_release")
    @patch("web.mb.get_release_group_year")
    @patch("scripts.pipeline_cli.fetch_mb_release")
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
    @patch("scripts.pipeline_cli.fetch_mb_release")
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
    @patch("scripts.pipeline_cli.fetch_mb_release")
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
    @patch("scripts.pipeline_cli.fetch_mb_release")
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
    @patch("scripts.pipeline_cli.fetch_mb_release")
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

    @patch("scripts.pipeline_cli.fetch_mb_release")
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


class TestCmdList(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_list_by_status(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        id2 = self.db.add_request(mb_release_id="b", artist_name="C", album_title="D", source="request")
        self.db.update_status(id2, "imported")

        args = MagicMock(filter_status="wanted")
        pipeline_cli.cmd_list(self.db, args)

    def test_list_all(self):
        self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        args = MagicMock(filter_status=None)
        pipeline_cli.cmd_list(self.db, args)


class TestCmdRetry(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_retry_resets_to_wanted(self):
        req_id = self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        self.db.update_status(req_id, "imported")
        args = MagicMock(id=req_id)
        pipeline_cli.cmd_retry(self.db, args)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "wanted")


class TestCmdCancel(unittest.TestCase):
    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_cancel_sets_manual(self):
        req_id = self.db.add_request(mb_release_id="a", artist_name="A", album_title="B", source="request")
        args = MagicMock(id=req_id)
        pipeline_cli.cmd_cancel(self.db, args)
        req = self.db.get_request(req_id)
        assert req is not None
        self.assertEqual(req["status"], "manual")


class TestCmdSet(unittest.TestCase):
    @patch("builtins.print")
    @patch("scripts.pipeline_cli.finalize_request")
    def test_set_routes_dynamic_status_through_shared_finalizer(
        self,
        mock_finalize,
        _mock_print,
    ):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=9,
            status="manual",
            artist_name="A",
            album_title="B",
        ))

        args = MagicMock(id=9, status="imported")
        pipeline_cli.cmd_set(cast(Any, db), args)

        called_db, request_id, transition = mock_finalize.call_args.args
        self.assertIs(called_db, db)
        self.assertEqual(request_id, 9)
        self.assertEqual(transition.target_status, "imported")
        self.assertEqual(transition.from_status, "manual")


class TestTracksFromMbRelease(unittest.TestCase):
    def test_extract_tracks(self):
        tracks = pipeline_cli.tracks_from_mb_release(SAMPLE_MB_RELEASE)
        self.assertEqual(len(tracks), 3)
        self.assertEqual(tracks[0]["title"], "Houdini Crush")
        self.assertEqual(tracks[0]["disc_number"], 1)
        self.assertAlmostEqual(tracks[0]["length_seconds"], 200.0)


class TestCmdForceImport(unittest.TestCase):
    @patch("builtins.print")
    @patch("scripts.pipeline_cli._resolve_failed_path", return_value="/tmp/Test Album")
    def test_force_import_passes_source_username_to_queue(self, _mock_resolve, _mock_print):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_dedupe_key

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        ))
        # Seed a download_log entry that ``get_download_log_entry`` will
        # retrieve.  ``cmd_force_import`` reads it for the failed_path
        # and soulseek_username.  Production stores validation_result as
        # a JSONB dict — pass a dict, not the typed Struct, so the
        # downstream ``json.loads`` branch isn't tripped.
        db.log_download(
            request_id=123,
            soulseek_username="baduser",
            outcome="rejected",
            validation_result={"failed_path": "/tmp/Test Album"},
        )
        log_id = db.download_logs[0].id

        args = MagicMock(download_log_id=log_id)
        pipeline_cli.cmd_force_import(cast(Any, db), args)

        # Exactly one import job was enqueued. Inspect the persisted row.
        self.assertEqual(len(db._import_jobs), 1)
        job_row = db._import_jobs[0]
        self.assertEqual(job_row["job_type"], IMPORT_JOB_FORCE)
        self.assertEqual(job_row["request_id"], 123)
        self.assertEqual(job_row["dedupe_key"], force_import_dedupe_key(log_id))
        self.assertEqual(job_row["payload"]["failed_path"], "/tmp/Test Album")
        self.assertEqual(job_row["payload"]["source_username"], "baduser")


class TestCmdManualImport(unittest.TestCase):
    @patch("builtins.print")
    @patch("scripts.pipeline_cli._resolve_failed_path", return_value="/tmp/Album")
    def test_manual_import_prints_queued_job(self, _mock_resolve, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        ))

        args = MagicMock(id=123, path="/tmp/Album")
        pipeline_cli.cmd_manual_import(cast(Any, db), args)

        # FakePipelineDB assigns its own job ID; assert against the
        # actual one rather than the previous MagicMock placeholder.
        self.assertEqual(len(db._import_jobs), 1)
        job_id = db._import_jobs[0]["id"]
        _mock_print.assert_any_call(f"  [OK] Queued import job #{job_id} (queued).")

    @patch("builtins.print")
    @patch("scripts.pipeline_cli._resolve_failed_path", return_value="/tmp/Album")
    def test_manual_import_enqueues_job(self, _mock_resolve, _mock_print):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_dedupe_key

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        ))

        args = MagicMock(id=123, path="/tmp/Album")
        pipeline_cli.cmd_manual_import(cast(Any, db), args)

        # Exactly one import job was enqueued; assert on the persisted row.
        self.assertEqual(len(db._import_jobs), 1)
        job = db._import_jobs[0]
        self.assertEqual(job["job_type"], IMPORT_JOB_MANUAL)
        self.assertEqual(job["request_id"], 123)
        self.assertEqual(job["dedupe_key"], manual_import_dedupe_key(123, "/tmp/Album"))
        self.assertEqual(job["payload"]["failed_path"], "/tmp/Album")

    @patch("builtins.print")
    @patch("scripts.pipeline_cli._resolve_failed_path",
           return_value="/mnt/virtio/music/slskd/failed_imports/Foo - Bar")
    def test_manual_import_resolves_relative_path(self, _mock_resolve, _mock_print):
        """Manual-import must resolve relative paths the same way force-import
        does, so a user can type ``failed_imports/Foo - Bar`` without
        pre-absolutizing. Matches cmd_force_import behavior.
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        ))
        args = MagicMock(id=123, path="failed_imports/Foo - Bar")
        pipeline_cli.cmd_manual_import(cast(Any, db), args)

        _mock_resolve.assert_called_once_with("failed_imports/Foo - Bar")
        self.assertEqual(len(db._import_jobs), 1)
        self.assertEqual(
            db._import_jobs[0]["payload"]["failed_path"],
            "/mnt/virtio/music/slskd/failed_imports/Foo - Bar",
        )

    @patch("builtins.print")
    @patch("scripts.pipeline_cli._resolve_failed_path", return_value=None)
    def test_manual_import_aborts_when_path_cannot_be_resolved(
        self, _mock_resolve, mock_print
    ):
        """When the path can't be resolved, abort without calling dispatch."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=123, status="manual", min_bitrate=320,
            mb_release_id="mbid-123", artist_name="Artist", album_title="Album",
        ))
        args = MagicMock(id=123, path="nonexistent/path")
        pipeline_cli.cmd_manual_import(cast(Any, db), args)
        self.assertEqual(db._import_jobs, [])
        mock_print.assert_any_call("  Files not found at: nonexistent/path")


class TestCmdImportPreview(unittest.TestCase):
    def test_values_json_outputs_common_preview_json(self):
        """Values-mode JSON output round-trips a real preview verdict.

        Drives the real ``preview_import_from_values`` (no stub) with a
        FLAC scenario that classifies as ``would_import``. The pure
        decision's own coverage lives in ``tests/test_import_preview.py``;
        this CLI test just verifies the wire shape of the JSON output.
        """
        db = FakePipelineDB()
        args = SimpleNamespace(
            download_log_id=None,
            request_id=None,
            path=None,
            source_username=None,
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
        args = SimpleNamespace(
            download_log_id=None,
            request_id=None,
            path=None,
            source_username=None,
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
        args = SimpleNamespace(
            download_log_id=None,
            request_id=None,
            path=None,
            source_username=None,
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
        args = SimpleNamespace(
            download_log_id=None,
            request_id=None,
            path=None,
            source_username=None,
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

    def test_download_log_mode_delegates_to_preview_service(self):
        from lib.import_preview import ImportPreviewResult

        db = FakePipelineDB()
        args = SimpleNamespace(
            download_log_id=99,
            request_id=None,
            path=None,
            source_username=None,
            no_force=False,
            values=False,
            values_json=None,
            json=False,
        )
        stdout = io.StringIO()
        with patch(
            "lib.import_preview.preview_import_from_download_log",
            return_value=ImportPreviewResult(
                mode="download_log",
                verdict="confident_reject",
                decision="downgrade",
                confident_reject=True,
                cleanup_eligible=True,
            ),
        ) as mock_preview, redirect_stdout(stdout):
            rc = pipeline_cli.cmd_import_preview(db, args)

        self.assertEqual(rc, 0)
        mock_preview.assert_called_once_with(db, 99)
        self.assertIn("cleanup_eligible: yes", stdout.getvalue())


class TestCmdWrongMatchTriage(unittest.TestCase):
    def test_triage_requires_apply(self):
        db = FakePipelineDB()
        args = SimpleNamespace(apply=False, json=False)
        stderr = io.StringIO()
        with patch(
            "lib.wrong_match_cleanup_service.cleanup_all_wrong_matches"
        ) as cleanup, redirect_stderr(stderr):
            rc = pipeline_cli.cmd_wrong_match_triage(db, args)

        self.assertEqual(rc, 2)
        cleanup.assert_not_called()
        self.assertIn("--apply", stderr.getvalue())

    def test_triage_rejects_scope_flags(self):
        db = FakePipelineDB()
        args = SimpleNamespace(
            download_log_id=99,
            apply=True,
            json=False,
        )
        stderr = io.StringIO()
        with patch(
            "lib.wrong_match_cleanup_service.cleanup_all_wrong_matches"
        ) as cleanup, redirect_stderr(stderr):
            rc = pipeline_cli.cmd_wrong_match_triage(db, args)

        self.assertEqual(rc, 2)
        cleanup.assert_not_called()
        self.assertIn("whole Wrong Matches queue", stderr.getvalue())
        self.assertIn("--download-log-id", stderr.getvalue())

    def test_triage_apply_delegates_to_full_queue_service(self):
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        db = FakePipelineDB()
        args = SimpleNamespace(apply=True, json=True)
        summary = WrongMatchCleanupSummary(processed=1, deleted=1)
        stdout = io.StringIO()
        with patch(
            "lib.wrong_match_cleanup_service.cleanup_all_wrong_matches",
            return_value=summary,
        ) as cleanup, redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_triage(db, args)

        self.assertEqual(rc, 0)
        cleanup.assert_called_once_with(db, confirm_all_wrong_matches=True)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["processed"], 1)
        self.assertEqual(payload["deleted"], 1)


class TestCmdWrongMatchDelete(unittest.TestCase):
    def test_delete_requires_apply(self):
        db = FakePipelineDB()
        args = SimpleNamespace(download_log_id=42, apply=False, json=False)
        stderr = io.StringIO()
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match"
        ) as delete, redirect_stderr(stderr):
            rc = pipeline_cli.cmd_wrong_match_delete(db, args)

        self.assertEqual(rc, 2)
        delete.assert_not_called()
        self.assertIn("--apply", stderr.getvalue())

    def test_delete_apply_delegates_to_service(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_DELETED,
            WrongMatchDeleteResult,
        )

        db = FakePipelineDB()
        args = SimpleNamespace(download_log_id=42, apply=True, json=True)
        result = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_DELETED,
            success=True,
            cleared_rows=1,
            deleted_path="/fi/a",
        )
        stdout = io.StringIO()
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match",
            return_value=result,
        ) as delete, redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_delete(db, args)

        self.assertEqual(rc, 0)
        delete.assert_called_once_with(db, 42, require_visible=True)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["outcome"], OUTCOME_DELETED)
        self.assertEqual(payload["cleared_rows"], 1)

    def test_delete_active_job_returns_conflict_exit_code(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            WrongMatchDeleteResult,
        )

        db = FakePipelineDB()
        args = SimpleNamespace(download_log_id=42, apply=True, json=True)
        result = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_SKIPPED_ACTIVE_JOB,
            skipped=True,
            reason="active_import_job",
        )
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match",
            return_value=result,
        ), redirect_stdout(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_delete(db, args)

        self.assertEqual(rc, 4)

    def test_delete_missing_row_returns_not_found_exit_code(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_NOT_VISIBLE,
            WrongMatchDeleteResult,
        )

        db = FakePipelineDB()
        args = SimpleNamespace(download_log_id=42, apply=True, json=True)
        result = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_SKIPPED_NOT_VISIBLE,
            skipped=True,
            reason="wrong_match_not_visible",
        )
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match",
            return_value=result,
        ), redirect_stdout(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_delete(db, args)

        self.assertEqual(rc, 2)

    def test_delete_locked_returns_transient_exit_code(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_LOCKED,
            WrongMatchDeleteResult,
        )

        db = FakePipelineDB()
        args = SimpleNamespace(download_log_id=42, apply=True, json=True)
        result = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_SKIPPED_LOCKED,
            skipped=True,
            reason="cleanup_lock_unavailable",
        )
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match",
            return_value=result,
        ), redirect_stdout(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_delete(db, args)

        self.assertEqual(rc, 5)


class TestCmdWrongMatchDeleteGroup(unittest.TestCase):
    def test_delete_group_requires_apply(self):
        db = FakePipelineDB()
        args = SimpleNamespace(request_id=42, apply=False, json=False)
        stderr = io.StringIO()
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match_group"
        ) as delete, redirect_stderr(stderr):
            rc = pipeline_cli.cmd_wrong_match_delete_group(db, args)

        self.assertEqual(rc, 2)
        delete.assert_not_called()
        self.assertIn("--apply", stderr.getvalue())

    def test_delete_group_apply_delegates_to_service(self):
        from lib.wrong_match_delete_service import WrongMatchDeleteSummary

        db = FakePipelineDB()
        args = SimpleNamespace(request_id=42, apply=True, json=True)
        summary = WrongMatchDeleteSummary(
            request_id=42,
            outcome="deleted",
            success=True,
            processed=2,
            deleted=2,
            deleted_paths=2,
            cleared=2,
            skipped=0,
            errors=0,
            remaining=0,
            group_empty=True,
            results=(),
        )
        stdout = io.StringIO()
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match_group",
            return_value=summary,
        ) as delete, redirect_stdout(stdout):
            rc = pipeline_cli.cmd_wrong_match_delete_group(db, args)

        self.assertEqual(rc, 0)
        delete.assert_called_once_with(db, 42)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["deleted"], 2)
        self.assertEqual(payload["cleared"], 2)

    def test_delete_group_active_job_returns_conflict_exit_code(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            WrongMatchDeleteResult,
            WrongMatchDeleteSummary,
        )

        db = FakePipelineDB()
        args = SimpleNamespace(request_id=42, apply=True, json=True)
        result = WrongMatchDeleteResult(
            download_log_id=100,
            outcome=OUTCOME_SKIPPED_ACTIVE_JOB,
            skipped=True,
            reason="active_import_job",
        )
        summary = WrongMatchDeleteSummary(
            request_id=42,
            outcome="partial",
            success=False,
            processed=1,
            deleted=0,
            deleted_paths=0,
            cleared=0,
            skipped=1,
            errors=0,
            remaining=1,
            group_empty=False,
            results=(result,),
        )
        with patch(
            "lib.wrong_match_delete_service.delete_wrong_match_group",
            return_value=summary,
        ), redirect_stdout(io.StringIO()):
            rc = pipeline_cli.cmd_wrong_match_delete_group(db, args)

        self.assertEqual(rc, 4)


class TestMainExitCodes(unittest.TestCase):
    def test_main_propagates_command_return_code(self):
        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "wrong-match-triage",
        ]
        db = FakePipelineDB()
        with patch.object(sys, "argv", argv), patch(
            "scripts.pipeline_cli.PipelineDB",
            return_value=db,
        ):
            with self.assertRaises(SystemExit) as raised:
                pipeline_cli.main()

        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(db.close_calls, 1)

    def test_main_routes_wrong_match_delete(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_DELETED,
            WrongMatchDeleteResult,
        )

        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "wrong-match-delete",
            "42",
            "--apply",
            "--json",
        ]
        db = FakePipelineDB()
        result = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_DELETED,
            success=True,
            cleared_rows=1,
        )
        with patch.object(sys, "argv", argv), patch(
            "scripts.pipeline_cli.PipelineDB",
            return_value=db,
        ), patch(
            "lib.wrong_match_delete_service.delete_wrong_match",
            return_value=result,
        ) as delete, redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                pipeline_cli.main()

        self.assertEqual(raised.exception.code, 0)
        delete.assert_called_once_with(db, 42, require_visible=True)
        self.assertEqual(db.close_calls, 1)

    def test_main_routes_wrong_match_delete_group(self):
        from lib.wrong_match_delete_service import WrongMatchDeleteSummary

        argv = [
            "pipeline_cli.py",
            "--dsn",
            "postgresql://example/test",
            "wrong-match-delete-group",
            "42",
            "--apply",
            "--json",
        ]
        db = FakePipelineDB()
        summary = WrongMatchDeleteSummary(
            request_id=42,
            outcome="empty",
            success=True,
            processed=0,
            deleted=0,
            deleted_paths=0,
            cleared=0,
            skipped=0,
            errors=0,
            remaining=0,
            group_empty=True,
            results=(),
        )
        with patch.object(sys, "argv", argv), patch(
            "scripts.pipeline_cli.PipelineDB",
            return_value=db,
        ), patch(
            "lib.wrong_match_delete_service.delete_wrong_match_group",
            return_value=summary,
        ) as delete, redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                pipeline_cli.main()

        self.assertEqual(raised.exception.code, 0)
        delete.assert_called_once_with(db, 42)
        self.assertEqual(db.close_calls, 1)


class TestCmdQuery(unittest.TestCase):
    def test_query_renders_table_output_in_read_only_mode(self):
        db = FakePipelineDB()
        query_cur = MagicMock()
        query_cur.description = [("id",), ("artist_name",), ("details",)]
        query_cur.fetchall.return_value = [
            {"id": 7, "artist_name": "Buke and Gase", "details": {"tracks": 3}},
        ]
        db.queue_execute_results(MagicMock(), query_cur, MagicMock())

        args = MagicMock(sql="SELECT id, artist_name, details FROM album_requests", json=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(db, args)

        # Behavior: query succeeds, output is formatted, read-only mode was used
        self.assertIsNone(rc)
        # 3 _execute calls: enable read-only, run query, disable read-only
        self.assertEqual(len(db.execute_calls), 3)
        output = stdout.getvalue()
        self.assertIn("id | artist_name", output)
        self.assertIn('{"tracks": 3}', output)
        self.assertIn("(1 row)", output)

    def test_query_reads_sql_from_stdin_when_dash_is_passed(self):
        db = FakePipelineDB()
        query_cur = MagicMock()
        query_cur.description = [("value",)]
        query_cur.fetchall.return_value = [{"value": 1}]
        db.queue_execute_results(MagicMock(), query_cur, MagicMock())

        args = MagicMock(sql="-", json=False)
        stdout = io.StringIO()
        with patch("sys.stdin", io.StringIO("SELECT 1 AS value")), redirect_stdout(stdout):
            pipeline_cli.cmd_query(db, args)

        # Second _execute call is the query itself; the first/third are
        # read-only session toggles.
        self.assertEqual(db.execute_calls[1][0], "SELECT 1 AS value")
        self.assertIn("value", stdout.getvalue())

    def test_query_can_emit_json(self):
        db = FakePipelineDB()
        query_cur = MagicMock()
        query_cur.description = [("id",), ("status",)]
        query_cur.fetchall.return_value = [{"id": 3, "status": "wanted"}]
        db.queue_execute_results(MagicMock(), query_cur, MagicMock())

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
        # Cleanup call happened (3rd _execute call for read-only reset)
        self.assertEqual(len(db.execute_calls), 3)


class TestCmdQueryIntegration(unittest.TestCase):
    """Integration test: read-only session rejects writes against real DB."""

    def setUp(self):
        self.db = make_db()

    def tearDown(self):
        self.db.close()

    def test_query_rejects_writes(self):
        args = MagicMock(sql="DELETE FROM album_requests", json=False)
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertEqual(rc, 1)
        self.assertIn("read-only", stderr.getvalue().lower())

    def test_query_allows_reads(self):
        args = MagicMock(sql="SELECT count(*) AS n FROM album_requests", json=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertIsNone(rc)
        self.assertIn("n", stdout.getvalue())

    def test_query_accepts_like_patterns_with_percent(self):
        """Issue #97: SQL containing % (e.g. ILIKE '%foo%') must not be
        interpreted as psycopg2 printf-style placeholders."""
        args = MagicMock(
            sql="SELECT id FROM album_requests WHERE artist_name ILIKE '%nonexistent%'",
            json=False,
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_query(self.db, args)
        self.assertIsNone(rc, f"expected success, got stderr={stderr.getvalue()!r}")
        self.assertNotIn("IndexError", stderr.getvalue())


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
        ads = req.get("active_download_state")
        assert ads is not None
        self.assertEqual(ads["filetype"], "flac")
        self.assertEqual(len(ads["files"]), 1)


class TestCmdSetIntent(unittest.TestCase):
    """Tests for cmd_set_intent — lossless-on-disk toggle."""

    @patch("builtins.print")
    def test_set_lossless_on_wanted(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        ))
        args = MagicMock(id=1, intent="lossless")
        pipeline_cli.cmd_set_intent(cast(Any, db), args)
        self.assertEqual(db.update_request_fields_calls, [(1, dict(target_format="lossless"))])

    @patch("builtins.print")
    def test_set_default_clears_target(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        ))
        args = MagicMock(id=1, intent="default")
        pipeline_cli.cmd_set_intent(cast(Any, db), args)
        self.assertEqual(db.update_request_fields_calls, [(1, dict(target_format=None))])

    @patch("builtins.print")
    @patch("scripts.pipeline_cli.finalize_request")
    def test_set_lossless_on_imported_requeues(self, mock_finalize, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=2, status="imported", artist_name="A", album_title="B",
            min_bitrate=245,
        ))
        args = MagicMock(id=2, intent="lossless")
        pipeline_cli.cmd_set_intent(cast(Any, db), args)
        called_db, request_id, transition = mock_finalize.call_args.args
        self.assertIs(called_db, db)
        self.assertEqual(request_id, 2)
        self.assertEqual(transition.target_status, "wanted")
        self.assertEqual(transition.from_status, "imported")
        self.assertEqual(
            transition.fields,
            {"search_filetype_override": "lossless", "min_bitrate": 245},
        )
        self.assertEqual(db.update_request_fields_calls, [(2, dict(target_format="lossless"))])

    @patch("builtins.print")
    def test_set_default_clears_stale_lossless_override(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4, status="wanted", artist_name="A", album_title="B",
            target_format="lossless", search_filetype_override="lossless",
        ))
        args = MagicMock(id=4, intent="default")
        pipeline_cli.cmd_set_intent(cast(Any, db), args)
        self.assertEqual(db.update_request_fields_calls, [(
            4, dict(target_format=None, search_filetype_override=None))])

    @patch("builtins.print")
    def test_set_intent_refuses_downloading(self, _mock_print):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=3, status="downloading", artist_name="A", album_title="B",
        ))
        args = MagicMock(id=3, intent="lossless")
        pipeline_cli.cmd_set_intent(cast(Any, db), args)
        self.assertEqual(db.update_request_fields_calls, [])

    @patch("builtins.print")
    def test_set_intent_not_found(self, _mock_print):
        db = FakePipelineDB()
        # no rows seeded → get_request returns None
        args = MagicMock(id=99, intent="lossless")
        pipeline_cli.cmd_set_intent(cast(Any, db), args)
        self.assertEqual(db.update_request_fields_calls, [])


class TestCmdRepairSpectral(unittest.TestCase):
    """Regression tests for the rank-model repair flow."""

    def test_repair_spectral_reloads_full_request_metadata(self):
        """Lo-fi V0 repair must reload full request metadata, not trust the partial row."""
        from lib.beets_db import AlbumInfo

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
            clear_cur = MagicMock()
            delete_cur = MagicMock()
            delete_cur.fetchall.return_value = []
            finalize_request = MagicMock()

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
            db.queue_execute_results(candidate_cur, clear_cur, delete_cur)

            beets_info = AlbumInfo(
                album_id=1,
                track_count=10,
                min_bitrate_kbps=207,
                avg_bitrate_kbps=207,
                median_bitrate_kbps=207,
                format="MP3",
                is_cbr=False,
                album_path="/Beets/Artist/Album",
            )
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = beets_info

            args = MagicMock(dry_run=False)
            stdout = io.StringIO()
            with patch.dict(os.environ, {"CRATEDIGGER_RUNTIME_CONFIG": cfg_path}), \
                 patch("lib.beets_db.BeetsDB", return_value=mock_beets), \
                 patch("scripts.pipeline_cli.finalize_request", finalize_request), \
                 redirect_stdout(stdout):
                pipeline_cli.cmd_repair_spectral(cast(Any, db), args)

            output = stdout.getvalue()
            self.assertIn("quality_gate_decision → accept", output)
            self.assertIn("→ transitioned to imported", output)
            self.assertEqual(len(db.execute_calls), 3)
            called_db, request_id, transition = finalize_request.call_args.args
            self.assertIs(called_db, db)
            self.assertEqual(request_id, 42)
            self.assertEqual(transition.target_status, "imported")
            self.assertEqual(transition.from_status, "wanted")
            self.assertEqual(transition.fields, {"min_bitrate": 207})
        finally:
            os.unlink(cfg_path)


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

    def _run_quality(self, request_row, *, runtime_target: str | None):
        from lib.quality import QualityRankConfig

        db = FakePipelineDB()
        db.seed_request(request_row)

        beets_info = SimpleNamespace(
            is_cbr=False,
            avg_bitrate_kbps=245,
            median_bitrate_kbps=245,
            format="MP3",
        )

        stdout = io.StringIO()
        with patch("scripts.pipeline_cli._load_runtime_rank_config",
                   return_value=QualityRankConfig.defaults()), \
             patch("scripts.pipeline_cli._load_runtime_verified_lossless_target",
                   return_value=runtime_target or ""), \
             patch("scripts.pipeline_cli._load_beets_album_info",
                   return_value=beets_info), \
             redirect_stdout(stdout):
            pipeline_cli.cmd_quality(cast(Any, db), MagicMock(id=request_row["id"]))

        return stdout.getvalue()

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

    def test_quality_label_matches_gate_after_spectral_clamp(self):
        """AFX Analord 09 regression: displayed rank label must match the gate verdict.

        Reproduces the exact post-deploy scenario: VBR ~245kbps + spectral=160
        likely_transcode. Without the spectral clamp, the displayed label
        showed `rank=EXCELLENT` next to `NEEDS UPGRADE` — self-contradictory.
        After the fix, the displayed rank is the post-clamp rank that the
        gate actually used.
        """
        from lib.quality import QualityRankConfig

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

        beets_info = SimpleNamespace(
            is_cbr=False,
            avg_bitrate_kbps=245,
            median_bitrate_kbps=245,
            format="MP3",
        )

        db = FakePipelineDB()
        db.seed_request(request_row)
        stdout = io.StringIO()
        with patch("scripts.pipeline_cli._load_runtime_rank_config",
                   return_value=QualityRankConfig.defaults()), \
             patch("scripts.pipeline_cli._load_runtime_verified_lossless_target",
                   return_value=""), \
             patch("scripts.pipeline_cli._load_beets_album_info",
                   return_value=beets_info), \
             redirect_stdout(stdout):
            pipeline_cli.cmd_quality(cast(Any, db), MagicMock(id=9))

        output = stdout.getvalue()
        # The gate must say NEEDS UPGRADE (not DONE) — real quality_gate_decision
        # called by cmd_quality classifies the album below EXCELLENT.
        self.assertIn("NEEDS UPGRADE", output)
        # And the displayed rank must agree — post-clamp 160kbps lands ACCEPTABLE.
        # Use parenthesized form to disambiguate from `gate_min_rank=EXCELLENT`
        # in the cfg display line.
        self.assertIn("(rank=ACCEPTABLE)", output)
        self.assertNotIn("(rank=TRANSPARENT)", output)
        self.assertNotIn("(rank=EXCELLENT)", output)


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

    def set_stub_search_history(self, rows: list[dict[str, object]]) -> None:
        self._stub_search_history = list(rows)

    def get_search_history(self, request_id: int) -> list[dict[str, object]]:  # type: ignore[override]
        return [row for row in self._stub_search_history
                if row.get("request_id") == request_id]


class TestCmdShowSearchForensics(unittest.TestCase):
    """Unit cover for U7 forensic surfacing in `pipeline-cli show`.

    No TEST_DB_DSN required — drives ``cmd_show`` against a
    ``_ForensicsDB`` (typed FakePipelineDB subclass) seeded with the
    forensic blob the test author cared about, and verifies the printed
    text contains variant + final_state + manual_reason + the top-3
    candidate table from the JSONB blob.
    """

    def _row(self, **overrides):
        row = make_request_row(**overrides)
        return row

    def _capture(self, db, request_id):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            pipeline_cli.cmd_show(cast(Any, db), SimpleNamespace(id=request_id))
        return stdout.getvalue()

    def test_show_renders_variant_final_state_and_top_3(self):
        db = _ForensicsDB()
        db.seed_request(self._row(
            id=1843, manual_reason=None, status="wanted",
        ))
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

    def test_show_renders_manual_reason_chip(self):
        db = _ForensicsDB()
        db.seed_request(self._row(
            id=2, manual_reason="search_exhausted", status="manual",
        ))
        db.set_stub_search_history([{
            "id": 1, "request_id": 2, "query": "q",
            "result_count": 0, "elapsed_s": 0.1, "outcome": "exhausted",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": None,
            "variant": "exhausted", "final_state": None,
        }])

        out = self._capture(db, 2)

        self.assertIn("manual_reason:  search_exhausted", out)
        self.assertIn("variant:        exhausted", out)

    def test_show_handles_null_candidates_gracefully(self):
        """Historical row with NULL candidates → no crash, no top list."""
        db = _ForensicsDB()
        db.seed_request(self._row(id=3, manual_reason=None))
        db.set_stub_search_history([{
            "id": 1, "request_id": 3, "query": "q",
            "result_count": None, "elapsed_s": None, "outcome": "timeout",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": None, "variant": None, "final_state": None,
        }])

        out = self._capture(db, 3)

        # Pre-U1 / NULL row prints the "no forensic data" sentinel because
        # variant + final_state + manual_reason are all NULL.
        self.assertIn("(no forensic data yet)", out)
        # And the per-row table renders the variant column as a dash.
        self.assertIn("-", out)

    def test_show_handles_empty_candidates_list(self):
        db = _ForensicsDB()
        db.seed_request(self._row(id=4, manual_reason=None))
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
        args = SimpleNamespace(id=rid, json=json_out)
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
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
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
        args = SimpleNamespace(id=rid, json=False, no_stats=False)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_search_plan_show(db, args)
        self.assertEqual(rc, 0)
        out = stdout.getvalue()
        self.assertIn("Stats:", out)
        self.assertIn("cache_attribution_level: cycle_only", out)

    def test_show_suppresses_stats_when_no_stats_flag(self):
        db, rid = self._seed_with_plan()
        args = SimpleNamespace(id=rid, json=False, no_stats=True)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            rc = pipeline_cli.cmd_search_plan_show(db, args)
        self.assertEqual(rc, 0)
        out = stdout.getvalue()
        self.assertNotIn("Stats:", out)

    def test_show_json_contains_stats_block(self):
        db, rid = self._seed_with_plan()
        args = SimpleNamespace(id=rid, json=True, no_stats=False)
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
        args = SimpleNamespace(id=rid, json=True, no_stats=False)
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
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
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
        args = SimpleNamespace(
            id=rid, json=json_out, prepend_artist=prepend)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            with patch("lib.config.read_runtime_config") as mock_cfg:
                from lib.config import CratediggerConfig
                # Build a minimal real config from defaults so the service
                # can read escalation_threshold etc.
                import configparser
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

    def test_regenerate_deterministic_failure_returns_3_preserves_old_plan(self):
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
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
        args = SimpleNamespace(
            id=rid, json=json_out, prepend_artist=prepend)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            with patch("lib.config.read_runtime_config") as mock_cfg:
                from lib.config import CratediggerConfig
                import configparser
                cp = configparser.RawConfigParser()
                cp.read_string("[General]\n")
                mock_cfg.return_value = CratediggerConfig.from_ini(cp)
                rc = pipeline_cli.cmd_search_plan_dry_run(db, args)
        return rc, stdout.getvalue()

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
        args = SimpleNamespace(
            id=rid, json=json_out, window_days=window_days)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            with patch("lib.config.read_runtime_config") as mock_cfg:
                from lib.config import CratediggerConfig
                import configparser
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
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import (
            ConsumedAttemptInput, SearchPlanItemInput,
        )
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from lib.search_plan_service import SearchPlanService
        from lib.config import CratediggerConfig
        import configparser
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
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
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
        args = SimpleNamespace(
            id=rid, to_ordinal=to_ordinal, to_strategy=to_strategy,
            json=json_out,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            with patch("lib.config.read_runtime_config") as mock_cfg:
                from lib.config import CratediggerConfig
                import configparser
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

    def test_advance_json_output_carries_full_payload(self):
        db, rid = self._seed_plan()
        rc, out = self._run(db, rid, to_ordinal=5, json_out=True)
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual(payload["outcome"], "advanced")
        self.assertEqual(payload["new_ordinal"], 5)
        self.assertEqual(payload["new_strategy"], "track_0")
        self.assertEqual(payload["new_query"], "Love Till Tuesday")


class TestCmdReplace(unittest.TestCase):
    """``pipeline-cli replace`` wraps
    ``MbidReplaceService.replace_request_mbid``. Counterpart of the
    API endpoint ``POST /api/pipeline/<id>/replace`` — both must stay
    in sync; see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"."""

    def _run(self, *, mock_outcome, mock_kwargs=None, json_out=False,
             req_id=42, target_mbid="new-mbid"):
        from lib.mbid_replace_service import ReplaceResult

        result = ReplaceResult(
            outcome=mock_outcome,
            request_id=req_id,
            **(mock_kwargs or {}),
        )
        args = SimpleNamespace(
            id=req_id, target_mb_release_id=target_mbid, json=json_out,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            with patch("lib.config.read_runtime_config") as mock_cfg, \
                 patch("lib.mbid_replace_service.MbidReplaceService") as MS:
                from lib.config import CratediggerConfig
                import configparser
                cp = configparser.RawConfigParser()
                cp.read_string("[General]\n")
                mock_cfg.return_value = CratediggerConfig.from_ini(cp)
                MS.return_value.replace_request_mbid.return_value = result
                rc = pipeline_cli.cmd_replace(MagicMock(), args)
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

    def test_exit_5_on_transient(self):
        rc, _ = self._run(mock_outcome="transient")
        self.assertEqual(rc, 5)

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


class TestCmdBeetsDistance(unittest.TestCase):
    """``pipeline-cli beets-distance`` wraps
    ``lib.beets_distance.compute_beets_distance``. Counterpart of
    ``GET /api/beets-distance/<download_log_id>/<mbid>``. Service-layer
    correctness lives in ``tests.test_beets_distance``; here we pin the
    exit-code mapping per the CLI ⇄ API symmetry rule."""

    UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def _run(self, *, outcome, json_out=False, **result_kwargs):
        from lib.beets_distance import BeetsDistanceResult
        result = BeetsDistanceResult(outcome=outcome, **result_kwargs)
        args = SimpleNamespace(
            download_log_id=100, mbid=self.UUID, json=json_out,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            with patch(
                "lib.beets_distance.compute_beets_distance",
                return_value=result,
            ):
                rc = pipeline_cli.cmd_beets_distance(MagicMock(), args)
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


class TestCmdYoutubeAlbum(unittest.TestCase):
    """``pipeline-cli youtube-album`` wraps
    ``lib.youtube_album_service.resolve_youtube_album``. Counterpart of
    ``GET /api/youtube-album`` (U8). Service-layer correctness lives in
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
            ResolvedDistance, ResolvedYoutubeRelease,
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
             refresh: bool = False, json_out: bool = False):
        if result is None:
            assert outcome is not None, "must pass outcome= or result="
            result = self._make_result(outcome=outcome)
        args = SimpleNamespace(
            identifier=self.IDENT, refresh=refresh, json=json_out,
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
                return None

        _FakeSession.close_calls = 0
        self._last_session_cls = _FakeSession

        with redirect_stdout(stdout):
            with patch(
                "scripts.pipeline_cli._build_youtube_client",
                return_value=(object(), _FakeSession()),
            ), patch(
                "scripts.pipeline_cli._RedisYoutubeCache",
                return_value=object(),
            ), patch(
                "scripts.pipeline_cli.resolve_youtube_album",
                return_value=result,
            ) as mock_resolve:
                rc = pipeline_cli.cmd_youtube_album(FakePipelineDB(), args)
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
        from tests.fakes import FakePipelineDB

        class _FakeSession:
            close_calls = 0

            def close(self) -> None:
                type(self).close_calls += 1
                return None

        _FakeSession.close_calls = 0

        args = SimpleNamespace(
            identifier=self.IDENT, refresh=False, json=False,
        )

        def _raising_resolver(*_a, **_kw):
            raise RuntimeError("simulated mid-CLI failure")

        with patch(
            "scripts.pipeline_cli._build_youtube_client",
            return_value=(object(), _FakeSession()),
        ), patch(
            "scripts.pipeline_cli._RedisYoutubeCache",
            return_value=object(),
        ), patch(
            "scripts.pipeline_cli.resolve_youtube_album",
            side_effect=_raising_resolver,
        ):
            with self.assertRaises(RuntimeError):
                pipeline_cli.cmd_youtube_album(FakePipelineDB(), args)

        self.assertEqual(
            _FakeSession.close_calls, 1,
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
        args = SimpleNamespace(
            id=rid,
            limit=limit,
            before_id=before_id,
            json=json_out,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            with patch("lib.config.read_runtime_config") as mock_cfg:
                from lib.config import CratediggerConfig
                import configparser
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
        rc, out = self._run(db, rid, limit=0)
        self.assertEqual(rc, 3)

    def test_history_returns_3_on_negative_before_id(self):
        db, rid = self._seed(n=2)
        rc, out = self._run(db, rid, limit=10, before_id=0)
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

    def _seed_healthy(self, db, rid: int) -> None:
        from tests.helpers import make_request_row
        db.seed_request(make_request_row(
            id=rid, artist_name="Healthy", album_title="Imported Album",
            status="imported", failure_class="resolved",
        ))

    def _seed_unfindable(self, db, rid: int, category: str = "artist_absent") -> None:
        from datetime import datetime, timezone
        from tests.helpers import make_request_row
        now = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
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
        args = SimpleNamespace(id=rid, json=json_out)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            rc = pipeline_cli.cmd_triage_show(db, args)
        return rc, stdout.getvalue(), stderr.getvalue()

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
        rc, out, err = self._run_show(db, 9999)
        self.assertEqual(rc, 2)
        # Human path writes to stderr; the operator running `triage show`
        # should see the error there, not on stdout.
        self.assertIn("9999", err)
        self.assertIn("not found", err.lower())

    def test_show_unknown_id_json_returns_2_with_structured_payload(self):
        db = FakePipelineDB()
        rc, out, err = self._run_show(db, 9999, json_out=True)
        self.assertEqual(rc, 2)
        payload = json.loads(out)
        self.assertEqual(payload["error"], "Not found")
        self.assertEqual(payload["request_id"], 9999)

    def test_show_healthy_request_renders_no_unfindable_signal(self):
        db = FakePipelineDB()
        self._seed_healthy(db, 1)
        rc, out, err = self._run_show(db, 1)
        self.assertEqual(rc, 0)
        self.assertIn("Healthy", out)
        self.assertIn("Imported Album", out)
        self.assertIn("(no signals)", out)

    # --- triage list ----------------------------------------------------

    def _run_list(self, db, *, filter_spec="all", limit=50, after=None,
                  json_out=False):
        args = SimpleNamespace(
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
        rc, out, err = self._run_list(db, filter_spec="data_quality")
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
        rc, out, err = self._run_list(
            db, filter_spec="data_quality:status=unresolved_4xx_client",
        )
        self.assertEqual(rc, 0)
        self.assertIn("DataOnly 4", out)

    def test_list_data_quality_reason_code_filter(self):
        """``data_quality:reason=<code>`` complementary filter on the
        ``reason_code`` column (HTTP code-specific)."""
        db = self._seed_cohort()
        rc, out, err = self._run_list(
            db, filter_spec="data_quality:reason=http_400",
        )
        self.assertEqual(rc, 0)
        self.assertIn("DataOnly 4", out)

    def test_list_invalid_filter_returns_3_and_emits_valid_forms(self):
        db = self._seed_cohort()
        rc, out, err = self._run_list(db, filter_spec="garbage_value")
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
        rc, out, err = self._run_list(db, filter_spec="unfindable")
        self.assertEqual(rc, 0)
        self.assertIn("No results", out)

    def test_list_limit_returns_page_with_next_after_footer(self):
        db = self._seed_cohort()
        # 2 unfindable rows seeded (ids 2 and 3); limit=2 means full page
        # and the footer should print the next --after cursor.
        rc, out, err = self._run_list(
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
        rc, out, err = self._run_list(
            db, filter_spec="unfindable", limit=10,
        )
        self.assertEqual(rc, 0)
        self.assertNotIn("--after=", out)


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
            "scripts.pipeline_cli.PipelineDB", return_value=db,
        ), redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(SystemExit) as raised:
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
        for expected in ("list", "search-plan show", "triage list", "routes"):
            self.assertIn(expected, names_set)

        # Sort invariant — operators consume this as a stable index.
        # Compare the raw list against ``sorted(names_list)`` (not
        # ``sorted(names_set)``) so a duplicate subcommand surfaces as
        # the inequality it is, rather than being silently deduped.
        self.assertEqual(names_list, sorted(names_list))


if __name__ == "__main__":
    unittest.main()
