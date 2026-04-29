"""Tests for lightweight fakes and shared builders."""

import inspect
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import PipelineDB, RequestSpectralStateUpdate
from lib.quality import SpectralContext, SpectralMeasurement, ValidationResult
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import (
    make_download_file,
    make_grab_list_entry,
    make_request_row,
    make_spectral_context,
    make_validation_result,
)


class TestFakePipelineDB(unittest.TestCase):
    def test_record_attempt_updates_retry_metadata(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))

        db.record_attempt(42, "validation")

        row = db.request(42)
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["last_attempt_at"])
        self.assertIsNotNone(row["next_retry_after"])
        self.assertIsNotNone(row["updated_at"])
        self.assertEqual(db.recorded_attempts, [(42, "validation")])

    def test_set_downloading_sets_attempt_timestamps(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))

        result = db.set_downloading(42, '{"enqueued_at":"2026-01-01T00:00:00+00:00"}')

        self.assertTrue(result)
        row = db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertIsNotNone(row["last_attempt_at"])
        self.assertIsNotNone(row["updated_at"])
        self.assertEqual(
            row["active_download_state"],
            '{"enqueued_at":"2026-01-01T00:00:00+00:00"}',
        )
        self.assertEqual(db.status_history, [(42, "downloading")])

    def test_update_download_state_rewrites_json_state(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        db.update_download_state(42, '{"filetype":"flac"}')

        row = db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertEqual(row["active_download_state"], {"filetype": "flac"})
        self.assertEqual(
            db.update_download_state_calls,
            [(42, '{"filetype":"flac"}')],
        )

    def test_update_download_state_current_path_rewrites_nested_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"filetype": "flac", "files": []},
        ))

        db.update_download_state_current_path(42, "/tmp/staged")

        row = db.request(42)
        self.assertEqual(row["active_download_state"]["current_path"], "/tmp/staged")

    def test_update_download_state_current_path_noop_when_not_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="imported",
            active_download_state=None,
        ))

        db.update_download_state_current_path(42, "/tmp/staged")

        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["active_download_state"])

    def test_update_download_state_current_path_noop_when_state_missing(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state=None,
        ))

        db.update_download_state_current_path(42, "/tmp/staged")

        row = db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertIsNone(row["active_download_state"])

    def test_update_spectral_state(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))

        update = RequestSpectralStateUpdate(
            current=SpectralMeasurement(grade="genuine", bitrate_kbps=None),
        )
        db.update_spectral_state(42, update)

        row = db.request(42)
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertIsNone(row["current_spectral_bitrate"])

    def test_update_imported_path_by_release_id_matches_mb_albumid(self):
        """Issue #132 P2 / #133: sibling ``imported_path`` propagation.
        MB-sourced match on ``mb_release_id``."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=17, mb_release_id="mbid-sibling",
            imported_path="/Beets/Old/Path"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="mbid-sibling",
            discogs_albumid="",
            new_path="/Beets/New/Path [2006]",
        )

        self.assertEqual(rows, 1)
        self.assertEqual(
            db.request(17)["imported_path"], "/Beets/New/Path [2006]")

    def test_update_imported_path_by_release_id_matches_discogs(self):
        """Discogs-sourced match on ``discogs_release_id``."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=18, mb_release_id=None,
            discogs_release_id="12856590",
            imported_path="/Beets/Old/Discogs"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="",
            discogs_albumid="12856590",
            new_path="/Beets/New/Discogs [2006]",
        )

        self.assertEqual(rows, 1)
        self.assertEqual(
            db.request(18)["imported_path"], "/Beets/New/Discogs [2006]")

    def test_update_imported_path_by_release_id_untracked_returns_zero(self):
        """No matching request → rowcount=0, no rows touched."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=19, mb_release_id="other-mbid",
            imported_path="/Beets/Other"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="unknown-mbid",
            discogs_albumid="",
            new_path="/Beets/Ignored",
        )

        self.assertEqual(rows, 0)
        self.assertEqual(db.request(19)["imported_path"], "/Beets/Other")

    def test_update_imported_path_discogs_matches_legacy_mb_release_id(self):
        """Codex R2 P2: beets-side ``discogs_albumid`` must match
        pipeline rows that stored the Discogs numeric in
        ``mb_release_id`` (legacy "pipeline compat" layout from
        CLAUDE.md) OR in ``discogs_release_id``."""
        db = FakePipelineDB()
        # Legacy layout: numeric in mb_release_id, discogs_release_id None.
        db.seed_request(make_request_row(
            id=21, mb_release_id="12856590",
            discogs_release_id=None, imported_path="/Beets/Legacy/Old"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="",
            discogs_albumid="12856590",
            new_path="/Beets/Legacy/New",
        )

        self.assertEqual(rows, 1)
        self.assertEqual(
            db.request(21)["imported_path"], "/Beets/Legacy/New")

    def test_update_imported_path_by_release_id_both_empty_is_noop(self):
        """Both release ids empty → rowcount=0, no UPDATE fires at all.
        Mirrors the prod short-circuit that guards against accidentally
        matching every row where a column is NULL/empty."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=20, mb_release_id="some-mbid",
            imported_path="/Beets/Keep"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="", discogs_albumid="", new_path="/Beets/Bogus")

        self.assertEqual(rows, 0)
        self.assertEqual(db.request(20)["imported_path"], "/Beets/Keep")

    def test_clear_on_disk_quality_fields_matches_real_db(self):
        """FakePipelineDB must mirror PipelineDB.clear_on_disk_quality_fields:
        zero the on-disk spectral + verified_lossless + imported_path,
        preserve min_bitrate and last_download_spectral_* (those aren't
        on-disk state).
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            min_bitrate=320,
            verified_lossless=True,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            last_download_spectral_grade="suspect",
            last_download_spectral_bitrate=192,
            imported_path="/mnt/virtio/Music/Beets/Stale/Path",
        ))

        db.clear_on_disk_quality_fields(42)

        row = db.request(42)
        self.assertFalse(row["verified_lossless"])
        self.assertIsNone(row["current_spectral_grade"])
        self.assertIsNone(row["current_spectral_bitrate"])
        self.assertIsNone(row["imported_path"],
                          "imported_path must clear — the web UI renders it "
                          "directly and a stale path after beet rm is worse "
                          "than no path at all.")
        # min_bitrate preserved as baseline for next gate.
        self.assertEqual(row["min_bitrate"], 320)
        # Recent download's spectral is an audit trail, not on-disk state.
        self.assertEqual(row["last_download_spectral_grade"], "suspect")
        self.assertEqual(row["last_download_spectral_bitrate"], 192)

    def test_get_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=3, status="downloading"))

        rows = db.get_downloading()
        self.assertEqual(len(rows), 2)
        ids = {r["id"] for r in rows}
        self.assertEqual(ids, {1, 3})

    def test_list_requests_by_artist_prefers_mb_artist_id_and_legacy_fallback(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="Test Artist",
            album_title="Exact MBID",
            mb_artist_id="artist-1234-uuid",
        ))
        db.seed_request(make_request_row(
            id=2,
            artist_name="Test Artist",
            album_title="Legacy Name Match",
            mb_artist_id=None,
        ))
        db.seed_request(make_request_row(
            id=3,
            artist_name="Test Artist",
            album_title="Other MBID",
            mb_artist_id="other-artist-uuid",
        ))

        rows = db.list_requests_by_artist("Test Artist", "artist-1234-uuid")

        self.assertEqual([row["id"] for row in rows], [1, 2])

    def test_list_requests_by_artist_name_only_matches_substring(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="The National",
            album_title="Boxer",
            year=2007,
        ))
        db.seed_request(make_request_row(
            id=2,
            artist_name="The National",
            album_title="Sleep Well Beast",
            year=2017,
        ))
        db.seed_request(make_request_row(
            id=3,
            artist_name="Nation of Language",
            album_title="Introduction, Presence",
            year=2020,
        ))

        rows = db.list_requests_by_artist("The National")

        self.assertEqual([row["id"] for row in rows], [1, 2])

    def test_assert_log_passes(self):
        db = FakePipelineDB()
        log_id = db.log_download(42, outcome="success", soulseek_username="user1")

        # Should not raise
        self.assertEqual(log_id, db.download_logs[0].id)
        db.assert_log(self, 0, outcome="success", request_id=42)

    def test_assert_log_checks_extra_fields(self):
        db = FakePipelineDB()
        db.log_download(42, outcome="success", spectral_grade="genuine")

        db.assert_log(self, 0, outcome="success")
        # Extra field goes into .extra dict
        self.assertEqual(db.download_logs[0].extra["spectral_grade"], "genuine")

    def test_advisory_lock_default_yields_true(self):
        db = FakePipelineDB()
        with db.advisory_lock(0x1234, 42) as acquired:
            self.assertTrue(acquired)
        self.assertEqual(db.advisory_lock_calls, [(0x1234, 42)])

    def test_advisory_lock_configurable(self):
        db = FakePipelineDB()
        db.set_advisory_lock_result(False)
        with db.advisory_lock(0x1234, 42) as acquired:
            self.assertFalse(acquired)
        self.assertEqual(db.advisory_lock_calls, [(0x1234, 42)])


class TestFakeSlskdAPI(unittest.TestCase):
    def test_get_downloads_returns_queued_snapshots(self):
        first = [{"username": "user1", "directories": [{"files": []}]}]
        second = [{"username": "user1", "directories": [{"files": [
            {"filename": "track.mp3", "id": "tid-1"},
        ]}]}]
        slskd = FakeSlskdAPI(download_snapshots=[first, second])

        self.assertEqual(slskd.transfers.get_all_downloads(includeRemoved=True), first)
        self.assertEqual(slskd.transfers.get_all_downloads(includeRemoved=True), second)
        self.assertEqual(slskd.transfers.get_all_downloads(includeRemoved=True), second)
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True, True, True])

    def test_get_download_matches_username_and_id(self):
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="user1",
            directory="user1\\Music",
            filename="user1\\Music\\01.flac",
            id="tid-1",
            state="Completed, Succeeded",
        )

        transfer = slskd.transfers.get_download("user1", "tid-1")

        self.assertEqual(transfer["filename"], "user1\\Music\\01.flac")
        self.assertEqual(transfer["state"], "Completed, Succeeded")
        self.assertEqual(slskd.transfers.get_download_calls, [("user1", "tid-1")])

    def test_records_enqueue_and_cancel_calls(self):
        slskd = FakeSlskdAPI()
        files = [{"filename": "track.mp3", "size": 1000}]

        self.assertTrue(slskd.transfers.enqueue("user1", files))
        self.assertTrue(slskd.transfers.cancel_download("user1", "tid-1"))

        self.assertEqual(slskd.transfers.enqueue_calls[0].username, "user1")
        self.assertEqual(slskd.transfers.enqueue_calls[0].files, files)
        self.assertEqual(slskd.transfers.cancel_download_calls[0].id, "tid-1")

    def test_user_directories_record_results_and_errors(self):
        slskd = FakeSlskdAPI()
        directory = [{"directory": "Music\\Album", "files": []}]
        slskd.users.set_directory("user1", "Music\\Album", directory)
        slskd.users.set_directory_error(
            "user1",
            "Music\\Broken",
            Exception("Peer offline"),
        )

        self.assertEqual(slskd.users.directory("user1", "Music\\Album"), directory)
        with self.assertRaises(Exception):
            slskd.users.directory("user1", "Music\\Broken")
        self.assertEqual(slskd.users.directory_calls, [
            ("user1", "Music\\Album"),
            ("user1", "Music\\Broken"),
        ])


class TestBuilders(unittest.TestCase):
    def test_make_download_file_defaults(self):
        f = make_download_file()
        self.assertIsInstance(f, DownloadFile)
        self.assertEqual(f.filename, "01 - Track.mp3")
        self.assertEqual(f.username, "user1")
        self.assertEqual(f.size, 5_000_000)

    def test_make_download_file_overrides(self):
        f = make_download_file(username="beta", bitRate=192)
        self.assertEqual(f.username, "beta")
        self.assertEqual(f.bitRate, 192)

    def test_make_grab_list_entry_defaults(self):
        entry = make_grab_list_entry()
        self.assertIsInstance(entry, GrabListEntry)
        self.assertEqual(entry.artist, "Test Artist")
        self.assertEqual(len(entry.files), 1)
        self.assertIsInstance(entry.files[0], DownloadFile)

    def test_make_grab_list_entry_overrides(self):
        files = [make_download_file(username="a"), make_download_file(username="b")]
        entry = make_grab_list_entry(files=files, db_request_id=42, db_source="request")
        self.assertEqual(len(entry.files), 2)
        self.assertEqual(entry.db_request_id, 42)

    def test_make_validation_result_defaults(self):
        vr = make_validation_result()
        self.assertIsInstance(vr, ValidationResult)
        self.assertTrue(vr.valid)
        self.assertEqual(vr.distance, 0.05)
        self.assertEqual(vr.scenario, "strong_match")

    def test_make_validation_result_overrides(self):
        vr = make_validation_result(valid=False, distance=0.5, scenario="bad_match",
                                     failed_path="/tmp/failed")
        self.assertFalse(vr.valid)
        self.assertEqual(vr.distance, 0.5)
        self.assertEqual(vr.failed_path, "/tmp/failed")

    def test_make_spectral_context_defaults(self):
        sc = make_spectral_context()
        self.assertIsInstance(sc, SpectralContext)
        self.assertFalse(sc.needs_check)
        self.assertIsNone(sc.grade)

    def test_make_spectral_context_overrides(self):
        sc = make_spectral_context(needs_check=True, grade="suspect", bitrate=192)
        self.assertTrue(sc.needs_check)
        self.assertEqual(sc.grade, "suspect")
        self.assertEqual(sc.bitrate, 192)


class TestFakePipelineDBDiscogs(unittest.TestCase):
    """Tests for Discogs-related FakePipelineDB methods."""

    def test_get_request_by_mb_release_id_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="abc-uuid"))
        result = db.get_request_by_mb_release_id("abc-uuid")
        assert result is not None
        self.assertEqual(result["id"], 1)

    def test_get_request_by_mb_release_id_not_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="abc-uuid"))
        self.assertIsNone(db.get_request_by_mb_release_id("other"))

    def test_get_request_by_discogs_release_id_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, discogs_release_id="12345"))
        result = db.get_request_by_discogs_release_id("12345")
        assert result is not None
        self.assertEqual(result["id"], 1)

    def test_get_request_by_discogs_release_id_not_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, discogs_release_id="12345"))
        self.assertIsNone(db.get_request_by_discogs_release_id("99999"))

    def test_get_request_by_release_id_normalizes_uppercase_uuid(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
        result = db.get_request_by_release_id("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA")
        assert result is not None
        self.assertEqual(result["id"], 1)

    def test_get_request_by_release_id_falls_back_to_legacy_numeric_mb_column(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            mb_release_id="12856590",
            discogs_release_id=None,
        ))
        result = db.get_request_by_release_id("0012856590")
        assert result is not None
        self.assertEqual(result["id"], 1)


class TestFakePipelineDBNewStubs(unittest.TestCase):
    """Self-tests for fake methods retroactively added under issue #140.

    These cover behaviour that tests relying on the fake may start
    exercising. Matches the rule in ``.claude/rules/code-quality.md``:
    "every new PipelineDB method needs an equivalent stub on
    FakePipelineDB with a self-test in tests/test_fakes.py."
    """

    def test_close_marks_flag(self):
        db = FakePipelineDB()
        self.assertFalse(db.closed)
        db.close()
        self.assertTrue(db.closed)

    def test_import_job_queue_methods_mirror_core_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        first = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:42",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
        )
        duplicate = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:42",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
        )
        self.assertEqual(first.id, duplicate.id)
        self.assertTrue(duplicate.deduped)
        self.assertEqual(db.count_import_jobs_by_status(), {"queued": 1})
        db.mark_import_job_preview_importable(
            first.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )

        claimed = db.claim_next_import_job(worker_id="fake-worker")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.attempts, 1)
        self.assertEqual(claimed.worker_id, "fake-worker")
        self.assertTrue(db.heartbeat_import_job(claimed.id))

        requeued = db.requeue_running_import_jobs(message="retry")
        self.assertEqual([job.id for job in requeued], [claimed.id])
        self.assertEqual(requeued[0].status, "queued")
        self.assertIsNone(requeued[0].worker_id)

        claimed = db.claim_next_import_job(worker_id="fake-worker-2")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.attempts, 2)
        self.assertEqual(claimed.worker_id, "fake-worker-2")

        completed = db.mark_import_job_completed(
            claimed.id,
            result={"success": True},
            message="done",
        )
        assert completed is not None
        self.assertEqual(completed.status, "completed")

        later = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:42",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
        )
        self.assertNotEqual(first.id, later.id)
        failed = db.mark_import_job_failed(
            later.id,
            error="boom",
            message="failed",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")

    def test_import_job_queue_defaults_to_importable_without_preview_gate(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        with patch.dict("os.environ", {}, clear=True):
            queued = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key="manual:preview-disabled",
                payload=manual_import_payload(failed_path="/tmp/manual"),
            )

        self.assertEqual(queued.preview_status, "would_import")
        self.assertEqual(queued.preview_message, "Preview gate disabled")
        self.assertIsNotNone(queued.preview_completed_at)
        self.assertIsNotNone(queued.importable_at)
        self.assertIsNone(db.claim_next_import_preview_job(worker_id="preview"))

        claimed = db.claim_next_import_job(worker_id="fake-worker")
        assert claimed is not None
        self.assertEqual(claimed.id, queued.id)
        self.assertEqual(claimed.status, "running")

    def test_import_job_preview_methods_mirror_core_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        queued = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:preview",
            payload=manual_import_payload(failed_path="/tmp/manual"),
            preview_enabled=True,
        )
        self.assertEqual(queued.preview_status, "waiting")

        claimed = db.claim_next_import_preview_job(worker_id="fake-preview")
        assert claimed is not None
        self.assertEqual(claimed.status, "queued")
        self.assertEqual(claimed.preview_status, "running")
        self.assertEqual(claimed.preview_attempts, 1)
        self.assertEqual(claimed.preview_worker_id, "fake-preview")
        self.assertTrue(db.heartbeat_import_job_preview(claimed.id))

        importable = db.mark_import_job_preview_importable(
            claimed.id,
            preview_result={"verdict": "would_import"},
            message="Preview would import",
        )
        assert importable is not None
        self.assertEqual(importable.preview_status, "would_import")
        self.assertEqual(importable.preview_result, {"verdict": "would_import"})
        self.assertIsNotNone(importable.importable_at)

        rejected = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=43,
            dedupe_key="manual:preview-reject",
            payload=manual_import_payload(failed_path="/tmp/reject"),
            preview_enabled=True,
        )
        failed = db.mark_import_job_preview_failed(
            rejected.id,
            preview_status="confident_reject",
            error="spectral_reject",
            preview_result={
                "verdict": "confident_reject",
                "reason": "spectral_reject",
            },
            message="Preview rejected: spectral_reject",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.preview_status, "confident_reject")
        self.assertEqual(failed.preview_error, "spectral_reject")
        self.assertEqual(failed.error, "spectral_reject")

    def test_add_request_assigns_monotonic_id(self):
        db = FakePipelineDB()
        rid1 = db.add_request("Artist A", "Album A", source="request")
        rid2 = db.add_request("Artist B", "Album B", source="request")
        self.assertEqual((rid1, rid2), (1, 2))
        self.assertEqual(db.request(rid1)["artist_name"], "Artist A")
        self.assertEqual(db.request(rid2)["status"], "wanted")

    def test_add_request_seeds_full_row_shape(self):
        """Codex R7: rows must carry the DB-defaulted columns
        production readers index directly (``beets_distance``,
        ``imported_path``, ``*_attempts``, spectral + verified_lossless)
        so fake-backed tests don't raise ``KeyError`` where Postgres
        would return NULL/0."""
        db = FakePipelineDB()
        rid = db.add_request("X", "Y", source="request")
        row = db.request(rid)
        for key in (
            "beets_distance", "beets_scenario", "imported_path",
            "search_attempts", "download_attempts", "validation_attempts",
            "last_download_spectral_grade", "current_spectral_grade",
            "current_lossless_source_v0_probe_avg_bitrate",
            "verified_lossless", "min_bitrate", "prev_min_bitrate",
            "search_filetype_override", "target_format",
            "active_download_state",
        ):
            self.assertIn(key, row,
                          f"add_request row missing '{key}' — "
                          "production readers index it directly")
        self.assertEqual(row["search_attempts"], 0)
        self.assertEqual(row["download_attempts"], 0)
        self.assertEqual(row["validation_attempts"], 0)
        self.assertFalse(row["verified_lossless"])

    def test_add_request_coexists_with_seeded_ids(self):
        """Seeded ids must advance the auto-increment cursor so
        ``add_request`` cannot collide."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        rid = db.add_request("X", "Y", source="request")
        self.assertEqual(rid, 43)

    def test_sort_mixes_seeded_iso_strings_and_added_datetimes(self):
        """``make_request_row`` seeds ISO strings, ``add_request``
        stores datetimes — the fake must normalise them so sorts
        don't raise ``TypeError`` on mixed input (codex R2)."""
        db = FakePipelineDB()
        # Seeded: ISO string timestamps.
        db.seed_request(make_request_row(id=1, status="wanted"))
        # Added: datetime timestamps.
        db.add_request("Artist", "Album", source="request")
        # Both of these would crash on ``str < datetime`` without
        # normalisation.
        rows = db.get_by_status("wanted")
        self.assertEqual(len(rows), 2)
        # Populate download history for both then ensure ``get_recent``
        # also sorts through the mixed shapes without raising.
        db.log_download(1, outcome="success")
        db.log_download(2, outcome="success")
        recent = db.get_recent()
        self.assertEqual({r["id"] for r in recent}, {1, 2})

    def test_delete_request_removes_row_and_tracks(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.set_tracks(1, [{"track_number": 1, "title": "T"}])
        db.delete_request(1)
        self.assertNotIn(1, db._requests)  # type: ignore[attr-defined]
        self.assertEqual(db.get_tracks(1), [])

    def test_delete_request_cascades_to_child_tables(self):
        """Real SQL has ``ON DELETE CASCADE`` from album_requests to
        download_log, search_log, and source_denylist. The fake must
        prune those too so tests cannot observe an impossible state
        where orphaned child rows survive their parent (codex R2)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.log_download(1, outcome="success")
        db.log_download(2, outcome="success")
        db.log_search(1, outcome="found")
        db.log_search(2, outcome="no_match")
        db.add_denylist(1, "badguy")
        db.add_denylist(2, "other")

        db.delete_request(1)

        self.assertEqual([e.request_id for e in db.download_logs], [2])
        self.assertEqual([e.request_id for e in db.search_logs], [2])
        self.assertEqual([e.request_id for e in db.denylist], [2])

    def test_get_wanted_prioritizes_new_and_respects_limit(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted",
                                          search_attempts=0))
        db.seed_request(make_request_row(id=2, status="wanted",
                                          search_attempts=5))
        db.seed_request(make_request_row(id=3, status="imported"))
        rows = db.get_wanted()
        self.assertEqual([r["id"] for r in rows], [1, 2])
        self.assertEqual(
            [r["id"] for r in db.get_wanted(limit=1)], [1])

    def test_get_wanted_skips_albums_inside_retry_window(self):
        db = FakePipelineDB()
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        db.seed_request(make_request_row(
            id=1, status="wanted", next_retry_after=future))
        db.seed_request(make_request_row(id=2, status="wanted"))
        rows = db.get_wanted()
        self.assertEqual([r["id"] for r in rows], [2])

    def test_get_wanted_tie_break_is_set_not_order(self):
        """Within a priority bucket the real DB randomises order —
        callers must assert on set membership, not list position."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", search_attempts=0))
        db.seed_request(make_request_row(
            id=2, status="wanted", search_attempts=0))
        db.seed_request(make_request_row(
            id=3, status="wanted", search_attempts=0))
        rows = db.get_wanted()
        self.assertEqual({r["id"] for r in rows}, {1, 2, 3})

    def test_get_log_filters_and_orders_newest_first(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, album_title="Album A"))
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="failed")
        db.log_download(1, outcome="rejected")
        all_rows = db.get_log()
        self.assertEqual([r["outcome"] for r in all_rows],
                         ["rejected", "failed", "success"])
        imported = db.get_log(outcome_filter="imported")
        self.assertEqual([r["outcome"] for r in imported], ["success"])
        rejected = db.get_log(outcome_filter="rejected")
        self.assertEqual([r["outcome"] for r in rejected],
                         ["rejected", "failed"])
        # Joined request columns present.
        self.assertEqual(all_rows[0]["album_title"], "Album A")

    def test_get_log_surfaces_auxiliary_columns(self):
        """Real ``get_log`` returns ``dl.*`` — every ``log_download``
        column must be present, including fields parked in
        ``entry.extra`` (bitrate, spectral_grade, final_format, etc.)
        Codex R2: callers that feed these rows into LogEntry.from_row
        would otherwise classify incomplete data (codex R2)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(
            1, outcome="success",
            bitrate=256, spectral_grade="genuine",
            final_format="mp3 v0", actual_min_bitrate=245)
        rows = db.get_log()
        self.assertEqual(rows[0]["bitrate"], 256)
        self.assertEqual(rows[0]["spectral_grade"], "genuine")
        self.assertEqual(rows[0]["final_format"], "mp3 v0")
        self.assertEqual(rows[0]["actual_min_bitrate"], 245)

    def test_get_by_status_sorts_by_created_at(self):
        db = FakePipelineDB()
        now = datetime.now(timezone.utc)
        db.seed_request(make_request_row(
            id=1, status="wanted", created_at=now + timedelta(seconds=2)))
        db.seed_request(make_request_row(
            id=2, status="wanted", created_at=now))
        rows = db.get_by_status("wanted")
        self.assertEqual([r["id"] for r in rows], [2, 1])

    def test_get_recent_requires_download_history(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.log_download(1, outcome="success")
        rows = db.get_recent()
        self.assertEqual([r["id"] for r in rows], [1])

    def test_get_recent_deterministic_with_missing_updated_at(self):
        """Sort key must not call ``_utcnow()`` per comparison —
        multiple rows with no ``updated_at`` must fall into a stable
        insertion order so tests cannot flake."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, updated_at=None))
        db.seed_request(make_request_row(id=2, updated_at=None))
        db.seed_request(make_request_row(id=3, updated_at=None))
        db.log_download(1, outcome="success")
        db.log_download(2, outcome="success")
        db.log_download(3, outcome="success")
        rows = db.get_recent()
        self.assertEqual({r["id"] for r in rows}, {1, 2, 3})

    def test_count_by_status(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=3, status="imported"))
        self.assertEqual(
            db.count_by_status(), {"wanted": 2, "imported": 1})

    def test_count_by_status_preserves_none_bucket(self):
        """Real SQL ``GROUP BY status`` keeps NULL as its own key; the
        fake must not collapse it to an empty string."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status=None))
        db.seed_request(make_request_row(id=2, status="wanted"))
        self.assertEqual(db.count_by_status(), {None: 1, "wanted": 1})

    def test_tracks_round_trip_and_count(self):
        db = FakePipelineDB()
        db.set_tracks(1, [
            {"track_number": 2, "title": "Second"},
            {"track_number": 1, "title": "First"},
        ])
        rows = db.get_tracks(1)
        self.assertEqual([t["track_number"] for t in rows], [1, 2])
        self.assertEqual(db.get_track_counts([1, 99]), {1: 2})

    def test_download_log_history_and_lookup_by_id(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="failed")
        db.log_download(2, outcome="rejected")

        history_1 = db.get_download_history(1)
        self.assertEqual([r["outcome"] for r in history_1],
                         ["failed", "success"])
        batch = db.get_download_history_batch([1, 2])
        self.assertEqual({k: [r["outcome"] for r in v]
                          for k, v in batch.items()},
                         {1: ["failed", "success"], 2: ["rejected"]})

        first_id = db.download_logs[0].id
        entry = db.get_download_log_entry(first_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "success")
        self.assertIsNone(db.get_download_log_entry(99999))

    def test_get_wrong_matches_collapses_per_request_and_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, artist_name="A", album_title="B"))
        # Two rejections on the same (request, failed_path) — keep newest.
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1"})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1"})
        # Different path — separate row.
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p2"})
        # Scenario filtered out.
        db.log_download(1, outcome="rejected", validation_result={
            "failed_path": "/p3", "scenario": "audio_corrupt"})
        # Non-rejected — ignored.
        db.log_download(1, outcome="success",
                        validation_result={"failed_path": "/p4"})

        rows = db.get_wrong_matches()
        paths = sorted([
            (r["validation_result"] or {}).get("failed_path")  # type: ignore[union-attr]
            for r in rows])
        self.assertEqual(paths, ["/p1", "/p2"])

    def test_clear_wrong_match_path_strips_key(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1",
                                           "scenario": "wrong_match"})
        log_id = db.download_logs[0].id
        self.assertTrue(db.clear_wrong_match_path(log_id))
        vr = db.download_logs[0].validation_result
        assert isinstance(vr, dict)
        self.assertNotIn("failed_path", vr)
        self.assertEqual(vr["scenario"], "wrong_match")
        # Second call returns False (already stripped).
        self.assertFalse(db.clear_wrong_match_path(log_id))

    def test_clear_wrong_match_path_handles_json_string(self):
        """Real ``validation_result`` is JSONB — fakes also accept JSON
        strings so tests can pass either shape."""
        import json as _json
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result=_json.dumps(
                            {"failed_path": "/p", "x": 1}))
        self.assertTrue(
            db.clear_wrong_match_path(db.download_logs[0].id))
        stored = _json.loads(db.download_logs[0].validation_result)  # type: ignore[arg-type]
        self.assertNotIn("failed_path", stored)

    def test_clear_wrong_match_paths_clears_matching_request_and_paths(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "failed_imports/A",
                                           "x": 1})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 2})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/abs/B",
                                           "x": 3})
        db.log_download(2, outcome="rejected",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 4})
        db.log_download(1, outcome="success",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 5})

        cleared = db.clear_wrong_match_paths(
            1, ["failed_imports/A", "/abs/A"])

        self.assertEqual(cleared, 2)
        rows = db.get_wrong_matches()
        remaining = {
            (row["request_id"], row["validation_result"]["failed_path"])  # type: ignore[index]
            for row in rows
        }
        self.assertEqual(remaining, {(1, "/abs/B"), (2, "/abs/A")})

    def test_clear_wrong_match_paths_handles_json_string_payloads(self):
        import json as _json
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result=_json.dumps(
                            {"failed_path": "/p", "x": 1}))

        cleared = db.clear_wrong_match_paths(1, ["/p"])

        self.assertEqual(cleared, 1)
        stored = _json.loads(db.download_logs[0].validation_result)  # type: ignore[arg-type]
        self.assertNotIn("failed_path", stored)
        self.assertEqual(stored["x"], 1)

    def test_search_log_history_and_batch(self):
        db = FakePipelineDB()
        db.log_search(1, query="a b", outcome="found", result_count=10,
                      elapsed_s=0.5)
        db.log_search(1, query="c d", outcome="no_match")
        db.log_search(2, query="e f", outcome="error")

        history_1 = db.get_search_history(1)
        self.assertEqual([r["outcome"] for r in history_1],
                         ["no_match", "found"])
        batch = db.get_search_history_batch([1, 2])
        self.assertEqual(
            {k: [r["outcome"] for r in v] for k, v in batch.items()},
            {1: ["no_match", "found"], 2: ["error"]})

    def test_user_cooldowns_upsert_and_filter(self):
        db = FakePipelineDB()
        now = datetime.now(timezone.utc)
        db.add_cooldown("alice", now + timedelta(days=3), reason="x")
        db.add_cooldown("bob", now - timedelta(days=1), reason="expired")
        # Upsert — second call on alice replaces cooldown_until/reason.
        db.add_cooldown("alice", now + timedelta(days=7), reason="y")

        active = db.get_cooled_down_users()
        self.assertEqual(active, ["alice"])

        rows = db.get_user_cooldowns()
        # Newest cooldown_until first.
        self.assertEqual([r["username"] for r in rows], ["alice", "bob"])
        self.assertEqual(rows[0]["reason"], "y")


class TestFakeBadAudioHashes(unittest.TestCase):
    """Self-tests for the bad_audio_hashes fake methods (plan U2)."""

    def _hash(self, n: int) -> bytes:
        return bytes([n]) * 32

    def test_add_bad_audio_hashes_returns_count_for_fresh_inserts(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="m4a"),
        ]
        n = db.add_bad_audio_hashes(42, "H@rco", "bad rip", inputs)
        self.assertEqual(n, 3)
        self.assertEqual(len(db.bad_audio_hashes), 3)
        self.assertEqual(db.bad_audio_hashes[0].request_id, 42)
        self.assertEqual(db.bad_audio_hashes[0].reported_username, "H@rco")
        self.assertEqual(db.bad_audio_hashes[0].reason, "bad rip")
        # Auto-incrementing ids
        ids = [r.id for r in db.bad_audio_hashes]
        self.assertEqual(ids, [1, 2, 3])

    def test_add_bad_audio_hashes_returns_zero_on_full_duplicate(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
        ]
        first = db.add_bad_audio_hashes(42, "H@rco", "bad rip", inputs)
        second = db.add_bad_audio_hashes(99, "OtherUser", "duplicate", inputs)
        self.assertEqual(first, 2)
        self.assertEqual(second, 0)
        self.assertEqual(len(db.bad_audio_hashes), 2)
        # First-writer-wins on attribution
        self.assertEqual(db.bad_audio_hashes[0].request_id, 42)
        self.assertEqual(db.bad_audio_hashes[0].reported_username, "H@rco")

    def test_add_bad_audio_hashes_partial_overlap(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        first_batch = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
        ]
        db.add_bad_audio_hashes(42, "H@rco", "bad rip", first_batch)
        second_batch = [
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="flac"),
        ]
        n = db.add_bad_audio_hashes(43, "OtherUser", "bad rip", second_batch)
        # Only the genuinely-new (3, flac) row inserted
        self.assertEqual(n, 1)
        self.assertEqual(len(db.bad_audio_hashes), 3)

    def test_add_bad_audio_hashes_empty_list_is_zero(self):
        db = FakePipelineDB()
        n = db.add_bad_audio_hashes(42, "u", "r", [])
        self.assertEqual(n, 0)

    def test_add_bad_audio_hashes_same_hash_different_format_both_inserted(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(1), audio_format="mp3"),
        ]
        n = db.add_bad_audio_hashes(42, "u", "r", inputs)
        self.assertEqual(n, 2)

    def test_lookup_bad_audio_hash_hits_when_present(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        db.add_bad_audio_hashes(
            42, "u", "r",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        row = db.lookup_bad_audio_hash(self._hash(7), "flac")
        assert row is not None
        self.assertEqual(row.hash_value, self._hash(7))
        self.assertEqual(row.audio_format, "flac")
        self.assertEqual(row.request_id, 42)
        self.assertEqual(row.reported_username, "u")

    def test_lookup_bad_audio_hash_miss_returns_none(self):
        db = FakePipelineDB()
        self.assertIsNone(db.lookup_bad_audio_hash(self._hash(9), "flac"))

    def test_lookup_bad_audio_hash_format_must_match(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        db.add_bad_audio_hashes(
            42, "u", "r",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        # Same hash, different format → miss
        self.assertIsNone(db.lookup_bad_audio_hash(self._hash(7), "mp3"))
        # Same format, different hash → miss
        self.assertIsNone(db.lookup_bad_audio_hash(self._hash(8), "flac"))

    def test_has_any_bad_audio_hashes_false_on_fresh_fake(self):
        db = FakePipelineDB()
        self.assertFalse(db.has_any_bad_audio_hashes())

    def test_has_any_bad_audio_hashes_true_after_one_insert(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        db.add_bad_audio_hashes(
            42, None, None,
            [BadAudioHashInput(hash_value=self._hash(1), audio_format="flac")],
        )
        self.assertTrue(db.has_any_bad_audio_hashes())


class TestFakeRecentSuccessfulUploader(unittest.TestCase):
    """Self-tests for FakePipelineDB.get_recent_successful_uploader (plan U2)."""

    def test_returns_none_on_empty_logs(self):
        db = FakePipelineDB()
        self.assertIsNone(db.get_recent_successful_uploader(42))

    def test_returns_none_when_no_successful_log(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="bob", outcome="rejected")
        db.log_download(42, soulseek_username="alice", outcome="error")
        self.assertIsNone(db.get_recent_successful_uploader(42))

    def test_returns_most_recent_success(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username="bob", outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "bob")

    def test_returns_most_recent_force_import(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username="harco", outcome="force_import")
        self.assertEqual(db.get_recent_successful_uploader(42), "harco")

    def test_ignores_other_request_ids(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(99, soulseek_username="bob", outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "alice")
        self.assertEqual(db.get_recent_successful_uploader(99), "bob")

    def test_skips_null_uploader_rows(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username=None, outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "alice")


class TestFakeActiveImportJobForRequest(unittest.TestCase):
    """Self-tests for FakePipelineDB.get_active_import_job_for_request (plan U2)."""

    def _enqueue(self, db: FakePipelineDB, *, request_id: int, dedupe_key: str):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        return db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=manual_import_payload(failed_path="/tmp/x"),
            preview_enabled=False,
        )

    def test_returns_none_when_no_jobs(self):
        db = FakePipelineDB()
        self.assertIsNone(db.get_active_import_job_for_request(42))

    def test_returns_queued_job_for_request(self):
        db = FakePipelineDB()
        job = self._enqueue(db, request_id=42, dedupe_key="manual:42")
        result = db.get_active_import_job_for_request(42)
        assert result is not None
        self.assertEqual(result["id"], job.id)
        self.assertEqual(result["status"], "queued")

    def test_returns_running_job_for_request(self):
        db = FakePipelineDB()
        self._enqueue(db, request_id=42, dedupe_key="manual:42")
        # Move it to "would_import" then claim → status='running'.
        db.mark_import_job_preview_importable(
            db._import_jobs[0]["id"],
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        result = db.get_active_import_job_for_request(42)
        assert result is not None
        self.assertEqual(result["status"], "running")
        self.assertEqual(result["id"], claimed.id)

    def test_returns_none_for_completed_job(self):
        db = FakePipelineDB()
        job = self._enqueue(db, request_id=42, dedupe_key="manual:42")
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        db.mark_import_job_completed(claimed.id, result={"ok": True})
        self.assertIsNone(db.get_active_import_job_for_request(42))

    def test_returns_none_for_failed_job(self):
        db = FakePipelineDB()
        job = self._enqueue(db, request_id=42, dedupe_key="manual:42")
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        db.mark_import_job_failed(claimed.id, error="boom")
        self.assertIsNone(db.get_active_import_job_for_request(42))

    def test_returns_only_jobs_for_the_requested_request_id(self):
        db = FakePipelineDB()
        self._enqueue(db, request_id=42, dedupe_key="manual:42")
        self._enqueue(db, request_id=99, dedupe_key="manual:99")
        r42 = db.get_active_import_job_for_request(42)
        r99 = db.get_active_import_job_for_request(99)
        assert r42 is not None and r99 is not None
        self.assertEqual(r42["request_id"], 42)
        self.assertEqual(r99["request_id"], 99)

    def test_returns_most_recent_job_when_multiple_active(self):
        db = FakePipelineDB()
        # First job with one dedupe_key
        first = self._enqueue(db, request_id=42, dedupe_key="manual:42:a")
        second = self._enqueue(db, request_id=42, dedupe_key="manual:42:b")
        result = db.get_active_import_job_for_request(42)
        assert result is not None
        # Most recent by id
        self.assertEqual(result["id"], max(first.id, second.id))


def _public_methods(cls: type) -> set[str]:
    """Return the set of non-underscore method names defined on ``cls``."""
    return {
        name for name, obj in vars(cls).items()
        if callable(obj) and not name.startswith("_")
    }


class TestPipelineDBFakeContract(unittest.TestCase):
    """Enforce FakePipelineDB stays in lockstep with PipelineDB.

    Models ``TestRouteContractAudit`` (tests/test_web_server.py): the
    convention in ``.claude/rules/code-quality.md`` — "every new
    PipelineDB method must have a matching stub on FakePipelineDB with
    a self-test in tests/test_fakes.py" — is enforced at test time, not
    at review time.

    Silent drift was possible before this test existed. In PR #136
    ``update_imported_path_by_release_id`` only got its direct self-test
    after the final-review agent flagged it; any orchestration test
    that tried to call the method via a fake that lacked it would have
    crashed with ``AttributeError``. A new kwarg on a real method would
    be silently swallowed if the fake accepted ``**kwargs``.
    """

    def test_fake_exposes_every_public_method_of_real(self) -> None:
        """Every non-underscore method on ``PipelineDB`` must exist on
        ``FakePipelineDB``."""
        real = _public_methods(PipelineDB)
        fake = _public_methods(FakePipelineDB)
        missing = real - fake
        self.assertEqual(
            missing, set(),
            f"FakePipelineDB is missing stubs for: {sorted(missing)}. "
            "See .claude/rules/code-quality.md 'New PipelineDB method' "
            "in the new-work checklist.",
        )

    def test_fake_only_methods_stay_on_the_allowlist(self) -> None:
        """Methods on ``FakePipelineDB`` that don't mirror ``PipelineDB``
        must be intentional test helpers on an explicit allowlist.

        Catches typos in new stub names
        (``update_importred_path_by_release_id`` would pass the
        ``real - fake`` check because the method isn't on real, but
        the sigcheck never exercises it). Without this inverse
        enforcement, a typo'd stub would compile and tests against it
        would crash with ``AttributeError`` — the exact silent-drift
        vector this contract is meant to prevent.
        """
        allowed_fake_only = {
            "seed_request",
            "request",
            "assert_log",
            "set_advisory_lock_result",
            "set_cooldown_result",
        }
        real = _public_methods(PipelineDB)
        fake = _public_methods(FakePipelineDB)
        unexpected = fake - real - allowed_fake_only
        self.assertEqual(
            unexpected, set(),
            f"FakePipelineDB has methods not on PipelineDB and not on "
            f"the allowlist: {sorted(unexpected)}. If these are "
            "intentional test helpers, add them to "
            "``allowed_fake_only``. If they're typo'd stubs meant to "
            "mirror a real method, rename them.",
        )

    def test_fake_signatures_compatible_with_real(self) -> None:
        """For every shared method, each named parameter on the real
        method must be declared by name on the fake with a compatible
        kind and no stricter requiredness.

        This catches "real added a new kwarg; fake silently ignored it"
        drift. Crucially, a bare ``**kwargs`` on the fake is NOT allowed
        to absorb a named real parameter — otherwise a fake that
        accepts ``**kwargs`` would pass this check for any real
        signature, reproducing the exact silent-drift failure mode the
        contract is meant to prevent.

        ``**kwargs`` on the fake may still absorb test-only extras and
        matches the real's own ``**kwargs`` when present. Return types
        and type annotations are not checked — the fake is free to use
        ``Any`` for brevity.
        """
        mismatches = _diff_signatures(PipelineDB, FakePipelineDB)
        self.assertEqual(
            mismatches, [],
            "FakePipelineDB signatures drifted from PipelineDB. "
            "Every real parameter must be named explicitly on the fake "
            "(bare **kwargs does NOT satisfy the contract). "
            "Mismatches:\n  "
            + "\n  ".join(mismatches),
        )


_POSITIONAL_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _diff_signatures(real_cls: type, fake_cls: type) -> list[str]:
    """Return a list of signature drift messages between two classes.

    The invariant the reviewers kept circling: the fake must be
    substitutable for the real in every production-valid call pattern.
    Checks, in order, what a caller could observe:

    1. Positional layout must match exactly. Any reorder, insertion,
       or rename at a positional slot would bind ``add_request("A",
       "B", "request")`` to the wrong parameter on the fake (codex R4).
    2. Every named real parameter must be declared by name on the
       fake. ``**kwargs`` absorption is NOT sufficient — a fake that
       absorbs a renamed kwarg silently reproduces the drift this
       contract is meant to prevent (round 1).
    3. Kinds must match exactly. Narrowing positional-or-keyword to
       keyword-only breaks positional callers (codex R3).
    4. Requiredness drift in both directions: real required → fake
       optional lets the fake accept calls real would reject; real
       optional → fake required crashes calls real would handle
       (codex R3).
    5. ``*args`` / ``**kwargs`` on real require equivalents on fake
       so variadic callers don't silently lose arguments.

    The fake may add trailing keyword-only parameters with defaults
    (for test-only bookkeeping) and absorb test-only extras with
    ``**kwargs`` — those are not visible to any real-valid caller so
    they do not need to be mirrored back onto real.
    """
    real_methods = _public_methods(real_cls)
    fake_methods = _public_methods(fake_cls)
    shared = real_methods & fake_methods

    mismatches: list[str] = []
    for name in sorted(shared):
        real_sig = inspect.signature(getattr(real_cls, name))
        fake_sig = inspect.signature(getattr(fake_cls, name))

        mismatches.extend(_diff_positional_layout(name, real_sig, fake_sig))
        mismatches.extend(_diff_named_params(name, real_sig, fake_sig))
        mismatches.extend(_diff_variadic(name, real_sig, fake_sig))
        mismatches.extend(_diff_fake_only_required(name, real_sig, fake_sig))
    return mismatches


def _positional_params(
    sig: inspect.Signature,
) -> list[inspect.Parameter]:
    return [
        p for p in sig.parameters.values()
        if p.name != "self" and p.kind in _POSITIONAL_KINDS
    ]


def _diff_positional_layout(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Positional slots must match real exactly — no reorder, no extras.

    Python binds positional args by index; a fake that adds
    ``add_request(album_title, artist_name, source)`` would satisfy the
    name-matching check while binding ``add_request("Artist", "Album",
    "request")`` to the wrong parameters (codex R4).
    """
    out: list[str] = []
    real_pos = _positional_params(real_sig)
    fake_pos = _positional_params(fake_sig)

    for i, rp in enumerate(real_pos):
        if i >= len(fake_pos):
            out.append(
                f"{method}: positional slot {i} ('{rp.name}') "
                "present on real but missing from fake's positional "
                "sequence")
            continue
        fp = fake_pos[i]
        if fp.name != rp.name:
            out.append(
                f"{method}: positional slot {i} — real='{rp.name}', "
                f"fake='{fp.name}' (reorder, rename, or inserted "
                "parameter would break positional callers)")
    if len(fake_pos) > len(real_pos):
        extras = [fp.name for fp in fake_pos[len(real_pos):]]
        out.append(
            f"{method}: fake has extra positional parameters beyond "
            f"real: {extras} (a positional call on real would bind "
            "nothing to these slots on the fake)")
    return out


def _diff_named_params(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Every named real param must be declared on the fake with a
    compatible kind and requiredness."""
    out: list[str] = []
    fake_params = fake_sig.parameters
    for pname, param in real_sig.parameters.items():
        if pname == "self":
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL,
                          inspect.Parameter.VAR_KEYWORD):
            continue
        if pname not in fake_params:
            out.append(
                f"{method}: param '{pname}' present on real but "
                "not declared on fake (declare it explicitly — "
                "**kwargs does not count)")
            continue
        fp = fake_params[pname]
        if fp.kind != param.kind:
            out.append(
                f"{method}({pname}): kind mismatch — "
                f"real={param.kind.name}, fake={fp.kind.name}")
            continue
        real_required = param.default is inspect.Parameter.empty
        fake_required = fp.default is inspect.Parameter.empty
        if real_required and not fake_required:
            out.append(
                f"{method}({pname}): real requires this param but "
                "fake gives it a default (silently makes it optional)")
        elif fake_required and not real_required:
            out.append(
                f"{method}({pname}): real has a default but fake "
                "requires this param (production calls that omit it "
                "would crash against the fake)")
    return out


def _diff_fake_only_required(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Fake params absent from real must have defaults.

    A fake that adds a required keyword-only parameter
    (e.g. ``def m(self, request_id, *, new_required):``) has no match
    in ``_diff_named_params`` — that helper walks only real params.
    Every production call that omits the new kwarg works against real
    but raises ``TypeError`` against the fake. Codex R5.

    Optional extras (with defaults) are fine — they represent
    test-only bookkeeping the fake may accept.
    """
    out: list[str] = []
    real_names = {p.name for p in real_sig.parameters.values()}
    for fp in fake_sig.parameters.values():
        if fp.name == "self":
            continue
        if fp.kind in (inspect.Parameter.VAR_POSITIONAL,
                       inspect.Parameter.VAR_KEYWORD):
            continue
        if fp.name in real_names:
            continue
        # Fake-only parameter. Required → crashes real-valid callers.
        if fp.default is inspect.Parameter.empty:
            out.append(
                f"{method}({fp.name}): fake requires a parameter not "
                "on real — production calls that omit it would crash "
                "against the fake (give it a default, or remove it)")
    return out


def _diff_variadic(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """``*args`` / ``**kwargs`` on real require equivalents on fake."""
    out: list[str] = []
    fake_accepts_varargs = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL
        for p in fake_sig.parameters.values())
    fake_accepts_kwargs = any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in fake_sig.parameters.values())
    for param in real_sig.parameters.values():
        if (param.kind == inspect.Parameter.VAR_POSITIONAL
                and not fake_accepts_varargs):
            out.append(
                f"{method}: real has *{param.name} but fake does "
                "not accept variable positional args")
        elif (param.kind == inspect.Parameter.VAR_KEYWORD
                and not fake_accepts_kwargs):
            out.append(
                f"{method}: real has **{param.name} but fake does "
                "not accept variable keyword args")
    return out


class TestPipelineDBFakeContractInternals(unittest.TestCase):
    """Regression tests for the drift detector itself.

    The detector must fail when real and fake disagree, otherwise the
    outer contract test is a silent no-op. Exercise the drift cases
    directly.
    """

    def test_kwargs_does_not_absorb_named_param(self):
        """Bare **kwargs on fake must NOT satisfy a named real param."""
        class Real:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, request_id: int, **kwargs: Any) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("'flag'" in m for m in diff),
            f"Expected drift for named param 'flag', got: {diff}")

    def test_renamed_param_is_caught(self):
        class Real:
            def m(self, spectral_grade: str | None = None) -> None:
                ...
        class Fake:
            def m(self, grade: str | None = None) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("'spectral_grade'" in m for m in diff),
            f"Expected drift for renamed param, got: {diff}")

    def test_required_becoming_optional_is_caught(self):
        class Real:
            def m(self, release_id: str) -> None:
                ...
        class Fake:
            def m(self, release_id: str = "") -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("release_id" in m and "optional" in m for m in diff),
            f"Expected requiredness drift, got: {diff}")

    def test_clean_signature_yields_no_diff(self):
        class Real:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        self.assertEqual(_diff_signatures(Real, Fake), [])

    def test_star_kwargs_on_real_still_requires_fake_kwargs(self):
        class Real:
            def m(self, **extra: Any) -> None:
                ...
        class Fake:
            def m(self) -> None:  # no **kwargs
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("**extra" in m for m in diff),
            f"Expected drift when fake drops **kwargs, got: {diff}")

    def test_positional_or_keyword_narrowed_to_keyword_only_is_caught(self):
        """Codex R3: a fake that narrows pos-or-keyword to keyword-only
        would break every caller using positional args — must fail the
        contract so fake-backed tests cannot silently green."""
        class Real:
            def m(self, artist_name: str, album_title: str) -> None:
                ...
        class Fake:
            def m(self, *, artist_name: str, album_title: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("kind mismatch" in m for m in diff),
            f"Expected drift for narrowed kind, got: {diff}")

    def test_optional_becoming_required_on_fake_is_caught(self):
        """Codex R3: a fake that drops a default would force production
        callers to pass the arg — production calls that omit it would
        work against real but crash the fake."""
        class Real:
            def m(self, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, flag: bool) -> None:  # no default
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("fake requires this param" in m for m in diff),
            f"Expected drift for tightened requiredness, got: {diff}")

    def test_positional_reorder_is_caught(self):
        """Codex R4: a fake that swaps positional parameter order
        would bind positional args to the wrong params. Name-matching
        alone cannot catch this — the positional layout must be
        checked by index."""
        class Real:
            def m(self, artist_name: str, album_title: str,
                  source: str) -> None:
                ...
        class Fake:
            def m(self, album_title: str, artist_name: str,
                  source: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("positional slot" in m for m in diff),
            f"Expected drift for reordered positional params, got: "
            f"{diff}")

    def test_fake_with_extra_positional_param_is_caught(self):
        """Codex R4: a fake that adds an extra positional parameter
        beyond real breaks positional callers — real's call pattern
        would leave that slot unbound on the fake."""
        class Real:
            def m(self, artist_name: str, album_title: str) -> None:
                ...
        class Fake:
            def m(self, artist_name: str, album_title: str,
                  new_required: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("extra positional parameters" in m for m in diff),
            f"Expected drift for fake with extra positional, got: "
            f"{diff}")

    def test_fake_with_required_keyword_only_not_on_real_is_caught(self):
        """Codex R5: a fake that adds a required keyword-only
        parameter real doesn't have would crash any production-valid
        call that omits it."""
        class Real:
            def m(self, request_id: int) -> None:
                ...
        class Fake:
            def m(self, request_id: int, *, new_required: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("new_required" in m and "not on real" in m
                for m in diff),
            f"Expected drift for required fake-only kwarg, got: "
            f"{diff}")

    def test_fake_with_optional_keyword_only_not_on_real_is_allowed(self):
        """Optional fake-only params (for test-only bookkeeping) are
        permitted — real-valid callers never pass them, so they don't
        affect call compatibility."""
        class Real:
            def m(self, request_id: int) -> None:
                ...
        class Fake:
            def m(self, request_id: int, *,
                  test_only: bool = False) -> None:
                ...
        self.assertEqual(_diff_signatures(Real, Fake), [])
