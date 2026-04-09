"""Tests for lightweight fakes and shared builders."""

import unittest

from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import RequestSpectralStateUpdate
from lib.quality import SpectralContext, SpectralMeasurement, ValidationResult
from tests.fakes import FakePipelineDB
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

    def test_get_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=3, status="downloading"))

        rows = db.get_downloading()
        self.assertEqual(len(rows), 2)
        ids = {r["id"] for r in rows}
        self.assertEqual(ids, {1, 3})

    def test_assert_log_passes(self):
        db = FakePipelineDB()
        db.log_download(42, outcome="success", soulseek_username="user1")

        # Should not raise
        db.assert_log(self, 0, outcome="success", request_id=42)

    def test_assert_log_checks_extra_fields(self):
        db = FakePipelineDB()
        db.log_download(42, outcome="success", spectral_grade="genuine")

        db.assert_log(self, 0, outcome="success")
        # Extra field goes into .extra dict
        self.assertEqual(db.download_logs[0].extra["spectral_grade"], "genuine")


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
