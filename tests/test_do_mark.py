"""Tests for _do_mark_done and rejection recording — standalone DB operations.

Orchestration tests using FakePipelineDB. Assert domain state (request
status/fields, download log rows, recorded attempts) rather than mock
call shapes.
"""

import unittest

from lib.quality import DownloadInfo, SpectralMeasurement
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestDoMarkDone(unittest.TestCase):
    """_do_mark_done must transition to imported, log download, handle spectral state."""

    def _call(self, dl_info=None, outcome_label="success", **kwargs):
        from lib.import_dispatch import _do_mark_done
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        if dl_info is None:
            dl_info = DownloadInfo(username="testuser", filetype="mp3")
        _do_mark_done(
            db=db,  # type: ignore[arg-type]
            request_id=42,
            dl_info=dl_info,
            distance=0.05,
            scenario="strong_match",
            dest_path="/tmp/staged/Artist - Album",
            outcome_label=outcome_label,
            **kwargs,
        )
        return db

    def test_transitions_to_imported(self):
        db = self._call()
        self.assertEqual(db.request(42)["status"], "imported")

    def test_logs_download_with_outcome_label(self):
        db = self._call(outcome_label="force_import")
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.download_logs[0].outcome, "force_import")

    def test_logs_download_with_default_success(self):
        db = self._call()
        self.assertEqual(db.download_logs[0].outcome, "success")

    def test_verified_lossless_override_respected(self):
        dl = DownloadInfo(username="testuser")
        dl.verified_lossless_override = True
        dl.download_spectral = SpectralMeasurement(grade="genuine", bitrate_kbps=None)
        db = self._call(dl_info=dl)
        self.assertTrue(db.request(42)["verified_lossless"])

    def test_spectral_state_written(self):
        dl = DownloadInfo(username="testuser")
        dl.download_spectral = SpectralMeasurement(grade="suspect", bitrate_kbps=192)
        db = self._call(dl_info=dl)
        row = db.request(42)
        self.assertEqual(row["last_download_spectral_grade"], "suspect")
        self.assertEqual(row["last_download_spectral_bitrate"], 192)

    def test_logs_username_and_distance(self):
        dl = DownloadInfo(username="cooluser", filetype="flac")
        db = self._call(dl_info=dl)
        log = db.download_logs[0]
        self.assertEqual(log.soulseek_username, "cooluser")
        assert log.beets_distance is not None
        self.assertAlmostEqual(log.beets_distance, 0.05)


class TestRecordRejectionAndMaybeRequeue(unittest.TestCase):
    """Rejection recording must log failure and optionally requeue."""

    def _call(self, requeue=True, outcome_label="rejected",
              search_filetype_override=None, dl_info=None,
              validation_result=None, staged_path=None):
        from lib.import_dispatch import _record_rejection_and_maybe_requeue
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        if dl_info is None:
            dl_info = DownloadInfo(username="baduser")
        _record_rejection_and_maybe_requeue(
            db=db,  # type: ignore[arg-type]
            request_id=42,
            dl_info=dl_info,
            distance=0.35,
            scenario="quality_downgrade",
            detail="new 128kbps <= existing 320kbps",
            error=None,
            requeue=requeue,
            outcome_label=outcome_label,
            search_filetype_override=search_filetype_override,
            validation_result=validation_result,
            staged_path=staged_path,
        )
        return db

    def test_requeue_true_transitions_to_wanted(self):
        db = self._call(requeue=True)
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["last_attempt_at"])

    def test_requeue_false_does_not_transition(self):
        db = self._call(requeue=False)
        self.assertEqual(db.request(42)["status"], "downloading")

    def test_always_logs_download(self):
        db = self._call(requeue=False)
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.download_logs[0].outcome, "rejected")

    def test_outcome_label_in_log(self):
        db = self._call(outcome_label="failed")
        self.assertEqual(db.download_logs[0].outcome, "failed")

    def test_records_attempt_when_requeuing(self):
        db = self._call(requeue=True)
        self.assertEqual(db.recorded_attempts, [(42, "validation")])

    def test_no_attempt_record_when_not_requeuing(self):
        db = self._call(requeue=False)
        self.assertEqual(db.recorded_attempts, [])

    def test_search_override_passed_to_transition(self):
        db = self._call(requeue=True, search_filetype_override="flac,mp3 v0")
        self.assertEqual(db.request(42)["search_filetype_override"], "flac,mp3 v0")

    def test_explicit_validation_result_and_staged_path_logged(self):
        db = self._call(
            requeue=False,
            validation_result='{"scenario":"quality_downgrade"}',
            staged_path="/tmp/staged/Artist - Album",
        )
        log = db.download_logs[0]
        self.assertEqual(log.validation_result, '{"scenario":"quality_downgrade"}')
        self.assertEqual(log.staged_path, "/tmp/staged/Artist - Album")


if __name__ == "__main__":
    unittest.main()
