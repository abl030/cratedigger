"""Tests for _do_mark_done and _do_mark_failed — standalone DB operations.

These functions inline the logic from DatabaseSource.mark_done/mark_failed
but take PipelineDB directly, enabling dispatch_import_core to operate
without a DatabaseSource wrapper.
"""

import unittest
from unittest.mock import MagicMock

from lib.quality import (DownloadInfo, SpectralMeasurement)
from tests.helpers import make_request_row


class TestDoMarkDone(unittest.TestCase):
    """_do_mark_done must transition to imported, log download, handle spectral state."""

    def _call(self, dl_info=None, outcome_label="success", **kwargs):
        from lib.import_dispatch import _do_mark_done
        db = MagicMock()
        db.get_request.return_value = make_request_row(status="downloading")
        if dl_info is None:
            dl_info = DownloadInfo(username="testuser", filetype="mp3")
        _do_mark_done(
            db=db,
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
        """Must call update_status with 'imported'."""
        db = self._call()
        # apply_transition calls update_status or reset_to_wanted
        update_calls = db.update_status.call_args_list
        self.assertTrue(len(update_calls) > 0, "Expected update_status call")
        # First positional arg is request_id, second is status
        self.assertEqual(update_calls[0][0][1], "imported")

    def test_logs_download_with_outcome_label(self):
        """Must log download with the provided outcome_label."""
        db = self._call(outcome_label="force_import")
        log_calls = db.log_download.call_args_list
        self.assertEqual(len(log_calls), 1)
        self.assertEqual(log_calls[0].kwargs["outcome"], "force_import")

    def test_logs_download_with_default_success(self):
        """Default outcome_label should be 'success'."""
        db = self._call()
        log_calls = db.log_download.call_args_list
        self.assertEqual(log_calls[0].kwargs["outcome"], "success")

    def test_verified_lossless_override_respected(self):
        """When dl_info has verified_lossless_override=True, transition should include it."""
        dl = DownloadInfo(username="testuser")
        dl.verified_lossless_override = True
        dl.download_spectral = SpectralMeasurement(grade="genuine", bitrate_kbps=None)
        db = self._call(dl_info=dl)
        update_kwargs = db.update_status.call_args.kwargs
        self.assertTrue(update_kwargs.get("verified_lossless"))

    def test_spectral_state_written(self):
        """Spectral data from dl_info must be written to album_requests."""
        dl = DownloadInfo(username="testuser")
        dl.download_spectral = SpectralMeasurement(grade="suspect", bitrate_kbps=192)
        db = self._call(dl_info=dl)
        update_kwargs = db.update_status.call_args.kwargs
        self.assertEqual(update_kwargs.get("last_download_spectral_grade"), "suspect")
        self.assertEqual(update_kwargs.get("last_download_spectral_bitrate"), 192)

    def test_logs_username_and_distance(self):
        """Download log must include username and beets distance."""
        dl = DownloadInfo(username="cooluser", filetype="flac")
        db = self._call(dl_info=dl)
        log_kwargs = db.log_download.call_args.kwargs
        self.assertEqual(log_kwargs["soulseek_username"], "cooluser")
        self.assertAlmostEqual(log_kwargs["beets_distance"], 0.05)


class TestDoMarkFailed(unittest.TestCase):
    """_do_mark_failed must log failure, optionally requeue, handle cooldowns."""

    def _call(self, requeue=True, outcome_label="rejected",
              search_filetype_override=None, dl_info=None):
        from lib.import_dispatch import _do_mark_failed
        db = MagicMock()
        db.get_request.return_value = make_request_row(status="downloading")
        if dl_info is None:
            dl_info = DownloadInfo(username="baduser")
        _do_mark_failed(
            db=db,
            request_id=42,
            dl_info=dl_info,
            distance=0.35,
            scenario="quality_downgrade",
            detail="new 128kbps <= existing 320kbps",
            error=None,
            requeue=requeue,
            outcome_label=outcome_label,
            search_filetype_override=search_filetype_override,
        )
        return db

    def test_requeue_true_transitions_to_wanted(self):
        """When requeue=True, must transition back to 'wanted'."""
        db = self._call(requeue=True)
        # apply_transition for downloading->wanted calls reset_to_wanted
        db.reset_to_wanted.assert_called_once()

    def test_requeue_false_does_not_transition(self):
        """When requeue=False (force/manual import), must NOT change status."""
        db = self._call(requeue=False)
        db.reset_to_wanted.assert_not_called()
        # Should not call update_status to change to wanted either
        for call in db.update_status.call_args_list:
            if len(call.args) > 1:
                self.assertNotEqual(call.args[1], "wanted")

    def test_always_logs_download(self):
        """Must always create a download_log entry regardless of requeue."""
        db = self._call(requeue=False)
        db.log_download.assert_called_once()
        self.assertEqual(db.log_download.call_args.kwargs["outcome"], "rejected")

    def test_outcome_label_in_log(self):
        """Must use the provided outcome_label in download_log."""
        db = self._call(outcome_label="failed")
        self.assertEqual(db.log_download.call_args.kwargs["outcome"], "failed")

    def test_records_attempt_when_requeuing(self):
        """When requeuing, must record the validation attempt."""
        db = self._call(requeue=True)
        db.record_attempt.assert_called_once_with(42, "validation")

    def test_no_attempt_record_when_not_requeuing(self):
        """When not requeuing, must NOT record attempt (force/manual import)."""
        db = self._call(requeue=False)
        db.record_attempt.assert_not_called()

    def test_search_override_passed_to_transition(self):
        """search_filetype_override must be forwarded to the transition."""
        db = self._call(requeue=True, search_filetype_override="flac,mp3 v0")
        reset_kwargs = db.reset_to_wanted.call_args.kwargs
        self.assertEqual(reset_kwargs.get("search_filetype_override"), "flac,mp3 v0")


if __name__ == "__main__":
    unittest.main()
