"""Tests for download state reducer — pure decision function."""

import unittest

from lib.quality import (
    DownloadDecision,
    DownloadVerdict,
    decide_download_action,
)


class TestDecideDownloadAction(unittest.TestCase):
    """Test the pure download decision function."""

    def _decide(self, **overrides):
        """Build default args and apply overrides."""
        defaults = dict(
            album_done=False,
            error_filenames=None,
            total_files=3,
            all_remote_queued=False,
            elapsed_seconds=60.0,
            idle_seconds=10.0,
            remote_queue_timeout=3600,
            stalled_timeout=1800,
            file_retries={},
            max_file_retries=5,
            processing_started=False,
        )
        defaults.update(overrides)
        return decide_download_action(**defaults)  # type: ignore[arg-type]

    def test_processing_started(self):
        v = self._decide(processing_started=True)
        self.assertEqual(v.decision, DownloadDecision.processing)

    def test_complete_no_errors(self):
        v = self._decide(album_done=True, error_filenames=None)
        self.assertEqual(v.decision, DownloadDecision.complete)

    def test_remote_queue_timeout(self):
        v = self._decide(all_remote_queued=True,
                         elapsed_seconds=3601, remote_queue_timeout=3600)
        self.assertEqual(v.decision, DownloadDecision.timeout_remote_queue)

    def test_remote_queue_not_timed_out(self):
        v = self._decide(all_remote_queued=True,
                         elapsed_seconds=1800, remote_queue_timeout=3600)
        self.assertEqual(v.decision, DownloadDecision.in_progress)

    def test_all_files_errored(self):
        v = self._decide(error_filenames=["a.flac", "b.flac", "c.flac"],
                         total_files=3)
        self.assertEqual(v.decision, DownloadDecision.timeout_all_errored)

    def test_partial_errors_retries_left(self):
        v = self._decide(error_filenames=["a.flac"],
                         file_retries={"a.flac": 2},
                         max_file_retries=5)
        self.assertEqual(v.decision, DownloadDecision.retry_files)
        self.assertEqual(v.files_to_retry, ["a.flac"])

    def test_partial_errors_max_retries(self):
        v = self._decide(error_filenames=["a.flac"],
                         file_retries={"a.flac": 5},
                         max_file_retries=5)
        self.assertEqual(v.decision, DownloadDecision.timeout_stalled)
        self.assertIn("retry limit", v.reason)

    def test_stalled_timeout(self):
        v = self._decide(idle_seconds=1801, stalled_timeout=1800)
        self.assertEqual(v.decision, DownloadDecision.timeout_stalled)
        self.assertIn("no download progress", v.reason)

    def test_stalled_not_checked_when_remote_queued(self):
        """Stall timer doesn't apply when all files are remotely queued."""
        v = self._decide(all_remote_queued=True,
                         idle_seconds=9999, stalled_timeout=1800,
                         elapsed_seconds=100, remote_queue_timeout=3600)
        self.assertEqual(v.decision, DownloadDecision.in_progress)

    def test_in_progress(self):
        v = self._decide()
        self.assertEqual(v.decision, DownloadDecision.in_progress)

    def test_multiple_retries_only_returns_eligible(self):
        """Only files below max_retries are in files_to_retry."""
        v = self._decide(
            error_filenames=["a.flac", "b.flac"],
            file_retries={"a.flac": 5, "b.flac": 2},
            max_file_retries=5,
        )
        # a.flac is at limit → timeout
        self.assertEqual(v.decision, DownloadDecision.timeout_stalled)

    def test_error_file_not_in_retries_dict(self):
        """File with no retry history → 0 retries, should retry."""
        v = self._decide(error_filenames=["new.flac"],
                         file_retries={})
        self.assertEqual(v.decision, DownloadDecision.retry_files)
        self.assertEqual(v.files_to_retry, ["new.flac"])


class TestDownloadDecisionEnum(unittest.TestCase):
    def test_all_values(self):
        self.assertEqual(len(DownloadDecision), 7)


if __name__ == "__main__":
    unittest.main()
