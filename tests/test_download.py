"""Tests for lib/download.py — download processing functions.

Tests _build_download_info, _gather_spectral_context, cancel_and_delete,
slskd_download_status, downloads_all_done, monitor_downloads, grab_most_wanted
(extracted from soularr.py).
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock
import os
import time

from lib.quality import DownloadInfo, SpectralContext


def _make_file(filename="01 - Track.mp3", username="user1",
               bitRate=320, sampleRate=44100, bitDepth=None,
               isVariableBitRate=None, file_dir="user1\\Music",
               size=5000000):
    """Build a mock DownloadFile."""
    f = MagicMock()
    f.filename = filename
    f.username = username
    f.bitRate = bitRate
    f.sampleRate = sampleRate
    f.bitDepth = bitDepth
    f.isVariableBitRate = isVariableBitRate
    f.file_dir = file_dir
    f.size = size
    f.id = "file-id-1"
    f.status = None
    f.retry = None
    f.import_path = None
    f.disk_no = None
    f.disk_count = None
    return f


def _make_album_data(files=None, filetype="mp3", is_vbr=False,
                     mb_release_id: str | None = "test-mbid", artist="Test Artist",
                     title="Test Album", year="2020"):
    """Build a mock GrabListEntry."""
    mock = MagicMock()
    mock.files = files or [_make_file()]
    mock.artist = artist
    mock.title = title
    mock.year = year
    mock.mb_release_id = mb_release_id
    mock.filetype = filetype
    mock.spectral_grade = None
    mock.spectral_bitrate = None
    mock.existing_spectral_bitrate = None
    mock.existing_min_bitrate = None
    mock.album_id = 123
    mock.db_request_id = 1
    mock.db_source = "request"
    return mock


def _make_ctx(cfg=None, slskd=None, pipeline_db_source=None):
    """Build a mock SoularrContext."""
    from lib.context import SoularrContext
    if cfg is None:
        cfg = MagicMock()
        cfg.slskd_download_dir = "/tmp/test_downloads"
        cfg.beets_validation_enabled = False
        cfg.beets_distance_threshold = 0.15
        cfg.beets_staging_dir = "/tmp/staging"
        cfg.beets_harness_path = "/tmp/harness"
        cfg.audio_check_mode = "normal"
        cfg.stalled_timeout = 300
        cfg.remote_queue_timeout = 120
        cfg.slskd_host_url = "http://localhost:5030"
        cfg.slskd_url_base = "/"
        cfg.pipeline_db_enabled = True
        cfg.meelo_url = None
    if slskd is None:
        slskd = MagicMock()
    if pipeline_db_source is None:
        pipeline_db_source = MagicMock()
    return SoularrContext(cfg=cfg, slskd=slskd,
                          pipeline_db_source=pipeline_db_source)


class TestBuildDownloadInfo(unittest.TestCase):

    def test_basic(self):
        from lib.import_dispatch import _build_download_info
        files = [_make_file(bitRate=320, sampleRate=44100)]
        album = _make_album_data(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "user1")
        self.assertEqual(dl.filetype, "mp3")
        self.assertEqual(dl.bitrate, 320)
        self.assertEqual(dl.sample_rate, 44100)

    def test_empty_files(self):
        from lib.import_dispatch import _build_download_info
        mock = MagicMock()
        mock.files = []
        dl = _build_download_info(mock)
        self.assertIsNone(dl.username)
        self.assertIsNone(dl.filetype)

    def test_multi_user(self):
        from lib.import_dispatch import _build_download_info
        files = [
            _make_file(username="beta_user"),
            _make_file(username="alpha_user"),
        ]
        album = _make_album_data(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "alpha_user, beta_user")


class TestGatherSpectralContext(unittest.TestCase):

    def test_non_mp3_returns_no_check(self):
        from lib.import_dispatch import _build_download_info
        files = [_make_file(filename="01 - Track.flac")]
        album = _make_album_data(files=files, filetype="flac")
        dl = _build_download_info(album)
        # For FLAC, spectral check happens in import_one.py, not pre-staging
        # _gather_spectral_context checks filetype string for "mp3"
        filetype_str = (dl.filetype or "").lower()
        is_mp3 = "mp3" in filetype_str and "flac" not in filetype_str
        self.assertFalse(is_mp3)

    def test_vbr_mp3_no_check(self):
        from lib.import_dispatch import _build_download_info
        files = [_make_file(isVariableBitRate=True)]
        album = _make_album_data(files=files, filetype="mp3", is_vbr=True)
        dl = _build_download_info(album)
        is_vbr = dl.is_vbr or False
        is_mp3 = "mp3" in (dl.filetype or "").lower()
        # VBR MP3 doesn't need spectral check
        self.assertTrue(is_mp3)
        self.assertTrue(is_vbr)

    def test_cbr_mp3_needs_check(self):
        from lib.import_dispatch import _build_download_info
        files = [_make_file(bitRate=320, isVariableBitRate=False)]
        album = _make_album_data(files=files, filetype="mp3")
        dl = _build_download_info(album)
        is_vbr = dl.is_vbr or False
        is_mp3 = "mp3" in (dl.filetype or "").lower()
        self.assertTrue(is_mp3)
        self.assertFalse(is_vbr)


class TestCheckQualityGateDecision(unittest.TestCase):
    """Test that quality gate calls the pure decision function correctly."""

    def test_verified_lossless_accepts(self):
        from lib.quality import quality_gate_decision, AudioQualityMeasurement
        m = AudioQualityMeasurement(min_bitrate_kbps=207, verified_lossless=True)
        self.assertEqual(quality_gate_decision(m), "accept")

    def test_low_bitrate_requeues(self):
        from lib.quality import quality_gate_decision, AudioQualityMeasurement
        m = AudioQualityMeasurement(min_bitrate_kbps=190)
        self.assertEqual(quality_gate_decision(m), "requeue_upgrade")

    def test_cbr_requeues_flac(self):
        from lib.quality import quality_gate_decision, AudioQualityMeasurement
        m = AudioQualityMeasurement(min_bitrate_kbps=320, is_cbr=True)
        self.assertEqual(quality_gate_decision(m), "requeue_flac")

    def test_vbr_above_threshold_accepts(self):
        from lib.quality import quality_gate_decision, AudioQualityMeasurement
        m = AudioQualityMeasurement(min_bitrate_kbps=245)
        self.assertEqual(quality_gate_decision(m), "accept")


# === NEW tests for functions moving to lib/download.py ===

class TestDownloadsAllDone(unittest.TestCase):
    """downloads_all_done is pure logic — test all branches."""

    def test_all_succeeded(self):
        from lib.download import downloads_all_done
        files = [_make_file(), _make_file()]
        files[0].status = {"state": "Completed, Succeeded"}
        files[1].status = {"state": "Completed, Succeeded"}
        done, problems, queued = downloads_all_done(files)
        self.assertTrue(done)
        self.assertIsNone(problems)
        self.assertEqual(queued, 0)

    def test_one_errored(self):
        from lib.download import downloads_all_done
        files = [_make_file(), _make_file()]
        files[0].status = {"state": "Completed, Succeeded"}
        files[1].status = {"state": "Completed, Errored"}
        done, problems, queued = downloads_all_done(files)
        self.assertFalse(done)
        self.assertIsNotNone(problems)
        assert problems is not None
        self.assertEqual(len(problems), 1)
        self.assertEqual(queued, 0)

    def test_queued_remotely(self):
        from lib.download import downloads_all_done
        files = [_make_file(), _make_file()]
        files[0].status = {"state": "Completed, Succeeded"}
        files[1].status = {"state": "Queued, Remotely"}
        done, problems, queued = downloads_all_done(files)
        self.assertFalse(done)
        self.assertIsNone(problems)
        self.assertEqual(queued, 1)

    def test_all_error_states(self):
        """Every error state should appear in problems list."""
        from lib.download import downloads_all_done
        error_states = [
            "Completed, Cancelled",
            "Completed, TimedOut",
            "Completed, Errored",
            "Completed, Rejected",
            "Completed, Aborted",
        ]
        for state in error_states:
            files = [_make_file()]
            files[0].status = {"state": state}
            done, problems, _ = downloads_all_done(files)
            self.assertFalse(done, f"state={state} should not be done")
            self.assertIsNotNone(problems, f"state={state} should be a problem")

    def test_none_status_skipped(self):
        from lib.download import downloads_all_done
        files = [_make_file()]
        files[0].status = None
        done, problems, queued = downloads_all_done(files)
        # None status means we can't confirm done
        self.assertTrue(done)  # loop body skips None
        self.assertIsNone(problems)


class TestCancelAndDelete(unittest.TestCase):
    """cancel_and_delete uses ctx.slskd and ctx.cfg."""

    def test_cancels_and_removes_dir(self):
        from lib.download import cancel_and_delete
        ctx = _make_ctx()
        f = _make_file(file_dir="someuser\\Album Folder")
        with patch("os.path.isdir", return_value=True), \
             patch("shutil.rmtree") as mock_rm:
            cancel_and_delete([f], ctx)
        ctx.slskd.transfers.cancel_download.assert_called_once_with(
            username="user1", id="file-id-1")
        mock_rm.assert_called_once()

    def test_cancel_failure_continues(self):
        """Should not raise if cancel_download throws."""
        from lib.download import cancel_and_delete
        ctx = _make_ctx()
        ctx.slskd.transfers.cancel_download.side_effect = Exception("network error")
        f = _make_file()
        with patch("os.path.exists", return_value=False), \
             patch("os.chdir"):
            cancel_and_delete([f], ctx)  # should not raise


class TestSlskdDownloadStatus(unittest.TestCase):

    def test_populates_status(self):
        from lib.download import slskd_download_status
        ctx = _make_ctx()
        ctx.slskd.transfers.get_download.return_value = {"state": "Completed, Succeeded"}
        f = _make_file()
        ok = slskd_download_status([f], ctx)
        self.assertTrue(ok)
        self.assertEqual(f.status["state"], "Completed, Succeeded")

    def test_error_sets_none(self):
        from lib.download import slskd_download_status
        ctx = _make_ctx()
        ctx.slskd.transfers.get_download.side_effect = Exception("fail")
        f = _make_file()
        ok = slskd_download_status([f], ctx)
        self.assertFalse(ok)
        self.assertIsNone(f.status)


class TestSlskdDoEnqueue(unittest.TestCase):

    def test_successful_enqueue(self):
        from lib.download import slskd_do_enqueue
        ctx = _make_ctx()
        ctx.slskd.transfers.enqueue.return_value = True
        ctx.slskd.transfers.get_downloads.return_value = {
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track.mp3", "id": "new-id"}]
            }]
        }
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "new-id")

    def test_enqueue_failure_returns_none(self):
        from lib.download import slskd_do_enqueue
        ctx = _make_ctx()
        ctx.slskd.transfers.enqueue.side_effect = Exception("fail")
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", [], "dir", ctx)
        self.assertIsNone(result)


class TestGatherSpectralContextFunction(unittest.TestCase):
    """Test the actual _gather_spectral_context function from lib/download."""

    def test_flac_returns_no_check(self):
        from lib.download import _gather_spectral_context
        files = [_make_file(filename="01.flac", isVariableBitRate=False)]
        album = _make_album_data(files=files, filetype="flac")
        ctx = _make_ctx()
        result = _gather_spectral_context(album, "/tmp/folder", ctx)
        self.assertIsInstance(result, SpectralContext)
        self.assertFalse(result.needs_check)

    def test_vbr_mp3_returns_no_check(self):
        from lib.download import _gather_spectral_context
        files = [_make_file(isVariableBitRate=True)]
        album = _make_album_data(files=files, filetype="mp3", is_vbr=True)
        ctx = _make_ctx()
        result = _gather_spectral_context(album, "/tmp/folder", ctx)
        self.assertFalse(result.needs_check)

    @patch("lib.download.spectral_analyze")
    def test_cbr_mp3_runs_analysis(self, mock_spectral):
        from lib.download import _gather_spectral_context
        mock_spectral.return_value = MagicMock(
            grade="genuine", estimated_bitrate_kbps=320, suspect_pct=0.0)
        files = [_make_file(bitRate=320, isVariableBitRate=False)]
        album = _make_album_data(files=files, filetype="mp3", is_vbr=False,
                                 mb_release_id=None)
        ctx = _make_ctx()
        result = _gather_spectral_context(album, "/tmp/folder", ctx)
        self.assertTrue(result.needs_check)
        self.assertEqual(result.grade, "genuine")
        self.assertEqual(result.bitrate, 320)

    @patch("lib.download.BeetsDB")
    @patch("lib.download.spectral_analyze")
    def test_cbr_mp3_checks_existing(self, mock_spectral, mock_beets_cls):
        from lib.download import _gather_spectral_context
        # New download spectral
        mock_spectral.return_value = MagicMock(
            grade="suspect", estimated_bitrate_kbps=192, suspect_pct=80.0)
        # Existing beets album
        mock_beets = MagicMock()
        mock_beets.get_album_info.return_value = MagicMock(
            min_bitrate_kbps=256, album_path="/tmp/existing")
        mock_beets.__enter__ = MagicMock(return_value=mock_beets)
        mock_beets.__exit__ = MagicMock(return_value=False)
        mock_beets_cls.return_value = mock_beets

        files = [_make_file(bitRate=320, isVariableBitRate=False)]
        album = _make_album_data(files=files, filetype="mp3", is_vbr=False)
        ctx = _make_ctx()

        with patch("os.path.isdir", return_value=True):
            # Second spectral call for existing files
            mock_spectral.side_effect = [
                MagicMock(grade="suspect", estimated_bitrate_kbps=192, suspect_pct=80.0),
                MagicMock(grade="genuine", estimated_bitrate_kbps=310, suspect_pct=0.0),
            ]
            result = _gather_spectral_context(album, "/tmp/folder", ctx)

        self.assertTrue(result.needs_check)
        self.assertEqual(result.existing_min_bitrate, 256)
        self.assertEqual(result.existing_spectral_bitrate, 310)


class TestGrabMostWanted(unittest.TestCase):
    """grab_most_wanted receives search_and_queue as callable."""

    def test_no_albums_returns_zero(self):
        from lib.download import grab_most_wanted
        ctx = _make_ctx()
        search_fn = MagicMock(return_value=({}, [], []))
        with patch("time.sleep"):
            count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 0)

    def test_failed_search_counted(self):
        from lib.download import grab_most_wanted
        from album_source import AlbumRecord
        ctx = _make_ctx()
        failed_album = AlbumRecord(
            id=-1, title="Album", release_date="2024-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[], db_request_id=1, db_source="request",
            db_mb_release_id="", db_quality_override=None,
        )
        search_fn = MagicMock(return_value=({}, [failed_album], []))
        with patch("time.sleep"):
            count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)

    def test_failed_grab_counted(self):
        from lib.download import grab_most_wanted
        from album_source import AlbumRecord
        ctx = _make_ctx()
        failed_album = AlbumRecord(
            id=-1, title="Album", release_date="2024-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[], db_request_id=1, db_source="request",
            db_mb_release_id="", db_quality_override=None,
        )
        search_fn = MagicMock(return_value=({}, [], [failed_album]))
        with patch("time.sleep"):
            count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
