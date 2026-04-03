"""Tests for lib/download.py — download processing functions.

Tests _build_download_info, _gather_spectral_context, cancel_and_delete,
slskd_download_status, downloads_all_done, poll_active_downloads, grab_most_wanted
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
    """grab_most_wanted enqueues and persists state, no blocking monitor."""

    def test_no_albums_returns_zero(self):
        from lib.download import grab_most_wanted
        ctx = _make_ctx()
        search_fn = MagicMock(return_value=({}, [], []))
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
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)

    def test_sets_downloading_status(self):
        """After enqueue, album_requests.status = 'downloading'."""
        from lib.download import grab_most_wanted
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid", db_request_id=42, db_source="request",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000),
            ],
        )
        ctx = _make_ctx()
        mock_db = MagicMock()
        ctx.pipeline_db_source._get_db.return_value = mock_db
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        grab_most_wanted([], search_fn, ctx)
        mock_db.set_downloading.assert_called_once()
        call_args = mock_db.set_downloading.call_args
        self.assertEqual(call_args[0][0], 42)  # request_id

    def test_writes_active_download_state(self):
        """JSONB written with correct structure."""
        from lib.download import grab_most_wanted
        from lib.grab_list import GrabListEntry, DownloadFile
        import json
        entry = GrabListEntry(
            album_id=1, filetype="mp3 v0", title="T", artist="A", year="2020",
            mb_release_id="mbid", db_request_id=42, db_source="request",
            files=[
                DownloadFile(filename="u\\M\\01.mp3", id="tid-1",
                             file_dir="u\\M", username="user1", size=5000000),
            ],
        )
        ctx = _make_ctx()
        mock_db = MagicMock()
        ctx.pipeline_db_source._get_db.return_value = mock_db
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        grab_most_wanted([], search_fn, ctx)
        state_json = mock_db.set_downloading.call_args[0][1]
        state = json.loads(state_json)
        self.assertEqual(state["filetype"], "mp3 v0")
        self.assertEqual(len(state["files"]), 1)

    def test_no_blocking_monitor(self):
        """grab_most_wanted returns immediately without blocking."""
        from lib.download import grab_most_wanted
        from lib.grab_list import GrabListEntry, DownloadFile
        import time as _time
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid", db_request_id=42, db_source="request",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000),
            ],
        )
        ctx = _make_ctx()
        mock_db = MagicMock()
        ctx.pipeline_db_source._get_db.return_value = mock_db
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        start = _time.time()
        grab_most_wanted([], search_fn, ctx)
        elapsed = _time.time() - start
        self.assertLess(elapsed, 2.0)  # Must return fast (no blocking loop)


class TestMatchTransferId(unittest.TestCase):
    """Test match_transfer_id() — find slskd transfer ID by filename."""

    def test_exact_filename_match(self):
        from lib.download import match_transfer_id
        downloads = {
            "directories": [{
                "directory": "user\\Music",
                "files": [
                    {"filename": "user\\Music\\01.flac", "id": "abc-123"},
                    {"filename": "user\\Music\\02.flac", "id": "def-456"},
                ],
            }],
        }
        result = match_transfer_id(downloads, "user\\Music\\01.flac")
        self.assertEqual(result, "abc-123")

    def test_not_found(self):
        from lib.download import match_transfer_id
        downloads = {"directories": [{"directory": "user\\Music", "files": []}]}
        result = match_transfer_id(downloads, "user\\Music\\missing.flac")
        self.assertIsNone(result)

    def test_multi_directory(self):
        from lib.download import match_transfer_id
        downloads = {
            "directories": [
                {"directory": "d1", "files": [
                    {"filename": "d1\\01.flac", "id": "id-1"},
                ]},
                {"directory": "d2", "files": [
                    {"filename": "d2\\01.flac", "id": "id-2"},
                ]},
            ],
        }
        result = match_transfer_id(downloads, "d2\\01.flac")
        self.assertEqual(result, "id-2")


class TestRederiveTransferIds(unittest.TestCase):
    """Test rederive_transfer_ids() — re-derive IDs from slskd API."""

    def test_updates_files_in_place(self):
        from lib.download import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, files=[
                DownloadFile(filename="u\\M\\01.flac", id="", file_dir="u\\M",
                             username="user1", size=1000),
                DownloadFile(filename="u\\M\\02.flac", id="", file_dir="u\\M",
                             username="user1", size=2000),
            ],
            filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
        )
        mock_slskd = MagicMock()
        mock_slskd.transfers.get_downloads.return_value = {
            "directories": [{"directory": "u\\M", "files": [
                {"filename": "u\\M\\01.flac", "id": "new-id-1"},
                {"filename": "u\\M\\02.flac", "id": "new-id-2"},
            ]}],
        }
        rederive_transfer_ids(entry, mock_slskd)
        self.assertEqual(entry.files[0].id, "new-id-1")
        self.assertEqual(entry.files[1].id, "new-id-2")

    def test_missing_transfer_keeps_empty_id(self):
        from lib.download import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, files=[
                DownloadFile(filename="u\\M\\01.flac", id="", file_dir="u\\M",
                             username="user1", size=1000),
            ],
            filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
        )
        mock_slskd = MagicMock()
        mock_slskd.transfers.get_downloads.return_value = {
            "directories": [{"directory": "u\\M", "files": []}],
        }
        rederive_transfer_ids(entry, mock_slskd)
        self.assertEqual(entry.files[0].id, "")


class TestProcessCompletedAlbumReturnsBool(unittest.TestCase):
    """Test process_completed_album returns True/False."""

    @patch("lib.download.music_tag")
    def test_returns_true_on_success(self, mock_mt):
        """Successful file move + processing returns True."""
        from lib.download import process_completed_album
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source file
            src_dir = os.path.join(tmpdir, "source_dir")
            os.makedirs(src_dir)
            src_file = os.path.join(src_dir, "01 - Track.mp3")
            with open(src_file, "w") as f:
                f.write("fake audio")

            files = [_make_file(filename="source_dir\\01 - Track.mp3",
                                file_dir="source_dir")]
            album = _make_album_data(files=files, mb_release_id=None)
            ctx = _make_ctx()
            ctx.cfg.slskd_download_dir = tmpdir
            ctx.cfg.beets_validation_enabled = False
            mock_mt.load_file.return_value = MagicMock()
            result = process_completed_album(album, [], ctx)
            self.assertTrue(result)

    def test_returns_false_on_file_move_failure(self):
        """File move failure returns False."""
        from lib.download import process_completed_album
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmpdir:
            # Source dir exists but file doesn't — move will fail
            files = [_make_file(filename="nonexistent_dir\\01 - Track.mp3",
                                file_dir="nonexistent_dir")]
            album = _make_album_data(files=files, mb_release_id=None)
            ctx = _make_ctx()
            ctx.cfg.slskd_download_dir = tmpdir
            ctx.cfg.beets_validation_enabled = False
            result = process_completed_album(album, [], ctx)
            self.assertFalse(result)


class TestPollActiveDownloads(unittest.TestCase):
    """Test poll_active_downloads() — core polling function."""

    def _make_downloading_row(self, request_id=1, state_dict=None):
        """Build a mock album_requests row with status='downloading'."""
        if state_dict is None:
            state_dict = {
                "filetype": "flac",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            }
        return {
            "id": request_id,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            "mb_release_id": "test-mbid",
            "source": "request",
            "quality_override": None,
            "status": "downloading",
            "active_download_state": state_dict,
        }

    def _make_poll_ctx(self, downloading_rows=None, slskd_downloads=None):
        """Build context with mocked DB + slskd for polling."""
        ctx = _make_ctx()
        mock_db = MagicMock()
        mock_db.get_downloading.return_value = downloading_rows or []
        mock_db.get_request.return_value = None  # default: album not found after processing
        ctx.pipeline_db_source._get_db.return_value = mock_db

        if slskd_downloads is not None:
            ctx.slskd.transfers.get_downloads.return_value = slskd_downloads
        else:
            # Default: return transfers that match the files
            ctx.slskd.transfers.get_downloads.return_value = {
                "directories": [{"directory": "user1\\Music", "files": [
                    {"filename": "user1\\Music\\01.flac", "id": "tid-1"},
                ]}],
            }
        return ctx, mock_db

    def test_poll_active_no_downloading(self):
        """No downloading albums → no-op."""
        from lib.download import poll_active_downloads
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[])
        poll_active_downloads(ctx)
        mock_db.clear_download_state.assert_not_called()

    @patch("lib.download.process_completed_album")
    def test_poll_active_all_complete(self, mock_process):
        """1 downloading album, all files complete → calls process_completed_album."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        # Mock slskd status: file is complete
        def mock_status(downloads, ctx_arg):
            for f in downloads:
                f.status = {"state": "Completed, Succeeded"}
            return True
        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            mock_process.return_value = True
            # After process_completed_album, DB shows status='imported'
            mock_db.get_request.return_value = {"id": 1, "status": "imported"}
            poll_active_downloads(ctx)

        mock_db.clear_download_state.assert_called_once_with(1)
        mock_process.assert_called_once()

    @patch("lib.download.process_completed_album")
    def test_poll_active_all_complete_no_beets(self, mock_process):
        """beets_validation_enabled=False → process returns True, poll sets imported."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        def mock_status(downloads, ctx_arg):
            for f in downloads:
                f.status = {"state": "Completed, Succeeded"}
            return True
        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            mock_process.return_value = True
            # After process_completed_album, status still 'downloading' (no beets)
            mock_db.get_request.return_value = {"id": 1, "status": "downloading"}
            poll_active_downloads(ctx)

        mock_db.update_status.assert_called_once_with(1, "imported")

    def test_poll_active_timeout(self):
        """enqueued_at is old, timeout exceeded → cancel, log, reset to wanted."""
        from lib.download import poll_active_downloads
        # Use an enqueued_at far in the past
        state_dict = {
            "filetype": "flac",
            "enqueued_at": "2020-01-01T00:00:00+00:00",  # Very old
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        # Should have logged a timeout download and reset to wanted
        mock_db.log_download.assert_called_once()
        kwargs = mock_db.log_download.call_args.kwargs
        self.assertEqual(kwargs["outcome"], "timeout")
        # Check the _reset_to_wanted was called via raw SQL
        reset_calls = [c for c in mock_db._execute.call_args_list
                       if "wanted" in str(c)]
        self.assertTrue(len(reset_calls) > 0, "Expected _reset_to_wanted SQL call")

    def test_poll_active_transfer_vanished_all(self):
        """slskd returns no matching transfers → treat as timeout."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, mock_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads={"directories": [{"directory": "user1\\Music", "files": []}]},
        )
        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        mock_db.log_download.assert_called_once()
        # Check outcome is timeout
        kwargs = mock_db.log_download.call_args.kwargs
        self.assertEqual(kwargs["outcome"], "timeout")

    def test_poll_active_in_progress(self):
        """Files still downloading → no action, remains downloading."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        def mock_status(downloads, ctx_arg):
            for f in downloads:
                f.status = {"state": "InProgress"}
            return True
        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            poll_active_downloads(ctx)

        # Should NOT process or timeout
        mock_db.clear_download_state.assert_not_called()
        mock_db.log_download.assert_not_called()

    @patch("lib.download.process_completed_album")
    def test_poll_active_multiple_albums(self, mock_process):
        """2 albums: 1 completes, 1 in progress → correct handling."""
        from lib.download import poll_active_downloads
        row1 = self._make_downloading_row(request_id=1)
        state2 = {
            "filetype": "mp3 v0",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [
                {"username": "user2", "filename": "user2\\Music\\01.mp3",
                 "file_dir": "user2\\Music", "size": 5000000},
            ],
        }
        row2 = self._make_downloading_row(request_id=2, state_dict=state2)
        row2["album_title"] = "Album 2"
        row2["artist_name"] = "Artist 2"

        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row1, row2])

        # slskd returns transfers for both users
        def get_downloads_side_effect(username=None):
            if username == "user1":
                return {"directories": [{"directory": "user1\\Music", "files": [
                    {"filename": "user1\\Music\\01.flac", "id": "tid-1"},
                ]}]}
            elif username == "user2":
                return {"directories": [{"directory": "user2\\Music", "files": [
                    {"filename": "user2\\Music\\01.mp3", "id": "tid-2"},
                ]}]}
            return {"directories": []}
        ctx.slskd.transfers.get_downloads.side_effect = get_downloads_side_effect

        call_count = [0]
        def mock_status(downloads, ctx_arg):
            for f in downloads:
                if f.username == "user1":
                    f.status = {"state": "Completed, Succeeded"}
                else:
                    f.status = {"state": "InProgress"}
            return True

        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            mock_process.return_value = True
            mock_db.get_request.return_value = {"id": 1, "status": "imported"}
            poll_active_downloads(ctx)

        # Album 1 completed, album 2 still in progress
        mock_db.clear_download_state.assert_called_once_with(1)
        mock_process.assert_called_once()

    def test_poll_crash_recovery_no_state(self):
        """Downloading album with no active_download_state → reset to wanted."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        row["active_download_state"] = None  # Simulates crash
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        poll_active_downloads(ctx)

        # Should call _reset_to_wanted
        reset_calls = [c for c in mock_db._execute.call_args_list
                       if "wanted" in str(c)]
        self.assertTrue(len(reset_calls) > 0)

    @patch("lib.download.process_completed_album")
    def test_poll_active_all_errors(self, mock_process):
        """All files errored → timeout the album."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        def mock_status(downloads, ctx_arg):
            for f in downloads:
                f.status = {"state": "Completed, Errored"}
            return True
        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            with patch("lib.download.cancel_and_delete"):
                poll_active_downloads(ctx)

        mock_process.assert_not_called()
        mock_db.log_download.assert_called_once()

    def test_poll_active_remote_queue_timeout(self):
        """All files queued remotely past timeout → timeout."""
        from lib.download import poll_active_downloads
        # enqueued long enough ago to exceed remote_queue_timeout but not stalled_timeout
        from datetime import datetime, timezone, timedelta
        past = (datetime.now(timezone.utc) - timedelta(seconds=200)).isoformat()
        state_dict = {
            "filetype": "flac",
            "enqueued_at": past,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])
        ctx.cfg.remote_queue_timeout = 120  # 2 minutes
        ctx.cfg.stalled_timeout = 600  # 10 minutes (not exceeded)

        def mock_status(downloads, ctx_arg):
            for f in downloads:
                f.status = {"state": "Queued, Remotely"}
            return True
        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            with patch("lib.download.cancel_and_delete"):
                poll_active_downloads(ctx)

        mock_db.log_download.assert_called_once()
        kwargs = mock_db.log_download.call_args.kwargs
        self.assertEqual(kwargs["outcome"], "timeout")

    @patch("lib.download.process_completed_album")
    def test_poll_transfer_vanished_partial(self, mock_process):
        """7/12 files vanish → treated as errors, not complete."""
        from lib.download import poll_active_downloads
        # 12 files, only 5 have transfers in slskd
        files = []
        for i in range(12):
            files.append({"username": "user1",
                          "filename": f"user1\\Music\\{i:02d}.flac",
                          "file_dir": "user1\\Music", "size": 30000000})
        state_dict = {
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": files,
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        # Only files 0-4 have transfers in slskd
        slskd_files = [{"filename": f"user1\\Music\\{i:02d}.flac", "id": f"tid-{i}"}
                       for i in range(5)]
        ctx.slskd.transfers.get_downloads.return_value = {
            "directories": [{"directory": "user1\\Music", "files": slskd_files}],
        }

        def mock_status(downloads, ctx_arg):
            for f in downloads:
                if f.id:  # Only files with IDs get polled
                    f.status = {"state": "InProgress"}
            return True
        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            poll_active_downloads(ctx)

        # Should NOT process — 7 files vanished (errored), album not complete
        mock_process.assert_not_called()
        mock_db.clear_download_state.assert_not_called()


    @patch("lib.download.process_completed_album")
    def test_poll_active_partial_errors_with_retry(self, mock_process):
        """Some files errored, retries available → re-enqueue those files."""
        from lib.download import poll_active_downloads
        # 3 files: 2 complete, 1 errored
        state_dict = {
            "filetype": "flac",
            "enqueued_at": "2026-04-03T12:00:00+00:00",
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
                {"username": "user1", "filename": "user1\\Music\\02.flac",
                 "file_dir": "user1\\Music", "size": 25000000},
                {"username": "user1", "filename": "user1\\Music\\03.flac",
                 "file_dir": "user1\\Music", "size": 20000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        # All 3 files have transfers in slskd
        ctx.slskd.transfers.get_downloads.return_value = {
            "directories": [{"directory": "user1\\Music", "files": [
                {"filename": "user1\\Music\\01.flac", "id": "tid-1"},
                {"filename": "user1\\Music\\02.flac", "id": "tid-2"},
                {"filename": "user1\\Music\\03.flac", "id": "tid-3"},
            ]}],
        }

        def mock_status(downloads, ctx_arg):
            for f in downloads:
                if f.filename.endswith("03.flac"):
                    f.status = {"state": "Completed, Errored"}
                else:
                    f.status = {"state": "Completed, Succeeded"}
            return True

        mock_requeue_file = MagicMock()
        mock_requeue_file.id = "new-tid-3"
        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            with patch("lib.download.slskd_do_enqueue", return_value=[mock_requeue_file]) as mock_enqueue:
                poll_active_downloads(ctx)

        # Should NOT process (not all done) and NOT timeout
        mock_process.assert_not_called()
        mock_db.log_download.assert_not_called()
        # Should re-enqueue the errored file
        mock_enqueue.assert_called_once()
        call_args = mock_enqueue.call_args
        self.assertEqual(call_args[0][0], "user1")  # username
        self.assertEqual(call_args[0][1][0]["filename"], "user1\\Music\\03.flac")

    @patch("lib.download.process_completed_album")
    def test_poll_no_redownload_window(self, mock_process):
        """Album stays 'downloading' during process_completed_album — no redownload window."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, mock_db = self._make_poll_ctx(downloading_rows=[row])

        def mock_status(downloads, ctx_arg):
            for f in downloads:
                f.status = {"state": "Completed, Succeeded"}
            return True

        # After process_completed_album, mark_done set status to 'imported'
        mock_db.get_request.return_value = {"id": 1, "status": "imported"}

        # Track all update_status and _reset_to_wanted calls to verify
        # album was never set to 'wanted' during processing
        status_updates = []
        original_update = mock_db.update_status.side_effect
        def track_update(*args, **kwargs):
            status_updates.append(args)
        mock_db.update_status.side_effect = track_update

        with patch("lib.download.slskd_download_status", side_effect=mock_status):
            mock_process.return_value = True
            poll_active_downloads(ctx)

        # clear_download_state was called (state cleared while still 'downloading')
        mock_db.clear_download_state.assert_called_once_with(1)
        # process_completed_album ran
        mock_process.assert_called_once()
        # The album was NEVER set to 'wanted' before processing
        for args in status_updates:
            if len(args) >= 2:
                self.assertNotEqual(args[1], "wanted",
                                  "Album should never be set to 'wanted' during processing")
        # _reset_to_wanted uses _execute, not update_status — check no 'wanted' SQL
        for call in mock_db._execute.call_args_list:
            sql = str(call[0][0]) if call[0] else ""
            if "wanted" in sql:
                self.fail("Album was reset to 'wanted' during processing — redownload window!")


class TestBuildActiveDownloadState(unittest.TestCase):
    """Test build_active_download_state() — GrabListEntry → ActiveDownloadState."""

    def test_basic(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.filetype, "flac")
        self.assertIsNotNone(state.enqueued_at)
        self.assertEqual(len(state.files), 1)
        self.assertEqual(state.files[0].username, "user1")
        self.assertEqual(state.files[0].filename, "u\\M\\01.flac")
        self.assertEqual(state.files[0].size, 30000000)

    def test_multi_disc(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\D1-01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000,
                             disk_no=1, disk_count=2),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.files[0].disk_no, 1)
        self.assertEqual(state.files[0].disk_count, 2)

    def test_enqueued_at_is_utc_iso(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        from datetime import datetime as dt, timezone as tz
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid", files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=1000),
            ],
        )
        state = build_active_download_state(entry)
        parsed = dt.fromisoformat(state.enqueued_at)
        self.assertEqual(parsed.tzinfo, tz.utc)


class TestReconstructGrabListEntry(unittest.TestCase):
    """Test reconstruct_grab_list_entry() — rebuild GrabListEntry from DB row + state."""

    def test_reconstruct_basic(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=30000000,
                ),
            ],
        )
        request = {
            "id": 42,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            "mb_release_id": "test-mbid",
            "source": "request",
            "quality_override": None,
        }
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.album_id, 42)
        self.assertEqual(entry.title, "Test Album")
        self.assertEqual(entry.artist, "Test Artist")
        self.assertEqual(entry.year, "2020")
        self.assertEqual(entry.filetype, "flac")
        self.assertEqual(entry.mb_release_id, "test-mbid")
        self.assertEqual(entry.db_request_id, 42)
        self.assertEqual(entry.db_source, "request")
        self.assertEqual(len(entry.files), 1)
        self.assertEqual(entry.files[0].filename, "user1\\Music\\01.flac")
        self.assertEqual(entry.files[0].id, "")  # Must be re-derived

    def test_reconstruct_multi_disc(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\D1-01.flac",
                    file_dir="user1\\Music", size=30000000,
                    disk_no=1, disk_count=2,
                ),
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\D2-01.flac",
                    file_dir="user1\\Music", size=25000000,
                    disk_no=2, disk_count=2,
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "quality_override": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].disk_no, 1)
        self.assertEqual(entry.files[0].disk_count, 2)
        self.assertEqual(entry.files[1].disk_no, 2)

    def test_reconstruct_quality_override(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "quality_override": "flac"}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.db_quality_override, "flac")

    def test_reconstruct_missing_year(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": None, "mb_release_id": "mbid", "source": "request",
                   "quality_override": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.year, "")


if __name__ == "__main__":
    unittest.main()
