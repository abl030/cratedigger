"""Integration tests — exercise the full search→enqueue→download→process flow.

Uses realistic slskd API fixtures with mocked API calls. Catches type
mismatches at the boundary between raw slskd dicts and DownloadFile instances.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

# Mock heavy deps before importing soularr
sys.modules["music_tag"] = MagicMock()
sys.modules["slskd_api"] = MagicMock()
sys.modules["slskd_api.apis"] = MagicMock()
sys.modules["slskd_api.apis.users"] = MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import soularr
from lib.grab_list import GrabListEntry, DownloadFile


# ─── Fixtures ────────────────────────────────────────────────────────

SEARCH_FILE_FLAC = {
    "filename": "Music\\Artist\\Album\\01 - Track One.flac",
    "size": 52428800,
    "bitRate": 1411,
    "sampleRate": 44100,
    "bitDepth": 16,
    "isVariableBitRate": False,
}

SEARCH_FILE_MP3_V0 = {
    "filename": "Music\\Artist\\Album\\01 - Track One.mp3",
    "size": 8000000,
    "bitRate": 245,
    "sampleRate": 44100,
    "bitDepth": 16,
    "isVariableBitRate": True,
}

SEARCH_FILE_MP3_320 = {
    "filename": "Music\\Artist\\Album\\01 - Track One.mp3",
    "size": 10000000,
    "bitRate": 320,
    "sampleRate": 44100,
    "bitDepth": 16,
    "isVariableBitRate": False,
}

DIRECTORY_FILE_FLAC = {
    "filename": "01 - Track One.flac",
    "size": 52428800,
    "id": "transfer-abc-123",
    "bitRate": 1411,
    "sampleRate": 44100,
    "bitDepth": 16,
    "isVariableBitRate": False,
}

DIRECTORY_FILE_MP3 = {
    "filename": "01 - Track One.mp3",
    "size": 8000000,
    "id": "transfer-def-456",
    "bitRate": 245,
    "sampleRate": 44100,
    "bitDepth": 16,
    "isVariableBitRate": True,
}

DOWNLOAD_STATUS_DONE = {"state": "Completed, Succeeded"}
DOWNLOAD_STATUS_QUEUED = {"state": "Queued, Remotely"}
DOWNLOAD_STATUS_FAILED = {"state": "Completed, Errored"}


def make_search_result(username, files, upload_speed=1048576):
    """Build a search result dict as slskd returns it."""
    return {
        "username": username,
        "uploadSpeed": upload_speed,
        "files": files,
    }


def make_directory(dir_path, files):
    """Build a directory dict as slskd.users.directory() returns it."""
    return {
        "directory": dir_path,
        "files": files,
    }


def make_download_list(directories):
    """Build a get_downloads response dict."""
    return {"directories": directories}


# ─── Tests ───────────────────────────────────────────────────────────


class TestRawDictBoundary(unittest.TestCase):
    """Verify that functions receiving raw slskd dicts work with plain dicts,
    not DownloadFile instances."""

    def test_verify_filetype_with_raw_dict(self):
        """verify_filetype receives raw slskd file dicts, not DownloadFile."""
        result = soularr.verify_filetype(SEARCH_FILE_FLAC, "flac")
        self.assertTrue(result)

    def test_verify_filetype_mp3_v0(self):
        result = soularr.verify_filetype(SEARCH_FILE_MP3_V0, "mp3 v0")
        self.assertTrue(result)

    def test_verify_filetype_mp3_320(self):
        result = soularr.verify_filetype(SEARCH_FILE_MP3_320, "mp3 320")
        self.assertTrue(result)

    def test_verify_filetype_rejects_wrong_type(self):
        result = soularr.verify_filetype(SEARCH_FILE_FLAC, "mp3")
        self.assertFalse(result)

    def test_verify_filetype_bitdepth_samplerate(self):
        result = soularr.verify_filetype(SEARCH_FILE_FLAC, "flac 16/44.1")
        self.assertTrue(result)

    def test_verify_filetype_rejects_wrong_bitdepth(self):
        result = soularr.verify_filetype(SEARCH_FILE_FLAC, "flac 24/96")
        self.assertFalse(result)

    def test_album_track_num_with_raw_dicts(self):
        """album_track_num receives raw directory dicts."""
        orig_cfg = soularr.cfg
        mock_cfg = MagicMock()
        mock_cfg.allowed_filetypes = ("flac", "mp3")
        soularr.cfg = mock_cfg
        try:
            directory = make_directory("Music\\Album", [
                {"filename": "01 - Track.flac", "size": 100},
                {"filename": "02 - Track.flac", "size": 100},
                {"filename": "cover.jpg", "size": 50},
            ])
            result = soularr.album_track_num(directory)
            self.assertEqual(result["count"], 2)
            self.assertEqual(result["filetype"], "flac")
        finally:
            soularr.cfg = orig_cfg

    def test_download_filter_with_raw_dicts(self):
        """download_filter receives raw directory dicts."""
        orig_cfg = soularr.cfg
        mock_cfg = MagicMock()
        mock_cfg.download_filtering = True
        mock_cfg.use_extension_whitelist = True
        mock_cfg.extensions_whitelist = ("jpg", "txt")
        soularr.cfg = mock_cfg
        try:
            directory = make_directory("Music\\Album", [
                {"filename": "01 - Track.flac", "size": 100},
                {"filename": "cover.jpg", "size": 50},
                {"filename": "info.nfo", "size": 10},
            ])
            soularr.download_filter("flac", directory)
            filenames = [f["filename"] for f in directory["files"]]
            self.assertIn("01 - Track.flac", filenames)
            self.assertIn("cover.jpg", filenames)
            self.assertNotIn("info.nfo", filenames)
        finally:
            soularr.cfg = orig_cfg


class TestSlskdDoEnqueue(unittest.TestCase):
    """Verify slskd_do_enqueue returns DownloadFile instances."""

    def setUp(self):
        self._orig_slskd = soularr.slskd
        soularr.slskd = MagicMock()

    def tearDown(self):
        soularr.slskd = self._orig_slskd

    def test_returns_download_files(self):
        """slskd_do_enqueue should return list of DownloadFile, not dicts."""
        file_dir = "Music\\Artist\\Album"
        files = [{"filename": file_dir + "\\01 - Track.flac", "size": 52428800}]

        soularr.slskd.transfers.enqueue.return_value = True
        soularr.slskd.transfers.get_downloads.return_value = make_download_list([
            make_directory(file_dir, [
                {"filename": file_dir + "\\01 - Track.flac", "id": "xfer-1", "size": 52428800},
            ])
        ])

        result = soularr.slskd_do_enqueue("testuser", files, file_dir)

        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], DownloadFile)
        self.assertEqual(result[0].filename, file_dir + "\\01 - Track.flac")
        self.assertEqual(result[0].id, "xfer-1")
        self.assertEqual(result[0].username, "testuser")
        self.assertEqual(result[0].size, 52428800)

    def test_enqueue_failure_returns_none(self):
        soularr.slskd.transfers.enqueue.side_effect = Exception("connection error")
        result = soularr.slskd_do_enqueue("user", [{"filename": "f", "size": 1}], "dir")
        self.assertIsNone(result)

    def test_multiple_files(self):
        file_dir = "Music\\Album"
        files = [
            {"filename": file_dir + "\\01 - A.flac", "size": 100},
            {"filename": file_dir + "\\02 - B.flac", "size": 200},
        ]
        soularr.slskd.transfers.enqueue.return_value = True
        soularr.slskd.transfers.get_downloads.return_value = make_download_list([
            make_directory(file_dir, [
                {"filename": file_dir + "\\01 - A.flac", "id": "id1", "size": 100},
                {"filename": file_dir + "\\02 - B.flac", "id": "id2", "size": 200},
            ])
        ])

        result = soularr.slskd_do_enqueue("user", files, file_dir)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], DownloadFile)
        self.assertIsInstance(result[1], DownloadFile)


class TestDownloadStatusFlow(unittest.TestCase):
    """Verify download status polling works with DownloadFile instances."""

    def setUp(self):
        self._orig_slskd = soularr.slskd
        soularr.slskd = MagicMock()

    def tearDown(self):
        soularr.slskd = self._orig_slskd

    def test_status_set_on_download_file(self):
        """slskd_download_status sets .status on DownloadFile instances."""
        f = DownloadFile(filename="track.flac", id="xfer-1",
                         file_dir="dir", username="user", size=100)
        soularr.slskd.transfers.get_download.return_value = DOWNLOAD_STATUS_DONE

        ok = soularr.slskd_download_status([f])

        self.assertTrue(ok)
        self.assertIsNotNone(f.status)
        self.assertEqual(f.status["state"], "Completed, Succeeded")

    def test_downloads_all_done(self):
        """downloads_all_done reads .status on DownloadFile instances."""
        f1 = DownloadFile(filename="a.flac", id="1", file_dir="d", username="u", size=1)
        f2 = DownloadFile(filename="b.flac", id="2", file_dir="d", username="u", size=1)
        f1.status = DOWNLOAD_STATUS_DONE
        f2.status = DOWNLOAD_STATUS_DONE

        done, problems, queued = soularr.downloads_all_done([f1, f2])

        self.assertTrue(done)
        self.assertIsNone(problems)
        self.assertEqual(queued, 0)

    def test_downloads_partial_queued(self):
        f1 = DownloadFile(filename="a.flac", id="1", file_dir="d", username="u", size=1)
        f2 = DownloadFile(filename="b.flac", id="2", file_dir="d", username="u", size=1)
        f1.status = DOWNLOAD_STATUS_DONE
        f2.status = DOWNLOAD_STATUS_QUEUED

        done, problems, queued = soularr.downloads_all_done([f1, f2])

        self.assertFalse(done)
        self.assertEqual(queued, 1)

    def test_downloads_with_errors(self):
        f1 = DownloadFile(filename="a.flac", id="1", file_dir="d", username="u", size=1)
        f1.status = DOWNLOAD_STATUS_FAILED

        done, problems, queued = soularr.downloads_all_done([f1])

        self.assertFalse(done)
        self.assertIsNotNone(problems)
        self.assertEqual(len(problems), 1)


class TestBuildDownloadInfo(unittest.TestCase):
    """Verify _build_download_info reads from DownloadFile instances."""

    def test_extracts_metadata(self):
        entry = GrabListEntry(
            album_id=-1,
            files=[
                DownloadFile(
                    filename="Music\\01 - Track.flac", id="1",
                    file_dir="Music", username="user1", size=100,
                    bitRate=1411000, sampleRate=44100, bitDepth=16,
                    isVariableBitRate=False,
                ),
            ],
            filetype="flac", title="T", artist="A",
            year="2024", mb_release_id="x",
        )

        info = soularr._build_download_info(entry)

        self.assertEqual(info.username, "user1")
        self.assertIn("flac", info.filetype)
        self.assertEqual(info.bitrate, 1411000)
        self.assertEqual(info.sample_rate, 44100)
        self.assertEqual(info.bit_depth, 16)
        self.assertFalse(info.is_vbr)

    def test_handles_missing_metadata(self):
        """Audio metadata is optional — shouldn't crash when absent."""
        entry = GrabListEntry(
            album_id=-1,
            files=[
                DownloadFile(
                    filename="Music\\01 - Track.mp3", id="1",
                    file_dir="Music", username="user1", size=100,
                ),
            ],
            filetype="mp3", title="T", artist="A",
            year="2024", mb_release_id="x",
        )

        info = soularr._build_download_info(entry)

        self.assertEqual(info.username, "user1")
        self.assertIsNone(info.bitrate)
        self.assertIsNone(info.sample_rate)


class TestCancelAndDelete(unittest.TestCase):
    """Verify cancel_and_delete works with DownloadFile instances."""

    def setUp(self):
        self._orig_slskd = soularr.slskd
        self._orig_cfg = soularr.cfg
        soularr.slskd = MagicMock()
        mock_cfg = MagicMock()
        mock_cfg.slskd_download_dir = tempfile.mkdtemp()
        soularr.cfg = mock_cfg

    def tearDown(self):
        soularr.slskd = self._orig_slskd
        soularr.cfg = self._orig_cfg

    def test_cancels_download_files(self):
        files = [
            DownloadFile(filename="track.flac", id="xfer-1",
                         file_dir="Music\\Album", username="user1", size=100),
        ]
        soularr.cancel_and_delete(files)
        soularr.slskd.transfers.cancel_download.assert_called_once_with(
            username="user1", id="xfer-1"
        )


class TestAlbumRecordDictCompat(unittest.TestCase):
    """Verify album_source methods work with both raw dicts and GrabListEntry."""

    def test_get_tracks_with_raw_dict(self):
        """get_tracks receives raw from_db_row() dicts — must use .get()."""
        raw = {"_db_request_id": None, "title": "T"}
        from album_source import DatabaseSource
        source = DatabaseSource.__new__(DatabaseSource)
        # Should return empty list for None request_id, not crash
        result = source.get_tracks(raw)
        self.assertEqual(result, [])

    def test_get_tracks_with_grab_list_entry(self):
        """get_tracks also works with GrabListEntry via bridge."""
        entry = GrabListEntry(
            album_id=-1, files=[], filetype="mp3", title="T", artist="A",
            year="2024", mb_release_id="x", db_request_id=None,
        )
        from album_source import DatabaseSource
        source = DatabaseSource.__new__(DatabaseSource)
        result = source.get_tracks(entry)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
