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
from lib.download import (cancel_and_delete, slskd_download_status,
                          slskd_do_enqueue, downloads_all_done)
from lib.import_dispatch import _build_download_info
from lib.context import SoularrContext


def _make_ctx(cfg=None, slskd=None):
    """Build a test SoularrContext."""
    return SoularrContext(
        cfg=cfg or MagicMock(),
        slskd=slskd or MagicMock(),
        pipeline_db_source=MagicMock(),
    )


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
    """Build a transfer-group directories payload."""
    return {"directories": directories}


# ─── Tests ───────────────────────────────────────────────────────────


class TestRawDictBoundary(unittest.TestCase):
    """Verify that functions receiving raw slskd dicts work with plain dicts,
    not DownloadFile instances."""

    def test_verify_filetype_with_raw_dict(self):
        """verify_filetype receives raw slskd file dicts, not DownloadFile."""
        from lib.quality import verify_filetype
        self.assertTrue(verify_filetype(SEARCH_FILE_FLAC, "flac"))

    def test_verify_filetype_mp3_v0(self):
        from lib.quality import verify_filetype
        self.assertTrue(verify_filetype(SEARCH_FILE_MP3_V0, "mp3 v0"))

    def test_verify_filetype_mp3_320(self):
        from lib.quality import verify_filetype
        self.assertTrue(verify_filetype(SEARCH_FILE_MP3_320, "mp3 320"))

    def test_verify_filetype_rejects_wrong_type(self):
        from lib.quality import verify_filetype
        self.assertFalse(verify_filetype(SEARCH_FILE_FLAC, "mp3"))

    def test_verify_filetype_bitdepth_samplerate(self):
        from lib.quality import verify_filetype
        self.assertTrue(verify_filetype(SEARCH_FILE_FLAC, "flac 16/44.1"))

    def test_verify_filetype_rejects_wrong_bitdepth(self):
        from lib.quality import verify_filetype
        self.assertFalse(verify_filetype(SEARCH_FILE_FLAC, "flac 24/96"))

    def test_album_track_num_with_raw_dicts(self):
        """album_track_num receives raw directory dicts."""
        from lib.quality import parse_filetype_config
        orig_cfg = soularr.cfg
        mock_cfg = MagicMock()
        mock_cfg.allowed_filetypes = ("flac", "mp3")
        mock_cfg.allowed_specs = tuple(parse_filetype_config(s) for s in mock_cfg.allowed_filetypes)
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

    def test_returns_download_files(self):
        """slskd_do_enqueue should return list of DownloadFile, not dicts."""
        ctx = _make_ctx()
        file_dir = "Music\\Artist\\Album"
        files = [{"filename": file_dir + "\\01 - Track.flac", "size": 52428800}]

        ctx.slskd.transfers.enqueue.return_value = True
        ctx.slskd.transfers.get_all_downloads.return_value = [{
            "username": "testuser",
            "directories": make_download_list([
                make_directory(file_dir, [
                    {"filename": file_dir + "\\01 - Track.flac",
                     "id": "xfer-1", "size": 52428800},
                ])
            ])["directories"],
        }]

        result = slskd_do_enqueue("testuser", files, file_dir, ctx)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], DownloadFile)
        self.assertEqual(result[0].filename, file_dir + "\\01 - Track.flac")
        self.assertEqual(result[0].id, "xfer-1")
        self.assertEqual(result[0].username, "testuser")
        self.assertEqual(result[0].size, 52428800)

    def test_enqueue_failure_returns_none(self):
        ctx = _make_ctx()
        ctx.slskd.transfers.enqueue.side_effect = Exception("connection error")
        result = slskd_do_enqueue("user", [{"filename": "f", "size": 1}], "dir", ctx)
        self.assertIsNone(result)

    def test_multiple_files(self):
        ctx = _make_ctx()
        file_dir = "Music\\Album"
        files = [
            {"filename": file_dir + "\\01 - A.flac", "size": 100},
            {"filename": file_dir + "\\02 - B.flac", "size": 200},
        ]
        ctx.slskd.transfers.enqueue.return_value = True
        ctx.slskd.transfers.get_all_downloads.return_value = [{
            "username": "user",
            "directories": make_download_list([
                make_directory(file_dir, [
                    {"filename": file_dir + "\\01 - A.flac", "id": "id1", "size": 100},
                    {"filename": file_dir + "\\02 - B.flac", "id": "id2", "size": 200},
                ])
            ])["directories"],
        }]

        result = slskd_do_enqueue("user", files, file_dir, ctx)
        assert result is not None
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], DownloadFile)
        self.assertIsInstance(result[1], DownloadFile)


class TestDownloadStatusFlow(unittest.TestCase):
    """Verify download status polling works with DownloadFile instances."""

    def test_status_set_on_download_file(self):
        """slskd_download_status sets .status on DownloadFile instances."""
        ctx = _make_ctx()
        f = DownloadFile(filename="track.flac", id="xfer-1",
                         file_dir="dir", username="user", size=100)
        ctx.slskd.transfers.get_download.return_value = DOWNLOAD_STATUS_DONE

        ok = slskd_download_status([f], ctx)

        self.assertTrue(ok)
        self.assertIsNotNone(f.status)
        assert f.status is not None
        self.assertEqual(f.status["state"], "Completed, Succeeded")

    def test_downloads_all_done(self):
        """downloads_all_done reads .status on DownloadFile instances."""
        f1 = DownloadFile(filename="a.flac", id="1", file_dir="d", username="u", size=1)
        f2 = DownloadFile(filename="b.flac", id="2", file_dir="d", username="u", size=1)
        f1.status = DOWNLOAD_STATUS_DONE
        f2.status = DOWNLOAD_STATUS_DONE

        done, problems, queued = downloads_all_done([f1, f2])

        self.assertTrue(done)
        self.assertIsNone(problems)
        self.assertEqual(queued, 0)

    def test_downloads_partial_queued(self):
        f1 = DownloadFile(filename="a.flac", id="1", file_dir="d", username="u", size=1)
        f2 = DownloadFile(filename="b.flac", id="2", file_dir="d", username="u", size=1)
        f1.status = DOWNLOAD_STATUS_DONE
        f2.status = DOWNLOAD_STATUS_QUEUED

        done, problems, queued = downloads_all_done([f1, f2])

        self.assertFalse(done)
        self.assertEqual(queued, 1)

    def test_downloads_with_errors(self):
        f1 = DownloadFile(filename="a.flac", id="1", file_dir="d", username="u", size=1)
        f1.status = DOWNLOAD_STATUS_FAILED

        done, problems, queued = downloads_all_done([f1])

        self.assertFalse(done)
        self.assertIsNotNone(problems)
        assert problems is not None
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

        info = _build_download_info(entry)

        self.assertEqual(info.username, "user1")
        assert info.filetype is not None
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

        info = _build_download_info(entry)

        self.assertEqual(info.username, "user1")
        self.assertIsNone(info.bitrate)
        self.assertIsNone(info.sample_rate)


class TestCancelAndDelete(unittest.TestCase):
    """Verify cancel_and_delete works with DownloadFile instances."""

    def test_cancels_download_files(self):
        mock_cfg = MagicMock()
        mock_cfg.slskd_download_dir = tempfile.mkdtemp()
        ctx = _make_ctx(cfg=mock_cfg)
        files = [
            DownloadFile(filename="track.flac", id="xfer-1",
                         file_dir="Music\\Album", username="user1", size=100),
        ]
        cancel_and_delete(files, ctx)
        ctx.slskd.transfers.cancel_download.assert_called_once_with(
            username="user1", id="xfer-1"
        )


class TestAlbumRecordAttrAccess(unittest.TestCase):
    """Verify album_source methods work with AlbumRecord and GrabListEntry."""

    def test_get_tracks_with_album_record(self):
        """get_tracks receives AlbumRecord — uses .db_request_id attribute."""
        from album_source import AlbumRecord, DatabaseSource, ReleaseRecord
        record = AlbumRecord(
            id=-1, title="T", release_date="2024-01-01T00:00:00Z",
            artist_id=0, artist_name="A", foreign_artist_id="",
            releases=[], db_request_id=0, db_source="request",
            db_mb_release_id="", db_quality_override=None,
        )
        source = DatabaseSource.__new__(DatabaseSource)
        # db_request_id=0 is falsy, should return empty list
        result = source.get_tracks(record)
        self.assertEqual(result, [])

    def test_get_tracks_with_grab_list_entry(self):
        """get_tracks also works with GrabListEntry via getattr."""
        entry = GrabListEntry(
            album_id=-1, files=[], filetype="mp3", title="T", artist="A",
            year="2024", mb_release_id="x", db_request_id=None,
        )
        from album_source import DatabaseSource
        source = DatabaseSource.__new__(DatabaseSource)
        result = source.get_tracks(entry)
        self.assertEqual(result, [])


class TestVersionCheckCaching(unittest.TestCase):
    """Verify slskd.application.version() is called once, not per-browse."""

    def setUp(self):
        from lib.quality import parse_filetype_config
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_folder_cache = soularr.folder_cache
        self._orig_broken_user = soularr.broken_user
        self._orig_album_cache = soularr._current_album_cache

        mock_cfg = MagicMock()
        mock_cfg.allowed_filetypes = ("flac",)
        mock_cfg.allowed_specs = tuple(parse_filetype_config(s) for s in mock_cfg.allowed_filetypes)
        mock_cfg.minimum_match_ratio = 0.5
        mock_cfg.ignored_users = ()
        soularr.cfg = mock_cfg

        self.mock_slskd = MagicMock()
        self.mock_slskd.application.version.return_value = "0.23.0"
        # Return a directory with no matching audio files (won't match)
        self.mock_slskd.users.directory.return_value = [
            make_directory("Music\\Album", [
                {"filename": "01 - Track.flac", "size": 100},
            ])
        ]
        soularr.slskd = self.mock_slskd

        soularr.folder_cache = {}
        soularr.broken_user = []
        soularr._current_album_cache = {}
        # Reset the cached version check
        soularr._slskd_version_gt_0_22_2 = None

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.folder_cache = self._orig_folder_cache
        soularr.broken_user = self._orig_broken_user
        soularr._current_album_cache = self._orig_album_cache
        soularr._slskd_version_gt_0_22_2 = None

    def test_version_called_once_across_multiple_dirs(self):
        """slskd.application.version() should be called once, not per directory."""
        # 3 tracks expected, but each dir has 1 file — no match, forces browsing all dirs
        tracks = [
            {"albumId": 1, "title": "Track One", "mediumNumber": 1},
            {"albumId": 1, "title": "Track Two", "mediumNumber": 1},
            {"albumId": 1, "title": "Track Three", "mediumNumber": 1},
        ]
        soularr._current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        file_dirs = ["Music\\Dir1", "Music\\Dir2", "Music\\Dir3"]
        soularr.check_for_match(tracks, "flac", file_dirs, "testuser")

        # version() should be called exactly once, not 3 times
        self.assertEqual(self.mock_slskd.application.version.call_count, 1)
        # But directory() should be called 3 times (one per dir)
        self.assertEqual(self.mock_slskd.users.directory.call_count, 3)

    def test_version_cached_across_check_for_match_calls(self):
        """Version check should persist across separate check_for_match calls."""
        tracks = [
            {"albumId": 1, "title": "Track One", "mediumNumber": 1},
        ]
        soularr._current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        soularr.check_for_match(tracks, "flac", ["Music\\Dir1"], "user1")
        soularr.check_for_match(tracks, "flac", ["Music\\Dir2"], "user2")

        # Still only 1 version call total
        self.assertEqual(self.mock_slskd.application.version.call_count, 1)

    def test_old_version_cached_as_false(self):
        """Old slskd version (≤ 0.22.2) should be cached as False."""
        self.mock_slskd.application.version.return_value = "0.20.0"
        # Old slskd returns dict directly, not list
        self.mock_slskd.users.directory.return_value = make_directory(
            "Music\\Album", [{"filename": "01 - Track.flac", "size": 100}]
        )
        tracks = [
            {"albumId": 1, "title": "Track One", "mediumNumber": 1},
        ]
        soularr._current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        soularr.check_for_match(tracks, "flac", ["Music\\Dir1", "Music\\Dir2"], "user1")

        self.assertEqual(self.mock_slskd.application.version.call_count, 1)
        self.assertFalse(soularr._slskd_version_gt_0_22_2)


class TestMultiEnqueueNoDeepCopy(unittest.TestCase):
    """Verify try_multi_enqueue works without deepcopy on results."""

    def setUp(self):
        from lib.quality import parse_filetype_config
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_folder_cache = soularr.folder_cache
        self._orig_broken_user = soularr.broken_user
        self._orig_album_cache = soularr._current_album_cache
        self._orig_pdb = soularr.pipeline_db_source

        mock_cfg = MagicMock()
        mock_cfg.allowed_filetypes = ("flac",)
        mock_cfg.allowed_specs = tuple(parse_filetype_config(s) for s in mock_cfg.allowed_filetypes)
        mock_cfg.minimum_match_ratio = 0.5
        mock_cfg.ignored_users = ()
        mock_cfg.download_filtering = False
        soularr.cfg = mock_cfg

        soularr.folder_cache = {}
        soularr.broken_user = []
        soularr._current_album_cache = {}
        soularr._slskd_version_gt_0_22_2 = None

        mock_slskd = MagicMock()
        mock_slskd.application.version.return_value = "0.23.0"
        # Return dirs with 1 file each — won't match multi-disc tracks
        mock_slskd.users.directory.return_value = [
            make_directory("Music\\Disc1", [
                {"filename": "01 - Track.flac", "size": 100},
            ])
        ]
        soularr.slskd = mock_slskd

        # Mock pipeline_db_source for _get_denied_users
        mock_pdb = MagicMock()
        mock_pdb.get_denied_users.return_value = []
        soularr.pipeline_db_source = mock_pdb

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.folder_cache = self._orig_folder_cache
        soularr.broken_user = self._orig_broken_user
        soularr._current_album_cache = self._orig_album_cache
        soularr.pipeline_db_source = self._orig_pdb
        soularr._slskd_version_gt_0_22_2 = None

    def test_results_dict_not_mutated(self):
        """try_multi_enqueue should not mutate the results dict."""
        results = {
            "user1": {
                "flac": ["Music\\Disc1", "Music\\Disc2"],
            }
        }
        # Deep copy to compare later
        import copy
        original_results = copy.deepcopy(results)

        # Mock a release with 2 media
        release = MagicMock()
        media1 = MagicMock()
        media1.medium_number = 1
        media2 = MagicMock()
        media2.medium_number = 2
        release.media = [media1, media2]

        tracks = [
            {"albumId": 1, "title": "Track One", "mediumNumber": 1},
            {"albumId": 1, "title": "Track Two", "mediumNumber": 2},
        ]
        soularr._current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        # Will fail to match (no slskd mock set up), but should not mutate results
        soularr.try_multi_enqueue(release, tracks, results, "flac")

        self.assertEqual(results, original_results)


class TestDeepcopyDeferredToMatch(unittest.TestCase):
    """Verify deepcopy only happens for matched directories, not every lookup."""

    def setUp(self):
        from lib.quality import parse_filetype_config
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_folder_cache = soularr.folder_cache
        self._orig_broken_user = soularr.broken_user
        self._orig_album_cache = soularr._current_album_cache

        mock_cfg = MagicMock()
        mock_cfg.allowed_filetypes = ("flac",)
        mock_cfg.allowed_specs = tuple(parse_filetype_config(s) for s in mock_cfg.allowed_filetypes)
        mock_cfg.minimum_match_ratio = 0.5
        mock_cfg.ignored_users = ()
        mock_cfg.download_filtering = True
        mock_cfg.use_extension_whitelist = True
        mock_cfg.extensions_whitelist = ("jpg",)
        soularr.cfg = mock_cfg

        soularr.folder_cache = {}
        soularr.broken_user = []
        soularr._current_album_cache = {}
        soularr._slskd_version_gt_0_22_2 = True

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.folder_cache = self._orig_folder_cache
        soularr.broken_user = self._orig_broken_user
        soularr._current_album_cache = self._orig_album_cache
        soularr._slskd_version_gt_0_22_2 = None

    def test_folder_cache_not_corrupted_after_download_filter(self):
        """download_filter mutates directory['files'], but folder_cache should be intact."""
        dir_files = [
            {"filename": "01 - Track One.flac", "size": 100},
            {"filename": "cover.jpg", "size": 50},
            {"filename": "info.nfo", "size": 10},
        ]
        # Pre-populate folder_cache (simulating a previous browse)
        soularr.folder_cache["testuser"] = {
            "Music\\Album": {"files": list(dir_files), "directory": "Music\\Album"}
        }

        soularr._current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        # First call: match with 1 track, 1 audio file
        tracks = [{"albumId": 1, "title": "Track One", "mediumNumber": 1}]
        found, directory, file_dir = soularr.check_for_match(
            tracks, "flac", ["Music\\Album"], "testuser"
        )
        self.assertTrue(found)

        # Mutate the returned directory via download_filter (as try_enqueue does)
        soularr.download_filter("flac", directory)

        # folder_cache should still have ALL 3 files (including .nfo)
        cached = soularr.folder_cache["testuser"]["Music\\Album"]
        cached_filenames = [f["filename"] for f in cached["files"]]
        self.assertIn("info.nfo", cached_filenames)
        self.assertEqual(len(cached["files"]), 3)

    def test_second_lookup_after_mutation_still_works(self):
        """A second check_for_match on the same dir should work after first was mutated."""
        dir_files = [
            {"filename": "01 - Track One.flac", "size": 100},
            {"filename": "cover.jpg", "size": 50},
        ]
        soularr.folder_cache["testuser"] = {
            "Music\\Album": {"files": list(dir_files), "directory": "Music\\Album"}
        }

        soularr._current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
        tracks = [{"albumId": 1, "title": "Track One", "mediumNumber": 1}]

        # First match
        found1, dir1, _ = soularr.check_for_match(
            tracks, "flac", ["Music\\Album"], "testuser"
        )
        self.assertTrue(found1)

        # Mutate it
        soularr.download_filter("flac", dir1)

        # Second match on the same cached dir should still succeed
        found2, dir2, _ = soularr.check_for_match(
            tracks, "flac", ["Music\\Album"], "testuser"
        )
        self.assertTrue(found2)
        # And should have both files (not the filtered version)
        self.assertEqual(len(dir2["files"]), 2)


if __name__ == "__main__":
    unittest.main()
