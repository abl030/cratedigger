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
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch, PropertyMock

if TYPE_CHECKING:
    from lib.config import SoularrConfig

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


def _make_ctx(cfg=None, slskd=None, pipeline_db_source=None, **cache_overrides):
    """Build a test SoularrContext."""
    return SoularrContext(
        cfg=cfg or MagicMock(),
        slskd=slskd or MagicMock(),
        pipeline_db_source=pipeline_db_source or MagicMock(),
        **cache_overrides,
    )


def _make_matching_cfg(
    allowed_filetypes: tuple[str, ...] = ("flac",),
    minimum_match_ratio: float = 0.5,
    ignored_users: tuple[str, ...] = (),
    browse_parallelism: int = 4,
    download_filtering: bool = False,
    use_extension_whitelist: bool = False,
    extensions_whitelist: tuple[str, ...] = (),
    **extra,
) -> "SoularrConfig":
    """Build a SoularrConfig with matching-friendly defaults.

    Uses the real frozen dataclass so tests break immediately when new
    required fields are added, rather than silently passing via MagicMock.
    """
    from lib.config import SoularrConfig
    return SoularrConfig(
        allowed_filetypes=allowed_filetypes,
        minimum_match_ratio=minimum_match_ratio,
        ignored_users=ignored_users,
        browse_parallelism=browse_parallelism,
        download_filtering=download_filtering,
        use_extension_whitelist=use_extension_whitelist,
        extensions_whitelist=extensions_whitelist,
        **extra,
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


def make_tracks(*track_defs: tuple[int, str, int]) -> list["soularr.TrackRecord"]:
    """Build a list of TrackRecord dicts from (albumId, title, mediumNumber) tuples."""
    return [{"albumId": a, "title": t, "mediumNumber": m} for a, t, m in track_defs]  # type: ignore[misc]


def make_directory(dir_path: str, files: list[dict[str, object]]) -> "soularr.SlskdDirectory":
    """Build a directory dict as slskd.users.directory() returns it."""
    return {
        "directory": dir_path,
        "files": files,  # type: ignore[typeddict-item]  # test dicts are structurally compatible
    }


def make_download_list(directories):
    """Build a transfer-group directories payload."""
    return {"directories": directories}


# ─── Tests ───────────────────────────────────────────────────────────


class TestBuildSearchCache(unittest.TestCase):
    """Direct tests for _build_search_cache — the pure cache-building function."""

    def _specs(self, *filetypes):
        from lib.quality import parse_filetype_config
        return [(ft, parse_filetype_config(ft)) for ft in filetypes]

    def test_basic_flac(self):
        """FLAC files should be cached under their username and filetype."""
        results = [make_search_result("user1", [
            {"filename": "Music\\Album\\01.flac", "size": 100},
            {"filename": "Music\\Album\\02.flac", "size": 100},
        ])]
        entries, speeds, counts = soularr._build_search_cache(results, self._specs("flac"))
        self.assertIn("user1", entries)
        self.assertIn("flac", entries["user1"])
        self.assertEqual(entries["user1"]["flac"], ["Music\\Album"])
        self.assertEqual(counts["user1"]["Music\\Album"], 2)

    def test_upload_speed_max(self):
        """Fastest upload speed per user should be kept."""
        results = [
            make_search_result("user1", [
                {"filename": "A\\01.flac", "size": 100},
            ], upload_speed=1000),
            make_search_result("user1", [
                {"filename": "B\\01.flac", "size": 100},
            ], upload_speed=5000),
        ]
        _, speeds, _ = soularr._build_search_cache(results, self._specs("flac"))
        self.assertEqual(speeds["user1"], 5000)

    def test_multiple_filetypes(self):
        """Files should be categorized into separate filetype buckets."""
        results = [make_search_result("user1", [
            {"filename": "A\\01.flac", "size": 100, "bitRate": 1411,
             "sampleRate": 44100, "bitDepth": 16, "isVariableBitRate": False},
            {"filename": "A\\01.mp3", "size": 50, "bitRate": 245,
             "sampleRate": 44100, "bitDepth": 0, "isVariableBitRate": True},
        ])]
        entries, _, _ = soularr._build_search_cache(
            results, self._specs("flac", "mp3 v0")
        )
        self.assertIn("flac", entries["user1"])
        self.assertIn("mp3 v0", entries["user1"])

    def test_non_audio_ignored(self):
        """Non-audio files should not be counted or cached."""
        results = [make_search_result("user1", [
            {"filename": "A\\cover.jpg", "size": 50},
        ])]
        entries, _, counts = soularr._build_search_cache(results, self._specs("flac"))
        # User entry created but no filetypes
        self.assertEqual(entries.get("user1", {}), {})

    def test_empty_results(self):
        """Empty search results should return empty dicts."""
        entries, speeds, counts = soularr._build_search_cache([], self._specs("flac"))
        self.assertEqual(entries, {})
        self.assertEqual(speeds, {})
        self.assertEqual(counts, {})

    def test_dedup_dirs(self):
        """Same directory from multiple files should appear once."""
        results = [make_search_result("user1", [
            {"filename": "A\\01.flac", "size": 100},
            {"filename": "A\\02.flac", "size": 100},
            {"filename": "A\\03.flac", "size": 100},
        ])]
        entries, _, counts = soularr._build_search_cache(results, self._specs("flac"))
        self.assertEqual(entries["user1"]["flac"], ["A"])
        self.assertEqual(counts["user1"]["A"], 3)


class TestGetUserDirs(unittest.TestCase):
    """Direct tests for _get_user_dirs — candidate directory selection."""

    def test_specific_filetype(self):
        """Should return dirs for the exact filetype requested."""
        results = {"flac": ["Dir1", "Dir2"], "mp3 v0": ["Dir3"]}
        self.assertEqual(soularr._get_user_dirs(results, "flac"), ["Dir1", "Dir2"])

    def test_missing_filetype_returns_none(self):
        """Should return None when the user has no dirs for that filetype."""
        results = {"flac": ["Dir1"]}
        self.assertIsNone(soularr._get_user_dirs(results, "mp3 v0"))

    def test_catch_all_merges_all(self):
        """Catch-all '*' should merge dirs from all filetypes, deduped."""
        results = {"flac": ["Dir1", "Dir2"], "mp3 v0": ["Dir2", "Dir3"]}
        dirs = soularr._get_user_dirs(results, "*")
        self.assertEqual(dirs, ["Dir1", "Dir2", "Dir3"])

    def test_catch_all_empty_returns_none(self):
        """Catch-all with no dirs should return None."""
        results = {"flac": [], "mp3 v0": []}
        self.assertIsNone(soularr._get_user_dirs(results, "*"))

    def test_catch_all_single_filetype(self):
        """Catch-all with one filetype should return those dirs."""
        results = {"flac": ["Dir1"]}
        self.assertEqual(soularr._get_user_dirs(results, "*"), ["Dir1"])


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
        orig_cfg = soularr.cfg
        soularr.cfg = _make_matching_cfg(allowed_filetypes=("flac", "mp3"))
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
        """download_filter returns a new dict without mutating the input."""
        orig_cfg = soularr.cfg
        soularr.cfg = _make_matching_cfg(
            download_filtering=True,
            use_extension_whitelist=True,
            extensions_whitelist=("jpg", "txt"),
        )
        try:
            directory = make_directory("Music\\Album", [
                {"filename": "01 - Track.flac", "size": 100},
                {"filename": "cover.jpg", "size": 50},
                {"filename": "info.nfo", "size": 10},
            ])
            filtered = soularr.download_filter("flac", directory)
            filenames = [f["filename"] for f in filtered["files"]]
            self.assertIn("01 - Track.flac", filenames)
            self.assertIn("cover.jpg", filenames)
            self.assertNotIn("info.nfo", filenames)
            # Original should be unchanged
            self.assertEqual(len(directory["files"]), 3)
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
        ctx = _make_ctx(cfg=_make_matching_cfg(slskd_download_dir=tempfile.mkdtemp()))
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



class TestMultiEnqueueNoDeepCopy(unittest.TestCase):
    """Verify try_multi_enqueue works without deepcopy on results."""

    def setUp(self):
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_pdb = soularr.pipeline_db_source

        mock_cfg = _make_matching_cfg()
        soularr.cfg = mock_cfg

        mock_slskd = MagicMock()
        mock_slskd.users.directory.return_value = [
            make_directory("Music\\Disc1", [
                {"filename": "01 - Track.flac", "size": 100},
            ])
        ]
        soularr.slskd = mock_slskd

        mock_pdb = MagicMock()
        mock_pdb.get_denied_users.return_value = []
        soularr.pipeline_db_source = mock_pdb

        self.ctx = _make_ctx(cfg=mock_cfg, slskd=mock_slskd, pipeline_db_source=mock_pdb)

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.pipeline_db_source = self._orig_pdb

    def test_results_dict_not_mutated(self):
        """try_multi_enqueue should not mutate the results dict."""
        results = {
            "user1": {
                "flac": ["Music\\Disc1", "Music\\Disc2"],
            }
        }
        import copy
        original_results = copy.deepcopy(results)

        release = MagicMock()
        media1 = MagicMock()
        media1.medium_number = 1
        media2 = MagicMock()
        media2.medium_number = 2
        release.media = [media1, media2]

        tracks = make_tracks((1, "Track One", 1), (1, "Track Two", 2))
        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        soularr.try_multi_enqueue(release, tracks, results, "flac", self.ctx)

        self.assertEqual(results, original_results)


class TestDeepcopyDeferredToMatch(unittest.TestCase):
    """Verify deepcopy only happens for matched directories, not every lookup."""

    def setUp(self):
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_pdb = soularr.pipeline_db_source

        mock_cfg = _make_matching_cfg(
            download_filtering=True,
            use_extension_whitelist=True,
            extensions_whitelist=("jpg",),
        )
        soularr.cfg = mock_cfg

        mock_slskd = MagicMock()
        soularr.slskd = mock_slskd

        mock_pdb = MagicMock()
        mock_pdb.get_denied_users.return_value = []
        soularr.pipeline_db_source = mock_pdb

        self.ctx = _make_ctx(cfg=mock_cfg, slskd=mock_slskd, pipeline_db_source=mock_pdb)

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.pipeline_db_source = self._orig_pdb

    def test_folder_cache_not_corrupted_after_download_filter(self):
        """download_filter returns a new dict — folder_cache should be intact."""
        dir_files = [
            {"filename": "01 - Track One.flac", "size": 100},
            {"filename": "cover.jpg", "size": 50},
            {"filename": "info.nfo", "size": 10},
        ]
        # Pre-populate folder_cache (simulating a previous browse)
        self.ctx.folder_cache["testuser"] = {
            "Music\\Album": {"files": list(dir_files), "directory": "Music\\Album"}
        }

        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        # First call: match with 1 track, 1 audio file
        tracks = make_tracks((1, "Track One", 1))
        found, directory, file_dir = soularr.check_for_match(
            tracks, "flac", ["Music\\Album"], "testuser", self.ctx
        )
        self.assertTrue(found)

        # Mutate the returned directory via download_filter (as try_enqueue does)
        soularr.download_filter("flac", directory)

        # folder_cache should still have ALL 3 files (including .nfo)
        cached = self.ctx.folder_cache["testuser"]["Music\\Album"]
        cached_filenames = [f["filename"] for f in cached["files"]]
        self.assertIn("info.nfo", cached_filenames)
        self.assertEqual(len(cached["files"]), 3)

    def test_second_lookup_after_mutation_still_works(self):
        """A second check_for_match on the same dir should work after first was mutated."""
        dir_files = [
            {"filename": "01 - Track One.flac", "size": 100},
            {"filename": "cover.jpg", "size": 50},
        ]
        self.ctx.folder_cache["testuser"] = {
            "Music\\Album": {"files": list(dir_files), "directory": "Music\\Album"}
        }

        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
        tracks = make_tracks((1, "Track One", 1))

        # First match
        found1, dir1, _ = soularr.check_for_match(
            tracks, "flac", ["Music\\Album"], "testuser", self.ctx
        )
        self.assertTrue(found1)

        # Mutate it
        soularr.download_filter("flac", dir1)

        # Second match on the same cached dir should still succeed
        found2, dir2, _ = soularr.check_for_match(
            tracks, "flac", ["Music\\Album"], "testuser", self.ctx
        )
        self.assertTrue(found2)
        # And should have both files (not the filtered version)
        self.assertEqual(len(dir2["files"]), 2)


class TestNegativeMatchCache(unittest.TestCase):
    """Verify negative match cache prevents re-evaluating known mismatches."""

    def setUp(self):
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_pdb = soularr.pipeline_db_source

        mock_cfg = _make_matching_cfg()
        soularr.cfg = mock_cfg

        mock_slskd = MagicMock()
        soularr.slskd = mock_slskd

        mock_pdb = MagicMock()
        mock_pdb.get_denied_users.return_value = []
        soularr.pipeline_db_source = mock_pdb

        self.ctx = _make_ctx(cfg=mock_cfg, slskd=mock_slskd, pipeline_db_source=mock_pdb)

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.pipeline_db_source = self._orig_pdb

    def test_same_dir_same_track_count_skipped(self):
        """A dir that failed matching should be skipped on retry with same track count."""
        # Pre-populate cache with a dir that has 1 file (won't match 3 tracks)
        self.ctx.folder_cache["user1"] = {
            "Music\\Album": {"files": [{"filename": "01.flac", "size": 100}], "directory": "Music\\Album"}
        }
        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        tracks = make_tracks(
            (1, "Track One", 1),
            (1, "Track Two", 1),
            (1, "Track Three", 1),
        )

        # First call — misses (1 file vs 3 tracks)
        found1, _, _ = soularr.check_for_match(tracks, "flac", ["Music\\Album"], "user1", self.ctx)
        self.assertFalse(found1)

        # Negative cache should contain (user1, Music\Album, 3, flac)
        self.assertIn(("user1", "Music\\Album", 3, "flac"), self.ctx.negative_matches)

        # Second call with same track count — should skip (no album_track_num re-eval)
        # We verify by checking that the negative cache hit prevents redundant work
        found2, _, _ = soularr.check_for_match(tracks, "flac", ["Music\\Album"], "user1", self.ctx)
        self.assertFalse(found2)

    def test_same_dir_different_filetype_not_skipped(self):
        """A dir cached as negative for 'flac' should still be tried for '*'."""
        self.ctx.folder_cache["user1"] = {
            "Music\\Album": {
                "files": [{"filename": "01 - Track One.mp3", "size": 100}],
                "directory": "Music\\Album",
            }
        }
        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
        tracks = make_tracks((1, "Track One", 1))

        # Fails for "flac" (file is .mp3, album_track_num won't count it as flac)
        soularr.check_for_match(tracks, "flac", ["Music\\Album"], "user1", self.ctx)
        self.assertIn(("user1", "Music\\Album", 1, "flac"), self.ctx.negative_matches)

        # Should NOT be skipped for "*" (different filetype key)
        self.assertNotIn(("user1", "Music\\Album", 1, "*"), self.ctx.negative_matches)

    def test_same_dir_different_track_count_not_skipped(self):
        """A dir that failed for 3 tracks should still be tried for 1 track."""
        # Dir has 1 audio file
        self.ctx.folder_cache["user1"] = {
            "Music\\Album": {"files": [{"filename": "01 - Track One.flac", "size": 100}], "directory": "Music\\Album"}
        }
        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        # Fail with 3 tracks
        tracks_3 = make_tracks(
            (1, "Track One", 1),
            (1, "Track Two", 1),
            (1, "Track Three", 1),
        )
        soularr.check_for_match(tracks_3, "flac", ["Music\\Album"], "user1", self.ctx)

        # Now try with 1 track — should NOT be skipped (different track count)
        tracks_1 = make_tracks((1, "Track One", 1))
        found, _, _ = soularr.check_for_match(tracks_1, "flac", ["Music\\Album"], "user1", self.ctx)
        self.assertTrue(found)  # 1 file matches 1 track


class TestSearchResultPreFiltering(unittest.TestCase):
    """Verify directories with wrong audio file count are skipped before browsing."""

    def setUp(self):
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_pdb = soularr.pipeline_db_source

        mock_cfg = _make_matching_cfg()
        soularr.cfg = mock_cfg

        self.mock_slskd = MagicMock()
        soularr.slskd = self.mock_slskd

        mock_pdb = MagicMock()
        mock_pdb.get_denied_users.return_value = []
        soularr.pipeline_db_source = mock_pdb

        self.ctx = _make_ctx(cfg=mock_cfg, slskd=self.mock_slskd, pipeline_db_source=mock_pdb)

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.pipeline_db_source = self._orig_pdb

    def test_dir_with_wrong_count_skipped_before_browse(self):
        """Directory with 3 audio files should be skipped when we need 12 tracks."""
        # Search metadata says this dir has 3 audio files
        self.ctx.search_dir_audio_count = {
            "user1": {"Music\\Album": 3}
        }
        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        tracks: list[soularr.TrackRecord] = [{"albumId": 1, "title": f"Track {i}", "mediumNumber": 1} for i in range(12)]  # type: ignore[misc]
        soularr.check_for_match(tracks, "flac", ["Music\\Album"], "user1", self.ctx)

        # Should NOT have called slskd.users.directory — skipped before browse
        self.mock_slskd.users.directory.assert_not_called()

    def test_dir_with_close_count_not_skipped(self):
        """Directory with 13 audio files should NOT be skipped for 12 tracks (tolerance +-2)."""
        self.ctx.search_dir_audio_count = {
            "user1": {"Music\\Album": 13}
        }
        # Set up directory return for when it does browse
        self.mock_slskd.users.directory.return_value = [
            make_directory("Music\\Album", [
                {"filename": f"0{i} - Track.flac", "size": 100} for i in range(13)
            ])
        ]
        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        tracks: list[soularr.TrackRecord] = [{"albumId": 1, "title": f"Track {i}", "mediumNumber": 1} for i in range(12)]  # type: ignore[misc]
        soularr.check_for_match(tracks, "flac", ["Music\\Album"], "user1", self.ctx)

        # SHOULD have browsed — count is close enough
        self.mock_slskd.users.directory.assert_called_once()

    def test_dir_without_metadata_not_skipped(self):
        """Directory with no search metadata should still be browsed."""
        # ctx.search_dir_audio_count is empty by default
        self.mock_slskd.users.directory.return_value = [
            make_directory("Music\\Album", [{"filename": "01.flac", "size": 100}])
        ]
        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")

        tracks: list[soularr.TrackRecord] = [{"albumId": 1, "title": f"Track {i}", "mediumNumber": 1} for i in range(12)]  # type: ignore[misc]
        soularr.check_for_match(tracks, "flac", ["Music\\Album"], "user1", self.ctx)

        # Should browse — no metadata to pre-filter
        self.mock_slskd.users.directory.assert_called_once()


class TestRankCandidateDirs(unittest.TestCase):
    """Verify candidate directory ranking promotes good paths and demotes bad ones."""

    def test_album_name_in_path_promoted(self):
        dirs = [
            "Music\\Various\\Collection",
            "Music\\Artist\\The Album Name",
            "Music\\Other\\Random",
        ]
        ranked = soularr.rank_candidate_dirs(dirs, "The Album Name", "Artist")
        self.assertEqual(ranked[0], "Music\\Artist\\The Album Name")

    def test_artist_name_in_path_promoted(self):
        dirs = [
            "Music\\Other\\Album",
            "Music\\Artist\\Album",
        ]
        ranked = soularr.rank_candidate_dirs(dirs, "Album", "Artist")
        self.assertEqual(ranked[0], "Music\\Artist\\Album")

    def test_penalty_keywords_demoted(self):
        dirs = [
            "Music\\Artist\\Best Of Artist",
            "Music\\Artist\\The Real Album",
            "Music\\Artist\\Greatest Hits",
        ]
        ranked = soularr.rank_candidate_dirs(dirs, "The Real Album", "Artist")
        self.assertEqual(ranked[0], "Music\\Artist\\The Real Album")
        # Best Of and Greatest Hits should be last
        penalty_dirs = ranked[1:]
        self.assertTrue(all(
            any(kw in d.lower() for kw in ["best of", "greatest hits"])
            for d in penalty_dirs
        ))

    def test_discography_demoted(self):
        dirs = [
            "Music\\Artist\\Discography\\Album",
            "Music\\Artist\\Album",
        ]
        ranked = soularr.rank_candidate_dirs(dirs, "Album", "Artist")
        self.assertEqual(ranked[0], "Music\\Artist\\Album")

    def test_case_insensitive(self):
        dirs = [
            "music\\other\\random",
            "MUSIC\\ARTIST\\THE ALBUM",
        ]
        ranked = soularr.rank_candidate_dirs(dirs, "The Album", "Artist")
        self.assertEqual(ranked[0], "MUSIC\\ARTIST\\THE ALBUM")

    def test_preserves_order_for_equal_scores(self):
        """Dirs with equal scores should preserve original order."""
        dirs = ["Music\\Dir1", "Music\\Dir2", "Music\\Dir3"]
        ranked = soularr.rank_candidate_dirs(dirs, "Unrelated", "Nobody")
        self.assertEqual(ranked, dirs)


class TestParallelDirectoryBrowsing(unittest.TestCase):
    """Verify parallel directory browsing populates folder_cache correctly."""

    def setUp(self):
        self._orig_cfg = soularr.cfg
        self._orig_slskd = soularr.slskd
        self._orig_pdb = soularr.pipeline_db_source

        mock_cfg = _make_matching_cfg()
        soularr.cfg = mock_cfg

        self.mock_slskd = MagicMock()
        soularr.slskd = self.mock_slskd

        mock_pdb = MagicMock()
        mock_pdb.get_denied_users.return_value = []
        soularr.pipeline_db_source = mock_pdb

        self.ctx = _make_ctx(cfg=mock_cfg, slskd=self.mock_slskd, pipeline_db_source=mock_pdb)

    def tearDown(self):
        soularr.cfg = self._orig_cfg
        soularr.slskd = self._orig_slskd
        soularr.pipeline_db_source = self._orig_pdb

    def test_parallel_browse_populates_cache(self):
        """_browse_directories should populate folder_cache for all dirs."""
        def mock_directory(username, directory):
            return [make_directory(directory, [
                {"filename": "01 - Track.flac", "size": 100}
            ])]

        self.mock_slskd.users.directory.side_effect = mock_directory

        dirs_to_browse = ["Music\\Dir1", "Music\\Dir2", "Music\\Dir3"]
        results = soularr._browse_directories(dirs_to_browse, "user1", max_workers=2)

        self.assertEqual(len(results), 3)
        self.assertIn("Music\\Dir1", results)
        self.assertIn("Music\\Dir2", results)
        self.assertIn("Music\\Dir3", results)

    def test_parallel_browse_handles_failures(self):
        """Failed browses should be collected, not crash."""
        call_count = [0]

        def mock_directory(username, directory):
            call_count[0] += 1
            if "Dir2" in directory:
                raise Exception("Peer offline")
            return [make_directory(directory, [
                {"filename": "01 - Track.flac", "size": 100}
            ])]

        self.mock_slskd.users.directory.side_effect = mock_directory

        dirs_to_browse = ["Music\\Dir1", "Music\\Dir2", "Music\\Dir3"]
        results = soularr._browse_directories(dirs_to_browse, "user1", max_workers=2)

        # Dir1 and Dir3 should succeed, Dir2 should fail
        self.assertEqual(len(results), 2)
        self.assertIn("Music\\Dir1", results)
        self.assertNotIn("Music\\Dir2", results)
        self.assertIn("Music\\Dir3", results)

    def test_browse_all_fail_marks_broken(self):
        """If all browses for a user fail, they should be marked broken."""
        self.mock_slskd.users.directory.side_effect = Exception("Peer gone")

        self.ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
        tracks = make_tracks((1, "Track One", 1))

        found, _, _ = soularr.check_for_match(
            tracks, "flac", ["Music\\Dir1", "Music\\Dir2"], "user1", self.ctx
        )
        self.assertFalse(found)
        self.assertIn("user1", self.ctx.broken_user)


if __name__ == "__main__":
    unittest.main()
