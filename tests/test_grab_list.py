"""Tests for GrabListEntry dataclass — typed replacement for grab_list album dicts.

Covers construction, bridge methods (__getitem__, __setitem__, __contains__, get),
alias mapping for underscore-prefixed keys, and lifecycle simulation matching
the exact dict shapes used in find_download -> monitor_downloads -> process_completed_album.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.grab_list import GrabListEntry, DownloadFile


def _make_entry(**overrides):
    """Helper: construct a minimal GrabListEntry with sensible defaults."""
    defaults = dict(
        album_id=-42,
        files=[DownloadFile(filename="01 - Track.mp3", id="abc", username="user1",
                           file_dir="\\Music\\Album", size=5000000)],
        filetype="mp3",
        title="Test Album",
        artist="Test Artist",
        year="2024",
        mb_release_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    )
    defaults.update(overrides)
    return GrabListEntry(**defaults)  # type: ignore[arg-type]


class TestConstruction(unittest.TestCase):
    """GrabListEntry can be constructed with required fields; optionals default to None."""

    def test_required_fields(self):
        e = _make_entry()
        self.assertEqual(e.album_id, -42)
        self.assertEqual(e.filetype, "mp3")
        self.assertEqual(e.title, "Test Album")
        self.assertEqual(e.artist, "Test Artist")
        self.assertEqual(e.year, "2024")
        self.assertEqual(e.mb_release_id, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        self.assertEqual(len(e.files), 1)

    def test_db_defaults(self):
        e = _make_entry()
        self.assertIsNone(e.db_request_id)
        self.assertIsNone(e.db_source)
        self.assertIsNone(e.db_quality_override)

    def test_monitoring_defaults(self):
        e = _make_entry()
        self.assertIsNone(e.count_start)
        self.assertIsNone(e.rejected_retries)
        self.assertIsNone(e.error_count)

    def test_processing_defaults(self):
        e = _make_entry()
        self.assertIsNone(e.import_folder)
        self.assertIsNone(e.spectral_grade)
        self.assertIsNone(e.spectral_bitrate)
        self.assertIsNone(e.existing_min_bitrate)
        self.assertIsNone(e.existing_spectral_bitrate)

    def test_full_construction(self):
        e = GrabListEntry(
            album_id=-5, files=[], filetype="flac", title="T", artist="A",
            year="2020", mb_release_id="x",
            db_request_id=99, db_source="request", db_quality_override="flac",
            count_start=100.0, rejected_retries=3, error_count=1,
            import_folder="/tmp/test", spectral_grade="genuine",
            spectral_bitrate=320, existing_min_bitrate=240,
            existing_spectral_bitrate=310,
        )
        self.assertEqual(e.db_request_id, 99)
        self.assertEqual(e.spectral_grade, "genuine")


class TestBridgeGetItem(unittest.TestCase):
    """entry["key"] works for both direct field names and underscore aliases."""

    def test_direct_field(self):
        e = _make_entry()
        self.assertEqual(e["artist"], "Test Artist")
        self.assertEqual(e["title"], "Test Album")
        self.assertEqual(e["album_id"], -42)

    def test_alias_db_request_id(self):
        e = _make_entry(db_request_id=77)
        self.assertEqual(e["_db_request_id"], 77)
        self.assertEqual(e["db_request_id"], 77)

    def test_alias_db_source(self):
        e = _make_entry(db_source="request")
        self.assertEqual(e["_db_source"], "request")

    def test_alias_spectral_grade(self):
        e = _make_entry(spectral_grade="suspect")
        self.assertEqual(e["_spectral_grade"], "suspect")

    def test_unknown_key_raises(self):
        e = _make_entry()
        with self.assertRaises(KeyError):
            _ = e["nonexistent"]

    def test_files_accessible(self):
        e = _make_entry()
        self.assertIsInstance(e["files"], list)
        self.assertEqual(len(e["files"]), 1)


class TestBridgeSetItem(unittest.TestCase):
    """entry["key"] = value sets the corresponding attribute."""

    def test_set_import_folder(self):
        e = _make_entry()
        e["import_folder"] = "/mnt/incoming"
        self.assertEqual(e.import_folder, "/mnt/incoming")

    def test_set_via_alias(self):
        e = _make_entry()
        e["_spectral_grade"] = "genuine"
        self.assertEqual(e.spectral_grade, "genuine")

    def test_set_spectral_bitrate_alias(self):
        e = _make_entry()
        e["_spectral_bitrate"] = 256
        self.assertEqual(e.spectral_bitrate, 256)

    def test_set_existing_min_bitrate_alias(self):
        e = _make_entry()
        e["_existing_min_bitrate"] = 192
        self.assertEqual(e.existing_min_bitrate, 192)

    def test_set_existing_spectral_bitrate_alias(self):
        e = _make_entry()
        e["_existing_spectral_bitrate"] = 310
        self.assertEqual(e.existing_spectral_bitrate, 310)

    def test_set_count_start(self):
        e = _make_entry()
        e["count_start"] = 12345.0
        self.assertEqual(e.count_start, 12345.0)

    def test_set_rejected_retries(self):
        e = _make_entry()
        e["rejected_retries"] = 0
        self.assertEqual(e.rejected_retries, 0)

    def test_set_error_count(self):
        e = _make_entry()
        e["error_count"] = 0
        self.assertEqual(e.error_count, 0)

    def test_set_album_id(self):
        """Line 1781: album_data["album_id"] = album_id — must work via bridge."""
        e = _make_entry(album_id=-42)
        e["album_id"] = -42
        self.assertEqual(e.album_id, -42)

    def test_set_unknown_key_raises(self):
        e = _make_entry()
        with self.assertRaises(KeyError):
            e["nonexistent"] = "value"


class TestBridgeContains(unittest.TestCase):
    """'key in entry' returns True only when the field exists and is not None."""

    def test_required_field_always_in(self):
        e = _make_entry()
        self.assertIn("artist", e)
        self.assertIn("title", e)
        self.assertIn("files", e)
        self.assertIn("album_id", e)

    def test_optional_none_not_in(self):
        e = _make_entry()
        self.assertNotIn("count_start", e)
        self.assertNotIn("rejected_retries", e)
        self.assertNotIn("error_count", e)
        self.assertNotIn("import_folder", e)

    def test_optional_set_is_in(self):
        e = _make_entry(count_start=100.0)
        self.assertIn("count_start", e)

    def test_optional_set_via_bridge_is_in(self):
        e = _make_entry()
        e["count_start"] = 100.0
        self.assertIn("count_start", e)

    def test_alias_not_in_when_none(self):
        e = _make_entry()
        self.assertNotIn("_db_request_id", e)
        self.assertNotIn("_spectral_grade", e)

    def test_alias_in_when_set(self):
        e = _make_entry(db_request_id=5)
        self.assertIn("_db_request_id", e)

    def test_unknown_key_not_in(self):
        e = _make_entry()
        self.assertNotIn("nonexistent", e)

class TestBridgeGet(unittest.TestCase):
    """entry.get(key, default) matches dict.get() semantics."""

    def test_get_existing(self):
        e = _make_entry()
        self.assertEqual(e.get("artist"), "Test Artist")

    def test_get_none_returns_none(self):
        e = _make_entry()
        self.assertIsNone(e.get("_db_request_id"))

    def test_get_none_with_default(self):
        e = _make_entry()
        self.assertEqual(e.get("_db_source", "redownload"), "redownload")

    def test_get_unknown_returns_default(self):
        e = _make_entry()
        self.assertEqual(e.get("nonexistent", "fallback"), "fallback")

    def test_get_unknown_returns_none(self):
        e = _make_entry()
        self.assertIsNone(e.get("nonexistent"))

    def test_get_set_value(self):
        e = _make_entry(db_request_id=10)
        self.assertEqual(e.get("_db_request_id"), 10)

    def test_get_files_default(self):
        """album_data.get("files", []) pattern used in _build_download_info."""
        e = _make_entry()
        files = e.get("files", [])
        assert files is not None
        self.assertEqual(len(files), 1)


class TestLifecycle(unittest.TestCase):
    """Simulate the full find_download -> monitor -> process lifecycle."""

    def test_find_download_shape(self):
        """Entry as constructed by find_download (DB mode)."""
        e = GrabListEntry(
            album_id=-99,
            files=[DownloadFile(filename="01.mp3", id="x", username="u",
                               file_dir="\\dir", size=1000)],
            filetype="mp3 v0",
            title="Blue Album",
            artist="Weezer",
            year="1994",
            mb_release_id="abc-123",
            db_request_id=42,
            db_source="request",
        )
        # All find_download consumers work
        self.assertEqual(e["artist"], "Weezer")
        self.assertEqual(e["title"], "Blue Album")
        self.assertEqual(e["filetype"], "mp3 v0")
        self.assertEqual(e.get("_db_request_id"), 42)
        self.assertNotIn("count_start", e)

    def test_monitor_downloads_mutations(self):
        """monitor_downloads adds transient fields."""
        e = _make_entry()
        # count_start pattern (line 1653-1654)
        self.assertNotIn("count_start", e)
        e["count_start"] = 1000.0
        self.assertIn("count_start", e)
        self.assertEqual(e["count_start"], 1000.0)

        # rejected_retries pattern (line 1727-1728)
        self.assertNotIn("rejected_retries", e)
        e["rejected_retries"] = 0
        self.assertIn("rejected_retries", e)
        e["rejected_retries"] += 1
        self.assertEqual(e["rejected_retries"], 1)

        # error_count pattern (line 1787-1789)
        self.assertNotIn("error_count", e)
        e["error_count"] = 0
        e["error_count"] += 1
        self.assertEqual(e["error_count"], 1)

    def test_process_completed_album_mutations(self):
        """process_completed_album mutates spectral and import fields."""
        e = _make_entry(db_request_id=10)
        # album_id already set (was late-set at line 1781)
        self.assertEqual(e["album_id"], -42)
        # import_folder (line 1154)
        e["import_folder"] = "/mnt/virtio/music/incoming"
        self.assertEqual(e.import_folder, "/mnt/virtio/music/incoming")
        # spectral mutations (lines 1248-1272)
        e["_spectral_grade"] = "suspect"
        e["_spectral_bitrate"] = 192
        e["_existing_min_bitrate"] = 240
        e["_existing_spectral_bitrate"] = 310
        self.assertEqual(e.spectral_grade, "suspect")
        self.assertEqual(e.spectral_bitrate, 192)
        self.assertEqual(e.existing_min_bitrate, 240)
        # Reading via .get() (album_source.py pattern)
        self.assertEqual(e.get("_db_request_id"), 10)
        self.assertEqual(e.get("_spectral_grade"), "suspect")



def _make_file(**overrides):
    """Helper: construct a minimal DownloadFile with sensible defaults."""
    defaults = dict(
        filename="\\Music\\Artist\\Album\\01 - Track.mp3",
        id="abc-123",
        file_dir="\\Music\\Artist\\Album",
        username="testuser",
        size=5000000,
    )
    defaults.update(overrides)
    return DownloadFile(**defaults)  # type: ignore[arg-type]


class TestDownloadFileConstruction(unittest.TestCase):
    """DownloadFile can be constructed with required fields; optionals default to None."""

    def test_required_fields(self):
        f = _make_file()
        self.assertEqual(f.filename, "\\Music\\Artist\\Album\\01 - Track.mp3")
        self.assertEqual(f.id, "abc-123")
        self.assertEqual(f.file_dir, "\\Music\\Artist\\Album")
        self.assertEqual(f.username, "testuser")
        self.assertEqual(f.size, 5000000)

    def test_audio_metadata_defaults(self):
        f = _make_file()
        self.assertIsNone(f.bitRate)
        self.assertIsNone(f.sampleRate)
        self.assertIsNone(f.bitDepth)
        self.assertIsNone(f.isVariableBitRate)

    def test_multi_disc_defaults(self):
        f = _make_file()
        self.assertIsNone(f.disk_no)
        self.assertIsNone(f.disk_count)

    def test_transient_defaults(self):
        f = _make_file()
        self.assertIsNone(f.status)
        self.assertIsNone(f.retry)
        self.assertIsNone(f.import_path)

    def test_full_construction(self):
        f = DownloadFile(
            filename="track.flac", id="x", file_dir="\\dir", username="u", size=100,
            bitRate=320000, sampleRate=44100, bitDepth=16, isVariableBitRate=False,
            disk_no=1, disk_count=2,
            status={"state": "Completed, Succeeded"}, retry=3,
            import_path="/tmp/import/track.flac",
        )
        self.assertEqual(f.bitRate, 320000)
        self.assertEqual(f.disk_no, 1)
        assert f.status is not None
        self.assertEqual(f.status["state"], "Completed, Succeeded")
        self.assertEqual(f.import_path, "/tmp/import/track.flac")


class TestDownloadFileBridge(unittest.TestCase):
    """Dict-style access on DownloadFile for backward compat."""

    def test_getitem(self):
        f = _make_file()
        self.assertEqual(f["filename"], f.filename)
        self.assertEqual(f["username"], f.username)
        self.assertEqual(f["size"], 5000000)

    def test_getitem_unknown_raises(self):
        f = _make_file()
        with self.assertRaises(KeyError):
            _ = f["nonexistent"]

    def test_setitem(self):
        f = _make_file()
        f["status"] = {"state": "Completed, Succeeded"}
        assert f.status is not None
        self.assertEqual(f.status["state"], "Completed, Succeeded")

    def test_setitem_retry(self):
        f = _make_file()
        f["retry"] = 0
        f["retry"] += 1
        self.assertEqual(f.retry, 1)

    def test_setitem_import_path(self):
        f = _make_file()
        f["import_path"] = "/tmp/dest.mp3"
        self.assertEqual(f.import_path, "/tmp/dest.mp3")

    def test_setitem_id_on_requeue(self):
        """monitor_downloads reassigns id on requeue."""
        f = _make_file(id="old-id")
        f["id"] = "new-id"
        self.assertEqual(f.id, "new-id")

    def test_contains_required(self):
        f = _make_file()
        self.assertIn("filename", f)
        self.assertIn("username", f)

    def test_contains_optional_none(self):
        f = _make_file()
        self.assertNotIn("retry", f)
        self.assertNotIn("status", f)
        self.assertNotIn("disk_no", f)

    def test_contains_optional_set(self):
        f = _make_file(disk_no=1)
        self.assertIn("disk_no", f)

    def test_get_existing(self):
        f = _make_file()
        self.assertEqual(f.get("username"), "testuser")

    def test_get_optional_none(self):
        f = _make_file()
        self.assertIsNone(f.get("bitRate"))

    def test_get_unknown_default(self):
        f = _make_file()
        self.assertEqual(f.get("nonexistent", "fallback"), "fallback")


class TestDownloadFileLifecycle(unittest.TestCase):
    """Simulate the enqueue → monitor → process lifecycle."""

    def test_enqueue_to_monitor(self):
        """Created in slskd_do_enqueue, then status set by polling."""
        f = _make_file(bitRate=320000, isVariableBitRate=False)
        self.assertNotIn("status", f)
        f["status"] = {"state": "Queued, Locally"}
        self.assertIn("status", f)
        self.assertEqual(f["status"]["state"], "Queued, Locally")

    def test_retry_cycle(self):
        """Error → initialize retry → increment → requeue (new id)."""
        f = _make_file()
        self.assertNotIn("retry", f)
        f["retry"] = 0
        f["retry"] += 1
        self.assertEqual(f["retry"], 1)
        f["id"] = "requeue-id"
        self.assertEqual(f.id, "requeue-id")

    def test_process_completed(self):
        """File move sets import_path, then tags read disk_no."""
        f = _make_file(disk_no=2, disk_count=3)
        f["import_path"] = "/mnt/incoming/Disk 2 - track.mp3"
        self.assertEqual(f.import_path, "/mnt/incoming/Disk 2 - track.mp3")
        self.assertEqual(f.disk_no, 2)
        self.assertEqual(f.disk_count, 3)

    def test_build_download_info_compat(self):
        """_build_download_info reads via .get() and ["filename"].split(".")."""
        f = _make_file(
            filename="\\Music\\01 - Track.flac",
            username="user1",
            bitRate=1000000,
            sampleRate=44100,
            bitDepth=16,
            isVariableBitRate=False,
        )
        # Simulating _build_download_info patterns
        self.assertEqual(f.get("username"), "user1")
        ext = f["filename"].split(".")[-1].lower()
        self.assertEqual(ext, "flac")
        self.assertEqual(f.get("bitRate"), 1000000)
        self.assertFalse(f.get("isVariableBitRate"))

    def test_cancel_and_delete_compat(self):
        """cancel_and_delete reads username, id, file_dir."""
        f = _make_file()
        self.assertEqual(f["username"], "testuser")
        self.assertEqual(f["id"], "abc-123")
        self.assertEqual(f["file_dir"], "\\Music\\Artist\\Album")


if __name__ == "__main__":
    unittest.main()
