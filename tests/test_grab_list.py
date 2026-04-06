"""Tests for GrabListEntry and DownloadFile dataclasses.

Covers construction, attribute access, defaults, and lifecycle simulation
matching the find_download -> poll_active_downloads -> process_completed_album flow.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.grab_list import GrabListEntry, DownloadFile
from lib.quality import SpectralMeasurement


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
        self.assertIsNone(e.db_search_filetype_override)

    def test_processing_defaults(self):
        e = _make_entry()
        self.assertIsNone(e.import_folder)
        self.assertIsNone(e.download_spectral)
        self.assertIsNone(e.current_min_bitrate)
        self.assertIsNone(e.current_spectral)

    def test_full_construction(self):
        e = GrabListEntry(
            album_id=-5, files=[], filetype="flac", title="T", artist="A",
            year="2020", mb_release_id="x",
            db_request_id=99, db_source="request", db_search_filetype_override="flac",
            import_folder="/tmp/test",
            download_spectral=SpectralMeasurement(grade="genuine", bitrate_kbps=320),
            current_min_bitrate=240,
            current_spectral=SpectralMeasurement(grade="genuine", bitrate_kbps=310),
        )
        self.assertEqual(e.db_request_id, 99)
        assert e.download_spectral is not None
        self.assertEqual(e.download_spectral.grade, "genuine")


class TestAttributeAccess(unittest.TestCase):
    """All field access is via attributes — no dict-style access."""

    def test_read_required(self):
        e = _make_entry()
        self.assertEqual(e.artist, "Test Artist")
        self.assertEqual(e.title, "Test Album")
        self.assertEqual(e.album_id, -42)

    def test_read_db_fields(self):
        e = _make_entry(db_request_id=77, db_source="request")
        self.assertEqual(e.db_request_id, 77)
        self.assertEqual(e.db_source, "request")

    def test_write_transient(self):
        e = _make_entry()
        e.import_folder = "/mnt/incoming"
        self.assertEqual(e.import_folder, "/mnt/incoming")

    def test_write_spectral(self):
        e = _make_entry()
        e.download_spectral = SpectralMeasurement(grade="genuine", bitrate_kbps=256)
        e.current_min_bitrate = 192
        e.current_spectral = SpectralMeasurement(grade="genuine", bitrate_kbps=310)
        assert e.download_spectral is not None
        self.assertEqual(e.download_spectral.grade, "genuine")
        self.assertEqual(e.download_spectral.bitrate_kbps, 256)
        self.assertEqual(e.current_min_bitrate, 192)
        assert e.current_spectral is not None
        self.assertEqual(e.current_spectral.bitrate_kbps, 310)

    def test_no_dict_access(self):
        """Dict-style access must raise TypeError — no dual interface."""
        e = _make_entry()
        with self.assertRaises(TypeError):
            _ = e["artist"]  # type: ignore[index]
        with self.assertRaises(TypeError):
            e["artist"] = "X"  # type: ignore[index]


class TestLifecycle(unittest.TestCase):
    """Simulate the full find_download -> poll -> process lifecycle."""

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
        self.assertEqual(e.artist, "Weezer")
        self.assertEqual(e.title, "Blue Album")
        self.assertEqual(e.filetype, "mp3 v0")
        self.assertEqual(e.db_request_id, 42)

    def test_process_completed_album_mutations(self):
        """process_completed_album mutates spectral and import fields."""
        e = _make_entry(db_request_id=10)
        self.assertEqual(e.album_id, -42)
        e.import_folder = "/mnt/virtio/music/incoming"
        self.assertEqual(e.import_folder, "/mnt/virtio/music/incoming")
        e.download_spectral = SpectralMeasurement(grade="suspect", bitrate_kbps=192)
        e.current_min_bitrate = 240
        e.current_spectral = SpectralMeasurement(grade="genuine", bitrate_kbps=310)
        assert e.download_spectral is not None
        self.assertEqual(e.download_spectral.grade, "suspect")
        self.assertEqual(e.download_spectral.bitrate_kbps, 192)
        self.assertEqual(e.current_min_bitrate, 240)
        self.assertEqual(e.db_request_id, 10)


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


class TestDownloadFileAttributeAccess(unittest.TestCase):
    """Attribute access on DownloadFile — no dict-style access."""

    def test_read_fields(self):
        f = _make_file()
        self.assertEqual(f.filename, "\\Music\\Artist\\Album\\01 - Track.mp3")
        self.assertEqual(f.username, "testuser")
        self.assertEqual(f.size, 5000000)

    def test_write_status(self):
        f = _make_file()
        f.status = {"state": "Completed, Succeeded"}
        assert f.status is not None
        self.assertEqual(f.status["state"], "Completed, Succeeded")

    def test_write_retry(self):
        f = _make_file()
        f.retry = 0
        f.retry += 1
        self.assertEqual(f.retry, 1)

    def test_write_import_path(self):
        f = _make_file()
        f.import_path = "/tmp/dest.mp3"
        self.assertEqual(f.import_path, "/tmp/dest.mp3")

    def test_write_id_on_requeue(self):
        """monitor_downloads reassigns id on requeue."""
        f = _make_file(id="old-id")
        f.id = "new-id"
        self.assertEqual(f.id, "new-id")

    def test_no_dict_access(self):
        """Dict-style access must raise TypeError — no dual interface."""
        f = _make_file()
        with self.assertRaises(TypeError):
            _ = f["filename"]  # type: ignore[index]
        with self.assertRaises(TypeError):
            f["status"] = {"state": "test"}  # type: ignore[index]


class TestDownloadFileLifecycle(unittest.TestCase):
    """Simulate the enqueue -> poll -> process lifecycle."""

    def test_enqueue_to_monitor(self):
        """Created in slskd_do_enqueue, then status set by polling."""
        f = _make_file(bitRate=320000, isVariableBitRate=False)
        self.assertIsNone(f.status)
        f.status = {"state": "Queued, Locally"}
        assert f.status is not None
        self.assertEqual(f.status["state"], "Queued, Locally")

    def test_retry_cycle(self):
        """Error -> initialize retry -> increment -> requeue (new id)."""
        f = _make_file()
        self.assertIsNone(f.retry)
        f.retry = 0
        f.retry += 1
        self.assertEqual(f.retry, 1)
        f.id = "requeue-id"
        self.assertEqual(f.id, "requeue-id")

    def test_process_completed(self):
        """File move sets import_path, then tags read disk_no."""
        f = _make_file(disk_no=2, disk_count=3)
        f.import_path = "/mnt/incoming/Disk 2 - track.mp3"
        self.assertEqual(f.import_path, "/mnt/incoming/Disk 2 - track.mp3")
        self.assertEqual(f.disk_no, 2)
        self.assertEqual(f.disk_count, 3)

    def test_build_download_info_compat(self):
        """_build_download_info reads attributes."""
        f = _make_file(
            filename="\\Music\\01 - Track.flac",
            username="user1",
            bitRate=1000000,
            sampleRate=44100,
            bitDepth=16,
            isVariableBitRate=False,
        )
        self.assertEqual(f.username, "user1")
        ext = f.filename.split(".")[-1].lower()
        self.assertEqual(ext, "flac")
        self.assertEqual(f.bitRate, 1000000)
        self.assertFalse(f.isVariableBitRate)

    def test_cancel_and_delete_compat(self):
        """cancel_and_delete reads username, id, file_dir."""
        f = _make_file()
        self.assertEqual(f.username, "testuser")
        self.assertEqual(f.id, "abc-123")
        self.assertEqual(f.file_dir, "\\Music\\Artist\\Album")


if __name__ == "__main__":
    unittest.main()
