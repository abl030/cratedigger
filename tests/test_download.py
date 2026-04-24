"""Tests for lib/download.py — download processing functions.

Tests _build_download_info, cancel_and_delete, slskd_download_status,
downloads_all_done, poll_active_downloads, grab_most_wanted.

Pre-import gate behavior (audio integrity + spectral transcode detection)
is shared with the force/manual import paths and tested against
``lib.preimport.run_preimport_gates`` in ``tests/test_force_import_gates.py``
and ``tests/test_integration_slices.py::TestSpectralPropagationSlice``.
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, cast

from tests.helpers import (
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
)
from tests.fakes import FakePipelineDB, FakeSlskdAPI


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_transfer_mock(filename="01 - Track.mp3", username="user1",
                        bitRate=320, sampleRate=44100, bitDepth=None,
                        isVariableBitRate=None, file_dir="user1\\Music",
                        size=5000000, bytes_transferred=None, last_state=None):
    """Build a mock slskd transfer object with runtime state attributes.

    Use this ONLY for tests that need runtime attributes like status,
    bytes_transferred, last_state, import_path — these don't exist on
    the real DownloadFile dataclass. For tests that only need DownloadFile
    fields, use make_download_file() from tests.helpers instead.
    """
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
    f.bytes_transferred = bytes_transferred
    f.last_state = last_state
    f.import_path = None
    f.disk_no = None
    f.disk_count = None
    return f


def _make_ctx(cfg=None, slskd=None, pipeline_db_source=None):
    """Build a mock CratediggerContext."""
    from lib.context import CratediggerContext
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
    return CratediggerContext(cfg=cfg, slskd=slskd,
                          pipeline_db_source=pipeline_db_source)


class TestBuildDownloadInfo(unittest.TestCase):

    def test_basic(self):
        from lib.import_dispatch import _build_download_info
        files = [make_download_file(bitRate=320, sampleRate=44100)]
        album = make_grab_list_entry(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "user1")
        self.assertEqual(dl.filetype, "mp3")
        self.assertEqual(dl.bitrate, 320)
        self.assertEqual(dl.sample_rate, 44100)

    def test_empty_files(self):
        from lib.import_dispatch import _build_download_info
        album = make_grab_list_entry(files=[])
        dl = _build_download_info(album)
        self.assertIsNone(dl.username)
        self.assertIsNone(dl.filetype)

    def test_multi_user(self):
        from lib.import_dispatch import _build_download_info
        files = [
            make_download_file(username="beta_user"),
            make_download_file(username="alpha_user"),
        ]
        album = make_grab_list_entry(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "alpha_user, beta_user")


## TestGatherSpectralContext and TestCheckQualityGateDecision removed:
## - TestGatherSpectralContext never called the function it claimed to test —
##   it reimplemented the condition logic in test code and asserted on that.
## - TestCheckQualityGateDecision duplicated tests already in
##   test_quality_decisions.py::TestQualityGateDecision.


# === NEW tests for functions moving to lib/download.py ===

class TestDownloadsAllDone(unittest.TestCase):
    """downloads_all_done is pure logic — test all branches."""

    def test_all_succeeded(self):
        from lib.download import downloads_all_done
        files = [_make_transfer_mock(), _make_transfer_mock()]
        files[0].status = {"state": "Completed, Succeeded"}
        files[1].status = {"state": "Completed, Succeeded"}
        done, problems, queued = downloads_all_done(files)
        self.assertTrue(done)
        self.assertIsNone(problems)
        self.assertEqual(queued, 0)

    def test_one_errored(self):
        from lib.download import downloads_all_done
        files = [_make_transfer_mock(), _make_transfer_mock()]
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
        files = [_make_transfer_mock(), _make_transfer_mock()]
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
            files = [_make_transfer_mock()]
            files[0].status = {"state": state}
            done, problems, _ = downloads_all_done(files)
            self.assertFalse(done, f"state={state} should not be done")
            self.assertIsNotNone(problems, f"state={state} should be a problem")

    def test_none_status_skipped(self):
        from lib.download import downloads_all_done
        files = [_make_transfer_mock()]
        files[0].status = None
        done, problems, queued = downloads_all_done(files)
        # None status means we can't confirm done
        self.assertTrue(done)  # loop body skips None
        self.assertIsNone(problems)


class TestCancelAndDelete(unittest.TestCase):
    """cancel_and_delete uses ctx.slskd and ctx.cfg."""

    def test_cancels_and_removes_dir(self):
        from lib.download import cancel_and_delete
        slskd = FakeSlskdAPI()
        ctx = _make_ctx(slskd=slskd)
        f = make_download_file(file_dir="someuser\\Album Folder")
        with patch("os.path.isdir", return_value=True), \
             patch("shutil.rmtree") as mock_rm:
            cancel_and_delete([f], ctx)
        self.assertEqual(
            [(call.username, call.id)
             for call in slskd.transfers.cancel_download_calls],
            [("user1", "file-id-1")],
        )
        mock_rm.assert_called_once_with(
            os.path.join("/tmp/test_downloads", "Album Folder"))

    def test_cancel_failure_continues(self):
        """Should not raise if cancel_download throws."""
        from lib.download import cancel_and_delete
        slskd = FakeSlskdAPI()
        slskd.transfers.cancel_download_error = Exception("network error")
        ctx = _make_ctx(slskd=slskd)
        f = make_download_file()
        with self.assertLogs("cratedigger", level="WARNING") as logs, \
             patch("os.path.isdir", return_value=False):
            cancel_and_delete([f], ctx)  # should not raise
        self.assertIn("Failed to cancel download", "\n".join(logs.output))
        self.assertEqual(
            [(call.username, call.id)
             for call in slskd.transfers.cancel_download_calls],
            [("user1", "file-id-1")],
        )


class TestSlskdDownloadStatus(unittest.TestCase):

    def test_populates_status(self):
        from lib.download import slskd_download_status
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="user1",
            directory="user1\\Music",
            filename="01 - Track.mp3",
            id="file-id-1",
            state="Completed, Succeeded",
        )
        ctx = _make_ctx(slskd=slskd)
        f = make_download_file(id="file-id-1")
        ok = slskd_download_status([f], ctx)
        self.assertTrue(ok)
        self.assertIsNotNone(f.status)
        assert f.status is not None
        self.assertEqual(f.status["state"], "Completed, Succeeded")
        self.assertEqual(slskd.transfers.get_download_calls, [("user1", "file-id-1")])

    def test_error_sets_none(self):
        from lib.download import slskd_download_status
        slskd = FakeSlskdAPI()
        slskd.transfers.get_download_error = Exception("fail")
        ctx = _make_ctx(slskd=slskd)
        f = make_download_file(id="file-id-1")
        ok = slskd_download_status([f], ctx)
        self.assertFalse(ok)
        self.assertIsNone(f.status)

    def test_bulk_snapshot_populates_status(self):
        """When snapshot is provided, use match_transfer instead of per-file API."""
        from lib.download import slskd_download_status
        slskd = FakeSlskdAPI()
        ctx = _make_ctx(slskd=slskd)
        f = make_download_file(filename="Music\\01 - Track.mp3", username="user1")
        snapshot = [{
            "username": "user1",
            "directories": [{"files": [{
                "filename": "Music\\01 - Track.mp3",
                "id": "file-id-1",
                "state": "Completed, Succeeded",
                "size": 5000000,
            }]}],
        }]
        ok = slskd_download_status([f], ctx, snapshot=snapshot)
        self.assertTrue(ok)
        self.assertIsNotNone(f.status)
        assert f.status is not None
        self.assertEqual(f.status["state"], "Completed, Succeeded")
        # No per-file API calls should have been made
        self.assertEqual(slskd.transfers.get_download_calls, [])

    def test_bulk_snapshot_file_not_found(self):
        """When snapshot doesn't contain the file, status is None, returns False."""
        from lib.download import slskd_download_status
        ctx = _make_ctx(slskd=FakeSlskdAPI())
        f = make_download_file(filename="Music\\missing.mp3", username="user1")
        snapshot = [{"username": "user1", "directories": [{"files": []}]}]
        ok = slskd_download_status([f], ctx, snapshot=snapshot)
        self.assertFalse(ok)
        self.assertIsNone(f.status)


class TestSlskdDoEnqueue(unittest.TestCase):

    def test_successful_enqueue(self):
        from lib.download import slskd_do_enqueue
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track.mp3", "id": "new-id"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "new-id")
        self.assertEqual(slskd.transfers.enqueue_calls[0].files, files)
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True])

    def test_enqueue_failure_returns_none(self):
        from lib.download import slskd_do_enqueue
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = Exception("fail")
        ctx = _make_ctx(slskd=slskd)
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", [], "dir", ctx)
        self.assertIsNone(result)

    def test_enqueue_polls_until_ids_found(self):
        """Transfer IDs appear on 2nd poll — should resolve in 2 iterations, not 5s."""
        from lib.download import slskd_do_enqueue
        snapshot_with_id = [{
            "username": "user1",
            "directories": [{"files": [{"filename": "track.mp3", "id": "tid-1"}]}],
        }]
        snapshot_without_id = [{
            "username": "user1",
            "directories": [{"files": []}],
        }]
        slskd = FakeSlskdAPI(
            download_snapshots=[snapshot_without_id, snapshot_with_id])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "tid-1")
        # Should have polled twice
        self.assertEqual(len(slskd.transfers.get_all_downloads_calls), 2)

    def test_enqueue_timeout_returns_partial(self):
        """Transfer IDs never appear — should return whatever we have after timeout."""
        from lib.download import slskd_do_enqueue
        # Never returns the transfer ID
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"files": []}],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        # Should return empty list (no files matched), not None
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 0)


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
            db_mb_release_id="", db_search_filetype_override=None, db_target_format=None,
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
            db_mb_release_id="", db_search_filetype_override=None, db_target_format=None,
        )
        search_fn = MagicMock(return_value=({}, [], [failed_album]))
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)

    def test_sets_downloading_status(self):
        """After enqueue, album_requests.status = 'downloading'."""
        from lib.download import grab_most_wanted
        entry = make_grab_list_entry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.flac",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=30000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        grab_most_wanted([], search_fn, ctx)
        row = fake_db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertEqual(fake_db.status_history, [(42, "downloading")])

    def test_writes_active_download_state(self):
        """JSONB written with correct structure."""
        from lib.download import grab_most_wanted
        import json
        entry = make_grab_list_entry(
            album_id=1,
            filetype="mp3 v0",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.mp3",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=5000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        grab_most_wanted([], search_fn, ctx)
        state_raw = fake_db.request(42)["active_download_state"]
        assert isinstance(state_raw, str)
        state = json.loads(state_raw)
        self.assertEqual(state["filetype"], "mp3 v0")
        self.assertEqual(len(state["files"]), 1)

    def test_no_blocking_monitor(self):
        """grab_most_wanted returns immediately without blocking."""
        from lib.download import grab_most_wanted
        import time as _time
        entry = make_grab_list_entry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.flac",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=30000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
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

    def test_bulk_downloads_respects_username(self):
        from lib.download import match_transfer_id
        downloads = [
            {
                "username": "Mr. Odd",
                "directories": [{"directory": "a", "files": [
                    {"filename": "shared\\01.flac", "id": "wrong-id"},
                ]}],
            },
            {
                "username": "Miick Starr",
                "directories": [{"directory": "b", "files": [
                    {"filename": "shared\\01.flac", "id": "right-id"},
                ]}],
            },
        ]
        result = match_transfer_id(
            downloads,
            "shared\\01.flac",
            username="Miick Starr",
        )
        self.assertEqual(result, "right-id")

    def test_bulk_downloads_prefers_active_over_old_completed(self):
        from lib.download import match_transfer
        downloads = [
            {
                "username": "user1",
                "directories": [{"directory": "d", "files": [
                    {
                        "filename": "shared\\01.flac",
                        "id": "completed-id",
                        "state": "Completed, Succeeded",
                        "endedAt": "2026-04-03T21:00:00+00:00",
                    },
                    {
                        "filename": "shared\\01.flac",
                        "id": "active-id",
                        "state": "InProgress",
                        "startedAt": "2026-04-03T22:00:00+00:00",
                    },
                ]}],
            },
        ]
        result = match_transfer(downloads, "shared\\01.flac", username="user1")
        assert result is not None
        self.assertEqual(result["id"], "active-id")

    def test_bulk_downloads_prefers_latest_successful_attempt(self):
        from lib.download import match_transfer
        downloads = [
            {
                "username": "user1",
                "directories": [{"directory": "d", "files": [
                    {
                        "filename": "shared\\01.flac",
                        "id": "old-cancelled",
                        "state": "Completed, Cancelled",
                        "endedAt": "2026-04-03T20:00:00+00:00",
                    },
                    {
                        "filename": "shared\\01.flac",
                        "id": "new-succeeded",
                        "state": "Completed, Succeeded",
                        "endedAt": "2026-04-03T21:00:00+00:00",
                    },
                ]}],
            },
        ]
        result = match_transfer(downloads, "shared\\01.flac", username="user1")
        assert result is not None
        self.assertEqual(result["id"], "new-succeeded")


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
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "u\\M", "files": [
                {"filename": "u\\M\\01.flac", "id": "new-id-1"},
                {"filename": "u\\M\\02.flac", "id": "new-id-2"},
            ]}],
        }])
        rederive_transfer_ids(entry, slskd)
        self.assertEqual(entry.files[0].id, "new-id-1")
        self.assertEqual(entry.files[1].id, "new-id-2")
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True])

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
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "u\\M", "files": []}],
        }])
        rederive_transfer_ids(entry, slskd)
        self.assertEqual(entry.files[0].id, "")

    def test_uses_bulk_downloads_for_spacey_usernames(self):
        from lib.download import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="Miick Starr\\Album\\01.flac",
                    id="",
                    file_dir="Miick Starr\\Album",
                    username="Miick Starr",
                    size=1000,
                ),
                DownloadFile(
                    filename="Mr. Odd\\Album\\01.flac",
                    id="",
                    file_dir="Mr. Odd\\Album",
                    username="Mr. Odd",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[
            {
                "username": "Mr. Odd",
                "directories": [{"directory": "Mr. Odd\\Album", "files": [
                    {"filename": "Mr. Odd\\Album\\01.flac", "id": "odd-id"},
                ]}],
            },
            {
                "username": "Miick Starr",
                "directories": [{"directory": "Miick Starr\\Album", "files": [
                    {"filename": "Miick Starr\\Album\\01.flac", "id": "starr-id"},
                ]}],
            },
        ])

        rederive_transfer_ids(entry, slskd)

        self.assertEqual(entry.files[0].id, "starr-id")
        self.assertEqual(entry.files[1].id, "odd-id")
        self.assertEqual(slskd.transfers.get_downloads_calls, [])

    def test_terminal_snapshot_sets_file_status(self):
        from lib.download import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="user1\\Album\\01.flac",
                    id="",
                    file_dir="user1\\Album",
                    username="user1",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "user1\\Album", "files": [
                {
                    "filename": "user1\\Album\\01.flac",
                    "id": "done-id",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 1000,
                },
            ]}],
        }])

        rederive_transfer_ids(entry, slskd)

        self.assertEqual(entry.files[0].id, "done-id")
        status = entry.files[0].status
        self.assertIsNotNone(status)
        assert status is not None
        self.assertEqual(status["state"], "Completed, Succeeded")


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

            files = [make_download_file(filename="source_dir\\01 - Track.mp3",
                                        file_dir="source_dir")]
            album = make_grab_list_entry(files=files, mb_release_id="")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False
            mock_mt.load_file.return_value = MagicMock()
            result = process_completed_album(album, [], ctx)
            self.assertTrue(result)

    def test_returns_false_on_file_move_failure(self):
        """File move failure returns False."""
        from lib.download import process_completed_album
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmpdir:
            # Source dir exists but file doesn't — move will fail
            files = [make_download_file(filename="nonexistent_dir\\01 - Track.mp3",
                                        file_dir="nonexistent_dir")]
            album = make_grab_list_entry(files=files, mb_release_id="")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False
            result = process_completed_album(album, [], ctx)
            self.assertFalse(result)


class TestResolveSlskdLocalPath(unittest.TestCase):
    """Pure tests for the slskd on-disk path resolution helper.

    slskd places files at ``{download_root}/{last_remote_folder}/{filename}``.
    On filename collision with an existing file, slskd appends a `_<ticks>`
    suffix before the extension (the `_<18-19-digit-integer>` pattern seen on
    disk). The resolver must find the file in both cases. Regression guard
    for issue #144.
    """

    def test_returns_expected_path_when_file_exists(self):
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(tmpdir, "CD1")
            os.makedirs(folder)
            fname = "17.- The Crystals - Da Doo Ron Ron.mp3"
            src = os.path.join(folder, fname)
            with open(src, "w") as fp:
                fp.write("x")
            f = make_download_file(
                filename=f"@@ctvhz\\Shared Music\\Phil Spector\\CD1\\{fname}",
                file_dir="@@ctvhz\\Shared Music\\Phil Spector\\CD1",
                size=1,
            )
            self.assertEqual(resolve_slskd_local_path(f, tmpdir), src)

    def test_matches_case_insensitive_filename_in_leaf_folder(self):
        """Production regression: slskd's on-disk casing can drift from the
        remote path. Resolve by case-insensitive basename match before giving
        up on the folder."""
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(tmpdir, "Kid A")
            os.makedirs(folder)
            src = os.path.join(folder, "04 How to Disappear Completely.mp3")
            with open(src, "w") as fp:
                fp.write("x" * 1234)
            f = make_download_file(
                filename="@@wcren\\Music\\Radiohead\\Kid A\\04 How To Disappear Completely.mp3",
                file_dir="@@wcren\\Music\\Radiohead\\Kid A",
                size=1234,
            )
            self.assertEqual(resolve_slskd_local_path(f, tmpdir), src)

    def test_finds_collision_renamed_file(self):
        """slskd renames colliding filenames as ``<base>_<ticks><ext>``.

        When the plain filename is absent, the resolver must match the
        ticks-suffixed variant. Reproduces the #144 crash scenario — file
        ``17.- ...Da Doo Ron Ron.mp3`` landed on disk as
        ``17.- ...Da Doo Ron Ron_639123086573912108.mp3``.
        """
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(tmpdir, "CD1")
            os.makedirs(folder)
            base = "17.- The Crystals - Da Doo Ron Ron"
            suffixed = os.path.join(folder, f"{base}_639123086573912108.mp3")
            with open(suffixed, "w") as fp:
                fp.write("x" * 2048)
            f = make_download_file(
                filename=f"@@ctvhz\\CD1\\{base}.mp3",
                file_dir="@@ctvhz\\CD1",
                size=2048,
            )
            self.assertEqual(resolve_slskd_local_path(f, tmpdir), suffixed)

    def test_prefers_size_match_when_multiple_collision_candidates(self):
        """Size tiebreak disambiguates when multiple collision-suffixed
        variants exist (multiple users uploading same-named files to the
        same on-disk folder — frequent with generic ``CD1`` folders)."""
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(tmpdir, "CD1")
            os.makedirs(folder)
            base = "01 - Track"
            wrong = os.path.join(folder, f"{base}_639000000000000001.mp3")
            right = os.path.join(folder, f"{base}_639000000000000002.mp3")
            with open(wrong, "w") as fp:
                fp.write("x" * 100)
            with open(right, "w") as fp:
                fp.write("x" * 5555)
            f = make_download_file(
                filename=f"user\\CD1\\{base}.mp3",
                file_dir="user\\CD1",
                size=5555,
            )
            self.assertEqual(resolve_slskd_local_path(f, tmpdir), right)

    def test_recursively_finds_incomplete_ancestor_folder(self):
        """Production regression: some installs keep completed files under
        ``incomplete/<album>/CD2`` instead of ``<download_root>/CD2``.
        The resolver must search likely ancestor folders, not just the leaf
        ``CD2`` directory."""
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(
                tmpdir, "incomplete", "1991- Back To Mono (1958-1969)", "CD2")
            os.makedirs(folder)
            fname = "01.- The Ronettes - Be My Baby.mp3"
            src = os.path.join(folder, fname)
            with open(src, "w") as fp:
                fp.write("x" * 4321)
            f = make_download_file(
                filename=(
                    "@@ctvhz\\Shared Music\\Everything Else Music\\"
                    "Phil Spector [USA]\\1991- Back To Mono (1958-1969)\\"
                    f"CD2\\{fname}"
                ),
                file_dir=(
                    "@@ctvhz\\Shared Music\\Everything Else Music\\"
                    "Phil Spector [USA]\\1991- Back To Mono (1958-1969)\\CD2"
                ),
                size=4321,
            )
            self.assertEqual(resolve_slskd_local_path(f, tmpdir), src)

    def test_does_not_cross_album_match_from_download_root(self):
        """Never fall back to a basename-only hit from an unrelated album."""
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            wrong_folder = os.path.join(tmpdir, "Other Album", "CD1")
            os.makedirs(wrong_folder)
            wrong = os.path.join(wrong_folder, "04.mp3")
            with open(wrong, "w") as fp:
                fp.write("x" * 4096)
            f = make_download_file(
                filename="@@user\\Wanted Album\\CD1\\04.mp3",
                file_dir="@@user\\Wanted Album\\CD1",
                size=4096,
            )
            self.assertIsNone(resolve_slskd_local_path(f, tmpdir))

    def test_request_source_without_mbid_requeues_instead_of_marking_done(self):
        """Request rows without an MBID must requeue, not mark imported."""
        from lib.download import _handle_valid_result
        import tempfile

        album = make_grab_list_entry(
            files=[make_download_file()],
            mb_release_id="",
            db_source="request",
            db_request_id=42,
        )
        bv_result = MagicMock()
        bv_result.distance = 0.05
        bv_result.scenario = "strong_match"
        bv_result.to_json.return_value = '{"valid": true}'

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ctx = make_ctx_with_fake_db(db)
        ctx.cfg.beets_distance_threshold = 0.15

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch("lib.download.log_validation_result"):
            import_dir = os.path.join(tmpdir, "Test Artist - Test Album")
            os.makedirs(import_dir)
            with open(os.path.join(import_dir, "01 - Track.mp3"), "w",
                      encoding="utf-8") as fp:
                fp.write("x")

            outcome = _handle_valid_result(album, bv_result, import_dir, ctx)

            assert outcome is not None
            self.assertFalse(outcome.success)
            self.assertFalse(os.path.exists(import_dir))
            failed_dir = os.path.join(tmpdir, "failed_imports")
            self.assertTrue(os.path.isdir(failed_dir))
            self.assertEqual(len(os.listdir(failed_dir)), 1)

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(db.request(42)["validation_attempts"], 1)
        self.assertEqual(len(db.download_logs), 1)

    def test_returns_none_when_file_missing(self):
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(os.path.join(tmpdir, "CD1"))
            f = make_download_file(
                filename="user\\CD1\\nope.mp3",
                file_dir="user\\CD1",
                size=1,
            )
            self.assertIsNone(resolve_slskd_local_path(f, tmpdir))

    def test_does_not_match_partial_base_prefix(self):
        """Globbing must not match ``01 - Track Two.mp3`` when looking for
        ``01 - Track.mp3`` — the collision suffix is ``_<digits>``, not a
        free-form extension of the basename."""
        from lib.download import resolve_slskd_local_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(tmpdir, "CD1")
            os.makedirs(folder)
            with open(os.path.join(folder, "01 - Track Two.mp3"), "w") as fp:
                fp.write("x")
            f = make_download_file(
                filename="user\\CD1\\01 - Track.mp3",
                file_dir="user\\CD1",
                size=1,
            )
            self.assertIsNone(resolve_slskd_local_path(f, tmpdir))


class TestProcessCompletedAlbumCollisionSuffix(unittest.TestCase):
    """Integration: ``process_completed_album`` must handle slskd's collision
    rename. Directly reproduces the issue #144 crash — file landed on disk
    with ``_<ticks>`` appended; the move must still succeed."""

    @patch("lib.download.music_tag")
    def test_moves_collision_renamed_files(self, mock_mt):
        from lib.download import process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(tmpdir, "CD1")
            os.makedirs(folder)
            base = "17.- The Crystals - Da Doo Ron Ron"
            src_suffixed = os.path.join(folder, f"{base}_639123086573912108.mp3")
            with open(src_suffixed, "w") as fp:
                fp.write("fake audio")

            files = [make_download_file(
                filename=f"@@ctvhz\\Phil Spector\\CD1\\{base}.mp3",
                file_dir="@@ctvhz\\Phil Spector\\CD1",
                size=len("fake audio"),
            )]
            album = make_grab_list_entry(files=files, mb_release_id="",
                                          artist="Phil Spector", title="Back to Mono",
                                          year="1991")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False
            mock_mt.load_file.return_value = MagicMock()
            result = process_completed_album(album, [], ctx)
            self.assertTrue(result)
            # Source should be gone; destination should exist.
            self.assertFalse(os.path.exists(src_suffixed))
            import_folder = os.path.join(
                tmpdir, "Phil Spector - Back to Mono (1991)")
            moved = os.listdir(import_folder)
            self.assertEqual(len(moved), 1)
            # Destination keeps the clean (non-suffixed) basename.
            self.assertEqual(moved[0], f"{base}.mp3")

    @patch("lib.download.music_tag")
    def test_moves_file_from_incomplete_ancestor_folder(self, mock_mt):
        """Regression: full processing must tolerate legacy slskd layouts
        where the finished files still sit under ``incomplete/<album>/CD2``.
        """
        from lib.download import process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(
                tmpdir, "incomplete", "1991- Back To Mono (1958-1969)", "CD2")
            os.makedirs(folder)
            fname = "01.- The Ronettes - Be My Baby.mp3"
            src = os.path.join(folder, fname)
            with open(src, "w") as fp:
                fp.write("fake audio")

            files = [make_download_file(
                filename=(
                    "@@ctvhz\\Shared Music\\Everything Else Music\\"
                    "Phil Spector [USA]\\1991- Back To Mono (1958-1969)\\"
                    f"CD2\\{fname}"
                ),
                file_dir=(
                    "@@ctvhz\\Shared Music\\Everything Else Music\\"
                    "Phil Spector [USA]\\1991- Back To Mono (1958-1969)\\CD2"
                ),
                size=len("fake audio"),
            )]
            files[0].disk_no = 2
            files[0].disk_count = 3
            album = make_grab_list_entry(
                files=files, mb_release_id="", artist="Phil Spector",
                title="Back to Mono", year="1991")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False
            mock_mt.load_file.return_value = MagicMock()

            result = process_completed_album(album, [], ctx)

            self.assertTrue(result)
            self.assertFalse(os.path.exists(src))
            self.assertFalse(os.path.exists(folder))
            import_folder = os.path.join(
                tmpdir, "Phil Spector - Back to Mono (1991)")
            moved = os.listdir(import_folder)
            self.assertEqual(moved, ["Disk 2 - 01.- The Ronettes - Be My Baby.mp3"])

    @patch("lib.download.music_tag")
    def test_moves_file_with_forward_slash_remote_path(self, mock_mt):
        """Compatibility: some remote path payloads may arrive slash-normalized.
        Basename extraction and on-disk resolution must accept either separator.
        """
        from lib.download import process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = os.path.join(tmpdir, "Kid A")
            os.makedirs(folder)
            fname = "04 How To Disappear Completely.mp3"
            src = os.path.join(folder, fname)
            with open(src, "w") as fp:
                fp.write("fake audio")

            files = [make_download_file(
                filename=f"@@wcren/Music/Radiohead/Kid A/{fname}",
                file_dir="@@wcren/Music/Radiohead/Kid A",
                size=len("fake audio"),
            )]
            album = make_grab_list_entry(
                files=files, mb_release_id="", artist="Radiohead",
                title="Kid A", year="2000")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False
            mock_mt.load_file.return_value = MagicMock()

            result = process_completed_album(album, [], ctx)

            self.assertTrue(result)
            self.assertFalse(os.path.exists(src))
            import_folder = os.path.join(tmpdir, "Radiohead - Kid A (2000)")
            moved = os.listdir(import_folder)
            self.assertEqual(moved, [fname])


class TestPollActiveDownloads(unittest.TestCase):
    """Test poll_active_downloads() — core polling function."""

    def _make_downloading_row(self, request_id=1, state_dict=None):
        """Build a mock album_requests row with status='downloading'."""
        if state_dict is None:
            state_dict = {
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
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
            "search_filetype_override": None,
            "target_format": None,
            "status": "downloading",
            "active_download_state": state_dict,
        }

    def _make_poll_ctx(self, downloading_rows=None, slskd_downloads=None):
        """Build context with fake DB + fake slskd for polling."""
        if slskd_downloads is None:
            # Default: return transfers that match the files
            slskd_downloads = [{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "InProgress",
                        "bytesTransferred": 1,
                    },
                ]}],
            }]
        fake_db = FakePipelineDB()
        for row in downloading_rows or []:
            fake_db.seed_request(row)
        cfg = _make_ctx().cfg
        ctx = make_ctx_with_fake_db(
            fake_db,
            cfg=cfg,
            slskd=FakeSlskdAPI(downloads=slskd_downloads),
        )
        return ctx, fake_db

    def _download_state(self, fake_db: FakePipelineDB, request_id: int = 1):
        state = fake_db.request(request_id)["active_download_state"]
        assert isinstance(state, dict)
        return state

    def test_poll_active_no_downloading(self):
        """No downloading albums → no-op."""
        from lib.download import poll_active_downloads
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[])
        poll_active_downloads(ctx)
        self.assertEqual(fake_db.clear_download_state_calls, [])

    @patch("lib.download.process_completed_album")
    def test_poll_active_all_complete(self, mock_process):
        """1 downloading album, all files complete → calls process_completed_album."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        def mark_imported(*args):
            fake_db.update_status(1, "imported")
            return True

        mock_process.side_effect = mark_imported
        poll_active_downloads(ctx)

        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        self.assertEqual(fake_db.request(1)["status"], "imported")
        mock_process.assert_called_once()

    @patch("lib.download.process_completed_album")
    def test_poll_active_all_complete_no_beets(self, mock_process):
        """beets_validation_enabled=False → process returns True, poll sets imported."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        mock_process.return_value = True
        poll_active_downloads(ctx)

        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        self.assertEqual(fake_db.request(1)["status"], "imported")
        self.assertIsNone(fake_db.request(1)["active_download_state"])

    def test_poll_active_timeout(self):
        """No byte/state progress for stalled_timeout → cancel, log, reset to wanted."""
        from lib.download import poll_active_downloads
        stale = "2020-01-01T00:00:00+00:00"
        state_dict = {
            "filetype": "flac",
            "enqueued_at": stale,
            "last_progress_at": stale,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "bytes_transferred": 12345, "last_state": "InProgress"},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 12345,
                }]}],
            }],
        )

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.recorded_attempts, [(1, "download")])

    def test_poll_active_old_album_with_progress_does_not_timeout(self):
        """Fresh byte progress should refresh stall timer even for an old album."""
        from lib.download import poll_active_downloads
        stale = "2020-01-01T00:00:00+00:00"
        state_dict = {
            "filetype": "flac",
            "enqueued_at": stale,
            "last_progress_at": stale,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "bytes_transferred": 12345, "last_state": "InProgress"},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 22345,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][0]["bytes_transferred"], 22345)
        self.assertIsNotNone(persisted["last_progress_at"])

    def test_poll_active_transfer_vanished_all(self):
        """slskd returns no matching transfers → treat as timeout."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": []}],
            }],
        )
        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    @patch("lib.download.process_completed_album")
    def test_poll_active_completed_removed_transfer_uses_snapshot_status(self, mock_process):
        """Completed transfers from includeRemoved=true should import, not timeout."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "done-id",
                        "state": "Completed, Succeeded",
                        "bytesTransferred": 30000000,
                        "endedAt": "2026-04-03T21:00:00+00:00",
                    },
                ]}],
            }],
        )

        def mark_imported(*args):
            fake_db.update_status(1, "imported")
            return True

        mock_process.side_effect = mark_imported

        with patch("lib.download.slskd_download_status") as mock_status:
            poll_active_downloads(ctx)

        mock_status.assert_not_called()
        mock_process.assert_called_once()
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "imported")

    def test_poll_active_in_progress(self):
        """Files still downloading with fresh state transition → persist progress snapshot."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 2048,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        # Should NOT process or timeout
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

    @patch("lib.download.process_completed_album")
    def test_poll_active_multiple_albums(self, mock_process):
        """2 albums: 1 completes, 1 in progress → correct handling."""
        from lib.download import poll_active_downloads
        row1 = self._make_downloading_row(request_id=1)
        state2 = {
            "filetype": "mp3 v0",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user2", "filename": "user2\\Music\\01.mp3",
                 "file_dir": "user2\\Music", "size": 5000000},
            ],
        }
        row2 = self._make_downloading_row(request_id=2, state_dict=state2)
        row2["album_title"] = "Album 2"
        row2["artist_name"] = "Artist 2"

        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row1, row2],
            slskd_downloads=[
                {
                    "username": "user1",
                    "directories": [{"directory": "user1\\Music", "files": [{
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "Completed, Succeeded",
                        "bytesTransferred": 30000000,
                    }]}],
                },
                {
                    "username": "user2",
                    "directories": [{"directory": "user2\\Music", "files": [{
                        "filename": "user2\\Music\\01.mp3",
                        "id": "tid-2",
                        "state": "InProgress",
                        "bytesTransferred": 2048,
                    }]}],
                },
            ],
        )

        # slskd returns transfers for both users
        self.assertEqual(cast(FakeSlskdAPI, ctx.slskd).transfers.get_all_downloads_calls, [])

        def mark_imported(*args):
            fake_db.update_status(1, "imported")
            return True

        mock_process.side_effect = mark_imported
        poll_active_downloads(ctx)

        # Album 1 persists processing_started_at, album 2 persists progress.
        self.assertEqual(len(fake_db.update_download_state_calls), 2)
        update_request_ids = [
            request_id for request_id, _ in fake_db.update_download_state_calls
        ]
        self.assertEqual(update_request_ids, [1, 2])
        self.assertIsNone(fake_db.request(1)["active_download_state"])
        self.assertIsNotNone(self._download_state(fake_db, 2)["last_progress_at"])
        mock_process.assert_called_once()

    def test_poll_crash_recovery_no_state(self):
        """Downloading album with no active_download_state → reset to wanted."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        row["active_download_state"] = None  # Simulates crash
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])

        poll_active_downloads(ctx)

        # apply_transition calls reset_to_wanted for downloading→wanted
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.status_history, [(1, "wanted")])

    @patch("lib.download.process_completed_album")
    def test_poll_active_all_errors(self, mock_process):
        """All files errored → timeout the album."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "Completed, Errored",
                    },
                ]}],
            }],
        )
        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        mock_process.assert_not_called()
        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

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
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Queued, Remotely",
                }]}],
            }],
        )
        cfg = cast(Any, ctx.cfg)
        cfg.remote_queue_timeout = 120  # 2 minutes
        cfg.stalled_timeout = 600  # 10 minutes (not exceeded)

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    def test_poll_active_remote_queue_does_not_use_stalled_timeout(self):
        """Fully remote-queued albums should not hit stalled_timeout first."""
        from lib.download import poll_active_downloads
        now = datetime.now(timezone.utc)
        enqueued_at = (now - timedelta(seconds=200)).isoformat()
        stale_progress = (now - timedelta(seconds=1200)).isoformat()
        state_dict = {
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "last_progress_at": stale_progress,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Queued, Remotely",
                }]}],
            }],
        )
        cfg = cast(Any, ctx.cfg)
        cfg.remote_queue_timeout = 3600
        cfg.stalled_timeout = 120

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.update_download_state_calls, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

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
            "enqueued_at": _utc_now_iso(),
            "files": files,
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])

        # Only files 0-4 have transfers in slskd
        slskd_files = [
            {
                "filename": f"user1\\Music\\{i:02d}.flac",
                "id": f"tid-{i}",
                "state": "InProgress",
            }
            for i in range(5)
        ]
        slskd = cast(FakeSlskdAPI, ctx.slskd)
        slskd.set_downloads([{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": slskd_files}],
        }])
        slskd.transfers.enqueue_result = False

        with self.assertLogs("cratedigger", level="WARNING") as logs:
            poll_active_downloads(ctx)

        # Should NOT process — 7 files vanished (errored), album not complete
        mock_process.assert_not_called()
        self.assertEqual(
            "\n".join(logs.output).count("Failed to re-enqueue file"),
            7,
        )
        self.assertEqual(len(slskd.transfers.enqueue_calls), 7)
        self.assertEqual(fake_db.clear_download_state_calls, [])
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")


    @patch("lib.download.process_completed_album")
    def test_poll_active_partial_errors_with_retry(self, mock_process):
        """Some files errored, retries available → re-enqueue those files."""
        from lib.download import poll_active_downloads
        # 3 files: 2 complete, 1 errored
        state_dict = {
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
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
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[],
        )

        # First snapshot is the poll cycle; second snapshot is the
        # post-requeue transfer-id lookup.
        poll_snapshot = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [
                {
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                },
                {
                    "filename": "user1\\Music\\02.flac",
                    "id": "tid-2",
                    "state": "Completed, Succeeded",
                },
                {
                    "filename": "user1\\Music\\03.flac",
                    "id": "tid-3",
                    "state": "Completed, Errored",
                },
            ]}],
        }]
        requeue_snapshot = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\03.flac",
                "id": "new-tid-3",
                "state": "Queued, Locally",
            }]}],
        }]
        slskd = cast(FakeSlskdAPI, ctx.slskd)
        slskd.queue_download_snapshots(poll_snapshot, requeue_snapshot)

        with patch("time.sleep"):
            poll_active_downloads(ctx)

        # Should NOT process (not all done) and NOT timeout
        mock_process.assert_not_called()
        self.assertEqual(fake_db.download_logs, [])
        # Should re-enqueue the errored file
        self.assertEqual(len(slskd.transfers.enqueue_calls), 1)
        enqueue_call = slskd.transfers.enqueue_calls[0]
        self.assertEqual(enqueue_call.username, "user1")
        self.assertEqual(
            enqueue_call.files,
            [{"filename": "user1\\Music\\03.flac", "size": 20000000}],
        )
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][2]["retry_count"], 1)

    def test_poll_active_get_all_downloads_api_error_waits_for_next_cycle(self):
        """Transient bulk-download API failures must not be treated as vanished transfers."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])
        cast(FakeSlskdAPI, ctx.slskd).transfers.get_all_downloads_error = (
            RuntimeError("temporary slskd failure")
        )

        with patch("lib.download.cancel_and_delete") as mock_cancel:
            poll_active_downloads(ctx)

        mock_cancel.assert_not_called()
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.update_download_state_calls, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(fake_db.status_history, [])

    @patch("lib.download.process_completed_album")
    def test_poll_active_completion_exception_persists_processing_state(self, mock_process):
        """Exceptions after completion should leave persisted state for the next cycle to resume."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        mock_process.side_effect = RuntimeError("boom")
        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.status_history, [])
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        persisted = self._download_state(fake_db)
        self.assertIsNotNone(persisted["processing_started_at"])

    @patch("lib.download.process_completed_album")
    def test_poll_no_redownload_window(self, mock_process):
        """Album stays 'downloading' during process_completed_album — no redownload window."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        def mark_imported(*args):
            fake_db.update_status(1, "imported")
            return True

        mock_process.side_effect = mark_imported
        poll_active_downloads(ctx)

        # process_completed_album ran
        mock_process.assert_called_once()
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        self.assertNotIn((1, "wanted"), fake_db.status_history)
        self.assertEqual(fake_db.request(1)["status"], "imported")


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
        self.assertEqual(state.files[0].retry_count, 0)

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

    def test_persists_retry_count(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000,
                             retry=4),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.files[0].retry_count, 4)

    def test_persists_progress_fields(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(
                    filename="u\\M\\01.flac", id="tid-1",
                    file_dir="u\\M", username="user1", size=30000000,
                    bytes_transferred=2048, last_state="InProgress",
                ),
            ],
        )
        state = build_active_download_state(
            entry,
            enqueued_at="2026-04-03T12:00:00+00:00",
            last_progress_at="2026-04-03T12:05:00+00:00",
        )
        self.assertEqual(state.last_progress_at, "2026-04-03T12:05:00+00:00")
        self.assertEqual(state.files[0].bytes_transferred, 2048)
        self.assertEqual(state.files[0].last_state, "InProgress")

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
        self.assertEqual(state.last_progress_at, state.enqueued_at)


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
            "search_filetype_override": None,
            "target_format": None,
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
        self.assertEqual(entry.files[0].retry, 0)

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
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].disk_no, 1)
        self.assertEqual(entry.files[0].disk_count, 2)
        self.assertEqual(entry.files[1].disk_no, 2)

    def test_reconstruct_search_filetype_override(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": "flac", "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.db_search_filetype_override, "flac")

    def test_reconstruct_retry_count(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=30000000, retry_count=5,
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].retry, 5)

    def test_reconstruct_progress_fields(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            last_progress_at="2026-04-03T12:05:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1",
                    filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music",
                    size=30000000,
                    bytes_transferred=4096,
                    last_state="InProgress",
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].bytes_transferred, 4096)
        self.assertEqual(entry.files[0].last_state, "InProgress")

    def test_reconstruct_missing_year(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": None, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.year, "")


# ============================================================================
# _compute_rejection_backfill — orchestration test for cfg threading
# ============================================================================
#
# Pins that ctx.cfg.quality_ranks actually reaches rejection_backfill_override.
# Pure-function tests in test_quality_decisions.py cover the decision logic
# itself; this test guards the wiring layer between download.py and
# lib/quality.py so a future refactor can't silently drop the cfg argument.

class TestComputeRejectionBackfillCfgThreading(unittest.TestCase):
    """_compute_rejection_backfill must thread ctx.cfg.quality_ranks through
    to rejection_backfill_override."""

    def _setup(self, *, gate_min_rank, on_disk_min_bitrate=180):
        """Build the fixtures: fake DB with a genuine non-lossless request,
        a ctx with the requested gate_min_rank, and a mock BeetsDB returning
        an MP3 VBR album at on_disk_min_bitrate."""
        from lib.quality import QualityRankConfig
        from lib.beets_db import AlbumInfo

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-test",
            current_spectral_grade="genuine",
            verified_lossless=False,
            search_filetype_override=None,
        ))

        # Real CratediggerConfig is heavy; a SimpleNamespace with quality_ranks
        # is enough — _compute_rejection_backfill only reads ctx.cfg.quality_ranks.
        from types import SimpleNamespace
        cfg = SimpleNamespace(
            quality_ranks=QualityRankConfig(gate_min_rank=gate_min_rank),
        )
        ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)

        album_data = make_grab_list_entry(
            db_request_id=42,
            db_search_filetype_override=None,
            mb_release_id="mbid-test",
        )

        beets_info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=on_disk_min_bitrate,
            avg_bitrate_kbps=on_disk_min_bitrate,
            format="MP3", is_cbr=False,
            album_path="/Beets/A/B",
        )
        return album_data, ctx, beets_info

    def _run(self, album_data, ctx, beets_info):
        from lib.download import _compute_rejection_backfill
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = beets_info
            mock_beets_cls.return_value = mock_beets
            return _compute_rejection_backfill(album_data, ctx)

    def test_lenient_gate_min_rank_fires_backfill(self):
        """gate_min_rank=GOOD: 180kbps VBR genuine → GOOD rank → backfill fires."""
        from lib.quality import QualityRank, QUALITY_LOSSLESS
        album_data, ctx, beets_info = self._setup(
            gate_min_rank=QualityRank.GOOD)
        result = self._run(album_data, ctx, beets_info)
        self.assertEqual(result, QUALITY_LOSSLESS,
                         "lenient cfg must reach rejection_backfill_override")

    def test_default_gate_min_rank_blocks_backfill(self):
        """Default gate_min_rank=EXCELLENT: same 180kbps blocks (GOOD < EXCELLENT)."""
        from lib.quality import QualityRank
        album_data, ctx, beets_info = self._setup(
            gate_min_rank=QualityRank.EXCELLENT)
        result = self._run(album_data, ctx, beets_info)
        self.assertIsNone(result,
                          "strict cfg must also reach rejection_backfill_override "
                          "— if cfg threading were broken, both branches would "
                          "use the same default and this assertion would silently pass")


if __name__ == "__main__":
    unittest.main()
