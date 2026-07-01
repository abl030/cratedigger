"""Tests for the phase-1 slskd event ingestion (issue #146).

Events are seeded newest-first (index 0 = most recent), mirroring the
live feed. Payload `data` is a JSON string exactly as slskd emits it.
"""

from __future__ import annotations

import json
import unittest

from lib.quality import ActiveDownloadState, ActiveDownloadFileState
from lib.slskd_events import (
    EVENT_PAGE_LIMIT,
    MAX_EVENT_PAGES,
    ingest_download_file_events,
)
from tests.fakes import FakePipelineDB, FakeSlskdAPI


def _file_complete_data(
    *,
    username: str,
    filename: str,
    local_filename: str,
    transfer_id: str = "t-1",
    size: int = 1000,
) -> str:
    return json.dumps({
        "version": 0,
        "localFilename": local_filename,
        "remoteFilename": filename,
        "transfer": {
            "id": transfer_id,
            "username": username,
            "filename": filename,
            "size": size,
        },
    })


def _state_json(files: list[ActiveDownloadFileState]) -> str:
    return ActiveDownloadState(
        filetype="flac",
        enqueued_at="2026-07-01T00:00:00+00:00",
        files=files,
    ).to_json()


def _file_state(
    username: str = "peer1",
    filename: str = "music\\Artist\\Album\\01 track.flac",
) -> ActiveDownloadFileState:
    return ActiveDownloadFileState(
        username=username,
        filename=filename,
        file_dir="music\\Artist\\Album",
        size=1000,
    )


class SlskdEventIngestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.slskd = FakeSlskdAPI()

    def seed_downloading(
        self,
        request_id: int = 1,
        files: list[ActiveDownloadFileState] | None = None,
        status: str = "downloading",
    ) -> None:
        self.db.seed_request({
            "id": request_id,
            "status": status,
            "artist_name": "Artist",
            "album_title": "Album",
            "active_download_state": json.loads(
                _state_json(files if files is not None else [_file_state()])),
        })

    def event(self, *, id: str, timestamp: str,
              type: str = "DownloadFileComplete", data: str = "{}"):
        return self.slskd.events.make_event(
            id=id, timestamp=timestamp, type=type, data=data)

    def ingest(self):
        return ingest_download_file_events(
            self.db, self.slskd, self.db.get_downloading())

    def file_local_path(self, request_id: int = 1, index: int = 0) -> str | None:
        state = ActiveDownloadState.from_dict(
            self.db.request(request_id)["active_download_state"])
        return state.files[index].local_path


class TestBootstrap(SlskdEventIngestCase):
    def test_no_cursor_seeds_from_newest_without_processing(self):
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-new", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
        ])

        result = self.ingest()

        self.assertEqual(result.outcome, "bootstrapped")
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-new")
        # Bootstrap never backfills: the event predates the cursor.
        self.assertIsNone(self.file_local_path())

    def test_empty_feed_writes_no_cursor(self):
        result = self.ingest()

        self.assertEqual(result.outcome, "empty_feed")
        self.assertIsNone(self.db.get_slskd_event_cursor())


class TestIngestStamping(SlskdEventIngestCase):
    def setUp(self) -> None:
        super().setUp()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")

    def test_new_file_complete_event_stamps_local_path(self):
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        result = self.ingest()

        self.assertEqual(result.outcome, "ingested")
        self.assertEqual(result.events_seen, 1)
        self.assertEqual(result.file_events, 1)
        self.assertEqual(result.files_stamped, 1)
        self.assertEqual(result.requests_updated, 1)
        self.assertFalse(result.cursor_gap)
        self.assertEqual(self.file_local_path(), "/dl/Album/01 track.flac")
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-1")

    def test_collision_suffixed_local_filename_is_stored_verbatim(self):
        # The whole point of the refactor: slskd's _<ticks> rename is
        # authoritative and needs no reverse-engineering.
        suffixed = "/dl/Album/01 track_638827305447447018.flac"
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename=suffixed)),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        self.ingest()

        self.assertEqual(self.file_local_path(), suffixed)

    def test_events_match_by_username_and_filename(self):
        self.seed_downloading(files=[
            _file_state(username="peer1"),
            _file_state(username="peer2"),  # same filename, other peer
        ])
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer2",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        self.ingest()

        self.assertIsNone(self.file_local_path(index=0))
        self.assertEqual(
            self.file_local_path(index=1), "/dl/Album/01 track.flac")

    def test_newest_event_wins_for_repeated_downloads(self):
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-2", timestamp="2026-07-01T11:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track_999.flac")),
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        self.ingest()

        self.assertEqual(self.file_local_path(), "/dl/Album/01 track_999.flac")

    def test_undecodable_payload_is_skipped_not_fatal(self):
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-2", timestamp="2026-07-01T11:00:00.0000000Z",
                data=json.dumps({"localFilename": 42})),  # type drift
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        result = self.ingest()

        self.assertEqual(result.files_stamped, 1)
        self.assertEqual(self.file_local_path(), "/dl/Album/01 track.flac")

    def test_directory_complete_events_are_counted_but_not_stamped(self):
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                type="DownloadDirectoryComplete",
                data=json.dumps({
                    "version": 0,
                    "localDirectoryName": "/dl/Album",
                    "remoteDirectoryName": "music\\Artist\\Album",
                    "username": "peer1",
                })),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        result = self.ingest()

        self.assertEqual(result.outcome, "ingested")
        self.assertEqual(result.events_seen, 1)
        self.assertEqual(result.file_events, 0)
        self.assertIsNone(self.file_local_path())

    def test_non_downloading_row_is_not_updated(self):
        self.seed_downloading(status="manual")
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        result = ingest_download_file_events(self.db, self.slskd, [])

        self.assertEqual(result.requests_updated, 0)
        self.assertIsNone(self.file_local_path())

    def test_second_run_with_advanced_cursor_is_idempotent(self):
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
            self.event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"),
        ])

        first = self.ingest()
        second = self.ingest()

        self.assertEqual(first.outcome, "ingested")
        self.assertEqual(second.outcome, "no_new_events")
        self.assertEqual(second.files_stamped, 0)


class TestCursorPaging(SlskdEventIngestCase):
    def setUp(self) -> None:
        super().setUp()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")

    def test_older_timestamp_stops_scan_when_cursor_id_missing(self):
        # Cursor event pruned from the feed: the timestamp bound stops
        # the walk instead of scanning the full history.
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Artist\\Album\\01 track.flac",
                    local_filename="/dl/Album/01 track.flac")),
            self.event(
                id="ev-ancient", timestamp="2026-06-30T00:00:00.0000000Z"),
        ])

        result = self.ingest()

        self.assertEqual(result.outcome, "ingested")
        self.assertEqual(result.events_seen, 1)
        self.assertFalse(result.cursor_gap)

    def test_multi_page_scan_collects_across_pages(self):
        events = [
            self.event(
                id=f"ev-{i}",
                timestamp=f"2026-07-01T10:00:{59 - (i % 60):02d}.0000000Z",
                type="Noise")
            for i in range(EVENT_PAGE_LIMIT + 5)
        ]
        events.append(self.event(
            id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"))
        self.slskd.events.set_events(events)

        result = self.ingest()

        self.assertEqual(result.outcome, "ingested")
        self.assertEqual(result.events_seen, EVENT_PAGE_LIMIT + 5)
        self.assertFalse(result.cursor_gap)
        # Two pages fetched.
        self.assertGreaterEqual(len(self.slskd.events.list_calls), 2)

    def test_page_cap_reports_cursor_gap_and_still_advances(self):
        events = [
            self.event(
                id=f"ev-{i}", timestamp="2026-07-01T10:00:00.0000000Z",
                type="Noise")
            for i in range(EVENT_PAGE_LIMIT * MAX_EVENT_PAGES + 1)
        ]
        events.append(self.event(
            id="ev-cursor", timestamp="2026-07-01T09:00:00.0000000Z"))
        self.slskd.events.set_events(events)

        result = self.ingest()

        self.assertEqual(result.outcome, "ingested")
        self.assertTrue(result.cursor_gap)
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-0")


if __name__ == "__main__":
    unittest.main()
