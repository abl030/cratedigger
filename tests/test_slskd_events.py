"""Tests for the phase-1 slskd event ingestion (issue #146).

Events are seeded newest-first (index 0 = most recent), mirroring the
live feed. Payload `data` is a JSON string exactly as slskd emits it.
"""

from __future__ import annotations

import copy
import json
import unittest
from collections.abc import Callable, Sequence

from lib.pipeline_db import TransferLedgerRow
from lib.quality import ActiveDownloadFileState, ActiveDownloadState
from lib.slskd_events import (
    EVENT_PAGE_LIMIT,
    MAX_EVENT_PAGES,
    EventIngestResult,
    ingest_download_file_events,
)
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import handoff_automation_owner, own_transfer_keys
from tests.helpers import (
    make_active_download_file_state as _file_state,
)
from tests.helpers import (
    make_file_complete_event_data as _file_complete_data,
)


class _BeforeStateWriteDB(FakePipelineDB):
    """Inject one incarnation replacement immediately before event CAS."""

    def __init__(self) -> None:
        super().__init__()
        self.before_state_write: Callable[[], None] | None = None
        self.state_write_attempts = 0

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        self.state_write_attempts += 1
        before_state_write = self.before_state_write
        self.before_state_write = None
        if before_state_write is not None:
            before_state_write()
        return super().update_download_state_if_downloading(
            request_id,
            state_json,
            expected_enqueued_at=expected_enqueued_at,
        )


class _LedgerStampFailureDB(FakePipelineDB):
    """Raise at one ordered ledger effect while preserving earlier writes."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_ledger_stamp_call: int | None = 2
        self.ledger_stamp_calls: list[tuple[str, str, str]] = []

    def stamp_transfer_completion(
        self,
        username: str,
        filename: str,
        local_path: str,
    ) -> int:
        self.ledger_stamp_calls.append((username, filename, local_path))
        if len(self.ledger_stamp_calls) == self.fail_ledger_stamp_call:
            raise RuntimeError("injected second ledger stamp failure")
        return super().stamp_transfer_completion(
            username,
            filename,
            local_path,
        )


class _CursorUpsertFailureDB(FakePipelineDB):
    """Fail the first post-bootstrap cursor commit, after earlier effects."""

    def __init__(self) -> None:
        super().__init__()
        self.cursor_upsert_calls = 0
        self.fail_cursor_upsert_call: int | None = 2

    def upsert_slskd_event_cursor(
        self,
        last_event_id: str,
        last_event_timestamp: str,
    ) -> None:
        self.cursor_upsert_calls += 1
        if self.cursor_upsert_calls == self.fail_cursor_upsert_call:
            raise RuntimeError("injected final cursor upsert failure")
        super().upsert_slskd_event_cursor(
            last_event_id,
            last_event_timestamp,
        )


class SlskdEventIngestCase(unittest.TestCase):
    #: Whether `seed_downloading` also writes the accepted ledger rows a
    #: production `downloading` request always carries. True is the
    #: production shape; classes that drive ledger state themselves (to
    #: test the ledger's own gate, or a partial/failed ledger world) set
    #: this False and own that seeding explicitly.
    SEED_OWNS_FILES = True

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.slskd = FakeSlskdAPI()

    def seed_downloading(
        self,
        request_id: int = 1,
        files: list[ActiveDownloadFileState] | None = None,
        status: str = "downloading",
        enqueued_at: str = "2026-07-01T00:00:00+00:00",
        own_files: bool | None = None,
    ) -> None:
        """Seed a request in the shape production actually persists.

        ``own_files`` defaults True because a ``downloading`` request's
        files ALWAYS carry accepted write-ahead ledger rows in
        production -- see ``tests.helpers.own_transfer_keys`` for the
        write order that guarantees it. Pass False deliberately to model
        the foreign / never-accepted key.
        """
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at=enqueued_at,
            files=files if files is not None else [_file_state()],
        )
        self.db.seed_request({
            "id": request_id,
            "status": status,
            "artist_name": "Artist",
            "album_title": "Album",
            "active_download_state": json.loads(state.to_json()),
        })
        owns = self.SEED_OWNS_FILES if own_files is None else own_files
        if not owns:
            return
        self.own_files(state.files, request_id=request_id)

    def own_files(
        self,
        files: Sequence[ActiveDownloadFileState],
        *,
        request_id: int = 1,
    ) -> None:
        """Ensure these files' queue keys carry accepted ledger rows."""
        own_transfer_keys(
            self.db,
            [(file.username, file.filename) for file in files],
            request_id=request_id,
        )

    def event(self, *, id: str, timestamp: str,
              type: str = "DownloadFileComplete", data: str = "{}"):
        return self.slskd.events.make_event(
            id=id, timestamp=timestamp, type=type, data=data)

    def ingest(self):
        return ingest_download_file_events(self.db, self.slskd)

    def file_local_path(self, request_id: int = 1, index: int = 0) -> str | None:
        state = ActiveDownloadState.from_dict(
            self.db.request(request_id)["active_download_state"])
        return state.files[index].local_path


class TestEventIngestResult(unittest.TestCase):
    def test_log_line_exposes_cursor_commit_and_hold_reason(self):
        result = EventIngestResult(
            outcome="ingested",
            events_seen=2,
            file_events=1,
            files_stamped=0,
            requests_updated=0,
            transfers_stamped=1,
            cursor_gap=False,
            cursor_advanced=False,
            cursor_hold_reason="lost_current_incarnation_write",
        )

        self.assertEqual(
            result.to_log_line(),
            "SLSKD EVENTS: outcome=ingested events_seen=2 file_events=1 "
            "files_stamped=0 requests_updated=0 transfers_stamped=1 "
            "cursor_gap=False cursor_advanced=False "
            "cursor_hold_reason=lost_current_incarnation_write",
        )


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
        self.assertTrue(result.cursor_advanced)
        self.assertIsNone(result.cursor_hold_reason)
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

    def test_foreign_completion_never_stamps_our_state(self):
        """#1278 item 1: ONE ingestion pass, ONE ownership rule.

        `_stamp_transfer_ledger` refuses a key with no accepted POST, in
        this same pass, off the same decoded events. `_stamp_local_paths`
        did not — it matched purely on `(username, filename)` plus a time
        bound, so a foreign client's completion at our queue key was
        written into OUR `active_download_state` as an authoritative
        local path, and everything downstream then treated a stranger's
        file as the album we downloaded.
        """
        self.seed_downloading(own_files=False)
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

        self.assertIsNone(self.file_local_path())
        self.assertEqual(result.files_stamped, 0)
        self.assertEqual(result.requests_updated, 0)

    def test_pending_intent_alone_never_stamps_our_state(self):
        """A write-ahead row records that we ASKED, never that slskd
        agreed — the same boundary `stamp_transfer_completion` enforces
        for the ledger's own `local_path`."""
        self.seed_downloading(own_files=False)
        self.db.record_transfer_enqueue([TransferLedgerRow(
            request_id=1,
            username="peer1",
            filename="music\\Artist\\Album\\01 track.flac",
        )])
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

        self.ingest()

        self.assertIsNone(self.file_local_path())

    def test_owned_key_from_an_earlier_attempt_still_stamps(self):
        """Deliberately per-KEY, not per-attempt.

        Live measurement (2026-08-26) drove this: pending ledger ROWS
        still occur weekly (35 in one recent week), but pending-only
        KEYS — a key never once accepted — stopped in July. An ambiguous
        POST is normally followed by an accepted retry on the SAME key,
        so a per-attempt gate would strand exactly the downloads that
        currently recover, while a per-key gate still excludes a key we
        have never owned. Having once created a transfer for this exact
        (peer, remote path) is what makes the completed bytes ours.
        """
        self.seed_downloading(own_files=True)
        # A later ambiguous attempt adds pending intent on the same key.
        self.db.record_transfer_enqueue([TransferLedgerRow(
            request_id=1,
            username="peer1",
            filename="music\\Artist\\Album\\01 track.flac",
        )])
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

        self.ingest()

        self.assertEqual(self.file_local_path(), "/dl/Album/01 track.flac")

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
                id="ev-3", timestamp="2026-07-01T11:30:00.0000000Z",
                data="{ not valid json"),  # DecodeError, not ValidationError
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
        self.seed_downloading(status="unsearchable")
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

        result = ingest_download_file_events(self.db, self.slskd)

        self.assertEqual(result.requests_updated, 0)
        self.assertIsNone(self.file_local_path())

    def test_processing_row_is_not_updated(self):
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-07-01T00:00:00+00:00",
            files=[_file_state()],
        )
        self.db.seed_request({
            "id": 1,
            "status": "wanted",
            "artist_name": "Artist",
            "album_title": "Album",
        })
        handoff_automation_owner(self.db, 1, state=state.to_json())
        before = copy.deepcopy(self.db.request(1)["active_download_state"])
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

        result = ingest_download_file_events(self.db, self.slskd)

        self.assertEqual(self.db.get_downloading(), [])
        self.assertEqual(result.requests_updated, 0)
        self.assertEqual(
            self.db.request(1)["active_download_state"],
            before,
        )

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
        self.assertFalse(second.cursor_advanced)
        self.assertIsNone(second.cursor_hold_reason)


class TestIncarnationAwareStamping(SlskdEventIngestCase):
    """Issue #898 U3: current-attempt and replay-window contracts."""

    def setUp(self) -> None:
        super().setUp()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")

    def _file_event(
        self,
        *,
        event_id: str,
        timestamp: str,
        username: str = "peer1",
        filename: str = "music\\Artist\\Album\\01 track.flac",
        local_path: str = "/dl/current.flac",
    ):
        return self.event(
            id=event_id,
            timestamp=timestamp,
            data=_file_complete_data(
                username=username,
                filename=filename,
                local_filename=local_path,
            ),
        )

    def _cursor_event(self):
        return self.event(
            id="ev-cursor",
            timestamp="2026-07-01T00:00:00.0000000Z",
            type="Noise",
        )

    def test_same_key_event_before_current_incarnation_does_not_stamp(self):
        self.seed_downloading(enqueued_at="2026-07-01T10:00:00+00:00")
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-old",
                timestamp="2026-07-01T09:59:59.9999999Z",
            ),
            self._cursor_event(),
        ])

        result = self.ingest()

        self.assertIsNone(self.file_local_path())
        self.assertEqual(result.files_stamped, 0)
        self.assertTrue(result.cursor_advanced)
        self.assertIsNone(result.cursor_hold_reason)

    def test_same_key_event_at_current_incarnation_stamps_with_utc_normalization(self):
        # The witness remains the exact CAS token while comparison normalizes
        # its offset through the event timestamp parser.
        witness = "2026-07-01T18:00:00+08:00"
        self.seed_downloading(enqueued_at=witness)
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-at",
                timestamp="2026-07-01T10:00:00.0000000Z",
            ),
            self._cursor_event(),
        ])

        result = self.ingest()

        self.assertEqual(self.file_local_path(), "/dl/current.flac")
        self.assertEqual(result.files_stamped, 1)
        state = ActiveDownloadState.from_dict(
            self.db.request(1)["active_download_state"])
        self.assertEqual(state.enqueued_at, witness)

    def test_newer_ineligible_same_key_candidate_does_not_shadow_older_eligible(self):
        self.seed_downloading(enqueued_at="2026-07-01T10:00:00+00:00")
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-newer-feed-position",
                timestamp="2026-07-01T09:00:00.0000000Z",
                local_path="/dl/ineligible.flac",
            ),
            self._file_event(
                event_id="ev-older-feed-position",
                timestamp="2026-07-01T10:00:00.0000000Z",
                local_path="/dl/eligible.flac",
            ),
            self._cursor_event(),
        ])

        self.ingest()

        self.assertEqual(self.file_local_path(), "/dl/eligible.flac")

    def test_fresh_different_key_incarnation_uses_current_attempt(self):
        self.seed_downloading(
            files=[_file_state(username="peer-a", filename="A\\01.flac")],
            enqueued_at="2026-07-01T08:00:00+00:00",
        )
        self.seed_downloading(
            files=[_file_state(username="peer-b", filename="B\\01.flac")],
            enqueued_at="2026-07-01T10:00:00+00:00",
        )
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-b",
                timestamp="2026-07-01T10:00:00.0000000Z",
                username="peer-b",
                filename="B\\01.flac",
                local_path="/dl/B/01.flac",
            ),
            self._cursor_event(),
        ])

        result = ingest_download_file_events(self.db, self.slskd)

        self.assertEqual(result.files_stamped, 1)
        self.assertEqual(self.file_local_path(), "/dl/B/01.flac")

    def test_fresh_different_key_pre_incarnation_event_does_not_stamp(self):
        self.seed_downloading(
            files=[_file_state(username="peer-a", filename="A\\01.flac")],
            enqueued_at="2026-07-01T08:00:00+00:00",
        )
        self.seed_downloading(
            files=[_file_state(username="peer-b", filename="B\\01.flac")],
            enqueued_at="2026-07-01T10:00:00+00:00",
        )
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-pre-b",
                timestamp="2026-07-01T09:59:59.0000000Z",
                username="peer-b",
                filename="B\\01.flac",
                local_path="/dl/B/old.flac",
            ),
            self._cursor_event(),
        ])

        result = ingest_download_file_events(self.db, self.slskd)

        self.assertEqual(result.files_stamped, 0)
        self.assertIsNone(self.file_local_path())
        self.assertTrue(result.cursor_advanced)

    def test_lost_dirty_write_holds_complete_cursor_then_replay_converges(self):
        self.db = _BeforeStateWriteDB()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")
        # seed_downloading owns the key (SEED_OWNS_FILES) -- one accepted
        # ledger row, which the replacement below reuses rather than
        # duplicating (see tests.helpers.own_transfer_keys).
        self.seed_downloading(enqueued_at="2026-07-01T09:00:00+00:00")
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-complete",
                timestamp="2026-07-01T10:00:00.0000000Z",
            ),
            self._cursor_event(),
        ])
        cursor_before = self.db.get_slskd_event_cursor()
        assert cursor_before is not None
        replacement_rows: list[dict[str, object]] = []

        def replace_b_with_c() -> None:
            self.seed_downloading(
                enqueued_at="2026-07-01T09:30:00+00:00")
            replacement_rows.append(copy.deepcopy(self.db.request(1)))

        self.db.before_state_write = replace_b_with_c

        first = self.ingest()

        self.assertEqual(first.files_stamped, 0)
        self.assertEqual(first.requests_updated, 0)
        self.assertEqual(first.transfers_stamped, 1)
        self.assertFalse(first.cursor_advanced)
        self.assertEqual(
            first.cursor_hold_reason, "lost_current_incarnation_write")
        self.assertEqual(self.db.get_slskd_event_cursor(), cursor_before)
        self.assertEqual(len(replacement_rows), 1)
        self.assertEqual(self.db.request(1), replacement_rows[0])
        self.assertIsNone(self.file_local_path())
        self.assertEqual(self.db.state_write_attempts, 1)
        ledger_rows = list(self.db._transfer_ledger.values())
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(ledger_rows[0].local_path, "/dl/current.flac")

        second = self.ingest()

        self.assertEqual(second.files_stamped, 1)
        self.assertEqual(second.transfers_stamped, 0)
        self.assertTrue(second.cursor_advanced)
        self.assertIsNone(second.cursor_hold_reason)
        self.assertEqual(self.file_local_path(), "/dl/current.flac")
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-complete")
        self.assertEqual(
            cursor["last_event_timestamp"],
            "2026-07-01T10:00:00.0000000Z",
        )
        self.assertEqual(len(self.db._transfer_ledger), 1)
        self.assertEqual(ledger_rows[0].local_path, "/dl/current.flac")

    def test_exception_after_partial_success_holds_cursor_and_replay_converges(self):
        self.seed_downloading(
            request_id=1,
            files=[_file_state(username="peer1", filename="A\\01.flac")],
            enqueued_at="2026-07-01T09:00:00+00:00",
        )
        self.seed_downloading(
            request_id=2,
            files=[_file_state(username="peer2", filename="B\\01.flac")],
            enqueued_at="2026-07-01T09:00:00+00:00",
        )
        self.db.set_update_download_state_error(
            2, RuntimeError("injected second write failure"))
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-a",
                timestamp="2026-07-01T10:00:00.0000000Z",
                username="peer1",
                filename="A\\01.flac",
                local_path="/dl/A/01.flac",
            ),
            self._file_event(
                event_id="ev-b",
                timestamp="2026-07-01T10:00:00.0000000Z",
                username="peer2",
                filename="B\\01.flac",
                local_path="/dl/B/01.flac",
            ),
            self._cursor_event(),
        ])
        cursor_before = self.db.get_slskd_event_cursor()
        assert cursor_before is not None

        with self.assertRaisesRegex(RuntimeError, "second write failure"):
            self.ingest()

        self.assertEqual(self.file_local_path(1), "/dl/A/01.flac")
        self.assertIsNone(self.file_local_path(2))
        self.assertEqual(self.db.get_slskd_event_cursor(), cursor_before)

        self.db._update_download_state_errors.pop(2)
        replay = self.ingest()

        self.assertEqual(replay.files_stamped, 1)
        self.assertEqual(self.file_local_path(1), "/dl/A/01.flac")
        self.assertEqual(self.file_local_path(2), "/dl/B/01.flac")
        self.assertTrue(replay.cursor_advanced)

    def test_partial_ledger_exception_holds_cursor_and_replay_converges(self):
        self.db = _LedgerStampFailureDB()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")
        keys = (
            ("peer1", "A\\01.flac", "/dl/A/01.flac"),
            ("peer2", "B\\01.flac", "/dl/B/01.flac"),
        )
        for request_id, (username, filename, _local_path) in enumerate(
            keys,
            start=1,
        ):
            # seed_downloading owns each key (SEED_OWNS_FILES): one
            # accepted ledger row per request.
            self.seed_downloading(
                request_id=request_id,
                files=[_file_state(username=username, filename=filename)],
                enqueued_at="2026-07-01T09:00:00+00:00",
            )
        self.slskd.events.set_events([
            self._file_event(
                event_id=f"ev-{index}",
                timestamp="2026-07-01T10:00:00.0000000Z",
                username=username,
                filename=filename,
                local_path=local_path,
            )
            for index, (username, filename, local_path) in enumerate(keys)
        ] + [self._cursor_event()])
        cursor_before = self.db.get_slskd_event_cursor()
        assert cursor_before is not None

        with self.assertRaisesRegex(
            RuntimeError,
            "second ledger stamp failure",
        ):
            self.ingest()

        # Active-state effects precede the ledger projection and are already
        # durable; replay must treat them as clean no-ops.
        self.assertEqual(self.file_local_path(1), "/dl/A/01.flac")
        self.assertEqual(self.file_local_path(2), "/dl/B/01.flac")
        self.assertEqual(self.db.get_slskd_event_cursor(), cursor_before)
        ledger_rows = list(self.db._transfer_ledger.values())
        self.assertEqual(len(ledger_rows), 2)
        self.assertEqual(
            [row.local_path for row in ledger_rows],
            ["/dl/A/01.flac", None],
        )

        self.db.fail_ledger_stamp_call = None
        replay = self.ingest()

        self.assertEqual(replay.files_stamped, 0)
        self.assertEqual(replay.requests_updated, 0)
        self.assertEqual(replay.transfers_stamped, 1)
        self.assertTrue(replay.cursor_advanced)
        self.assertEqual(len(self.db._transfer_ledger), 2)
        self.assertEqual(
            [row.local_path for row in ledger_rows],
            ["/dl/A/01.flac", "/dl/B/01.flac"],
        )
        self.assertEqual(
            self.db.ledger_stamp_calls,
            [
                ("peer1", "A\\01.flac", "/dl/A/01.flac"),
                ("peer2", "B\\01.flac", "/dl/B/01.flac"),
                ("peer1", "A\\01.flac", "/dl/A/01.flac"),
                ("peer2", "B\\01.flac", "/dl/B/01.flac"),
            ],
        )
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-0")
        self.assertEqual(
            cursor["last_event_timestamp"],
            "2026-07-01T10:00:00.0000000Z",
        )

    def test_cursor_upsert_failure_replays_pre_cursor_effects_idempotently(self):
        self.db = _CursorUpsertFailureDB()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")
        self.seed_downloading(enqueued_at="2026-07-01T09:00:00+00:00")
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-complete",
                timestamp="2026-07-01T10:00:00.0000000Z",
            ),
            self._cursor_event(),
        ])
        cursor_before = self.db.get_slskd_event_cursor()
        assert cursor_before is not None

        with self.assertRaisesRegex(
            RuntimeError,
            "final cursor upsert failure",
        ):
            self.ingest()

        state_after_failure = copy.deepcopy(
            self.db.request(1)["active_download_state"])
        self.assertEqual(self.file_local_path(), "/dl/current.flac")
        self.assertEqual(
            state_after_failure["enqueued_at"],
            "2026-07-01T09:00:00+00:00",
        )
        self.assertEqual(self.db.get_slskd_event_cursor(), cursor_before)
        ledger_rows = list(self.db._transfer_ledger.values())
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(ledger_rows[0].local_path, "/dl/current.flac")

        replay = self.ingest()

        self.assertEqual(replay.files_stamped, 0)
        self.assertEqual(replay.requests_updated, 0)
        self.assertEqual(replay.transfers_stamped, 0)
        self.assertTrue(replay.cursor_advanced)
        self.assertIsNone(replay.cursor_hold_reason)
        self.assertEqual(
            self.db.request(1)["active_download_state"],
            state_after_failure,
        )
        replay_ledger_rows = list(self.db._transfer_ledger.values())
        self.assertEqual(len(replay_ledger_rows), 1)
        self.assertEqual(
            replay_ledger_rows[0].local_path,
            "/dl/current.flac",
        )
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-complete")
        self.assertEqual(
            cursor["last_event_timestamp"],
            "2026-07-01T10:00:00.0000000Z",
        )
        self.assertEqual(self.db.cursor_upsert_calls, 3)

    def test_malformed_occurrence_is_ledger_only_and_does_not_hold_cursor(self):
        self.seed_downloading(enqueued_at="2026-07-01T09:00:00+00:00")
        self.slskd.events.set_events([
            self._file_event(
                event_id="ev-malformed-time",
                timestamp="not-an-occurrence-time",
            ),
            self._cursor_event(),
        ])

        result = self.ingest()

        self.assertIsNone(self.file_local_path())
        self.assertEqual(result.transfers_stamped, 1)
        self.assertTrue(result.cursor_advanced)
        self.assertIsNone(result.cursor_hold_reason)
        ledger_rows = list(self.db._transfer_ledger.values())
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(ledger_rows[0].local_path, "/dl/current.flac")
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-malformed-time")
        self.assertEqual(
            cursor["last_event_timestamp"],
            "not-an-occurrence-time",
        )

    def test_invalid_current_witness_excludes_active_but_keeps_ledger_progress(self):
        for witness in ("", "not-an-iso-witness"):
            with self.subTest(witness=witness):
                self.db = FakePipelineDB()
                self.db.upsert_slskd_event_cursor(
                    "ev-cursor", "2026-07-01T00:00:00.0000000Z")
                self.seed_downloading(enqueued_at=witness)
                state_before = copy.deepcopy(
                    self.db.request(1)["active_download_state"])
                self.slskd.events.set_events([
                    self._file_event(
                        event_id="ev-valid",
                        timestamp="2026-07-01T10:00:00.0000000Z",
                    ),
                    self._cursor_event(),
                ])

                with self.assertLogs(
                    "cratedigger",
                    level="WARNING",
                ) as captured:
                    result = self.ingest()

                self.assertTrue(any(
                    "invalid enqueued_at witness" in line
                    for line in captured.output))
                self.assertEqual(
                    self.db.request(1)["active_download_state"],
                    state_before,
                )
                self.assertIsNone(self.file_local_path())
                self.assertEqual(result.transfers_stamped, 1)
                self.assertTrue(result.cursor_advanced)
                ledger_rows = list(self.db._transfer_ledger.values())
                self.assertEqual(len(ledger_rows), 1)
                self.assertEqual(
                    ledger_rows[0].local_path,
                    "/dl/current.flac",
                )
                cursor = self.db.get_slskd_event_cursor()
                assert cursor is not None
                self.assertEqual(cursor["last_event_id"], "ev-valid")
                self.assertEqual(
                    cursor["last_event_timestamp"],
                    "2026-07-01T10:00:00.0000000Z",
                )

    def test_cursor_gap_fail_open_advances_despite_lost_dirty_write(self):
        self.db = _BeforeStateWriteDB()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")
        self.seed_downloading(enqueued_at="2026-07-01T09:00:00+00:00")
        events = [
            self._file_event(
                event_id="ev-0",
                timestamp="2026-07-01T10:00:00.0000000Z",
            ),
        ]
        events.extend(
            self.event(
                id=f"ev-{i}",
                timestamp="2026-07-01T10:00:00.0000000Z",
                type="Noise",
            )
            for i in range(1, EVENT_PAGE_LIMIT * MAX_EVENT_PAGES + 1)
        )
        events.append(self._cursor_event())
        self.slskd.events.set_events(events)

        def replace_b_with_c() -> None:
            self.seed_downloading(
                enqueued_at="2026-07-01T11:00:00+00:00")

        self.db.before_state_write = replace_b_with_c

        with self.assertLogs("cratedigger", level="WARNING") as captured:
            result = self.ingest()

        self.assertTrue(result.cursor_gap)
        self.assertTrue(result.cursor_advanced)
        self.assertIsNone(result.cursor_hold_reason)
        self.assertTrue(any("fail-open" in line for line in captured.output))
        cursor = self.db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-0")
        self.assertIsNone(self.file_local_path())


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

    def test_unparseable_new_event_timestamp_does_not_terminate_scan(self):
        # A garbage timestamp on one NEW event must not act as an
        # "older than cursor" terminator stranding everything behind it.
        self.seed_downloading()
        self.slskd.events.set_events([
            self.event(
                id="ev-2", timestamp="not-a-timestamp", type="Noise"),
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

        self.assertEqual(result.events_seen, 2)
        self.assertEqual(result.files_stamped, 1)
        self.assertEqual(self.file_local_path(), "/dl/Album/01 track.flac")

    def test_missing_total_count_keeps_paging_to_cursor(self):
        # total_count=None (header absent) must not stop the scan after
        # page 1 — the cursor stop still has to be reached.
        self.seed_downloading()
        events = [
            self.event(
                id=f"ev-{i}",
                timestamp="2026-07-01T10:00:00.0000000Z", type="Noise")
            for i in range(EVENT_PAGE_LIMIT + 3)
        ]
        events.append(self.event(
            id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z"))
        self.slskd.events.set_events(events)
        self.slskd.events.omit_total_count = True

        result = self.ingest()

        self.assertEqual(result.outcome, "ingested")
        self.assertEqual(result.events_seen, EVENT_PAGE_LIMIT + 3)
        self.assertFalse(result.cursor_gap)
        self.assertGreaterEqual(len(self.slskd.events.list_calls), 2)

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


class TestRecentCompletionPaths(SlskdEventIngestCase):
    def _dir_complete_data(
        self,
        *,
        username: str,
        remote_dir: str,
        local_dir: str,
    ) -> str:
        return json.dumps({
            "version": 0,
            "localDirectoryName": local_dir,
            "remoteDirectoryName": remote_dir,
            "username": username,
        })

    def test_maps_file_and_directory_events(self):
        from lib.slskd_events import recent_completion_paths
        self.slskd.events.set_events([
            self.event(
                id="ev-1", timestamp="2026-07-02T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Album\\01.flac",
                    local_filename="/dl/Album/01.flac")),
            self.event(
                id="ev-2", timestamp="2026-07-02T10:00:00.0000000Z",
                type="DownloadDirectoryComplete",
                data=self._dir_complete_data(
                    username="peer1",
                    remote_dir="music\\Album",
                    local_dir="/dl/Album")),
        ])

        recent = recent_completion_paths(self.slskd)

        self.assertEqual(
            recent.files[("peer1", "music\\Album\\01.flac")],
            "/dl/Album/01.flac")
        self.assertEqual(
            recent.directories[("peer1", "music\\Album")], "/dl/Album")

    def test_undecodable_payload_is_skipped_not_fatal(self):
        from lib.slskd_events import recent_completion_paths
        self.slskd.events.set_events([
            self.event(
                id="ev-bad-schema", timestamp="2026-07-02T10:00:00.0000000Z",
                data="{}"),
            # Malformed (non-JSON) data raises msgspec.DecodeError, not
            # ValidationError — must be skipped, not escape the pass.
            self.event(
                id="ev-bad-json", timestamp="2026-07-02T10:00:00.0000000Z",
                data="{ not valid json"),
            self.event(
                id="ev-good", timestamp="2026-07-02T10:00:00.0000000Z",
                data=_file_complete_data(
                    username="peer1",
                    filename="music\\Album\\01.flac",
                    local_filename="/dl/Album/01.flac")),
        ])

        recent = recent_completion_paths(self.slskd)

        self.assertEqual(len(recent.files), 1)

    def test_feed_failure_returns_empty_maps(self):
        from lib.slskd_events import recent_completion_paths
        self.slskd.events.list_error = RuntimeError("events down")

        recent = recent_completion_paths(self.slskd)

        self.assertEqual(recent.files, {})
        self.assertEqual(recent.directories, {})


class TestTransferLedgerStamping(SlskdEventIngestCase):
    """T2 pin (issue #571): the transfer ledger is stamped in the SAME
    ingestion pass, from the SAME (username, remote filename) events,
    that already stamps ``active_download_state``."""

    # This class exists to test the ledger's OWN gate, so it seeds every
    # ledger row itself; pre-owning would make "unledgered" worlds
    # unreachable.
    SEED_OWNS_FILES = False

    def setUp(self) -> None:
        super().setUp()
        self.db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")

    def test_ledgered_pair_gets_stamped_in_the_same_pass(self):
        from lib.pipeline_db import TransferLedgerRow

        self.seed_downloading()
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=1, username="peer1",
                filename="music\\Artist\\Album\\01 track.flac"),
        ])
        self.db.confirm_transfer_enqueue(
            "peer1", "music\\Artist\\Album\\01 track.flac",
            request_id=1)
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

        self.assertEqual(result.transfers_stamped, 1)
        rows = list(self.db._transfer_ledger.values())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].local_path, "/dl/Album/01 track.flac")
        self.assertIsNotNone(rows[0].accepted_at)
        # Same pass, same key -- active_download_state is ALSO stamped.
        self.assertEqual(self.file_local_path(), "/dl/Album/01 track.flac")

    def test_unledgered_pair_stamps_nothing_and_invents_no_row(self):
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

        self.assertEqual(result.transfers_stamped, 0)
        self.assertEqual(self.db._transfer_ledger, {})
        # One pass, one ownership rule (#1278 item 1). This assertion used
        # to read "the two writes are independent" and pin the OPPOSITE
        # path -- the unledgered pair still stamped active_download_state.
        # That independence was the defect: the ledger refused the key
        # while the active stamp accepted it off the same event.
        self.assertIsNone(self.file_local_path())

    def test_reprocessing_the_same_event_window_is_idempotent(self):
        from lib.pipeline_db import TransferLedgerRow

        self.seed_downloading()
        self.db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=1, username="peer1",
                filename="music\\Artist\\Album\\01 track.flac"),
        ])
        self.db.confirm_transfer_enqueue(
            "peer1", "music\\Artist\\Album\\01 track.flac",
            request_id=1)
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
        self.assertEqual(first.transfers_stamped, 1)

        # The cursor advanced past ev-1; a second pass over the SAME feed
        # sees no new events and re-stamps nothing.
        second = self.ingest()

        self.assertEqual(second.outcome, "no_new_events")
        self.assertEqual(second.transfers_stamped, 0)
        rows = list(self.db._transfer_ledger.values())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].local_path, "/dl/Album/01 track.flac")

if __name__ == "__main__":
    unittest.main()
